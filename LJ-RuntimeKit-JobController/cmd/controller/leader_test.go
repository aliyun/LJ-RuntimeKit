/*
Copyright (c) 2024-2026 Alibaba Cloud.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/textlogger"
)

type electionTestLock struct {
	mu      sync.Mutex
	record  resourcelock.LeaderElectionRecord
	exists  bool
	fail    atomic.Bool
	updates atomic.Int32
}

func (l *electionTestLock) Get(context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.exists {
		return nil, nil, apierrors.NewNotFound(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, "test")
	}
	if l.fail.Load() {
		return nil, nil, errors.New("API unavailable")
	}
	record := l.record
	data, err := json.Marshal(record)
	return &record, data, err
}

func (l *electionTestLock) Create(_ context.Context, record resourcelock.LeaderElectionRecord) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.record = record
	l.exists = true
	return nil
}

func (l *electionTestLock) Update(_ context.Context, record resourcelock.LeaderElectionRecord) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.fail.Load() {
		return errors.New("API unavailable")
	}
	l.record = record
	l.updates.Add(1)
	return nil
}

func (*electionTestLock) RecordEvent(string) {}
func (*electionTestLock) Identity() string   { return "test-controller" }
func (*electionTestLock) Describe() string   { return "test-lease" }

func electionTestConfig(lock *electionTestLock) leaderelection.LeaderElectionConfig {
	return leaderelection.LeaderElectionConfig{
		Lock:          lock,
		LeaseDuration: time.Second,
		RenewDeadline: 500 * time.Millisecond,
		RetryPeriod:   25 * time.Millisecond,
	}
}

func waitElectionSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for election signal")
	}
}

func waitElectionResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("leader election did not return")
		return nil
	}
}

func TestLeaderElectionLogsAtDefaultVerbosity(t *testing.T) {
	for _, loss := range []bool{false, true} {
		t.Run(fmt.Sprintf("loss=%v", loss), func(t *testing.T) {
			var output bytes.Buffer
			logger := textlogger.NewLogger(textlogger.NewConfig(textlogger.Verbosity(0), textlogger.Output(&output)))
			ctx, cancel := context.WithCancel(klog.NewContext(context.Background(), logger))
			defer cancel()
			lock := &electionTestLock{}
			started, canceled, drain := make(chan struct{}), make(chan struct{}), make(chan struct{})
			result := make(chan error, 1)
			go func() {
				result <- runLeaderElection(ctx, electionTestConfig(lock), func(ctx context.Context) error {
					close(started)
					<-ctx.Done()
					close(canceled)
					<-drain
					return nil
				})
			}()
			waitElectionSignal(t, started)
			if loss {
				lock.fail.Store(true)
			} else {
				cancel()
			}
			waitElectionSignal(t, canceled)
			if !loss {
				close(drain)
			}
			err := waitElectionResult(t, result)
			if loss {
				close(drain)
				if err == nil || !strings.Contains(err.Error(), "lost leadership") {
					t.Fatalf("丢锁结果不正确: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			text := output.String()
			for _, field := range []string{`lease="test-lease"`, `identity="test-controller"`, "Leader election stopped"} {
				if !strings.Contains(text, field) {
					t.Fatalf("选主日志缺少 %s: %s", field, text)
				}
			}
			if loss {
				if !strings.Contains(text, "Lost leadership;") || !strings.Contains(text, "electionCanceled=false") {
					t.Fatalf("缺少异常丢锁日志: %s", text)
				}
			} else if !strings.Contains(text, "draining controllers while keeping the Lease") || !strings.Contains(text, "without releasing the Lease") || strings.Contains(text, "Lost leadership") {
				t.Fatalf("正常退出不应报告异常丢锁: %s", text)
			}
		})
	}
}

func TestLeaderElectionRenewsDuringDrain(t *testing.T) {
	lock := &electionTestLock{}
	started := make(chan struct{})
	canceled := make(chan struct{})
	drain := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- runLeaderElection(ctx, electionTestConfig(lock), func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			close(canceled)
			<-drain
			return nil
		})
	}()
	waitElectionSignal(t, started)
	cancel()
	waitElectionSignal(t, canceled)
	before := lock.updates.Load()
	err := wait.PollUntilContextTimeout(context.Background(), 5*time.Millisecond, time.Second, true, func(context.Context) (bool, error) {
		return lock.updates.Load() > before, nil
	})
	if err != nil {
		t.Fatal("Lease renewal stopped before controller drain completed")
	}
	select {
	case err := <-result:
		t.Fatalf("returned before drain: %v", err)
	default:
	}
	close(drain)
	if err := waitElectionResult(t, result); err != nil {
		t.Fatal(err)
	}
	lock.mu.Lock()
	defer lock.mu.Unlock()
	if lock.record.HolderIdentity != lock.Identity() {
		t.Fatal("Lease was released before its normal expiry")
	}
}

func TestLeaderElectionPropagatesControllerFailure(t *testing.T) {
	failure := errors.New("controller failed")
	result := make(chan error, 1)
	go func() {
		result <- runLeaderElection(context.Background(), electionTestConfig(&electionTestLock{}), func(context.Context) error {
			return failure
		})
	}()
	if err := waitElectionResult(t, result); !errors.Is(err, failure) {
		t.Fatalf("got %v, want controller failure", err)
	}
}

func TestLeaderElectionLossDoesNotWaitForDrain(t *testing.T) {
	lock := &electionTestLock{}
	started := make(chan struct{})
	canceled := make(chan struct{})
	drain := make(chan struct{})
	drained := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- runLeaderElection(context.Background(), electionTestConfig(lock), func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			close(canceled)
			<-drain
			close(drained)
			return nil
		})
	}()
	waitElectionSignal(t, started)
	lock.fail.Store(true)
	waitElectionSignal(t, canceled)
	if err := waitElectionResult(t, result); err == nil || !strings.Contains(err.Error(), "lost leadership") {
		t.Fatalf("expected leadership failure, got %v", err)
	}
	close(drain)
	waitElectionSignal(t, drained)
}

func TestCanceledElectionDoesNotStartControllers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var started atomic.Bool
	err := runLeaderElection(ctx, electionTestConfig(&electionTestLock{}), func(context.Context) error {
		started.Store(true)
		return nil
	})
	if err != nil || started.Load() {
		t.Fatalf("canceled election: started=%v error=%v", started.Load(), err)
	}
}
