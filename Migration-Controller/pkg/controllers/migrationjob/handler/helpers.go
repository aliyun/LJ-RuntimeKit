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

package handler

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"

	"github.com/container-migration/controller/api/v1beta1"
	"github.com/container-migration/controller/pkg/util"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

// forEachConcurrently runs fn(items[i]) concurrently and returns only after
// all return.
//
// Why concurrency is mandatory: in a multi-node task every node's worker RPC
// may take up to --rpc-timeout (default 2min) to fail. A serial traversal
// **accumulates** these waits - N bad nodes means N x 2min, and healthy nodes
// queued behind get processed late; the more hidden consequence is that
// RoundCheckHandler sits early in the pipeline and its timeout verdict only
// runs once at the start of each reconcile round, so while this round is
// blocked for tens of minutes the timeout is enforced by nobody. Concurrency
// turns the total cost from a "sum" into a "max".
//
// The caller must guarantee fn only writes its own data (e.g. each node's own
// dest pointer). When results need aggregating, use the passed index i to
// write into a pre-allocated result slice (different goroutines write
// different indices, no lock needed); do not write a shared map or accumulate
// into an external variable inside fn.
func forEachConcurrently[T any](ctx context.Context, items []T, fn func(i int, item T)) {
	// A single item is not worth a goroutine; it also keeps the call stack of single-node scenarios intact.
	if len(items) < 2 {
		for i := range items {
			runGuarded(ctx, func() { fn(i, items[i]) })
		}
		return
	}

	var wg sync.WaitGroup
	wg.Add(len(items))
	for i := range items {
		i, item := i, items[i]
		go func() {
			defer wg.Done()
			runGuarded(ctx, func() { fn(i, item) })
		}()
	}
	wg.Wait()
}

// runGuarded catches panics inside fn, logging only without rethrowing.
//
// This project does not enable controller-runtime's RecoverPanic (default
// nil), which re-panics after recovering, so any panic in a reconcile kills
// the process outright today. After concurrency this behavior is even more
// dangerous: a panic in a goroutine loses even that log line, and one node's
// anomaly would drag down all nodes' tasks - exactly against the isolation
// goal of concurrency. So it is neutralized here: the failed replica stays in
// its current phase and retries on the next requeue; other nodes are
// unaffected.
func runGuarded(ctx context.Context, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			log.FromContext(ctx).Error(fmt.Errorf("panic: %v", r),
				"recovered panic in concurrent per-node work", "stack", string(debug.Stack()))
		}
	}()

	fn()
}

// uniqueDestNodeIPs returns the deduplicated node IPs of replicas in first
// occurrence order, skipping empty IPs. Multiple replicas on the same node
// share one copy of data and one requestID, cleanup can run only once; the
// dedup must happen before the concurrency, otherwise the shared cleaned map
// becomes a race point.
func uniqueDestNodeIPs(dests []v1beta1.JobDestinationStatus) []string {
	seen := make(map[string]struct{}, len(dests))
	ips := make([]string, 0, len(dests))
	for i := range dests {
		ip := dests[i].NodeIP
		if ip == "" {
			continue
		}
		if _, ok := seen[ip]; ok {
			continue
		}
		seen[ip] = struct{}{}
		ips = append(ips, ip)
	}
	return ips
}

// isTransientError returns true when a gRPC error represents a transient
// connectivity issue that may be resolved by re-dialing the worker.
func isTransientError(err error) bool {
	if err == nil {
		return false
	}
	s, ok := grpcstatus.FromError(err)
	if !ok {
		return false
	}
	switch s.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted,
		codes.Aborted, codes.Canceled:
		return true
	}
	return false
}

// copyStringMap shallow-copies a map; nil input returns nil. The source pod's
// labels / annotations must all be modified on the copy, not pollute the
// original data in the CR status.
func copyStringMap(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// setWorkerBlockedConditionOnce writes the job-level WorkerBlocked=True
// condition, updating LastTransitionTime only when the state or wording
// changes, avoiding an etcd write storm from writing every round in the same
// blocked state. Returns true when a state transition happened this time
// (callers emit the event only once based on it).
func setWorkerBlockedConditionOnce(status *v1beta1.ContainerStateMigrationJobRequestStatus, message string) bool {
	existing := util.GetCondition(v1beta1.JobConditionWorkerBlocked, status.Conditions)
	if existing != nil && existing.Status == metav1.ConditionTrue && existing.Message == message {
		return false
	}
	status.Conditions = util.AddOrUpdateCondition(&metav1.Condition{
		Type:               v1beta1.JobConditionWorkerBlocked,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             "WaitingWorkerDrain",
		Message:            message,
	}, status.Conditions)
	return true
}

// clearWorkerBlockedCondition sets WorkerBlocked to False after the worker is
// ready (keeping history); untouched when it does not exist or is already
// False, avoiding pointless writes.
func clearWorkerBlockedCondition(status *v1beta1.ContainerStateMigrationJobRequestStatus) {
	existing := util.GetCondition(v1beta1.JobConditionWorkerBlocked, status.Conditions)
	if existing == nil || existing.Status == metav1.ConditionFalse {
		return
	}
	status.Conditions = util.AddOrUpdateCondition(&metav1.Condition{
		Type:               v1beta1.JobConditionWorkerBlocked,
		Status:             metav1.ConditionFalse,
		LastTransitionTime: metav1.Now(),
		Reason:             "WorkerReady",
		Message:            "Worker ready",
	}, status.Conditions)
}
