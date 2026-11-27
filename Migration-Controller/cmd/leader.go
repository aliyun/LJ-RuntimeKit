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
	"context"
	"fmt"
	"sync"

	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/klog/v2"
)

func runLeaderElection(ctx context.Context, config leaderelection.LeaderElectionConfig, run func(context.Context) error) error {
	log := klog.FromContext(ctx).WithName("leader-election")
	if config.Lock != nil {
		log = log.WithValues("lease", config.Lock.Describe(), "identity", config.Lock.Identity())
	}
	// Keep renewing during graceful drain; a lost lease must not be released early.
	electionCtx, cancelElection := context.WithCancel(context.Background())
	defer cancelElection()
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	var mu sync.Mutex
	var started, stopping bool
	runDone := make(chan error, 1)
	electionDone := make(chan struct{})
	config.ReleaseOnCancel = false
	config.Callbacks.OnStartedLeading = func(leaderCtx context.Context) {
		mu.Lock()
		if stopping || ctx.Err() != nil || leaderCtx.Err() != nil {
			mu.Unlock()
			log.Info("Leadership acquired during shutdown; controllers will not be started")
			return
		}
		started = true
		mu.Unlock()
		stop := context.AfterFunc(leaderCtx, cancelRun)
		defer stop()
		runDone <- run(runCtx)
	}
	config.Callbacks.OnStoppedLeading = func() {
		cancelRun()
		mu.Lock()
		wasStarted, shuttingDown := started, stopping
		mu.Unlock()
		log.Info("Leader election stopped", "controllersStarted", wasStarted, "shuttingDown", shuttingDown,
			"shutdownRequested", ctx.Err() != nil, "electionCanceled", electionCtx.Err() != nil)
	}
	elector, err := leaderelection.NewLeaderElector(config)
	if err != nil {
		return err
	}
	go func() {
		elector.Run(electionCtx)
		close(electionDone)
	}()
	stopStarting := func() bool {
		mu.Lock()
		defer mu.Unlock()
		stopping = true
		return started
	}

	select {
	case err = <-runDone:
		stopStarting()
		cancelRun()
		cancelElection()
		log.Info("Controllers exited; stopping leader election", "error", err, "shutdownRequested", ctx.Err() != nil)
		<-electionDone
		if err == nil && ctx.Err() == nil {
			return fmt.Errorf("controller manager stopped before shutdown or lost leadership")
		}
		return err
	case <-ctx.Done():
		wasStarted := stopStarting()
		cancelRun()
		log.Info("Shutdown requested; draining controllers while keeping the Lease", "controllersStarted", wasStarted)
		if wasStarted {
			select {
			case err = <-runDone:
				log.Info("Controllers drained; stopping leader election without releasing the Lease", "error", err)
			case <-electionDone:
				log.Error(nil, "Lost leadership while draining controllers")
				return fmt.Errorf("lost leadership while draining controllers")
			}
		}
		cancelElection()
		<-electionDone
		return err
	case <-electionDone:
		stopStarting()
		cancelRun()
		log.Error(nil, "Lost leadership; process will exit so another replica can take over")
		return fmt.Errorf("lost leadership")
	}
}
