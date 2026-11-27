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

package controllers

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// TestGetOrCreateWorkerClient_ConcurrentAcrossNodes covers cross-node concurrent access to workerClients.
//
// Regressing a real defect: this map was only protected by the per-IP-keyed
// workerPodLock, and a keyed lock can only serialize accesses of "the same
// IP" - two goroutines operating different IPs hold different key locks yet
// concurrently write the same Go map, triggering the runtime "concurrent map
// read and map write". That is a fatal error that recover() cannot catch and
// terminates the whole controller process.
//
// The trigger is not just concurrent reconciles: the transient error branch
// of restore_xpu.go calls RemoveWorkerClientWithCallback inside async
// goroutines that outlive the current reconcile; while it operates node B, a
// reconcile may be dialing node A. So this test deliberately makes the
// "connect" and "remove" operation kinds concurrent across IPs.
//
// Meaningful only when run with -race.
func TestGetOrCreateWorkerClient_ConcurrentAcrossNodes(t *testing.T) {
	fc := newFakeClient()
	putWorkerConfigMap(fc, "50051")
	sc := newTestContext(t, fc)
	ctx := context.Background()

	// Prewarm the config cache so the concurrent phase only stresses the workerClients structure.
	if _, err := sc.getWorkerConfig(ctx, true); err != nil {
		t.Fatalf("warm up worker config: %v", err)
	}

	const (
		nodes  = 8
		rounds = 25
	)

	var wg sync.WaitGroup
	for i := 0; i < nodes; i++ {
		ip := fmt.Sprintf("10.0.0.%d", i+1)

		wg.Add(2)
		go func() {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				if _, err := sc.GetOrCreateWorkerClient(ctx, ip); err != nil {
					t.Errorf("GetOrCreateWorkerClient(%s): %v", ip, err)
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				if err := sc.RemoveWorkerClientWithCallback(ip, func() error { return nil }); err != nil {
					t.Errorf("RemoveWorkerClientWithCallback(%s): %v", ip, err)
					return
				}
			}
		}()
	}
	wg.Wait()

	// Cleanup: close the connections the concurrent phase may have left behind; do not leak gRPC clients to later cases.
	for i := 0; i < nodes; i++ {
		_ = sc.RemoveWorkerClientWithCallback(fmt.Sprintf("10.0.0.%d", i+1), func() error { return nil })
	}
	if len(sc.workerClients) != 0 {
		t.Fatalf("expected an empty client cache after cleanup, got %d entries", len(sc.workerClients))
	}
}

// The same IP must reuse the same connection and different IPs must each build their own - the locking refactor must not break the cache semantics.
func TestGetOrCreateWorkerClient_CachesPerIP(t *testing.T) {
	fc := newFakeClient()
	putWorkerConfigMap(fc, "50051")
	sc := newTestContext(t, fc)
	ctx := context.Background()

	first, err := sc.GetOrCreateWorkerClient(ctx, "10.0.0.1")
	if err != nil {
		t.Fatalf("first dial: %v", err)
	}
	again, err := sc.GetOrCreateWorkerClient(ctx, "10.0.0.1")
	if err != nil {
		t.Fatalf("second dial: %v", err)
	}
	if first != again {
		t.Fatal("same IP should reuse the cached connection")
	}

	other, err := sc.GetOrCreateWorkerClient(ctx, "10.0.0.2")
	if err != nil {
		t.Fatalf("dial other node: %v", err)
	}
	if other == first {
		t.Fatal("different IPs must not share a connection")
	}
	if len(sc.workerClients) != 2 {
		t.Fatalf("expected 2 cached clients, got %d", len(sc.workerClients))
	}
}

// When the port changed (grpcPort changed in the ConfigMap), the old
// connection must be dropped and rebuilt: gRPC bakes the port into the dial
// address; updating only the cached port value does not make an established
// connection switch to the new port.
func TestGetOrCreateWorkerClient_RedialsOnPortChange(t *testing.T) {
	fc := newFakeClient()
	putWorkerConfigMap(fc, "50051")
	sc := newTestContext(t, fc)
	ctx := context.Background()

	const ip = "10.0.0.1"
	old, err := sc.GetOrCreateWorkerClient(ctx, ip)
	if err != nil {
		t.Fatalf("initial dial: %v", err)
	}

	putWorkerConfigMap(fc, "50052")
	if err := sc.SyncConfigMap(ctx); err != nil {
		t.Fatalf("sync config map: %v", err)
	}

	fresh, err := sc.GetOrCreateWorkerClient(ctx, ip)
	if err != nil {
		t.Fatalf("redial: %v", err)
	}
	if fresh == old {
		t.Fatal("port change must force a new connection")
	}

	entry, ok := sc.lookupWorkerClient(ip)
	if !ok {
		t.Fatal("client cache lost the entry after redial")
	}
	if entry.port != 50052 {
		t.Fatalf("cached port not updated: got %d, want 50052", entry.port)
	}
	if len(sc.workerClients) != 1 {
		t.Fatalf("redial leaked a cache entry: got %d, want 1", len(sc.workerClients))
	}
}

// Removing a non-existent IP must be a harmless no-op, and the callback still runs.
func TestRemoveWorkerClient_AbsentIPIsNoOp(t *testing.T) {
	sc := newTestContext(t, newFakeClient())

	called := false
	if err := sc.RemoveWorkerClientWithCallback("10.0.0.254", func() error {
		called = true
		return nil
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("callback must run even when no client was cached")
	}
}

// takeWorkerClient must "take" rather than "read": the second call gets nothing and Close happens only once.
func TestTakeWorkerClient_RemovesEntry(t *testing.T) {
	sc := newTestContext(t, newFakeClient())
	sc.storeWorkerClient("10.0.0.1", workerClientEntry{port: 50051})

	if _, ok := sc.takeWorkerClient("10.0.0.1"); !ok {
		t.Fatal("first take should find the entry")
	}
	if _, ok := sc.takeWorkerClient("10.0.0.1"); ok {
		t.Fatal("second take should find nothing")
	}
	if len(sc.workerClients) != 0 {
		t.Fatalf("entry not deleted, cache still holds %d", len(sc.workerClients))
	}
}
