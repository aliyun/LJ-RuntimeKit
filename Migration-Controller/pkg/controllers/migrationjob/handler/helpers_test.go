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
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/container-migration/controller/api/v1beta1"
)

// The core purpose of concurrency: one slow node must not drag down the
// others. Serially the total cost is a "sum"; concurrent it should be a
// "max".
func TestForEachConcurrently_SlowItemDoesNotDelayOthers(t *testing.T) {
	const (
		items = 6
		delay = 200 * time.Millisecond
	)

	work := make([]int, items)
	for i := range work {
		work[i] = i
	}

	start := time.Now()
	forEachConcurrently(context.Background(), work, func(_ int, _ int) {
		time.Sleep(delay)
	})
	elapsed := time.Since(start)

	// Serial would be 6x200ms=1.2s; concurrent should approach a single 200ms. Generous margin to avoid CI flake false alarms.
	if elapsed >= time.Duration(items)*delay {
		t.Fatalf("work was executed serially: elapsed=%v, serial would be %v",
			elapsed, time.Duration(items)*delay)
	}
	if elapsed < delay {
		t.Fatalf("elapsed=%v is shorter than a single unit of work (%v), test is not measuring anything",
			elapsed, delay)
	}
}

// Each callback must get the element strictly matching its own index - each
// goroutine writes its non-overlapping result slot by this index; a mismatch
// would cross the data.
func TestForEachConcurrently_IndexMatchesItem(t *testing.T) {
	work := []string{"a", "b", "c", "d", "e"}
	got := make([]string, len(work))

	forEachConcurrently(context.Background(), work, func(i int, item string) {
		got[i] = item
	})

	for i := range work {
		if got[i] != work[i] {
			t.Fatalf("index/item mismatch at %d: got %q, want %q", i, got[i], work[i])
		}
	}
}

// Every element must be executed exactly once.
func TestForEachConcurrently_RunsEveryItemOnce(t *testing.T) {
	for _, n := range []int{0, 1, 2, 17} {
		work := make([]int, n)
		var calls int64

		forEachConcurrently(context.Background(), work, func(_ int, _ int) {
			atomic.AddInt64(&calls, 1)
		})

		if int(calls) != n {
			t.Fatalf("n=%d: fn called %d times, want %d", n, calls, n)
		}
	}
}

// A single element takes the fast path without spawning a goroutine, but the semantics must be identical.
func TestForEachConcurrently_SingleItemRunsInline(t *testing.T) {
	var ran bool
	forEachConcurrently(context.Background(), []int{42}, func(i int, item int) {
		if i != 0 || item != 42 {
			t.Errorf("unexpected callback args: i=%d item=%d", i, item)
		}
		ran = true
	})
	if !ran {
		t.Fatal("single item was not processed")
	}
}

// A panic on one node must not drag down other nodes, nor kill the whole process.
//
// This project does not enable controller-runtime RecoverPanic, which
// re-panics after recovering; and a panic in a goroutine never reaches that
// layer at all and directly terminates the process - that would completely
// break the premise of "one node problem does not affect other nodes". So
// runGuarded must catch it.
func TestForEachConcurrently_PanicIsIsolated(t *testing.T) {
	work := []int{0, 1, 2, 3, 4}
	var completed int64

	forEachConcurrently(context.Background(), work, func(_ int, item int) {
		if item == 2 {
			panic("simulated worker failure")
		}
		atomic.AddInt64(&completed, 1)
	})

	if completed != int64(len(work)-1) {
		t.Fatalf("panic in one item affected others: completed=%d, want %d",
			completed, len(work)-1)
	}
}

// Concurrent writes to non-overlapping slots must be clean (meaningful only with -race).
func TestForEachConcurrently_NoRaceOnDistinctSlots(t *testing.T) {
	const n = 32
	work := make([]int, n)
	for i := range work {
		work[i] = i
	}

	results := make([][]string, n)
	forEachConcurrently(context.Background(), work, func(i int, item int) {
		results[i] = []string{"node", string(rune('a' + item%26))}
	})

	for i := range results {
		if len(results[i]) != 2 {
			t.Fatalf("slot %d not written: %v", i, results[i])
		}
	}
}

// runGuarded must return normally after catching a panic, not rethrow.
func TestRunGuarded_RecoversAndReturns(t *testing.T) {
	returned := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("runGuarded let the panic escape: %v", r)
			}
		}()
		runGuarded(context.Background(), func() { panic("boom") })
		returned = true
	}()
	if !returned {
		t.Fatal("runGuarded did not return normally after recovering")
	}
}

// Node dedup: keep first occurrence order, skip empty IPs, keep only one per node for multiple replicas.
//
// Dedup must happen before the concurrency - once the original shared cleaned map goes into the concurrent loop it is a race point.
func TestUniqueDestNodeIPs(t *testing.T) {
	dests := []v1beta1.JobDestinationStatus{
		{NodeIP: "10.0.0.2"},
		{NodeIP: ""},
		{NodeIP: "10.0.0.1"},
		{NodeIP: "10.0.0.2"},
		{NodeIP: "10.0.0.1"},
		{NodeIP: "10.0.0.3"},
		{NodeIP: ""},
	}

	got := uniqueDestNodeIPs(dests)
	want := []string{"10.0.0.2", "10.0.0.1", "10.0.0.3"}

	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order not preserved: got %v, want %v", got, want)
		}
	}
}

func TestUniqueDestNodeIPs_EmptyAndAllBlank(t *testing.T) {
	if got := uniqueDestNodeIPs(nil); len(got) != 0 {
		t.Fatalf("nil input should yield no node, got %v", got)
	}
	blank := []v1beta1.JobDestinationStatus{{NodeIP: ""}, {NodeIP: ""}}
	if got := uniqueDestNodeIPs(blank); len(got) != 0 {
		t.Fatalf("blank IPs should be skipped, got %v", got)
	}
}

// The dedup result feeds the concurrent cleanup and must guarantee each node is cleaned exactly once.
func TestUniqueDestNodeIPs_CleanupHappensOncePerNode(t *testing.T) {
	dests := []v1beta1.JobDestinationStatus{
		{NodeIP: "10.0.0.1"}, {NodeIP: "10.0.0.1"}, {NodeIP: "10.0.0.1"},
		{NodeIP: "10.0.0.2"}, {NodeIP: "10.0.0.2"},
	}

	var mu sync.Mutex
	cleaned := map[string]int{}
	forEachConcurrently(context.Background(), uniqueDestNodeIPs(dests), func(_ int, nodeIP string) {
		mu.Lock()
		defer mu.Unlock()
		cleaned[nodeIP]++
	})

	if len(cleaned) != 2 {
		t.Fatalf("expected 2 distinct nodes cleaned, got %v", cleaned)
	}
	nodes := make([]string, 0, len(cleaned))
	for ip, times := range cleaned {
		nodes = append(nodes, ip)
		if times != 1 {
			t.Fatalf("node %s cleaned %d times, want exactly 1", ip, times)
		}
	}
	sort.Strings(nodes)
	if nodes[0] != "10.0.0.1" || nodes[1] != "10.0.0.2" {
		t.Fatalf("unexpected nodes cleaned: %v", nodes)
	}
}
