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

package preheatjob

import (
	"testing"

	"github.com/container-migration/controller/api/v1beta1"

	"k8s.io/apimachinery/pkg/types"
)

func newLogTestReconciler() *PreheatJobReconciler {
	return &PreheatJobReconciler{
		lastLoggedPhase: make(map[types.NamespacedName]v1beta1.PreheatJobPhase),
	}
}

// TestObservePhaseForLog_SuppressesRepeats is the core of this change:
// ProbeController writes status every 10s of probing and repeatedly wakes this
// controller, but no log should be emitted when the phase is unchanged.
func TestObservePhaseForLog_SuppressesRepeats(t *testing.T) {
	r := newLogTestReconciler()
	key := types.NamespacedName{Namespace: "default", Name: "demo-gpu-0034"}

	if !r.observePhaseForLog(key, v1beta1.PreheatJobPhaseReady) {
		t.Fatal("first sight of the object should count as a change, got false")
	}
	// Simulate the steady-state repeat wake every 10s: the phase stays Ready.
	for i := 0; i < 5; i++ {
		if r.observePhaseForLog(key, v1beta1.PreheatJobPhaseReady) {
			t.Fatalf("repeat observation #%d of the same phase must not count as a change", i+1)
		}
	}
}

// TestObservePhaseForLog_TransitionLogsAgain confirms a real phase transition still logs.
func TestObservePhaseForLog_TransitionLogsAgain(t *testing.T) {
	r := newLogTestReconciler()
	key := types.NamespacedName{Namespace: "default", Name: "job-a"}

	seq := []struct {
		phase v1beta1.PreheatJobPhase
		want  bool
	}{
		{v1beta1.PreheatJobPhaseReady, true},  // first sight
		{v1beta1.PreheatJobPhaseReady, false}, // steady-state repeat
		{v1beta1.PreheatJobPhaseStale, true},  // Ready -> Stale, cache invalidated, worth logging
		{v1beta1.PreheatJobPhaseStale, false}, // steady-state repeat
		{v1beta1.PreheatJobPhaseReady, true},  // Stale -> Ready, cache recovered, worth logging
	}
	for i, s := range seq {
		if got := r.observePhaseForLog(key, s.phase); got != s.want {
			t.Fatalf("step %d phase=%s: got %v, want %v", i, s.phase, got, s.want)
		}
	}
}

// TestObservePhaseForLog_IsolatedPerObject confirms each object records on its
// own; one object's steady state must not suppress another object's first
// log.
func TestObservePhaseForLog_IsolatedPerObject(t *testing.T) {
	r := newLogTestReconciler()
	a := types.NamespacedName{Namespace: "default", Name: "job-a"}
	b := types.NamespacedName{Namespace: "default", Name: "job-b"}

	if !r.observePhaseForLog(a, v1beta1.PreheatJobPhaseReady) {
		t.Fatal("job-a first sight should be a change")
	}
	if !r.observePhaseForLog(b, v1beta1.PreheatJobPhaseReady) {
		t.Fatal("job-b first sight should be a change, unaffected by job-a's record")
	}
}

// TestForgetPhase_ReallowsLoggingAfterRecreate confirms the table is cleared
// on object deletion, so a same-name CR rebuild logs START again, while
// preventing unbounded map growth.
func TestForgetPhase_ReallowsLoggingAfterRecreate(t *testing.T) {
	r := newLogTestReconciler()
	key := types.NamespacedName{Namespace: "default", Name: "job-a"}

	r.observePhaseForLog(key, v1beta1.PreheatJobPhaseReady)
	if r.observePhaseForLog(key, v1beta1.PreheatJobPhaseReady) {
		t.Fatal("steady-state repeat before deletion must not count as a change")
	}

	r.forgetPhase(key)
	if len(r.lastLoggedPhase) != 0 {
		t.Fatalf("forgetPhase should clear the record, still %d entries left", len(r.lastLoggedPhase))
	}
	if !r.observePhaseForLog(key, v1beta1.PreheatJobPhaseReady) {
		t.Fatal("after recreate it should count as a change again and log START once")
	}
}
