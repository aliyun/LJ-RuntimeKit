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
	"testing"
	"time"

	"github.com/container-migration/controller/api/v1beta1"
	"github.com/container-migration/controller/pkg/util"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestSetWorkerBlockedConditionOnce_NoWriteStorm is the most critical
// anti-write-storm guarantee of this feature: repeated calls in the same
// blocked state (same message) return true (emit event) only the first time,
// and LastTransitionTime stops changing, avoiding one Status().Update() per
// reconcile round hammering etcd.
func TestSetWorkerBlockedConditionOnce_NoWriteStorm(t *testing.T) {
	status := &v1beta1.ContainerStateMigrationJobRequestStatus{}
	const msg = "node=10.0.0.1 blockedBy=ns/a"

	if !setWorkerBlockedConditionOnce(status, msg) {
		t.Fatal("first call should report a transition (send event)")
	}
	first := util.GetCondition(v1beta1.JobConditionWorkerBlocked, status.Conditions)
	if first == nil || first.Status != metav1.ConditionTrue {
		t.Fatalf("condition should be True, got %+v", first)
	}
	firstTime := first.LastTransitionTime

	time.Sleep(2 * time.Millisecond)
	if setWorkerBlockedConditionOnce(status, msg) {
		t.Error("same message must NOT report a transition (no repeated event)")
	}
	again := util.GetCondition(v1beta1.JobConditionWorkerBlocked, status.Conditions)
	if !again.LastTransitionTime.Equal(&firstTime) {
		t.Error("LastTransitionTime must stay stable for identical blocked state (avoids etcd write storm)")
	}

	// A change of the blocked object (message changed) counts as a new transition and re-emits the event.
	if !setWorkerBlockedConditionOnce(status, "node=10.0.0.1 blockedBy=ns/b") {
		t.Error("changed message should report a transition")
	}
}

func TestClearWorkerBlockedCondition(t *testing.T) {
	status := &v1beta1.ContainerStateMigrationJobRequestStatus{}

	// Clearing when absent should be a no-op (no new condition added).
	clearWorkerBlockedCondition(status)
	if len(status.Conditions) != 0 {
		t.Fatalf("clearing absent condition must be a no-op, got %v", status.Conditions)
	}

	setWorkerBlockedConditionOnce(status, "node=n blockedBy=ns/a")
	clearWorkerBlockedCondition(status)
	cond := util.GetCondition(v1beta1.JobConditionWorkerBlocked, status.Conditions)
	if cond == nil || cond.Status != metav1.ConditionFalse {
		t.Fatalf("condition should be flipped to False, got %+v", cond)
	}

	// Clearing again when already False should be a no-op (timestamp unchanged).
	falseTime := cond.LastTransitionTime
	time.Sleep(2 * time.Millisecond)
	clearWorkerBlockedCondition(status)
	cond = util.GetCondition(v1beta1.JobConditionWorkerBlocked, status.Conditions)
	if !cond.LastTransitionTime.Equal(&falseTime) {
		t.Error("clearing an already-False condition must not bump LastTransitionTime")
	}
}
