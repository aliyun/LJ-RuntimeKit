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

package util

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestAddOrUpdateCondition_StatusUnchangedKeepsTimestamp is the gate test of
// the self-feedback loop fix: when the same Status is written repeatedly,
// LastTransitionTime must stay unchanged, even though a new Now() is passed
// every time. If someone reverts the implementation to unconditional
// overwrite (conditions[i] = *condition), this case fails immediately.
// If someone reverts the implementation to unconditional overwrite (conditions[i] = *condition), this case fails immediately.
func TestAddOrUpdateCondition_StatusUnchangedKeepsTimestamp(t *testing.T) {
	t0 := metav1.NewTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	conds := []metav1.Condition{{
		Type:               "Restored",
		Status:             metav1.ConditionTrue,
		LastTransitionTime: t0,
		Reason:             "SnapshotPrepared",
		Message:            "Checkpoint data extracted",
	}}

	// Simulate later rounds: Status unchanged but the caller passes a new timestamp.
	later := metav1.NewTime(t0.Add(5 * time.Second))
	for i := 0; i < 5; i++ {
		conds = AddOrUpdateCondition(&metav1.Condition{
			Type:               "Restored",
			Status:             metav1.ConditionTrue,
			LastTransitionTime: later,
			Reason:             "SnapshotPrepared",
			Message:            "Checkpoint data extracted",
		}, conds)
	}

	if len(conds) != 1 {
		t.Fatalf("should not add a condition, got %d", len(conds))
	}
	if !conds[0].LastTransitionTime.Equal(&t0) {
		t.Fatalf("LastTransitionTime should stay %v when Status is unchanged, got %v", t0, conds[0].LastTransitionTime)
	}
}

// TestAddOrUpdateCondition_StatusChangedUpdatesTimestamp confirms a real status transition still refreshes the timestamp.
func TestAddOrUpdateCondition_StatusChangedUpdatesTimestamp(t *testing.T) {
	t0 := metav1.NewTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	conds := []metav1.Condition{{
		Type:               "Restored",
		Status:             metav1.ConditionFalse,
		LastTransitionTime: t0,
		Reason:             "Pending",
	}}

	t1 := metav1.NewTime(t0.Add(3 * time.Second))
	conds = AddOrUpdateCondition(&metav1.Condition{
		Type:               "Restored",
		Status:             metav1.ConditionTrue, // False -> True, a real transition
		LastTransitionTime: t1,
		Reason:             "SnapshotPrepared",
	}, conds)

	if !conds[0].LastTransitionTime.Equal(&t1) {
		t.Fatalf("LastTransitionTime should update to %v on a Status transition, got %v", t1, conds[0].LastTransitionTime)
	}
	if conds[0].Status != metav1.ConditionTrue {
		t.Fatalf("Status should update to True, got %v", conds[0].Status)
	}
}

// TestAddOrUpdateCondition_ReasonMessageStillUpdated confirms that when
// Status is unchanged but Reason/Message change, these fields are still
// written back (they carry real information), just without touching
// LastTransitionTime.
func TestAddOrUpdateCondition_ReasonMessageStillUpdated(t *testing.T) {
	t0 := metav1.NewTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	conds := []metav1.Condition{{
		Type:               "Restored",
		Status:             metav1.ConditionTrue,
		LastTransitionTime: t0,
		Reason:             "Old",
		Message:            "old message",
	}}

	conds = AddOrUpdateCondition(&metav1.Condition{
		Type:               "Restored",
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.NewTime(t0.Add(time.Minute)),
		Reason:             "New",
		Message:            "new message",
	}, conds)

	if conds[0].Reason != "New" || conds[0].Message != "new message" {
		t.Fatalf("Reason/Message should update, got reason=%q message=%q", conds[0].Reason, conds[0].Message)
	}
	if !conds[0].LastTransitionTime.Equal(&t0) {
		t.Fatalf("LastTransitionTime should stay %v when Status is unchanged, got %v", t0, conds[0].LastTransitionTime)
	}
}

// TestAddOrUpdateCondition_NewConditionAppended confirms a new condition is appended normally and a missing timestamp is filled.
func TestAddOrUpdateCondition_NewConditionAppended(t *testing.T) {
	conds := AddOrUpdateCondition(&metav1.Condition{
		Type:   "Restored",
		Status: metav1.ConditionTrue,
		Reason: "SnapshotPrepared",
	}, nil)

	if len(conds) != 1 {
		t.Fatalf("should append 1 condition, got %d", len(conds))
	}
	if conds[0].LastTransitionTime.IsZero() {
		t.Fatal("a new condition missing its timestamp should be filled with the current time, got zero value")
	}
}
