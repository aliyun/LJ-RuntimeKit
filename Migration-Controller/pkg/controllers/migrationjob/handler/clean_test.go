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
	"testing"

	"github.com/container-migration/controller/api/v1beta1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// cleanedJob builds a "terminal + already cleaned" CR. CleanHandler returns at
// these two pre-checks without touching sharedContext, so it is testable with
// zero dependencies.
func cleanedJob(phase v1beta1.JobPhase, cleaned bool) *v1beta1.ContainerStateMigrationJobRequest {
	job := &v1beta1.ContainerStateMigrationJobRequest{
		Status: v1beta1.ContainerStateMigrationJobRequestStatus{Phase: phase},
	}
	if cleaned {
		job.Status.Conditions = []metav1.Condition{{
			Type:               v1beta1.JobConditionCleaned,
			Status:             metav1.ConditionTrue,
			Reason:             "Cleaned",
			LastTransitionTime: metav1.Now(),
		}}
	}
	return job
}

// TestCleanHandler_AlreadyCleanedStopsRequeue is the core assertion of this change.
//
// When this branch returned Skip, Result.ToCtrlResult() fell into the fallback
// branch yielding RequeueAfter: 1s, and CleanHandler is the chain tail, so
// every cleaned terminal CR would re-run all 12 handlers every second forever.
// Measured: 20 terminal CRs push the real migration history out of the log
// buffer.
func TestCleanHandler_AlreadyCleanedStopsRequeue(t *testing.T) {
	h := &CleanHandler{}

	for _, phase := range []v1beta1.JobPhase{
		v1beta1.JobPhaseSucceeded,
		v1beta1.JobPhasePartiallySucceeded,
		v1beta1.JobPhaseFailed,
		v1beta1.JobPhaseTerminated,
	} {
		t.Run(string(phase), func(t *testing.T) {
			res := h.Handle(context.Background(), cleanedJob(phase, true))

			if !res.IsDone() {
				t.Fatalf("expected Done, got done=%v skip=%v", res.IsDone(), res.IsSkip())
			}
			ctrlRes, err := res.ToCtrlResult()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ctrlRes.RequeueAfter != 0 || ctrlRes.Requeue {
				t.Fatalf("terminal and cleaned still requeues: %+v", ctrlRes)
			}
		})
	}
}

// TestCleanHandler_NonTerminalKeepsRequeue is the control group of the above, preventing over-fixing.
//
// In non-terminal phases the requeue of this round relies on Skip falling into
// the fallback branch to schedule the next check - changing Skip's semantics
// too would stop in-flight tasks from advancing.
func TestCleanHandler_NonTerminalKeepsRequeue(t *testing.T) {
	h := &CleanHandler{}

	res := h.Handle(context.Background(), cleanedJob(v1beta1.JobPhaseRestoring, false))

	if !res.IsSkip() {
		t.Fatalf("non-terminal expects Skip, got done=%v skip=%v", res.IsDone(), res.IsSkip())
	}
	ctrlRes, err := res.ToCtrlResult()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ctrlRes.RequeueAfter == 0 {
		t.Fatal("non-terminal no longer requeues, in-flight tasks would stop advancing")
	}
}

// TestCleanHandler_TerminalButNotCleanedProceeds confirms "terminal but not
// cleaned" is not short-circuited by this change - that round must really go
// down and run the cleanup.
func TestCleanHandler_TerminalButNotCleanedProceeds(t *testing.T) {
	job := cleanedJob(v1beta1.JobPhaseSucceeded, false)

	// RestoreStatus / CheckpointStatus are both nil, so the cleanup itself has
	// nothing to do and never touches sharedContext, but it runs to the end and
	// writes the Cleaned condition.
	res := (&CleanHandler{}).Handle(context.Background(), job)

	if !res.IsDone() {
		t.Fatalf("expected Done, got done=%v skip=%v", res.IsDone(), res.IsSkip())
	}
	found := false
	for _, c := range job.Status.Conditions {
		if c.Type == v1beta1.JobConditionCleaned && c.Status == metav1.ConditionTrue {
			found = true
		}
	}
	if !found {
		t.Fatal("this round should write the Cleaned condition, otherwise the next round repeats the cleanup")
	}
}
