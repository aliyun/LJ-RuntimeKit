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

package activemonitoring

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	gpuv1alpha1 "github.com/lj-runtimekit/job-controller/pkg/apis/gpu/v1alpha1"
)

// TestCountPodsSchemeA verifies the scheme-a gate counting: Running and fresh
// Pending occupy a slot (gateLive), while a timed-out Pending is exempted.
func TestCountPodsSchemeA(t *testing.T) {
	now := time.Now()
	cr := testCR("default", "t1", gpuv1alpha1.ActiveMonitoringSpec{})
	running := testPod("default", "p-run", "t1", v1.PodRunning, now)
	freshPending := testPod("default", "p-fresh", "t1", v1.PodPending, now.Add(-1*time.Minute))
	oldPending := testPod("default", "p-old", "t1", v1.PodPending, now.Add(-10*time.Minute))

	c, cancel := newTestController(t, cr, running, freshPending, oldPending)
	defer cancel()

	gateLive := c.countPods(cr)
	if gateLive != 2 {
		t.Errorf("gateLive = %d, want 2 (Running + fresh Pending; timed-out exempted)", gateLive)
	}
}

func TestConcurrencyAllows(t *testing.T) {
	cr := testCR("default", "t1", gpuv1alpha1.ActiveMonitoringSpec{Concurrency: 2})
	if !concurrencyAllows(1, cr) {
		t.Error("under the cap should allow")
	}
	if concurrencyAllows(2, cr) {
		t.Error("gateLive>=concurrency should block")
	}
	unlimited := testCR("default", "t2", gpuv1alpha1.ActiveMonitoringSpec{}) // 0 = unlimited
	if !concurrencyAllows(100, unlimited) {
		t.Error("zero cap should be unlimited")
	}
}

func TestPendingTimedOut(t *testing.T) {
	fresh := mustUnstructured(t, testPod("default", "p", "t1", v1.PodPending, time.Now().Add(-1*time.Minute)))
	if pendingTimedOut(fresh) {
		t.Error("fresh pending should not be timed out")
	}
	old := mustUnstructured(t, testPod("default", "p", "t1", v1.PodPending, time.Now().Add(-10*time.Minute)))
	if !pendingTimedOut(old) {
		t.Error("old pending should be timed out")
	}
}

func TestRecountStatus(t *testing.T) {
	cr := testCR("default", "t1", gpuv1alpha1.ActiveMonitoringSpec{})
	cr.Status.Queue = []gpuv1alpha1.TaskStatus{
		{State: gpuv1alpha1.TaskStateCompleted},
		{State: gpuv1alpha1.TaskStateCompleted},
		{State: gpuv1alpha1.TaskStateRunning},
		{State: gpuv1alpha1.TaskStatePending},
		{State: gpuv1alpha1.TaskStatePreempted},
		{State: gpuv1alpha1.TaskStateFailed},
	}
	recountStatus(cr)
	if cr.Status.TotalTasks != 6 || cr.Status.CompletedTasks != 2 || cr.Status.RunningTasks != 1 ||
		cr.Status.PendingTasks != 1 || cr.Status.PreemptedTasks != 1 || cr.Status.FailedTasks != 1 {
		t.Fatalf("unexpected counts: %+v", cr.Status)
	}
}

func TestAdvanceTaskCycle(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name, state, interval string
		rounds, cycle, total  int
		endedAgo              time.Duration
		want                  bool
	}{
		{"interval has not elapsed since completion", gpuv1alpha1.TaskStateCompleted, "1h", 2, 1, 0, 59 * time.Minute, false},
		{"interval has just elapsed", gpuv1alpha1.TaskStateCompleted, "1h", 2, 1, 0, time.Hour, true},
		{"advance only once after prolonged downtime", gpuv1alpha1.TaskStateCompleted, "1h", 2, 1, 0, 24 * time.Hour, true},
		{"do not advance during an intermediate round", gpuv1alpha1.TaskStatePending, "1h", 1, 1, 0, 2 * time.Hour, false},
		{"do not advance before all rounds complete", gpuv1alpha1.TaskStateCompleted, "1h", 1, 1, 0, 2 * time.Hour, false},
		{"do not advance while running", gpuv1alpha1.TaskStateRunning, "1h", 2, 1, 0, 2 * time.Hour, false},
		{"do not advance while preempted", gpuv1alpha1.TaskStatePreempted, "1h", 1, 1, 0, 2 * time.Hour, false},
		{"advance from a failed terminal state", gpuv1alpha1.TaskStateFailed, "1h", 2, 1, 0, 2 * time.Hour, true},
		{"do not advance the cycle after an intermediate round fails even if the interval elapsed", gpuv1alpha1.TaskStateFailed, "1h", 1, 1, 0, 2 * time.Hour, false},
		{"do not advance the cycle after an intermediate round fails before the interval elapses", gpuv1alpha1.TaskStateFailed, "1h", 1, 1, 0, 59 * time.Minute, false},
		{"task reached the cycle limit", gpuv1alpha1.TaskStateCompleted, "1h", 2, 3, 3, 2 * time.Hour, false},
		{"task has not reached the cycle limit", gpuv1alpha1.TaskStateCompleted, "1h", 2, 2, 3, 2 * time.Hour, true},
		{"empty interval runs only one cycle", gpuv1alpha1.TaskStateCompleted, "", 2, 1, 3, 2 * time.Hour, false},
		{"zero interval runs only one cycle", gpuv1alpha1.TaskStateCompleted, "0s", 2, 1, 0, 2 * time.Hour, false},
		{"negative interval runs only one cycle", gpuv1alpha1.TaskStateCompleted, "-1h", 2, 1, 0, 2 * time.Hour, false},
		{"invalid interval prevents recurrence", gpuv1alpha1.TaskStateCompleted, "invalid", 2, 1, 0, 2 * time.Hour, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cr := testCR("default", "t1", gpuv1alpha1.ActiveMonitoringSpec{Schedule: gpuv1alpha1.SchedulePolicy{
				Interval: tc.interval, TotalCycles: tc.total, RoundsPerCycle: 2,
			}})
			task := &gpuv1alpha1.TaskStatus{ID: "a", NodeName: "n1", GPUMinor: 3,
				CurrentCycle: tc.cycle, State: tc.state, CompletedRounds: tc.rounds, Attempts: 3,
				StartTime:       &metav1.Time{Time: now.Add(-48 * time.Hour)},
				EndTime:         &metav1.Time{Time: now.Add(-tc.endedAgo)},
				LastPreemptTime: &metav1.Time{Time: now.Add(-48 * time.Hour)},
				Result:          &gpuv1alpha1.TaskResult{Passed: false, Errors: []string{"old"}},
			}
			before := task.DeepCopy()
			if got := advanceTaskCycle(cr, task, now); got != tc.want {
				t.Fatalf("advance = %v, want %v", got, tc.want)
			}
			if !tc.want {
				if !reflect.DeepEqual(before, task) {
					t.Fatalf("state must not change while waiting: %+v", task)
				}
				return
			}
			want := &gpuv1alpha1.TaskStatus{ID: "a", NodeName: "n1", GPUMinor: 3,
				CurrentCycle: tc.cycle + 1, State: gpuv1alpha1.TaskStatePending}
			if !reflect.DeepEqual(task, want) {
				t.Fatalf("incorrect cycle reset: %+v", task)
			}
		})
	}
}

func TestAdvanceTaskCycleMissingEndTime(t *testing.T) {
	now := time.Now()
	cr := testCR("default", "t1", gpuv1alpha1.ActiveMonitoringSpec{Schedule: gpuv1alpha1.SchedulePolicy{Interval: "1h"}})
	task := &gpuv1alpha1.TaskStatus{CurrentCycle: 1, State: gpuv1alpha1.TaskStateCompleted, CompletedRounds: 1}
	if advanceTaskCycle(cr, task, now) || task.EndTime == nil || !task.EndTime.Time.Equal(now) {
		t.Fatalf("missing endTime should be initialized to now before waiting: %+v", task)
	}
	if advanceTaskCycle(cr, task, now.Add(30*time.Minute)) || !task.EndTime.Time.Equal(now) {
		t.Fatal("repeated observations must not refresh the end time")
	}
	if !advanceTaskCycle(cr, task, now.Add(time.Hour)) {
		t.Fatal("advance when the interval elapses")
	}
}

func TestPhaseAndSettledUseEveryTask(t *testing.T) {
	cases := []struct {
		name, interval string
		total          int
		tasks          []gpuv1alpha1.TaskStatus
		phase          string
		settled        bool
	}{
		{"empty queue", "1h", 0, nil, gpuv1alpha1.PhasePending, false},
		{"new task", "1h", 0, []gpuv1alpha1.TaskStatus{{State: gpuv1alpha1.TaskStatePending}}, gpuv1alpha1.PhasePending, false},
		{"tasks in different cycles with one still running", "1h", 3, []gpuv1alpha1.TaskStatus{
			{State: gpuv1alpha1.TaskStateCompleted, CurrentCycle: 3, CompletedRounds: 1},
			{State: gpuv1alpha1.TaskStateRunning, CurrentCycle: 1}}, gpuv1alpha1.PhaseRunning, false},
		{"completed but waiting for the next cycle", "1h", 3, []gpuv1alpha1.TaskStatus{
			{State: gpuv1alpha1.TaskStateCompleted, CurrentCycle: 3, CompletedRounds: 1},
			{State: gpuv1alpha1.TaskStateCompleted, CurrentCycle: 2, CompletedRounds: 1}}, gpuv1alpha1.PhaseCompleted, false},
		{"all tasks reached the cycle limit", "1h", 3, []gpuv1alpha1.TaskStatus{
			{State: gpuv1alpha1.TaskStateCompleted, CurrentCycle: 3, CompletedRounds: 1},
			{State: gpuv1alpha1.TaskStateCompleted, CurrentCycle: 3, CompletedRounds: 1}}, gpuv1alpha1.PhaseCompleted, true},
		{"failed task can continue", "1h", 0, []gpuv1alpha1.TaskStatus{
			{State: gpuv1alpha1.TaskStateFailed, CurrentCycle: 1, CompletedRounds: 1}}, gpuv1alpha1.PhaseFailed, false},
		{"failed task reached the cycle limit", "1h", 1, []gpuv1alpha1.TaskStatus{
			{State: gpuv1alpha1.TaskStateFailed, CurrentCycle: 1, CompletedRounds: 1}}, gpuv1alpha1.PhaseFailed, true},
		{"cannot settle while another task is retrying", "1h", 1, []gpuv1alpha1.TaskStatus{
			{State: gpuv1alpha1.TaskStateFailed, CurrentCycle: 1, CompletedRounds: 1},
			{State: gpuv1alpha1.TaskStatePending, CurrentCycle: 1, Attempts: 1}}, gpuv1alpha1.PhaseRunning, false},
		{"empty interval stops even when the limit allows more cycles", "", 3, []gpuv1alpha1.TaskStatus{
			{State: gpuv1alpha1.TaskStateCompleted, CurrentCycle: 1, CompletedRounds: 1}}, gpuv1alpha1.PhaseCompleted, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cr := testCR("default", "t1", gpuv1alpha1.ActiveMonitoringSpec{Schedule: gpuv1alpha1.SchedulePolicy{Interval: tc.interval, TotalCycles: tc.total}})
			cr.Status.Queue = tc.tasks
			updatePhase(cr)
			if cr.Status.Phase != tc.phase || crSettled(cr) != tc.settled {
				t.Fatalf("phase=%s settled=%v, want %s/%v", cr.Status.Phase, crSettled(cr), tc.phase, tc.settled)
			}
		})
	}
}

func TestAppendTaskErrorCap(t *testing.T) {
	task := &gpuv1alpha1.TaskStatus{}
	total := maxTaskErrors + 10
	for i := 0; i < total; i++ {
		appendTaskError(task, fmt.Sprintf("e%d", i))
	}
	if len(task.Result.Errors) != maxTaskErrors {
		t.Fatalf("errors len = %d, want cap %d", len(task.Result.Errors), maxTaskErrors)
	}
	// Oldest entries dropped: the first retained should be e10.
	if task.Result.Errors[0] != fmt.Sprintf("e%d", total-maxTaskErrors) {
		t.Fatalf("expected oldest dropped, first = %q", task.Result.Errors[0])
	}
}

func TestMergeResult(t *testing.T) {
	task := &gpuv1alpha1.TaskStatus{CompletedRounds: 1, Result: &gpuv1alpha1.TaskResult{Passed: true, Duration: 5, Errors: []string{"old"}}}
	mergeResult(task, &gpuv1alpha1.TaskResult{Passed: false, Duration: 9, Errors: []string{"new"}})
	if task.Result.Passed != false || task.Result.Duration != 9 {
		t.Error("passed and duration should come from the last round")
	}
	if len(task.Result.Errors) != 2 || task.Result.Errors[0] != "old" || task.Result.Errors[1] != "new" {
		t.Errorf("errors should accumulate across rounds, got %v", task.Result.Errors)
	}
	task.CompletedRounds++
	mergeResult(task, &gpuv1alpha1.TaskResult{Passed: true, Duration: 12})
	if !task.Result.Passed || task.Result.Duration != 12 {
		t.Fatalf("PASS in the last round should override FAIL in earlier rounds: %+v", task.Result)
	}
	// A successful retry in the first round must not be marked as failed by an earlier attempt's error.
	retried := &gpuv1alpha1.TaskStatus{CompletedRounds: 1}
	appendTaskError(retried, "round 1 attempt 1: pod failed")
	mergeResult(retried, &gpuv1alpha1.TaskResult{Passed: true})
	if !retried.Result.Passed || len(retried.Result.Errors) != 1 {
		t.Fatalf("a successful retry should retain errors but report passed: %+v", retried.Result)
	}
	// Merging into a nil Result allocates it.
	nilTask := &gpuv1alpha1.TaskStatus{}
	mergeResult(nilTask, &gpuv1alpha1.TaskResult{Passed: true, Duration: 1})
	if nilTask.Result == nil || nilTask.Result.Duration != 1 {
		t.Fatalf("expected Result allocated: %+v", nilTask.Result)
	}
}

// Each valid detection result completes a round without retrying; passed uses the last round and errors retain history.
func TestDetectionCompletionUsesLastRoundResult(t *testing.T) {
	cases := []struct {
		name   string
		rounds []string
		passed bool
	}{
		{"single-round FAIL completes", []string{"FAIL"}, false},
		{"single-round ERROR completes", []string{"ERROR"}, false},
		{"all rounds pass", []string{"PASS", "PASS"}, true},
		{"PASS after FAIL overrides the verdict", []string{"FAIL", "PASS"}, true},
		{"PASS after ERROR overrides the verdict", []string{"ERROR", "PASS"}, true},
		{"FAIL after PASS", []string{"PASS", "FAIL"}, false},
		{"ERROR after PASS", []string{"PASS", "ERROR"}, false},
		{"all failed detection results still complete", []string{"FAIL", "ERROR", "FAIL"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cr := testCR("default", "last-round", gpuv1alpha1.ActiveMonitoringSpec{
				Schedule: gpuv1alpha1.SchedulePolicy{RoundsPerCycle: len(tc.rounds), TotalCycles: 1},
			})
			cr.Status.Queue = []gpuv1alpha1.TaskStatus{{CurrentCycle: 1, State: gpuv1alpha1.TaskStatePending}}
			task, c := &cr.Status.Queue[0], &Controller{}
			var wantErrors []string
			for round, verdict := range tc.rounds {
				recordTaskPod(task, "pod", time.Now())
				if task.Attempts != 1 {
					t.Fatalf("each new round should start at attempt one: %+v", task)
				}
				if verdict != detectionStatusPass {
					wantErrors = append(wantErrors, fmt.Sprintf("round %d attempt 1: detection %s", round+1, verdict))
				}
				pod := succeededPod(cr.Namespace, "pod", cr.Name, time.Now(), fmt.Sprintf(`{"status":%q}`, verdict))
				c.handleSucceeded(cr, task, mustUnstructured(t, pod), time.Now(), defaultMaxRetries)
				if task.CompletedRounds != round+1 || task.Attempts != 0 || task.PodName != "" ||
					task.Result == nil || task.Result.Passed != (verdict == detectionStatusPass) {
					t.Fatalf("a valid detection result should finish the round and overwrite the verdict: %+v", task)
				}
				if round+1 < len(tc.rounds) {
					updatePhase(cr)
					if task.State != gpuv1alpha1.TaskStatePending || cr.Status.Phase != gpuv1alpha1.PhaseRunning || crSettled(cr) {
						t.Fatalf("remaining rounds should continue without retrying the previous round: %+v", cr.Status)
					}
				}
			}
			updatePhase(cr)
			if task.Result.Passed != tc.passed || task.Result.Duration != 120 || !reflect.DeepEqual(task.Result.Errors, wantErrors) ||
				task.State != gpuv1alpha1.TaskStateCompleted || cr.Status.Phase != gpuv1alpha1.PhaseCompleted || !crSettled(cr) {
				t.Fatalf("execution completion must be independent of the detection verdict: %+v, result=%+v", cr.Status, task.Result)
			}
		})
	}
}

// Exhausting an earlier round's execution budget must not block later rounds; the last round determines state and verdict while error history is retained.
func TestFailedRoundThenDetectionResult(t *testing.T) {
	for _, verdict := range []string{detectionStatusPass, detectionStatusFail, detectionStatusError} {
		t.Run(verdict, func(t *testing.T) {
			cr := testCR("default", "round-recovery", gpuv1alpha1.ActiveMonitoringSpec{
				Schedule: gpuv1alpha1.SchedulePolicy{RoundsPerCycle: 2, TotalCycles: 1, Interval: "1h"},
			})
			cr.Status.Queue = []gpuv1alpha1.TaskStatus{{CurrentCycle: 1, State: gpuv1alpha1.TaskStatePending}}
			task, c := &cr.Status.Queue[0], &Controller{}
			var wantErrors []string
			for attempt := 1; attempt <= defaultMaxRetries; attempt++ {
				recordTaskPod(task, "pod", time.Now())
				c.handlePodFailed(cr, task, time.Now(), defaultMaxRetries, "pod failed")
				detail := fmt.Sprintf("round 1 attempt %d: pod failed", attempt)
				if attempt == defaultMaxRetries {
					detail += ", retry budget exhausted"
				}
				wantErrors = append(wantErrors, detail)
			}
			updatePhase(cr)
			if task.State != gpuv1alpha1.TaskStateFailed || task.CompletedRounds != 1 || task.Attempts != 0 ||
				cr.Status.Phase != gpuv1alpha1.PhaseRunning || crSettled(cr) {
				t.Fatalf("execution failure in the first round must not end the cycle: %+v", cr.Status)
			}
			gate := 0
			c.processTask(context.Background(), cr, task, &gate, time.Now())
			if task.State != gpuv1alpha1.TaskStatePending || task.CurrentCycle != 1 {
				t.Fatalf("the next round must not wait for the cycle interval or enter a new cycle: %+v", task)
			}
			recordTaskPod(task, "next-round", time.Now())
			pod := succeededPod(cr.Namespace, "next-round", cr.Name, time.Now(), fmt.Sprintf(`{"status":%q}`, verdict))
			c.handleSucceeded(cr, task, mustUnstructured(t, pod), time.Now(), defaultMaxRetries)
			if verdict != detectionStatusPass {
				wantErrors = append(wantErrors, "round 2 attempt 1: detection "+verdict)
			}
			updatePhase(cr)
			if task.State != gpuv1alpha1.TaskStateCompleted || task.CompletedRounds != 2 || task.Attempts != 0 ||
				task.Result.Passed != (verdict == detectionStatusPass) || !reflect.DeepEqual(task.Result.Errors, wantErrors) ||
				cr.Status.Phase != gpuv1alpha1.PhaseCompleted || !crSettled(cr) {
				t.Fatalf("normal detection completion in the last round should override earlier execution failures: %+v, result=%+v", cr.Status, task.Result)
			}
		})
	}
}

// TestAnnotateFailedRun locks the round/attempt locator contract for runs ending
// with passed=false: with no container-reported details it appends one generic
// entry; with details it rewrites them IN PLACE and appends nothing, so the later
// mergeResult records each exactly once (no duplicates, no unattributed entries).
func TestAnnotateFailedRun(t *testing.T) {
	// No details: one generic entry carrying round + attempt. CompletedRounds=1
	// means the round in flight is round 2.
	bare := &gpuv1alpha1.TaskStatus{CompletedRounds: 1, Attempts: 2}
	annotateFailedRun(bare, "detection FAIL", nil)
	if bare.Result == nil || len(bare.Result.Errors) != 1 {
		t.Fatalf("expected exactly 1 error, got %+v", bare.Result)
	}
	wantBare := "round 2 attempt 2: detection FAIL"
	if bare.Result.Errors[0] != wantBare {
		t.Fatalf("error = %q, want %q", bare.Result.Errors[0], wantBare)
	}

	// With details: prefixed in place, and NOTHING appended here -- finishRound's
	// mergeResult is the single writer.
	details := []string{"SDC detected CRC error", "HBM stress timeout"}
	detailed := &gpuv1alpha1.TaskStatus{CompletedRounds: 0, Attempts: 1}
	annotateFailedRun(detailed, "detection ERROR", details)
	if detailed.Result != nil {
		t.Fatalf("must not append when details are rewritten in place, got %+v", detailed.Result)
	}
	wantD0 := "round 1 attempt 1: detection ERROR, SDC detected CRC error"
	wantD1 := "round 1 attempt 1: detection ERROR, HBM stress timeout"
	if details[0] != wantD0 || details[1] != wantD1 {
		t.Fatalf("details not prefixed in place: %v", details)
	}

	// mergeResult then records each exactly once.
	mergeResult(detailed, &gpuv1alpha1.TaskResult{Passed: false, Duration: 120, Errors: details})
	if len(detailed.Result.Errors) != 2 {
		t.Fatalf("expected 2 errors (no duplicates), got %v", detailed.Result.Errors)
	}
	if detailed.Result.Errors[0] != wantD0 || detailed.Result.Errors[1] != wantD1 {
		t.Fatalf("merged errors = %v", detailed.Result.Errors)
	}
	if detailed.Result.Passed || detailed.Result.Duration != 120 {
		t.Fatalf("passed/duration not merged: %+v", detailed.Result)
	}
}
