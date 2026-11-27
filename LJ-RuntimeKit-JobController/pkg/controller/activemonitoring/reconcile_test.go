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
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	gpuv1alpha1 "github.com/lj-runtimekit/job-controller/pkg/apis/gpu/v1alpha1"
)

// getCRFromAPI reads the CR back from the (fake) API server, reflecting any
// updateStatus write made during reconcile.
func (c *Controller) getCRFromAPI(t *testing.T, ns, name string) *gpuv1alpha1.ActiveMonitoring {
	t.Helper()
	u, err := c.config.DynamicClient.Resource(activemonitoringGVR).Namespace(ns).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get CR from API: %v", err)
	}
	cr, err := toActiveMonitoring(u)
	if err != nil {
		t.Fatalf("convert CR: %v", err)
	}
	return cr
}

// TestReconcileAdvancesHeldCompletedToNextCycle is a regression test for the bug
// where the isTerminalPhase early-return blocked cycle advancement: a non-final
// cycle held at Completed must still roll into the next cycle once the interval
// elapses (Phase → Pending, tasks reset, counts recounted after the advance).
func TestReconcileAdvancesHeldCompletedToNextCycle(t *testing.T) {
	start := time.Now().Add(-2 * time.Hour)
	cr := testCR("default", "t1", gpuv1alpha1.ActiveMonitoringSpec{
		Schedule: gpuv1alpha1.SchedulePolicy{TotalCycles: 0, Interval: "1h", RoundsPerCycle: 1},
	})
	cr.Status.Phase = gpuv1alpha1.PhaseCompleted // held result of cycle 1
	cr.Status.CompletedTasks = 1
	cr.Status.TotalTasks = 1
	cr.Status.Queue = []gpuv1alpha1.TaskStatus{
		{ID: "n1-gpu-0", NodeName: "n1", GPUMinor: 0, CurrentCycle: 1, State: gpuv1alpha1.TaskStateCompleted, CompletedRounds: 1, EndTime: &metav1.Time{Time: start}},
	}

	c, cancel := newTestController(t, cr, gpuNode("n1", 1))
	defer cancel()

	if err := c.reconcile(context.Background(), "default/t1"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := c.getCRFromAPI(t, "default", "t1")
	if got.Status.Queue[0].CurrentCycle != 2 {
		t.Fatalf("currentCycle = %d, want 2 (interval elapsed → next cycle)", got.Status.Queue[0].CurrentCycle)
	}
	if got.Status.Phase != gpuv1alpha1.PhasePending {
		t.Fatalf("phase = %q, want Pending (new cycle restarts from Pending)", got.Status.Phase)
	}
	if got.Status.Queue[0].State != gpuv1alpha1.TaskStatePending || got.Status.Queue[0].CompletedRounds != 0 {
		t.Fatalf("task not reset for next cycle: %+v", got.Status.Queue[0])
	}
	// recount runs after advance: the freshly-reset cycle shows 1 pending task.
	if got.Status.PendingTasks != 1 || got.Status.CompletedTasks != 0 {
		t.Fatalf("counts = pending %d completed %d, want pending 1 completed 0", got.Status.PendingTasks, got.Status.CompletedTasks)
	}
}

// Completed tasks advance independently of other tasks that are running, waiting for an interval, or at the cycle limit.
func TestReconcileIndependentTaskCycles(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	cr := testCR("default", "t1", gpuv1alpha1.ActiveMonitoringSpec{
		Concurrency: 1,
		Schedule:    gpuv1alpha1.SchedulePolicy{Interval: "1h", TotalCycles: 3, RoundsPerCycle: 2},
	})
	cr.Status.Phase = gpuv1alpha1.PhaseRunning
	cr.Status.Queue = []gpuv1alpha1.TaskStatus{
		{ID: "fast", NodeName: "n1", CurrentCycle: 1, State: gpuv1alpha1.TaskStateCompleted, CompletedRounds: 2,
			EndTime: &metav1.Time{Time: now.Add(-2 * time.Hour)}, Result: &gpuv1alpha1.TaskResult{Passed: true}},
		{ID: "slow", NodeName: "n2", CurrentCycle: 1, State: gpuv1alpha1.TaskStateRunning, PodName: "slow-pod", Attempts: 2},
		{ID: "waiting", NodeName: "n3", CurrentCycle: 2, State: gpuv1alpha1.TaskStateCompleted, CompletedRounds: 2,
			EndTime: &metav1.Time{Time: now}, Result: &gpuv1alpha1.TaskResult{Passed: false, Errors: []string{"old"}}},
		{ID: "done", NodeName: "n4", CurrentCycle: 3, State: gpuv1alpha1.TaskStateCompleted, CompletedRounds: 2,
			EndTime: &metav1.Time{Time: now.Add(-2 * time.Hour)}},
	}
	c, cancel := newTestController(t, cr, testPod("default", "slow-pod", "t1", v1.PodRunning, now))
	defer cancel()
	if err := c.reconcile(context.Background(), "default/t1"); err != nil {
		t.Fatal(err)
	}
	got := c.getCRFromAPI(t, "default", "t1")
	fast := got.Status.Queue[0]
	if fast.CurrentCycle != 2 || fast.State != gpuv1alpha1.TaskStatePending || fast.Result != nil || fast.EndTime != nil {
		t.Fatalf("task whose interval elapsed did not advance independently: %+v", fast)
	}
	if !reflect.DeepEqual(got.Status.Queue[1:], cr.Status.Queue[1:]) {
		t.Fatalf("other tasks were modified: %+v", got.Status.Queue[1:])
	}
	if got.Status.Phase != gpuv1alpha1.PhaseRunning || got.Status.PendingTasks != 1 || got.Status.RunningTasks != 1 || got.Status.CompletedTasks != 2 {
		t.Fatalf("incorrect status summary: %+v", got.Status)
	}
	// After restart, each task advances from its own state; the concurrency gate still limits new Pod creation.
	cancel()
	restarted, stop := newTestController(t, got, testPod("default", "slow-pod", "t1", v1.PodRunning, now))
	defer stop()
	if err := restarted.reconcile(context.Background(), "default/t1"); err != nil {
		t.Fatal(err)
	}
	if task := restarted.getCRFromAPI(t, "default", "t1").Status.Queue[0]; task.CurrentCycle != 2 || task.PodName != "" {
		t.Fatalf("incorrect cycle or concurrency gate behavior after restart: %+v", task)
	}
	assertPodCount(t, restarted, "default", 1)
}

func TestFinishRoundStartsIntervalAfterLastRound(t *testing.T) {
	now := time.Now()
	cr := testCR("default", "t1", gpuv1alpha1.ActiveMonitoringSpec{Schedule: gpuv1alpha1.SchedulePolicy{RoundsPerCycle: 2, Interval: "1h"}})
	c := &Controller{}
	task := &gpuv1alpha1.TaskStatus{CurrentCycle: 1, State: gpuv1alpha1.TaskStateRunning, Attempts: 3}
	c.finishRound(cr, task, &gpuv1alpha1.TaskResult{Passed: false}, now.Add(-2*time.Hour))
	if advanceTaskCycle(cr, task, now) || task.CompletedRounds != 1 {
		t.Fatal("an intermediate round must not advance the cycle")
	}
	c.finishRound(cr, task, &gpuv1alpha1.TaskResult{Passed: true}, now)
	if advanceTaskCycle(cr, task, now.Add(59*time.Minute)) {
		t.Fatal("the full interval must elapse after the last round ends")
	}
	if !advanceTaskCycle(cr, task, now.Add(time.Hour)) {
		t.Fatal("did not advance after the interval elapsed")
	}
}

func TestFindTaskPodUsesTaskCycle(t *testing.T) {
	cr := testCR("default", "t1", gpuv1alpha1.ActiveMonitoringSpec{Concurrency: 1})
	task := gpuv1alpha1.TaskStatus{ID: "n1-gpu-0", NodeName: "n1", CurrentCycle: 4, State: gpuv1alpha1.TaskStatePending}
	oldTask := task
	oldTask.CurrentCycle = 3
	oldPod := mustPodFromBuild(t, cr, oldTask)
	oldPod.Status.Phase = v1.PodSucceeded
	orphan := mustPodFromBuild(t, cr, task)
	orphan.Status.Phase = v1.PodRunning
	c, cancel := newTestController(t, cr, oldPod, orphan)
	defer cancel()
	gateLive := 1
	c.processPending(context.Background(), cr, &task, &gateLive, time.Now(), defaultMaxRetries)
	if task.PodName != orphan.Name || gateLive != 1 {
		t.Fatalf("adopted a Pod from the wrong cycle or counted it twice: %+v", task)
	}
	assertPodCount(t, c, "default", 2)
}

func TestReconcileStableStatusAndResidualCleanup(t *testing.T) {
	for _, interval := range []string{"", "1h"} {
		t.Run("interval="+interval, func(t *testing.T) {
			cr := testCR("default", "t1", gpuv1alpha1.ActiveMonitoringSpec{Schedule: gpuv1alpha1.SchedulePolicy{Interval: interval}})
			cr.Status.Queue = []gpuv1alpha1.TaskStatus{{ID: "n1-gpu-0", NodeName: "n1", CurrentCycle: 1,
				State: gpuv1alpha1.TaskStateCompleted, CompletedRounds: 1, EndTime: &metav1.Time{Time: time.Now().Truncate(time.Second)}}}
			updatePhase(cr)
			recountStatus(cr)
			pod := testPod("default", "residual", "t1", v1.PodSucceeded, time.Now())
			c, cancel := newTestController(t, cr, pod)
			defer cancel()
			fake := c.config.DynamicClient.(*dynamicfake.FakeDynamicClient)
			fake.PrependReactor("update", "activemonitorings", func(action k8stesting.Action) (bool, runtime.Object, error) {
				t.Error("unchanged status must not be written again")
				return false, nil, nil
			})
			if err := c.reconcile(context.Background(), "default/t1"); err != nil {
				t.Fatal(err)
			}
			assertPodGone(t, c, "default", "residual")
		})
	}
}

func TestReconcileKeepsSettledTaskCycle(t *testing.T) {
	cr := testCR("default", "t1", gpuv1alpha1.ActiveMonitoringSpec{Schedule: gpuv1alpha1.SchedulePolicy{TotalCycles: 4, Interval: "1h"}})
	cr.Status.Phase = gpuv1alpha1.PhaseCompleted
	cr.Status.Queue = []gpuv1alpha1.TaskStatus{{ID: "n1-gpu-0", NodeName: "n1", CurrentCycle: 4, State: gpuv1alpha1.TaskStateCompleted, CompletedRounds: 1}}
	c, cancel := newTestController(t, cr)
	defer cancel()
	if err := c.reconcile(context.Background(), "default/t1"); err != nil {
		t.Fatal(err)
	}
	u, err := c.config.DynamicClient.Resource(activemonitoringGVR).Namespace("default").Get(context.Background(), "t1", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"currentCycle", "cycleStartTime"} {
		if _, found, _ := unstructured.NestedFieldNoCopy(u.Object, "status", field); found {
			t.Fatalf("must not write top-level cycle field %s", field)
		}
	}
	got := c.getCRFromAPI(t, "default", "t1")
	if got.Status.Queue[0].CurrentCycle != 4 || !crSettled(got) {
		t.Fatalf("a settled task's cycle must not change: %+v", got.Status)
	}
}

// Do not rerun failed rounds; continue remaining rounds in the current cycle before considering the next cycle.
func TestReconcileFailedTaskCyclePolicy(t *testing.T) {
	for _, rounds := range []int{1, 2} {
		for _, total := range []int{1, 2} {
			t.Run(fmt.Sprintf("completedRounds=%d/totalCycles=%d", rounds, total), func(t *testing.T) {
				cr := testCR("default", "failed-cycle", gpuv1alpha1.ActiveMonitoringSpec{
					Schedule: gpuv1alpha1.SchedulePolicy{TotalCycles: total, Interval: "1h", RoundsPerCycle: 2},
				})
				cr.Status.Phase = gpuv1alpha1.PhaseFailed
				cr.Status.Queue = []gpuv1alpha1.TaskStatus{{ID: "n1-gpu-0", NodeName: "n1", CurrentCycle: 1,
					State: gpuv1alpha1.TaskStateFailed, CompletedRounds: rounds,
					EndTime: &metav1.Time{Time: time.Now().Add(-2 * time.Hour).Truncate(time.Second)},
					Result:  &gpuv1alpha1.TaskResult{Passed: false, Errors: []string{"round 1 attempt 3: pod failed, retry budget exhausted"}},
				}}
				c, cancel := newTestController(t, cr)
				defer cancel()
				if err := c.reconcile(context.Background(), cr.Namespace+"/"+cr.Name); err != nil {
					t.Fatal(err)
				}
				got := c.getCRFromAPI(t, cr.Namespace, cr.Name)
				task := got.Status.Queue[0]
				if rounds < cr.Spec.Schedule.RoundsPerCycle {
					if task.State != gpuv1alpha1.TaskStatePending || task.CurrentCycle != 1 || task.CompletedRounds != rounds ||
						task.Attempts != 0 || !reflect.DeepEqual(task.Result, cr.Status.Queue[0].Result) ||
						got.Status.Phase != gpuv1alpha1.PhaseRunning || crSettled(got) {
						t.Fatalf("remaining rounds in the current cycle must run first even if the interval elapsed or the cycle limit was reached: %+v", got.Status)
					}
				} else if total == 1 {
					if !reflect.DeepEqual(got.Status.Queue, cr.Status.Queue) || got.Status.Phase != gpuv1alpha1.PhaseFailed || !crSettled(got) {
						t.Fatalf("a settled schedule must not automatically restart failed rounds: %+v", got.Status)
					}
				} else if task.CurrentCycle != 2 || task.CompletedRounds != 0 || task.Attempts != 0 || task.Result != nil ||
					task.State != gpuv1alpha1.TaskStatePending || got.Status.Phase != gpuv1alpha1.PhasePending {
					t.Fatalf("did not start a new cycle after the interval elapsed: %+v", got.Status)
				}
				assertPodCount(t, c, cr.Namespace, 0)
			})
		}
	}
}

// preemptedTask builds a Preempted Task with the given PodName/Attempts.
func preemptedTask(podName string, attempts int) *gpuv1alpha1.TaskStatus {
	return &gpuv1alpha1.TaskStatus{
		ID: "n1-gpu-0", NodeName: "n1", GPUMinor: 0, CurrentCycle: 1,
		State: gpuv1alpha1.TaskStatePreempted, PodName: podName, Attempts: attempts,
	}
}

// A successful rebuild after preemption immediately returns to Pending and is counted only once.
func TestProcessPreemptedRebuildBecomesPending(t *testing.T) {
	cr := testCR("default", "t1", gpuv1alpha1.ActiveMonitoringSpec{})
	c, cancel := newTestController(t, cr)
	defer cancel()

	task := preemptedTask("", 1)
	gateLive := 0
	c.processPreempted(context.Background(), cr, task, &gateLive, time.Now(), defaultMaxRetries)

	if task.State != gpuv1alpha1.TaskStatePending {
		t.Fatalf("state after rebuilding should be Pending, got %q", task.State)
	}
	if task.PodName == "" {
		t.Fatal("expected a rebuild Pod to be created")
	}
	if task.Attempts != 2 {
		t.Fatalf("attempts = %d, want 2", task.Attempts)
	}
	if gateLive != 1 {
		t.Fatalf("gateLive = %d, want 1", gateLive)
	}
}

// TestProcessPreemptedGateBlocked verifies that a full concurrency gate keeps
// the Task Preempted without creating a Pod.
func TestProcessPreemptedGateBlocked(t *testing.T) {
	cr := testCR("default", "t1", gpuv1alpha1.ActiveMonitoringSpec{Concurrency: 1})
	c, cancel := newTestController(t, cr)
	defer cancel()

	task := preemptedTask("", 1)
	gateLive := 1 // gate already full
	c.processPreempted(context.Background(), cr, task, &gateLive, time.Now(), defaultMaxRetries)

	if task.State != gpuv1alpha1.TaskStatePreempted || task.PodName != "" {
		t.Fatalf("task = %+v, want Preempted without a Pod", task)
	}
	if task.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (unchanged)", task.Attempts)
	}
}

// An unscheduled replacement Pod should leave the task Pending, not Preempted.
func TestProcessPreemptedWithPodPending(t *testing.T) {
	now := time.Now()
	cr := testCR("default", "t1", gpuv1alpha1.ActiveMonitoringSpec{})
	pod := testPod("default", "p-rebuild", "t1", v1.PodPending, now.Add(-1*time.Minute))
	c, cancel := newTestController(t, cr, pod)
	defer cancel()

	task := preemptedTask("p-rebuild", 2)
	gateLive := 1 // counted by countPods already
	c.processPreempted(context.Background(), cr, task, &gateLive, now, defaultMaxRetries)

	if task.State != gpuv1alpha1.TaskStatePending {
		t.Fatalf("an unscheduled Pod should leave the task Pending, got %q", task.State)
	}
	if task.PodName != "p-rebuild" || task.Attempts != 2 {
		t.Fatalf("task = %+v, want PodName/Attempts unchanged", task)
	}
	if gateLive != 1 {
		t.Fatalf("gateLive = %d, want unchanged 1", gateLive)
	}
}

// TestProcessPreemptedWithPodRunning verifies the Task flips to Running only
// once the rebuild Pod is truly K8s-Running.
func TestProcessPreemptedWithPodRunning(t *testing.T) {
	now := time.Now()
	cr := testCR("default", "t1", gpuv1alpha1.ActiveMonitoringSpec{})
	pod := testPod("default", "p-rebuild", "t1", v1.PodRunning, now.Add(-1*time.Minute))
	c, cancel := newTestController(t, cr, pod)
	defer cancel()

	task := preemptedTask("p-rebuild", 2)
	gateLive := 1
	c.processPreempted(context.Background(), cr, task, &gateLive, now, defaultMaxRetries)

	if task.State != gpuv1alpha1.TaskStateRunning {
		t.Fatalf("state = %q, want Running", task.State)
	}
}

// TestProcessPreemptedWithPodVanished verifies that when a Preempted Task's
// rebuild Pod is preempted again, handlePreemptedPod clears PodName (so the next
// reconcile rebuilds) and keeps the Task Preempted WITHOUT consuming budget at
// detection time — Attempts is counted once per Pod creation, not per preemption.
func TestProcessPreemptedWithPodVanished(t *testing.T) {
	cr := testCR("default", "t1", gpuv1alpha1.ActiveMonitoringSpec{})
	c, cancel := newTestController(t, cr)
	defer cancel()

	task := preemptedTask("p-gone", 1) // rebuild Pod vanished (preempted again)
	gateLive := 0
	c.processPreempted(context.Background(), cr, task, &gateLive, time.Now(), defaultMaxRetries)

	if task.State != gpuv1alpha1.TaskStatePreempted {
		t.Fatalf("state = %q, want Preempted", task.State)
	}
	if task.PodName != "" {
		t.Fatalf("podName = %q, want empty (cleared so next reconcile rebuilds)", task.PodName)
	}
	if task.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (unchanged; budget counted at rebuild)", task.Attempts)
	}
	if task.LastPreemptTime == nil {
		t.Fatal("expected LastPreemptTime to be refreshed")
	}
	if task.Result == nil || len(task.Result.Errors) != 1 {
		t.Fatalf("expected one preempted error entry, got %+v", task.Result)
	}
}

// TestProcessPreemptedExhaustedNoPod verifies that a Preempted Task without a
// Pod and with the retry budget exhausted fails the round (finishRound).
func TestProcessPreemptedExhaustedNoPod(t *testing.T) {
	cr := testCR("default", "t1", gpuv1alpha1.ActiveMonitoringSpec{Schedule: gpuv1alpha1.SchedulePolicy{RoundsPerCycle: 1}})
	c, cancel := newTestController(t, cr)
	defer cancel()

	task := preemptedTask("", 3) // attempts == maxRetries
	gateLive := 0
	c.processPreempted(context.Background(), cr, task, &gateLive, time.Now(), defaultMaxRetries)

	if task.State != gpuv1alpha1.TaskStateFailed {
		t.Fatalf("exhausting the budget should result in Failed, got %q", task.State)
	}
	if task.CompletedRounds != 1 {
		t.Fatalf("completedRounds = %d, want 1", task.CompletedRounds)
	}
	if task.Result == nil || task.Result.Passed {
		t.Fatalf("result = %+v, want passed=false", task.Result)
	}
}

// TestProcessPreemptedExhaustedAfterRePreemption verifies the corrected budget:
// a re-preemption clears PodName (no budget consumed at detection), then the
// following processPreempted sees Attempts>=maxRetries and fails the round.
func TestProcessPreemptedExhaustedAfterRePreemption(t *testing.T) {
	cr := testCR("default", "t1", gpuv1alpha1.ActiveMonitoringSpec{Schedule: gpuv1alpha1.SchedulePolicy{RoundsPerCycle: 1}})
	c, cancel := newTestController(t, cr)
	defer cancel()

	task := preemptedTask("p-gone", 3) // Attempts already at maxRetries, Pod vanished
	gateLive := 0

	// Step 1: detect the re-preemption → clears PodName, stays Preempted.
	c.processPreempted(context.Background(), cr, task, &gateLive, time.Now(), defaultMaxRetries)
	if task.State != gpuv1alpha1.TaskStatePreempted || task.PodName != "" {
		t.Fatalf("after re-preemption: task = %+v, want Preempted with empty PodName", task)
	}

	// Step 2: next reconcile → budget exhausted → finishRound(failed).
	c.processPreempted(context.Background(), cr, task, &gateLive, time.Now(), defaultMaxRetries)
	if task.State != gpuv1alpha1.TaskStateFailed {
		t.Fatalf("repeated preemption exhausting the budget should result in Failed, got %q", task.State)
	}
	if task.CompletedRounds != 1 || task.Attempts != 0 || task.PodName != "" {
		t.Fatalf("task = %+v, want round finished and reset", task)
	}
	if task.Result == nil || task.Result.Passed {
		t.Fatalf("result = %+v, want passed=false", task.Result)
	}
}

// TestProcessPendingPodVanishedBecomesPreempted verifies the Pending-with-Pod
// path still flips to Preempted without consuming budget (the rebuild attempt
// is counted later in processPreempted).
func TestProcessPendingPodVanishedBecomesPreempted(t *testing.T) {
	cr := testCR("default", "t1", gpuv1alpha1.ActiveMonitoringSpec{})
	c, cancel := newTestController(t, cr)
	defer cancel()

	task := &gpuv1alpha1.TaskStatus{
		ID: "n1-gpu-0", NodeName: "n1", GPUMinor: 0, CurrentCycle: 1,
		State: gpuv1alpha1.TaskStatePending, PodName: "p-gone", Attempts: 1,
	}
	gateLive := 0
	c.processPending(context.Background(), cr, task, &gateLive, time.Now(), defaultMaxRetries)

	if task.State != gpuv1alpha1.TaskStatePreempted {
		t.Fatalf("state = %q, want Preempted", task.State)
	}
	if task.PodName != "" {
		t.Fatalf("podName = %q, want empty (cleared for rebuild)", task.PodName)
	}
	if task.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (unchanged; rebuild counts later)", task.Attempts)
	}
	if task.LastPreemptTime == nil {
		t.Fatal("expected LastPreemptTime to be set")
	}
}

// Adding nodes does not change an already initialized queue.
func TestReconcileKeepsQueueWhenNodesAdded(t *testing.T) {
	start := time.Now().Add(-2 * time.Hour)
	cr := testCR("default", "t1", gpuv1alpha1.ActiveMonitoringSpec{
		Schedule: gpuv1alpha1.SchedulePolicy{TotalCycles: 0, Interval: "1h", RoundsPerCycle: 1},
	})
	cr.Status.Phase = gpuv1alpha1.PhaseCompleted // held result of cycle 1
	cr.Status.Queue = []gpuv1alpha1.TaskStatus{
		{ID: "n1-gpu-0", NodeName: "n1", GPUMinor: 0, CurrentCycle: 1, State: gpuv1alpha1.TaskStateCompleted, CompletedRounds: 1, EndTime: &metav1.Time{Time: start}},
	}

	// n2 (2 GPUs) joined after cycle 1's queue was built for n1 only.
	c, cancel := newTestController(t, cr, gpuNode("n1", 1), gpuNode("n2", 2))
	defer cancel()

	if err := c.reconcile(context.Background(), "default/t1"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := c.getCRFromAPI(t, "default", "t1")
	if got.Status.Queue[0].CurrentCycle != 2 {
		t.Fatalf("currentCycle = %d, want 2", got.Status.Queue[0].CurrentCycle)
	}
	if got.Status.Phase != gpuv1alpha1.PhasePending {
		t.Fatalf("phase = %q, want Pending", got.Status.Phase)
	}
	if len(got.Status.Queue) != 1 || got.Status.Queue[0].ID != "n1-gpu-0" {
		t.Fatalf("queue membership must not change: %+v", got.Status.Queue)
	}
	if got.Status.PendingTasks != 1 || got.Status.TotalTasks != 1 {
		t.Fatalf("counts should be 1/1: %+v", got.Status)
	}
}

// Removing nodes preserves existing task identities and queue positions.
func TestReconcileKeepsQueueWhenNodesRemoved(t *testing.T) {
	start := time.Now().Add(-2 * time.Hour)
	cr := testCR("default", "t1", gpuv1alpha1.ActiveMonitoringSpec{
		Schedule: gpuv1alpha1.SchedulePolicy{TotalCycles: 0, Interval: "1h", RoundsPerCycle: 1},
	})
	cr.Status.Phase = gpuv1alpha1.PhaseCompleted
	// Cycle 1 enumerated n1 and n2; n2 has since been removed from the cluster.
	cr.Status.Queue = []gpuv1alpha1.TaskStatus{
		{ID: "n1-gpu-0", NodeName: "n1", GPUMinor: 0, CurrentCycle: 1, State: gpuv1alpha1.TaskStateCompleted, CompletedRounds: 1, EndTime: &metav1.Time{Time: start}},
		{ID: "n2-gpu-0", NodeName: "n2", GPUMinor: 0, CurrentCycle: 1, State: gpuv1alpha1.TaskStateCompleted, CompletedRounds: 1, EndTime: &metav1.Time{Time: start}},
	}

	c, cancel := newTestController(t, cr, gpuNode("n1", 1)) // n2 gone
	defer cancel()

	if err := c.reconcile(context.Background(), "default/t1"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := c.getCRFromAPI(t, "default", "t1")
	if got.Status.Queue[0].CurrentCycle != 2 {
		t.Fatalf("currentCycle = %d, want 2", got.Status.Queue[0].CurrentCycle)
	}
	if len(got.Status.Queue) != 2 || got.Status.Queue[0].ID != "n1-gpu-0" || got.Status.Queue[1].ID != "n2-gpu-0" {
		t.Fatalf("queue membership and order must not change: %+v", got.Status.Queue)
	}
	if got.Status.PendingTasks != 2 || got.Status.TotalTasks != 2 || got.Status.Queue[1].CurrentCycle != 2 {
		t.Fatalf("incorrect counts or cycle advancement: %+v", got.Status)
	}
}

// All tasks start in cycle 1 / Pending after initial queue creation.
func TestInitQueueStartsPending(t *testing.T) {
	cr := testCR("default", "t1", gpuv1alpha1.ActiveMonitoringSpec{}) // empty Queue → initQueue path
	node := gpuNode("n1", 1)
	node.Labels[NodeEnabledLabelKey] = NodeEnabledLabelValue
	c, cancel := newTestController(t, cr, node)
	defer cancel()

	if err := c.reconcile(context.Background(), "default/t1"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := c.getCRFromAPI(t, "default", "t1")
	if got.Status.Phase != gpuv1alpha1.PhasePending {
		t.Fatalf("phase after init = %q, want Pending (consistent with new-cycle refresh)", got.Status.Phase)
	}
	if len(got.Status.Queue) != 1 || got.Status.Queue[0].ID != "n1-gpu-0" ||
		got.Status.Queue[0].State != gpuv1alpha1.TaskStatePending {
		t.Fatalf("queue not initialized as expected: %+v", got.Status.Queue)
	}
	if got.Status.Queue[0].CurrentCycle != 1 {
		t.Fatalf("currentCycle = %d, want 1", got.Status.Queue[0].CurrentCycle)
	}
	if got.Status.PendingTasks != 1 || got.Status.TotalTasks != 1 {
		t.Fatalf("counts = pending %d total %d, want 1/1", got.Status.PendingTasks, got.Status.TotalTasks)
	}
}

// Unfinished tasks must not advance the cycle even if the previous round's endTime is far in the past.
func TestReconcileNoRollWhileTasksInProgress(t *testing.T) {
	start := time.Now().Add(-2 * time.Hour) // interval (1h) already elapsed
	cr := testCR("default", "t1", gpuv1alpha1.ActiveMonitoringSpec{
		Schedule: gpuv1alpha1.SchedulePolicy{TotalCycles: 0, Interval: "1h", RoundsPerCycle: 1},
	})
	cr.Status.Phase = gpuv1alpha1.PhaseRunning
	cr.Status.Queue = []gpuv1alpha1.TaskStatus{
		{ID: "n1-gpu-0", NodeName: "n1", GPUMinor: 0, CurrentCycle: 1, State: gpuv1alpha1.TaskStatePending, EndTime: &metav1.Time{Time: start}}, // not terminal
	}

	c, cancel := newTestController(t, cr, gpuNode("n1", 1))
	defer cancel()

	if err := c.reconcile(context.Background(), "default/t1"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := c.getCRFromAPI(t, "default", "t1")
	if got.Status.Queue[0].CurrentCycle != 1 {
		t.Fatalf("currentCycle = %d, want 1 (no roll while a task is still in progress)", got.Status.Queue[0].CurrentCycle)
	}
	if got.Status.Phase != gpuv1alpha1.PhaseRunning {
		t.Fatalf("phase = %q, want Running", got.Status.Phase)
	}
	// Step 2 created the detection Pod for the Pending task and it was NOT
	// discarded by a rebuild: the queue still holds exactly that task, with a Pod.
	if len(got.Status.Queue) != 1 || got.Status.Queue[0].PodName == "" {
		t.Fatalf("expected the in-progress task to keep its created Pod: %+v", got.Status.Queue)
	}
}

// Once a Pending task creates a Pod, the CR reports Running without waiting for the Pod to become Running.
func TestReconcilePendingBecomesRunning(t *testing.T) {
	cr := testCR("default", "t1", gpuv1alpha1.ActiveMonitoringSpec{
		Schedule: gpuv1alpha1.SchedulePolicy{RoundsPerCycle: 1},
	})
	cr.Status.Phase = gpuv1alpha1.PhasePending // as persisted by initQueue/refresh
	cr.Status.Queue = []gpuv1alpha1.TaskStatus{
		{ID: "n1-gpu-0", NodeName: "n1", GPUMinor: 0, CurrentCycle: 1, State: gpuv1alpha1.TaskStatePending},
	}

	c, cancel := newTestController(t, cr, gpuNode("n1", 1))
	defer cancel()

	if err := c.reconcile(context.Background(), "default/t1"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := c.getCRFromAPI(t, "default", "t1")
	if got.Status.Phase != gpuv1alpha1.PhaseRunning {
		t.Fatalf("phase = %q, want Running (cycle in progress: a Task is non-terminal)", got.Status.Phase)
	}
	if len(got.Status.Queue) != 1 || got.Status.Queue[0].PodName == "" {
		t.Fatalf("expected Step 2 to create the detection Pod: %+v", got.Status.Queue)
	}
}

// mustPodFromBuild materialises the detection Pod buildTestPod would create for a
// Task, as a typed *v1.Pod (so it can be seeded into the fake client/informer).
func mustPodFromBuild(t *testing.T, cr *gpuv1alpha1.ActiveMonitoring, task gpuv1alpha1.TaskStatus) *v1.Pod {
	t.Helper()
	u, err := buildTestPod(cr, task, defaultDetectionSettings())
	if err != nil {
		t.Fatalf("buildTestPod: %v", err)
	}
	pod := &v1.Pod{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, pod); err != nil {
		t.Fatalf("convert built pod: %v", err)
	}
	return pod
}

// assertPodCount asserts the number of Pods persisted in the (fake) API for ns.
func assertPodCount(t *testing.T, c *Controller, ns string, want int) {
	t.Helper()
	list, err := c.config.DynamicClient.Resource(podGVR).Namespace(ns).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(list.Items) != want {
		t.Fatalf("live pod count = %d, want %d", len(list.Items), want)
	}
}

// TestProcessPendingAdoptsOrphanPod verifies the Issue #4 fix: when a Task's
// PodName is empty but a live detection Pod for it already exists (a prior
// reconcile created the Pod but failed to persist task.PodName — updateStatus
// error or process crash), the controller ADOPTS that Pod instead of creating a
// duplicate orphan. Adoption happens even though the concurrency gate is full
// (the Pod is already counted by countPods), so an orphan can never wedge it.
func TestProcessPendingAdoptsOrphanPod(t *testing.T) {
	cr := testCR("default", "t1", gpuv1alpha1.ActiveMonitoringSpec{Concurrency: 1})
	cr.Status.Phase = gpuv1alpha1.PhaseRunning
	cr.Status.Queue = []gpuv1alpha1.TaskStatus{
		{ID: "n1-gpu-0", NodeName: "n1", GPUMinor: 0, CurrentCycle: 1, State: gpuv1alpha1.TaskStatePending}, // PodName empty
	}
	// The orphaned Pod from the failed reconcile: live and Running (so it occupies
	// the single gate slot), correctly labeled for this Task + cycle-round.
	orphan := mustPodFromBuild(t, cr, cr.Status.Queue[0])
	orphan.Status.Phase = v1.PodRunning
	orphanName := orphan.GetName()

	c, cancel := newTestController(t, cr, gpuNode("n1", 1), orphan)
	defer cancel()

	if err := c.reconcile(context.Background(), "default/t1"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := c.getCRFromAPI(t, "default", "t1")
	if got.Status.Queue[0].PodName != orphanName {
		t.Fatalf("task.PodName = %q, want adopted orphan %q (must NOT create a duplicate)", got.Status.Queue[0].PodName, orphanName)
	}
	if got.Status.Queue[0].Attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (adopted Pod seeded as first attempt)", got.Status.Queue[0].Attempts)
	}
	assertPodCount(t, c, "default", 1) // orphan adopted; no duplicate despite the full gate
}

// TestProcessPreemptedAdoptsOrphanPod is the Preempted-path counterpart: a
// rebuild Pod created but not persisted (PodName empty, State Preempted) is
// adopted rather than duplicated, and Attempts is incremented to reflect the
// rebuild attempt the failed reconcile intended.
func TestProcessPreemptedAdoptsOrphanPod(t *testing.T) {
	cr := testCR("default", "t1", gpuv1alpha1.ActiveMonitoringSpec{})
	cr.Status.Phase = gpuv1alpha1.PhaseRunning
	cr.Status.Queue = []gpuv1alpha1.TaskStatus{
		{ID: "n1-gpu-0", NodeName: "n1", GPUMinor: 0, CurrentCycle: 1, State: gpuv1alpha1.TaskStatePreempted, Attempts: 1}, // PodName empty
	}
	orphan := mustPodFromBuild(t, cr, cr.Status.Queue[0])
	orphan.Status.Phase = v1.PodRunning
	orphanName := orphan.GetName()

	c, cancel := newTestController(t, cr, gpuNode("n1", 1), orphan)
	defer cancel()

	if err := c.reconcile(context.Background(), "default/t1"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := c.getCRFromAPI(t, "default", "t1")
	if got.Status.Queue[0].PodName != orphanName {
		t.Fatalf("task.PodName = %q, want adopted orphan %q", got.Status.Queue[0].PodName, orphanName)
	}
	if got.Status.Queue[0].Attempts != 2 {
		t.Fatalf("attempts = %d, want 2 (adopted rebuild counts as the attempt)", got.Status.Queue[0].Attempts)
	}
	if got.Status.Queue[0].State != gpuv1alpha1.TaskStatePending {
		t.Fatalf("state after adopting a replacement Pod should be Pending, got %q", got.Status.Queue[0].State)
	}
	assertPodCount(t, c, "default", 1)
}

// succeededPod builds a Succeeded detection Pod carrying the given termination
// message so parseTerminationMessage can consume it. It reuses testPod's labels
// (LabelActiveMonitoringCR) so listCRPods / the by-CR index can find it. The terminated
// window is fixed at 120s because the detection wall time is now derived from
// StartedAt/FinishedAt rather than reported in the message.
func succeededPod(ns, name, crName string, createdAt time.Time, terminationMsg string) *v1.Pod {
	pod := testPod(ns, name, crName, v1.PodSucceeded, createdAt)
	pod.Status.ContainerStatuses = []v1.ContainerStatus{{
		Name: ActiveMonitoringContainerName,
		State: v1.ContainerState{Terminated: &v1.ContainerStateTerminated{
			ExitCode:   0,
			Message:    terminationMsg,
			StartedAt:  metav1.NewTime(createdAt),
			FinishedAt: metav1.NewTime(createdAt.Add(120 * time.Second)),
		}},
	}}
	return pod
}

// assertPodGone asserts the Pod is absent from the (fake) API.
func assertPodGone(t *testing.T, c *Controller, ns, name string) {
	t.Helper()
	_, err := c.config.DynamicClient.Resource(podGVR).Namespace(ns).Get(context.Background(), name, metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("pod %s should be deleted, got err=%v", name, err)
	}
}

// assertPodExists asserts the Pod is still present in the (fake) API.
func assertPodExists(t *testing.T, c *Controller, ns, name string) {
	t.Helper()
	if _, err := c.config.DynamicClient.Resource(podGVR).Namespace(ns).Get(context.Background(), name, metav1.GetOptions{}); err != nil {
		t.Fatalf("pod %s should exist, got err=%v", name, err)
	}
}

// TestSweepResidualPodsDeletesOnlyTerminalUnreferenced locks the sweep contract:
// only Pods that are BOTH terminal (Succeeded/Failed) AND no longer referenced by
// any Task are deleted. A live-but-unreferenced Pod (an adoptable orphan) and a
// terminal-but-referenced Pod are both kept.
func TestSweepResidualPodsDeletesOnlyTerminalUnreferenced(t *testing.T) {
	cr := testCR("default", "t1", gpuv1alpha1.ActiveMonitoringSpec{})
	cr.Status.Queue = []gpuv1alpha1.TaskStatus{
		{ID: "n1-gpu-0", NodeName: "n1", GPUMinor: 0, CurrentCycle: 1, PodName: "p-wanted"},
	}
	now := time.Now()
	pConsumed := testPod("default", "p-consumed", "t1", v1.PodSucceeded, now) // unreferenced + terminal -> delete
	pRunning := testPod("default", "p-running", "t1", v1.PodRunning, now)     // unreferenced but alive -> keep
	pWanted := testPod("default", "p-wanted", "t1", v1.PodSucceeded, now)     // referenced -> keep

	c, cancel := newTestController(t, cr, pConsumed, pRunning, pWanted)
	defer cancel()

	c.sweepResidualPods(context.Background(), cr)

	assertPodCount(t, c, "default", 2)
	assertPodGone(t, c, "default", "p-consumed")
	assertPodExists(t, c, "default", "p-running")
	assertPodExists(t, c, "default", "p-wanted")
}

// TestReconcileDeletesSucceededPodAfterPersist verifies the happy path: a Running
// Task whose detection Pod has Succeeded is consumed (round advanced, result
// merged) and its Pod is deleted by the post-persist sweep.
func TestReconcileDeletesSucceededPodAfterPersist(t *testing.T) {
	now := time.Now()
	cr := testCR("default", "t1", gpuv1alpha1.ActiveMonitoringSpec{
		Schedule: gpuv1alpha1.SchedulePolicy{RoundsPerCycle: 1, TotalCycles: 1},
	})
	cr.Status.Phase = gpuv1alpha1.PhaseRunning
	cr.Status.Queue = []gpuv1alpha1.TaskStatus{
		{ID: "n1-gpu-0", NodeName: "n1", GPUMinor: 0, CurrentCycle: 1, State: gpuv1alpha1.TaskStateRunning, PodName: "p1", Attempts: 1},
	}
	pod := succeededPod("default", "p1", "t1", now, `{"status":"PASS","message":""}`)

	c, cancel := newTestController(t, cr, gpuNode("n1", 1), pod)
	defer cancel()

	if err := c.reconcile(context.Background(), "default/t1"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := c.getCRFromAPI(t, "default", "t1")
	if got.Status.Queue[0].CompletedRounds != 1 {
		t.Fatalf("completedRounds = %d, want 1 (round consumed)", got.Status.Queue[0].CompletedRounds)
	}
	if r := got.Status.Queue[0].Result; r == nil || !r.Passed || r.Duration != 120 {
		t.Fatalf("result not merged from termination message: %+v", r)
	}
	if got.Status.Queue[0].PodName != "" {
		t.Fatalf("podName = %q, want empty after the round is consumed", got.Status.Queue[0].PodName)
	}
	assertPodCount(t, c, "default", 0) // consumed Pod swept after the persist succeeded
	assertPodGone(t, c, "default", "p1")
}

// Valid FAIL/ERROR results do not trigger retries: finish the round and record details, keeping completion state independent of passed.
func TestReconcileSucceededButFailedRecordsRoundAndAttempt(t *testing.T) {
	for _, verdict := range []string{detectionStatusFail, detectionStatusError} {
		for _, detail := range []string{"SDC detected CRC error", ""} {
			for attempt := 1; attempt <= defaultMaxRetries; attempt++ {
				t.Run(fmt.Sprintf("%s/detail=%s/attempt=%d", verdict, detail, attempt), func(t *testing.T) {
					cr := testCR("default", "t1", gpuv1alpha1.ActiveMonitoringSpec{
						Schedule: gpuv1alpha1.SchedulePolicy{RoundsPerCycle: 2, TotalCycles: 1},
					})
					cr.Status.Phase = gpuv1alpha1.PhaseRunning
					// The first round passed; FAIL/ERROR in the last round overwrites passed while execution still ends as Completed.
					cr.Status.Queue = []gpuv1alpha1.TaskStatus{{
						ID: "n1-gpu-0", NodeName: "n1", CurrentCycle: 1, State: gpuv1alpha1.TaskStateRunning,
						PodName: "p1", Attempts: attempt, CompletedRounds: 1,
						Result: &gpuv1alpha1.TaskResult{Passed: true, Duration: 7},
					}}
					pod := succeededPod("default", "p1", "t1", time.Now(),
						fmt.Sprintf(`{"status":%q,"message":%q}`, verdict, detail))
					c, cancel := newTestController(t, cr, pod)
					defer cancel()
					if err := c.reconcile(context.Background(), "default/t1"); err != nil {
						t.Fatal(err)
					}
					got := c.getCRFromAPI(t, "default", "t1")
					task := got.Status.Queue[0]
					wantError := fmt.Sprintf("round 2 attempt %d: detection %s", attempt, verdict)
					if task.State != gpuv1alpha1.TaskStateCompleted || got.Status.Phase != gpuv1alpha1.PhaseCompleted ||
						got.Status.CompletedTasks != 1 || got.Status.FailedTasks != 0 || !crSettled(got) {
						t.Fatalf("detection FAIL/ERROR should complete normally even when all attempts have been used: %+v", got.Status)
					}
					if task.CompletedRounds != 2 || task.Attempts != 0 || task.EndTime == nil ||
						task.Result == nil || task.Result.Passed || task.Result.Duration != 120 {
						t.Fatalf("incorrect round count or latest detection result: %+v", task)
					}
					if detail != "" {
						wantError += ", " + detail
					}
					if !reflect.DeepEqual(task.Result.Errors, []string{wantError}) || task.PodName != "" || task.CurrentCycle != 1 {
						t.Fatalf("error details are duplicated or missing, or the state is incorrect: %+v", task)
					}
					assertPodGone(t, c, "default", "p1")
					restarted, stop := newTestController(t, got)
					defer stop()
					if err := restarted.reconcile(context.Background(), "default/t1"); err != nil {
						t.Fatal(err)
					}
					assertPodCount(t, restarted, "default", 0)
				})
			}
		}
	}
}

// Keep the Pod when status persistence fails; delete it only after reparsing and successfully persisting the result.
func TestReconcilePersistFailureKeepsPodForReparse(t *testing.T) {
	for _, verdict := range []string{detectionStatusPass, detectionStatusFail, detectionStatusError} {
		t.Run(verdict, func(t *testing.T) {
			now := time.Now()
			cr := testCR("default", "t1", gpuv1alpha1.ActiveMonitoringSpec{
				Schedule: gpuv1alpha1.SchedulePolicy{RoundsPerCycle: 1, TotalCycles: 1},
			})
			cr.Status.Phase = gpuv1alpha1.PhaseRunning
			cr.Status.Queue = []gpuv1alpha1.TaskStatus{
				{ID: "n1-gpu-0", NodeName: "n1", GPUMinor: 0, CurrentCycle: 1, State: gpuv1alpha1.TaskStateRunning, PodName: "p1", Attempts: 1},
			}
			pod := succeededPod("default", "p1", "t1", now, fmt.Sprintf(`{"status":%q,"message":""}`, verdict))

			c, cancel := newTestController(t, cr, gpuNode("n1", 1), pod)
			defer cancel()

			// Fail the FIRST status persist with a non-conflict error; later ones succeed.
			fake := c.config.DynamicClient.(*dynamicfake.FakeDynamicClient)
			var statusCalls int32
			fake.PrependReactor("update", "activemonitorings", func(action k8stesting.Action) (bool, runtime.Object, error) {
				if action.GetSubresource() != "status" {
					return false, nil, nil
				}
				if atomic.AddInt32(&statusCalls, 1) == 1 {
					return true, nil, apierrors.NewInternalError(fmt.Errorf("injected persist failure"))
				}
				return false, nil, nil
			})

			// Reconcile #1: persist fails -> reconcile returns the error and the sweep is
			// skipped, so the consumed Pod survives and the status is NOT advanced.
			if err := c.reconcile(context.Background(), "default/t1"); err == nil {
				t.Fatal("expected reconcile to surface the injected persist error")
			}
			assertPodExists(t, c, "default", "p1") // NOT deleted before the result is durable
			if got := c.getCRFromAPI(t, "default", "t1"); got.Status.Queue[0].CompletedRounds != 0 {
				t.Fatalf("status advanced despite the failed persist: %+v", got.Status.Queue[0])
			}

			// Reconcile #2: persist succeeds -> the Succeeded Pod is re-parsed (idempotent),
			// the round advances, and only then is the Pod swept.
			if err := c.reconcile(context.Background(), "default/t1"); err != nil {
				t.Fatalf("second reconcile: %v", err)
			}
			got := c.getCRFromAPI(t, "default", "t1")
			task := got.Status.Queue[0]
			if task.CompletedRounds != 1 || task.State != gpuv1alpha1.TaskStateCompleted || task.Result == nil ||
				task.Result.Passed != (verdict == detectionStatusPass) || got.Status.Phase != gpuv1alpha1.PhaseCompleted {
				t.Fatalf("retrying persistence should save the detection result and complete normally: %+v", got.Status)
			}
			if verdict != detectionStatusPass && !reflect.DeepEqual(task.Result.Errors, []string{"round 1 attempt 1: detection " + verdict}) {
				t.Fatalf("retrying persistence must not duplicate detection errors: %+v", task.Result)
			}
			assertPodCount(t, c, "default", 0)
			assertPodGone(t, c, "default", "p1")
		})
	}
}

// Exclude repeated adoption of old Pods: remove each old Pod after failure before checking actual creation counts.
func TestRetryBudgetCountsCreatedPods(t *testing.T) {
	for _, mode := range []string{"failed", "preempted", "mixed", "parse-error"} {
		t.Run(mode, func(t *testing.T) {
			cr := testCR("default", "budget", gpuv1alpha1.ActiveMonitoringSpec{
				Schedule: gpuv1alpha1.SchedulePolicy{RoundsPerCycle: 2, TotalCycles: 1},
			})
			cr.Status.Queue = []gpuv1alpha1.TaskStatus{{ID: "n1-gpu-0", NodeName: "n1", CurrentCycle: 1, State: gpuv1alpha1.TaskStatePending}}
			c, cancel := newTestController(t, cr)
			defer cancel()
			task := &cr.Status.Queue[0]
			ctx := context.Background()
			createdCount := 0
			for round := 0; round < cr.Spec.Schedule.RoundsPerCycle; round++ {
				for attempt := 1; attempt <= defaultMaxRetries; attempt++ {
					gate := 0
					c.processTask(ctx, cr, task, &gate, time.Now())
					if task.Attempts != attempt || task.PodName == "" || task.State != gpuv1alpha1.TaskStatePending || gate != 1 {
						t.Fatalf("attempt=%d has incorrect state after creation: %+v gate=%d", attempt, task, gate)
					}
					name := task.PodName
					pod, err := c.config.DynamicClient.Resource(podGVR).Namespace(cr.Namespace).Get(ctx, name, metav1.GetOptions{})
					if err != nil || pod.GetLabels()[LabelActiveMonitoringAttempt] != strconv.Itoa(attempt) ||
						pod.GetLabels()[LabelActiveMonitoringRound] != roundKey(1, round) {
						t.Fatalf("incorrect Pod round/attempt labels: pod=%v err=%v", pod, err)
					}
					createdCount++
					// Wait for creation and deletion events so repeated adoption of old terminal Pods cannot affect the budget test.
					if err := wait.PollUntilContextTimeout(ctx, time.Millisecond, time.Second, true, func(context.Context) (bool, error) {
						return c.getPod(cr.Namespace, name) != nil, nil
					}); err != nil {
						t.Fatal(err)
					}
					c.deleteResidualPod(ctx, cr.Namespace, name)
					if err := wait.PollUntilContextTimeout(ctx, time.Millisecond, time.Second, true, func(context.Context) (bool, error) {
						return c.getPod(cr.Namespace, name) == nil, nil
					}); err != nil {
						t.Fatal(err)
					}
					preempted := mode == "preempted" || mode == "mixed" && attempt == 2
					if preempted {
						c.handlePreemptedPod(task, time.Now())
						if task.Attempts != attempt {
							t.Fatal("preemption handling must not increment attempts")
						}
						if attempt == defaultMaxRetries {
							c.processTask(ctx, cr, task, &gate, time.Now())
						}
					} else if mode == "failed" || mode == "mixed" && attempt == 1 {
						c.handlePodFailed(cr, task, time.Now(), defaultMaxRetries, "pod failed")
					} else {
						terminal := succeededPod(cr.Namespace, name, cr.Name, time.Now(), "invalid json")
						c.handleSucceeded(cr, task, mustUnstructured(t, terminal), time.Now(), defaultMaxRetries)
					}
					if attempt < defaultMaxRetries && (task.Attempts != attempt || task.CompletedRounds != round) {
						t.Fatalf("failure handling incremented counters or finished the round prematurely: %+v", task)
					}
				}
				updatePhase(cr)
				if task.State != gpuv1alpha1.TaskStateFailed || task.Attempts != 0 || task.CompletedRounds != round+1 ||
					task.PodName != "" || task.Result == nil || task.Result.Passed {
					t.Fatalf("budget exhaustion should finish only the current round and reset its budget: %+v", task)
				}
				if round+1 < cr.Spec.Schedule.RoundsPerCycle {
					if cr.Status.Phase != gpuv1alpha1.PhaseRunning || crSettled(cr) {
						t.Fatalf("an intermediate round failure must not settle the entire ActiveMonitoring: %+v", cr.Status)
					}
					gate := 0
					c.processTask(ctx, cr, task, &gate, time.Now())
					if task.State != gpuv1alpha1.TaskStatePending || task.CurrentCycle != 1 || task.CompletedRounds != round+1 ||
						task.Attempts != 0 || task.PodName != "" || gate != 0 {
						t.Fatalf("the next reconcile should enter the next round without advancing the cycle or consuming budget: %+v", task)
					}
				}
			}
			if createdCount != 6 || cr.Status.Phase != gpuv1alpha1.PhaseFailed || !crSettled(cr) {
				t.Fatalf("each round has its own budget; the cycle ends only after all rounds exhaust theirs: created=%d status=%+v", createdCount, cr.Status)
			}
			before := task.DeepCopy()
			gate := 0
			c.processTask(ctx, cr, task, &gate, time.Now())
			if !reflect.DeepEqual(before, task) || gate != 0 {
				t.Fatalf("all rounds are complete; no more Pods may be created: %+v", task)
			}
			assertPodCount(t, c, cr.Namespace, 0)
		})
	}
}

// Pod execution failures may retry; any valid detection result completes the retry, and Pods are cleaned up only after status is persisted.
func TestReconcileExecutionRetryThenVerdict(t *testing.T) {
	for _, verdict := range []string{detectionStatusPass, detectionStatusFail, detectionStatusError} {
		t.Run(verdict, func(t *testing.T) {
			ctx := context.Background()
			cr := testCR("default", "retry-pass", gpuv1alpha1.ActiveMonitoringSpec{
				Concurrency: 1, Schedule: gpuv1alpha1.SchedulePolicy{TotalCycles: 1},
			})
			cr.Status.Phase = gpuv1alpha1.PhaseRunning
			cr.Status.Queue = []gpuv1alpha1.TaskStatus{{ID: "n1-gpu-0", NodeName: "n1", CurrentCycle: 1}}
			pod := mustPodFromBuild(t, cr, cr.Status.Queue[0])
			pod.Status = v1.PodStatus{Phase: v1.PodFailed, Reason: "ExecutionFailed"}
			recordTaskPod(&cr.Status.Queue[0], pod.Name, time.Now().Truncate(time.Second))
			cr.Status.Queue[0].State = gpuv1alpha1.TaskStateRunning
			c, cancel := newTestController(t, cr, pod)
			defer cancel()
			fake := c.config.DynamicClient.(*dynamicfake.FakeDynamicClient)
			var statusCalls int32
			fake.PrependReactor("update", "activemonitorings", func(action k8stesting.Action) (bool, runtime.Object, error) {
				if action.GetSubresource() == "status" && atomic.AddInt32(&statusCalls, 1) == 1 {
					return true, nil, apierrors.NewInternalError(fmt.Errorf("injected persist failure"))
				}
				return false, nil, nil
			})
			key := cr.Namespace + "/" + cr.Name
			if err := c.reconcile(ctx, key); err == nil {
				t.Fatal("the first status write failure should be returned")
			}
			assertPodExists(t, c, cr.Namespace, pod.Name)
			if got := c.getCRFromAPI(t, cr.Namespace, cr.Name); !reflect.DeepEqual(got.Status.Queue, cr.Status.Queue) {
				t.Fatalf("a persistence failure must not advance the task: %+v", got.Status)
			}
			if err := c.reconcile(ctx, key); err != nil {
				t.Fatal(err)
			}
			got := c.getCRFromAPI(t, cr.Namespace, cr.Name)
			task := got.Status.Queue[0]
			wantErrors := []string{"round 1 attempt 1: pod failed, ExecutionFailed"}
			if task.State != gpuv1alpha1.TaskStatePending || task.Attempts != 1 || task.CompletedRounds != 0 ||
				task.PodName != "" || task.EndTime != nil || task.Result == nil || !reflect.DeepEqual(task.Result.Errors, wantErrors) ||
				got.Status.Phase != gpuv1alpha1.PhaseRunning || got.Status.FailedTasks != 0 {
				t.Fatalf("record the error once and wait to retry the same round: %+v", got.Status)
			}
			assertPodGone(t, c, cr.Namespace, pod.Name)
			// Wait for the informer to converge; repeated adoption of old terminal Pods is outside this test's scope.
			if err := wait.PollUntilContextTimeout(ctx, time.Millisecond, time.Second, true, func(context.Context) (bool, error) {
				cached := c.getCR(key)
				return cached != nil && reflect.DeepEqual(cached.Status, got.Status) && c.getPod(cr.Namespace, pod.Name) == nil, nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := c.reconcile(ctx, key); err != nil {
				t.Fatal(err)
			}
			got = c.getCRFromAPI(t, cr.Namespace, cr.Name)
			task = got.Status.Queue[0]
			if task.Attempts != 2 || task.PodName == "" || task.PodName == pod.Name || task.CompletedRounds != 0 || task.CurrentCycle != 1 {
				t.Fatalf("did not create the second attempt in the same round: %+v", task)
			}
			assertPodCount(t, c, cr.Namespace, 1)
			replacement, err := c.config.DynamicClient.Resource(podGVR).Namespace(cr.Namespace).Get(ctx, task.PodName, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if replacement.GetLabels()[LabelActiveMonitoringAttempt] != "2" || replacement.GetLabels()[LabelActiveMonitoringRound] != roundKey(1, 0) {
				t.Fatalf("incorrect round or attempt labels on the replacement Pod: %v", replacement.GetLabels())
			}
			completed := mustUnstructured(t, succeededPod(cr.Namespace, task.PodName, cr.Name, time.Now(),
				fmt.Sprintf(`{"status":%q,"message":"diagnostic failed"}`, verdict)))
			replacement.Object["status"] = completed.Object["status"]
			if _, err := c.config.DynamicClient.Resource(podGVR).Namespace(cr.Namespace).UpdateStatus(ctx, replacement, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if err := wait.PollUntilContextTimeout(ctx, time.Millisecond, time.Second, true, func(context.Context) (bool, error) {
				cached := c.getCR(key)
				return cached != nil && reflect.DeepEqual(cached.Status, got.Status) && podPhase(c.getPod(cr.Namespace, task.PodName)) == v1.PodSucceeded, nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := c.reconcile(ctx, key); err != nil {
				t.Fatal(err)
			}
			got = c.getCRFromAPI(t, cr.Namespace, cr.Name)
			task = got.Status.Queue[0]
			if verdict != detectionStatusPass {
				wantErrors = append(wantErrors, "round 1 attempt 2: detection "+verdict+", diagnostic failed")
			}
			if task.State != gpuv1alpha1.TaskStateCompleted || task.Attempts != 0 || task.CompletedRounds != 1 ||
				task.Result == nil || task.Result.Passed != (verdict == detectionStatusPass) || task.Result.Duration != 120 ||
				!reflect.DeepEqual(task.Result.Errors, wantErrors) || got.Status.Phase != gpuv1alpha1.PhaseCompleted ||
				got.Status.CompletedTasks != 1 || got.Status.FailedTasks != 0 {
				t.Fatalf("a valid retry result should complete the task while retaining failure history: %+v", got.Status)
			}
			assertPodCount(t, c, cr.Namespace, 0)
		})
	}
}

// Every execution failure path ends only the current round when its budget is exhausted; remaining rounds resume from persisted state.
func TestReconcileExecutionBudgetExhausted(t *testing.T) {
	cases := []struct {
		name, state, message string
		phase                v1.PodPhase
	}{
		{"budget exhausted when resuming Pending", gpuv1alpha1.TaskStatePending, "", ""},
		{"budget exhausted when resuming Preempted", gpuv1alpha1.TaskStatePreempted, "", ""},
		{"PodFailed", gpuv1alpha1.TaskStateRunning, "", v1.PodFailed},
		{"PodFailed takes precedence over a valid detection result", gpuv1alpha1.TaskStateRunning, `{"status":"FAIL"}`, v1.PodFailed},
		{"detection message parsing error", gpuv1alpha1.TaskStateRunning, "invalid json", v1.PodSucceeded},
		{"missing detection message", gpuv1alpha1.TaskStateRunning, "", v1.PodSucceeded},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cr := testCR("default", "exhausted", gpuv1alpha1.ActiveMonitoringSpec{
				Schedule: gpuv1alpha1.SchedulePolicy{RoundsPerCycle: 2, TotalCycles: 1},
			})
			cr.Status.Phase = gpuv1alpha1.PhaseRunning
			cr.Status.Queue = []gpuv1alpha1.TaskStatus{{ID: "n1-gpu-0", NodeName: "n1", CurrentCycle: 1,
				State: tc.state, Attempts: defaultMaxRetries}}
			objects := []runtime.Object{cr}
			if tc.phase != "" {
				pod := succeededPod(cr.Namespace, "p1", cr.Name, time.Now(), tc.message)
				pod.Status.Phase = tc.phase
				cr.Status.Queue[0].PodName = pod.Name
				objects = append(objects, pod)
			}
			c, cancel := newTestController(t, objects...)
			defer cancel()
			if err := c.reconcile(context.Background(), cr.Namespace+"/"+cr.Name); err != nil {
				t.Fatal(err)
			}
			got := c.getCRFromAPI(t, cr.Namespace, cr.Name)
			task := got.Status.Queue[0]
			if task.State != gpuv1alpha1.TaskStateFailed || task.CompletedRounds != 1 || task.Attempts != 0 ||
				task.PodName != "" || task.EndTime == nil || task.Result == nil || task.Result.Passed ||
				len(task.Result.Errors) != 1 || !strings.Contains(task.Result.Errors[0], "round 1 attempt 3:") ||
				!strings.Contains(task.Result.Errors[0], "retry budget exhausted") ||
				got.Status.Phase != gpuv1alpha1.PhaseRunning || got.Status.FailedTasks != 1 || crSettled(got) {
				t.Fatalf("execution budget exhaustion should persist Failed for this round without ending the cycle: %+v", got.Status)
			}
			assertPodCount(t, c, cr.Namespace, 0)
			// After restart, restore Pending first, then create the next round's Pod with an independent budget.
			restarted, stop := newTestController(t, got)
			defer stop()
			ctx := context.Background()
			key := cr.Namespace + "/" + cr.Name
			if err := restarted.reconcile(ctx, key); err != nil {
				t.Fatal(err)
			}
			next := restarted.getCRFromAPI(t, cr.Namespace, cr.Name)
			if next.Status.Queue[0].State != gpuv1alpha1.TaskStatePending || next.Status.FailedTasks != 0 || next.Status.PendingTasks != 1 {
				t.Fatalf("should resume in Pending for the next round: %+v", next.Status)
			}
			if err := wait.PollUntilContextTimeout(ctx, time.Millisecond, time.Second, true, func(context.Context) (bool, error) {
				cached := restarted.getCR(key)
				return cached != nil && reflect.DeepEqual(cached.Status, next.Status), nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := restarted.reconcile(ctx, key); err != nil {
				t.Fatal(err)
			}
			next = restarted.getCRFromAPI(t, cr.Namespace, cr.Name)
			nextTask := next.Status.Queue[0]
			if nextTask.CurrentCycle != 1 || nextTask.CompletedRounds != 1 || nextTask.Attempts != 1 || nextTask.PodName == "" ||
				!reflect.DeepEqual(nextTask.Result, task.Result) {
				t.Fatalf("the next round should count attempts independently and retain earlier errors: %+v", nextTask)
			}
			pod, err := restarted.config.DynamicClient.Resource(podGVR).Namespace(cr.Namespace).Get(ctx, nextTask.PodName, metav1.GetOptions{})
			if err != nil || pod.GetLabels()[LabelActiveMonitoringAttempt] != "1" || pod.GetLabels()[LabelActiveMonitoringRound] != roundKey(1, 1) {
				t.Fatalf("incorrect Pod labels for the next round: pod=%v err=%v", pod, err)
			}
			assertPodCount(t, restarted, cr.Namespace, 1)
		})
	}
}

// attempts tracks the current round's active budget; omitempty omits it after reset at round completion, while errors retain historical attempt numbers.
func TestTaskAttemptsOmittedAfterRound(t *testing.T) {
	for _, exhausted := range []bool{false, true} {
		t.Run(fmt.Sprintf("executionExhausted=%t", exhausted), func(t *testing.T) {
			cr := testCR("default", "attempts", gpuv1alpha1.ActiveMonitoringSpec{})
			cr.Status.Queue = []gpuv1alpha1.TaskStatus{{CurrentCycle: 1, State: gpuv1alpha1.TaskStateRunning, Attempts: 1, PodName: "pod"}}
			task := &cr.Status.Queue[0]
			if exhausted {
				task.Attempts = defaultMaxRetries
			}
			attempt := task.Attempts
			before, err := fromActiveMonitoring(cr)
			if err != nil {
				t.Fatal(err)
			}
			queue, _, _ := unstructured.NestedSlice(before.Object, "status", "queue")
			if queue[0].(map[string]interface{})["attempts"] != int64(attempt) {
				t.Fatalf("an active round should include the actual attempts count: %v", queue)
			}
			c := &Controller{}
			if exhausted {
				c.handlePodFailed(cr, task, time.Now(), defaultMaxRetries, "pod failed")
			} else {
				pod := succeededPod(cr.Namespace, "pod", cr.Name, time.Now(), `{"status":"FAIL"}`)
				c.handleSucceeded(cr, task, mustUnstructured(t, pod), time.Now(), defaultMaxRetries)
			}
			after, err := fromActiveMonitoring(cr)
			if err != nil {
				t.Fatal(err)
			}
			queue, _, _ = unstructured.NestedSlice(after.Object, "status", "queue")
			if _, exists := queue[0].(map[string]interface{})["attempts"]; exists || task.Attempts != 0 {
				t.Fatalf("attempts should be zero and omitted after round completion: %v", queue)
			}
			if task.Result == nil || len(task.Result.Errors) != 1 ||
				!strings.Contains(task.Result.Errors[0], fmt.Sprintf("round 1 attempt %d:", attempt)) {
				t.Fatalf("resetting the counter must not lose historical attempt numbers: %+v", task.Result)
			}
		})
	}
}

func TestCreateFailureVisibleWithoutConsumingAttempt(t *testing.T) {
	for _, state := range []string{gpuv1alpha1.TaskStatePending, gpuv1alpha1.TaskStatePreempted} {
		t.Run(state, func(t *testing.T) {
			cr := testCR("default", "create-error", gpuv1alpha1.ActiveMonitoringSpec{})
			cr.Status.Queue = []gpuv1alpha1.TaskStatus{{ID: "n1-gpu-0", NodeName: "n1", CurrentCycle: 1, State: state, Attempts: 1}}
			c, cancel := newTestController(t, cr)
			defer cancel()
			fake := c.config.DynamicClient.(*dynamicfake.FakeDynamicClient)
			fail := true
			fake.PrependReactor("create", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
				if fail {
					return true, nil, apierrors.NewForbidden(podGVR.GroupResource(), "", fmt.Errorf("quota exhausted"))
				}
				return false, nil, nil
			})
			if err := c.reconcile(context.Background(), "default/create-error"); err != nil {
				t.Fatal(err)
			}
			task := &c.getCRFromAPI(t, cr.Namespace, cr.Name).Status.Queue[0]
			if task.Attempts != 1 || task.PodName != "" || task.State != state || task.Result == nil ||
				len(task.Result.Errors) != 1 || !strings.Contains(task.Result.Errors[0], "round 1 attempt 2: create pod failed") ||
				!strings.Contains(task.Result.Errors[0], "quota exhausted") {
				t.Fatalf("CR is missing the creation failure reason or incorrectly consumed budget: %+v", task)
			}
			gate := 0
			c.processTask(context.Background(), cr, task, &gate, time.Now())
			if len(task.Result.Errors) != 1 || task.Attempts != 1 || gate != 0 {
				t.Fatalf("the same creation error must not be appended twice or occupy a concurrency slot: %+v", task)
			}
			fail = false
			c.processTask(context.Background(), cr, task, &gate, time.Now())
			if task.State != gpuv1alpha1.TaskStatePending || task.PodName == "" || task.Attempts != 2 {
				t.Fatalf("successful creation after recovery should increment attempts and transition to Pending: %+v", task)
			}
		})
	}
}

// If rebuilding succeeds but status persistence fails, adopt the replacement Pod after restart instead of creating another.
func TestPreemptedReplacementAdoptedAfterStatusFailure(t *testing.T) {
	cr := testCR("default", "recover", gpuv1alpha1.ActiveMonitoringSpec{Concurrency: 1})
	cr.Status.Queue = []gpuv1alpha1.TaskStatus{*preemptedTask("", 1)}
	c, cancel := newTestController(t, cr)
	defer cancel()
	fake := c.config.DynamicClient.(*dynamicfake.FakeDynamicClient)
	fake.PrependReactor("update", "activemonitorings", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewInternalError(fmt.Errorf("status unavailable"))
	})
	if err := c.reconcile(context.Background(), "default/recover"); err == nil {
		t.Fatal("expected status write failure")
	}
	persisted := c.getCRFromAPI(t, cr.Namespace, cr.Name)
	if persisted.Status.Queue[0].State != gpuv1alpha1.TaskStatePreempted || persisted.Status.Queue[0].PodName != "" || persisted.Status.Queue[0].Attempts != 1 {
		t.Fatalf("the API must not contain unpersisted state after a write failure: %+v", persisted.Status)
	}
	pods, err := c.config.DynamicClient.Resource(podGVR).Namespace(cr.Namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil || len(pods.Items) != 1 {
		t.Fatalf("the replacement Pod should already exist: pods=%v err=%v", pods, err)
	}
	replacement := pods.Items[0].DeepCopy()
	if err := unstructured.SetNestedField(replacement.Object, string(v1.PodRunning), "status", "phase"); err != nil {
		t.Fatal(err)
	}
	cancel()
	restarted, stop := newTestController(t, persisted, replacement)
	defer stop()
	if err := restarted.reconcile(context.Background(), "default/recover"); err != nil {
		t.Fatal(err)
	}
	got := restarted.getCRFromAPI(t, cr.Namespace, cr.Name)
	task := &got.Status.Queue[0]
	if task.PodName != replacement.GetName() || task.State != gpuv1alpha1.TaskStatePending || task.Attempts != 2 {
		t.Fatalf("restart did not adopt the existing replacement Pod: %+v", task)
	}
	assertPodCount(t, restarted, cr.Namespace, 1)
	gate := restarted.countPods(got)
	restarted.processTask(context.Background(), got, task, &gate, time.Now())
	if task.State != gpuv1alpha1.TaskStateRunning || task.Attempts != 2 {
		t.Fatalf("after adoption, follow the actual Pod state without counting the attempt again: %+v", task)
	}
}

func TestTaskRunningRequiresPodRunning(t *testing.T) {
	cr := testCR("default", "phase", gpuv1alpha1.ActiveMonitoringSpec{})
	pod := testPod(cr.Namespace, "replacement", cr.Name, v1.PodPending, time.Now())
	pod.Spec.NodeName = "n1"
	pod.Status.Conditions = []v1.PodCondition{{Type: v1.PodScheduled, Status: v1.ConditionTrue}}
	c, cancel := newTestController(t, cr, pod)
	defer cancel()
	task := preemptedTask(pod.Name, 2)
	gate := 1
	c.processPreempted(context.Background(), cr, task, &gate, time.Now(), defaultMaxRetries)
	if task.State != gpuv1alpha1.TaskStatePending {
		t.Fatalf("a bound Pod that is not yet Running should leave the task Pending: %+v", task)
	}
	pod.Status.Phase = v1.PodRunning
	if err := c.podInformer.GetStore().Update(mustUnstructured(t, pod)); err != nil {
		t.Fatal(err)
	}
	c.processTask(context.Background(), cr, task, &gate, time.Now())
	if task.State != gpuv1alpha1.TaskStateRunning || task.Attempts != 2 {
		t.Fatalf("incorrect task state after the Pod becomes Running: %+v", task)
	}
}

func TestUnknownPodRetainsReferenceAndRecovers(t *testing.T) {
	for _, state := range []string{gpuv1alpha1.TaskStatePending, gpuv1alpha1.TaskStateRunning} {
		t.Run(state, func(t *testing.T) {
			cr := testCR("default", "unknown", gpuv1alpha1.ActiveMonitoringSpec{Concurrency: 1})
			cr.Status.Queue = []gpuv1alpha1.TaskStatus{{ID: "n1-gpu-0", NodeName: "n1", CurrentCycle: 1,
				State: state, PodName: "unknown-pod", Attempts: 2}}
			pod := testPod(cr.Namespace, "unknown-pod", cr.Name, v1.PodUnknown, time.Now().Add(-time.Hour))
			pod.Status.Reason, pod.Status.Message = "NodeLost", "node is unreachable"
			c, cancel := newTestController(t, cr, pod)
			defer cancel()
			if err := c.reconcile(context.Background(), "default/unknown"); err != nil {
				t.Fatal(err)
			}
			task := &c.getCRFromAPI(t, cr.Namespace, cr.Name).Status.Queue[0]
			before := task.DeepCopy()
			gate := c.countPods(cr)
			if gate != 1 || concurrencyAllows(gate, cr) {
				t.Fatal("Unknown Pods must occupy a concurrency slot even for prolonged periods")
			}
			c.processTask(context.Background(), cr, task, &gate, time.Now())
			if !reflect.DeepEqual(before, task) || task.PodName != pod.Name || task.State != state || task.Attempts != 2 ||
				task.Result == nil || len(task.Result.Errors) != 1 || !strings.Contains(task.Result.Errors[0], "NodeLost") {
				t.Fatalf("Unknown should preserve the Pod reference and task state and record the error only once: %+v", task)
			}
			assertPodCount(t, c, cr.Namespace, 1)
			pod.Status.Phase = v1.PodRunning
			if err := c.podInformer.GetStore().Update(mustUnstructured(t, pod)); err != nil {
				t.Fatal(err)
			}
			c.processTask(context.Background(), cr, task, &gate, time.Now())
			if task.State != gpuv1alpha1.TaskStateRunning || task.Attempts != 2 {
				t.Fatalf("recovery from Unknown must not create a new Pod or consume budget: %+v", task)
			}
			completed := succeededPod(cr.Namespace, pod.Name, cr.Name, time.Now(), `{"status":"PASS","message":""}`)
			if err := c.podInformer.GetStore().Update(mustUnstructured(t, completed)); err != nil {
				t.Fatal(err)
			}
			c.processTask(context.Background(), cr, task, &gate, time.Now())
			if task.State != gpuv1alpha1.TaskStateCompleted || task.PodName != "" || len(task.Result.Errors) != 1 {
				t.Fatalf("the result did not converge normally after recovery from Unknown: %+v", task)
			}
		})
	}
}

func TestStatusConflictDoesNotReplayOldResult(t *testing.T) {
	latest := testCR("default", "conflict", gpuv1alpha1.ActiveMonitoringSpec{Schedule: gpuv1alpha1.SchedulePolicy{TotalCycles: 1}})
	latest.ResourceVersion = "2"
	latest.Status.Queue = []gpuv1alpha1.TaskStatus{{ID: "n1-gpu-0", NodeName: "n1", CurrentCycle: 1,
		State: gpuv1alpha1.TaskStateCompleted, CompletedRounds: 1,
		Result: &gpuv1alpha1.TaskResult{Passed: true, Duration: 120}}}
	updatePhase(latest)
	recountStatus(latest)
	residual := testPod(latest.Namespace, "residual", latest.Name, v1.PodSucceeded, time.Now())
	c, cancel := newTestController(t, latest, residual)
	t.Cleanup(c.queue.ShutDown)
	cancel()
	c.crFactory.Shutdown()
	c.nodeFactory.Shutdown()
	c.podFactory.Shutdown()
	stale := latest.DeepCopy()
	stale.ResourceVersion = "1"
	stale.Status.Phase = gpuv1alpha1.PhaseRunning
	stale.Status.Queue[0].State = gpuv1alpha1.TaskStateRunning
	stale.Status.Queue[0].PodName = "already-gone"
	stale.Status.Queue[0].CompletedRounds = 0
	stale.Status.Queue[0].Result = nil
	if err := c.crInformer.GetStore().Update(mustUnstructured(t, stale)); err != nil {
		t.Fatal(err)
	}
	fake := c.config.DynamicClient.(*dynamicfake.FakeDynamicClient)
	updates := 0
	fake.PrependReactor("update", "activemonitorings", func(action k8stesting.Action) (bool, runtime.Object, error) {
		updates++
		u := action.(k8stesting.UpdateAction).GetObject().(*unstructured.Unstructured)
		if u.GetResourceVersion() != latest.ResourceVersion {
			return true, nil, apierrors.NewConflict(activemonitoringGVR.GroupResource(), latest.Name, fmt.Errorf("stale version"))
		}
		return false, nil, nil
	})
	if err := c.reconcile(context.Background(), "default/conflict"); !apierrors.IsConflict(err) {
		t.Fatalf("conflicts should return to the workqueue for recomputation: %v", err)
	}
	if updates != 1 {
		t.Fatalf("stale status must not be replayed with a new RV, update count=%d", updates)
	}
	assertPodExists(t, c, latest.Namespace, residual.Name)
	if got := c.getCRFromAPI(t, latest.Namespace, latest.Name); !reflect.DeepEqual(got.Status, latest.Status) {
		t.Fatalf("persisted result was overwritten by stale state: %+v", got.Status)
	}
	// While the cache is stale, the real workqueue retries with backoff; each reconcile still attempts only one write.
	key := latest.Namespace + "/" + latest.Name
	c.queue.Add(key)
	if !c.processNext(context.Background()) || c.queue.NumRequeues(key) != 1 || updates != 2 {
		t.Fatalf("conflicts should trigger workqueue backoff and recomputation: updates=%d requeues=%d", updates, c.queue.NumRequeues(key))
	}
	assertPodExists(t, c, latest.Namespace, residual.Name)
	if got := c.getCRFromAPI(t, latest.Namespace, latest.Name); !reflect.DeepEqual(got.Status, latest.Status) {
		t.Fatalf("workqueue retries must not overwrite persisted results: %+v", got.Status)
	}
	// Once the cache converges, recompute from the latest result without rebuilding completed tasks.
	if err := c.crInformer.GetStore().Update(mustUnstructured(t, latest)); err != nil {
		t.Fatal(err)
	}
	c.queue.Add(key)
	if !c.processNext(context.Background()) || c.queue.NumRequeues(key) != 0 {
		t.Fatal("successfully processing the latest state should clear the workqueue backoff count")
	}
	if updates != 2 {
		t.Fatal("completed status must not be written again")
	}
	assertPodGone(t, c, latest.Namespace, residual.Name)
}
