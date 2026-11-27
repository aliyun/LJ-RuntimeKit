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
	"fmt"
	"slices"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	gpuv1alpha1 "github.com/lj-runtimekit/job-controller/pkg/apis/gpu/v1alpha1"
)

// ---------------------------------------------------------------------------
// Generic normalization helpers (design §3.2 default values).
// ---------------------------------------------------------------------------

func orDefaultInt(i, def int) int {
	if i <= 0 {
		return def
	}
	return i
}

func parseDurationOr(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return def
	}
	return d
}

// ---------------------------------------------------------------------------
// concurrency gate (scheme a: timed-out Pending exempted, Pod NOT deleted).
// ---------------------------------------------------------------------------

func (c *Controller) countPods(cr *gpuv1alpha1.ActiveMonitoring) int {
	var gateLive int
	for _, u := range c.listCRPods(cr) {
		switch podPhase(u) {
		case v1.PodRunning, v1.PodUnknown:
			gateLive++
		case v1.PodPending:
			if !pendingTimedOut(u) {
				gateLive++
			}
		}
	}
	return gateLive
}

// pendingTimedOut reports whether a Pod has been K8s-Pending longer than the
// hardcoded pendingTimeout (5m).
func pendingTimedOut(pod *unstructured.Unstructured) bool {
	timeout := parseDurationOr(defaultPendingTimeout, 5*time.Minute)
	return time.Since(pod.GetCreationTimestamp().Time) > timeout
}

// concurrencyAllows applies the concurrency cap: gateLive < concurrency.
// A zero value means "unlimited".
func concurrencyAllows(gateLive int, cr *gpuv1alpha1.ActiveMonitoring) bool {
	if cr.Spec.Concurrency > 0 && gateLive >= cr.Spec.Concurrency {
		return false
	}
	return true
}

// podPhase reads status.phase from an unstructured Pod.
func podPhase(u *unstructured.Unstructured) v1.PodPhase {
	if u == nil {
		return ""
	}
	phase, _, _ := unstructured.NestedString(u.Object, "status", "phase")
	return v1.PodPhase(phase)
}

// ---------------------------------------------------------------------------
// Counting, cycle advance and Phase determination (design §4.10).
// ---------------------------------------------------------------------------

// recountStatus recomputes the aggregate counters from the Queue states.
func recountStatus(cr *gpuv1alpha1.ActiveMonitoring) {
	var completed, running, pending, preempted, failed int
	for _, t := range cr.Status.Queue {
		switch t.State {
		case gpuv1alpha1.TaskStateCompleted:
			completed++
		case gpuv1alpha1.TaskStateRunning:
			running++
		case gpuv1alpha1.TaskStatePending:
			pending++
		case gpuv1alpha1.TaskStatePreempted:
			preempted++
		case gpuv1alpha1.TaskStateFailed:
			failed++
		}
	}
	cr.Status.TotalTasks = len(cr.Status.Queue)
	cr.Status.CompletedTasks = completed
	cr.Status.RunningTasks = running
	cr.Status.PendingTasks = pending
	cr.Status.PreemptedTasks = preempted
	cr.Status.FailedTasks = failed
}

func taskCycleComplete(cr *gpuv1alpha1.ActiveMonitoring, task *gpuv1alpha1.TaskStatus) bool {
	if task.PodName != "" {
		return false
	}
	return (task.State == gpuv1alpha1.TaskStateFailed || task.State == gpuv1alpha1.TaskStateCompleted) &&
		task.CompletedRounds >= orDefaultInt(cr.Spec.Schedule.RoundsPerCycle, defaultRoundsPerCycle)
}

func taskHasNextCycle(cr *gpuv1alpha1.ActiveMonitoring, task *gpuv1alpha1.TaskStatus) bool {
	return parseDurationOr(cr.Spec.Schedule.Interval, 0) > 0 &&
		(cr.Spec.Schedule.TotalCycles == 0 || task.CurrentCycle < cr.Spec.Schedule.TotalCycles)
}

func advanceTaskCycle(cr *gpuv1alpha1.ActiveMonitoring, task *gpuv1alpha1.TaskStatus, now time.Time) bool {
	if !taskCycleComplete(cr, task) || !taskHasNextCycle(cr, task) {
		return false
	}
	if task.EndTime == nil || task.EndTime.IsZero() {
		task.EndTime = &metav1.Time{Time: now}
		return false
	}
	if now.Before(task.EndTime.Add(parseDurationOr(cr.Spec.Schedule.Interval, 0))) {
		return false
	}
	task.CurrentCycle++
	task.State = gpuv1alpha1.TaskStatePending
	task.Attempts = 0
	task.CompletedRounds = 0
	task.Result = nil
	task.PodName = ""
	task.StartTime = nil
	task.EndTime = nil
	task.LastPreemptTime = nil
	return true
}

func updatePhase(cr *gpuv1alpha1.ActiveMonitoring) {
	allDone, allPending := len(cr.Status.Queue) > 0, true
	hasFailed := false
	for i := range cr.Status.Queue {
		task := &cr.Status.Queue[i]
		allDone = allDone && taskCycleComplete(cr, task)
		allPending = allPending && (task.State == gpuv1alpha1.TaskStatePending || task.State == "") &&
			task.PodName == "" && task.Attempts == 0 && task.CompletedRounds == 0
		hasFailed = hasFailed || task.State == gpuv1alpha1.TaskStateFailed
	}
	switch {
	case allDone && hasFailed:
		cr.Status.Phase = gpuv1alpha1.PhaseFailed
	case allDone:
		cr.Status.Phase = gpuv1alpha1.PhaseCompleted
	case allPending:
		cr.Status.Phase = gpuv1alpha1.PhasePending
	default:
		cr.Status.Phase = gpuv1alpha1.PhaseRunning
	}
}

func isTerminalPhase(cr *gpuv1alpha1.ActiveMonitoring) bool {
	return cr.Status.Phase == gpuv1alpha1.PhaseCompleted || cr.Status.Phase == gpuv1alpha1.PhaseFailed
}

func crSettled(cr *gpuv1alpha1.ActiveMonitoring) bool {
	if !isTerminalPhase(cr) || len(cr.Status.Queue) == 0 {
		return false
	}
	for i := range cr.Status.Queue {
		task := &cr.Status.Queue[i]
		if !taskCycleComplete(cr, task) || taskHasNextCycle(cr, task) {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Condition / result helpers.
// ---------------------------------------------------------------------------

// setCondition upserts a status Condition by type.
func setCondition(cr *gpuv1alpha1.ActiveMonitoring, ctype string, status metav1.ConditionStatus, reason, message string) {
	now := metav1.Now()
	for i := range cr.Status.Conditions {
		if cr.Status.Conditions[i].Type == ctype {
			c := &cr.Status.Conditions[i]
			if c.Status != status {
				c.LastTransitionTime = now
			}
			c.Status = status
			c.Reason = reason
			c.Message = message
			return
		}
	}
	cr.Status.Conditions = append(cr.Status.Conditions, metav1.Condition{
		Type:               ctype,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: now,
	})
}

func appendTaskError(task *gpuv1alpha1.TaskStatus, msg string) {
	if task.Result == nil {
		task.Result = &gpuv1alpha1.TaskResult{}
	}
	task.Result.Errors = append(task.Result.Errors, msg)
	if len(task.Result.Errors) > maxTaskErrors {
		task.Result.Errors = task.Result.Errors[len(task.Result.Errors)-maxTaskErrors:]
	}
}

func appendTaskErrorOnce(task *gpuv1alpha1.TaskStatus, msg string) {
	if task.Result != nil {
		if slices.Contains(task.Result.Errors, msg) {
			return
		}
	}
	appendTaskError(task, msg)
}

func annotateFailedRun(task *gpuv1alpha1.TaskStatus, reason string, details []string) {
	round, attempt := task.CompletedRounds+1, task.Attempts
	if len(details) == 0 {
		appendTaskError(task, fmt.Sprintf("round %d attempt %d: %s", round, attempt, reason))
		return
	}
	for i, d := range details {
		details[i] = fmt.Sprintf("round %d attempt %d: %s, %s", round, attempt, reason, d)
	}
}

func mergeResult(task *gpuv1alpha1.TaskStatus, latest *gpuv1alpha1.TaskResult) {
	if latest == nil {
		return
	}
	if task.Result == nil {
		task.Result = &gpuv1alpha1.TaskResult{}
	}
	task.Result.Passed = latest.Passed
	task.Result.Duration = latest.Duration
	for _, e := range latest.Errors {
		appendTaskError(task, e)
	}
}
