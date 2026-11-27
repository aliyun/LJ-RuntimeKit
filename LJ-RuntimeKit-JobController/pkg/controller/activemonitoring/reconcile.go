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
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/klog/v2"

	gpuv1alpha1 "github.com/lj-runtimekit/job-controller/pkg/apis/gpu/v1alpha1"
)

func (c *Controller) reconcile(ctx context.Context, key string) error {
	cr := c.getCR(key)
	if cr == nil {
		return nil
	}
	if cr.DeletionTimestamp != nil {
		return nil
	}

	now := time.Now()

	if len(cr.Status.Queue) == 0 {
		return c.initQueue(ctx, cr)
	}
	previous := cr.Status.DeepCopy()
	if !crSettled(cr) {
		gateLive := c.countPods(cr)
		for i := range cr.Status.Queue {
			c.processTask(ctx, cr, &cr.Status.Queue[i], &gateLive, now)
		}
	}

	updatePhase(cr)
	recountStatus(cr)
	if !equality.Semantic.DeepEqual(previous, &cr.Status) {
		if err := c.updateStatus(ctx, cr); err != nil {
			return err
		}
	}

	c.sweepResidualPods(ctx, cr)
	return nil
}

// initQueue seeds the Queue on first sight of a CR (design §4.3 Step 1 / §3.4).
func (c *Controller) initQueue(ctx context.Context, cr *gpuv1alpha1.ActiveMonitoring) error {
	queue, err := c.buildQueue(cr)
	if err != nil {
		return err
	}
	if len(queue) == 0 {
		// No node matched (or matched nodes have no GPU): stay Pending and let the
		// ticker retry so the queue auto-initialises once nodes/GPUs appear.
		cr.Status.Phase = gpuv1alpha1.PhasePending
		setCondition(cr, gpuv1alpha1.ConditionQueueInitialized, metav1.ConditionFalse,
			"NoNodeMatched", "no runtimekit-enabled GPU node matched the selection policy")
		return c.updateStatus(ctx, cr)
	}
	cr.Status.Queue = queue
	cr.Status.Phase = gpuv1alpha1.PhasePending
	setCondition(cr, gpuv1alpha1.ConditionQueueInitialized, metav1.ConditionTrue,
		"NodesMatched", fmt.Sprintf("matched %d nodes, %d gpus", countDistinctNodes(queue), len(queue)))
	recountStatus(cr)
	return c.updateStatus(ctx, cr)
}

// processTask advances a single Task through its state machine.
func (c *Controller) processTask(ctx context.Context, cr *gpuv1alpha1.ActiveMonitoring, task *gpuv1alpha1.TaskStatus, gateLive *int, now time.Time) {
	maxRetries := defaultMaxRetries

	switch task.State {
	case gpuv1alpha1.TaskStatePending, "":
		c.processPending(ctx, cr, task, gateLive, now, maxRetries)
	case gpuv1alpha1.TaskStateRunning:
		c.processRunning(cr, task, now, maxRetries)
	case gpuv1alpha1.TaskStatePreempted:
		c.processPreempted(ctx, cr, task, gateLive, now, maxRetries)
	case gpuv1alpha1.TaskStateCompleted, gpuv1alpha1.TaskStateFailed:
		if task.State == gpuv1alpha1.TaskStateFailed && task.PodName == "" &&
			task.CompletedRounds < orDefaultInt(cr.Spec.Schedule.RoundsPerCycle, defaultRoundsPerCycle) {
			task.State = gpuv1alpha1.TaskStatePending
			return
		}
		advanceTaskCycle(cr, task, now)
	}
}

// processPending handles both Pending sub-cases distinguished by PodName:
// empty → create a Pod once the gate allows; non-empty → drive it to Running.
func (c *Controller) processPending(ctx context.Context, cr *gpuv1alpha1.ActiveMonitoring, task *gpuv1alpha1.TaskStatus, gateLive *int, now time.Time, maxRetries int) {
	if task.PodName == "" {
		if task.Attempts >= maxRetries {
			annotateFailedRun(task, "retry budget exhausted", nil)
			c.failRound(cr, task, now)
			return
		}
		if existing := c.findTaskPod(cr, task); existing != nil {
			recordTaskPod(task, existing.GetName(), now)
			klog.InfoS("adopted existing detection pod for task", "cr", cr.Namespace+"/"+cr.Name, "task", task.ID, "pod", task.PodName)
			return
		}
		if !concurrencyAllows(*gateLive, cr) {
			return
		}
		if err := c.createTaskPod(ctx, cr, task, now); err != nil {
			appendTaskErrorOnce(task, fmt.Sprintf("round %d attempt %d: create pod failed: %v",
				task.CompletedRounds+1, task.Attempts+1, err))
			klog.ErrorS(err, "create detection pod failed", "cr", cr.Namespace+"/"+cr.Name, "task", task.ID)
			return
		}
		*gateLive++
		return
	}
	c.drivePodPhase(cr, task, now, maxRetries)
}

// processRunning watches a K8s-Running detection Pod to completion or preemption.
func (c *Controller) processRunning(cr *gpuv1alpha1.ActiveMonitoring, task *gpuv1alpha1.TaskStatus, now time.Time, maxRetries int) {
	c.drivePodPhase(cr, task, now, maxRetries)
}

// drivePodPhase watches the Task's existing detection Pod and drives the Task
// state from the Pod phase. The Task keeps its current state (Pending or Preempted)
// until the Pod is K8s-Running; a vanished or terminating Pod is handled as a DP preemption.
func (c *Controller) drivePodPhase(cr *gpuv1alpha1.ActiveMonitoring, task *gpuv1alpha1.TaskStatus, now time.Time, maxRetries int) {
	pod := c.getPod(cr.Namespace, task.PodName)
	if pod == nil || pod.GetDeletionTimestamp() != nil {
		c.handlePreemptedPod(task, now)
		return
	}
	switch podPhase(pod) {
	case v1.PodPending:
		task.State = gpuv1alpha1.TaskStatePending
	case v1.PodRunning:
		task.State = gpuv1alpha1.TaskStateRunning
	case v1.PodSucceeded:
		c.handleSucceeded(cr, task, pod, now, maxRetries)
	case v1.PodFailed:
		c.handlePodFailed(cr, task, now, maxRetries, "pod failed, "+podFailureReason(pod))
	case v1.PodUnknown:
		reason, _, _ := unstructured.NestedString(pod.Object, "status", "reason")
		message, _, _ := unstructured.NestedString(pod.Object, "status", "message")
		appendTaskErrorOnce(task, fmt.Sprintf("round %d attempt %d: pod %s Unknown, reason=%s, message=%s; waiting for recovery",
			task.CompletedRounds+1, task.Attempts, task.PodName, reason, message))
	}
}

func (c *Controller) processPreempted(ctx context.Context, cr *gpuv1alpha1.ActiveMonitoring, task *gpuv1alpha1.TaskStatus, gateLive *int, now time.Time, maxRetries int) {
	if task.PodName != "" {
		c.drivePodPhase(cr, task, now, maxRetries)
		return
	}
	if task.Attempts >= maxRetries {
		annotateFailedRun(task, "preempted by business pod, retry budget exhausted", nil)
		c.failRound(cr, task, now)
		return
	}
	c.processPending(ctx, cr, task, gateLive, now, maxRetries)
}

func (c *Controller) handlePreemptedPod(task *gpuv1alpha1.TaskStatus, now time.Time) {
	task.State = gpuv1alpha1.TaskStatePreempted
	task.LastPreemptTime = &metav1.Time{Time: now}
	appendTaskError(task, fmt.Sprintf("round %d attempt %d: preempted by business pod",
		task.CompletedRounds+1, task.Attempts))
	task.PodName = ""
}

func (c *Controller) handleSucceeded(cr *gpuv1alpha1.ActiveMonitoring, task *gpuv1alpha1.TaskStatus, pod *unstructured.Unstructured, now time.Time, maxRetries int) {
	verdict, err := parseTerminationMessage(pod)
	if err != nil {
		c.handlePodFailed(cr, task, now, maxRetries, fmt.Sprintf("parse termination message failed: %v", err))
		return
	}
	if verdict.Status != detectionStatusPass {
		annotateFailedRun(task, "detection "+verdict.Status, verdict.Result.Errors)
	}
	c.finishRound(cr, task, verdict.Result, now)
}

func (c *Controller) createTaskPod(ctx context.Context, cr *gpuv1alpha1.ActiveMonitoring, task *gpuv1alpha1.TaskStatus, now time.Time) error {
	pod, err := buildTestPod(cr, *task, c.detectionConfig())
	if err != nil {
		return err
	}
	created, err := c.createPod(ctx, pod)
	if err != nil {
		return err
	}
	recordTaskPod(task, created.GetName(), now)
	return nil
}

func recordTaskPod(task *gpuv1alpha1.TaskStatus, name string, now time.Time) {
	if task.Attempts == 0 {
		task.StartTime = &metav1.Time{Time: now}
	}
	task.Attempts++
	task.PodName = name
	task.State = gpuv1alpha1.TaskStatePending
}

func roundKey(cycle, completedRounds int) string {
	return fmt.Sprintf("%d-%d", orDefaultInt(cycle, 1), completedRounds)
}

func (c *Controller) findTaskPod(cr *gpuv1alpha1.ActiveMonitoring, task *gpuv1alpha1.TaskStatus) *unstructured.Unstructured {
	wantRound := roundKey(task.CurrentCycle, task.CompletedRounds)
	var found *unstructured.Unstructured
	var foundTS time.Time
	for _, u := range c.listCRPods(cr) {
		labels := u.GetLabels()
		if labels[LabelActiveMonitoringTask] != task.ID || labels[LabelActiveMonitoringRound] != wantRound {
			continue
		}
		if ts := u.GetCreationTimestamp(); found == nil || ts.Time.Before(foundTS) {
			found, foundTS = u, ts.Time
		}
	}
	return found
}

func (c *Controller) finishRound(cr *gpuv1alpha1.ActiveMonitoring, task *gpuv1alpha1.TaskStatus, latest *gpuv1alpha1.TaskResult, now time.Time) {
	task.CompletedRounds++
	mergeResult(task, latest)
	task.EndTime = &metav1.Time{Time: now}
	task.PodName = ""
	task.Attempts = 0
	if task.CompletedRounds < orDefaultInt(cr.Spec.Schedule.RoundsPerCycle, defaultRoundsPerCycle) {
		task.State = gpuv1alpha1.TaskStatePending
	} else {
		task.State = gpuv1alpha1.TaskStateCompleted
	}
}

func (c *Controller) failRound(cr *gpuv1alpha1.ActiveMonitoring, task *gpuv1alpha1.TaskStatus, now time.Time) {
	c.finishRound(cr, task, &gpuv1alpha1.TaskResult{Passed: false}, now)
	task.State = gpuv1alpha1.TaskStateFailed
}

func (c *Controller) handlePodFailed(cr *gpuv1alpha1.ActiveMonitoring, task *gpuv1alpha1.TaskStatus, now time.Time, maxRetries int, detail string) {
	if task.Attempts < maxRetries {
		annotateFailedRun(task, detail, nil)
		task.PodName = ""
		task.State = gpuv1alpha1.TaskStatePending
		return
	}
	annotateFailedRun(task, detail+", retry budget exhausted", nil)
	c.failRound(cr, task, now)
}

// countDistinctNodes returns the number of unique node names in a queue.
func countDistinctNodes(queue []gpuv1alpha1.TaskStatus) int {
	seen := make(map[string]struct{}, len(queue))
	for _, t := range queue {
		seen[t.NodeName] = struct{}{}
	}
	return len(seen)
}
