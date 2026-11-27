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

package profiling

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	profilingv1alpha1 "github.com/lj-runtimekit/job-controller/api/v1alpha1"
	"github.com/lj-runtimekit/job-controller/pkg/collectorprotocol"
	"github.com/lj-runtimekit/job-controller/pkg/nodeagent"
)

// collectors may be stale only by Pods this job created, which never block it.
func (r *ProfilingJobReconciler) startTarget(ctx context.Context, job *profilingv1alpha1.ProfilingJob, cfg *nodeagent.Config, target *profilingv1alpha1.TargetStatus, collectors []corev1.Pod) (time.Duration, error) {
	// Creation may have succeeded before a status conflict or controller restart.
	if target.CollectorPodName != "" {
		existing := &corev1.Pod{}
		key := client.ObjectKey{Namespace: job.Spec.PersistentVolumeClaimNamespace, Name: target.CollectorPodName}
		if err := r.APIReader.Get(ctx, key, existing); err == nil {
			if !nodeagent.CollectorMatches(existing, job, target) {
				return r.collectorMismatch(cfg, target, key), nil
			}
			target.CollectorPodUID = string(existing.UID)
			now := metav1.NewTime(r.now())
			if !existing.CreationTimestamp.IsZero() {
				now = existing.CreationTimestamp
			}
			target.CollectorCreatedAt = &now
			target.Stage = profilingv1alpha1.StageCollecting
			return time.Millisecond, nil
		} else if !apierrors.IsNotFound(err) {
			return 0, err
		}
	}
	if blocker := collectorBlocker(collectors, job, target.TargetUID); blocker != "" {
		setTargetReason(target, "Queued", "waiting for collector "+blocker+" on the same target Pod")
		return queueRetryInterval, nil
	}
	pvc, allowed, reason, err := r.pvcReady(ctx, &job.Spec)
	if err != nil {
		var invalid *preflightError
		if errors.As(err, &invalid) {
			failTarget(target, invalid.reason, invalid.message)
			return 0, nil
		}
		return 0, err
	}
	if !allowed {
		setTargetReason(target, "PVCNotReady", reason)
		return waitRequeueInterval, nil
	}
	if err := validateAccessModes(pvc, job.Status.Targets); err != nil {
		failTarget(target, "PVCIncompatible", err.Error())
		return 0, nil
	}
	pod, reason, message, err := r.verifyTarget(ctx, target)
	if err != nil {
		return 0, err
	}
	if reason != "" {
		failTarget(target, reason, message)
		return 0, nil
	}
	if pod.Status.Phase != corev1.PodRunning {
		setTargetReason(target, "TargetNotReady", "waiting for target Pod to run")
		return waitRequeueInterval, nil
	}
	ids, waiting := gpuContainerIDs(pod, cfg)
	if waiting != "" {
		setTargetReason(target, "TargetNotReady", waiting)
		return waitRequeueInterval, nil
	}
	if !reflect.DeepEqual(ids, target.TargetContainerIds) {
		target.TargetContainerIds = ids
		return time.Millisecond, nil
	}
	if err := cfg.ValidateDuration(job.Status.RunConfig.Duration.Duration); err != nil {
		failTarget(target, "InvalidDuration", err.Error())
		return 0, nil
	}
	if target.CollectorPodName == "" {
		target.CollectorPodName = nodeagent.CollectorPodName(job, target)
		return time.Millisecond, nil
	}
	key := client.ObjectKey{Namespace: job.Spec.PersistentVolumeClaimNamespace, Name: target.CollectorPodName}
	current := &corev1.Pod{}
	if err := r.APIReader.Get(ctx, key, current); err != nil {
		if !apierrors.IsNotFound(err) {
			return 0, err
		}
		current = nodeagent.CollectorPod(cfg, job, target)
		log := ctrl.LoggerFrom(ctx).WithValues("target", target.TargetNamespace+"/"+target.TargetName, "collectorPod", key.String(), "node", target.TargetNodeName, "retryCount", target.RetryCount)
		if err := r.Create(ctx, current); err != nil {
			if apierrors.IsAlreadyExists(err) {
				log.Info("Collector Pod already exists; re-reading it")
				return time.Millisecond, nil
			}
			if apierrors.IsInvalid(err) || apierrors.IsBadRequest(err) {
				log.Error(err, "Collector Pod rejected by API server; failing target")
				failTarget(target, "CollectorCreateRejected", err.Error())
				return 0, nil
			}
			if apierrors.IsForbidden(err) {
				log.Error(err, "Collector Pod creation forbidden; scheduling retry")
				return r.targetFailure(cfg, target, collectorprotocol.Failed("CollectorCreateRejected", err.Error())), nil
			}
			return 0, err
		}
		log.Info("Created collector Pod", "uid", current.UID)
	}
	if !nodeagent.CollectorMatches(current, job, target) {
		return r.collectorMismatch(cfg, target, key), nil
	}
	if !current.DeletionTimestamp.IsZero() {
		return pollInterval, nil
	}
	target.CollectorPodUID = string(current.UID)
	now := metav1.NewTime(r.now())
	if !current.CreationTimestamp.IsZero() {
		now = current.CreationTimestamp
	}
	target.CollectorCreatedAt = &now
	target.Stage = profilingv1alpha1.StageCollecting
	setTargetReason(target, "CollectorCreated", fmt.Sprintf("collector Pod %s created on node %s", key, target.TargetNodeName))
	return time.Millisecond, nil
}

func (r *ProfilingJobReconciler) pollTarget(ctx context.Context, job *profilingv1alpha1.ProfilingJob, cfg *nodeagent.Config, target *profilingv1alpha1.TargetStatus) (time.Duration, error) {
	pod := &corev1.Pod{}
	key := client.ObjectKey{Namespace: job.Spec.PersistentVolumeClaimNamespace, Name: target.CollectorPodName}
	if err := r.APIReader.Get(ctx, key, pod); err != nil {
		if !apierrors.IsNotFound(err) {
			return 0, err
		}
		return r.targetFailure(cfg, target, collectorprotocol.Failed("CollectorLost", "collector Pod disappeared before its result was recorded")), nil
	}
	if !nodeagent.CollectorMatches(pod, job, target) {
		return r.collectorMismatch(cfg, target, key), nil
	}
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		if started := collectorStartedAt(pod); started != nil {
			r.markCollecting(job, *started)
		}
		res := nodeagent.CollectorResult(pod)
		if res.Phase == collectorprotocol.PhaseSucceeded {
			target.Stage, target.Reason, target.Message = profilingv1alpha1.StageCompleted, "", res.Message
			target.CollectedDuration = max(res.ElapsedMillis, maxElapsed(res.Containers))
			target.OutputFiles = nil
			for _, f := range nodeagent.OutputFiles(job, res) {
				target.OutputFiles = append(target.OutputFiles, volumePath(f))
			}
			return 0, nil
		}
		r.recordCollectorFailure(target, res)
		target.RetryAt = nil
		failTarget(target, target.Reason, target.Message)
		return 0, nil
	}
	if !pod.DeletionTimestamp.IsZero() {
		return pollInterval, nil
	}
	if pod.Status.Phase == corev1.PodRunning {
		started := metav1.NewTime(r.now())
		if t := collectorStartedAt(pod); t != nil {
			started = *t
		}
		r.markCollecting(job, started)
		setTargetReason(target, "CollectorRunning", fmt.Sprintf("collector Pod %s is running", key))
		return pollInterval, nil
	}
	// A collector is pinned to the frozen node, so a lost target can never start it.
	if _, reason, message, err := r.verifyTarget(ctx, target); err != nil {
		return 0, err
	} else if reason != "" {
		r.recordCollectorFailure(target, collectorprotocol.Failed(reason, message))
		target.Stage, target.RetryAt = profilingv1alpha1.StageRetrying, nil
		return time.Millisecond, nil
	}
	res := nodeagent.CollectorResult(pod)
	if target.CollectorCreatedAt != nil && r.now().Sub(target.CollectorCreatedAt.Time) >= cfg.PendingTimeout.Duration {
		// 启动等待超时仅告警，不中断镜像拉取、调度或卷挂载。
		res.Message = fmt.Sprintf("collector did not start within %s; continuing to wait: %s: %s", cfg.PendingTimeout.Duration, res.Reason, res.Message)
		res.Reason = "CollectorPendingTimeout"
	}
	target.Reason, target.Message = res.Reason, res.Message
	return pollInterval, nil
}

// collectorMismatch replaces a collector Pod that no longer matches its target
// through the retry path, which deletes it and recreates a fresh retry.
func (r *ProfilingJobReconciler) collectorMismatch(cfg *nodeagent.Config, target *profilingv1alpha1.TargetStatus, key client.ObjectKey) time.Duration {
	return r.targetFailure(cfg, target, collectorprotocol.Failed("CollectorMismatch", fmt.Sprintf("collector Pod %s does not match this target; it will be deleted and recreated", key)))
}

func collectorStartedAt(pod *corev1.Pod) *metav1.Time {
	for _, cs := range pod.Status.ContainerStatuses {
		if s := cs.State.Running; s != nil && !s.StartedAt.IsZero() {
			return &s.StartedAt
		}
		if s := cs.State.Terminated; s != nil && !s.StartedAt.IsZero() {
			return &s.StartedAt
		}
	}
	return nil
}

// markCollecting records the earliest collector start in the latest collection.
func (r *ProfilingJobReconciler) markCollecting(job *profilingv1alpha1.ProfilingJob, started metav1.Time) {
	st := &job.Status
	first := st.StartTime == nil
	if first || started.Before(st.StartTime) {
		st.StartTime = started.DeepCopy()
	}
	if !first {
		return
	}
	st.OutputLocation = volumePath(nodeagent.RoundPrefix(job))
	r.event(job, corev1.EventTypeNormal, "CollectionStarted", "profiling round %d started writing to %s", *st.LastRunIndex, st.OutputLocation)
}

func (r *ProfilingJobReconciler) recordCollectorFailure(target *profilingv1alpha1.TargetStatus, res collectorprotocol.Result) {
	target.Reason, target.Message = res.Reason, res.Message
	target.CollectedDuration = max(res.ElapsedMillis, maxElapsed(res.Containers))
	if target.Reason == "" {
		target.Reason = "CollectionFailed"
	}
	if res.StopError != "" {
		target.Message += "; stopError: " + res.StopError
	}
}

func (r *ProfilingJobReconciler) targetFailure(cfg *nodeagent.Config, target *profilingv1alpha1.TargetStatus, res collectorprotocol.Result) time.Duration {
	r.recordCollectorFailure(target, res)
	target.Stage = profilingv1alpha1.StageRetrying
	if target.RetryCount >= *cfg.MaxRetries || !retryableResult(res) {
		target.RetryAt = nil
		return time.Millisecond
	}
	backoff := cfg.RetryBackoff.Duration
	for i := int32(0); i < target.RetryCount && backoff < cfg.MaxRetryBackoff.Duration; i++ {
		if backoff > cfg.MaxRetryBackoff.Duration/2 {
			backoff = cfg.MaxRetryBackoff.Duration
		} else {
			backoff *= 2
		}
	}
	if backoff > cfg.MaxRetryBackoff.Duration {
		backoff = cfg.MaxRetryBackoff.Duration
	}
	at := metav1.NewTime(r.now().Add(backoff))
	target.RetryAt = &at
	return time.Millisecond
}

func retryableResult(res collectorprotocol.Result) bool {
	if res.Reason == collectorprotocol.ReasonInvalidArgs {
		return false
	}
	for _, c := range res.Containers {
		if c.Reason == collectorprotocol.ReasonInvalidArgs {
			return false
		}
	}
	return true
}

func (r *ProfilingJobReconciler) retryTarget(ctx context.Context, job *profilingv1alpha1.ProfilingJob, cfg *nodeagent.Config, target *profilingv1alpha1.TargetStatus) (time.Duration, error) {
	pod := &corev1.Pod{}
	key := client.ObjectKey{Namespace: job.Spec.PersistentVolumeClaimNamespace, Name: target.CollectorPodName}
	if err := r.APIReader.Get(ctx, key, pod); err == nil {
		// Delete whatever occupies this retry's name, including a mismatched Pod.
		if pod.DeletionTimestamp.IsZero() {
			ctrl.LoggerFrom(ctx).Info("Deleting collector Pod before retry", "target", target.TargetNamespace+"/"+target.TargetName,
				"collectorPod", key.String(), "uid", pod.UID, "retryCount", target.RetryCount, "statusReason", target.Reason)
			if err := deletePod(ctx, r.Client, pod); err != nil {
				return 0, err
			}
		}
		return pollInterval, nil
	} else if !apierrors.IsNotFound(err) {
		return 0, err
	}
	if target.RetryAt == nil {
		failTarget(target, target.Reason, target.Message)
		return 0, nil
	}
	if after := target.RetryAt.Sub(r.now()); after > 0 {
		return after, nil
	}
	target.RetryCount++
	target.Stage = profilingv1alpha1.StageQueued
	target.CollectorPodName, target.CollectorPodUID = "", ""
	target.CollectorCreatedAt, target.RetryAt = nil, nil
	target.CollectedDuration = 0
	return time.Millisecond, nil
}

// verifyTarget re-reads the frozen target Pod. A non-empty reason means the
// target can no longer be collected in this round.
func (r *ProfilingJobReconciler) verifyTarget(ctx context.Context, target *profilingv1alpha1.TargetStatus) (*corev1.Pod, string, string, error) {
	pod := &corev1.Pod{}
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: target.TargetNamespace, Name: target.TargetName}, pod); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, "", "", err
		}
		return nil, "TargetLost", "target Pod no longer exists", nil
	}
	if string(pod.UID) != target.TargetUID || pod.Spec.NodeName != target.TargetNodeName || !pod.DeletionTimestamp.IsZero() {
		return pod, "TargetReplaced", "target Pod identity or node changed", nil
	}
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return pod, "TargetTerminated", fmt.Sprintf("target Pod %s/%s is %s", pod.Namespace, pod.Name, pod.Status.Phase), nil
	}
	return pod, "", "", nil
}

func failTarget(target *profilingv1alpha1.TargetStatus, reason, msg string) {
	target.Stage, target.Reason, target.Message = profilingv1alpha1.StageFailed, reason, msg
}

func setTargetReason(target *profilingv1alpha1.TargetStatus, reason, msg string) {
	target.Reason, target.Message = reason, msg
}

func (r *ProfilingJobReconciler) reportCollectorChanges(job *profilingv1alpha1.ProfilingJob, previous *profilingv1alpha1.ProfilingJobStatus) {
	if len(previous.Targets) == 0 || lastRunIndex(previous) != lastRunIndex(&job.Status) || job.Status.Phase == profilingv1alpha1.PhaseCanceled {
		return
	}
	before := make(map[string]profilingv1alpha1.TargetStatus, len(previous.Targets))
	for _, target := range previous.Targets {
		before[target.TargetUID] = target
	}
	counts := map[profilingv1alpha1.TargetStage]int{}
	var changes []string
	eventType := corev1.EventTypeNormal
	for _, target := range job.Status.Targets {
		counts[target.Stage]++
		old, exists := before[target.TargetUID]
		if !exists || (old.Stage == target.Stage && old.Reason == target.Reason && old.RetryCount == target.RetryCount && old.CollectorPodUID == target.CollectorPodUID) {
			continue
		}
		if target.Stage == profilingv1alpha1.StageFailed || target.Stage == profilingv1alpha1.StageRetrying || target.Reason == "CollectorPendingTimeout" {
			eventType = corev1.EventTypeWarning
		}
		detail := fmt.Sprintf("%s/%s stage=%s retry=%d", target.TargetNamespace, target.TargetName, target.Stage, target.RetryCount)
		if target.CollectorPodName != "" {
			detail += " collector=" + job.Spec.PersistentVolumeClaimNamespace + "/" + target.CollectorPodName
		}
		if target.Reason != "" {
			detail += ": " + target.Reason
		}
		if target.Message != "" {
			detail += ": " + target.Message
		}
		if target.Stage == profilingv1alpha1.StageCompleted {
			detail += fmt.Sprintf(" elapsed=%s outputs=%s", time.Duration(target.CollectedDuration)*time.Millisecond, strings.Join(target.OutputFiles, ", "))
		}
		changes = append(changes, detail)
	}
	if len(changes) == 0 {
		return
	}
	r.event(job, eventType, "CollectorsUpdated", "profiling round %d collectors: queued=%d collecting=%d retrying=%d completed=%d failed=%d; %s",
		lastRunIndex(&job.Status), counts[profilingv1alpha1.StageQueued], counts[profilingv1alpha1.StageCollecting], counts[profilingv1alpha1.StageRetrying], counts[profilingv1alpha1.StageCompleted], counts[profilingv1alpha1.StageFailed], strings.Join(changes, "; "))
}

func deletePod(ctx context.Context, c client.Client, pod *corev1.Pod) error {
	uid := pod.UID
	return client.IgnoreNotFound(c.Delete(ctx, pod, &client.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}))
}

// releaseCollectors deletes job's collector Pods and returns those still present.
func (r *ProfilingJobReconciler) releaseCollectors(ctx context.Context, job *profilingv1alpha1.ProfilingJob) ([]corev1.Pod, error) {
	pods := &corev1.PodList{}
	if err := r.APIReader.List(ctx, pods, client.MatchingLabels{nodeagent.LabelComponent: nodeagent.ComponentCollector, nodeagent.LabelJobUID: string(job.UID)}); err != nil {
		return nil, err
	}
	var errs []error
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Annotations[nodeagent.AnnotationJobNS] != job.Namespace || pod.Annotations[nodeagent.AnnotationJobName] != job.Name {
			return nil, fmt.Errorf("collector %s/%s has conflicting ownership", pod.Namespace, pod.Name)
		}
		if pod.DeletionTimestamp.IsZero() {
			ctrl.LoggerFrom(ctx).Info("Deleting collector Pod to release profiling resources", "collectorPod", pod.Namespace+"/"+pod.Name,
				"uid", pod.UID, "node", pod.Spec.NodeName, "podPhase", pod.Status.Phase, "phase", job.Status.Phase)
			if err := deletePod(ctx, r.Client, pod); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return pods.Items, errors.Join(errs...)
}

// reportCleanupPending explains why collector cleanup is still waiting. A Pod
// past its deletion deadline usually sits on a NotReady node whose kubelet
// cannot confirm the exit.
func (r *ProfilingJobReconciler) reportCleanupPending(job *profilingv1alpha1.ProfilingJob, pods []corev1.Pod) {
	var stuck []string
	for _, pod := range pods {
		if !pod.DeletionTimestamp.IsZero() && r.now().After(pod.DeletionTimestamp.Add(cleanupStuckAfter)) {
			stuck = append(stuck, fmt.Sprintf("%s/%s on node %s", pod.Namespace, pod.Name, pod.Spec.NodeName))
		}
	}
	if len(stuck) == 0 {
		r.setCleanupMessage(job, "CleanupPending", fmt.Sprintf("waiting for %d collector Pod(s) to exit and release profiling resources", len(pods)))
		return
	}
	msg := fmt.Sprintf("collector Pod(s) %s did not exit after the deletion deadline; if the node is NotReady, recover the node or force-delete the Pod", strings.Join(stuck, ", "))
	if r.setCleanupMessage(job, "CleanupStuck", msg) {
		r.event(job, corev1.EventTypeWarning, "CleanupStuck", "%s", msg)
	}
}

// listCollectors reads every collector Pod uncached. A single reconcile worker
// keeps this snapshot, the blocker check and the creation atomic.
func (r *ProfilingJobReconciler) listCollectors(ctx context.Context) ([]corev1.Pod, error) {
	pods := &corev1.PodList{}
	if err := r.APIReader.List(ctx, pods, client.MatchingLabels{nodeagent.LabelComponent: nodeagent.ComponentCollector}); err != nil {
		return nil, err
	}
	return pods.Items, nil
}

// collectorBlocker returns another job's collector that may still be profiling podUID.
func collectorBlocker(collectors []corev1.Pod, current *profilingv1alpha1.ProfilingJob, podUID string) string {
	for _, pod := range collectors {
		if pod.Annotations[nodeagent.AnnotationTargetUID] != podUID || pod.Labels[nodeagent.LabelJobUID] == string(current.UID) {
			continue
		}
		// A deleting Pod may still be stopping its Runtime Kit session.
		if !pod.DeletionTimestamp.IsZero() || (pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed) {
			return pod.Namespace + "/" + pod.Name
		}
	}
	return ""
}

func maxElapsed(containers []collectorprotocol.ContainerResult) int64 {
	var elapsed int64
	for _, c := range containers {
		if c.ElapsedMillis > elapsed {
			elapsed = c.ElapsedMillis
		}
	}
	return elapsed
}
