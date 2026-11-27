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
	"slices"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	profilingv1alpha1 "github.com/lj-runtimekit/job-controller/api/v1alpha1"
	"github.com/lj-runtimekit/job-controller/pkg/nodeagent"
)

const (
	finalizerName           = "profiling.runtimekit.io/finalizer"
	MaxHistoryEntries       = 5
	waitRequeueInterval     = 10 * time.Second
	pollInterval            = 10 * time.Second
	queueRetryInterval      = 10 * time.Second
	cleanupStuckAfter       = time.Minute
	cleanupMessageSeparator = "\ncleanup: "
)

type ProfilingJobReconciler struct {
	client.Client
	APIReader          client.Reader
	Scheme             *runtime.Scheme
	Recorder           record.EventRecorder
	CollectorNamespace string
	ConfigMapName      string
	Now                func() time.Time
	mu                 sync.Mutex
	lastGood           *nodeagent.Config
	lastGoodRV         string
	lastBadRV          string
}

// +kubebuilder:rbac:groups=profiling.runtimekit.io,resources=profilingjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=profiling.runtimekit.io,resources=profilingjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=profiling.runtimekit.io,resources=profilingjobs/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;delete;patch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch
// +kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func (r *ProfilingJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, err error) {
	log := ctrl.LoggerFrom(ctx).WithValues("job", req.NamespacedName.String())
	ctx = ctrl.LoggerInto(ctx, log)
	start := time.Now()
	operation := "getJob"
	job := &profilingv1alpha1.ProfilingJob{}
	defer func() {
		if err != nil {
			log.Error(err, "ProfilingJob reconcile failed", "operation", operation, "phase", job.Status.Phase,
				"runIndex", lastRunIndex(&job.Status), "elapsed", time.Since(start).Round(time.Millisecond))
		}
	}()
	if err := r.APIReader.Get(ctx, req.NamespacedName, job); err != nil {
		if client.IgnoreNotFound(err) == nil {
			log.Info("ProfilingJob not found; skipping")
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	log = log.WithValues("specRunIndex", job.Spec.RunIndex, "generation", job.Generation)
	ctx = ctrl.LoggerInto(ctx, log)
	log.Info("Reconciling ProfilingJob", "runIndex", lastRunIndex(&job.Status), "phase", job.Status.Phase, "cancel", job.Spec.Cancel, "deleting", !job.DeletionTimestamp.IsZero())
	if !job.DeletionTimestamp.IsZero() {
		operation = "finalize"
		return r.finalize(ctx, job)
	}
	if controllerutil.AddFinalizer(job, finalizerName) {
		operation = "addFinalizer"
		if err := r.Update(ctx, job); err != nil {
			return ctrl.Result{}, err
		}
		log.Info("Added finalizer", "finalizer", finalizerName)
	}
	orig := job.Status.DeepCopy()
	operation = "reconcileRound"
	result, reconcileErr := r.reconcileRound(ctx, job)
	job.Status.ObservedGeneration = job.Generation
	if !equality.Semantic.DeepEqual(*orig, job.Status) {
		if err := r.Status().Update(ctx, job); err != nil {
			operation = "updateStatus"
			return ctrl.Result{}, errors.Join(reconcileErr, err)
		}
		logStatusChanges(ctx, job, orig)
		r.reportCollectorChanges(job, orig)
	}
	return result, reconcileErr
}

func (r *ProfilingJobReconciler) reconcileRound(ctx context.Context, job *profilingv1alpha1.ProfilingJob) (ctrl.Result, error) {
	st := &job.Status
	if job.Spec.Cancel && !st.Phase.IsTerminal() && st.Phase != profilingv1alpha1.PhaseCanceled {
		for i := range st.Targets {
			t := &st.Targets[i]
			if !t.Stage.IsTerminal() {
				t.Stage, t.Reason, t.Message = profilingv1alpha1.StageCanceled, "Canceled", "canceled by spec.cancel"
			}
		}
		r.finish(job, profilingv1alpha1.PhaseCanceled, "Canceled", "canceled by spec.cancel")
		r.setCleanupMessage(job, "CleanupPending", "waiting for collector Pods to exit and release profiling resources")
		return ctrl.Result{RequeueAfter: time.Millisecond}, nil
	}
	if st.Phase.IsTerminal() || st.Phase == profilingv1alpha1.PhaseCanceled {
		remaining, err := r.releaseCollectors(ctx, job)
		if err != nil {
			if r.setCleanupMessage(job, "CleanupFailed", err.Error()) {
				r.event(job, corev1.EventTypeWarning, "CleanupFailed", "deleting collector Pods failed: %v", err)
			}
			return ctrl.Result{}, err
		}
		if len(remaining) > 0 {
			r.reportCleanupPending(job, remaining)
			return ctrl.Result{RequeueAfter: pollInterval}, nil
		}
		if st.Phase == profilingv1alpha1.PhaseCanceled {
			if r.setCleanupMessage(job, "CollectorsRemoved", "all collector Pods have been removed") {
				r.event(job, corev1.EventTypeNormal, "CollectorsRemoved", "all collector Pods have been removed")
			}
		} else {
			// 清理结束仅移除附加诊断，不覆盖成功摘要或原失败详情。
			st.Message, _, _ = strings.Cut(st.Message, cleanupMessageSeparator)
		}
	}
	if job.Spec.Cancel {
		return ctrl.Result{}, nil
	}
	if job.Spec.RunIndex > lastRunIndex(st) && (st.LastRunIndex == nil || st.Phase.IsTerminal() || st.Phase == profilingv1alpha1.PhaseCanceled) {
		remaining, err := r.releaseCollectors(ctx, job)
		if err != nil {
			return ctrl.Result{}, err
		}
		if len(remaining) > 0 {
			return ctrl.Result{RequeueAfter: pollInterval}, nil
		}
		r.startNewRound(job)
		return ctrl.Result{RequeueAfter: time.Millisecond}, nil
	}
	if st.LastRunIndex == nil || st.Phase.IsTerminal() {
		return ctrl.Result{}, nil
	}
	if st.Phase == profilingv1alpha1.PhaseCanceled {
		if job.Spec.RunIndex != *st.LastRunIndex {
			return ctrl.Result{}, nil
		}
		r.resume(job)
		return ctrl.Result{RequeueAfter: time.Millisecond}, nil
	}
	return r.progress(ctx, job)
}

func (r *ProfilingJobReconciler) startNewRound(job *profilingv1alpha1.ProfilingJob) {
	st := &job.Status
	if st.LastRunIndex != nil {
		st.History = append(st.History, profilingv1alpha1.HistoryEntry{RunIndex: *st.LastRunIndex, Phase: st.Phase, StartTime: st.StartTime, EndTime: st.EndTime, OutputLocation: st.OutputLocation, CollectedDuration: st.CollectedDuration})
		if n := len(st.History); n > MaxHistoryEntries {
			st.History = st.History[n-MaxHistoryEntries:]
		}
	}
	st.LastRunIndex = ptr.To(job.Spec.RunIndex)
	st.Phase = profilingv1alpha1.PhasePending
	st.StartTime, st.EndTime = nil, nil
	st.OutputLocation, st.CollectedDuration = "", 0
	st.Targets, st.Message = nil, ""
	duration := job.Spec.Duration
	if limit := job.Spec.ProfilingMode.MaxDuration(); duration.Duration > limit {
		duration = metav1.Duration{Duration: limit}
		r.event(job, corev1.EventTypeNormal, "DurationTruncated", "duration %s exceeds the %s limit of %s mode and was truncated to %s",
			job.Spec.Duration.Duration, limit, job.Spec.ProfilingMode, limit)
	}
	st.RunConfig = &profilingv1alpha1.RunConfiguration{
		ProfilingRules: job.Spec.ProfilingRules, Duration: duration, ProfilingMode: job.Spec.ProfilingMode,
	}
	r.event(job, corev1.EventTypeNormal, "RoundCreated", "profiling round %d created", *st.LastRunIndex)
}

// lastRunIndex returns -1 before the first round so any valid runIndex starts it.
func lastRunIndex(st *profilingv1alpha1.ProfilingJobStatus) int64 {
	return ptr.Deref(st.LastRunIndex, -1)
}

func (r *ProfilingJobReconciler) resume(job *profilingv1alpha1.ProfilingJob) {
	st := &job.Status
	st.Phase = profilingv1alpha1.PhasePending
	st.Targets = nil
	st.StartTime, st.EndTime = nil, nil
	st.OutputLocation, st.CollectedDuration = "", 0
	st.Message = ""
	r.event(job, corev1.EventTypeNormal, "Resumed", "profiling round %d resumed", *st.LastRunIndex)
}

func (r *ProfilingJobReconciler) progress(ctx context.Context, job *profilingv1alpha1.ProfilingJob) (ctrl.Result, error) {
	st := &job.Status
	var cfg *nodeagent.Config
	if st.Targets == nil {
		pvc, allowed, reason, err := r.pvcReady(ctx, &job.Spec)
		if err != nil {
			return r.preflightFailure(job, err)
		}
		if !allowed {
			if r.setMessage(job, "PVCNotReady", reason) {
				r.event(job, corev1.EventTypeNormal, "PVCNotReady", "%s", reason)
			}
			return ctrl.Result{RequeueAfter: waitRequeueInterval}, nil
		}
		if st.RunConfig.Duration.Duration <= 0 {
			return r.preflightFailure(job, &preflightError{"InvalidDuration", "duration must be positive"})
		}
		cfg, err = r.collectorConfig(ctx)
		if err != nil {
			if r.setMessage(job, "CollectorConfigInvalid", err.Error()) {
				r.event(job, corev1.EventTypeWarning, "CollectorConfigInvalid", "%v", err)
			}
			return ctrl.Result{RequeueAfter: waitRequeueInterval}, nil
		}
		if err := cfg.ValidateDuration(st.RunConfig.Duration.Duration); err != nil {
			return r.preflightFailure(job, &preflightError{"InvalidDuration", err.Error()})
		}
		targets, reason, err := r.resolveTargets(ctx, job, cfg)
		if err != nil {
			return r.preflightFailure(job, err)
		}
		if reason != "" {
			if pvc.Status.Phase != corev1.ClaimBound {
				reason += "; WaitForFirstConsumer: collector Pods will trigger PVC binding before collection starts"
			}
			if r.setMessage(job, "TargetsNotReady", reason) {
				r.event(job, corev1.EventTypeNormal, "TargetsNotReady", "%s", reason)
			}
			return ctrl.Result{RequeueAfter: waitRequeueInterval}, nil
		}
		if err := validateAccessModes(pvc, targets); err != nil {
			return r.preflightFailure(job, err)
		}
		st.Targets = targets
		names := make([]string, len(targets))
		for i, t := range targets {
			names[i] = t.TargetNamespace + "/" + t.TargetName
		}
		msg := fmt.Sprintf("%d target Pod(s) selected; waiting for collectors to start", len(targets))
		if pvc.Status.Phase != corev1.ClaimBound {
			msg += "; WaitForFirstConsumer: collector Pods will trigger PVC binding before collection starts"
		}
		r.setMessage(job, "TargetsSelected", msg)
		r.event(job, corev1.EventTypeNormal, "TargetsSelected", "%d target Pod(s) selected: %s", len(targets), strings.Join(names, ", "))
		// Persist target identities before creating any collector Pod.
		return ctrl.Result{RequeueAfter: time.Millisecond}, nil
	}
	var err error
	cfg, err = r.collectorConfig(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}
	var requeue time.Duration
	var errs []error
	var collectors []corev1.Pod
	var listErr error
	if slices.ContainsFunc(st.Targets, func(t profilingv1alpha1.TargetStatus) bool { return t.Stage == profilingv1alpha1.StageQueued }) {
		if collectors, listErr = r.listCollectors(ctx); listErr != nil {
			errs = append(errs, fmt.Errorf("list collector Pods: %w", listErr))
		}
	}
	for i := range st.Targets {
		t := &st.Targets[i]
		var after time.Duration
		var err error
		switch t.Stage {
		case profilingv1alpha1.StageQueued:
			if listErr == nil {
				after, err = r.startTarget(ctx, job, cfg, t, collectors)
			}
		case profilingv1alpha1.StageCollecting:
			after, err = r.pollTarget(ctx, job, cfg, t)
		case profilingv1alpha1.StageRetrying:
			after, err = r.retryTarget(ctx, job, cfg, t)
		}
		if err != nil {
			ctrl.LoggerFrom(ctx).Error(err, "Profiling target operation failed", "target", t.TargetNamespace+"/"+t.TargetName,
				"targetUID", t.TargetUID, "node", t.TargetNodeName, "stage", t.Stage, "retryCount", t.RetryCount, "collectorPod", t.CollectorPodName)
			errs = append(errs, fmt.Errorf("target %s/%s: %w", t.TargetNamespace, t.TargetName, err))
		}
		if after > 0 && (requeue == 0 || after < requeue) {
			requeue = after
		}
	}
	r.summarize(job)
	if st.Phase.IsTerminal() {
		requeue = time.Millisecond
	}
	return ctrl.Result{RequeueAfter: requeue}, errors.Join(errs...)
}

type preflightError struct{ reason, message string }

func (e *preflightError) Error() string { return e.message }

func (r *ProfilingJobReconciler) preflightFailure(job *profilingv1alpha1.ProfilingJob, err error) (ctrl.Result, error) {
	var invalid *preflightError
	if !errors.As(err, &invalid) {
		return ctrl.Result{}, err
	}
	r.finish(job, profilingv1alpha1.PhaseFailed, invalid.reason, invalid.message)
	if job.Spec.RunIndex > lastRunIndex(&job.Status) {
		return ctrl.Result{RequeueAfter: time.Millisecond}, nil
	}
	return ctrl.Result{}, nil
}

func (r *ProfilingJobReconciler) summarize(job *profilingv1alpha1.ProfilingJob) {
	st := &job.Status
	counts := map[profilingv1alpha1.TargetStage]int{}
	for _, t := range st.Targets {
		counts[t.Stage]++
	}
	r.updateCollectedDuration(job, r.now())
	summary := fmt.Sprintf("queued=%d collecting=%d retrying=%d completed=%d failed=%d", counts[profilingv1alpha1.StageQueued], counts[profilingv1alpha1.StageCollecting], counts[profilingv1alpha1.StageRetrying], counts[profilingv1alpha1.StageCompleted], counts[profilingv1alpha1.StageFailed])
	if counts[profilingv1alpha1.StageCompleted]+counts[profilingv1alpha1.StageFailed] == len(st.Targets) {
		if counts[profilingv1alpha1.StageFailed] > 0 {
			r.finish(job, profilingv1alpha1.PhaseFailed, "TargetsFailed", summary+"; "+firstFailure(st.Targets))
		} else {
			r.finish(job, profilingv1alpha1.PhaseCompleted, "Completed", summary)
		}
		return
	}
	// Retries requeue targets, but a collection that has started stays Running.
	if counts[profilingv1alpha1.StageQueued] == len(st.Targets) && st.Phase == profilingv1alpha1.PhasePending {
		r.setMessage(job, "Queued", summary)
		return
	}
	st.Phase = profilingv1alpha1.PhaseRunning
	r.setMessage(job, "InProgress", summary)
}

// updateCollectedDuration measures wall time since the latest collection's first
// collector started running.
func (r *ProfilingJobReconciler) updateCollectedDuration(job *profilingv1alpha1.ProfilingJob, until time.Time) {
	st := &job.Status
	st.CollectedDuration = 0
	if st.StartTime != nil {
		st.CollectedDuration = max(until.Sub(st.StartTime.Time).Milliseconds(), 0)
	}
}

func (r *ProfilingJobReconciler) finish(job *profilingv1alpha1.ProfilingJob, phase profilingv1alpha1.ProfilingPhase, reason, msg string) {
	now := metav1.NewTime(r.now())
	job.Status.EndTime, job.Status.Phase = &now, phase
	r.updateCollectedDuration(job, now.Time)
	r.setMessage(job, reason, msg)
	eventType := corev1.EventTypeNormal
	if phase == profilingv1alpha1.PhaseFailed {
		eventType = corev1.EventTypeWarning
	}
	if reason == "TargetsFailed" {
		completed, failed := 0, 0
		for _, target := range job.Status.Targets {
			switch target.Stage {
			case profilingv1alpha1.StageCompleted:
				completed++
			case profilingv1alpha1.StageFailed:
				failed++
			}
		}
		msg = fmt.Sprintf("completed=%d failed=%d", completed, failed)
	}
	if job.Status.LastRunIndex == nil {
		r.event(job, eventType, string(phase), "profiling %s before the first round: %s", phase, msg)
		return
	}
	r.event(job, eventType, string(phase), "profiling round %d %s: %s", *job.Status.LastRunIndex, phase, msg)
}

func firstFailure(targets []profilingv1alpha1.TargetStatus) string {
	for _, t := range targets {
		if t.Stage == profilingv1alpha1.StageFailed {
			return fmt.Sprintf("%s/%s: %s: %s", t.TargetNamespace, t.TargetName, t.Reason, t.Message)
		}
	}
	return ""
}

func (r *ProfilingJobReconciler) finalize(ctx context.Context, job *profilingv1alpha1.ProfilingJob) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(job, finalizerName) {
		return ctrl.Result{}, nil
	}
	orig := job.Status.DeepCopy()
	remaining, err := r.releaseCollectors(ctx, job)
	if err == nil && len(remaining) == 0 {
		controllerutil.RemoveFinalizer(job, finalizerName)
		if err := r.Update(ctx, job); err != nil {
			return ctrl.Result{}, err
		}
		ctrl.LoggerFrom(ctx).Info("All collector Pods released; removed finalizer", "finalizer", finalizerName)
		return ctrl.Result{}, nil
	}
	if err != nil {
		if r.setCleanupMessage(job, "CleanupFailed", err.Error()) {
			r.event(job, corev1.EventTypeWarning, "CleanupFailed", "deleting collector Pods failed: %v", err)
		}
	} else {
		r.reportCleanupPending(job, remaining)
	}
	if !equality.Semantic.DeepEqual(*orig, job.Status) {
		if updateErr := r.Status().Update(ctx, job); updateErr != nil {
			err = errors.Join(err, updateErr)
		} else {
			logStatusChanges(ctx, job, orig)
		}
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: pollInterval}, nil
}

// setMessage 用类别前缀去重 Event，详情变化仍写回；不增加额外持久字段。
// message 仅用于诊断和事件去重，状态流转与清理屏障不解析它。
func (r *ProfilingJobReconciler) setMessage(job *profilingv1alpha1.ProfilingJob, reason, msg string) bool {
	changed := messageCategory(job.Status.Message) != reason
	job.Status.Message = reason + ": " + msg
	return changed
}

func messageCategory(msg string) string {
	reason, _, _ := strings.Cut(msg, ": ")
	return reason
}

// setCleanupMessage 单独替换清理诊断，保留终态结果且避免每轮不断追加。
func (r *ProfilingJobReconciler) setCleanupMessage(job *profilingv1alpha1.ProfilingJob, reason, msg string) bool {
	base, previous, _ := strings.Cut(job.Status.Message, cleanupMessageSeparator)
	changed := messageCategory(previous) != reason
	job.Status.Message = base + cleanupMessageSeparator + reason + ": " + msg
	return changed
}

func (r *ProfilingJobReconciler) event(job *profilingv1alpha1.ProfilingJob, eventType, reason, format string, args ...any) {
	if r.Recorder != nil {
		r.Recorder.Eventf(job, eventType, reason, format, args...)
	}
}

// logStatusChanges 在 status 持久化成功后记录 job 与 target 的状态流转；
// Event 会被聚合或过期，日志保留完整轨迹。仅在类别变化时输出，避免轮询刷屏。
func logStatusChanges(ctx context.Context, job *profilingv1alpha1.ProfilingJob, previous *profilingv1alpha1.ProfilingJobStatus) {
	log := ctrl.LoggerFrom(ctx)
	st := &job.Status
	runIndex := lastRunIndex(st)
	sameRound := runIndex == lastRunIndex(previous)
	base, cleanup, _ := strings.Cut(st.Message, cleanupMessageSeparator)
	previousBase, previousCleanup, _ := strings.Cut(previous.Message, cleanupMessageSeparator)
	if !sameRound || st.Phase != previous.Phase || messageCategory(base) != messageCategory(previousBase) {
		log.Info("ProfilingJob status changed", "runIndex", runIndex, "phase", st.Phase, "previousPhase", previous.Phase,
			"statusReason", messageCategory(base), "message", base)
	}
	if cleanup != "" && messageCategory(cleanup) != messageCategory(previousCleanup) {
		log.Info("ProfilingJob cleanup status changed", "runIndex", runIndex, "phase", st.Phase, "statusReason", messageCategory(cleanup), "message", cleanup)
	}
	before := make(map[string]profilingv1alpha1.TargetStatus, len(previous.Targets))
	if sameRound {
		for _, target := range previous.Targets {
			before[target.TargetUID] = target
		}
	}
	for _, target := range st.Targets {
		old, exists := before[target.TargetUID]
		if exists && old.Stage == target.Stage && old.Reason == target.Reason && old.RetryCount == target.RetryCount &&
			old.CollectorPodName == target.CollectorPodName && old.CollectorPodUID == target.CollectorPodUID {
			continue
		}
		kv := []any{"runIndex", runIndex, "target", target.TargetNamespace + "/" + target.TargetName, "targetUID", target.TargetUID,
			"node", target.TargetNodeName, "stage", target.Stage, "previousStage", old.Stage, "retryCount", target.RetryCount}
		if target.CollectorPodName != "" {
			kv = append(kv, "collectorPod", job.Spec.PersistentVolumeClaimNamespace+"/"+target.CollectorPodName)
		}
		if target.Reason != "" || target.Message != "" {
			kv = append(kv, "statusReason", target.Reason, "message", target.Message)
		}
		switch target.Stage {
		case profilingv1alpha1.StageRetrying:
			kv = append(kv, "willRetry", target.RetryAt != nil)
			if target.RetryAt != nil {
				kv = append(kv, "retryAt", target.RetryAt.Time)
			}
		case profilingv1alpha1.StageCompleted:
			kv = append(kv, "collectedDuration", time.Duration(target.CollectedDuration)*time.Millisecond, "outputFiles", target.OutputFiles)
		}
		log.Info("Profiling target status changed", kv...)
	}
}

func (r *ProfilingJobReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func jobUpdateRelevant(e event.UpdateEvent) bool {
	return e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration() ||
		(e.ObjectOld.GetDeletionTimestamp().IsZero() && !e.ObjectNew.GetDeletionTimestamp().IsZero())
}

func (r *ProfilingJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&profilingv1alpha1.ProfilingJob{}, builder.WithPredicates(predicate.Funcs{UpdateFunc: jobUpdateRelevant})).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(r.mapManagedPod), builder.WithPredicates(predicate.NewPredicateFuncs(func(o client.Object) bool {
			return o.GetLabels()[nodeagent.LabelComponent] == nodeagent.ComponentCollector
		}))).
		Watches(&corev1.PersistentVolumeClaim{}, handler.EnqueueRequestsFromMapFunc(r.activeJobs)).
		Watches(&storagev1.StorageClass{}, handler.EnqueueRequestsFromMapFunc(r.activeJobs)).
		Watches(&corev1.ConfigMap{}, handler.EnqueueRequestsFromMapFunc(r.activeJobs), builder.WithPredicates(predicate.NewPredicateFuncs(func(o client.Object) bool {
			return o.GetNamespace() == r.CollectorNamespace && o.GetName() == r.ConfigMapName
		}))).
		WithOptions(controller.Options{MaxConcurrentReconciles: 1}).
		Named("profilingjob").Complete(r)
}

func (r *ProfilingJobReconciler) mapManagedPod(_ context.Context, o client.Object) []reconcile.Request {
	a := o.GetAnnotations()
	if a[nodeagent.AnnotationJobNS] == "" || a[nodeagent.AnnotationJobName] == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: client.ObjectKey{Namespace: a[nodeagent.AnnotationJobNS], Name: a[nodeagent.AnnotationJobName]}}}
}

func (r *ProfilingJobReconciler) activeJobs(ctx context.Context, _ client.Object) []reconcile.Request {
	list := &profilingv1alpha1.ProfilingJobList{}
	if err := r.List(ctx, list); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "list ProfilingJobs")
		return nil
	}
	var reqs []reconcile.Request
	for _, j := range list.Items {
		if !j.Status.Phase.IsTerminal() && j.Status.Phase != profilingv1alpha1.PhaseCanceled {
			reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&j)})
		}
	}
	return reqs
}
