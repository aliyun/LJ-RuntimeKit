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
	"fmt"
	"sync"
	"time"

	"github.com/container-migration/controller/api/v1beta1"
	"github.com/container-migration/controller/pkg/controllers"
	"github.com/container-migration/controller/pkg/migrationworker"
	"github.com/container-migration/controller/pkg/util"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// xpuRestoreTransientBackoff is the resend interval for transient errors
	// (worker restart, network jitter) and plan-A retries (3s, consistent with
	// the download retry backoff). Without backoff it would resend every
	// RestoreRetryWaitTime (1s) round.
	xpuRestoreTransientBackoff = 3 * time.Second
)

// xpuTaskState is the state of a background XPU restore task in the in-memory table.
type xpuTaskState int

const (
	// xpuTaskInflight: a background goroutine is executing RestoreXPUOnNode.
	xpuTaskInflight xpuTaskState = iota
	// xpuTaskDone: a deterministic conclusion is available, waiting to be written into the CR status.
	xpuTaskDone
	// xpuTaskRetryWait: transient error, redispatch after the backoff elapses.
	xpuTaskRetryWait
)

// xpuRestoreTask records the XPU restore task of one replica.
//
// Only value types are stored: never hold jobReq / dest pointers - the entry
// outlives the reconcile that created it; holding pointers causes cross-round
// data races and keeps the whole CR object from being GC'd.
type xpuRestoreTask struct {
	state xpuTaskState
	// startAt records the dispatch time of this round.
	startAt time.Time
	// succeeded / message are the conclusion (valid when state == xpuTaskDone).
	succeeded bool
	message   string
	// eventSent: the conclusion event is emitted only once. A failed status
	// write replays the conclusion next round; the replay must not emit a
	// duplicate event.
	eventSent bool
	// retryAt is the redispatch time when state == xpuTaskRetryWait.
	retryAt time.Time
}

// xpuRestoreStore is the process-level XPU restore task table, grouped by CR UID.
//
// Why process-level instead of one table per CR: the handler is created only
// once at controller construction (see registerHandlers), all CR reconciles
// share the same instance. Grouping by CR UID turns "reclaim the whole group
// when the task ends" into a single delete.
//
// There are three reclamation exits; any one of them firing leaves no entry:
//  1. At the start of Handle the CR is found no longer in Restoring (the phase
//     is read from etcd, meaning the terminal state has landed) -> forgetJob
//     deletes the whole group;
//  2. The CR is deleted directly -> forgetJob in CleanupOnDelete;
//  3. A single entry after its conclusion lands (the dest is seen terminal
//     next round) -> forget deletes it. The RPC itself has no deadline: it
//     runs until the worker returns, bounded remotely by the XDM-side cr-tool
//     timeout (2h). A worker that stays reachable but never responds leaves
//     the replica in XPURestoring - an accepted tail risk, consistent with
//     the no-timeout restore design.
type xpuRestoreStore struct {
	mu sync.Mutex
	// jobs[CR UID][dest key]
	jobs map[string]map[string]*xpuRestoreTask
}

var xpuRestoreTasks = &xpuRestoreStore{jobs: map[string]map[string]*xpuRestoreTask{}}

// get returns a snapshot of the entry. The value copy (not a pointer) is returned, so the caller needs no lock while reading the conclusion.
func (s *xpuRestoreStore) get(jobUID, destKey string) (xpuRestoreTask, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, ok := s.jobs[jobUID][destKey]
	if !ok {
		return xpuRestoreTask{}, false
	}
	return *task, true
}

// start registers a new dispatch, overwriting the old entry of the same key.
func (s *xpuRestoreStore) start(jobUID, destKey string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.jobs[jobUID] == nil {
		s.jobs[jobUID] = map[string]*xpuRestoreTask{}
	}
	s.jobs[jobUID][destKey] = &xpuRestoreTask{
		state:   xpuTaskInflight,
		startAt: time.Now(),
	}
}

// finish is called by the background goroutine to write the conclusion back
// into the table.
// transient=true means a mere connection jitter; it moves to backoff and resends, not a final conclusion.
func (s *xpuRestoreStore) finish(jobUID, destKey string, succeeded bool, message string, transient bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, ok := s.jobs[jobUID][destKey]
	if !ok {
		// The entry was already reclaimed (CR deleted / task terminal), the conclusion has nowhere to go, drop it directly.
		return
	}
	task.message = message
	if transient {
		task.state = xpuTaskRetryWait
		task.retryAt = time.Now().Add(xpuRestoreTransientBackoff)
		return
	}
	task.state = xpuTaskDone
	task.succeeded = succeeded
}

// markEventSent marks the conclusion event as sent, avoiding duplicate events when the conclusion is replayed.
func (s *xpuRestoreStore) markEventSent(jobUID, destKey string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if task, ok := s.jobs[jobUID][destKey]; ok {
		task.eventSent = true
	}
}

// setRetryWait unconditionally writes (or overwrites) the RetryWait state, for
// failure paths without an in-flight entry (e.g. pod already gone before
// dispatch): after the backoff elapses, dispatch redispats.
func (s *xpuRestoreStore) setRetryWait(jobUID, destKey, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.jobs[jobUID] == nil {
		s.jobs[jobUID] = map[string]*xpuRestoreTask{}
	}
	s.jobs[jobUID][destKey] = &xpuRestoreTask{
		state:   xpuTaskRetryWait,
		retryAt: time.Now().Add(xpuRestoreTransientBackoff),
		message: message,
	}
}

// forget reclaims a single entry.
func (s *xpuRestoreStore) forget(jobUID, destKey string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.jobs[jobUID], destKey)
	if len(s.jobs[jobUID]) == 0 {
		delete(s.jobs, jobUID)
	}
}

// forgetJob reclaims the whole group of a CR.
func (s *xpuRestoreStore) forgetJob(jobUID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.jobs, jobUID)
}

// forgetXPURestoreTasks is called by the CR deletion path to reclaim all task entries of the CR.
func forgetXPURestoreTasks(jobUID string) {
	xpuRestoreTasks.forgetJob(jobUID)
}

// destTaskKey is the key identifying a replica in the in-memory table.
// The pod namespace/name is used instead of the Destinations index - indices
// have no stability guarantee across rounds of status updates.
func destTaskKey(dest *v1beta1.JobDestinationStatus) string {
	return dest.Pod.String()
}

// RestoreXPUHandler drives the XPU device state restore after the container
// process is back from checkpoint (WaitRestoreHandler confirmed Running and
// restoreAttempted):
//
//	controller -> worker RestoreXPUOnNode -> XDM RestoreXPU -> cr-tool
//
// Why driven by the controller: XPU state can only be restored onto a process
// that is already back running (cr-tool needs a pid), and the "container
// Running" observation point is only visible controller-side; the pid-file
// polling goroutine of the old XDM implementation is removed, the node side no
// longer decides the timing itself.
//
// Async dispatch: worker -> XDM -> cr-tool executes synchronously, one call
// can take over ten seconds. Waiting for it synchronously inside reconcile
// would occupy the whole round, and reconcile is the only observation point
// of "container Running" - replicas becoming ready later would only be seen
// after the earlier batch of RPCs fully finishes; measured as the restore
// times of replicas on two nodes being stretched apart by a whole batch of
// RPC duration.
//
// So this handler does only three non-blocking things per round:
//  1. Collect results: entries in the table with a conclusion are written into
//     dest with an event;
//  2. Reclaim: entries whose dest is already terminal (the conclusion has
//     landed) are deleted;
//  3. Dispatch: replicas without an entry get a background goroutine running
//     the RPC.
//
// Conclusions always land in the in-memory table first and are written to
// status by later reconciles, and **landing is confirmed before reclamation**:
// an entry is only deleted when the dest is seen terminal next round.
// Otherwise the conclusion of the round whose Status().Update() failed would
// be lost and the replica stuck in XPURestoring forever.
//
// Failure semantics (plan A, Q2/D2 decided): running cr-tool repeatedly on the
// same process is safe; a deterministic failure retries automatically (re-run
// cr-tool) until retryCount is exhausted and then falls back to cold start
// (handleXPUFailure); connection-level transient errors (worker restart,
// network jitter) resend after backoff and do not count toward retries.
//
// Known window: the in-memory table is process-level; a controller restart
// between dispatch and conclusion landing loses the conclusion, and the next
// round redispats once. D2 confirmed re-running cr-tool on the same process is
// safe, and the synchronous implementation has the same window (killed after
// RPC success but before status landed, the replica falls back to
// InitReleased and re-runs anyway); async does not enlarge it. Fully
// eliminating it needs a task table plus query API for RestoreXPUOnNode on the
// worker side (the requestID is reserved by xpuRestoreRequestID), out of scope
// for this handler.
type RestoreXPUHandler struct {
	sharedContext *controllers.SharedContext
}

var _ = (controllers.Handler[*v1beta1.ContainerStateMigrationJobRequest])(&RestoreXPUHandler{})

func NewRestoreXPUHandler(sharedContext *controllers.SharedContext) *RestoreXPUHandler {
	return &RestoreXPUHandler{sharedContext: sharedContext}
}

func (h *RestoreXPUHandler) GetName() string {
	return "RestoreXPUHandler"
}

func (h *RestoreXPUHandler) Handle(ctx context.Context, jobReq *v1beta1.ContainerStateMigrationJobRequest) (result controllers.Result) {
	status := &jobReq.Status
	jobUID := string(jobReq.UID)

	if jobReq.Spec.Restore == nil || status.Phase != v1beta1.JobPhaseRestoring ||
		status.RestoreStatus == nil || status.RestoreStatus.Phase != v1beta1.JobPhaseRestoring {
		// The phase read here comes from etcd: not in Restoring means the
		// terminal state has landed; the entries of this CR are useless,
		// reclaim the whole group.
		xpuRestoreTasks.forgetJob(jobUID)
		return controllers.NewSkipResult("not in Restoring phase")
	}

	for i := range status.RestoreStatus.Destinations {
		dest := &status.RestoreStatus.Destinations[i]
		switch dest.Phase {
		case v1beta1.DestPhaseXPURestoring:
			h.progressDestXPU(ctx, jobReq, dest)
		case v1beta1.DestPhaseSucceeded, v1beta1.DestPhaseFailed:
			// Read from etcd as terminal this round: the conclusion has landed, safe to reclaim the entry.
			xpuRestoreTasks.forget(jobUID, destTaskKey(dest))
		}
	}

	// Aggregate (terminal-state judgement converges here: a dest only counts Succeeded when the XPU state is restored together with it).
	succeeded, failed := 0, 0
	for i := range status.RestoreStatus.Destinations {
		switch status.RestoreStatus.Destinations[i].Phase {
		case v1beta1.DestPhaseSucceeded:
			succeeded++
		case v1beta1.DestPhaseFailed:
			failed++
		}
	}
	status.RestoreStatus.SucceededCount = succeeded
	status.RestoreStatus.FailedCount = failed

	total := len(status.RestoreStatus.Destinations)
	if succeeded+failed < total {
		// Replicas still running XPU restore (or in backoff retry), upstream stage unfinished, or cold-starting
		return controllers.NewAbortWithRetryAfterResult(h.sharedContext.Config.RestoreRetryWaitTime)
	}

	// A replica only succeeded via the cold-start fallback: add a job-level
	// Condition so "succeeded but took the fallback" is visible at a glance.
	// Cold-start success counts into succeeded; the convergence result is not changed here.
	coldStartCount := 0
	for i := range status.RestoreStatus.Destinations {
		if status.RestoreStatus.Destinations[i].StartupMode == v1beta1.StartupModeColdStart {
			coldStartCount++
		}
	}
	if coldStartCount > 0 {
		status.RestoreStatus.Conditions = util.AddOrUpdateCondition(&metav1.Condition{
			Type:               v1beta1.JobConditionColdStartFallback,
			Status:             metav1.ConditionTrue,
			LastTransitionTime: metav1.Now(),
			Reason:             "ColdStartFallback",
			Message:            fmt.Sprintf("%d/%d replicas started via cold start fallback", coldStartCount, total),
		}, status.RestoreStatus.Conditions)
	}

	switch {
	case failed == 0:
		status.RestoreStatus.Phase = v1beta1.JobPhaseSucceeded
		status.RestoreStatus.Message = fmt.Sprintf("All %d replicas restored", total)
		status.Phase = v1beta1.JobPhaseSucceeded
	case succeeded == 0:
		status.RestoreStatus.Phase = v1beta1.JobPhaseFailed
		status.RestoreStatus.Message = fmt.Sprintf("All %d replicas failed to restore", total)
		status.Phase = v1beta1.JobPhaseFailed
	default:
		status.RestoreStatus.Phase = v1beta1.JobPhasePartiallySucceeded
		status.RestoreStatus.Message = fmt.Sprintf("%d/%d replicas restored", succeeded, total)
		status.Phase = v1beta1.JobPhasePartiallySucceeded
	}
	status.Message = status.RestoreStatus.Message
	status.RestoreStatus.Conditions = util.AddOrUpdateCondition(&metav1.Condition{
		Type:               v1beta1.JobConditionRestored,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             "Restored",
		Message:            status.RestoreStatus.Message,
	}, status.RestoreStatus.Conditions)
	// Land the terminal state first, CleanHandler finishes after requeue; the entries are reclaimed via forgetJob next round.
	return controllers.NewAbortWithRetryAfterResult(h.sharedContext.Config.RestoreRetryWaitTime)
}

// progressDestXPU advances one XPURestoring replica: the table decides
// "collect result / wait / dispatch".
//
// No entry in the table == to be dispatched. All early-return branches leave
// no entry, the next round retries naturally.
func (h *RestoreXPUHandler) progressDestXPU(ctx context.Context,
	jobReq *v1beta1.ContainerStateMigrationJobRequest, dest *v1beta1.JobDestinationStatus) {
	jobUID := string(jobReq.UID)
	key := destTaskKey(dest)

	task, ok := xpuRestoreTasks.get(jobUID, key)
	if !ok {
		h.dispatchDestXPU(ctx, jobReq, dest)
		return
	}

	switch task.state {
	case xpuTaskDone:
		h.applyDestResult(ctx, jobReq, dest, task)
	case xpuTaskRetryWait:
		if time.Now().Before(task.retryAt) {
			dest.Message = task.message
			return
		}
		h.dispatchDestXPU(ctx, jobReq, dest)
	default:
		// No deadline: the RPC runs until the worker returns (bounded
		// remotely by the XDM-side cr-tool timeout, 2h). A worker that stays
		// reachable but never responds leaves the replica in XPURestoring -
		// accepted tail risk, consistent with the no-timeout restore design.
		dest.Message = "Restoring XPU state on node"
	}
}

// applyDestResult writes the table conclusion into dest.
//
// The success path is idempotent: a failed status write re-writes the same
// entry next round, the conclusion and message are identical; the event is
// guaranteed once by eventSent. The failure path (handleXPUFailure) turns the
// entry into RetryWait right after recording one attempt, the same entry is
// never consumed twice, the count never double-counts.
func (h *RestoreXPUHandler) applyDestResult(ctx context.Context,
	jobReq *v1beta1.ContainerStateMigrationJobRequest, dest *v1beta1.JobDestinationStatus,
	task xpuRestoreTask) {
	ctxLog := log.FromContext(ctx)

	if task.succeeded {
		// The re-restore annotation must be stamped before converging to
		// Succeeded: a failed patch means no terminal state, dest stays in
		// XPURestoring with the table entry kept, and the next round retries
		// the patch with the same conclusion replayed; otherwise a later
		// kubelet restart of this pod cannot trigger XDM self-driven cr-tool.
		if err := h.markPodRestoreSucceeded(ctx, dest); err != nil {
			ctxLog.Info("Failed to stamp restore-succeeded annotation, will retry",
				"pod", dest.Pod.String(), "error", err)
			dest.Message = fmt.Sprintf("Restored; retrying to stamp %s annotation: %v",
				AnnotationRestoreSucceeded, err)
			return
		}
		dest.Phase = v1beta1.DestPhaseSucceeded
		dest.StartupMode = v1beta1.StartupModeRestored
		dest.Message = task.message

		if task.eventSent {
			return
		}
		xpuRestoreTasks.markEventSent(string(jobReq.UID), destTaskKey(dest))

		ctxLog.Info("XPU state restored", "pod", dest.Pod.String())
		h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeNormal, "RestoreSucceeded",
			"Pod %s restored (containers and XPU state)", dest.Pod.String())
		return
	}

	// Deterministic failure: plan A re-runs cr-tool in place, cold start when exhausted (Q2/D2 decided).
	h.handleXPUFailure(ctx, jobReq, dest, "XPURestoreFailed", task.message)
}

// handleXPUFailure handles one deterministic failure of XPU restore (cr-tool
// execution failure / timeout / pod gone).
//
// Plan A (Q2 decided): retry means executing cr-tool again - the container is
// still Running, no re-download, no re-runc-restore. D2 confirmed re-running
// cr-tool on the same process is safe. Each failure records one attempt
// (PhaseRetries visible via describe), and dispatch redispats after a 3s
// backoff; retries exhausted (attempts > retryCount) -> delete the pod and
// fall back to cold start.
func (h *RestoreXPUHandler) handleXPUFailure(ctx context.Context,
	jobReq *v1beta1.ContainerStateMigrationJobRequest, dest *v1beta1.JobDestinationStatus,
	reason, message string) {
	retryCount := retryCountOf(jobReq)
	attempts := recordAttempt(dest, v1beta1.RetryPhaseXPU, reason, message, retryCount)
	if attempts <= retryCount {
		dest.Message = fmt.Sprintf("XPU restore failed (attempt %d/%d), will re-run cr-tool: %s",
			attempts, retryCount, message)
		h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeWarning, "XPURestoreRetry",
			"Pod %s: %s (attempt %d/%d)", dest.Pod.String(), message, attempts, retryCount)
		// Turn into RetryWait: progressDestXPU redispats after the backoff elapses (re-run cr-tool in place).
		xpuRestoreTasks.setRetryWait(string(jobReq.UID), destTaskKey(dest), message)
		return
	}
	enterColdStart(dest, "XPURestoreExhausted", fmt.Sprintf(
		"XPU restore retries exhausted (attempts=%d, retryCount=%d): %s", attempts, retryCount, message))
	h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeWarning, "ColdStartFallback",
		"Pod %s: XPU restore retries exhausted (%d/%d), falling back to cold start",
		dest.Pod.String(), attempts, retryCount)
}

// markPodRestoreSucceeded stamps the re-restore annotation on a successfully
// restored replica pod, so a later kubelet restart of the container is
// recognized by xpu-container-runtime and XDM self-drives cr-tool.
// Idempotent: already stamped returns directly; a pod that no longer exists
// (deleted / evicted) counts as done - no object to stamp, no re-restore need.
func (h *RestoreXPUHandler) markPodRestoreSucceeded(ctx context.Context,
	dest *v1beta1.JobDestinationStatus) error {
	pod := &corev1.Pod{}
	err := h.sharedContext.Client.Get(ctx, client.ObjectKey{
		Namespace: dest.Pod.Namespace,
		Name:      dest.Pod.Name,
	}, pod)
	if errors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if pod.Annotations[AnnotationRestoreSucceeded] == "true" {
		return nil
	}
	patch := client.MergeFrom(pod.DeepCopy())
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[AnnotationRestoreSucceeded] = "true"
	// The same patch records the first restore-success moment for troubleshooting (re-restore bypasses the controller and never updates it).
	pod.Annotations[AnnotationRestoreSucceededAt] = time.Now().UTC().Format(time.RFC3339)
	return h.sharedContext.Client.Patch(ctx, pod, patch)
}

// dispatchDestXPU dispatches the XPU restore of one replica: all cheap
// pre-checks (read pod, compose args, fetch the worker connection) run on the
// reconcile thread, then the only expensive RPC is handed to a background
// goroutine and this round of reconcile returns immediately.
//
// dest staying XPURestoring means unsettled this round; Succeeded/Failed means
// terminal.
func (h *RestoreXPUHandler) dispatchDestXPU(ctx context.Context,
	jobReq *v1beta1.ContainerStateMigrationJobRequest, dest *v1beta1.JobDestinationStatus) {
	ctxLog := log.FromContext(ctx)
	jobUID := string(jobReq.UID)
	key := destTaskKey(dest)

	// Remove the old entry first (the backoff-resend path enters with a
	// RetryWait entry): this round either dispatches successfully and
	// creates a new entry, or returns in place leaving no entry for the
	// next-round retry.
	xpuRestoreTasks.forget(jobUID, key)

	// The target container may have finished normally (restartPolicy=Never). An
	// exited container has no pid to restore device state onto, skip it; the
	// pod needs to be re-read for the latest container state.
	pod := &corev1.Pod{}
	err := h.sharedContext.Client.Get(ctx, client.ObjectKey{
		Namespace: dest.Pod.Namespace,
		Name:      dest.Pod.Name,
	}, pod)
	if errors.IsNotFound(err) {
		// WaitRestoreHandler just confirmed Running; NotFound here is an
		// abnormal window. Handle with the unified failure semantics (plan
		// A): record one attempt with backoff retry, cold start when
		// exhausted - the cold-start handler waits for NotFound first and
		// then builds a same-named pod, naturally continuous.
		h.handleXPUFailure(ctx, jobReq, dest, "PodDisappeared",
			fmt.Sprintf("Target pod %s disappeared before XPU restore", dest.Pod.String()))
		return
	}
	if err != nil {
		ctxLog.Error(err, "Failed to get target pod", "pod", dest.Pod.String())
		return
	}

	containerCheckpointDir := make(map[string]string, len(dest.ContainerMigrationStatuses))
	for j := range dest.ContainerMigrationStatuses {
		cms := &dest.ContainerMigrationStatuses[j]
		if cms.ContainerID == "" || cms.SnapshotPath == "" {
			markDestFailed(dest, fmt.Sprintf(
				"container %s has no containerID or snapshotPath, cannot restore XPU state", cms.Name))
			return
		}
		// A normally-exited container has no live pid, device state cannot be restored (and needs not to be).
		if containerRunning(pod, cms.Name) {
			containerCheckpointDir[cms.ContainerID] = cms.SnapshotPath
		}
	}

	if len(containerCheckpointDir) == 0 {
		dest.Phase = v1beta1.DestPhaseSucceeded
		dest.StartupMode = v1beta1.StartupModeRestored
		dest.Message = "Containers restored; all exited normally, no XPU state to restore"
		ctxLog.Info("All restored containers already exited, skipping XPU restore", "pod", dest.Pod.String())
		return
	}

	workerClient, err := h.sharedContext.WorkerClientFor(ctx, jobUID, dest.NodeIP)
	if err != nil {
		dest.Message = fmt.Sprintf("Cannot reach worker on %s, will retry: %v", dest.NodeIP, err)
		return
	}

	req := &migrationworker.RestoreXPUOnNodeRequest{
		RequestID:              xpuRestoreRequestID(jobReq.Status.RequestID, dest.NodeIP),
		PodName:                dest.Pod.Name,
		PodNamespace:           dest.Pod.Namespace,
		ContainerCheckpointDir: containerCheckpointDir,
	}
	nodeIP := dest.NodeIP
	podRef := dest.Pod.String()
	containerCount := len(containerCheckpointDir)

	// Dedicated ctx: the goroutine outlives this reconcile. Using the
	// reconcile ctx means the RPC disconnects halfway once it is cancelled
	// (a per-reconcile timeout in controller-runtime would be a reasonable
	// evolution) - while the cr-tool inside XDM is still running, exactly
	// the situation to avoid.
	//
	// No deadline of its own: the RPC runs until the worker returns, bounded
	// remotely by the XDM-side cr-tool timeout (2h). The WithNoRPCTimeout
	// mark keeps the client's fallback rpc-timeout off this call.
	taskCtx := migrationworker.WithNoRPCTimeout(context.WithoutCancel(ctx))

	xpuRestoreTasks.start(jobUID, key)
	dest.Message = "Restoring XPU state on node"
	ctxLog.Info("Dispatched XPU restore", "pod", podRef, "containers", containerCount)

	go func() {
		err := workerClient.RestoreXPUOnNode(taskCtx, req)
		switch {
		case err == nil:
			xpuRestoreTasks.finish(jobUID, key, true,
				"Containers restored and XPU state restored", false)
		case isTransientError(err):
			// Connection jitter: drop the connection, the subsequent
			// reconcile resends the whole group after backoff (the worker
			// executes synchronously, no residue task)
			_ = h.sharedContext.RemoveWorkerClientForWithCallback(jobUID, nodeIP, func() error { return nil })
			xpuRestoreTasks.finish(jobUID, key, false,
				fmt.Sprintf("XPU restore transient failure, will retry: %v", err), true)
			ctxLog.Info("Transient failure while restoring XPU state, will retry",
				"pod", podRef, "error", err)
		default:
			// Deterministic failure (incl. cr-tool execution
			// failure/timeout): write into the table, applyDestResult
			// re-runs cr-tool in place per plan A, cold start when
			// exhausted (D2 confirmed repeated execution is safe).
			xpuRestoreTasks.finish(jobUID, key, false,
				fmt.Sprintf("Restore XPU state failed: %v", err), false)
		}
	}()
}

// xpuRestoreRequestID generates the requestID of RestoreXPUOnNode. The
// worker-side call is synchronous and stateless, the requestID only correlates
// logs; the "-xpu" suffix distinguishes it from the data download requestID
// (nodeRestoreRequestID) at a glance during troubleshooting.
func xpuRestoreRequestID(baseRequestID, nodeIP string) string {
	return nodeRestoreRequestID(baseRequestID, nodeIP) + "-xpu"
}
