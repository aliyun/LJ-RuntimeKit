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
	"time"

	"github.com/container-migration/controller/api/v1beta1"
	"github.com/container-migration/controller/pkg/controllers"
	"github.com/container-migration/controller/pkg/migrationworker"
	"github.com/container-migration/controller/pkg/util"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// CleanHandler cleans up worker-side task records and on-disk data once the task reaches a terminal phase.
//
// Cleanup policy:
//   - checkpoint tasks only drop the in-memory task records on the worker; the
//     tar on storage is NOT deleted - it may be referenced by a later
//     restore-only CR via checkpointUID.
//   - restore tasks use cleanData=true: tell the worker to delete memory/ and
//     xpu/ on the target node, but KEEP rootfs/. XDM currently mounts the
//     checkpoint rootfs as the resident overlay lowerdir for every restored
//     pod, so restore completion does not mean rootfs is unreferenced; actual
//     rootfs reclamation is handled separately by node-side GC.
type CleanHandler struct {
	sharedContext *controllers.SharedContext
}

var _ = (controllers.Handler[*v1beta1.ContainerStateMigrationJobRequest])(&CleanHandler{})

func NewCleanHandler(sharedContext *controllers.SharedContext) *CleanHandler {
	return &CleanHandler{sharedContext: sharedContext}
}

// IsTerminalPhase reports whether the task has reached a terminal phase.
//
// The predicate is shared because RoundCheckHandler and CleanHandler Skip
// branches must be strictly complementary: the former skips only in terminal
// phases, the latter only in non-terminal ones. controller.go no longer prints
// every skipped stage, so this complementarity is the sole guarantee of "at
// least one execution log line per round" - if both sides kept their own copy
// of the terminal phase list, a change on either side would silently break it.
func IsTerminalPhase(phase v1beta1.JobPhase) bool {
	return util.AssertJobPhaseIn(phase, v1beta1.JobPhaseSucceeded, v1beta1.JobPhasePartiallySucceeded,
		v1beta1.JobPhaseFailed, v1beta1.JobPhaseTerminated)
}

// CleanSkipsPhase reports whether CleanHandler skips in this phase (skips only in non-terminal phases).
func CleanSkipsPhase(phase v1beta1.JobPhase) bool {
	return !IsTerminalPhase(phase)
}

// RoundCheckSkipsPhase reports whether RoundCheckHandler skips in this phase (skips only in terminal phases).
func RoundCheckSkipsPhase(phase v1beta1.JobPhase) bool {
	return IsTerminalPhase(phase)
}

func (h *CleanHandler) GetName() string {
	return "CleanHandler"
}

func (h *CleanHandler) Handle(ctx context.Context, jobReq *v1beta1.ContainerStateMigrationJobRequest) (result controllers.Result) {
	ctxLog := log.FromContext(ctx)

	status := &jobReq.Status
	if CleanSkipsPhase(status.Phase) {
		return controllers.NewSkipResult(fmt.Sprintf("job not in terminal state (%s)", status.Phase))
	}
	if util.AssertConditionInStatus(status.Conditions, v1beta1.JobConditionCleaned, metav1.ConditionTrue) {
		// Must be Done, not Skip. Skip falls into the fallback branch of
		// Result.ToCtrlResult(), which yields RequeueAfter: 1s; this handler
		// is the last in the chain, so any cleaned terminal CR would re-run
		// all 12 handlers every second forever. With 20 terminal CRs the
		// real migration history gets pushed out of the log buffer at 500+
		// lines/s.
		//
		// Using Done loses no state: Done or Skip alike, this handler is
		// the chain tail and the loop ends either way; status is persisted
		// outside the loop. The heavy round (RestoreXPUHandler reclaims the
		// memory table + this handler cleans up and writes the Cleaned
		// condition) is provided by AbortWithRetryAfter in restore_xpu.go
		// and already finished before reaching this branch.
		return controllers.NewDoneResult()
	}

	// Restore side: after success, checkpoint data (rootfs/memory/xpu) is NOT
	// deleted - the restored pod keeps the source template's restartPolicy
	// (Always in inference scenarios), so a later kubelet restart re-restores
	// from memory/ (criu) and xpu/ (cr-tool); the data must survive until the
	// CR is deleted.
	// CleanTask(cleanData=false) is still called so the worker releases the
	// task records by requestID, otherwise records grow without bound and blow
	// up worker memory; data is kept (cleanData=false), and on CR deletion the
	// controller deletes by path via checkpointDirs instead of relying on this
	// record, so releasing the record early does not affect later cleanup.
	if status.RestoreStatus != nil {
		forEachConcurrently(ctx, uniqueDestNodeIPs(status.RestoreStatus.Destinations), func(_ int, nodeIP string) {
			h.cleanTask(ctx, string(jobReq.UID), nodeIP, nodeRestoreRequestID(status.RequestID, nodeIP), false, nil)
			h.sharedContext.ReleaseWorkerUse(nodeIP, string(jobReq.UID))
		})
	}

	// Checkpoint side: only drop the task records, keep the data on storage.
	if status.CheckpointStatus != nil && jobReq.Spec.Checkpoint != nil {
		if nodeIP := h.sourceNodeIP(ctx, jobReq); nodeIP != "" {
			h.cleanTask(ctx, string(jobReq.UID), nodeIP, status.RequestID, false, nil)
			h.sharedContext.ReleaseWorkerUse(nodeIP, string(jobReq.UID))
		}
	}

	// PVC mode: the task-level worker is deleted as its stage completes - when
	// the whole job reaches a terminal phase, delete all task-level workers on
	// the source node and every restore node, releasing the PVC mounts and
	// ports. Delete is idempotent.
	if isPVCStorage(&jobReq.Spec) {
		nodes := map[string]struct{}{}
		if cs := status.CheckpointStatus; cs != nil && cs.SourceNodeIP != "" {
			nodes[cs.SourceNodeIP] = struct{}{}
		}
		if status.RestoreStatus != nil {
			for i := range status.RestoreStatus.Destinations {
				if d := &status.RestoreStatus.Destinations[i]; d.NodeIP != "" {
					nodes[d.NodeIP] = struct{}{}
				}
			}
		}
		for nodeIP := range nodes {
			if err := h.sharedContext.DeleteTaskWorker(ctx, nodeIP, string(jobReq.UID)); err != nil {
				// No retry here (once the Cleaned condition is set the handler chain
				// will not re-enter): leftover workers from failed deletes are
				// reclaimed by CleanupOnDelete when the CR is deleted.
				ctxLog.Info("Failed to delete task worker on cleanup, skipping; will be cleaned up on CR deletion",
					"nodeIP", nodeIP, "error", err)
			}
		}
	}

	status.Conditions = util.AddOrUpdateCondition(&metav1.Condition{
		Type:               v1beta1.JobConditionCleaned,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             "Cleaned",
		Message:            "Checkpoint worker task cleaned; restore data retained until CR deletion",
	}, status.Conditions)
	ctxLog.Info("Cleaned worker tasks")
	return controllers.NewDoneResult()
}

// CleanupOnDelete does a final cleanup when the CR is deleted, called by the finalizer.
//
// It covers the cases CleanHandler cannot: the user deletes the CR before the
// task reaches a terminal phase. At that point the worker may still hold task
// records and extracted data may remain on target nodes, but the normal
// handler chain will no longer run. Everything is best-effort; failures are
// only logged.
func CleanupOnDelete(ctx context.Context, sharedContext *controllers.SharedContext,
	jobReq *v1beta1.ContainerStateMigrationJobRequest) {
	ctxLog := log.FromContext(ctx)
	ctxLog.Info("CR is being deleted, running final cleanup",
		"job", fmt.Sprintf("%s/%s", jobReq.Namespace, jobReq.Name))

	h := &CleanHandler{sharedContext: sharedContext}
	status := &jobReq.Status

	// The XPU restore in-memory task table is process-local; when the CR is
	// deleted directly (before reaching a terminal phase), the whole set of
	// RestoreXPUHandler reclamation hooks never fires, so it must be cleared
	// here explicitly, otherwise the entries stay in the process forever.
	forgetXPURestoreTasks(string(jobReq.UID))

	if status.RestoreStatus != nil {
		// Data is kept until CR deletion, and worker in-memory records may
		// already be lost after a worker restart/replacement, so pass each
		// replica's checkpoint dirs (SnapshotPath, deduplicated per node)
		// explicitly and let the worker delete memory/xpu by path, without
		// looking up records by requestID.
		nodeDirs := collectCheckpointDirsByNode(status.RestoreStatus.Destinations)
		// Clean up per node concurrently, same rationale as CleanHandler.Handle:
		// one unresponsive node must not block the others.
		forEachConcurrently(ctx, uniqueDestNodeIPs(status.RestoreStatus.Destinations), func(_ int, nodeIP string) {
			// When the worker is unreachable, rebuild it via EnsureWorker first and
			// retry, so that a dead worker during the terminal phase does not leak
			// memory/xpu forever (see cleanTaskEnsuringWorker).
			h.cleanTaskEnsuringWorker(ctx, jobReq, nodeNameByIP(status.RestoreStatus.Destinations, nodeIP), nodeIP,
				nodeRestoreRequestID(status.RequestID, nodeIP), true, nodeDirs[nodeIP])
			sharedContext.ReleaseWorkerUse(nodeIP, string(jobReq.UID))
		})
		for i := range status.RestoreStatus.Destinations {
			// The target pod is normally removed by the cascade deletion triggered
			// via ownerReference, but when the CR and the target pod are in different
			// namespaces ownerReference does not work, so delete it explicitly.
			deleteDestPod(ctx, sharedContext, jobReq, &status.RestoreStatus.Destinations[i])
		}
	}

	if status.CheckpointStatus != nil && jobReq.Spec.Checkpoint != nil {
		if nodeIP := h.sourceNodeIP(ctx, jobReq); nodeIP != "" {
			h.cleanTask(ctx, string(jobReq.UID), nodeIP, status.RequestID, false, nil)
			sharedContext.ReleaseWorkerUse(nodeIP, string(jobReq.UID))
		}
	}

	// PVC mode: fallback deletion of task-level workers. Runs after the
	// CleanTask data cleanup above: leftover workers (CR deleted mid-task /
	// delete action failed / controller crash window, etc.) are all reclaimed
	// here, releasing PVC mounts and ports. Delete is idempotent.
	if isPVCStorage(&jobReq.Spec) {
		nodes := map[string]struct{}{}
		if cs := status.CheckpointStatus; cs != nil && cs.SourceNodeIP != "" {
			nodes[cs.SourceNodeIP] = struct{}{}
		}
		if status.RestoreStatus != nil {
			for i := range status.RestoreStatus.Destinations {
				if d := &status.RestoreStatus.Destinations[i]; d.NodeIP != "" {
					nodes[d.NodeIP] = struct{}{}
				}
			}
		}
		for nodeIP := range nodes {
			if err := sharedContext.DeleteTaskWorker(ctx, nodeIP, string(jobReq.UID)); err != nil {
				ctxLog.Info("Failed to delete task worker on CR deletion, skipping", "nodeIP", nodeIP, "error", err)
			}
		}
	}
}

// deleteDestPod deletes a replica pod; already-gone counts as success.
func deleteDestPod(ctx context.Context, sharedContext *controllers.SharedContext,
	jobReq *v1beta1.ContainerStateMigrationJobRequest, dest *v1beta1.JobDestinationStatus) {
	ctxLog := log.FromContext(ctx)
	if dest.Pod == nil || dest.Pod.Name == "" {
		return
	}
	// Same-namespace cases are already covered by ownerReference.
	if dest.Pod.Namespace == jobReq.Namespace {
		return
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: dest.Pod.Name, Namespace: dest.Pod.Namespace},
	}
	if err := sharedContext.Client.Delete(ctx, pod); err != nil && !k8serrors.IsNotFound(err) {
		ctxLog.Error(err, "Failed to delete restore target pod on cleanup", "pod", dest.Pod.String())
		return
	}
	ctxLog.Info("Deleted restore target pod on cleanup", "pod", dest.Pod.String())
}

// cleanTask: a failed cleanup does not change the task conclusion, it is only
// logged and returned to the caller: the worker has its own fallback
// reclamation, while the caller (cleanTaskEnsuringWorker) uses the error to
// decide whether the worker is unreachable and should be rebuilt and retried.
// When checkpointDirs is non-empty the worker deletes memory/xpu by these
// paths (independent of in-memory task records); otherwise it falls back to
// looking up records by requestID.
func (h *CleanHandler) cleanTask(ctx context.Context, jobUID, nodeIP, requestID string, cleanData bool, checkpointDirs []string) error {
	ctxLog := log.FromContext(ctx)

	workerClient, err := h.sharedContext.WorkerClientFor(ctx, jobUID, nodeIP)
	if err != nil {
		ctxLog.Info("Worker client unavailable for clean, skip", "nodeIP", nodeIP, "error", err)
		return err
	}
	if err := workerClient.CleanTask(ctx, &migrationworker.CleanTaskRequest{
		RequestID:      requestID,
		CleanData:      cleanData,
		CheckpointDirs: checkpointDirs,
	}); err != nil {
		ctxLog.Info("Failed to clean worker task, skip",
			"nodeIP", nodeIP, "requestID", requestID, "error", err)
		return err
	}
	return nil
}

// cleanTaskEnsuringWorker is the final data cleanup on CR deletion: it first
// sends CleanTask directly; when the worker is unreachable (gRPC Unavailable
// and similar transient errors) it ensures the worker on that node is rebuilt
// and Ready, then retries once.
//
// Background: the worker is a bare pod whose lifecycle is bound to active CRs;
// a terminal CR no longer maintains it. If the worker dies between "restore
// succeeded" and "CR deleted" and is not rebuilt, CleanupOnDelete's CleanTask
// is skipped as best-effort because the worker is unreachable: memory/xpu are
// never deleted, and the reaper keeps rootfs because the memory/ sentinel is
// still there - checkpoint data leaks forever. This path adds an "ensure the
// worker exists then retry" step before deleting data, plugging the leak at
// the moment of CR deletion. It stays best-effort: if the worker cannot come
// up (e.g. the node is unreachable) it gives up after WorkerReadyTimeout and
// emits a Warning event, never blocking finalizer removal.
func (h *CleanHandler) cleanTaskEnsuringWorker(ctx context.Context, jobReq *v1beta1.ContainerStateMigrationJobRequest,
	nodeName, nodeIP, requestID string, cleanData bool, checkpointDirs []string) {
	ctxLog := log.FromContext(ctx)

	err := h.cleanTask(ctx, string(jobReq.UID), nodeIP, requestID, cleanData, checkpointDirs)
	if err == nil || !isTransientError(err) {
		// Success, or an error that rebuilding cannot fix: keep the original
		// best-effort semantics.
		return
	}
	if nodeName == "" {
		ctxLog.Info("Cleanup: worker unreachable and nodeName unknown, cannot ensure worker; checkpoint data may leak",
			"nodeIP", nodeIP, "error", err)
		h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeWarning, "CleanupWorkerUnavailable",
			"Worker on node %s unreachable and nodeName unknown; checkpoint data may leak", nodeIP)
		return
	}
	ctxLog.Info("Cleanup: worker unreachable, ensuring worker then retrying clean",
		"nodeIP", nodeIP, "node", nodeName, "error", err)
	if !h.ensureWorkerReady(ctx, nodeName, nodeIP, string(jobReq.UID)) {
		ctxLog.Info("Cleanup: worker did not become ready in time, giving up; checkpoint data may leak",
			"nodeIP", nodeIP, "node", nodeName)
		h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeWarning, "CleanupWorkerUnavailable",
			"Worker on node %s (%s) not ready within timeout; checkpoint data under %v may leak",
			nodeName, nodeIP, checkpointDirs)
		return
	}
	if err2 := h.cleanTask(ctx, string(jobReq.UID), nodeIP, requestID, cleanData, checkpointDirs); err2 != nil {
		ctxLog.Info("Cleanup: retry clean after ensure-worker still failed; checkpoint data may leak",
			"nodeIP", nodeIP, "node", nodeName, "error", err2)
		return
	}
	ctxLog.Info("Cleanup: clean succeeded after ensuring worker", "nodeIP", nodeIP, "node", nodeName)
}

// ensureWorkerReady makes sure nodeIP has a worker matching the current
// config and Running+Ready, rebuilding it when missing; polling is capped at
// Config.WorkerReadyTimeout. allowReplace=false: only bring the worker up for
// cleanup, never replace a running (config-drifted) worker. Returns whether it
// became ready within the timeout.
func (h *CleanHandler) ensureWorkerReady(ctx context.Context, nodeName, nodeIP, jobUID string) bool {
	ctxLog := log.FromContext(ctx)

	interval := h.sharedContext.Config.WorkerDrainPollInterval
	if interval <= 0 {
		interval = 10 * time.Second
	}
	deadline := time.Now().Add(h.sharedContext.Config.WorkerReadyTimeout)
	for {
		switch res := h.sharedContext.EnsureWorkerUpToDate(ctx, nodeName, nodeIP, jobUID, false); res.State {
		case controllers.WorkerReady:
			return true
		case controllers.WorkerFailed:
			ctxLog.Info("ensureWorkerReady: ensure worker failed",
				"node", nodeName, "reason", res.Reason, "error", res.Err)
			return false
		default: // WorkerWaiting: creating / waiting for ready, keep polling until timeout
			if time.Now().After(deadline) {
				ctxLog.Info("ensureWorkerReady: timeout waiting for worker ready",
					"node", nodeName, "reason", res.Reason)
				return false
			}
			select {
			case <-ctx.Done():
				ctxLog.Info("ensureWorkerReady: context done while waiting for worker", "node", nodeName)
				return false
			case <-time.After(interval):
			}
		}
	}
}

// nodeNameByIP looks up the nodeName by nodeIP from replica statuses
// (EnsureWorkerUpToDate needs nodeName to build the worker pod). For multiple
// replicas on the same node, any replica's Pod.NodeName works.
func nodeNameByIP(dests []v1beta1.JobDestinationStatus, nodeIP string) string {
	for i := range dests {
		if dests[i].NodeIP == nodeIP && dests[i].Pod != nil && dests[i].Pod.NodeName != "" {
			return dests[i].Pod.NodeName
		}
	}
	return ""
}

// collectCheckpointDirsByNode aggregates each replica container's checkpoint
// data dir (SnapshotPath) by node IP, deduplicated per node. On CR deletion
// these are passed to the worker explicitly to delete memory/xpu, independent
// of the worker's in-memory task records (which may be lost after a worker
// restart/replacement).
func collectCheckpointDirsByNode(dests []v1beta1.JobDestinationStatus) map[string][]string {
	result := make(map[string][]string)
	seen := make(map[string]map[string]struct{})
	for i := range dests {
		dest := &dests[i]
		if dest.NodeIP == "" {
			continue
		}
		if seen[dest.NodeIP] == nil {
			seen[dest.NodeIP] = make(map[string]struct{})
		}
		for j := range dest.ContainerMigrationStatuses {
			p := dest.ContainerMigrationStatuses[j].SnapshotPath
			if p == "" {
				continue
			}
			if _, ok := seen[dest.NodeIP][p]; ok {
				continue
			}
			seen[dest.NodeIP][p] = struct{}{}
			result[dest.NodeIP] = append(result[dest.NodeIP], p)
		}
	}
	return result
}

// sourceNodeIP prefers the pinned value in status, falling back to a live lookup on the source pod.
func (h *CleanHandler) sourceNodeIP(ctx context.Context, jobReq *v1beta1.ContainerStateMigrationJobRequest) string {
	return resolveSourceNodeIP(ctx, h.sharedContext, jobReq)
}

// resolveSourceNodeIP prefers CheckpointStatus.SourceNodeIP (pinned),
// falling back to a live lookup on the source pod.
func resolveSourceNodeIP(ctx context.Context, sharedContext *controllers.SharedContext,
	jobReq *v1beta1.ContainerStateMigrationJobRequest) string {
	if jobReq.Status.CheckpointStatus != nil && jobReq.Status.CheckpointStatus.SourceNodeIP != "" {
		return jobReq.Status.CheckpointStatus.SourceNodeIP
	}
	if jobReq.Spec.Checkpoint == nil {
		return ""
	}
	pod := &corev1.Pod{}
	err := sharedContext.Client.Get(ctx, client.ObjectKey{
		Namespace: jobReq.Spec.Checkpoint.SourcePodNamespace,
		Name:      jobReq.Spec.Checkpoint.SourcePodName,
	}, pod)
	if err != nil {
		return ""
	}
	return pod.Status.HostIP
}

// nodeRestoreRequestID: all replicas of this CR on the same node share one
// requestID so the worker pulls the data only once; different nodes are
// independent.
func nodeRestoreRequestID(baseRequestID, nodeIP string) string {
	return fmt.Sprintf("%s-restore-%s", baseRequestID, nodeIP)
}
