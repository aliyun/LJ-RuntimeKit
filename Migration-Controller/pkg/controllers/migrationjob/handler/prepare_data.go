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
	"sort"
	"time"

	"github.com/container-migration/controller/api/v1beta1"
	"github.com/container-migration/controller/pkg/controllers"
	"github.com/container-migration/controller/pkg/migrationworker"
	migrationstatus "github.com/container-migration/controller/pkg/status"
	"github.com/container-migration/controller/pkg/util"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// PrepareDataHandler waits for the target pods to be scheduled, then has the
// worker on the target node download the checkpoint tar from storage and
// extract it into the directory restore-data-path points to.
//
// Each replica advances independently: this handler only pushes forward the
// replicas it can, then always Continues to the downstream handlers -
// unscheduled, downloading, and failed replicas do not block ready replicas
// from going on through ReleaseInit -> WaitRestore -> RestoreXPU. Requeueing
// is owned by RestoreXPUHandler at the chain tail (it Aborts when unsettled
// replicas exist), so no Abort is needed here.
//
// Multiple replicas on the same node share one copy of the data (same
// requestID) and pull it only once - unchanged, so a download failure on a
// node still drags along all replicas on that node, which is the inherent
// semantic of shared data.
type PrepareDataHandler struct {
	sharedContext *controllers.SharedContext
}

var _ = (controllers.Handler[*v1beta1.ContainerStateMigrationJobRequest])(&PrepareDataHandler{})

func NewPrepareDataHandler(sharedContext *controllers.SharedContext) *PrepareDataHandler {
	return &PrepareDataHandler{sharedContext: sharedContext}
}

func (h *PrepareDataHandler) GetName() string {
	return "PrepareDataHandler"
}

func (h *PrepareDataHandler) Handle(ctx context.Context, jobReq *v1beta1.ContainerStateMigrationJobRequest) (result controllers.Result) {
	status := &jobReq.Status
	if jobReq.Spec.Restore == nil || status.Phase != v1beta1.JobPhaseRestoring ||
		status.RestoreStatus == nil {
		return controllers.NewSkipResult("not in Restoring phase")
	}
	// CreatePodsHandler sets RestoreStatus.Phase to Restoring after creating
	// the pods; this handler is responsible for preparing the data in that
	// phase.
	if status.RestoreStatus.Phase != v1beta1.JobPhaseRestoring {
		return controllers.NewSkipResult(fmt.Sprintf("restoreStatus phase is %s", status.RestoreStatus.Phase))
	}
	// The tar paths in CheckpointStatus are needed below to dispatch the task.
	if status.CheckpointStatus == nil {
		return controllers.NewErrorResult(fmt.Errorf("checkpointStatus is missing, cannot prepare restore data"))
	}

	// First resolve each replica's scheduling result, then group by node: one pull per node.
	nodeGroups := map[string][]*v1beta1.JobDestinationStatus{}
	for i := range status.RestoreStatus.Destinations {
		dest := &status.RestoreStatus.Destinations[i]
		if util.AssertJobPhaseIn(dest.Phase, v1beta1.DestPhaseSnapshotPrepared,
			v1beta1.DestPhaseInitReleased, v1beta1.DestPhaseXPURestoring,
			v1beta1.DestPhaseSucceeded, v1beta1.DestPhaseFailed,
			v1beta1.DestPhaseColdStarting) {
			continue
		}
		if !h.resolveScheduling(ctx, jobReq, dest) {
			// Unscheduled (keep waiting), pod gone (enters cold start), or in download retry backoff (keep waiting).
			continue
		}
		if destInDownloadBackoff(dest) {
			// Retry backoff after a download failure (3s): not due yet, keep waiting.
			continue
		}
		nodeGroups[dest.NodeIP] = append(nodeGroups[dest.NodeIP], dest)
	}

	// Query/dispatch only once per node, fan the result out to all replicas on that node.
	//
	// Per-node concurrency: both QueryRestoreFromStorageStatus and
	// RestoreFromStorage inside prepareNode are blocking calls that can wait up
	// to --rpc-timeout (default 2min) in the worst case. A serial traversal
	// would accumulate these waits - one dead worker on a node could delay the
	// dispatch of every healthy node in the same task by minutes. Each
	// goroutine only writes its own node's dests and nodeResults[i]; no shared
	// writes.
	type nodeWork struct {
		nodeIP string
		dests  []*v1beta1.JobDestinationStatus
	}
	works := make([]nodeWork, 0, len(nodeGroups))
	for nodeIP, dests := range nodeGroups {
		works = append(works, nodeWork{nodeIP: nodeIP, dests: dests})
	}

	// Nodes blocked by worker config drift + busy node are aggregated and written as the job-level WorkerBlocked.
	nodeResults := make([][]string, len(works))
	forEachConcurrently(ctx, works, func(i int, w nodeWork) {
		nodeResults[i] = h.prepareNode(ctx, jobReq, w.nodeIP, w.dests)
	})

	// Job-level failure (prepareNode has already set status.Phase to Failed): converge immediately.
	if status.Phase == v1beta1.JobPhaseFailed {
		return controllers.NewDoneResult()
	}

	blockedUsers := map[string]struct{}{}
	for _, users := range nodeResults {
		for _, u := range users {
			blockedUsers[u] = struct{}{}
		}
	}

	// Aggregate and write back WorkerBlocked: set True when any node is blocked, otherwise clear. Stable wording, no write storm.
	if len(blockedUsers) > 0 {
		users := make([]string, 0, len(blockedUsers))
		for u := range blockedUsers {
			users = append(users, u)
		}
		sort.Strings(users)
		msg := controllers.FormatWorkerBlockedMessage("restore-targets", users)
		if setWorkerBlockedConditionOnce(status, msg) {
			h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeWarning, "WorkerBlocked",
				"Waiting for stale worker to drain before replacement: %s", msg)
		}
	} else {
		clearWorkerBlockedCondition(status)
	}

	// Record RestoreInvoked once any replica's data is ready, for describe /
	// troubleshooting to confirm restore dispatch has actually started (the
	// restore stage itself has no timeout, see RoundCheckHandler; this
	// condition currently has no other consumer).
	if !util.AssertConditionInStatus(status.RestoreStatus.Conditions,
		v1beta1.RequestConditionRestoreInvoked, metav1.ConditionTrue) && h.anyDataPrepared(status) {
		status.RestoreStatus.Conditions = util.AddOrUpdateCondition(&metav1.Condition{
			Type:               v1beta1.RequestConditionRestoreInvoked,
			Status:             metav1.ConditionTrue,
			LastTransitionTime: metav1.Now(),
			Reason:             "SnapshotPrepared",
			Message:            "Checkpoint data prepared on at least one target node",
		}, status.RestoreStatus.Conditions)
	}
	// Let through no matter how many replicas are not ready: ready replicas must be able to proceed this round.
	return controllers.NewContinueResult()
}

// anyDataPrepared reports whether any replica's checkpoint data preparation is done (including later phases).
func (h *PrepareDataHandler) anyDataPrepared(status *v1beta1.ContainerStateMigrationJobRequestStatus) bool {
	for i := range status.RestoreStatus.Destinations {
		if util.AssertJobPhaseIn(status.RestoreStatus.Destinations[i].Phase,
			v1beta1.DestPhaseSnapshotPrepared, v1beta1.DestPhaseInitReleased,
			v1beta1.DestPhaseXPURestoring, v1beta1.DestPhaseSucceeded) {
			return true
		}
	}
	return false
}

// resolveScheduling confirms whether the replica pod is scheduled and
// backfills the node info. Returns true when it is scheduled and can enter
// data preparation; false when still waiting or the pod is gone (enters cold
// start).
func (h *PrepareDataHandler) resolveScheduling(ctx context.Context,
	jobReq *v1beta1.ContainerStateMigrationJobRequest, dest *v1beta1.JobDestinationStatus) bool {
	ctxLog := log.FromContext(ctx)

	pod := &corev1.Pod{}
	err := h.sharedContext.Client.Get(ctx, client.ObjectKey{
		Namespace: dest.Pod.Namespace,
		Name:      dest.Pod.Name,
	}, pod)
	if errors.IsNotFound(err) {
		enterColdStart(dest, "PodDisappeared", fmt.Sprintf("Target pod %s disappeared", dest.Pod.String()))
		h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeWarning, "ColdStartFallback",
			"Pod %s disappeared, falling back to cold start", dest.Pod.String())
		return false
	}
	if err != nil {
		ctxLog.Error(err, "Failed to get target pod", "pod", dest.Pod.String())
		return false
	}
	if pod.Spec.NodeName == "" || pod.Status.HostIP == "" {
		dest.Phase = v1beta1.DestPhasePending
		dest.Message = "Waiting for scheduling"
		return false
	}
	dest.Pod.NodeName = pod.Spec.NodeName
	dest.Pod.UID = pod.UID
	dest.Pod.IP = pod.Status.PodIP
	dest.NodeIP = pod.Status.HostIP
	return true
}

// prepareNode prepares the shared data for all replicas on one node: queries
// / dispatches the download task only once, then fans the result out to every
// replica on that node.
//
// Replicas on the same node share one requestID and one copy of on-disk
// data, so their data preparation results are naturally identical (ready
// together or failed together); different nodes do not affect each other.
func (h *PrepareDataHandler) prepareNode(ctx context.Context, jobReq *v1beta1.ContainerStateMigrationJobRequest,
	nodeIP string, dests []*v1beta1.JobDestinationStatus) (blockedUsers []string) {
	ctxLog := log.FromContext(ctx)
	nodeName := dests[0].Pod.NodeName
	requestID := nodeRestoreRequestID(jobReq.Status.RequestID, nodeIP)

	// Whether this node has not been dispatched yet: replacing a drifted
	// worker is allowed only when all replicas are still in Pending/empty
	// phase. Once any replica has entered Scheduled (downloading), the worker
	// underneath it must never be replaced.
	allowReplace := true
	for _, d := range dests {
		if d.Phase != "" && d.Phase != v1beta1.DestPhasePending {
			allowReplace = false
			break
		}
	}

	// The target node must first have a worker matching the current config:
	// PVC mode uses the task-level worker, other modes use the shared worker.
	var res controllers.WorkerEnsureResult
	if isPVCStorage(&jobReq.Spec) {
		res = h.sharedContext.EnsureTaskWorker(ctx, nodeName, nodeIP, string(jobReq.UID), jobReq.Spec.Storage.PVC)
	} else {
		res = h.sharedContext.EnsureWorkerUpToDate(ctx, nodeName, nodeIP, string(jobReq.UID), allowReplace)
	}
	switch res.State {
	case controllers.WorkerFailed:
		// Illegal config / delete failure: do not fail the replica (it self-heals once the config is fixed); skip dispatch this round and retry later.
		ctxLog.Error(res.Err, "Worker not ready on target node, will retry", "node", nodeName, "nodeIP", nodeIP)
		for _, d := range dests {
			d.Message = "Waiting for migration worker on target node"
		}
		return nil
	case controllers.WorkerWaiting:
		for _, d := range dests {
			d.Message = "Waiting for migration worker on target node"
		}
		if res.Blocked {
			ctxLog.Info("Target node worker blocked by in-flight tasks, waiting to drain",
				"node", nodeName, "nodeIP", nodeIP, "reason", res.Reason)
			return res.Users
		}
		return nil
	}

	workerClient, err := h.sharedContext.WorkerClientFor(ctx, string(jobReq.UID), nodeIP)
	if err != nil {
		return nil
	}

	// Judge whether it was already dispatched by the worker's task table.
	// Multiple replicas on the same node share one requestID, so after the
	// first replica triggers the download, the others hit on the query and do
	// not pull again.
	existing, err := workerClient.QueryRestoreFromStorageStatus(ctx,
		&migrationworker.QueryRestoreFromStorageStatusRequest{RequestID: requestID})
	if err != nil {
		if isTransientError(err) {
			_ = h.sharedContext.RemoveWorkerClientForWithCallback(string(jobReq.UID), nodeIP, func() error { return nil })
			return
		}
		ctxLog.Error(err, "Failed to probe existing restore task", "nodeIP", nodeIP)
		return
	}
	if existing.Status != migrationstatus.StatusNotFound {
		h.applyRestoreStatusToNode(ctx, jobReq, nodeIP, dests, existing)
		return
	}

	containerCheckpointPath := map[string]string{}
	for i := range jobReq.Status.CheckpointStatus.ContainerMigrationStatuses {
		cms := &jobReq.Status.CheckpointStatus.ContainerMigrationStatuses[i]
		if cms.CheckpointAttributes == nil || cms.CheckpointAttributes.Path == "" {
			h.handleNodeDownloadFailure(ctx, jobReq, nodeIP, requestID, dests,
				"NoCheckpointDataPath", fmt.Sprintf("Container %s has no checkpoint data path", cms.Name))
			return
		}
		// Path is the checkpoint directory (containing part-00.tar ...), no longer a single tar file.
		containerCheckpointPath[cms.Name] = cms.CheckpointAttributes.Path

		// Validate part completeness before dispatch.
		//
		// FileCount is the authoritative part count recorded on the
		// checkpoint side. Both the worker and the preheat side discover
		// the actual parts on storage by probing, and pure probing has a
		// blind spot: it cannot distinguish "originally 7 parts" from
		// "should be 8 but the tail one is lost" - when 00..06 exist and 07
		// is missing, the sequence is still continuous and the continuity
		// check passes, ending with one part fewer and the container
		// silently missing files.
		//
		// Validate the expected count here rather than passing it through
		// to the worker: first, RestoreFromStorageRequest stays unchanged
		// (proto untouched); second, the failure happens before dispatch,
		// earlier and easier to troubleshoot.
		if err := h.verifyCheckpointParts(ctx, jobReq, cms); err != nil {
			h.handleNodeDownloadFailure(ctx, jobReq, nodeIP, requestID, dests,
				"CheckpointPartsIncomplete",
				fmt.Sprintf("Container %s checkpoint data is incomplete: %v", cms.Name, err))
			return
		}
	}

	req := &migrationworker.RestoreFromStorageRequest{
		RequestID:    requestID,
		PodName:      dests[0].Pod.Name,
		PodNamespace: dests[0].Pod.Namespace,
		// PodUID / Version are only used for path isolation on the worker
		// side, consistent with CreatePodsHandler's restore-data-path: CR
		// UID + fixed sharedVersion so replicas on the same node share one
		// copy of data.
		PodUID:                  string(jobReq.UID),
		Version:                 sharedVersion,
		ContainerCheckpointPath: containerCheckpointPath,
		StorageType:             jobReq.Spec.StorageType,
		Storage:                 convertJobStorageToGrpcStorage(jobReq.Spec.Storage),
		HomeDir:                 h.sharedContext.Config.CheckpointDataRootDir,
	}
	// Flat layout: data is extracted directly to <hostPath>/data; when not
	// flat, DataDir is left empty and the worker uses
	// HomeDir/<CR-UID>/shared/<container>/checkpoint.
	if dataDir, _, ok := restoreFlatDataDir(jobReq); ok {
		req.DataDir = dataDir
	}

	// Register usage before dispatch: covers the "just dispatched, status not
	// yet persisted" window that APIReader cannot see either, so a concurrent
	// job judging this node's busy state will not wrongly delete the worker
	// this task is using.
	h.sharedContext.MarkWorkerInUse(nodeIP, string(jobReq.UID))

	response, err := workerClient.RestoreFromStorage(ctx, req)
	if err != nil {
		if isTransientError(err) {
			// In PVC mode workerClient is the task-level worker (cache key
			// nodeIP:port), so the jobUID-carrying variant must be used to drop
			// the right entry; consistent with the transient error branch of
			// QueryRestoreFromStorageStatus above.
			_ = h.sharedContext.RemoveWorkerClientForWithCallback(string(jobReq.UID), nodeIP, func() error { return nil })
			return
		}
		ctxLog.Error(err, "Failed to call RestoreFromStorage", "nodeIP", nodeIP)
		h.handleNodeDownloadFailure(ctx, jobReq, nodeIP, requestID, dests,
			"RestoreFromStorageCallFailed", fmt.Sprintf("Call RestoreFromStorage failed: %v", err))
		return
	}
	if response.Status != migrationstatus.StatusOK {
		h.handleNodeDownloadFailure(ctx, jobReq, nodeIP, requestID, dests,
			"RestoreFromStorageRejected", fmt.Sprintf("RestoreFromStorage rejected: %s", response.Message))
		return
	}

	for _, d := range dests {
		d.Phase = v1beta1.DestPhaseScheduled
		d.Message = "Downloading and extracting checkpoint data"
	}
	ctxLog.Info("Requested checkpoint data preparation", "nodeIP", nodeIP, "replicas", len(dests))
	return nil
}

// applyRestoreStatusToNode fans the extraction progress returned by the worker out to all replicas on that node.
//
// nodeIP is only for logging: in a multi-node task, "data prepared" without
// a node marker makes it impossible to tell which node finished and which is
// still stuck.
func (h *PrepareDataHandler) applyRestoreStatusToNode(ctx context.Context,
	jobReq *v1beta1.ContainerStateMigrationJobRequest, nodeIP string,
	dests []*v1beta1.JobDestinationStatus, response *migrationworker.QueryRestoreFromStorageStatusResponse) {
	ctxLog := log.FromContext(ctx)

	switch response.Status {
	case migrationstatus.StatusPending:
		for _, dest := range dests {
			if dest.Phase != v1beta1.DestPhaseScheduled {
				dest.Phase = v1beta1.DestPhaseScheduled
			}
			dest.Message = "Extracting checkpoint data"
		}
	case migrationstatus.StatusOK:
		for _, dest := range dests {
			for i := range dest.ContainerMigrationStatuses {
				cms := &dest.ContainerMigrationStatuses[i]
				if attrs, ok := response.Data[cms.Name]; ok && attrs != nil && attrs.Path != "" {
					// Take the path actually returned by the worker to avoid drift between the two sides' path-joining logic.
					cms.SnapshotPath = attrs.Path
				}
				cms.Conditions = util.AddOrUpdateCondition(&metav1.Condition{
					Type:               v1beta1.JobConditionRestored,
					Status:             metav1.ConditionTrue,
					LastTransitionTime: metav1.Now(),
					Reason:             "SnapshotPrepared",
					Message:            "Checkpoint data extracted",
				}, cms.Conditions)
			}
			dest.Phase = v1beta1.DestPhaseSnapshotPrepared
			dest.Message = "Checkpoint data ready"
		}
		ctxLog.Info("Checkpoint data prepared", "nodeIP", nodeIP, "replicas", len(dests))
	default:
		h.handleNodeDownloadFailure(ctx, jobReq, nodeIP,
			nodeRestoreRequestID(jobReq.Status.RequestID, nodeIP), dests,
			"PrepareDataFailed", fmt.Sprintf("Prepare checkpoint data failed: %s", response.Message))
	}
}

// handleNodeDownloadFailure is the unified exit for download-stage failures:
// first CleanTask(cleanData=true) to clear the failed task and leftover data
// on the worker (PushIfAbsent does not overwrite a failed task, it must be
// cleared before redispatch), then count retries per replica independently:
// not exhausted -> fall back to Pending (redispatch next round, the retry is
// initiated by the controller); exhausted -> cold start.
//
// Same-node sharing semantics: replicas on the same node share the requestID
// and one copy of on-disk data, so CleanTask is called only once; each replica
// counts retries independently and enters retry or cold start on its own.
func (h *PrepareDataHandler) handleNodeDownloadFailure(ctx context.Context,
	jobReq *v1beta1.ContainerStateMigrationJobRequest, nodeIP, requestID string,
	dests []*v1beta1.JobDestinationStatus, reason, message string) {
	ctxLog := log.FromContext(ctx)
	retryCount := retryCountOf(jobReq)

	cleaner := &CleanHandler{sharedContext: h.sharedContext}
	if err := cleaner.cleanTask(ctx, string(jobReq.UID), nodeIP, requestID, true, nil); err != nil {
		ctxLog.Info("Failed to clean failed download task before retry", "nodeIP", nodeIP, "error", err)
	}

	for _, d := range dests {
		attempts := recordAttempt(d, v1beta1.RetryPhaseDownload, reason, message, retryCount)
		if attempts > retryCount {
			enterColdStart(d, fmt.Sprintf("Download retries exhausted (%d/%d): %s", attempts, retryCount, reason),
				message)
			h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeWarning, "ColdStartFallback",
				"Pod %s: download failed %d times (retryCount=%d), falling back to cold start: %s",
				d.Pod.String(), attempts, retryCount, message)
			continue
		}
		// Fall back to Pending: next round resolveScheduling -> prepareNode
		// redispatches (CleanTask has already cleared). The redispatch pace is
		// controlled by destInDownloadBackoff (3s backoff).
		d.Phase = v1beta1.DestPhasePending
		d.Message = fmt.Sprintf("Download failed (%d/%d), will retry: %s", attempts, retryCount, message)
		h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeWarning, "DownloadRetry",
			"Pod %s: %s (attempt %d/%d)", d.Pod.String(), message, attempts, retryCount)
	}
}

// destInDownloadBackoff reports whether the replica is in the 3s download
// retry backoff period. Only effective when Phase==Pending (falling back to
// redispatch), to avoid hurting normal scheduling waits.
func destInDownloadBackoff(dest *v1beta1.JobDestinationStatus) bool {
	if dest.Phase != v1beta1.DestPhasePending {
		return false
	}
	entry := retryStatusOf(dest, v1beta1.RetryPhaseDownload)
	if entry == nil || len(entry.History) == 0 {
		return false
	}
	last := entry.History[len(entry.History)-1].Time.Time
	return time.Since(last) < phaseRetryBackoff
}

// markDestFailed marks a replica as terminally failed and leaves the reason on each container condition.
func markDestFailed(dest *v1beta1.JobDestinationStatus, message string) {
	dest.Phase = v1beta1.DestPhaseFailed
	dest.Message = message
	for i := range dest.ContainerMigrationStatuses {
		dest.ContainerMigrationStatuses[i].Conditions = util.AddOrUpdateCondition(&metav1.Condition{
			Type:               v1beta1.JobConditionRestored,
			Status:             metav1.ConditionFalse,
			LastTransitionTime: metav1.Now(),
			Reason:             "RestoreFailed",
			Message:            message,
		}, dest.ContainerMigrationStatuses[i].Conditions)
	}
}
