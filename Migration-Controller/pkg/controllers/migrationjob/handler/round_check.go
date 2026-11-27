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

	"github.com/container-migration/controller/api/v1beta1"
	"github.com/container-migration/controller/pkg/controllers"
	"github.com/container-migration/controller/pkg/util"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// RoundCheckHandler inspects cancellation and performs periodic cleanup
// (PVC-mode source task-worker reclamation).
//
// Note: the checkpoint stage no longer does timeout inspection -
// spec.checkpoint.timeoutInMinute is not a controller-side deadline anymore;
// when explicitly set it is passed through to cr-tool as --timeout (minutes),
// unset passes no --timeout at all, and spec.checkpoint.enableCheckpointAutoMode
// (default true) controls cr-tool's --enable-auto-mode. The checkpoint stage
// itself waits indefinitely. The restore stage has no timeout either
// (the spec.restore.timeoutInMinute field is deleted) - a restore CRD is
// equivalent to "an app deploying multiple pods"; pod Pending / CRD staying in
// Restoring is a normal state. runc-restore and XPU retries each have their own
// self-terminating path (exhausted -> cold start), and cold-start Pending has
// no timeout, waiting for recovery or manual intervention.
type RoundCheckHandler struct {
	sharedContext *controllers.SharedContext
}

var _ = (controllers.Handler[*v1beta1.ContainerStateMigrationJobRequest])(&RoundCheckHandler{})

func NewRoundCheckHandler(sharedContext *controllers.SharedContext) *RoundCheckHandler {
	return &RoundCheckHandler{sharedContext: sharedContext}
}

func (h *RoundCheckHandler) GetName() string {
	return "RoundCheckHandler"
}

func (h *RoundCheckHandler) Handle(ctx context.Context, jobReq *v1beta1.ContainerStateMigrationJobRequest) (result controllers.Result) {
	// The "Starting/Finished + result struct" boilerplate log pair is no longer
	// printed here: controller.go already records the result and duration of
	// every stage uniformly, duplicating it would triple the line count.

	ctxLog := log.FromContext(ctx)
	status := &jobReq.Status

	// Terminal phases skip directly.
	if RoundCheckSkipsPhase(status.Phase) {
		return controllers.NewSkipResult(fmt.Sprintf("job in terminal state (%s)", status.Phase))
	}

	// Check the cancel flag.
	if jobReq.Spec.Cancel {
		status.Phase = v1beta1.JobPhaseTerminated
		status.Message = "Task canceled by user"
		h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeWarning, "Canceled", "Migration job canceled by user")
		return controllers.NewAbortWithRetryAfterResult(h.sharedContext.Config.RetryWaitTime)
	}

	// Inspection: in PVC mode the checkpoint is done but the source node's
	// task-level worker still exists -> delete it. A leftover source worker
	// would keep the PVC mount and port occupied for a long time. Only one
	// Delete per round (idempotent); failure retries next round.
	//
	// Skip while the restore stage still uses the same node: the task-level
	// worker is keyed by node + jobUID, so checkpoint and restore on one node
	// share the same pod; deleting it here would fight EnsureTaskWorker and
	// stall the restore in an endless create/delete loop. That worker is then
	// cleaned up by CleanHandler once the whole job reaches a terminal phase.
	if isPVCStorage(&jobReq.Spec) {
		if cs := status.CheckpointStatus; cs != nil && jobReq.Spec.Checkpoint != nil &&
			util.AssertJobPhaseIn(cs.Phase, v1beta1.JobPhaseSucceeded, v1beta1.JobPhaseFailed) &&
			cs.SourceNodeIP != "" &&
			!restoreInProgressOnNode(status, cs.SourceNodeIP) {
			if err := h.sharedContext.DeleteTaskWorker(ctx, cs.SourceNodeIP, string(jobReq.UID)); err != nil {
				ctxLog.Info("Failed to delete source task worker, will retry", "nodeIP", cs.SourceNodeIP, "error", err)
			}
		}
	}

	return controllers.NewContinueResult()
}

// restoreInProgressOnNode reports whether the restore stage is still running
// on the given node: the job is in Restoring and some destination pod is
// scheduled onto it. A running restore keeps the shared task-level worker of
// that node alive, so the source-worker inspection must not delete it.
func restoreInProgressOnNode(status *v1beta1.ContainerStateMigrationJobRequestStatus, nodeIP string) bool {
	if status.Phase != v1beta1.JobPhaseRestoring || status.RestoreStatus == nil {
		return false
	}
	for i := range status.RestoreStatus.Destinations {
		if status.RestoreStatus.Destinations[i].NodeIP == nodeIP {
			return true
		}
	}
	return false
}
