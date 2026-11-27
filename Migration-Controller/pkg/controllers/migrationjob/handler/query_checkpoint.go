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
	migrationstatus "github.com/container-migration/controller/pkg/status"
	"github.com/container-migration/controller/pkg/util"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// QueryCheckpointHandler polls the checkpoint task status; once the checkpoint
// succeeds it deletes the source pod (the controller owns this step now) and
// advances the task to Restoring / Succeeded.
type QueryCheckpointHandler struct {
	sharedContext *controllers.SharedContext
}

var _ = (controllers.Handler[*v1beta1.ContainerStateMigrationJobRequest])(&QueryCheckpointHandler{})

func NewQueryCheckpointHandler(sharedContext *controllers.SharedContext) *QueryCheckpointHandler {
	return &QueryCheckpointHandler{sharedContext: sharedContext}
}

func (h *QueryCheckpointHandler) GetName() string {
	return "QueryCheckpointHandler"
}

func (h *QueryCheckpointHandler) Handle(ctx context.Context, jobReq *v1beta1.ContainerStateMigrationJobRequest) (result controllers.Result) {
	ctxLog := log.FromContext(ctx)
	// Boilerplate logs are emitted uniformly by controller.go; not repeated here.

	status := &jobReq.Status

	// Only run in the Checkpointing phase.
	if status.Phase != v1beta1.JobPhaseCheckpointing || status.CheckpointStatus == nil {
		return controllers.NewSkipResult("not in Checkpointing phase")
	}
	if status.CheckpointStatus.Phase != v1beta1.JobPhaseCheckpointing {
		return controllers.NewSkipResult("checkpointStatus not in Checkpointing")
	}

	// Only recognize the source node IP pinned in status; no live source pod
	// lookup: the controller deletes the source pod once the checkpoint
	// succeeds (the point of migration is to free the source node); the query
	// would then stay NotFound and the task would wait for nothing until
	// timeout.
	nodeIP := resolveSourceNodeIP(ctx, h.sharedContext, jobReq)
	if nodeIP == "" {
		msg := "Source node IP is unknown, cannot query checkpoint progress"
		status.Phase = v1beta1.JobPhaseFailed
		status.CheckpointStatus.Phase = v1beta1.JobPhaseFailed
		status.Message = msg
		status.CheckpointStatus.Message = msg
		return controllers.NewDoneResult()
	}

	workerClient, err := h.sharedContext.WorkerClientFor(ctx, string(jobReq.UID), nodeIP)
	if err != nil {
		return controllers.NewErrorResult(err)
	}

	response, err := workerClient.QueryCheckpointToStorageStatus(ctx,
		&migrationworker.QueryCheckpointToStorageStatusRequest{
			RequestID: status.RequestID,
		})
	if err != nil {
		ctxLog.Error(err, "Failed to query checkpoint status", "nodeIP", nodeIP)
		return controllers.NewErrorResult(err)
	}

	// Task not found: re-trigger the checkpoint.
	if response.Status == migrationstatus.StatusNotFound {
		status.CheckpointStatus.Phase = v1beta1.JobPhasePending
		status.CheckpointStatus.Message = "Task not found, will retry checkpoint"
		status.Phase = v1beta1.JobPhasePending
		return controllers.NewAbortWithRetryAfterResult(h.sharedContext.Config.RetryWaitTime)
	}

	// Update the checkpoint attributes of each container.
	for i := range status.CheckpointStatus.ContainerMigrationStatuses {
		cms := &status.CheckpointStatus.ContainerMigrationStatuses[i]
		attr, ok := response.Data[cms.Name]
		if !ok {
			continue
		}
		cms.CheckpointAttributes = &v1beta1.CheckpointAttributes{
			Path:          attr.Path,
			Size:          attr.Size,
			FileCount:     attr.FileCount,
			TotalDuration: time.Duration(attr.TotalDuration),
			Extras:        attr.Extras,
		}
	}

	// Success.
	if response.Status == migrationstatus.StatusOK {
		// The checkpoint data is fully landed on storage; the source pod has
		// served its purpose and the controller deletes it to free the source
		// node. Delete first, advance the phase only after it lands: a failed
		// delete leaves every phase untouched, so the next round re-queries
		// (idempotent OK) and retries the delete instead of the task advancing
		// past the only execution point of this step.
		if err := h.deleteSourcePod(ctx, jobReq); err != nil {
			ctxLog.Info("Failed to delete source pod after checkpoint, will retry", "error", err)
			h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeWarning, "SourcePodDeleteRetry",
				"Failed to delete source pod %s/%s after checkpoint, will retry: %v",
				jobReq.Spec.Checkpoint.SourcePodNamespace, jobReq.Spec.Checkpoint.SourcePodName, err)
			return controllers.NewAbortWithRetryAfterResult(h.sharedContext.Config.RetryWaitTime)
		}

		status.CheckpointStatus.Phase = v1beta1.JobPhaseSucceeded
		status.CheckpointStatus.Message = response.Message
		status.CheckpointStatus.Conditions = util.AddOrUpdateCondition(&metav1.Condition{
			Type:               v1beta1.JobConditionCheckpointed,
			Status:             metav1.ConditionTrue,
			LastTransitionTime: metav1.Now(),
			Reason:             "CheckpointSucceeded",
			Message:            "All containers checkpointed successfully",
		}, status.CheckpointStatus.Conditions)

		// SourcePodTemplate / SourcePodMeta were already pinned in InitHandler; no source pod lookup here.

		status.Conditions = util.AddOrUpdateCondition(&metav1.Condition{
			Type:               v1beta1.JobConditionCheckpointed,
			Status:             metav1.ConditionTrue,
			LastTransitionTime: metav1.Now(),
			Reason:             "CheckpointSucceeded",
			Message:            "All containers checkpointed successfully",
		}, status.Conditions)

		h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeNormal, "CheckpointSucceeded",
			"All containers checkpointed successfully")

		// With a restore stage, enter Restoring; otherwise Succeeded directly.
		if jobReq.Spec.Restore != nil {
			status.Phase = v1beta1.JobPhaseRestoring
			status.RestoreStatus.Phase = v1beta1.JobPhasePending
		} else {
			status.Phase = v1beta1.JobPhaseSucceeded
			status.Message = "Checkpoint completed"
		}
		return controllers.NewAbortWithRetryAfterResult(h.sharedContext.Config.RetryWaitTime)
	}

	// Still in progress.
	if response.Status == migrationstatus.StatusPending {
		return controllers.NewAbortWithRetryAfterResult(h.sharedContext.Config.CheckpointRetryWaitTime)
	}

	// Failed.
	status.CheckpointStatus.Phase = v1beta1.JobPhaseFailed
	status.CheckpointStatus.Message = response.Message
	status.Phase = v1beta1.JobPhaseFailed
	status.Message = response.Message
	h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeWarning, "CheckpointFailed", response.Message)
	return controllers.NewAbortWithRetryAfterResult(h.sharedContext.Config.RetryWaitTime)
}

// deleteSourcePod deletes the source pod after the checkpoint stage succeeds.
//
// Already gone counts as success (an external actor may have removed it); a
// live pod is deleted only when its UID still matches the spec - a same-named
// pod recreated in between (e.g. by a StatefulSet) is a different instance and
// must not be killed.
func (h *QueryCheckpointHandler) deleteSourcePod(ctx context.Context,
	jobReq *v1beta1.ContainerStateMigrationJobRequest) error {
	ctxLog := log.FromContext(ctx)
	cp := jobReq.Spec.Checkpoint

	pod := &corev1.Pod{}
	err := h.sharedContext.Client.Get(ctx, client.ObjectKey{
		Namespace: cp.SourcePodNamespace,
		Name:      cp.SourcePodName,
	}, pod)
	if errors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if string(pod.UID) != cp.SourcePodUID {
		ctxLog.Info("Source pod was recreated with a different UID, skipping delete",
			"pod", fmt.Sprintf("%s/%s", cp.SourcePodNamespace, cp.SourcePodName))
		return nil
	}

	if err := h.sharedContext.Client.Delete(ctx, pod); err != nil && !errors.IsNotFound(err) {
		return err
	}
	ctxLog.Info("Deleted source pod after checkpoint", "pod", fmt.Sprintf("%s/%s", cp.SourcePodNamespace, cp.SourcePodName))
	h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeNormal, "SourcePodDeleted",
		"Source pod %s/%s deleted after checkpoint", cp.SourcePodNamespace, cp.SourcePodName)
	return nil
}
