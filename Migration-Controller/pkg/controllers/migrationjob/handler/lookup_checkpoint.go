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

	"sigs.k8s.io/controller-runtime/pkg/log"
)

// LookupCheckpointHandler gets the historical CRD info by checkpointUID in Restore-Only mode.
type LookupCheckpointHandler struct {
	sharedContext *controllers.SharedContext
}

var _ = (controllers.Handler[*v1beta1.ContainerStateMigrationJobRequest])(&LookupCheckpointHandler{})

func NewLookupCheckpointHandler(sharedContext *controllers.SharedContext) *LookupCheckpointHandler {
	return &LookupCheckpointHandler{sharedContext: sharedContext}
}

func (h *LookupCheckpointHandler) GetName() string {
	return "LookupCheckpointHandler"
}

func (h *LookupCheckpointHandler) Handle(ctx context.Context, jobReq *v1beta1.ContainerStateMigrationJobRequest) (result controllers.Result) {
	ctxLog := log.FromContext(ctx)
	// Boilerplate logs are emitted uniformly by controller.go; not repeated here.

	status := &jobReq.Status
	spec := &jobReq.Spec

	// Only run in Restore-Only mode (checkpoint=nil, restore!=nil).
	if spec.Checkpoint != nil || spec.Restore == nil {
		return controllers.NewSkipResult("not restore-only mode")
	}
	// Only run in Restoring + Pending phases (meaning the checkpoint has not been looked up yet).
	if status.Phase != v1beta1.JobPhaseRestoring || status.RestoreStatus == nil || status.RestoreStatus.Phase != v1beta1.JobPhasePending {
		return controllers.NewSkipResult("not in Restoring/Pending phase")
	}
	// Skip if checkpointStatus already exists (already looked up).
	if status.CheckpointStatus != nil {
		return controllers.NewSkipResult("checkpoint already looked up")
	}

	checkpointUID := spec.Restore.CheckpointUID

	// Find the CRD by checkpointUID via list (K8s does not support a direct Get by UID).
	crdList := &v1beta1.ContainerStateMigrationJobRequestList{}
	if err := h.sharedContext.Client.List(ctx, crdList); err != nil {
		return controllers.NewErrorResult(err)
	}
	var sourceCRD *v1beta1.ContainerStateMigrationJobRequest
	for i := range crdList.Items {
		if string(crdList.Items[i].UID) == checkpointUID {
			sourceCRD = &crdList.Items[i]
			break
		}
	}
	if sourceCRD == nil {
		status.Phase = v1beta1.JobPhaseFailed
		status.Message = fmt.Sprintf("Checkpoint CRD with UID %s not found", checkpointUID)
		return controllers.NewDoneResult()
	}

	// Validate checkpointStatus.
	if sourceCRD.Status.CheckpointStatus == nil || sourceCRD.Status.CheckpointStatus.Phase != v1beta1.JobPhaseSucceeded {
		status.Phase = v1beta1.JobPhaseFailed
		status.Message = fmt.Sprintf("Referenced checkpoint CRD %s is not in Succeeded state", checkpointUID)
		return controllers.NewDoneResult()
	}

	if sourceCRD.Status.CheckpointStatus.SourcePodTemplate == nil {
		status.Phase = v1beta1.JobPhaseFailed
		status.Message = fmt.Sprintf("Referenced checkpoint CRD %s has no sourcePodTemplate", checkpointUID)
		return controllers.NewDoneResult()
	}

	// Copy checkpointStatus for restore.
	status.CheckpointStatus = sourceCRD.Status.CheckpointStatus.DeepCopy()

	ctxLog.Info("Successfully looked up checkpoint CRD", "checkpointUID", checkpointUID,
		"sourcePodTemplate", sourceCRD.Status.CheckpointStatus.SourcePodTemplate != nil)

	return controllers.NewContinueResult()
}
