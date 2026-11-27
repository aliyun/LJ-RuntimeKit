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

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// isPVCStorage judges whether the task uses PVC direct-attach storage (storageType=pvc).
func isPVCStorage(spec *v1beta1.ContainerStateMigrationJobRequestSpec) bool {
	return spec.StorageType == v1beta1.StorageTypePVC
}

// validatePVCOnInit is InitHandler's extra validation for PVC mode:
//  1. The PVC must really exist (missing -> Failed directly);
//  2. Tasks with a restore stage (restore-only or checkpoint+restore
//     dual-stage) must use RWX (accessModes includes ReadWriteMany) - restore
//     replicas may spread across nodes, RWO single-node mounting cannot satisfy
//     the semantics, fail the whole job with the reason; pure checkpoint tasks
//     (restore empty) are single-node writes, RWO/RWX both fine, no accessMode
//     restriction.
//
// Non-PVC mode or validation passed returns ContinueResult; validation failed
// returns DoneResult (status already written Failed); transient read failure
// returns ErrorResult (retry, not judging the task dead).
func (h *InitHandler) validatePVCOnInit(ctx context.Context,
	jobReq *v1beta1.ContainerStateMigrationJobRequest) controllers.Result {
	spec := &jobReq.Spec
	if !isPVCStorage(spec) {
		return controllers.NewContinueResult()
	}

	pvc := &corev1.PersistentVolumeClaim{}
	if err := h.sharedContext.Client.Get(ctx, client.ObjectKey{
		Namespace: spec.Storage.PVC.PvcNamespace,
		Name:      spec.Storage.PVC.PvcName,
	}, pvc); err != nil {
		if k8serrors.IsNotFound(err) {
			failPVCInit(h.sharedContext, jobReq, fmt.Sprintf("PVC %s/%s not found",
				spec.Storage.PVC.PvcNamespace, spec.Storage.PVC.PvcName))
			return controllers.NewDoneResult()
		}
		// Transient read failure: retry, not judging the task dead.
		return controllers.NewErrorResult(err)
	}

	// The restore stage enforces RWX: checkpoint is a single-node write so
	// RWO/RWX both fine; restore replicas may spread across nodes, RWX
	// (ReadWriteMany) is mandatory. A CR with a restore stage using a non-RWX
	// PVC fails the whole job directly.
	if jobReq.Spec.Restore != nil && !hasAccessMode(pvc.Spec.AccessModes, corev1.ReadWriteMany) {
		failPVCInit(h.sharedContext, jobReq, fmt.Sprintf(
			"PVC %s/%s is not RWX (ReadWriteMany): PVC direct-attach restore requires a ReadWriteMany PVC so restore replicas can span multiple nodes",
			spec.Storage.PVC.PvcNamespace, spec.Storage.PVC.PvcName))
		return controllers.NewDoneResult()
	}
	return controllers.NewContinueResult()
}

// hasAccessMode judges whether accessModes includes the target mode.
func hasAccessMode(modes []corev1.PersistentVolumeAccessMode, target corev1.PersistentVolumeAccessMode) bool {
	for _, m := range modes {
		if m == target {
			return true
		}
	}
	return false
}

// failPVCInit writes the PVC entry-validation failure into status and events, then lets the handler chain converge.
func failPVCInit(sharedContext *controllers.SharedContext,
	jobReq *v1beta1.ContainerStateMigrationJobRequest, msg string) {
	jobReq.Status.Phase = v1beta1.JobPhaseFailed
	jobReq.Status.Message = msg
	sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeWarning, "PVCValidationFailed", "%s", msg)
}
