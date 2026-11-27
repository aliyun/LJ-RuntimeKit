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
	"strconv"

	"github.com/container-migration/controller/api/v1beta1"
	"github.com/container-migration/controller/pkg/controllers"
	"github.com/container-migration/controller/pkg/migrationworker"
	migrationstatus "github.com/container-migration/controller/pkg/status"
	"github.com/container-migration/controller/pkg/util"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// CheckpointHandler calls worker.CheckpointToStorage.
type CheckpointHandler struct {
	sharedContext *controllers.SharedContext
}

// getRuntimekitMigrationHostPath finds the hostPath of the
// lj-runtimekit-migration mount from the volumes; empty string when not
// found.
func getRuntimekitMigrationHostPath(volumes []corev1.Volume) string {
	for i := range volumes {
		if volumes[i].Name == runtimekitMigrationVolumeName && volumes[i].HostPath != nil {
			return volumes[i].HostPath.Path
		}
	}
	return ""
}

var _ = (controllers.Handler[*v1beta1.ContainerStateMigrationJobRequest])(&CheckpointHandler{})

func NewCheckpointHandler(sharedContext *controllers.SharedContext) *CheckpointHandler {
	return &CheckpointHandler{sharedContext: sharedContext}
}

func (h *CheckpointHandler) GetName() string {
	return "CheckpointHandler"
}

func (h *CheckpointHandler) Handle(ctx context.Context, jobReq *v1beta1.ContainerStateMigrationJobRequest) (result controllers.Result) {
	ctxLog := log.FromContext(ctx)
	// Boilerplate logs are emitted uniformly by controller.go; not repeated here.

	status := &jobReq.Status
	spec := &jobReq.Spec

	// Only run in Pending with a checkpoint stage.
	if status.Phase != v1beta1.JobPhasePending || spec.Checkpoint == nil {
		return controllers.NewSkipResult("not in Pending phase or no checkpoint spec")
	}
	// checkpointStatus must exist and be Pending.
	if status.CheckpointStatus == nil || status.CheckpointStatus.Phase != v1beta1.JobPhasePending {
		return controllers.NewSkipResult("checkpointStatus not in Pending")
	}

	checkpointSpec := spec.Checkpoint

	// Get the source pod's nodeIP.
	pod := &corev1.Pod{}
	err := h.sharedContext.Client.Get(ctx, client.ObjectKey{
		Namespace: checkpointSpec.SourcePodNamespace,
		Name:      checkpointSpec.SourcePodName,
	}, pod)
	if err != nil {
		ctxLog.Error(err, "Failed to get source pod for checkpoint",
			"pod", fmt.Sprintf("%s/%s", jobReq.Namespace, checkpointSpec.SourcePodName))
		return controllers.NewErrorResult(err)
	}

	nodeIP := pod.Status.HostIP
	if nodeIP == "" {
		return controllers.NewErrorResult(fmt.Errorf("source pod %s/%s has no hostIP", pod.Namespace, pod.Name))
	}
	// Pin the source node: the controller deletes the source pod once the
	// checkpoint succeeds (the point of migration is to free the source node);
	// later polling and cleanup must not rely on looking up the source pod.
	status.CheckpointStatus.SourceNodeIP = nodeIP

	// Read the hostPath of the runtimekit migration mount on the source pod
	// and pass it through advanceOptions to worker -> XDM (XDM uses
	// <hostPath>/data as the checkpoint working directory).
	runtimekitHostPath := getRuntimekitMigrationHostPath(pod.Spec.Volumes)
	if runtimekitHostPath == "" {
		ctxLog.Info("Volume lj-runtimekit-migration not found on source pod, XDM will use its default work dir",
			"pod", pod.Name)
	}

	// Build the checkpoint policy.
	checkpointPolicy := make([]*migrationworker.CheckpointPolicy, 0)
	for _, cms := range status.CheckpointStatus.ContainerMigrationStatuses {
		policy := &migrationworker.CheckpointPolicy{
			ContainerName: cms.Name,
			ContainerID:   cms.ContainerID,
		}
		// Merge the user-specified advanceOptions from the CRD with the
		// hostPath injection (copy into a new map, avoiding direct mutation of
		// the reference in spec).
		advanceOptions := make(map[string]string)
		if cp, ok := checkpointSpec.ContainerCheckpointPolicy[cms.Name]; ok && cp != nil {
			policy.ExcludeFilepath = cp.ExcludeFilepath
			for k, v := range cp.AdvanceOptions {
				advanceOptions[k] = v
			}
		}
		if runtimekitHostPath != "" {
			advanceOptions[advanceOptKeyCheckpointHostPath] = runtimekitHostPath
		}
		// spec.checkpoint.timeoutInMinute no longer controls any controller-side
		// deadline (round_check does not time out the checkpoint stage); it is
		// only injected when explicitly set (0 included) and passed through to
		// cr-tool as --timeout <minutes>. Unset means the key is absent and
		// XDM omits --timeout from the cr-tool command line.
		if checkpointSpec.TimeoutInMinute != nil {
			advanceOptions[advanceOptKeyCheckpointTimeout] = strconv.Itoa(*checkpointSpec.TimeoutInMinute)
		}
		// Auto mode defaults to true when unset: cr-tool runs
		// --enable-auto-mode and detects the steady-state point of the model
		// service itself. Only an explicit false leaves the key absent and
		// XDM omits --enable-auto-mode.
		if checkpointSpec.EnableCheckpointAutoMode == nil || *checkpointSpec.EnableCheckpointAutoMode {
			advanceOptions[advanceOptKeyCheckpointAutoMode] = "true"
		}
		if len(advanceOptions) > 0 {
			policy.AdvanceOptions = advanceOptions
		}
		checkpointPolicy = append(checkpointPolicy, policy)
	}

	// Build the gRPC request.
	req := &migrationworker.CheckpointToStorageRequest{
		RequestID:    status.RequestID,
		PodName:      checkpointSpec.SourcePodName,
		PodNamespace: checkpointSpec.SourcePodNamespace,
		PodUID:       checkpointSpec.SourcePodUID,
		Version:      h.sharedContext.GenerateUnixTimeVersion(),
		Policy:       checkpointPolicy,
		StorageType:  spec.StorageType,
		Storage:      convertJobStorageToGrpcStorage(spec.Storage),
	}

	workerClient, err := h.sharedContext.WorkerClientFor(ctx, string(jobReq.UID), nodeIP)
	if err != nil {
		ctxLog.Info("Worker client not ready for checkpoint, will retry", "nodeIP", nodeIP, "error", err)
		return controllers.NewAbortWithRetryAfterResult(h.sharedContext.Config.RetryWaitTime)
	}

	// Register usage before dispatch: covers the "just dispatched, status not
	// yet persisted" window, so a concurrent job judging the source node's busy
	// state will not wrongly delete the worker this task is using.
	h.sharedContext.MarkWorkerInUse(nodeIP, string(jobReq.UID))

	response, err := workerClient.CheckpointToStorage(ctx, req)
	if err != nil {
		if isTransientError(err) {
			ctxLog.Info("CheckpointToStorage transient error, will retry", "nodeIP", nodeIP, "error", err)
			h.sharedContext.RemoveWorkerClientForWithCallback(string(jobReq.UID), nodeIP, func() error { return nil })
			return controllers.NewAbortWithRetryAfterResult(h.sharedContext.Config.RetryWaitTime)
		}
		ctxLog.Error(err, "Failed to call CheckpointToStorage", "nodeIP", nodeIP)
		h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeWarning, "CallCheckpointAPIFailed", "Call checkpoint api error: %v", err)
		return controllers.NewErrorResult(err)
	}

	if response.Status != migrationstatus.StatusOK {
		status.Phase = v1beta1.JobPhaseFailed
		status.CheckpointStatus.Phase = v1beta1.JobPhaseFailed
		status.Message = response.Message
		status.CheckpointStatus.Message = response.Message
		return controllers.NewAbortWithRetryAfterResult(h.sharedContext.Config.RetryWaitTime)
	}

	// Mark checkpoint as invoked.
	status.CheckpointStatus.Conditions = util.AddOrUpdateCondition(&metav1.Condition{
		Type:               v1beta1.RequestConditionCheckpointInvoked,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             "CheckpointInvoked",
		Message:            "Call checkpoint api successfully",
	}, status.CheckpointStatus.Conditions)
	status.CheckpointStatus.Phase = v1beta1.JobPhaseCheckpointing
	status.Phase = v1beta1.JobPhaseCheckpointing
	h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeNormal, "CheckpointStarted", "Checkpoint started for containers %v", checkpointSpec.Containers)
	return controllers.NewAbortWithRetryAfterResult(h.sharedContext.Config.RetryWaitTime)
}

// convertJobStorageToGrpcStorage converts JobStorage to gRPC Storage.
func convertJobStorageToGrpcStorage(storage *v1beta1.JobStorage) *migrationworker.Storage {
	if storage == nil {
		return nil
	}
	if storage.Local != nil {
		return &migrationworker.Storage{
			Local: &migrationworker.LocalStorage{
				Path: storage.Local.Path,
			},
		}
	}
	if storage.NFS != nil {
		return &migrationworker.Storage{
			Nfs: &migrationworker.NFS{
				Server:  storage.NFS.Server,
				Path:    storage.NFS.Path,
				Options: storage.NFS.Options,
			},
		}
	}
	if storage.OSS != nil {
		return &migrationworker.Storage{
			Oss: &migrationworker.OSS{
				Endpoint:   storage.OSS.Endpoint,
				MountPoint: storage.OSS.MountPoint,
				Options:    storage.OSS.Options,
			},
		}
	}
	if storage.Dragonfly != nil {
		return &migrationworker.Storage{
			Dragonfly: &migrationworker.Dragonfly{
				Endpoint:      storage.Dragonfly.Endpoint,
				OssMountPoint: storage.Dragonfly.OSSMountPoint,
				OssEndpoint:   storage.Dragonfly.OSSEndpoint,
				Options:       storage.Dragonfly.Options,
				// Preheat coordinates must be passed through: the worker uses
				// them to read the PreheatJob CR for remoteURL and credentials;
				// without them the worker-side preheat branch never runs.
				PreheatJobName:      storage.Dragonfly.PreheatJobName,
				PreheatJobNamespace: storage.Dragonfly.PreheatJobNamespace,
			},
		}
	}
	if storage.EFC != nil {
		return &migrationworker.Storage{
			Efc: &migrationworker.EFC{
				BucketName: storage.EFC.BucketName,
				Options:    storage.EFC.Options,
				// Preheat coordinates must be passed through: the worker uses
				// them to read the PreheatJob CR for preheat info; without them
				// the worker-side preheat branch never runs.
				PreheatJobName:      storage.EFC.PreheatJobName,
				PreheatJobNamespace: storage.EFC.PreheatJobNamespace,
			},
		}
	}
	if storage.PVC != nil {
		return &migrationworker.Storage{
			Pvc: &migrationworker.PVC{
				PvcName:      storage.PVC.PvcName,
				PvcNamespace: storage.PVC.PvcNamespace,
			},
		}
	}
	return nil
}
