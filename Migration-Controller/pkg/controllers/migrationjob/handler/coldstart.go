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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// coldStartDeleteBuffer is the extra buffer on top of the pod's
	// terminationGracePeriodSeconds when waiting for the old pod to disappear,
	// covering the slow teardown of GPU / containerd.
	coldStartDeleteBuffer = 30 * time.Second
	// coldStartDeleteDeadlineCap caps the wait for NotFound: a pod with an
	// extra-long grace period must not block cold start indefinitely.
	coldStartDeleteDeadlineCap = 2 * time.Minute
)

// ColdStartHandler advances the cold-start fallback:
//
//	delete the failed pod -> wait NotFound -> build a same-name cold-start pod
//	from the source template (drop restore annotations, drop the blocking init,
//	strip migration-injected env, pin nodeSelector back to the failed node) ->
//	wait Running -> Succeeded.
//
// A cold-start pod that definitively fails (crash / create-error) -> that
// replica Failed; one stuck in Pending / image pull -> no timeout, the CRD
// stays in Restoring, waiting for recovery or manual intervention.
//
// A successful cold start does not get the restore-succeeded annotation: it
// was not restored in the first place, so a later kubelet restart goes through
// a normal cold start without triggering re-restore.
type ColdStartHandler struct {
	sharedContext *controllers.SharedContext
}

var _ = (controllers.Handler[*v1beta1.ContainerStateMigrationJobRequest])(&ColdStartHandler{})

func NewColdStartHandler(sharedContext *controllers.SharedContext) *ColdStartHandler {
	return &ColdStartHandler{sharedContext: sharedContext}
}

func (h *ColdStartHandler) GetName() string {
	return "ColdStartHandler"
}

func (h *ColdStartHandler) Handle(ctx context.Context, jobReq *v1beta1.ContainerStateMigrationJobRequest) (result controllers.Result) {
	status := &jobReq.Status
	if jobReq.Spec.Restore == nil || status.Phase != v1beta1.JobPhaseRestoring ||
		status.RestoreStatus == nil || status.RestoreStatus.Phase != v1beta1.JobPhaseRestoring {
		return controllers.NewSkipResult("not in Restoring phase")
	}

	for i := range status.RestoreStatus.Destinations {
		dest := &status.RestoreStatus.Destinations[i]
		if dest.Phase != v1beta1.DestPhaseColdStarting {
			continue
		}
		if jobReq.Spec.Restore.DisableColdStartFallback {
			// Debugging mode: do not advance the cold start - no pod deletion,
			// no pod recreation. The failing restore pod stays in place for
			// inspection (e.g. debugging cr-tool failures). The replica remains
			// ColdStarting until the field is cleared on the CR, after which
			// the cold start resumes normally.
			dest.Message = "Cold start fallback disabled, keeping the failed restore pod for debugging"
			continue
		}
		h.progressColdStart(ctx, jobReq, dest)
	}

	// Cold start advances asynchronously over multiple rounds: whatever one
	// round did not finish is continued on the next requeue. Requeueing is
	// owned by RestoreXPUHandler at the chain tail (it Aborts when unsettled
	// replicas exist).
	return controllers.NewContinueResult()
}

// progressColdStart advances a single replica's cold start through the three
// sub-stages DeletingPod -> CreatingPod -> WaitingRunning per
// dest.ColdStart.Stage.
func (h *ColdStartHandler) progressColdStart(ctx context.Context,
	jobReq *v1beta1.ContainerStateMigrationJobRequest, dest *v1beta1.JobDestinationStatus) {
	ctxLog := log.FromContext(ctx)

	// Exception fallback: initialize when ColdStart is nil (status residue from an old version).
	if dest.ColdStart == nil {
		enterColdStart(dest, "unknown", "Cold start fallback")
	}

	pod := &corev1.Pod{}
	err := h.sharedContext.Client.Get(ctx, client.ObjectKey{
		Namespace: dest.Pod.Namespace,
		Name:      dest.Pod.Name,
	}, pod)
	isNotFound := errors.IsNotFound(err)
	if err != nil && !isNotFound {
		ctxLog.Error(err, "Failed to get target pod during cold start", "pod", dest.Pod.String())
		return
	}

	switch dest.ColdStart.Stage {
	case coldStartStageDeletePod:
		// Wait for the old restore pod to disappear completely (a hard
		// constraint of same-name rebuild). The delete is idempotent: when the
		// previous round's Delete was issued but the status did not persist,
		// this round's Get sees a pod with DeletionTimestamp and goes straight
		// into the timeout verdict.
		if isNotFound {
			dest.ColdStart.Stage = coldStartStageCreatePod
			dest.Message = "Failed restore pod deleted, creating cold start pod"
			return
		}
		if pod.DeletionTimestamp == nil {
			if err := h.sharedContext.Client.Delete(ctx, pod); err != nil && !errors.IsNotFound(err) {
				ctxLog.Error(err, "Failed to delete failed restore pod", "pod", dest.Pod.String())
				return
			}
			dest.Message = "Deleting failed restore pod, waiting for it to disappear"
			return
		}
		// Deleting: still not gone past the deadline (kubelet / node stuck) ->
		// force delete (grace=0). The baseline uses pod.DeletionTimestamp (the
		// moment the Delete request actually landed) rather than
		// ColdStart.StartedAt: under controller restart / requeue delay the
		// Delete may be issued long after entering cold start, and using
		// StartedAt would wrongly eat up the buffer reserved for graceful
		// deletion.
		if time.Since(pod.DeletionTimestamp.Time) > coldStartDeleteDeadline(pod) {
			ctxLog.Info("Cold start: force deleting stuck restore pod", "pod", dest.Pod.String())
			if err := h.sharedContext.Client.Delete(ctx, pod, client.GracePeriodSeconds(0)); err != nil && !errors.IsNotFound(err) {
				ctxLog.Error(err, "Failed to force delete restore pod", "pod", dest.Pod.String())
				return
			}
			dest.Message = "Force deleting stuck restore pod"
			return
		}
		dest.Message = "Waiting for failed restore pod to be deleted"

	case coldStartStageCreatePod:
		if !isNotFound {
			// Idempotent re-entry: the previous round's Create succeeded but the status did not persist. Confirming it belongs to this task is enough.
			if pod.Annotations[AnnotationCheckpointJobUID] != string(jobReq.UID) {
				markDestFailed(dest, fmt.Sprintf(
					"Pod %s already exists and does not belong to this job, cannot cold start", dest.Pod.String()))
				return
			}
			dest.ColdStart.Stage = coldStartStageWaitRunning
			dest.Message = "Cold start pod exists, waiting for it to run"
			return
		}
		if jobReq.Status.CheckpointStatus == nil || jobReq.Status.CheckpointStatus.SourcePodTemplate == nil {
			markDestFailed(dest, "sourcePodTemplate is not available, cannot cold start")
			return
		}
		if dest.Pod.NodeName == "" {
			// The replica pod disappeared before scheduling (the
			// PodDisappeared path of resolveScheduling never backfilled the
			// target node), so cold start has no node to pin; a pod with an
			// empty nodeSelector must not be created (it matches no node and
			// stays Pending forever with no exit) - judge the replica failed
			// directly.
			markDestFailed(dest, "target pod disappeared before scheduling, cannot determine cold start node")
			return
		}
		coldPod := buildColdStartPod(jobReq, jobReq.Status.CheckpointStatus.SourcePodTemplate,
			dest.Pod.Name, dest.Pod.NodeName)
		err := h.sharedContext.Client.Create(ctx, coldPod)
		if err != nil && !errors.IsAlreadyExists(err) {
			ctxLog.Error(err, "Failed to create cold start pod", "pod", dest.Pod.String())
			return
		}
		dest.ColdStart.Stage = coldStartStageWaitRunning
		dest.Message = "Cold start pod created, waiting for it to run"
		ctxLog.Info("Created cold start pod", "pod", dest.Pod.String())

	case coldStartStageWaitRunning:
		if isNotFound {
			// The cold-start pod we created was deleted externally: fall back to rebuild (keeping the same-name constraint).
			dest.ColdStart.Stage = coldStartStageCreatePod
			dest.Message = "Cold start pod disappeared, recreating"
			return
		}
		containerNames := destContainerNames(dest)
		// The cold-start pod also carries the source init containers (other
		// than the blocking init) and the main container: the main container
		// verdict reuses containersSettled (Running or normal exit 0).
		if failed, msg := hasFailedContainer(pod, containerNames); failed {
			markDestFailed(dest, fmt.Sprintf("Cold start pod failed: %s", msg))
			h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeWarning, "ColdStartFailed",
				"Cold start pod %s failed: %s", dest.Pod.String(), msg)
			ctxLog.Info("Cold start pod failed", "pod", dest.Pod.String(), "message", msg)
			return
		}
		if !containersSettled(pod, containerNames) {
			// Pending / image pull / source init unfinished: no timeout, stop here and wait.
			dest.Message = fmt.Sprintf("Cold start pod in phase %s", pod.Status.Phase)
			return
		}
		dest.Phase = v1beta1.DestPhaseSucceeded
		dest.StartupMode = v1beta1.StartupModeColdStart
		dest.Message = "Cold start fallback succeeded"
		dest.Pod.UID = pod.UID
		dest.Pod.IP = pod.Status.PodIP
		h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeNormal, "ColdStartSucceeded",
			"Cold start pod %s is running", dest.Pod.String())
		ctxLog.Info("Cold start fallback succeeded", "pod", dest.Pod.String())
	}
}

// coldStartDeleteDeadline is the NotFound wait cap: pod grace + 30s buffer, capped at 2min.
func coldStartDeleteDeadline(pod *corev1.Pod) time.Duration {
	grace := 30 * time.Second
	if pod.Spec.TerminationGracePeriodSeconds != nil {
		grace = time.Duration(*pod.Spec.TerminationGracePeriodSeconds) * time.Second
	}
	deadline := grace + coldStartDeleteBuffer
	if deadline > coldStartDeleteDeadlineCap {
		deadline = coldStartDeleteDeadlineCap
	}
	return deadline
}

// buildColdStartPod builds the cold-start pod from the source template: same
// source and same name as the restore pod, but drops the restore annotations
// and the blocking init (cold start does not runc-restore, does not restore
// XPU), strips the migration-injected env (cold start does not participate in
// migration, avoiding being hung at the steady-state point by the migration
// stack without serving), and pins nodeSelector back to the failed node.
//
// Why pinning the node must use nodeSelector instead of pod.Spec.nodeName:
// the cold start is a GPU pod relying on the source's schedulerName + device
// plugin to allocate the card; pinning nodeName directly bypasses the
// scheduler, so the GPU allocation annotation is never obtained and the
// container cannot come up.
func buildColdStartPod(jobReq *v1beta1.ContainerStateMigrationJobRequest,
	template *corev1.PodTemplateSpec, podName, nodeName string) *corev1.Pod {
	pod := buildDestPod(jobReq, template, podName, "", "", "")

	// Drop the restore annotations: the runtime will not intercept, no runc restore, no re-restore triggered.
	for _, key := range []string{
		AnnotationRestoreEnable,
		AnnotationRestoreDataPath,
		AnnotationRestoreFlatContainer,
		AnnotationRestoreSucceeded,
		AnnotationRestoreSucceededAt,
	} {
		delete(pod.Annotations, key)
	}
	// Keep restore-created=true (to prevent automatic checkpoint cascade) and
	// migration-job-uid (ownership). The cold-start marker helps identification
	// and troubleshooting.
	pod.Annotations[AnnotationColdStartFallback] = "true"

	// Strip the migration-injected env inherited from the source template: the
	// cold-start pod does not participate in migration and no checkpoint session
	// will release the migration stack's steady-state hang; carrying them would
	// hang the app at the steady-state point and it would never serve.
	stripMigrationEnvs(pod)

	// Drop the injected blocking init container and the signal volume.
	removeSignalInit(pod)

	// Pin back to the failed node (guaranteeing the cold start comes up on the node where retries failed).
	pod.Spec.NodeName = ""
	if pod.Spec.NodeSelector == nil {
		pod.Spec.NodeSelector = map[string]string{}
	}
	pod.Spec.NodeSelector["kubernetes.io/hostname"] = nodeName
	return pod
}

// stripMigrationEnvs strips the migration-injected env
// (EnvKubergpuMigrationEnable / EnvAmpVgpuEnable) inherited from the source
// template on all containers (main + init), keeping other env as-is. Only used
// for cold-start fallback pods: they do not participate in migration, and no
// checkpoint session will release the migration stack's steady-state hang;
// restore pods go through buildDestPod without this filter, migration
// capability unaffected.
func stripMigrationEnvs(pod *corev1.Pod) {
	strip := func(env []corev1.EnvVar) []corev1.EnvVar {
		kept := make([]corev1.EnvVar, 0, len(env))
		for _, e := range env {
			if e.Name == EnvKubergpuMigrationEnable || e.Name == EnvAmpVgpuEnable {
				continue
			}
			kept = append(kept, e)
		}
		return kept
	}
	for i := range pod.Spec.Containers {
		pod.Spec.Containers[i].Env = strip(pod.Spec.Containers[i].Env)
	}
	for i := range pod.Spec.InitContainers {
		pod.Spec.InitContainers[i].Env = strip(pod.Spec.InitContainers[i].Env)
	}
}

// removeSignalInit removes the injected blocking init container
// (InitContainerName), the signal volume (SignalVolumeName) and its sole
// mount.
func removeSignalInit(pod *corev1.Pod) {
	initContainers := make([]corev1.Container, 0, len(pod.Spec.InitContainers))
	for i := range pod.Spec.InitContainers {
		if pod.Spec.InitContainers[i].Name == InitContainerName {
			continue
		}
		initContainers = append(initContainers, pod.Spec.InitContainers[i])
	}
	pod.Spec.InitContainers = initContainers

	volumes := make([]corev1.Volume, 0, len(pod.Spec.Volumes))
	for i := range pod.Spec.Volumes {
		if pod.Spec.Volumes[i].Name == SignalVolumeName {
			continue
		}
		volumes = append(volumes, pod.Spec.Volumes[i])
	}
	pod.Spec.Volumes = volumes
}
