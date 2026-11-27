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
	"path/filepath"
	"strings"

	"github.com/container-migration/controller/api/v1beta1"
	"github.com/container-migration/controller/pkg/controllers"
	migrationlabels "github.com/container-migration/controller/pkg/labels"
	"github.com/container-migration/controller/pkg/util"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// CreatePodsHandler creates replicas target pods from SourcePodTemplate.
//
// Key points:
//  1. Main containers use the **original image of the source pod**; checkpoint
//     data is injected by XDM at runc create time, no need to build rootfs
//     into an image.
//  2. The migration.io/restore-enable and restore-data-path annotations are
//     written at pod creation - the path does not depend on the scheduling
//     result and is isolated by the CR UID, so it can be computed ahead.
//  3. A blocking init container sits in front of the main container: upstream
//     K8s does not allow changing container fields after pod creation, so the
//     init container holds back the main container until the data is
//     extracted on the target node and released via exec.
type CreatePodsHandler struct {
	sharedContext *controllers.SharedContext
}

var _ = (controllers.Handler[*v1beta1.ContainerStateMigrationJobRequest])(&CreatePodsHandler{})

func NewCreatePodsHandler(sharedContext *controllers.SharedContext) *CreatePodsHandler {
	return &CreatePodsHandler{sharedContext: sharedContext}
}

func (h *CreatePodsHandler) GetName() string {
	return "CreatePodsHandler"
}

func (h *CreatePodsHandler) Handle(ctx context.Context, jobReq *v1beta1.ContainerStateMigrationJobRequest) (result controllers.Result) {
	ctxLog := log.FromContext(ctx)

	status := &jobReq.Status
	if jobReq.Spec.Restore == nil || status.Phase != v1beta1.JobPhaseRestoring {
		return controllers.NewSkipResult("not in Restoring phase")
	}
	if status.RestoreStatus == nil || status.RestoreStatus.Phase != v1beta1.JobPhasePending {
		return controllers.NewSkipResult("restoreStatus not in Pending")
	}
	if status.CheckpointStatus == nil || status.CheckpointStatus.SourcePodTemplate == nil {
		return controllers.NewErrorResult(fmt.Errorf("sourcePodTemplate is not available"))
	}

	template := status.CheckpointStatus.SourcePodTemplate
	// The target pod shares the source pod's namespace. All identity info is
	// read from template.ObjectMeta: SourcePodTemplate is stored schemaless,
	// metadata is not pruned by the apiserver.
	namespace := template.Namespace
	if namespace == "" {
		return controllers.NewErrorResult(fmt.Errorf("sourcePodTemplate has no namespace"))
	}
	// The apiserver rejects creation when the injected volume / init container
	// names collide with the source pod, and every retry round fails. This
	// cannot self-heal; fail the task directly with the reason spelled out.
	if conflict := signalNameConflict(&template.Spec); conflict != "" {
		status.Phase = v1beta1.JobPhaseFailed
		status.RestoreStatus.Phase = v1beta1.JobPhaseFailed
		status.Message = fmt.Sprintf(
			"Source pod already uses the reserved name %q needed by the migration init container", conflict)
		status.RestoreStatus.Message = status.Message
		return controllers.NewDoneResult()
	}

	destinations := make([]v1beta1.JobDestinationStatus, 0, jobReq.Spec.Restore.Replicas)
	// flat layout: the source pod carries the migration-dedicated hostPath
	// volume and only one container was checkpointed, data lands directly at
	// <hostPath>/data (rootfs/memory/xpu underneath), the restore-data-path
	// annotation also uses that directory and XDM takes it as the checkpoint
	// directory directly.
	flatDataDir, flatContainer, flat := restoreFlatDataDir(jobReq)
	for i := 0; i < jobReq.Spec.Restore.Replicas; i++ {
		podName := destPodName(jobReq, i)
		dataPath := h.restoreDataPath(jobReq)
		if flat {
			dataPath = flatDataDir
		}

		pod := buildDestPod(jobReq, template, podName, dataPath, flatContainer, h.sharedContext.Config.InitContainerImage)
		err := h.sharedContext.Client.Create(ctx, pod)
		switch {
		case err == nil:
			ctxLog.Info("Created restore target pod", "pod", client.ObjectKeyFromObject(pod),
				"dataPath", dataPath)
		case errors.IsAlreadyExists(err):
			// The name is taken. Normally the previous round already created it
			// (re-entry caused by a failed status update), but it could be
			// someone else's pod. The latter must stop: continuing would exec
			// into someone else's container and take their container state as
			// the restore result.
			owned, checkErr := h.podBelongsToJob(ctx, jobReq, namespace, podName)
			if checkErr != nil {
				return controllers.NewErrorResult(checkErr)
			}
			if !owned {
				status.Phase = v1beta1.JobPhaseFailed
				status.RestoreStatus.Phase = v1beta1.JobPhaseFailed
				status.Message = fmt.Sprintf(
					"Pod %s/%s already exists and does not belong to this job", namespace, podName)
				status.RestoreStatus.Message = status.Message
				return controllers.NewDoneResult()
			}
			ctxLog.Info("Restore target pod already exists and belongs to this job",
				"pod", client.ObjectKeyFromObject(pod))
		default:
			ctxLog.Error(err, "Failed to create restore target pod", "pod", client.ObjectKeyFromObject(pod))
			return controllers.NewErrorResult(err)
		}

		destinations = append(destinations, v1beta1.JobDestinationStatus{
			Phase: v1beta1.DestPhasePending,
			Pod: &v1beta1.SimplePodInfo{
				Name:      podName,
				Namespace: namespace,
			},
			ContainerMigrationStatuses: buildDestContainerStatuses(status.CheckpointStatus, dataPath, flatContainer),
		})
	}

	status.RestoreStatus.Destinations = destinations
	// Pods created, go straight to Restoring: data preparation (PrepareDataHandler) enters at this sub-phase.
	status.RestoreStatus.Phase = v1beta1.JobPhaseRestoring
	status.RestoreStatus.Conditions = addCondition(status.RestoreStatus.Conditions,
		v1beta1.JobConditionPodsCreated, "PodsCreated",
		fmt.Sprintf("Created %d restore target pods", len(destinations)))
	h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeNormal, "RestorePodsCreated",
		"Created %d restore target pods in namespace %s", len(destinations), namespace)
	return controllers.NewAbortWithRetryAfterResult(h.sharedContext.Config.RetryWaitTime)
}

// sharedVersion is the fixed segment of the restore landing path unrelated to
// the replica ordinal. All replicas of one CR share it, so multiple replicas on
// the same node share one copy of data: the worker homeDir is node-local disk,
// identical path strings are physically shared; across nodes each has its own.
const sharedVersion = "shared"

// restoreDataPath is the root directory holding this CR's checkpoint data on
// the target node. Unrelated to the replica ordinal: CR UID + the fixed
// sharedVersion segment form the isolation key, multiple replicas of the same
// CR on one node share the directory and only pull one copy of data. The worker
// extracts with the same podUID(=CR UID)/version(=shared) pair, ending at
// <dataPath>/<containerName>/checkpoint, consistent with the
// xpu-container-runtime contract.
func (h *CreatePodsHandler) restoreDataPath(jobReq *v1beta1.ContainerStateMigrationJobRequest) string {
	return filepath.Join(h.sharedContext.Config.CheckpointDataRootDir,
		string(jobReq.UID), sharedVersion)
}

// restoreFlatDataDir decides whether this task takes the flat layout, and
// returns the data directory and the target container name.
//
// Trigger: the source pod carries a hostPath volume named
// lj-runtimekit-migration and exactly one container was checkpointed. The flat
// layout has only one data/ directory and cannot express multiple containers,
// so multi-container falls back to the default layout. The returned dataDir
// looks like <hostPath>/data.
func restoreFlatDataDir(jobReq *v1beta1.ContainerStateMigrationJobRequest) (dataDir, containerName string, ok bool) {
	cp := jobReq.Status.CheckpointStatus
	if cp == nil || cp.SourcePodTemplate == nil {
		return "", "", false
	}
	if len(cp.ContainerMigrationStatuses) != 1 {
		return "", "", false
	}
	hostPath := getRuntimekitMigrationHostPath(cp.SourcePodTemplate.Spec.Volumes)
	if hostPath == "" {
		return "", "", false
	}
	return filepath.Join(hostPath, "data"), cp.ContainerMigrationStatuses[0].Name, true
}

// destPodName is the target pod name. It carries the CR UID prefix: the same
// checkpoint may well be referenced by multiple restore-only CRs; naming with
// only "source pod name + ordinal" would make the second CR collide onto the
// pod created by the first CR.
func destPodName(jobReq *v1beta1.ContainerStateMigrationJobRequest, index int) string {
	sourcePodName := jobReq.Status.CheckpointStatus.SourcePodTemplate.Name
	suffix := fmt.Sprintf("-%s-%d", shortUID(jobReq.UID), index)
	// Pod names are DNS-1123 subdomains, overlength is rejected by the
	// apiserver directly, and source pod names (e.g. StatefulSet-generated)
	// can be long to begin with. Truncate the source name rather than the
	// suffix on overflow: the UID prefix and replica ordinal in the suffix
	// are the source of uniqueness and must not be lost.
	if len(sourcePodName)+len(suffix) > maxPodNameLength {
		sourcePodName = sourcePodName[:maxPodNameLength-len(suffix)]
		// Truncation may stop right on '-' or '.', appending the '-'-prefixed
		// suffix would produce an illegal DNS-1123 name like "foo.-abc-0"; strip
		// trailing separators first.
		sourcePodName = strings.TrimRight(sourcePodName, "-.")
	}
	return sourcePodName + suffix
}

// shortUID takes the first 6 hex chars of the UID, enough to distinguish the migration tasks coexisting at any time.
func shortUID(uid types.UID) string {
	s := strings.ReplaceAll(string(uid), "-", "")
	if len(s) > 6 {
		s = s[:6]
	}
	if s == "" {
		return "restore"
	}
	return s
}

// podBelongsToJob checks whether an existing same-named pod was created by this task itself.
func (h *CreatePodsHandler) podBelongsToJob(ctx context.Context,
	jobReq *v1beta1.ContainerStateMigrationJobRequest, namespace, podName string) (bool, error) {
	existing := &corev1.Pod{}
	if err := h.sharedContext.Client.Get(ctx,
		client.ObjectKey{Namespace: namespace, Name: podName}, existing); err != nil {
		// Including NotFound: the pod was deleted after Create reported
		// AlreadyExists. This must not be judged as "not ours" and failed;
		// return an error to retry this round, the next Create will succeed.
		return false, err
	}
	return existing.Annotations[AnnotationCheckpointJobUID] == string(jobReq.UID), nil
}

// buildDestContainerStatuses initializes the restore-side container statuses
// from the checkpoint container list, recording the data directory of each
// container for troubleshooting. Under the flat layout the data directory of
// the target container is dataPath itself (<hostPath>/data), no
// <containerName>/checkpoint appended.
func buildDestContainerStatuses(checkpointStatus *v1beta1.JobCheckpointStatus,
	dataPath, flatContainer string) []v1beta1.ContainerMigrationStatus {
	result := make([]v1beta1.ContainerMigrationStatus, 0, len(checkpointStatus.ContainerMigrationStatuses))
	for i := range checkpointStatus.ContainerMigrationStatuses {
		name := checkpointStatus.ContainerMigrationStatuses[i].Name
		snapshotPath := filepath.Join(dataPath, name, "checkpoint")
		if flatContainer != "" && name == flatContainer {
			snapshotPath = dataPath
		}
		result = append(result, v1beta1.ContainerMigrationStatus{
			Name:         name,
			SnapshotPath: snapshotPath,
			Conditions:   make([]metav1.Condition, 0),
		})
	}
	return result
}

// buildDestPod builds the target pod from the source pod template (used by
// restore, reused and then trimmed by cold start).
// Package-level function: no handler state dependency, shared by
// CreatePodsHandler and ColdStartHandler.
// initImage is the image of the injected blocking init container (config
// value); cold start needs no blocking init, pass an empty string.
func buildDestPod(jobReq *v1beta1.ContainerStateMigrationJobRequest,
	template *corev1.PodTemplateSpec,
	podName, dataPath, flatContainer, initImage string) *corev1.Pod {
	// labels / annotations come from template.ObjectMeta: stored schemaless, metadata is not pruned.
	annotations := copyStringMap(template.Annotations)
	if annotations == nil {
		annotations = map[string]string{}
	}
	// The restored pod inherits all annotations of the source pod (including
	// the auto-checkpoint switch); this marker lets the Pod controller block
	// the checkpoint -> restore -> checkpoint cascade trigger.
	annotations[migrationlabels.AnnotationRestoreCreated] = "true"
	annotations[AnnotationRestoreEnable] = "true"
	annotations[AnnotationRestoreDataPath] = dataPath
	annotations[AnnotationCheckpointJobUID] = string(jobReq.UID)
	// Under the flat layout dataPath has no <containerName>/checkpoint
	// hierarchy; the target container must be named explicitly, otherwise the
	// runtime cannot tell the sandbox / injected init containers apart from
	// the real restore target.
	if flatContainer != "" {
		annotations[AnnotationRestoreFlatContainer] = flatContainer
	}

	// Source pod labels are kept as-is (after migration the pod must still
	// be selected by the original Service), plus a task identifier stamped for
	// finding/cleaning all replicas of one migration via a label selector.
	labels := copyStringMap(template.Labels)
	if labels == nil {
		labels = map[string]string{}
	}
	labels[LabelMigrationJobUID] = string(jobReq.UID)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        podName,
			Namespace:   template.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: *template.Spec.DeepCopy(),
	}
	// Let K8s garbage collection delete the pods: once the CR is deleted,
	// the replica pods follow automatically.
	// ownerReference cannot jump namespaces (otherwise GC treats the owner as
	// non-existent and deletes wrongly); the cross-namespace case is covered
	// by explicit deletion in the finalizer.
	if template.Namespace == jobReq.Namespace {
		pod.OwnerReferences = []metav1.OwnerReference{
			*metav1.NewControllerRef(jobReq, v1beta1.GroupVersion.WithKind("ContainerStateMigrationJobRequest")),
		}
	}
	// The target pod is handed to the scheduler to pick a node: pinning
	// NodeName directly would bypass the scheduler and miss GPU allocation.
	// nodeSelector is kept inherited: if the source pod explicitly declared a
	// node selector (business-level topology constraint), restore replicas
	// obey it too; remove the field from the source pod definition when
	// inheritance is undesired.
	pod.Spec.NodeName = ""
	// restartPolicy keeps the source pod template value (whatever the source
	// pod had, typically Always for inference), no longer forced to Never: a
	// successfully restored pod restarted by the kubelet needs restartPolicy
	// to trigger the re-pull, so re-restore can run (xpu-container-runtime
	// lets XDM self-drive cr-tool via the restore-succeeded annotation).
	// pod.Spec is deep-copied from template.Spec, the source template's
	// RestartPolicy is kept as-is, not overwritten here.

	injectBlockingInitContainer(pod, initImage)
	return pod
}

// injectBlockingInitContainer injects the signal volume and the blocking init
// container. The init container keeps polling the signal file; the controller
// creates the file via pods/exec to release it once data is confirmed ready.
func injectBlockingInitContainer(pod *corev1.Pod, initImage string) {
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
		Name: SignalVolumeName,
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{},
		},
	})

	// Default to the image of the first business container of the source pod:
	// it is usually pre-pulled on the target node and definitely ships sh. An
	// explicit --init-container-image config takes priority.
	image := initImage
	if image == "" && len(pod.Spec.Containers) > 0 {
		image = pod.Spec.Containers[0].Image
	}

	initContainer := corev1.Container{
		Name:  InitContainerName,
		Image: image,
		Command: []string{"sh", "-c",
			fmt.Sprintf("while [ ! -f %s/%s ]; do sleep 1; done", SignalMountPath, SignalFileName)},
		VolumeMounts: []corev1.VolumeMount{
			{Name: SignalVolumeName, MountPath: SignalMountPath},
		},
		// Inherit the securityContext of the source pod business container. If
		// the target namespace enforces PodSecurity (restricted requires
		// allowPrivilegeEscalation=false, drop ALL, runAsNonRoot etc.), a bare
		// init container gets the whole pod creation rejected, and such a
		// failure repeats identically every retry round - it cannot self-heal.
		// The source container's config has definitely passed validation.
		SecurityContext: inheritedSecurityContext(pod),
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("10m"),
				corev1.ResourceMemory: resource.MustParse("10Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("50Mi"),
			},
		},
	}
	// Put it first, guaranteeing it blocks ahead of all business init containers and the main container.
	pod.Spec.InitContainers = append([]corev1.Container{initContainer}, pod.Spec.InitContainers...)
}

// inheritedSecurityContext takes the securityContext of the first business
// container of the source pod for the injected init container. The source
// container runs, so this config definitely passes the target namespace
// admission validation.
func inheritedSecurityContext(pod *corev1.Pod) *corev1.SecurityContext {
	for i := range pod.Spec.Containers {
		if sc := pod.Spec.Containers[i].SecurityContext; sc != nil {
			return sc.DeepCopy()
		}
	}
	return nil
}

// signalNameConflict returns the reserved name already occupied by the source pod; empty string when no conflict.
func signalNameConflict(spec *corev1.PodSpec) string {
	for i := range spec.Volumes {
		if spec.Volumes[i].Name == SignalVolumeName {
			return SignalVolumeName
		}
	}
	for i := range spec.InitContainers {
		if spec.InitContainers[i].Name == InitContainerName {
			return InitContainerName
		}
	}
	for i := range spec.Containers {
		if spec.Containers[i].Name == InitContainerName {
			return InitContainerName
		}
	}
	return ""
}

// addCondition appends a True condition to conditions.
func addCondition(conditions []metav1.Condition, conditionType, reason, message string) []metav1.Condition {
	return util.AddOrUpdateCondition(&metav1.Condition{
		Type:               conditionType,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	}, conditions)
}
