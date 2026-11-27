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

package profiling

import (
	"context"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	profilingv1alpha1 "github.com/lj-runtimekit/job-controller/api/v1alpha1"
	"github.com/lj-runtimekit/job-controller/pkg/nodeagent"
)

func (r *ProfilingJobReconciler) collectorConfig(ctx context.Context) (*nodeagent.Config, error) {
	cm := &corev1.ConfigMap{}
	err := r.Get(ctx, client.ObjectKey{Namespace: r.CollectorNamespace, Name: r.ConfigMapName}, cm)
	var cfg *nodeagent.Config
	var revision string
	if err == nil {
		revision = cm.ResourceVersion
		data, ok := cm.Data[nodeagent.ConfigKey]
		if !ok {
			err = fmt.Errorf("ConfigMap %s/%s has no %q key", cm.Namespace, cm.Name, nodeagent.ConfigKey)
		} else {
			cfg, err = nodeagent.ParseConfig(data)
		}
	} else {
		revision = "missing"
		err = fmt.Errorf("get collector ConfigMap %s/%s: %w", r.CollectorNamespace, r.ConfigMapName, err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err == nil {
		if r.lastGood == nil || r.lastGoodRV != revision || r.lastBadRV != "" {
			ctrl.LoggerFrom(ctx).Info("Collector configuration loaded", "configMap", klog.KRef(r.CollectorNamespace, r.ConfigMapName), "resourceVersion", revision,
				"previousResourceVersion", r.lastGoodRV, "recoveredFromInvalid", r.lastBadRV != "", "image", cfg.Image, "maxRetries", ptr.Deref(cfg.MaxRetries, 0),
				"retryBackoff", cfg.RetryBackoff.Duration, "maxRetryBackoff", cfg.MaxRetryBackoff.Duration, "pendingTimeout", cfg.PendingTimeout.Duration)
		}
		r.lastGood = cfg
		r.lastGoodRV = revision
		r.lastBadRV = ""
		return cfg, nil
	}
	if r.lastBadRV != revision {
		ctrl.LoggerFrom(ctx).Error(err, "collector configuration rejected", "configMap", klog.KRef(r.CollectorNamespace, r.ConfigMapName), "resourceVersion", revision, "usingLastGood", r.lastGood != nil, "lastGoodResourceVersion", r.lastGoodRV)
		r.lastBadRV = revision
	}
	if r.lastGood != nil {
		return r.lastGood, nil
	}
	return nil, err
}

func (r *ProfilingJobReconciler) pvcReady(ctx context.Context, spec *profilingv1alpha1.ProfilingJobSpec) (*corev1.PersistentVolumeClaim, bool, string, error) {
	key := client.ObjectKey{Namespace: spec.PersistentVolumeClaimNamespace, Name: spec.PersistentVolumeClaimName}
	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, key, pvc); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, fmt.Sprintf("PersistentVolumeClaim %s does not exist", key), nil
		}
		return nil, false, "", err
	}
	if !pvc.DeletionTimestamp.IsZero() {
		return pvc, false, fmt.Sprintf("PersistentVolumeClaim %s is being deleted", key), nil
	}
	if pvc.Spec.VolumeMode != nil && *pvc.Spec.VolumeMode == corev1.PersistentVolumeBlock {
		return pvc, false, "", &preflightError{"PVCIncompatible", "PVC must use Filesystem volumeMode"}
	}
	if pvc.Status.Phase == corev1.ClaimBound {
		return pvc, true, "", nil
	}
	if pvc.Status.Phase == corev1.ClaimLost {
		return pvc, false, "", &preflightError{"PVCLost", fmt.Sprintf("PersistentVolumeClaim %s is Lost", key)}
	}
	if pvc.Spec.VolumeName == "" && pvc.Spec.StorageClassName != nil && *pvc.Spec.StorageClassName != "" {
		sc := &storagev1.StorageClass{}
		if err := r.Get(ctx, client.ObjectKey{Name: *pvc.Spec.StorageClassName}, sc); err != nil {
			if apierrors.IsNotFound(err) {
				return pvc, false, "waiting for PVC StorageClass", nil
			}
			return pvc, false, "", err
		}
		if sc.VolumeBindingMode != nil && *sc.VolumeBindingMode == storagev1.VolumeBindingWaitForFirstConsumer {
			return pvc, true, "WaitForFirstConsumer", nil
		}
	}
	return pvc, false, fmt.Sprintf("PersistentVolumeClaim %s is not Bound (phase %q)", key, pvc.Status.Phase), nil
}

func validateAccessModes(pvc *corev1.PersistentVolumeClaim, targets []profilingv1alpha1.TargetStatus) error {
	var rwx, rwo, rwop bool
	for _, mode := range pvc.Spec.AccessModes {
		switch mode {
		case corev1.ReadWriteMany:
			rwx = true
		case corev1.ReadWriteOnce:
			rwo = true
		case corev1.ReadWriteOncePod:
			rwop = true
		}
	}
	if rwop && len(targets) > 1 {
		return &preflightError{"PVCIncompatible", "ReadWriteOncePod PVC cannot support multiple collectors"}
	}
	if !rwx && !rwo && !rwop {
		return &preflightError{"PVCIncompatible", "PVC must declare a writable access mode"}
	}
	nodes := map[string]bool{}
	for _, t := range targets {
		nodes[t.TargetNodeName] = true
	}
	if len(nodes) > 1 && !rwx {
		return &preflightError{"PVCIncompatible", "cross-node collectors require a ReadWriteMany PVC"}
	}
	return nil
}

func (r *ProfilingJobReconciler) resolveTargets(ctx context.Context, job *profilingv1alpha1.ProfilingJob, cfg *nodeagent.Config) ([]profilingv1alpha1.TargetStatus, string, error) {
	var pods []corev1.Pod
	sel := job.Spec.TargetSelector
	if sel.PodName != "" {
		pod := &corev1.Pod{}
		key := client.ObjectKey{Namespace: sel.PodNamespace, Name: sel.PodName}
		if err := r.APIReader.Get(ctx, key, pod); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, fmt.Sprintf("target Pod %s does not exist", key), nil
			}
			return nil, "", err
		}
		pods = append(pods, *pod)
	} else {
		list := &corev1.PodList{}
		if err := r.APIReader.List(ctx, list, client.InNamespace(job.Namespace), client.MatchingLabels(sel.MatchLabels)); err != nil {
			return nil, "", err
		}
		pods = list.Items
	}
	if len(pods) == 0 {
		return nil, "no Pods match targetSelector", nil
	}
	sort.Slice(pods, func(i, j int) bool {
		if pods[i].Namespace != pods[j].Namespace {
			return pods[i].Namespace < pods[j].Namespace
		}
		return pods[i].Name < pods[j].Name
	})
	for _, pod := range pods {
		if !strings.EqualFold(pod.Labels[profilingv1alpha1.ProfilingEnableLabel], "true") {
			return nil, "", &preflightError{"ProfilingNotEnabled", fmt.Sprintf("target Pod %s/%s is not enabled by label %s=true", pod.Namespace, pod.Name, profilingv1alpha1.ProfilingEnableLabel)}
		}
	}
	targets := make([]profilingv1alpha1.TargetStatus, 0, len(pods))
	for i := range pods {
		pod := &pods[i]
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			return nil, "", &preflightError{"TargetTerminated", fmt.Sprintf("target Pod %s/%s is %s", pod.Namespace, pod.Name, pod.Status.Phase)}
		}
		if !pod.DeletionTimestamp.IsZero() || pod.Status.Phase != corev1.PodRunning || pod.Spec.NodeName == "" {
			return nil, fmt.Sprintf("target Pod %s/%s is not running on a node", pod.Namespace, pod.Name), nil
		}
		ids, waiting := gpuContainerIDs(pod, cfg)
		if waiting != "" {
			return nil, fmt.Sprintf("target Pod %s/%s: %s", pod.Namespace, pod.Name, waiting), nil
		}
		targets = append(targets, profilingv1alpha1.TargetStatus{TargetNamespace: pod.Namespace, TargetName: pod.Name, TargetUID: string(pod.UID), TargetNodeName: pod.Spec.NodeName, TargetContainerIds: ids, Stage: profilingv1alpha1.StageQueued})
	}
	return targets, "", nil
}

func gpuContainerIDs(pod *corev1.Pod, cfg *nodeagent.Config) ([]string, string) {
	statuses := make(map[string]corev1.ContainerStatus, len(pod.Status.ContainerStatuses))
	for _, status := range pod.Status.ContainerStatuses {
		statuses[status.Name] = status
	}
	var ids []string
	for _, container := range pod.Spec.Containers {
		if !requestsGPU(container.Resources, cfg) {
			continue
		}
		status := statuses[container.Name]
		// Kubelet keeps the dead instance ID on a waiting or terminated container.
		if status.State.Running == nil {
			return nil, fmt.Sprintf("GPU container %q is not running (%s)", container.Name, containerState(status))
		}
		if status.ContainerID == "" {
			return nil, fmt.Sprintf("GPU container %q has no runtime container ID yet", container.Name)
		}
		ids = append(ids, status.ContainerID)
	}
	if len(ids) == 0 {
		return nil, "no container requests a configured GPU resource"
	}
	sort.Strings(ids)
	return ids, ""
}

func containerState(status corev1.ContainerStatus) string {
	if waiting := status.State.Waiting; waiting != nil {
		if waiting.Reason != "" {
			return "waiting: " + waiting.Reason
		}
		return "waiting"
	}
	if terminated := status.State.Terminated; terminated != nil {
		if terminated.Reason != "" {
			return fmt.Sprintf("terminated: %s", terminated.Reason)
		}
		return fmt.Sprintf("terminated with exit code %d", terminated.ExitCode)
	}
	return "not started"
}

func requestsGPU(resources corev1.ResourceRequirements, cfg *nodeagent.Config) bool {
	for name, quantity := range resources.Requests {
		if !quantity.IsZero() && cfg.IsGPUResource(name) {
			return true
		}
	}
	for name, quantity := range resources.Limits {
		if !quantity.IsZero() && cfg.IsGPUResource(name) {
			return true
		}
	}
	return false
}

// volumePath renders a PVC-relative directory as an absolute path inside the volume.
func volumePath(rel string) string {
	return "/" + rel
}
