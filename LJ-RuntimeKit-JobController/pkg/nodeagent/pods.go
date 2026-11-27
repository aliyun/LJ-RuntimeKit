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

package nodeagent

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	v1alpha1 "github.com/lj-runtimekit/job-controller/api/v1alpha1"
	"github.com/lj-runtimekit/job-controller/pkg/collectorprotocol"
)

const (
	LabelComponent       = "profiling.runtimekit.io/component"
	ComponentCollector   = "collector"
	LabelJobUID          = "profiling.runtimekit.io/job-uid"
	AnnotationJobNS      = "profiling.runtimekit.io/job-namespace"
	AnnotationJobName    = "profiling.runtimekit.io/job-name"
	AnnotationTargetUID  = "profiling.runtimekit.io/target-uid"
	AnnotationRunIndex   = "profiling.runtimekit.io/run-index"
	AnnotationRetryCount = "profiling.runtimekit.io/retry-count"

	ReasonInvalidCollectorReport = "InvalidCollectorReport"
	ReasonMissingCollectorReport = "MissingCollectorReport"

	collectorBinary      = "/bin/profiling-collector"
	collectorDataMount   = "/data"
	collectorContainer   = "collector"
	collectorNamePrefix  = "profiling-collector-"
	terminationGraceSlop = 15
)

// CollectorPodName identifies one target retry in a frozen round.
// The hash avoids long names and includes the Job UID to isolate recreated Jobs.
func CollectorPodName(job *v1alpha1.ProfilingJob, target *v1alpha1.TargetStatus) string {
	identity := strings.Join([]string{
		string(job.UID), strconv.FormatInt(runIndex(job), 10),
		target.TargetUID, strconv.FormatInt(int64(target.RetryCount), 10),
	}, "\x00")
	return collectorNamePrefix + shortHash(identity)
}

// RoundPrefix is the PVC-relative directory shared by all of a round's files.
func RoundPrefix(job *v1alpha1.ProfilingJob) string {
	return path.Join(job.Namespace, job.Name, fmt.Sprintf("run-%d", runIndex(job)))
}

// runIndex is the started round's index; callers act only after a round starts.
func runIndex(job *v1alpha1.ProfilingJob) int64 {
	return ptr.Deref(job.Status.LastRunIndex, -1)
}

func OutputFiles(job *v1alpha1.ProfilingJob, result collectorprotocol.Result) []string {
	var files []string
	for _, container := range result.Containers {
		for _, name := range container.OutputFiles {
			files = append(files, path.Join(RoundPrefix(job), name))
		}
	}
	return files
}

// CollectorPod builds a one-shot collector with current settings and frozen round inputs.
// 身份、配置或阶段超时时长非法时返回 nil；创建前应调用 cfg.ValidateDuration。
// 不设置 Pod 总 deadline，启动等待仅告警，采集由 collector 内部超时控制。
func CollectorPod(cfg *Config, job *v1alpha1.ProfilingJob, target *v1alpha1.TargetStatus) *corev1.Pod {
	if job == nil || job.Status.RunConfig == nil || target == nil || job.UID == "" ||
		job.Namespace == "" || job.Name == "" || target.TargetUID == "" || target.TargetNodeName == "" ||
		target.TargetNamespace == "" || target.TargetName == "" || len(target.TargetContainerIds) == 0 ||
		runIndex(job) < 0 || target.RetryCount < 0 {
		return nil
	}
	run := job.Status.RunConfig
	if job.Spec.PersistentVolumeClaimNamespace == "" || job.Spec.PersistentVolumeClaimName == "" {
		return nil
	}
	if err := cfg.ValidateDuration(run.Duration.Duration); err != nil {
		return nil
	}
	socketDir := filepath.Dir(cfg.SocketPath)
	// 只下发每个 Pod 都不同的目标与轮次信息；超时、重试、残留会话清理等参数由 collector 自带默认配置，
	// 不再逐项透传，避免每个 Pod 携带一整串与默认值相同的参数。
	args := []string{
		"--socket=" + cfg.SocketPath,
		"--pod-namespace=" + target.TargetNamespace,
		"--pod-name=" + target.TargetName,
		"--pod-uid=" + target.TargetUID,
		"--container-ids=" + strings.Join(target.TargetContainerIds, ","),
		"--mode=" + string(run.ProfilingMode),
		"--duration=" + run.Duration.Duration.String(),
		"--output-dir=" + path.Join(collectorDataMount, RoundPrefix(job)),
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      CollectorPodName(job, target),
			Namespace: job.Spec.PersistentVolumeClaimNamespace,
			Labels: map[string]string{
				LabelComponent: ComponentCollector, LabelJobUID: string(job.UID),
			},
			Annotations: collectorAnnotations(job, target),
		},
		Spec: corev1.PodSpec{
			RestartPolicy:                 corev1.RestartPolicyNever,
			AutomountServiceAccountToken:  ptr.To(false),
			EnableServiceLinks:            ptr.To(false),
			TerminationGracePeriodSeconds: ptr.To(ceilSeconds(cfg.StopTimeout.Duration + terminationGraceSlop*time.Second)),
			Tolerations:                   cfg.Tolerations,
			PriorityClassName:             cfg.PriorityClassName,
			ImagePullSecrets:              cfg.ImagePullSecrets,
			// Do not set NodeName: the scheduler must honor PVC WFFC/topology.
			Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchFields: []corev1.NodeSelectorRequirement{{
							Key: "metadata.name", Operator: corev1.NodeSelectorOpIn,
							Values: []string{target.TargetNodeName},
						}},
					}},
				},
			}},
			Containers: []corev1.Container{{
				Name:            collectorContainer,
				Image:           cfg.Image,
				ImagePullPolicy: cfg.ImagePullPolicy,
				Command:         []string{collectorBinary, "collect"},
				Args:            args,
				Resources:       cfg.Resources,
				VolumeMounts: []corev1.VolumeMount{
					{Name: "runtimekit-socket", MountPath: socketDir},
					{Name: "data", MountPath: collectorDataMount},
				},
				SecurityContext:          rootSecurityContext(),
				TerminationMessagePath:   "/dev/termination-log",
				TerminationMessagePolicy: corev1.TerminationMessageReadFile,
			}},
			Volumes: []corev1.Volume{
				{Name: "runtimekit-socket", VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{Path: socketDir, Type: ptr.To(corev1.HostPathDirectory)},
				}},
				{Name: "data", VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: job.Spec.PersistentVolumeClaimName},
				}},
			},
		},
	}
	if pod.Namespace == job.Namespace {
		pod.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: "profiling.runtimekit.io/v1alpha1", Kind: "ProfilingJob",
			Name: job.Name, UID: job.UID, Controller: ptr.To(true), BlockOwnerDeletion: ptr.To(true),
		}}
	}
	// Cross-namespace Pods are cleaned up by the Job controller's finalizer.
	return pod
}

func collectorAnnotations(job *v1alpha1.ProfilingJob, target *v1alpha1.TargetStatus) map[string]string {
	return map[string]string{
		AnnotationJobNS: job.Namespace, AnnotationJobName: job.Name,
		AnnotationTargetUID:  target.TargetUID,
		AnnotationRunIndex:   strconv.FormatInt(runIndex(job), 10),
		AnnotationRetryCount: strconv.FormatInt(int64(target.RetryCount), 10),
	}
}

// CollectorMatches verifies a Pod belongs to this exact target retry. If the
// controller has recorded its Kubernetes UID, replacement Pods are rejected.
// Deletion/readiness are deliberately not identity checks.
func CollectorMatches(pod *corev1.Pod, job *v1alpha1.ProfilingJob, target *v1alpha1.TargetStatus) bool {
	if pod == nil || job == nil || target == nil || job.Status.RunConfig == nil ||
		job.UID == "" || target.TargetUID == "" {
		return false
	}
	if pod.Name != CollectorPodName(job, target) || pod.Namespace != job.Spec.PersistentVolumeClaimNamespace ||
		pod.Labels[LabelComponent] != ComponentCollector || pod.Labels[LabelJobUID] != string(job.UID) ||
		(target.CollectorPodUID != "" && string(pod.UID) != target.CollectorPodUID) ||
		(target.CollectorPodName != "" && pod.Name != target.CollectorPodName) ||
		(pod.Spec.NodeName != "" && pod.Spec.NodeName != target.TargetNodeName) {
		return false
	}
	for k, v := range collectorAnnotations(job, target) {
		if value, ok := pod.Annotations[k]; !ok || value != v {
			return false
		}
	}
	return true
}

// CollectorResult reads only the named collector container's structured report.
// Success requires both Kubernetes success and a valid successful report. It
// preserves per-container elapsed times for the controller to aggregate; retry
// classification belongs to the controller, not this decoder.
func CollectorResult(pod *corev1.Pod) collectorprotocol.Result {
	if pod == nil {
		return collectorprotocol.Failed(ReasonMissingCollectorReport, "collector Pod is missing")
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name != collectorContainer {
			continue
		}
		t := cs.State.Terminated
		if t == nil || strings.TrimSpace(t.Message) == "" {
			return collectorFallback(pod, &cs)
		}
		r, err := collectorprotocol.ParseResult(t.Message)
		if err != nil {
			return collectorprotocol.Failed(ReasonInvalidCollectorReport, fmt.Sprintf("decode collector termination message: %v", err))
		}
		if err := validateCollectorReport(r); err != nil {
			return collectorprotocol.Failed(ReasonInvalidCollectorReport, err.Error())
		}
		if r.Phase == collectorprotocol.PhaseSucceeded && (pod.Status.Phase != corev1.PodSucceeded || t.ExitCode != 0) {
			fallback := collectorFallback(pod, &cs)
			r.Phase, r.Reason, r.Message = fallback.Phase, fallback.Reason, fallback.Message
		}
		return r
	}
	return collectorFallback(pod, nil)
}

func validateCollectorReport(r collectorprotocol.Result) error {
	if r.Phase != collectorprotocol.PhaseSucceeded && r.Phase != collectorprotocol.PhaseFailed {
		return fmt.Errorf("invalid collector report phase %q", r.Phase)
	}
	if r.OmittedContainers < 0 || r.ElapsedMillis < 0 || (r.Phase == collectorprotocol.PhaseSucceeded && (r.StopError != "" || len(r.Containers)+r.OmittedContainers == 0)) {
		return fmt.Errorf("invalid collector report outcome")
	}
	for _, c := range r.Containers {
		if c.ContainerID == "" || (c.Phase != collectorprotocol.PhaseSucceeded && c.Phase != collectorprotocol.PhaseFailed) ||
			c.ElapsedMillis < 0 || c.Bytes < 0 ||
			(r.Phase == collectorprotocol.PhaseSucceeded && (c.Phase != collectorprotocol.PhaseSucceeded || c.StopError != "")) {
			return fmt.Errorf("invalid collector report container %q", c.ContainerID)
		}
		for _, name := range c.OutputFiles {
			if name == "" || name == "." || name == ".." || len(name) > 255 || strings.ContainsAny(name, "/\\\x00") {
				return fmt.Errorf("invalid collector output file %q", name)
			}
		}
	}
	return nil
}

func collectorFallback(pod *corev1.Pod, cs *corev1.ContainerStatus) collectorprotocol.Result {
	if cs != nil {
		if t := cs.State.Terminated; t != nil {
			reason := t.Reason
			if t.ExitCode == 0 {
				if pod.Status.Phase == corev1.PodSucceeded {
					reason = ReasonMissingCollectorReport
				} else {
					reason = pod.Status.Reason
					if reason == "" {
						reason = "PodNotSucceeded"
					}
				}
			} else if reason == "" {
				reason = "CollectorExitError"
			}
			message := fmt.Sprintf("collector exit code %d: %s", t.ExitCode, t.Reason)
			if pod.Status.Message != "" {
				message += "; " + pod.Status.Message
			}
			return collectorprotocol.Failed(reason, message)
		}
		if w := cs.State.Waiting; w != nil && w.Reason != "" {
			return collectorprotocol.Failed(w.Reason, w.Message)
		}
	}
	if pod.Status.Reason != "" {
		return collectorprotocol.Failed(pod.Status.Reason, pod.Status.Message)
	}
	for _, c := range pod.Status.Conditions {
		if c.Status != corev1.ConditionTrue && c.Reason != "" {
			return collectorprotocol.Failed(c.Reason, c.Message)
		}
	}
	return collectorprotocol.Failed(ReasonMissingCollectorReport, fmt.Sprintf("collector Pod phase %s: %s", pod.Status.Phase, pod.Status.Message))
}

func rootSecurityContext() *corev1.SecurityContext {
	// Root is required to reach the host Runtime Kit socket and write the PVC.
	return &corev1.SecurityContext{
		RunAsUser:                ptr.To(int64(0)),
		Privileged:               ptr.To(false),
		AllowPrivilegeEscalation: ptr.To(false),
		ReadOnlyRootFilesystem:   ptr.To(true),
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
	}
}

func shortHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])[:16]
}
