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

package activemonitoring

import (
	"strconv"
	"strings"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	gpuv1alpha1 "github.com/lj-runtimekit/job-controller/pkg/apis/gpu/v1alpha1"
)

// toPod converts the unstructured detection Pod back to a typed v1.Pod.
func toPod(t *testing.T, obj *unstructured.Unstructured) *v1.Pod {
	t.Helper()
	pod := &v1.Pod{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, pod); err != nil {
		t.Fatalf("convert from unstructured: %v", err)
	}
	return pod
}

func TestBuildTestPod(t *testing.T) {
	cr := testCR("default", "burn-test", gpuv1alpha1.ActiveMonitoringSpec{})
	task := gpuv1alpha1.TaskStatus{ID: "gpu-node-01-gpu-3", NodeName: "gpu-node-01", GPUMinor: 3, CurrentCycle: 7, CompletedRounds: 2}

	// Non-default image and socket prove both are taken from the pre-resolved
	// ConfigMap settings (detectionConfig), not from the built-in constants.
	const (
		wantImage  = "example/diag:test"
		wantSocket = "/run/test/dp2mgr.sock"
	)
	settings := &detectionSettings{image: wantImage, socketPath: wantSocket}

	obj, err := buildTestPod(cr, task, settings)
	if err != nil {
		t.Fatalf("buildTestPod: %v", err)
	}
	pod := toPod(t, obj)

	// Name: rt-diag-{taskID}-{5-char random suffix}.
	if !strings.HasPrefix(pod.Name, podNamePrefix+"-"+task.ID+"-") {
		t.Errorf("name = %q, want prefix %q", pod.Name, podNamePrefix+"-"+task.ID+"-")
	}
	if pod.Labels[LabelActiveMonitoringAttempt] != "1" {
		t.Errorf("首次 attempt 标签错误: %v", pod.Labels)
	}

	if pod.Namespace != cr.Namespace {
		t.Errorf("namespace = %q, want %q", pod.Namespace, cr.Namespace)
	}

	// Labels.
	if pod.Labels[LabelActiveMonitoring] != "true" {
		t.Errorf("label %q = %q, want true", LabelActiveMonitoring, pod.Labels[LabelActiveMonitoring])
	}
	if pod.Labels[LabelActiveMonitoringCR] != cr.Name {
		t.Errorf("label %q = %q, want %q", LabelActiveMonitoringCR, pod.Labels[LabelActiveMonitoringCR], cr.Name)
	}
	if pod.Labels[LabelActiveMonitoringTask] != task.ID {
		t.Errorf("label %q = %q, want %q", LabelActiveMonitoringTask, pod.Labels[LabelActiveMonitoringTask], task.ID)
	}
	if pod.Labels[LabelActiveMonitoringRound] != roundKey(task.CurrentCycle, task.CompletedRounds) {
		t.Errorf("label %q = %q, want %q", LabelActiveMonitoringRound, pod.Labels[LabelActiveMonitoringRound], roundKey(task.CurrentCycle, task.CompletedRounds))
	}

	if pod.Annotations[AnnoActiveMonitoringTargetMinor] != "3" {
		t.Errorf("annotation %q = %q, want 3", AnnoActiveMonitoringTargetMinor, pod.Annotations[AnnoActiveMonitoringTargetMinor])
	}

	// Owner reference: controlled by the CR so GC handles deletion.
	if len(pod.OwnerReferences) != 1 {
		t.Fatalf("ownerReferences len = %d, want 1", len(pod.OwnerReferences))
	}
	owner := pod.OwnerReferences[0]
	if owner.Kind != Kind || owner.Name != cr.Name || owner.UID != cr.UID {
		t.Errorf("ownerReference = %+v, want Kind=%q Name=%q UID=%q", owner, Kind, cr.Name, cr.UID)
	}
	if owner.Controller == nil || !*owner.Controller {
		t.Error("ownerReference.Controller should be true")
	}

	// Spec basics.
	if pod.Spec.SchedulerName != DefaultSchedulerName {
		t.Errorf("schedulerName = %q, want %q", pod.Spec.SchedulerName, DefaultSchedulerName)
	}
	if pod.Spec.PriorityClassName != ActiveMonitoringPriorityClassName {
		t.Errorf("priorityClassName = %q, want %q", pod.Spec.PriorityClassName, ActiveMonitoringPriorityClassName)
	}
	if pod.Spec.RestartPolicy != v1.RestartPolicyNever {
		t.Errorf("restartPolicy = %q, want Never", pod.Spec.RestartPolicy)
	}
	if pod.Spec.TerminationGracePeriodSeconds == nil || *pod.Spec.TerminationGracePeriodSeconds != defaultTerminationGracePeriodSeconds {
		t.Errorf("grace = %v, want %d", pod.Spec.TerminationGracePeriodSeconds, defaultTerminationGracePeriodSeconds)
	}

	// Node affinity pins to the target node via the hostname label.
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil {
		t.Fatal("expected nodeAffinity to be set")
	}
	terms := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(terms) != 1 || len(terms[0].MatchExpressions) != 1 {
		t.Fatalf("unexpected nodeAffinity terms: %+v", terms)
	}
	req := terms[0].MatchExpressions[0]
	if req.Key != HostnameLabelKey || req.Operator != v1.NodeSelectorOpIn ||
		len(req.Values) != 1 || req.Values[0] != task.NodeName {
		t.Errorf("nodeAffinity requirement = %+v, want hostname In [%s]", req, task.NodeName)
	}

	// Single container.
	if len(pod.Spec.Containers) != 1 {
		t.Fatalf("containers len = %d, want 1", len(pod.Spec.Containers))
	}
	ctr := pod.Spec.Containers[0]
	if ctr.Name != ActiveMonitoringContainerName {
		t.Errorf("container name = %q, want %q", ctr.Name, ActiveMonitoringContainerName)
	}
	if ctr.Image != wantImage {
		t.Errorf("image = %q, want %q", ctr.Image, wantImage)
	}

	// Resource-free card occupation: the detection Pod must NOT request any GPU resource;
	// the runtime mounts the target card via the XPU_VISIBLE_DEVICES env instead.
	if _, ok := ctr.Resources.Limits[v1.ResourceName(defaultGPUResourceName)]; ok {
		t.Errorf("gpu limit must be absent (the card is occupied via env, not via a GPU request)")
	}
	if _, ok := ctr.Resources.Requests[v1.ResourceName(defaultGPUResourceName)]; ok {
		t.Errorf("gpu request must be absent (the card is occupied via env, not via a GPU request)")
	}
	// CPU/memory are still requested so the container is schedulable and QoS-bounded.
	if c := ctr.Resources.Limits[v1.ResourceCPU]; c.Value() != 2 {
		t.Errorf("cpu limit = %v, want 2", c.String())
	}
	if m := ctr.Resources.Limits[v1.ResourceMemory]; m.String() != "24Gi" {
		t.Errorf("memory limit = %v, want 24Gi", m.String())
	}
	if m := ctr.Resources.Requests[v1.ResourceMemory]; m.String() != "16Gi" {
		t.Errorf("memory request = %v, want 16Gi", m.String())
	}

	if len(ctr.Command) != 0 {
		t.Errorf("Command = %v, want empty (image ENTRYPOINT must run)", ctr.Command)
	}
	if len(ctr.Args) != 0 {
		t.Errorf("Args = %v, want empty (image ENTRYPOINT must run)", ctr.Args)
	}
	// The single env var pins the target GPU minor for the runtime to mount.
	if len(ctr.Env) != 1 {
		t.Fatalf("Env len = %d, want 1 (%s)", len(ctr.Env), EnvXPUVisibleDevices)
	}
	if ctr.Env[0].Name != EnvXPUVisibleDevices || ctr.Env[0].Value != strconv.Itoa(task.GPUMinor) {
		t.Errorf("Env[0] = %+v, want %s=%s", ctr.Env[0], EnvXPUVisibleDevices, strconv.Itoa(task.GPUMinor))
	}
	if len(ctr.VolumeMounts) != 1 {
		t.Fatalf("VolumeMounts len = %d, want 1", len(ctr.VolumeMounts))
	}
	vm := ctr.VolumeMounts[0]
	if vm.Name != runtimeKitSocketVolumeName || vm.MountPath != wantSocket {
		t.Errorf("VolumeMount = %+v, want name=%q mountPath=%q", vm, runtimeKitSocketVolumeName, wantSocket)
	}
	if len(pod.Spec.Volumes) != 1 {
		t.Fatalf("Volumes len = %d, want 1", len(pod.Spec.Volumes))
	}
	vol := pod.Spec.Volumes[0]
	if vol.Name != runtimeKitSocketVolumeName {
		t.Errorf("Volume name = %q, want %q", vol.Name, runtimeKitSocketVolumeName)
	}
	if vol.HostPath == nil || vol.HostPath.Path != wantSocket {
		t.Errorf("Volume hostPath = %+v, want path=%q", vol.HostPath, wantSocket)
	}
	if vol.HostPath != nil && (vol.HostPath.Type == nil || *vol.HostPath.Type != v1.HostPathSocket) {
		t.Errorf("Volume hostPath type = %v, want Socket", vol.HostPath.Type)
	}

	if ctr.TerminationMessagePath != "/dev/termination-log" {
		t.Errorf("terminationMessagePath = %q, want /dev/termination-log", ctr.TerminationMessagePath)
	}
	if ctr.TerminationMessagePolicy != v1.TerminationMessageFallbackToLogsOnError {
		t.Errorf("terminationMessagePolicy = %q, want %q", ctr.TerminationMessagePolicy, v1.TerminationMessageFallbackToLogsOnError)
	}
}
