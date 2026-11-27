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
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/ptr"

	v1alpha1 "github.com/lj-runtimekit/job-controller/api/v1alpha1"
	"github.com/lj-runtimekit/job-controller/pkg/collectorprotocol"
)

func testJobTarget() (*v1alpha1.ProfilingJob, *v1alpha1.TargetStatus) {
	return &v1alpha1.ProfilingJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: "jobs", Name: "profile", UID: types.UID("job-uid")},
		// PVC references come from immutable spec; mutable run inputs deliberately disagree.
		Spec: v1alpha1.ProfilingJobSpec{
			RunIndex: 99, Duration: metav1.Duration{Duration: time.Hour}, ProfilingMode: v1alpha1.ProfilingModeLight,
			PersistentVolumeClaimNamespace: "storage", PersistentVolumeClaimName: "results",
		},
		Status: v1alpha1.ProfilingJobStatus{
			LastRunIndex: ptr.To(int64(7)),
			RunConfig: &v1alpha1.RunConfiguration{
				Duration: metav1.Duration{Duration: time.Minute}, ProfilingMode: v1alpha1.ProfilingModeFull,
			},
		},
	}, &v1alpha1.TargetStatus{TargetNamespace: "workload", TargetName: "gpu-pod", TargetUID: "target-uid", TargetNodeName: "node-a", TargetContainerIds: []string{"containerd://abc", "containerd://def"}}
}

func TestCollectorNamingAndPrefixes(t *testing.T) {
	job, target := testJobTarget()
	name := CollectorPodName(job, target)
	if name != CollectorPodName(job, target) || len(name) > 63 || len(validation.IsDNS1123Label(name)) != 0 {
		t.Fatalf("invalid or non-deterministic name %q", name)
	}
	if got := RoundPrefix(job); got != "jobs/profile/run-7" {
		t.Fatalf("RoundPrefix = %q", got)
	}
	cfg := testConfig(t)
	report := collectorprotocol.Result{Phase: collectorprotocol.PhaseSucceeded, Containers: []collectorprotocol.ContainerResult{
		{ContainerID: "abc", Phase: collectorprotocol.PhaseSucceeded, OutputFiles: []string{"workload_gpu-pod_abc_lastkernels.log", "workload_gpu-pod_abc_kerneltime.log"}},
		{ContainerID: "def", Phase: collectorprotocol.PhaseSucceeded, OutputFiles: []string{"daemon-chosen-empty.log"}},
	}}
	wantFiles := []string{
		"jobs/profile/run-7/workload_gpu-pod_abc_lastkernels.log",
		"jobs/profile/run-7/workload_gpu-pod_abc_kerneltime.log",
		"jobs/profile/run-7/daemon-chosen-empty.log",
	}
	if pod := CollectorPod(cfg, job, target); strings.Contains(strings.Join(pod.Spec.Containers[0].Args, " "), "--format") {
		t.Fatal("collector must not pass an obsolete format flag")
	}
	if got := OutputFiles(job, report); !reflect.DeepEqual(got, wantFiles) {
		t.Fatalf("OutputFiles = %q, want actual report files %q", got, wantFiles)
	}
	if got := OutputFiles(job, collectorprotocol.Result{Phase: collectorprotocol.PhaseSucceeded, OmittedContainers: 2}); len(got) != 0 {
		t.Fatalf("omitted report containers must not predict output names: %q", got)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*v1alpha1.ProfilingJob, *v1alpha1.TargetStatus)
	}{
		{"job UID", func(j *v1alpha1.ProfilingJob, _ *v1alpha1.TargetStatus) { j.UID = "other-job" }},
		{"run", func(j *v1alpha1.ProfilingJob, _ *v1alpha1.TargetStatus) { *j.Status.LastRunIndex++ }},
		{"target UID", func(_ *v1alpha1.ProfilingJob, s *v1alpha1.TargetStatus) { s.TargetUID = "other-target" }},
		{"retry", func(_ *v1alpha1.ProfilingJob, s *v1alpha1.TargetStatus) { s.RetryCount++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			j, s := testJobTarget()
			tc.mutate(j, s)
			if CollectorPodName(j, s) == name {
				t.Fatal("identity change did not isolate the collector Pod")
			}
		})
	}
	job.Name, target.TargetName = strings.Repeat("x", 253), strings.Repeat("y", 253)
	if got := CollectorPodName(job, target); len(got) > 63 || len(validation.IsDNS1123Label(got)) != 0 {
		t.Fatalf("long input name = %q", got)
	}
}

func TestCollectorPodSpec(t *testing.T) {
	cfg := testConfig(t)
	job, target := testJobTarget()
	pod := CollectorPod(cfg, job, target)
	if pod == nil {
		t.Fatal("nil collector")
	}
	if pod.Namespace != "storage" || pod.Name != CollectorPodName(job, target) {
		t.Fatalf("metadata = %+v", pod.ObjectMeta)
	}
	if pod.Labels[LabelComponent] != ComponentCollector || pod.Labels[LabelJobUID] != "job-uid" || !CollectorMatches(pod, job, target) {
		t.Fatal("missing identity")
	}
	wantAnnotations := map[string]string{
		AnnotationJobNS: "jobs", AnnotationJobName: "profile", AnnotationTargetUID: "target-uid",
		AnnotationRunIndex: "7", AnnotationRetryCount: "0",
	}
	if !reflect.DeepEqual(pod.Annotations, wantAnnotations) {
		t.Fatalf("annotations = %+v", pod.Annotations)
	}
	if len(pod.OwnerReferences) != 0 {
		t.Fatal("cross-namespace controller ownerRef is forbidden")
	}
	spec := pod.Spec
	if spec.RestartPolicy != corev1.RestartPolicyNever || spec.NodeName != "" || len(spec.NodeSelector) != 0 {
		t.Fatalf("restart/scheduling = %+v", spec)
	}
	wantAffinity := &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
		NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchFields: []corev1.NodeSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{"node-a"}}}}},
	}}}
	if !reflect.DeepEqual(spec.Affinity, wantAffinity) {
		t.Fatalf("affinity = %+v", spec.Affinity)
	}
	if spec.AutomountServiceAccountToken == nil || *spec.AutomountServiceAccountToken || spec.EnableServiceLinks == nil || *spec.EnableServiceLinks {
		t.Fatal("API token and service links must be disabled")
	}
	if spec.ActiveDeadlineSeconds != nil || spec.TerminationGracePeriodSeconds == nil || *spec.TerminationGracePeriodSeconds != 30 {
		t.Fatalf("deadline/grace = %v/%v", spec.ActiveDeadlineSeconds, spec.TerminationGracePeriodSeconds)
	}
	if len(spec.Containers) != 1 || len(spec.Volumes) != 2 {
		t.Fatal("unexpected container/volume count")
	}
	c := spec.Containers[0]
	if c.Name != "collector" || !reflect.DeepEqual(c.Command, []string{"/bin/profiling-collector", "collect"}) || c.Image != cfg.Image || c.ImagePullPolicy != cfg.ImagePullPolicy {
		t.Fatalf("container command/image = %+v", c)
	}
	if c.ReadinessProbe != nil || c.LivenessProbe != nil || c.StartupProbe != nil || len(c.Ports) != 0 {
		t.Fatal("one-shot collector must not have probes or ports")
	}
	if c.TerminationMessagePolicy != corev1.TerminationMessageReadFile || c.TerminationMessagePath != "/dev/termination-log" {
		t.Fatal("termination report must be read from file")
	}
	wantMounts := []corev1.VolumeMount{{Name: "runtimekit-socket", MountPath: "/run/amp/eKitC/xdm/sockets"}, {Name: "data", MountPath: "/data"}}
	if !reflect.DeepEqual(c.VolumeMounts, wantMounts) {
		t.Fatalf("mounts = %+v", c.VolumeMounts)
	}
	socket := spec.Volumes[0].HostPath
	if socket == nil || socket.Path != "/run/amp/eKitC/xdm/sockets" || socket.Type == nil || *socket.Type != corev1.HostPathDirectory {
		t.Fatalf("socket hostPath = %+v", socket)
	}
	pvc := spec.Volumes[1].PersistentVolumeClaim
	if pvc == nil || pvc.ClaimName != "results" || pvc.ReadOnly || spec.Volumes[1].HostPath != nil {
		t.Fatalf("data PVC = %+v", pvc)
	}
	sc := c.SecurityContext
	if sc == nil || sc.RunAsUser == nil || *sc.RunAsUser != 0 || sc.Privileged == nil || *sc.Privileged || sc.AllowPrivilegeEscalation == nil || *sc.AllowPrivilegeEscalation ||
		sc.ReadOnlyRootFilesystem == nil || !*sc.ReadOnlyRootFilesystem || sc.SeccompProfile == nil || sc.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault ||
		sc.Capabilities == nil || len(sc.Capabilities.Add) != 0 || !reflect.DeepEqual(sc.Capabilities.Drop, []corev1.Capability{"ALL"}) {
		t.Fatalf("security context = %+v", sc)
	}
	wantArgs := []string{
		"--socket=" + cfg.SocketPath, "--pod-namespace=workload", "--pod-name=gpu-pod", "--pod-uid=target-uid",
		"--container-ids=containerd://abc,containerd://def", "--mode=full", "--duration=1m0s",
		"--output-dir=/data/jobs/profile/run-7",
	}
	if !reflect.DeepEqual(c.Args, wantArgs) {
		t.Fatalf("args = %#v, want %#v", c.Args, wantArgs)
	}
}

func TestCollectorPodUsesFrozenProfilingMode(t *testing.T) {
	cfg := testConfig(t)
	for _, tc := range []struct {
		mode     v1alpha1.ProfilingMode
		specMode v1alpha1.ProfilingMode
		want     string
	}{
		{v1alpha1.ProfilingModeLight, v1alpha1.ProfilingModeFull, "--mode=light"},
		{v1alpha1.ProfilingModeFull, v1alpha1.ProfilingModeLight, "--mode=full"},
	} {
		t.Run(string(tc.mode), func(t *testing.T) {
			job, target := testJobTarget()
			job.Spec.ProfilingMode = tc.specMode
			job.Status.RunConfig.ProfilingMode = tc.mode
			pod := CollectorPod(cfg, job, target)
			if pod == nil {
				t.Fatal("合法模式未生成 collector Pod")
			}
			var modeArgs []string
			for _, arg := range pod.Spec.Containers[0].Args {
				if strings.HasPrefix(arg, "--mode=") {
					modeArgs = append(modeArgs, arg)
				}
			}
			if !reflect.DeepEqual(modeArgs, []string{tc.want}) {
				t.Fatalf("collector 必须使用冻结的采集模式：%v，期望 %s", modeArgs, tc.want)
			}
		})
	}
}

func TestCollectorPodOverridesAndOwner(t *testing.T) {
	cfg := testConfig(t)
	cfg.Resources = corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")}, Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("256Mi")}}
	cfg.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "registry"}}
	cfg.Tolerations = []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpExists}}
	cfg.PriorityClassName = "profiling-high"
	cfg.SocketPath = "/run/custom/runtime.sock"
	cfg.StopTimeout.Duration = 1500 * time.Millisecond
	cfg.MaxRetries = ptr.To(int32(0))
	cfg.RetryBackoff.Duration, cfg.MaxRetryBackoff.Duration = time.Second, 3*time.Second
	job, target := testJobTarget()
	job.Spec.PersistentVolumeClaimNamespace = job.Namespace
	pod := CollectorPod(cfg, job, target)
	if len(pod.OwnerReferences) != 1 {
		t.Fatal("same-namespace controller ownerRef missing")
	}
	o := pod.OwnerReferences[0]
	if o.APIVersion != "profiling.runtimekit.io/v1alpha1" || o.Kind != "ProfilingJob" || o.Name != job.Name || o.UID != job.UID || o.Controller == nil || !*o.Controller || o.BlockOwnerDeletion == nil || !*o.BlockOwnerDeletion {
		t.Fatalf("ownerRef = %+v", o)
	}
	if !reflect.DeepEqual(pod.Spec.Containers[0].Resources, cfg.Resources) || !reflect.DeepEqual(pod.Spec.Tolerations, cfg.Tolerations) || !reflect.DeepEqual(pod.Spec.ImagePullSecrets, cfg.ImagePullSecrets) || pod.Spec.PriorityClassName != cfg.PriorityClassName {
		t.Fatal("Pod overrides not preserved")
	}
	if pod.Spec.Volumes[0].HostPath.Path != "/run/custom" || pod.Spec.Containers[0].VolumeMounts[0].MountPath != "/run/custom" || *pod.Spec.TerminationGracePeriodSeconds != 17 || pod.Spec.ActiveDeadlineSeconds != nil {
		t.Fatal("custom socket or rounded timing not honored")
	}
	args := strings.Join(pod.Spec.Containers[0].Args, " ")
	if !strings.Contains(args, "--socket="+cfg.SocketPath) {
		t.Fatalf("socket 未下发: %s", args)
	}
	for _, arg := range []string{"--cleanup-leftovers", "--poll-interval", "--rpc-timeout", "--start-timeout", "--stream-idle-timeout", "--finish-timeout", "--stop-timeout", "--max-retries", "--retry-backoff", "--max-retry-backoff", "--start-busy-retries"} {
		if strings.Contains(args, arg) {
			t.Fatalf("只下发目标、轮次与 socket，不应携带 %s: %s", arg, args)
		}
	}
}

func TestCollectorLeavesCleanupToCollectorDefault(t *testing.T) {
	for _, retry := range []int32{0, 1, 3} {
		job, target := testJobTarget()
		target.RetryCount = retry
		pod := CollectorPod(testConfig(t), job, target)
		for _, arg := range pod.Spec.Containers[0].Args {
			// 残留会话清理由 collector 默认开启，controller 不得下发，也不得显式关掉。
			if strings.HasPrefix(arg, "--cleanup-leftovers") {
				t.Fatalf("retry %d must not pass %s", retry, arg)
			}
			if strings.HasPrefix(arg, "--attempt") {
				t.Fatalf("obsolete attempt argument: %s", arg)
			}
		}
	}
}

func TestCollectorPodInvalidInputs(t *testing.T) {
	cfg := testConfig(t)
	job, target := testJobTarget()
	if CollectorPod(nil, job, target) != nil || CollectorPod(cfg, nil, target) != nil || CollectorPod(cfg, job, nil) != nil {
		t.Fatal("nil input accepted")
	}
	unstarted := job.DeepCopy()
	unstarted.Status.LastRunIndex = nil
	if CollectorPod(cfg, unstarted, target) != nil {
		t.Fatal("collector built before the first round started")
	}
	zero := job.DeepCopy()
	zero.Status.LastRunIndex = ptr.To(int64(0))
	if pod := CollectorPod(cfg, zero, target); pod == nil || pod.Annotations[AnnotationRunIndex] != "0" || RoundPrefix(zero) != "jobs/profile/run-0" {
		t.Fatalf("round 0 collector = %v, prefix %q", pod, RoundPrefix(zero))
	}
	for _, mutate := range []func(*v1alpha1.ProfilingJobSpec){
		func(spec *v1alpha1.ProfilingJobSpec) { spec.PersistentVolumeClaimName = "" },
		func(spec *v1alpha1.ProfilingJobSpec) { spec.PersistentVolumeClaimNamespace = "" },
	} {
		missingPVC := job.DeepCopy()
		mutate(&missingPVC.Spec)
		if CollectorPod(cfg, missingPVC, target) != nil {
			t.Fatal("collector accepted incomplete immutable spec PVC references")
		}
	}
	job.Status.RunConfig.Duration.Duration = time.Duration(math.MaxInt64)
	if CollectorPod(cfg, job, target) != nil {
		t.Fatal("overflowing collector stage timeout accepted")
	}
	job.Status.RunConfig = nil
	if CollectorPod(cfg, job, target) != nil {
		t.Fatal("live spec used without frozen run")
	}
}

func TestCollectorConfigChangesDoNotChangeIdentity(t *testing.T) {
	job, target := testJobTarget()
	oldConfig := testConfig(t)
	old := CollectorPod(oldConfig, job, target)
	old.UID = "collector-uid"
	target.CollectorPodName, target.CollectorPodUID = old.Name, string(old.UID)
	before := old.DeepCopy()
	latest := testConfig(t)
	latest.Image = "collector:v2"
	latest.PollInterval.Duration = 7 * time.Second
	latest.MaxRetries = ptr.To(int32(1))
	latest.RetryBackoff.Duration, latest.MaxRetryBackoff.Duration = 3*time.Second, 9*time.Second
	fresh := CollectorPod(latest, job, target)
	if fresh == nil || fresh.Name != old.Name || !CollectorMatches(old, job, target) {
		t.Fatal("latest configuration changed collector identity or rejected the existing collector")
	}
	args := strings.Join(fresh.Spec.Containers[0].Args, " ")
	for _, want := range []string{"--duration=1m0s", "--mode=full"} {
		if !strings.Contains(args, want) {
			t.Fatalf("frozen run input missing: %s", args)
		}
	}
	for _, unwanted := range []string{"--poll-interval", "--max-retries", "--retry-backoff"} {
		if strings.Contains(args, unwanted) {
			t.Fatalf("超时与重试不再下发 collector，应使用默认配置: %s", args)
		}
	}
	if fresh.Spec.Containers[0].Image != "collector:v2" || reflect.DeepEqual(fresh.Spec, old.Spec) || !reflect.DeepEqual(old, before) {
		t.Fatal("latest config must affect new Pod specs only")
	}
	// An existing collector remains identifiable even if a new stage timeout cannot fit.
	latest.FinishTimeout.Duration = time.Duration(math.MaxInt64) - 30*time.Second
	if CollectorPod(latest, job, target) != nil || !CollectorMatches(old, job, target) {
		t.Fatal("duration validation must prevent new collectors, not invalidate existing ones")
	}
}

func TestCollectorMatches(t *testing.T) {
	job, target := testJobTarget()
	pod := CollectorPod(testConfig(t), job, target)
	for _, key := range []string{AnnotationJobNS, AnnotationJobName, AnnotationTargetUID, AnnotationRunIndex, AnnotationRetryCount} {
		copy := pod.DeepCopy()
		delete(copy.Annotations, key)
		if CollectorMatches(copy, job, target) {
			t.Errorf("missing %s accepted", key)
		}
		copy.Annotations[key] = "wrong"
		if CollectorMatches(copy, job, target) {
			t.Errorf("wrong %s accepted", key)
		}
	}
	for _, mutate := range []func(*corev1.Pod){
		func(p *corev1.Pod) { p.Name = "wrong" }, func(p *corev1.Pod) { p.Namespace = "wrong" },
		func(p *corev1.Pod) { p.Labels[LabelComponent] = "wrong" }, func(p *corev1.Pod) { p.Labels[LabelJobUID] = "wrong" },
		func(p *corev1.Pod) { p.Spec.NodeName = "node-b" },
	} {
		copy := pod.DeepCopy()
		mutate(copy)
		if CollectorMatches(copy, job, target) {
			t.Fatal("wrong identity accepted")
		}
	}
	target.CollectorPodName, target.CollectorPodUID = pod.Name, "pod-uid"
	pod.UID = "other-uid"
	if CollectorMatches(pod, job, target) {
		t.Fatal("replacement Pod accepted")
	}
	pod.UID, pod.Spec.NodeName = "pod-uid", "node-a"
	if !CollectorMatches(pod, job, target) {
		t.Fatal("recorded assigned Pod rejected")
	}
	if CollectorMatches(nil, job, target) || CollectorMatches(pod, nil, target) || CollectorMatches(pod, job, nil) {
		t.Fatal("nil identity accepted")
	}
}

func reportPod(phase corev1.PodPhase, exit int32, reason, message string) *corev1.Pod {
	return &corev1.Pod{Status: corev1.PodStatus{Phase: phase, ContainerStatuses: []corev1.ContainerStatus{{Name: "collector", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: exit, Reason: reason, Message: message}}}}}}
}

func TestCollectorResultStructured(t *testing.T) {
	for _, report := range []collectorprotocol.Result{
		{Phase: collectorprotocol.PhaseSucceeded, PodNamespace: "workload", PodName: "gpu-pod", PodUID: "target-uid", Containers: []collectorprotocol.ContainerResult{
			{ContainerID: "abc", Phase: collectorprotocol.PhaseSucceeded, ElapsedMillis: 1000, OutputFiles: []string{"workload_gpu-pod_abc_lastkernels.log", "workload_gpu-pod_abc_kerneltime.log"}, Bytes: 10},
			{ContainerID: "def", Phase: collectorprotocol.PhaseSucceeded, ElapsedMillis: 2000, OutputFiles: []string{"workload_gpu-pod_def_empty.log"}, Bytes: 0},
		}},
		{Phase: collectorprotocol.PhaseSucceeded, Containers: []collectorprotocol.ContainerResult{{ContainerID: "abc", Phase: collectorprotocol.PhaseSucceeded, OutputFiles: []string{strings.Repeat("a", 255)}}}},
		{Phase: collectorprotocol.PhaseSucceeded, Containers: []collectorprotocol.ContainerResult{{ContainerID: "abc", Phase: collectorprotocol.PhaseSucceeded, ElapsedMillis: 42}}},
		{Phase: collectorprotocol.PhaseFailed, Reason: collectorprotocol.ReasonStopFailed, StopError: "stop failed", Containers: []collectorprotocol.ContainerResult{{ContainerID: "abc", Phase: collectorprotocol.PhaseFailed, ElapsedMillis: 800}}},
	} {
		raw, _ := json.Marshal(report)
		phase, exit := corev1.PodSucceeded, int32(0)
		if report.Phase == collectorprotocol.PhaseFailed {
			phase, exit = corev1.PodFailed, 1
		}
		pod := reportPod(phase, exit, "", string(raw))
		// The first status must never be mistaken for the collector report.
		pod.Status.ContainerStatuses = append([]corev1.ContainerStatus{{Name: "sidecar", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Message: "bogus"}}}}, pod.Status.ContainerStatuses...)
		if got := CollectorResult(pod); !reflect.DeepEqual(got, report) {
			t.Fatalf("report = %+v, want %+v", got, report)
		}
	}
	// Bounded termination reports may legitimately omit all container details.
	if got := CollectorResult(reportPod(corev1.PodSucceeded, 0, "Completed", `{"phase":"Succeeded","omittedContainers":99}`)); got.Phase != collectorprotocol.PhaseSucceeded {
		t.Fatalf("slim valid report rejected: %+v", got)
	}
}

func TestCollectorResultRejectsFalseSuccess(t *testing.T) {
	for _, tc := range []struct {
		phase  corev1.PodPhase
		exit   int32
		reason string
	}{
		{corev1.PodFailed, 0, ""}, {corev1.PodFailed, 137, "OOMKilled"}, {corev1.PodSucceeded, 1, "Error"}, {corev1.PodRunning, 0, ""},
	} {
		got := CollectorResult(reportPod(tc.phase, tc.exit, tc.reason, `{"phase":"Succeeded","containers":[{"containerID":"abc","phase":"Succeeded","elapsedMillis":42}]}`))
		if got.Phase != collectorprotocol.PhaseFailed || got.Reason == "" || len(got.Containers) != 1 || got.Containers[0].ElapsedMillis != 42 {
			t.Fatalf("false success for %+v: %+v", tc, got)
		}
	}
}

func TestCollectorResultMalformed(t *testing.T) {
	for _, msg := range []string{
		"not JSON", "{", "null", "{}", `{"phase":"done"}`, `{"phase":"Succeeded","omittedContainers":-1}`,
		`{"phase":"Succeeded"}`, `{"phase":"Failed","elapsedMillis":-1}`,
		`{"phase":"Succeeded","containers":[{"containerID":"abc","phase":"Failed"}]}`,
		`{"phase":"Succeeded","stopError":"failed"}`, `{"phase":"Failed","containers":[{"containerID":"abc","phase":"Failed","elapsedMillis":-1}]}`,
	} {
		got := CollectorResult(reportPod(corev1.PodFailed, 1, "Error", msg))
		if got.Phase != collectorprotocol.PhaseFailed || got.Reason != ReasonInvalidCollectorReport {
			t.Errorf("malformed %q: %+v", msg, got)
		}
	}
}

func TestCollectorResultRejectsUnsafeOutputNames(t *testing.T) {
	for _, phase := range []collectorprotocol.Phase{collectorprotocol.PhaseSucceeded, collectorprotocol.PhaseFailed} {
		for _, tc := range []struct {
			name, fileName string
		}{
			{"empty", ""}, {"dot", "."}, {"dot dot", ".."}, {"too long", strings.Repeat("a", 256)},
			{"absolute", "/tmp/result.log"}, {"slash", "dir/result.log"}, {"traversal", "../result.log"},
			{"backslash", `dir\result.log`}, {"NUL", "result\x00.log"},
		} {
			t.Run(string(phase)+"/"+tc.name, func(t *testing.T) {
				report := collectorprotocol.Result{Phase: phase, Containers: []collectorprotocol.ContainerResult{{
					ContainerID: "abc", Phase: phase, OutputFiles: []string{"safe.log", tc.fileName},
				}}}
				raw, err := json.Marshal(report)
				if err != nil {
					t.Fatal(err)
				}
				podPhase, exit := corev1.PodSucceeded, int32(0)
				if phase == collectorprotocol.PhaseFailed {
					podPhase, exit = corev1.PodFailed, 1
				}
				got := CollectorResult(reportPod(podPhase, exit, "", string(raw)))
				if got.Phase != collectorprotocol.PhaseFailed || got.Reason != ReasonInvalidCollectorReport {
					t.Fatalf("unsafe output name %q accepted: %+v", tc.fileName, got)
				}
			})
		}
	}
}

func TestCollectorResultFallback(t *testing.T) {
	for _, tc := range []struct {
		name            string
		pod             *corev1.Pod
		reason, message string
	}{
		{"OOM", reportPod(corev1.PodFailed, 137, "OOMKilled", ""), "OOMKilled", "exit code 137"},
		{"exit code", reportPod(corev1.PodFailed, 7, "", ""), "CollectorExitError", "exit code 7"},
		{"success without report", reportPod(corev1.PodSucceeded, 0, "Completed", ""), ReasonMissingCollectorReport, "exit code 0"},
		{"waiting", &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodPending, ContainerStatuses: []corev1.ContainerStatus{{Name: "collector", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff", Message: "pull failed"}}}}}}, "ImagePullBackOff", "pull failed"},
		{"deadline", &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodFailed, Reason: "DeadlineExceeded", Message: "deadline elapsed"}}, "DeadlineExceeded", "deadline elapsed"},
		{"condition", &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: "Unschedulable", Message: "volume topology"}}}}, "Unschedulable", "volume topology"},
		{"no collector", &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodSucceeded, ContainerStatuses: []corev1.ContainerStatus{{Name: "sidecar", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Message: `{"phase":"Succeeded"}`}}}}}}, ReasonMissingCollectorReport, "Succeeded"},
		{"nil", nil, ReasonMissingCollectorReport, "missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := CollectorResult(tc.pod)
			if got.Phase != collectorprotocol.PhaseFailed || got.Reason != tc.reason || !strings.Contains(got.Message, tc.message) {
				t.Fatalf("fallback = %+v, want %s/%s", got, tc.reason, tc.message)
			}
		})
	}
}
