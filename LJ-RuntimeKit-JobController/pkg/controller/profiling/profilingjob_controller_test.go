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
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/textlogger"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	profilingv1alpha1 "github.com/lj-runtimekit/job-controller/api/v1alpha1"
	"github.com/lj-runtimekit/job-controller/pkg/collectorprotocol"
	"github.com/lj-runtimekit/job-controller/pkg/nodeagent"
)

const (
	ns          = "inference"
	collectorNS = "kube-system"
	configName  = "profiling-collector-config"
	nodeName    = "node-1"
)

type fixture struct {
	t      *testing.T
	ctx    context.Context
	client client.Client
	r      *ProfilingJobReconciler
	now    time.Time
}

func newFixture(t *testing.T, objs ...client.Object) *fixture {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := profilingv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&profilingv1alpha1.ProfilingJob{}, &corev1.Pod{}, &corev1.PersistentVolumeClaim{}).WithObjects(objs...).Build()
	f := &fixture{t: t, ctx: context.Background(), client: c, now: time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)}
	f.r = &ProfilingJobReconciler{Client: c, APIReader: c, Scheme: scheme, CollectorNamespace: collectorNS, ConfigMapName: configName, Now: func() time.Time { return f.now }}
	return f
}

func TestProfilingStartupLogsAtDefaultVerbosity(t *testing.T) {
	for _, synced := range []bool{true, false} {
		t.Run(fmt.Sprintf("synced=%v", synced), func(t *testing.T) {
			var output bytes.Buffer
			logger := textlogger.NewLogger(textlogger.NewConfig(textlogger.Verbosity(0), textlogger.Output(&output)))
			ctx, cancel := context.WithCancel(klog.NewContext(context.Background(), logger))
			defer cancel()
			a, c := testAdapter(func(ctx context.Context) error { <-ctx.Done(); return nil })
			for i, informer := range c.informers {
				informer.synced.Store(synced || i < 4)
			}
			if !synced {
				a.syncTimeout = 25 * time.Millisecond
			}
			done := make(chan error, 1)
			go func() { done <- a.Run(ctx) }()
			if synced {
				select {
				case <-a.Ready():
				case <-time.After(3 * time.Second):
					t.Fatal("Profiling 未就绪")
				}
				cancel()
			}
			err := awaitAdapterExit(t, done)
			if (err == nil) != synced {
				t.Fatalf("缓存同步结果不符合预期: %v", err)
			}
			text := output.String()
			for _, msg := range []string{"Starting profiling controller-runtime manager", "waiting for informer caches to sync", "Registering profiling informer", "elapsed="} {
				if !strings.Contains(text, msg) {
					t.Fatalf("默认级别缺少 %s: %s", msg, text)
				}
			}
			if synced {
				if !strings.Contains(text, "controller ready") || !strings.Contains(text, "Profiling controller-runtime manager exited") {
					t.Fatalf("缺少就绪或退出日志: %s", text)
				}
			} else if !strings.Contains(text, "pendingInformers=") || !strings.Contains(text, "*v1.ConfigMap") || !strings.Contains(text, "Profiling controller startup failed") || strings.Contains(text, "controller ready") {
				t.Fatalf("同步失败日志不能定位未同步资源: %s", text)
			}
		})
	}
}

func TestProfilingReconcileFailureLogsAtDefaultVerbosity(t *testing.T) {
	f := baseFixture(t)
	var output bytes.Buffer
	logger := textlogger.NewLogger(textlogger.NewConfig(textlogger.Verbosity(0), textlogger.Output(&output)))
	f.ctx = klog.NewContext(f.ctx, logger)
	f.r.Client = &statusWriteFailureClient{Client: f.client}
	_, err := f.r.Reconcile(f.ctx, ctrl.Request{NamespacedName: client.ObjectKey{Namespace: ns, Name: "job"}})
	if !apierrors.IsServiceUnavailable(err) {
		t.Fatalf("日志不能改变返回错误: %v", err)
	}
	text := output.String()
	for _, field := range []string{"ProfilingJob reconcile failed", `job="inference/job"`, `operation="updateStatus"`, "status persistence failed", "elapsed="} {
		if !strings.Contains(text, field) {
			t.Fatalf("调谐失败日志缺少 %s: %s", field, text)
		}
	}
	if strings.Contains(text, "ProfilingJob status changed") || strings.Contains(text, "Profiling target status changed") {
		t.Fatalf("写入失败时不应报告状态提交成功: %s", text)
	}
	output.Reset()
	f.r.Client = f.client
	f.start()
	for _, msg := range []string{"ProfilingJob status changed", "Profiling target status changed", "Created collector Pod"} {
		if !strings.Contains(output.String(), msg) {
			t.Fatalf("没有 EventRecorder 时缺少显式日志 %s: %s", msg, output.String())
		}
	}
}

type configLogTestClient struct {
	client.Client
	readErr error
	emptyRV bool
}

func (c *configLogTestClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1.ConfigMap); ok && c.readErr != nil {
		return c.readErr
	}
	if err := c.Client.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	if _, ok := obj.(*corev1.ConfigMap); ok && c.emptyRV {
		obj.SetResourceVersion("")
	}
	return nil
}

func TestCollectorConfigLoadAndRecoveryLogs(t *testing.T) {
	for _, emptyRV := range []bool{false, true} {
		t.Run(fmt.Sprintf("emptyRV=%v", emptyRV), func(t *testing.T) {
			f := baseFixture(t)
			var output bytes.Buffer
			logger := textlogger.NewLogger(textlogger.NewConfig(textlogger.Verbosity(0), textlogger.Output(&output)))
			f.ctx = klog.NewContext(f.ctx, logger)
			c := &configLogTestClient{Client: f.client, emptyRV: emptyRV}
			f.r.Client = c
			for i := 0; i < 2; i++ {
				if _, err := f.r.collectorConfig(f.ctx); err != nil {
					t.Fatal(err)
				}
			}
			if strings.Count(output.String(), "Collector configuration loaded") != 1 || !strings.Contains(output.String(), `configMap="kube-system/profiling-collector-config"`) {
				t.Fatalf("首次加载应记录一次，重复轮询不应重复记录: %s", output.String())
			}
			c.readErr = errors.New("temporary config read failure")
			for i := 0; i < 2; i++ {
				cfg, err := f.r.collectorConfig(f.ctx)
				if err != nil || cfg.Image != "collector:v1" {
					t.Fatalf("日志不能影响 lastGood 回退: cfg=%v err=%v", cfg, err)
				}
			}
			if strings.Count(output.String(), "collector configuration rejected") != 1 || !strings.Contains(output.String(), "usingLastGood=true") {
				t.Fatalf("缺少去重的配置回退日志: %s", output.String())
			}
			c.readErr = nil
			for i := 0; i < 2; i++ {
				if _, err := f.r.collectorConfig(f.ctx); err != nil {
					t.Fatal(err)
				}
			}
			if strings.Count(output.String(), "Collector configuration loaded") != 2 || !strings.Contains(output.String(), "recoveredFromInvalid=true") {
				t.Fatalf("同版本配置恢复后应记录一次: %s", output.String())
			}
		})
	}
}

func newJob() *profilingv1alpha1.ProfilingJob {
	return namedJob("job", "job-uid", time.Date(2026, 10, 21, 11, 0, 0, 0, time.UTC))
}
func namedJob(name string, uid types.UID, created time.Time) *profilingv1alpha1.ProfilingJob {
	return &profilingv1alpha1.ProfilingJob{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, UID: uid, CreationTimestamp: metav1.NewTime(created)}, Spec: profilingv1alpha1.ProfilingJobSpec{
		TargetSelector: profilingv1alpha1.TargetSelector{MatchLabels: map[string]string{"app": "inference"}}, RunIndex: 1, Duration: metav1.Duration{Duration: 10 * time.Second}, ProfilingMode: profilingv1alpha1.ProfilingModeFull,
		PersistentVolumeClaimName: "storage", PersistentVolumeClaimNamespace: ns,
	}}
}
func newTargetPod(enabled bool) *corev1.Pod {
	labels := map[string]string{"app": "inference"}
	if enabled {
		labels[profilingv1alpha1.ProfilingEnableLabel] = "true"
	}
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "target", UID: "target-uid", Labels: labels}, Spec: corev1.PodSpec{NodeName: nodeName, Containers: []corev1.Container{{Name: "inference", Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")}}}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{runningContainer("inference", "containerd://gpu-container")}}}
}
func runningContainer(name, id string) corev1.ContainerStatus {
	return corev1.ContainerStatus{Name: name, ContainerID: id, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}
}
func newPVC() *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "storage"}, Spec: corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}}
}
func newConfigMap(image string) *corev1.ConfigMap {
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: collectorNS, Name: configName}, Data: map[string]string{nodeagent.ConfigKey: "image: " + image + "\nmaxRetries: 3\nretryBackoff: 1s\nmaxRetryBackoff: 5s\npendingTimeout: 30s\n"}}
}
func baseFixture(t *testing.T) *fixture {
	return newFixture(t, newJob(), newTargetPod(true), newPVC(), newConfigMap("collector:v1"))
}
func (f *fixture) reconcile() ctrl.Result { return f.reconcileJob("job") }
func (f *fixture) reconcileJob(name string) ctrl.Result {
	f.t.Helper()
	return f.reconcileKey(client.ObjectKey{Namespace: ns, Name: name})
}
func (f *fixture) reconcileKey(key client.ObjectKey) ctrl.Result {
	f.t.Helper()
	res, err := f.r.Reconcile(f.ctx, ctrl.Request{NamespacedName: key})
	if err != nil {
		f.t.Fatalf("reconcile %s: %v", key, err)
	}
	return res
}
func (f *fixture) namedJob(name string) *profilingv1alpha1.ProfilingJob {
	f.t.Helper()
	j := &profilingv1alpha1.ProfilingJob{}
	if err := f.client.Get(f.ctx, client.ObjectKey{Namespace: ns, Name: name}, j); err != nil {
		f.t.Fatal(err)
	}
	return j
}
func (f *fixture) job() *profilingv1alpha1.ProfilingJob { return f.namedJob("job") }
func (f *fixture) patchSpec(mutate func(*profilingv1alpha1.ProfilingJobSpec)) {
	j := f.job()
	mutate(&j.Spec)
	if err := f.client.Update(f.ctx, j); err != nil {
		f.t.Fatal(err)
	}
}
func (f *fixture) expectPhase(want profilingv1alpha1.ProfilingPhase) *profilingv1alpha1.ProfilingJob {
	f.t.Helper()
	j := f.job()
	if j.Status.Phase != want {
		f.t.Fatalf("phase=%s want=%s status=%+v", j.Status.Phase, want, j.Status)
	}
	return j
}
func (f *fixture) collectors() []corev1.Pod {
	f.t.Helper()
	pods := &corev1.PodList{}
	if err := f.client.List(f.ctx, pods, client.MatchingLabels{nodeagent.LabelComponent: nodeagent.ComponentCollector}); err != nil {
		f.t.Fatal(err)
	}
	return pods.Items
}
func (f *fixture) expectCleanup(reason string) *profilingv1alpha1.ProfilingJob {
	f.t.Helper()
	j := f.expectPhase(profilingv1alpha1.PhaseCanceled)
	_, msg, _ := strings.Cut(j.Status.Message, cleanupMessageSeparator)
	if messageCategory(msg) != reason || !strings.HasPrefix(j.Status.Message, "Canceled: ") {
		f.t.Fatalf("cleanup message=%q want reason=%s", j.Status.Message, reason)
	}
	return j
}
func (f *fixture) waitForCanceledCleanup() *profilingv1alpha1.ProfilingJob {
	f.t.Helper()
	for i := 0; i < 8; i++ {
		f.reconcile()
		if len(f.collectors()) == 0 && strings.Contains(f.job().Status.Message, cleanupMessageSeparator+"CollectorsRemoved: ") {
			j := f.expectCleanup("CollectorsRemoved")
			if len(f.collectors()) != 0 {
				f.t.Fatal("cleanup complete while collectors still exist")
			}
			return j
		}
	}
	f.t.Fatalf("canceled collectors not cleaned up: %+v", f.job().Status)
	return nil
}
func (f *fixture) restart() {
	f.t.Helper()
	f.r = &ProfilingJobReconciler{Client: f.r.Client, APIReader: f.r.APIReader, Scheme: f.r.Scheme, Recorder: f.r.Recorder, CollectorNamespace: collectorNS, ConfigMapName: configName, Now: func() time.Time { return f.now }}
}
func (f *fixture) start() {
	f.t.Helper()
	for i := 0; i < 8; i++ {
		f.reconcile()
		j := f.job()
		ready := len(j.Status.Targets) > 0
		for _, target := range j.Status.Targets {
			if target.Stage != profilingv1alpha1.StageCollecting {
				ready = false
			}
		}
		if ready {
			return
		}
	}
	f.t.Fatalf("collectors not started: %+v", f.job().Status)
}
func (f *fixture) collectorFor(index int) *corev1.Pod {
	f.t.Helper()
	j := f.job()
	p := &corev1.Pod{}
	if err := f.client.Get(f.ctx, client.ObjectKey{Namespace: j.Spec.PersistentVolumeClaimNamespace, Name: j.Status.Targets[index].CollectorPodName}, p); err != nil {
		f.t.Fatal(err)
	}
	return p
}
func (f *fixture) running(index int, startedAt time.Time) {
	f.t.Helper()
	p := f.collectorFor(index)
	p.Status.Phase = corev1.PodRunning
	p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "collector", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(startedAt)}}}}
	if err := f.client.Status().Update(f.ctx, p); err != nil {
		f.t.Fatal(err)
	}
}
func (f *fixture) complete(index int, res collectorprotocol.Result) {
	f.t.Helper()
	p := f.collectorFor(index)
	code := int32(0)
	var started metav1.Time
	for _, cs := range p.Status.ContainerStatuses {
		if cs.State.Running != nil {
			started = cs.State.Running.StartedAt
		}
	}
	p.Status.Phase = corev1.PodSucceeded
	if res.Phase != collectorprotocol.PhaseSucceeded {
		code = 1
		p.Status.Phase = corev1.PodFailed
	}
	p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "collector", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: code, Message: string(res.Marshal(4096)), StartedAt: started}}}}
	if err := f.client.Status().Update(f.ctx, p); err != nil {
		f.t.Fatal(err)
	}
}
func recordedEvents(rec *record.FakeRecorder) []string {
	var events []string
	for {
		select {
		case e := <-rec.Events:
			events = append(events, e)
		default:
			return events
		}
	}
}
func eventReasons(rec *record.FakeRecorder) map[string]int {
	reasons := map[string]int{}
	for _, e := range recordedEvents(rec) {
		if fields := strings.Fields(e); len(fields) > 1 {
			reasons[fields[1]]++
		}
	}
	return reasons
}
func timeEqual(got *metav1.Time, want time.Time) bool { return got != nil && got.Time.Equal(want) }
func successResult() collectorprotocol.Result {
	return collectorprotocol.Result{Phase: collectorprotocol.PhaseSucceeded, Containers: []collectorprotocol.ContainerResult{{
		ContainerID: "gpu-container", Phase: collectorprotocol.PhaseSucceeded, ElapsedMillis: 60000,
		OutputFiles: []string{"inference_target_gpu-container_lastkernels.log", "inference_target_gpu-container_kerneltime.log"}, Bytes: 100,
	}}}
}

func TestHistoryKeepsLatestFiveRounds(t *testing.T) {
	f := baseFixture(t)
	job := newJob()
	for index := int64(0); index < 12; index++ {
		job.Spec.RunIndex = index
		f.r.startNewRound(job)
		if len(job.Status.History) != int(min(index, 5)) {
			t.Fatalf("round %d: history count=%d want=%d", index, len(job.Status.History), min(index, 5))
		}
		for i, entry := range job.Status.History {
			wantIndex := max(index-5, 0) + int64(i)
			if entry.RunIndex != wantIndex || entry.Phase != profilingv1alpha1.PhaseCompleted {
				t.Fatalf("round %d: history[%d]=%+v want completed round %d", index, i, entry, wantIndex)
			}
		}
		job.Status.Phase = profilingv1alpha1.PhaseCompleted
	}
}

func TestLifecycleAndDeferredRunIndex(t *testing.T) {
	f := baseFixture(t)
	f.start()
	f.running(0, f.now)
	first := f.job().Status.Targets[0].CollectorPodName
	frozen := f.job().Status.RunConfig.DeepCopy()
	f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) { s.RunIndex = 2; s.Duration.Duration = 12 * time.Second })
	f.reconcile()
	active := f.job().Status
	if lastRunIndex(&active) != 1 || active.Targets[0].CollectorPodName != first || !reflect.DeepEqual(active.RunConfig, frozen) || len(f.collectors()) != 1 {
		t.Fatal("active round superseded or frozen config changed")
	}
	f.now = f.now.Add(time.Minute)
	f.complete(0, successResult())
	result := f.reconcile()
	j := f.expectPhase(profilingv1alpha1.PhaseCompleted)
	wantFiles := []string{
		"/inference/job/run-1/inference_target_gpu-container_lastkernels.log",
		"/inference/job/run-1/inference_target_gpu-container_kerneltime.log",
	}
	if j.Status.CollectedDuration != 60000 || j.Status.OutputLocation != "/inference/job/run-1" || result.RequeueAfter != time.Millisecond || !reflect.DeepEqual(j.Status.Targets[0].OutputFiles, wantFiles) {
		t.Fatalf("result %+v status %+v", result, j.Status)
	}
	for i := 0; i < 10 && lastRunIndex(&f.job().Status) != 2; i++ {
		f.reconcile()
	}
	j = f.job()
	if lastRunIndex(&j.Status) != 2 || len(j.Status.History) != 1 || j.Status.History[0].RunIndex != 1 || j.Status.History[0].Phase != profilingv1alpha1.PhaseCompleted ||
		j.Status.History[0].CollectedDuration != 60000 || j.Status.History[0].OutputLocation != "/inference/job/run-1" ||
		j.Status.StartTime != nil || j.Status.OutputLocation != "" || j.Status.CollectedDuration != 0 {
		t.Fatalf("rollover %+v", j.Status)
	}
	if j.Status.RunConfig.Duration.Duration != 12*time.Second {
		t.Fatal("new round did not use new duration")
	}
}

func TestCollectorReportedFilesAcrossContainers(t *testing.T) {
	f := baseFixture(t)
	f.start()
	f.running(0, f.now)
	f.reconcile()
	f.now = f.now.Add(time.Minute)
	f.complete(0, collectorprotocol.Result{Phase: collectorprotocol.PhaseSucceeded, Containers: []collectorprotocol.ContainerResult{
		{ContainerID: "gpu-container", Phase: collectorprotocol.PhaseSucceeded, ElapsedMillis: 60000, OutputFiles: []string{"daemon-lastkernels.log", "daemon-kerneltime.log"}, Bytes: 100},
		{ContainerID: "empty-container", Phase: collectorprotocol.PhaseSucceeded, OutputFiles: []string{"daemon-empty.log"}, Bytes: 0},
	}})
	f.reconcile()
	j := f.expectPhase(profilingv1alpha1.PhaseCompleted)
	wantFiles := []string{
		"/inference/job/run-1/daemon-lastkernels.log",
		"/inference/job/run-1/daemon-kerneltime.log",
		"/inference/job/run-1/daemon-empty.log",
	}
	if !reflect.DeepEqual(j.Status.Targets[0].OutputFiles, wantFiles) {
		t.Fatalf("actual report files must all be retained without filename prediction: %+v", j.Status.Targets[0])
	}
	f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) { s.RunIndex = 2 })
	f.start()
	j = f.job()
	// History stores the round directory, not a per-file or per-target manifest.
	if len(j.Status.History) != 1 || j.Status.History[0].OutputLocation != "/inference/job/run-1" || j.Status.History[0].CollectedDuration != 60000 {
		t.Fatalf("multi-file round history lost: %+v", j.Status.History)
	}
}

func TestLatestDeferredRoundSurvivesRestart(t *testing.T) {
	f := baseFixture(t)
	f.start()
	first := f.collectorFor(0).Name
	frozen := f.job().Status.RunConfig.DeepCopy()
	restart := func() {
		f.r = &ProfilingJobReconciler{Client: f.client, APIReader: f.client, Scheme: f.r.Scheme, CollectorNamespace: collectorNS, ConfigMapName: configName, Now: func() time.Time { return f.now }}
	}
	assertFrozen := func() {
		t.Helper()
		j := f.job()
		if lastRunIndex(&j.Status) != 1 || j.Status.Targets[0].CollectorPodName != first || !reflect.DeepEqual(j.Status.RunConfig, frozen) || len(f.collectors()) != 1 {
			t.Fatalf("deferred update or restart changed active round: %+v", j.Status)
		}
	}
	f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) { s.RunIndex = 2; s.Duration.Duration = 12 * time.Second })
	f.reconcile()
	assertFrozen()
	restart()
	f.now = f.now.Add(-time.Hour)
	f.reconcile()
	assertFrozen()
	f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) {
		s.RunIndex = 3
		s.Duration.Duration = 45 * time.Second
		s.ProfilingMode = profilingv1alpha1.ProfilingModeLight
		s.ProfilingRules = "latest-rules"
	})
	if err := f.client.Update(f.ctx, newConfigMap("collector:v3")); err != nil {
		t.Fatal(err)
	}
	restart()
	f.reconcile()
	assertFrozen()
	f.complete(0, successResult())
	f.reconcile()
	f.expectPhase(profilingv1alpha1.PhaseCompleted)
	restart()
	f.start()
	j := f.job()
	if lastRunIndex(&j.Status) != 3 || len(j.Status.History) != 1 || j.Status.History[0].RunIndex != 1 || j.Status.History[0].Phase != profilingv1alpha1.PhaseCompleted {
		t.Fatalf("restart did not skip superseded round 2: %+v", j.Status)
	}
	p := f.collectorFor(0)
	if j.Status.RunConfig.Duration.Duration != 45*time.Second || j.Status.RunConfig.ProfilingMode != profilingv1alpha1.ProfilingModeLight || j.Status.RunConfig.ProfilingRules != "latest-rules" || p.Annotations[nodeagent.AnnotationRunIndex] != "3" || p.Spec.Containers[0].Image != "collector:v3" || p.Name == first || len(f.collectors()) != 1 {
		t.Fatalf("latest round did not use latest inputs: status=%+v collector=%+v", j.Status, p)
	}
}

func TestCancelBarrierAndFullDurationResume(t *testing.T) {
	for _, tc := range []struct {
		name          string
		newRound      bool
		crossNSHidden bool
	}{
		{"resume", false, false},
		{"latest new round", true, false},
		{"cross namespace hidden collector resume", false, true},
		{"cross namespace hidden collector new round", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job, pvc := newJob(), newPVC()
			if tc.crossNSHidden {
				job.Spec.PersistentVolumeClaimNamespace, pvc.Namespace = "storage-ns", "storage-ns"
			}
			f := newFixture(t, job, pvc, newTargetPod(true), newConfigMap("collector:v1"))
			if tc.crossNSHidden {
				f.r.Client = collectorBlindCache{f.client}
			}
			f.start()
			f.running(0, f.now)
			f.reconcile()
			old := f.collectorFor(0)
			if old.Spec.ActiveDeadlineSeconds != nil {
				t.Fatal("collector must not have a Pod deadline")
			}
			before := f.job().Status.DeepCopy()
			frozen := before.RunConfig.DeepCopy()
			if !timeEqual(before.StartTime, f.now) {
				t.Fatalf("运行中的 collector 未设置 startTime：%+v", before)
			}
			old.Finalizers = []string{"test/hold"}
			if err := f.client.Update(f.ctx, old); err != nil {
				t.Fatal(err)
			}
			f.now = f.now.Add(20 * time.Second)
			f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) { s.Cancel = true })
			if res := f.reconcile(); res.RequeueAfter != time.Millisecond {
				t.Fatalf("first cancel requeue=%+v", res)
			}
			j := f.expectCleanup("CleanupPending")
			ended := j.Status.EndTime.DeepCopy()
			if ended == nil || !ended.Time.Equal(f.now) || j.Status.CollectedDuration != 20000 || j.Status.Targets[0].Stage != profilingv1alpha1.StageCanceled || j.Status.Targets[0].Reason != "Canceled" {
				t.Fatalf("cancel did not finish immediately: %+v", j.Status)
			}
			if err := f.client.Get(f.ctx, client.ObjectKeyFromObject(old), old); err != nil || !old.DeletionTimestamp.IsZero() {
				t.Fatalf("first cancel deleted collector before persisting Canceled: pod=%+v error=%v", old, err)
			}
			f.restart()
			if res := f.reconcile(); res.RequeueAfter != pollInterval {
				t.Fatalf("deleting collector requeue=%+v", res)
			}
			if err := f.client.Get(f.ctx, client.ObjectKeyFromObject(old), old); err != nil || old.DeletionTimestamp.IsZero() {
				t.Fatalf("collector deletion not requested: %v", err)
			}
			f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) {
				s.Cancel = false
				s.Duration.Duration = 45 * time.Second
				s.ProfilingMode = profilingv1alpha1.ProfilingModeLight
				s.ProfilingRules = "latest-rules"
				if tc.newRound {
					s.RunIndex = 2
				}
			})
			if err := f.client.Update(f.ctx, newConfigMap("collector:v3")); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 3; i++ {
				if tc.newRound && i == 1 {
					f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) { s.RunIndex = 3 })
				}
				f.now = f.now.Add(time.Minute)
				f.restart()
				if res := f.reconcile(); res.RequeueAfter != pollInterval {
					t.Fatalf("cleanup barrier requeue=%+v", res)
				}
				j = f.expectCleanup("CleanupPending")
				pods := f.collectors()
				if lastRunIndex(&j.Status) != 1 || len(j.Status.History) != 0 || !reflect.DeepEqual(j.Status.RunConfig, frozen) || !reflect.DeepEqual(j.Status.EndTime, ended) || j.Status.Targets[0].Stage != profilingv1alpha1.StageCanceled || len(pods) != 1 || pods[0].Name != old.Name {
					t.Fatalf("resume/new round started or cancellation changed during cleanup: %+v pods=%+v", j.Status, pods)
				}
			}
			old.Finalizers = nil
			if err := f.client.Update(f.ctx, old); err != nil {
				t.Fatal(err)
			}
			f.restart()
			if res := f.reconcile(); res.RequeueAfter != time.Millisecond {
				t.Fatalf("empty cleanup did not advance immediately: %+v", res)
			}
			j = f.expectPhase(profilingv1alpha1.PhasePending)
			if len(f.collectors()) != 0 || len(j.Status.Targets) != 0 || j.Status.EndTime != nil || j.Status.StartTime != nil || j.Status.OutputLocation != "" || j.Status.CollectedDuration != 0 || j.Status.Message != "" {
				t.Fatalf("pending collection retained cleanup state or created collector: %+v", j.Status)
			}
			if tc.newRound {
				if lastRunIndex(&j.Status) != 3 || j.Status.Message != "" || len(j.Status.History) != 1 || j.Status.History[0].RunIndex != 1 || j.Status.History[0].Phase != profilingv1alpha1.PhaseCanceled || !reflect.DeepEqual(j.Status.History[0].EndTime, ended) || j.Status.History[0].CollectedDuration != 20000 {
					t.Fatalf("new round did not preserve canceled history or skip round 2: %+v", j.Status)
				}
			} else if lastRunIndex(&j.Status) != 1 || len(j.Status.History) != 0 || !reflect.DeepEqual(j.Status.RunConfig, frozen) || j.Status.StartTime != nil {
				t.Fatalf("resume did not retain round identity: %+v", j.Status)
			}
			f.restart()
			f.start()
			if j = f.job(); j.Status.StartTime != nil || j.Status.OutputLocation != "" {
				t.Fatalf("尚未运行的 collector 设置了 startTime：%+v", j.Status)
			}
			f.running(0, f.now)
			f.reconcile()
			j = f.job()
			pod := f.collectorFor(0)
			args := strings.Join(pod.Spec.Containers[0].Args, " ")
			if (pod.Name != old.Name) != tc.newRound || len(f.collectors()) != 1 || pod.Namespace != pvc.Namespace || j.Status.Targets[0].Stage != profilingv1alpha1.StageCollecting || !timeEqual(j.Status.StartTime, f.now) {
				t.Fatalf("restart did not start a fresh full-duration collector: status=%+v pod=%+v", j.Status, pod)
			}
			if tc.newRound {
				if !strings.Contains(args, "--duration=45s") || !strings.Contains(args, "--mode=light") || pod.Spec.Containers[0].Image != "collector:v3" || pod.Annotations[nodeagent.AnnotationRunIndex] != "3" ||
					!timeEqual(j.Status.StartTime, f.now) || j.Status.OutputLocation != "/inference/job/run-3" {
					t.Fatalf("new round ignored latest inputs: %s annotations=%v status=%+v", args, pod.Annotations, j.Status)
				}
			} else if !reflect.DeepEqual(j.Status.RunConfig, frozen) || !strings.Contains(args, "--duration=10s") || !strings.Contains(args, "--mode=full") || pod.Spec.Containers[0].Image != "collector:v3" || pod.Spec.ActiveDeadlineSeconds != nil ||
				!timeEqual(j.Status.StartTime, f.now) || j.Status.OutputLocation != "/inference/job/run-1" {
				t.Fatalf("resume did not restart frozen full duration: status=%+v args=%s", j.Status, args)
			}
		})
	}
}

func TestRepeatedResumeReusesCollectorNameWithFreshUID(t *testing.T) {
	f := baseFixture(t)
	var name string
	for execution := 0; execution < 3; execution++ {
		f.start()
		pod := f.collectorFor(0)
		if execution == 0 {
			name = pod.Name
		}
		j := f.job()
		if pod.Name != name || len(f.collectors()) != 1 || j.Status.Targets[0].RetryCount != 0 || j.Status.Targets[0].CollectorPodUID != "" {
			t.Fatalf("resume reused execution state: status=%+v pod=%+v", j.Status, pod)
		}
		pod.UID = types.UID(fmt.Sprintf("collector-%d", execution))
		if err := f.client.Update(f.ctx, pod); err != nil {
			t.Fatal(err)
		}
		j.Status.Targets[0].CollectorPodUID = string(pod.UID)
		if err := f.client.Status().Update(f.ctx, j); err != nil {
			t.Fatal(err)
		}
		f.running(0, f.now)
		f.reconcile()
		f.now = f.now.Add(5 * time.Second)
		if execution == 2 {
			f.complete(0, successResult())
			f.reconcile()
			j = f.expectPhase(profilingv1alpha1.PhaseCompleted)
			if j.Status.CollectedDuration != 5000 || len(j.Status.History) != 0 || lastRunIndex(&j.Status) != 1 {
				t.Fatalf("latest execution result changed round identity: %+v", j.Status)
			}
			break
		}
		f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) { s.Cancel = true })
		f.waitForCanceledCleanup()
		if len(f.collectors()) != 0 {
			t.Fatal("canceled collector survived cleanup")
		}
		f.now = f.now.Add(time.Minute)
		f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) { s.Cancel = false })
		f.restart()
		f.reconcile()
		j = f.expectPhase(profilingv1alpha1.PhasePending)
		if j.Status.Targets != nil || j.Status.StartTime != nil || j.Status.EndTime != nil || j.Status.CollectedDuration != 0 || len(j.Status.History) != 0 {
			t.Fatalf("resume retained previous results: %+v", j.Status)
		}
		f.restart()
	}
}

func TestQueuedCollectorsUseLatestConfigInSameRound(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprintf("restart=%t", restart), func(t *testing.T) {
			peer := newTargetPod(true)
			peer.Name, peer.UID, peer.Spec.NodeName = "target-2", "target-2-uid", "node-2"
			f := newFixture(t, newJob(), newTargetPod(true), peer, newPVC(), newConfigMap("collector:v1"))
			f.reconcile() // Freeze run inputs.
			f.reconcile() // Select targets.
			f.reconcile() // Persist deterministic names, before any Pod creation.
			j := f.job()
			if len(j.Status.Targets) != 2 || len(f.collectors()) != 0 {
				t.Fatalf("test must update config between selection and creation: %+v", j.Status)
			}
			for _, target := range j.Status.Targets {
				if target.Stage != profilingv1alpha1.StageQueued || target.CollectorPodName == "" {
					t.Fatalf("target not queued with persisted identity: %+v", target)
				}
			}
			frozen := j.Status.RunConfig.DeepCopy()
			f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) {
				s.Duration.Duration = 45 * time.Second
				s.ProfilingMode = profilingv1alpha1.ProfilingModeLight
				s.ProfilingRules = "not-this-round"
			})
			cm := newConfigMap("collector:v2")
			cm.Data[nodeagent.ConfigKey] = "image: collector:v2\npollInterval: 7s\nmaxRetries: 1\nretryBackoff: 3s\nmaxRetryBackoff: 9s\n"
			if err := f.client.Update(f.ctx, cm); err != nil {
				t.Fatal(err)
			}
			cfg, err := nodeagent.ParseConfig(cm.Data[nodeagent.ConfigKey])
			if err != nil {
				t.Fatal(err)
			}
			if restart {
				f.restart()
			}
			f.start()
			j = f.job()
			if len(f.collectors()) != 2 || lastRunIndex(&j.Status) != 1 || !reflect.DeepEqual(j.Status.RunConfig, frozen) || len(j.Status.History) != 0 {
				t.Fatalf("config update changed same-round identity or frozen inputs: %+v", j.Status)
			}
			for i := range j.Status.Targets {
				pod := f.collectorFor(i)
				want := nodeagent.CollectorPod(cfg, j, &j.Status.Targets[i])
				args := strings.Join(pod.Spec.Containers[0].Args, " ")
				if want == nil || !reflect.DeepEqual(pod.Spec, want.Spec) || pod.Spec.Containers[0].Image != "collector:v2" {
					t.Fatalf("queued target %d did not use latest config: pod=%+v want=%+v", i, pod.Spec, want)
				}
				for _, arg := range []string{"--duration=10s", "--mode=full"} {
					if !strings.Contains(args, arg) {
						t.Fatalf("target %d missing frozen input %s: %s", i, arg, args)
					}
				}
				if strings.Contains(args, "--poll-interval") || strings.Contains(args, "--max-retries") {
					t.Fatalf("target %d must not receive timeout or retry arguments: %s", i, args)
				}
			}
		})
	}
}

func TestConfigUpdatePreservesExistingCollectorAndQueuedPeer(t *testing.T) {
	for _, tc := range []struct {
		name                             string
		restart, recoverQueued, overflow bool
	}{
		{name: "running"},
		{name: "running after restart", restart: true},
		{name: "created before status persisted", restart: true, recoverQueued: true},
		{name: "new duration overflow", restart: true, overflow: true},
		{name: "recover existing despite new duration overflow", restart: true, recoverQueued: true, overflow: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peer := newTargetPod(true)
			peer.Name, peer.UID, peer.Spec.NodeName = "target-2", "target-2-uid", "node-2"
			f := newFixture(t, newJob(), newTargetPod(true), peer, newPVC(), newConfigMap("collector:v1"))
			f.reconcile()
			f.reconcile()
			peer.Status.Phase = corev1.PodPending
			if err := f.client.Status().Update(f.ctx, peer); err != nil {
				t.Fatal(err)
			}
			f.reconcile()
			f.reconcile()
			old := f.collectorFor(0)
			// The fake API does not assign UIDs; record one to detect replacement.
			old.UID = "stable-collector-uid"
			if err := f.client.Update(f.ctx, old); err != nil {
				t.Fatal(err)
			}
			j := f.job()
			j.Status.Targets[0].CollectorPodUID = string(old.UID)
			if err := f.client.Status().Update(f.ctx, j); err != nil {
				t.Fatal(err)
			}
			f.running(0, f.now)
			f.reconcile()
			old = f.collectorFor(0)
			j = f.expectPhase(profilingv1alpha1.PhaseRunning)
			frozen := j.Status.RunConfig.DeepCopy()
			if j.Status.Targets[1].Stage != profilingv1alpha1.StageQueued || len(f.collectors()) != 1 {
				t.Fatalf("test must retain a queued peer beside a running collector: %+v", j.Status)
			}
			if tc.recoverQueued {
				j.Status.Targets[0].Stage = profilingv1alpha1.StageQueued
				j.Status.Targets[0].CollectorPodUID = ""
				if err := f.client.Status().Update(f.ctx, j); err != nil {
					t.Fatal(err)
				}
			}
			cm := newConfigMap("collector:v2")
			cm.Data[nodeagent.ConfigKey] = "image: collector:v2\npollInterval: 7s\nmaxRetries: 1\nretryBackoff: 3s\nmaxRetryBackoff: 9s\n"
			if tc.overflow {
				cm.Data[nodeagent.ConfigKey] += "finishTimeout: " + (time.Duration(math.MaxInt64) - 5*time.Second).String() + "\n"
			}
			cfg, err := nodeagent.ParseConfig(cm.Data[nodeagent.ConfigKey])
			if err != nil {
				t.Fatal(err)
			}
			if err := f.client.Update(f.ctx, cm); err != nil {
				t.Fatal(err)
			}
			if tc.overflow {
				peer.Status.Phase = corev1.PodRunning
				if err := f.client.Status().Update(f.ctx, peer); err != nil {
					t.Fatal(err)
				}
			}
			for i := 0; i < 3; i++ {
				if tc.restart {
					f.restart()
				}
				f.reconcile()
				j = f.expectPhase(profilingv1alpha1.PhaseRunning)
				stable := f.collectorFor(0)
				if len(f.collectors()) != 1 || stable.UID != old.UID || stable.Name != old.Name || stable.ResourceVersion != old.ResourceVersion || !reflect.DeepEqual(stable.Spec, old.Spec) || j.Status.Targets[0].CollectorPodUID != string(old.UID) || j.Status.Targets[0].Stage != profilingv1alpha1.StageCollecting || !reflect.DeepEqual(j.Status.RunConfig, frozen) || f.r.lastGood == nil || f.r.lastGood.Image != "collector:v2" {
					t.Fatalf("config update rebuilt, rewrote or failed existing collector: status=%+v old=%+v current=%+v", j.Status, old, stable)
				}
			}
			if tc.overflow {
				if cfg.ValidateDuration(frozen.Duration.Duration) == nil || j.Status.Targets[1].Stage != profilingv1alpha1.StageFailed || j.Status.Targets[1].Reason != "InvalidDuration" {
					t.Fatalf("unsafe duration must fail only the target needing a new collector: %+v", j.Status)
				}
				f.complete(0, successResult())
				f.reconcile()
				j = f.expectPhase(profilingv1alpha1.PhaseFailed)
				if j.Status.Targets[0].Stage != profilingv1alpha1.StageCompleted || len(j.Status.Targets[0].OutputFiles) == 0 {
					t.Fatalf("existing collector could not finish after config overflow: %+v", j.Status)
				}
				return
			}
			peer.Status.Phase = corev1.PodRunning
			if err := f.client.Status().Update(f.ctx, peer); err != nil {
				t.Fatal(err)
			}
			f.start()
			j = f.job()
			fresh := f.collectorFor(1)
			want := nodeagent.CollectorPod(cfg, j, &j.Status.Targets[1])
			if len(f.collectors()) != 2 || fresh.Spec.Containers[0].Image != "collector:v2" || want == nil || !reflect.DeepEqual(fresh.Spec, want.Spec) || !reflect.DeepEqual(f.collectorFor(0), old) || !reflect.DeepEqual(j.Status.RunConfig, frozen) {
				t.Fatalf("queued peer ignored latest config or disturbed existing collector: %+v", j.Status)
			}
		})
	}
}

func TestResumeIgnoresSpecChangesUntilRunIndexIncreases(t *testing.T) {
	f := baseFixture(t)
	f.start()
	cancel := func() {
		f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) { s.Cancel = true })
		f.waitForCanceledCleanup()
	}
	cancel()
	f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) {
		s.Cancel = false
		s.Duration.Duration = 45 * time.Second
		s.ProfilingMode = profilingv1alpha1.ProfilingModeLight
		s.ProfilingRules = "rules-v2"
	})
	if err := f.client.Update(f.ctx, newConfigMap("collector:v2")); err != nil {
		t.Fatal(err)
	}
	f.start()
	j := f.job()
	run := j.Status.RunConfig
	args := strings.Join(f.collectorFor(0).Spec.Containers[0].Args, " ")
	if lastRunIndex(&j.Status) != 1 || run.Duration.Duration != 10*time.Second ||
		run.ProfilingMode != profilingv1alpha1.ProfilingModeFull || run.ProfilingRules != "" ||
		!strings.Contains(args, "--duration=10s") || !strings.Contains(args, "--mode=full") ||
		f.collectorFor(0).Spec.Containers[0].Image != "collector:v2" {
		t.Fatalf("resume must retain run inputs but use latest collector config: status %+v args %s", j.Status, args)
	}

	cancel()
	f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) { s.Cancel = false; s.RunIndex = 2 })
	f.start()
	j = f.job()
	run = j.Status.RunConfig
	args = strings.Join(f.collectorFor(0).Spec.Containers[0].Args, " ")
	if lastRunIndex(&j.Status) != 2 || run.Duration.Duration != 45*time.Second ||
		run.ProfilingMode != profilingv1alpha1.ProfilingModeLight || run.ProfilingRules != "rules-v2" ||
		!strings.Contains(args, "--duration=45s") || !strings.Contains(args, "--mode=light") ||
		f.collectorFor(0).Spec.Containers[0].Image != "collector:v2" {
		t.Fatalf("new round ignored latest spec: status %+v args %s", j.Status, args)
	}
}

func TestCancelPrecedesRunIndex(t *testing.T) {
	f := baseFixture(t)
	f.start()
	f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) { s.Cancel = true; s.RunIndex = 2 })
	f.reconcile()
	j := f.expectCleanup("CleanupPending")
	if lastRunIndex(&j.Status) != 1 || len(f.collectors()) != 1 || j.Status.Targets[0].Stage != profilingv1alpha1.StageCanceled {
		t.Fatal("first cancel consumed queued index or deleted collector")
	}
	j = f.waitForCanceledCleanup()
	if lastRunIndex(&j.Status) != 1 || len(j.Status.History) != 0 {
		t.Fatal("canceled run consumed queued index")
	}
	f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) { s.Cancel = false })
	f.start()
	j = f.job()
	if lastRunIndex(&j.Status) != 2 {
		t.Fatalf("new round %+v", j.Status)
	}
}

func TestRunIndexZeroIsFirstRound(t *testing.T) {
	j := newJob()
	j.Spec.RunIndex = 0
	f := newFixture(t, j, newTargetPod(true), newPVC(), newConfigMap("collector:v1"))
	f.start()
	got := f.job()
	if got.Status.LastRunIndex == nil || *got.Status.LastRunIndex != 0 || got.Status.RunConfig == nil {
		t.Fatalf("round 0 not started: %+v", got.Status)
	}
	if c := f.collectorFor(0); c.Annotations[nodeagent.AnnotationRunIndex] != "0" || !strings.Contains(strings.Join(c.Spec.Containers[0].Args, " "), "--output-dir=/data/inference/job/run-0") {
		t.Fatalf("collector not bound to round 0: %v %v", c.Annotations, c.Spec.Containers[0].Args)
	}
	f.complete(0, successResult())
	f.reconcile()
	f.expectPhase(profilingv1alpha1.PhaseCompleted)
	f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) { s.RunIndex = 1 })
	f.start()
	got = f.job()
	if lastRunIndex(&got.Status) != 1 || len(got.Status.History) != 1 || got.Status.History[0].RunIndex != 0 || got.Status.History[0].Phase != profilingv1alpha1.PhaseCompleted {
		t.Fatalf("round 0 not recorded before round 1: %+v", got.Status)
	}
}

func TestInitiallyCanceledDoesNotConsumeRunIndex(t *testing.T) {
	j := newJob()
	j.Spec.Cancel = true
	j.Spec.RunIndex = 0
	f := newFixture(t, j, newTargetPod(true), newPVC(), newConfigMap("collector:v1"))
	if res := f.reconcile(); res.RequeueAfter != time.Millisecond {
		t.Fatalf("initial cancel requeue=%+v", res)
	}
	got := f.expectCleanup("CleanupPending")
	ended := got.Status.EndTime.DeepCopy()
	if ended == nil || got.Status.LastRunIndex != nil || got.Status.RunConfig != nil || len(got.Status.Targets) != 0 || len(f.collectors()) != 0 {
		t.Fatalf("initial cancel consumed round 0: %+v", got.Status)
	}
	f.now = f.now.Add(time.Minute)
	f.restart()
	got = f.waitForCanceledCleanup()
	for i := 0; i < 2; i++ {
		f.now = f.now.Add(time.Minute)
		f.reconcile()
	}
	got = f.expectCleanup("CollectorsRemoved")
	if got.Status.LastRunIndex != nil || got.Status.RunConfig != nil || len(got.Status.History) != 0 || !reflect.DeepEqual(got.Status.EndTime, ended) || len(f.collectors()) != 0 {
		t.Fatalf("initial cancel triggered collection or finished twice: %+v", got.Status)
	}
	f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) { s.Cancel = false })
	f.start()
	if got := f.job().Status; got.LastRunIndex == nil || *got.LastRunIndex != 0 || len(got.History) != 0 {
		t.Fatalf("uncancel did not start round 0: %+v", got)
	}
}

func TestCancelFinishesOnceAndPreservesTerminalTargets(t *testing.T) {
	for _, stage := range []profilingv1alpha1.TargetStage{
		profilingv1alpha1.StageQueued, profilingv1alpha1.StageCollecting, profilingv1alpha1.StageRetrying,
		profilingv1alpha1.StageCompleted, profilingv1alpha1.StageFailed, profilingv1alpha1.StageCanceled,
	} {
		t.Run(string(stage), func(t *testing.T) {
			job := newJob()
			job.Spec.Cancel = true
			job.Generation = 7
			job.Status = profilingv1alpha1.ProfilingJobStatus{
				Phase: profilingv1alpha1.PhaseRunning, LastRunIndex: ptr.To(int64(1)),
				RunConfig: &profilingv1alpha1.RunConfiguration{Duration: job.Spec.Duration, ProfilingMode: job.Spec.ProfilingMode},
				Targets: []profilingv1alpha1.TargetStatus{{
					TargetNamespace: ns, TargetName: "target", TargetUID: "target-uid", Stage: stage,
					CollectorPodName: "old-collector", RetryCount: 1, Reason: "PreviousReason", Message: "previous result",
					OutputFiles: []string{"previous/output.json"}, CollectedDuration: 12345,
				}},
			}
			for i, peerStage := range []profilingv1alpha1.TargetStage{
				profilingv1alpha1.StageCompleted, profilingv1alpha1.StageFailed, profilingv1alpha1.StageQueued,
				profilingv1alpha1.StageCollecting, profilingv1alpha1.StageRetrying,
			} {
				job.Status.Targets = append(job.Status.Targets, profilingv1alpha1.TargetStatus{
					TargetNamespace: ns, TargetName: fmt.Sprintf("peer-%d", i), TargetUID: fmt.Sprintf("peer-%d-uid", i), Stage: peerStage,
					Reason: "PeerReason", Message: "peer result", RetryCount: 2,
					OutputFiles: []string{fmt.Sprintf("peer-%d/output.json", i)}, CollectedDuration: 23456,
				})
			}
			if stage == profilingv1alpha1.StageQueued {
				job.Status.Phase = profilingv1alpha1.PhasePending
			}
			before := job.Status.DeepCopy()
			f := newFixture(t, job)
			recorder := record.NewFakeRecorder(10)
			f.r.Recorder = recorder
			if res := f.reconcile(); res.RequeueAfter != time.Millisecond {
				t.Fatalf("cancel requeue=%+v", res)
			}
			j := f.expectCleanup("CleanupPending")
			wantTargets := before.Targets
			for i := range wantTargets {
				if !wantTargets[i].Stage.IsTerminal() {
					wantTargets[i].Stage, wantTargets[i].Reason, wantTargets[i].Message = profilingv1alpha1.StageCanceled, "Canceled", "canceled by spec.cancel"
				}
			}
			if !reflect.DeepEqual(j.Status.Targets, wantTargets) || j.Status.EndTime == nil || !j.Status.EndTime.Time.Equal(f.now) || messageCategory(j.Status.Message) != "Canceled" {
				t.Fatalf("cancel changed terminal result or failed to mark targets: %+v", j.Status)
			}
			ended := j.Status.EndTime.DeepCopy()
			select {
			case event := <-recorder.Events:
				if !strings.HasPrefix(event, "Normal Canceled ") {
					t.Fatalf("unexpected finish event: %s", event)
				}
			default:
				t.Fatal("cancel did not emit its finish event")
			}
			for i := 0; i < 3; i++ {
				f.now = f.now.Add(time.Minute)
				f.restart()
				res := f.reconcile()
				j = f.expectCleanup("CollectorsRemoved")
				if res != (ctrl.Result{}) || !reflect.DeepEqual(j.Status.EndTime, ended) || !reflect.DeepEqual(j.Status.RunConfig, before.RunConfig) || !reflect.DeepEqual(j.Status.Targets, wantTargets) || lastRunIndex(&j.Status) != 1 || len(j.Status.History) != 0 {
					t.Fatalf("cleanup finished canceled round more than once: %+v result=%+v", j.Status, res)
				}
			}
			if reasons := eventReasons(recorder); !reflect.DeepEqual(reasons, map[string]int{"CollectorsRemoved": 1}) {
				t.Fatalf("cleanup events=%v, want one CollectorsRemoved and no second finish", reasons)
			}
		})
	}
}

func TestCancelDoesNotOverwriteCompletedOrFailedRound(t *testing.T) {
	for _, phase := range []profilingv1alpha1.ProfilingPhase{profilingv1alpha1.PhaseCompleted, profilingv1alpha1.PhaseFailed} {
		t.Run(string(phase), func(t *testing.T) {
			f := baseFixture(t)
			f.start()
			result := successResult()
			if phase == profilingv1alpha1.PhaseFailed {
				result = collectorprotocol.Failed(collectorprotocol.ReasonInvalidArgs, "invalid mode")
			}
			f.complete(0, result)
			for i := 0; i < 8 && f.job().Status.Phase != phase; i++ {
				f.reconcile()
			}
			before := f.expectPhase(phase).Status.DeepCopy()
			f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) { s.Cancel = true; s.RunIndex = 3 })
			for i := 0; i < 4; i++ {
				f.now = f.now.Add(time.Minute)
				f.restart()
				f.reconcile()
				got := f.expectPhase(phase).Status.DeepCopy()
				got.Message, _, _ = strings.Cut(got.Message, cleanupMessageSeparator)
				if !reflect.DeepEqual(got, before) {
					t.Fatalf("cancel overwrote finished result: before=%+v after=%+v", before, got)
				}
			}
			if len(f.collectors()) != 0 {
				t.Fatal("cancel failed to clean completed/failed collector")
			}
			f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) { s.Cancel = false })
			f.start()
			j := f.job()
			if lastRunIndex(&j.Status) != 3 || len(j.Status.History) != 1 || j.Status.History[0].Phase != phase || !reflect.DeepEqual(j.Status.History[0].EndTime, before.EndTime) || j.Status.History[0].OutputLocation != before.OutputLocation || j.Status.History[0].CollectedDuration != before.CollectedDuration {
				t.Fatalf("queued round lost previous terminal result: %+v", j.Status)
			}
		})
	}
}

func TestJobMessagesAndEventsAcrossPreflightRecovery(t *testing.T) {
	f := newFixture(t, newJob(), newTargetPod(true))
	recorder := record.NewFakeRecorder(100)
	f.r.Recorder = recorder
	f.reconcile()
	f.reconcile()
	if got := f.expectPhase(profilingv1alpha1.PhasePending).Status.Message; !strings.HasPrefix(got, "PVCNotReady: ") {
		t.Fatalf("缺少 PVC 等待诊断：%q", got)
	}
	for i := 0; i < 3; i++ {
		f.restart()
		f.reconcile()
	}
	if n := eventReasons(recorder)["PVCNotReady"]; n != 1 {
		t.Fatalf("PVC 等待事件重复：%d", n)
	}
	if err := f.client.Create(f.ctx, newPVC()); err != nil {
		t.Fatal(err)
	}
	cm := newConfigMap("collector:v1")
	cm.Data[nodeagent.ConfigKey] = "unknownField: true\n"
	if err := f.client.Create(f.ctx, cm); err != nil {
		t.Fatal(err)
	}
	f.reconcile()
	if got := f.job().Status.Message; !strings.HasPrefix(got, "CollectorConfigInvalid: ") {
		t.Fatalf("未替换过期 PVC 诊断：%q", got)
	}
	if err := f.client.Get(f.ctx, client.ObjectKeyFromObject(cm), cm); err != nil {
		t.Fatal(err)
	}
	cm.Data[nodeagent.ConfigKey] = "anotherUnknownField: true\n"
	if err := f.client.Update(f.ctx, cm); err != nil {
		t.Fatal(err)
	}
	f.restart()
	f.reconcile()
	if !strings.Contains(f.job().Status.Message, "anotherUnknownField") || eventReasons(recorder)["CollectorConfigInvalid"] != 1 {
		t.Fatal("配置错误详情应刷新，同类别事件应去重")
	}
	cm.Data[nodeagent.ConfigKey] = newConfigMap("collector:v2").Data[nodeagent.ConfigKey]
	if err := f.client.Update(f.ctx, cm); err != nil {
		t.Fatal(err)
	}
	if err := f.client.Delete(f.ctx, newTargetPod(true)); err != nil {
		t.Fatal(err)
	}
	f.reconcile()
	if got := f.job().Status.Message; !strings.HasPrefix(got, "TargetsNotReady: no Pods match") {
		t.Fatalf("缺少目标等待诊断：%q", got)
	}
	for i := 0; i < 3; i++ {
		f.restart()
		f.reconcile()
	}
	if n := eventReasons(recorder)["TargetsNotReady"]; n != 1 {
		t.Fatalf("目标等待事件重复：%d", n)
	}
	if err := f.client.Create(f.ctx, newTargetPod(true)); err != nil {
		t.Fatal(err)
	}
	f.reconcile()
	if messageCategory(f.job().Status.Message) != "TargetsSelected" {
		t.Fatal("目标就绪后未替换等待消息")
	}
	f.start()
	if messageCategory(f.job().Status.Message) != "InProgress" {
		t.Fatal("缺少进行中摘要")
	}
	f.complete(0, successResult())
	f.reconcile()
	if got := f.expectPhase(profilingv1alpha1.PhaseCompleted).Status.Message; !strings.HasPrefix(got, "Completed: ") || !strings.Contains(got, "completed=1") {
		t.Fatalf("缺少完成摘要：%q", got)
	}
}

func TestTerminalCleanupMessagesPreserveResults(t *testing.T) {
	for _, phase := range []profilingv1alpha1.ProfilingPhase{profilingv1alpha1.PhaseCompleted, profilingv1alpha1.PhaseFailed} {
		t.Run(string(phase), func(t *testing.T) {
			f := baseFixture(t)
			f.start()
			p := f.collectorFor(0)
			p.Finalizers = []string{"test/hold"}
			if err := f.client.Update(f.ctx, p); err != nil {
				t.Fatal(err)
			}
			result := successResult()
			if phase == profilingv1alpha1.PhaseFailed {
				result = collectorprotocol.Failed(collectorprotocol.ReasonStreamFailed, "stream broken")
			}
			f.complete(0, result)
			f.reconcile()
			original := f.expectPhase(phase).Status.DeepCopy()
			c := &collectorCleanupErrorClient{Client: f.client, failDelete: true}
			f.r.Client = c
			recorder := record.NewFakeRecorder(100)
			f.r.Recorder = recorder
			for i := 0; i < 3; i++ {
				f.restart()
				_, err := f.r.Reconcile(f.ctx, ctrl.Request{NamespacedName: client.ObjectKey{Namespace: ns, Name: "job"}})
				if err == nil {
					t.Fatal("未返回清理错误")
				}
				got := f.job().Status.DeepCopy()
				base, cleanup, _ := strings.Cut(got.Message, cleanupMessageSeparator)
				if base != original.Message || messageCategory(cleanup) != "CleanupFailed" {
					t.Fatalf("清理覆盖终态结果：%q", got.Message)
				}
				got.Message = base
				if !reflect.DeepEqual(got, original) {
					t.Fatal("清理修改了终态字段")
				}
			}
			if n := eventReasons(recorder)["CleanupFailed"]; n != 1 {
				t.Fatalf("清理失败事件重复：%d", n)
			}
			c.failDelete = false
			f.reconcile()
			if err := f.client.Get(f.ctx, client.ObjectKeyFromObject(p), p); err != nil {
				t.Fatal(err)
			}
			f.now = p.DeletionTimestamp.Add(2 * cleanupStuckAfter)
			for i := 0; i < 3; i++ {
				f.restart()
				f.reconcile()
			}
			if msg := f.job().Status.Message; !strings.HasPrefix(msg, original.Message+cleanupMessageSeparator+"CleanupStuck: ") || strings.Count(msg, cleanupMessageSeparator) != 1 {
				t.Fatalf("清理诊断未替换或原结果丢失：%q", msg)
			}
			if n := eventReasons(recorder)["CleanupStuck"]; n != 1 {
				t.Fatalf("清理卡住事件重复：%d", n)
			}
			p.Finalizers = nil
			if err := f.client.Update(f.ctx, p); err != nil {
				t.Fatal(err)
			}
			f.reconcile()
			if got := f.job().Status.DeepCopy(); !reflect.DeepEqual(got, original) {
				t.Fatalf("清理后原结果未恢复：%+v", got)
			}
		})
	}
}

func TestCleanupBarrierDoesNotTrustMessage(t *testing.T) {
	for _, mode := range []string{"resume", "new-round", "delete"} {
		t.Run(mode, func(t *testing.T) {
			f := baseFixture(t)
			f.start()
			p := f.collectorFor(0)
			p.Finalizers = []string{"test/hold"}
			if err := f.client.Update(f.ctx, p); err != nil {
				t.Fatal(err)
			}
			f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) { s.Cancel = true })
			f.reconcile()
			j := f.job()
			j.Status.Message = "Canceled: canceled" + cleanupMessageSeparator + "CollectorsRemoved: fabricated"
			if err := f.client.Status().Update(f.ctx, j); err != nil {
				t.Fatal(err)
			}
			if mode == "delete" {
				if err := f.client.Delete(f.ctx, j); err != nil {
					t.Fatal(err)
				}
			} else {
				f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) {
					s.Cancel = false
					if mode == "new-round" {
						s.RunIndex++
					}
				})
			}
			f.restart()
			if res := f.reconcile(); res.RequeueAfter != pollInterval {
				t.Fatalf("诊断消息越过清理屏障：%+v", res)
			}
			j = f.job()
			if lastRunIndex(&j.Status) != 1 || len(f.collectors()) != 1 || !strings.Contains(j.Status.Message, "CleanupPending: ") {
				t.Fatalf("未按实际 Pod 阻止恢复/新轮次/删除：%+v", j.Status)
			}
		})
	}
}

func TestMissingPVCBlocksBeforeConfig(t *testing.T) {
	f := newFixture(t, newJob(), newTargetPod(true))
	f.reconcile()
	result := f.reconcile()
	f.expectPhase(profilingv1alpha1.PhasePending)
	if result.RequeueAfter != waitRequeueInterval || f.r.lastBadRV != "" || len(f.collectors()) != 0 {
		t.Fatal("missing PVC allowed collection/config")
	}
}

func TestPendingPVCAndWaitForFirstConsumer(t *testing.T) {
	for _, tt := range []struct {
		name             string
		mode             storagev1.VolumeBindingMode
		volumeName       string
		allowBeforeBound bool
	}{
		{name: "Immediate", mode: storagev1.VolumeBindingImmediate},
		{name: "Immediate with volumeName", mode: storagev1.VolumeBindingImmediate, volumeName: "prebound-volume"},
		{name: "WaitForFirstConsumer", mode: storagev1.VolumeBindingWaitForFirstConsumer, allowBeforeBound: true},
		{name: "WaitForFirstConsumer with volumeName", mode: storagev1.VolumeBindingWaitForFirstConsumer, volumeName: "prebound-volume"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pvc := newPVC()
			pvc.Status.Phase = corev1.ClaimPending
			pvc.Spec.StorageClassName = ptr.To("storage")
			pvc.Spec.VolumeName = tt.volumeName
			f := newFixture(t, newJob(), newTargetPod(true), pvc, newConfigMap("collector:v1"), &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "storage"}, VolumeBindingMode: &tt.mode})
			for i := 0; i < 6; i++ {
				f.reconcile()
			}
			if tt.allowBeforeBound {
				if len(f.collectors()) != 1 {
					t.Fatal("WFFC deadlocked")
				}
				p := f.collectorFor(0)
				if p.Spec.NodeName != "" || p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchFields[0].Values[0] != nodeName {
					t.Fatal("WFFC needs scheduler on exact target node")
				}
			} else {
				if len(f.collectors()) != 0 {
					t.Fatal("collector started before PVC was Bound")
				}
				j := f.expectPhase(profilingv1alpha1.PhasePending)
				if j.Status.Targets != nil || !strings.HasPrefix(j.Status.Message, "PVCNotReady: ") || !strings.Contains(j.Status.Message, "not Bound") {
					t.Fatalf("PVC binding wait was not preserved: %+v", j.Status)
				}
				if result := f.reconcile(); result.RequeueAfter != waitRequeueInterval {
					t.Fatalf("PVC binding wait requeue=%s want=%s", result.RequeueAfter, waitRequeueInterval)
				}
			}
			if err := f.client.Get(f.ctx, client.ObjectKeyFromObject(pvc), pvc); err != nil {
				t.Fatal(err)
			}
			pvc.Status.Phase = corev1.ClaimBound
			if err := f.client.Status().Update(f.ctx, pvc); err != nil {
				t.Fatal(err)
			}
			f.start()
			if len(f.collectors()) != 1 {
				t.Fatal("Bound PVC did not start exactly one collector")
			}
		})
	}
}

func TestCrossNamespacePVCAndDeletionCleanup(t *testing.T) {
	j := newJob()
	j.Spec.PersistentVolumeClaimNamespace = "storage-ns"
	pvc := newPVC()
	pvc.Namespace = "storage-ns"
	f := newFixture(t, j, pvc, newTargetPod(true), newConfigMap("collector:v1"))
	f.start()
	p := f.collectorFor(0)
	if p.Namespace != "storage-ns" || len(p.OwnerReferences) != 0 {
		t.Fatal("cross namespace ownerref or PVC mount namespace invalid")
	}
	p.Finalizers = []string{"test/hold"}
	if err := f.client.Update(f.ctx, p); err != nil {
		t.Fatal(err)
	}
	j = f.job()
	if err := f.client.Delete(f.ctx, j); err != nil {
		t.Fatal(err)
	}
	f.reconcile()
	if err := f.client.Get(f.ctx, client.ObjectKeyFromObject(j), j); err != nil {
		t.Fatal("CR removed before collector exit")
	}
	if err := f.client.Get(f.ctx, client.ObjectKeyFromObject(p), p); err != nil {
		t.Fatal(err)
	}
	p.Finalizers = nil
	if err := f.client.Update(f.ctx, p); err != nil {
		t.Fatal(err)
	}
	f.reconcile()
	if err := f.client.Get(f.ctx, client.ObjectKeyFromObject(j), j); !apierrors.IsNotFound(err) {
		t.Fatalf("CR not finalized %v", err)
	}
}

func TestStuckCollectorCleanupIsReported(t *testing.T) {
	for _, mode := range []string{"delete", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			f := baseFixture(t)
			recorder := record.NewFakeRecorder(100)
			f.r.Recorder = recorder
			f.start()
			p := f.collectorFor(0)
			p.Finalizers, p.Spec.NodeName = []string{"test/hold"}, nodeName
			if err := f.client.Update(f.ctx, p); err != nil {
				t.Fatal(err)
			}
			if mode == "delete" {
				if err := f.client.Delete(f.ctx, f.job()); err != nil {
					t.Fatal(err)
				}
			} else {
				f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) { s.Cancel = true })
			}
			cleanup := func() string {
				_, msg, _ := strings.Cut(f.job().Status.Message, cleanupMessageSeparator)
				return msg
			}
			// The fake client stamps deletionTimestamp with the wall clock.
			f.now = time.Now()
			f.reconcile()
			f.reconcile()
			if c := cleanup(); messageCategory(c) != "CleanupPending" {
				t.Fatalf("cleanup before deadline=%+v", c)
			}
			if n := eventReasons(recorder)["CleanupStuck"]; n != 0 {
				t.Fatalf("CleanupStuck reported before deadline: %d", n)
			}
			f.now = time.Now().Add(2 * cleanupStuckAfter)
			for i := 0; i < 3; i++ {
				f.reconcile()
			}
			want := p.Namespace + "/" + p.Name + " on node " + nodeName
			if c := cleanup(); messageCategory(c) != "CleanupStuck" || !strings.Contains(c, want) {
				t.Fatalf("stuck cleanup condition=%+v want message containing %q", c, want)
			}
			if n := eventReasons(recorder)["CleanupStuck"]; n != 1 {
				t.Fatalf("CleanupStuck events=%d want 1", n)
			}
			if err := f.client.Get(f.ctx, client.ObjectKeyFromObject(p), p); err != nil {
				t.Fatal(err)
			}
			p.Finalizers = nil
			if err := f.client.Update(f.ctx, p); err != nil {
				t.Fatal(err)
			}
			f.reconcile()
			if mode == "delete" {
				j := &profilingv1alpha1.ProfilingJob{}
				if err := f.client.Get(f.ctx, client.ObjectKey{Namespace: ns, Name: "job"}, j); !apierrors.IsNotFound(err) {
					t.Fatalf("CR not finalized: %v", err)
				}
				return
			}
			f.expectCleanup("CollectorsRemoved")
		})
	}
}

func TestProfilingNotEnabledFailsWholePreflight(t *testing.T) {
	good := newTargetPod(true)
	good.Name = "good"
	good.UID = "good-uid"
	f := newFixture(t, newJob(), newTargetPod(false), good, newPVC(), newConfigMap("collector:v1"))
	recorder := record.NewFakeRecorder(100)
	f.r.Recorder = recorder
	f.reconcile()
	f.reconcile()
	j := f.expectPhase(profilingv1alpha1.PhaseFailed)
	if messageCategory(j.Status.Message) != "ProfilingNotEnabled" || len(f.collectors()) != 0 {
		t.Fatalf("preflight %+v", j.Status)
	}
	var warnings []string
	for len(recorder.Events) > 0 {
		if e := <-recorder.Events; strings.HasPrefix(e, "Warning ") {
			warnings = append(warnings, e)
		}
	}
	if len(warnings) != 1 || !strings.HasPrefix(warnings[0], "Warning Failed ") || !strings.Contains(warnings[0], strings.TrimPrefix(j.Status.Message, "ProfilingNotEnabled: ")) {
		t.Fatalf("preflight failure event must retain detailed diagnostics: %v", warnings)
	}
	f.restart()
	f.reconcile()
	if reasons := eventReasons(recorder); reasons["Failed"] != 0 {
		t.Fatalf("preflight failure event repeated: %v", reasons)
	}
}

func TestBatchTerminalFailureDoesNotRetryOrDisturbPeer(t *testing.T) {
	p := newTargetPod(true)
	p.Name, p.UID, p.Spec.NodeName = "target-2", "target-2-uid", "node-2"
	cm := newConfigMap("collector:v1")
	cm.Data[nodeagent.ConfigKey] = "image: collector:v1\nmaxRetries: 1\nretryBackoff: 1s\n"
	f := newFixture(t, newJob(), newTargetPod(true), p, newPVC(), cm)
	f.start()
	f.running(1, f.now)
	f.reconcile()
	stable := f.job().Status.Targets[1].CollectorPodName
	first := f.job().Status.Targets[0].CollectorPodName
	frozen := f.job().Status.RunConfig.DeepCopy()
	report := collectorprotocol.Failed(collectorprotocol.ReasonStreamFailed, "stream broken")
	report.ElapsedMillis, report.StopError = 123, "stop unavailable"
	report.Containers = []collectorprotocol.ContainerResult{{ContainerID: "c1", Phase: collectorprotocol.PhaseFailed, ElapsedMillis: 456}}
	recorder := record.NewFakeRecorder(100)
	f.r.Recorder = recorder
	f.complete(0, report)
	f.reconcile()
	j := f.job()
	failed, peer := j.Status.Targets[0], j.Status.Targets[1]
	if j.Status.Phase.IsTerminal() || failed.Stage != profilingv1alpha1.StageFailed || failed.RetryAt != nil || failed.RetryCount != 0 || failed.CollectorPodName != first || failed.CollectedDuration != 456 || !strings.Contains(failed.Message, "stopError: stop unavailable") || peer.Stage != profilingv1alpha1.StageCollecting || peer.CollectorPodName != stable {
		t.Fatalf("终态失败应立即记录且不影响其他目标: %+v", j.Status)
	}
	want := []string{fmt.Sprintf("Warning CollectorsUpdated profiling round 1 collectors: queued=0 collecting=1 retrying=0 completed=0 failed=1; %s/target stage=Failed retry=0 collector=%s/%s: %s: stream broken; stopError: stop unavailable", ns, ns, first, collectorprotocol.ReasonStreamFailed)}
	if events := recordedEvents(recorder); !reflect.DeepEqual(events, want) {
		t.Fatalf("目标终态失败应只发送一次详细汇总: %v want %v", events, want)
	}
	f.now = f.now.Add(time.Hour)
	f.restart()
	f.reconcile()
	if target := f.job().Status.Targets[0]; target.Stage != profilingv1alpha1.StageFailed || target.RetryCount != 0 || target.CollectorPodName != first || len(f.collectors()) != 2 {
		t.Fatalf("终态失败被重建: %+v", target)
	}
	if events := recordedEvents(recorder); len(events) != 0 {
		t.Fatalf("目标终态失败事件在重启后重复: %v", events)
	}
	f.complete(1, successResult())
	f.reconcile()
	j = f.expectPhase(profilingv1alpha1.PhaseFailed)
	if lastRunIndex(&j.Status) != 1 || !reflect.DeepEqual(j.Status.RunConfig, frozen) || j.Status.Targets[1].RetryCount != 0 || len(j.Status.Targets[1].OutputFiles) == 0 {
		t.Fatalf("轮次配置或成功结果被改变: %+v", j.Status)
	}
	if !strings.Contains(j.Status.Message, "stream broken; stopError: stop unavailable") {
		t.Fatalf("轮次状态丢失失败详情: %+v", j.Status)
	}
	want = []string{
		"Warning Failed profiling round 1 Failed: completed=1 failed=1",
		fmt.Sprintf("Normal CollectorsUpdated profiling round 1 collectors: queued=0 collecting=0 retrying=0 completed=1 failed=1; %s/target-2 stage=Completed retry=0 collector=%s/%s elapsed=1m0s outputs=/inference/job/run-1/inference_target_gpu-container_lastkernels.log, /inference/job/run-1/inference_target_gpu-container_kerneltime.log", ns, ns, stable),
	}
	if events := recordedEvents(recorder); !reflect.DeepEqual(events, want) {
		t.Fatalf("轮次失败事件应独立保留，汇总只含本次成功目标: %v want %v", events, want)
	}
}

func TestTerminalCollectorFailureEvents(t *testing.T) {
	for _, tc := range []struct {
		name, reason, message, stopError string
	}{
		{"stream", collectorprotocol.ReasonStreamFailed, "stream broken", "stop unavailable"},
		{"invalid-args", collectorprotocol.ReasonInvalidArgs, "invalid mode", ""},
		{"missing-reason", "", "collector crashed", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := baseFixture(t)
			f.start()
			recorder := record.NewFakeRecorder(100)
			f.r.Recorder = recorder
			report := collectorprotocol.Failed(tc.reason, tc.message)
			report.StopError = tc.stopError
			f.complete(0, report)
			f.reconcile()
			j := f.expectPhase(profilingv1alpha1.PhaseFailed)
			reason, message := tc.reason, tc.message
			if reason == "" {
				reason = "CollectionFailed"
			}
			if tc.stopError != "" {
				message += "; stopError: " + tc.stopError
			}
			if target := j.Status.Targets[0]; target.Reason != reason || target.Message != message || messageCategory(j.Status.Message) != "TargetsFailed" || !strings.Contains(j.Status.Message, firstFailure(j.Status.Targets)) {
				t.Fatalf("failure details were lost from status: %+v", j.Status)
			}
			var warnings []string
			for len(recorder.Events) > 0 {
				if e := <-recorder.Events; strings.HasPrefix(e, "Warning ") {
					warnings = append(warnings, e)
				}
			}
			want := []string{
				"Warning Failed profiling round 1 Failed: completed=0 failed=1",
				fmt.Sprintf("Warning CollectorsUpdated profiling round 1 collectors: queued=0 collecting=0 retrying=0 completed=0 failed=1; %s/target stage=Failed retry=0 collector=%s/%s: %s: %s", ns, ns, j.Status.Targets[0].CollectorPodName, reason, message),
			}
			if !reflect.DeepEqual(warnings, want) {
				t.Fatalf("failure events = %v, want %v", warnings, want)
			}
			f.reconcile()
			f.restart()
			f.reconcile()
			f.reconcile()
			if events := recordedEvents(recorder); len(events) != 0 {
				t.Fatalf("terminal failure events repeated during cleanup or restart: %v", events)
			}
		})
	}
}

func TestLostSingleTargetRetryKeepsRunningPhase(t *testing.T) {
	f := baseFixture(t)
	f.start()
	f.running(0, f.now)
	f.reconcile()
	f.expectPhase(profilingv1alpha1.PhaseRunning)
	collectorName := f.collectorFor(0).Name
	recorder := record.NewFakeRecorder(100)
	f.r.Recorder = recorder
	if err := f.client.Delete(f.ctx, f.collectorFor(0)); err != nil {
		t.Fatal(err)
	}
	requeued := false
	for i := 0; i < 10 && !requeued; i++ {
		f.reconcile()
		target := f.expectPhase(profilingv1alpha1.PhaseRunning).Status.Targets[0]
		requeued = target.RetryCount == 1 && target.Stage != profilingv1alpha1.StageRetrying
		f.now = f.now.Add(2 * time.Second)
	}
	if !requeued {
		t.Fatalf("target never retried: %+v", f.job().Status)
	}
	want := []string{
		fmt.Sprintf("Warning CollectorsUpdated profiling round 1 collectors: queued=0 collecting=0 retrying=1 completed=0 failed=0; %s/target stage=Retrying retry=0 collector=%s/%s: CollectorLost: collector Pod disappeared before its result was recorded", ns, ns, collectorName),
		fmt.Sprintf("Normal CollectorsUpdated profiling round 1 collectors: queued=1 collecting=0 retrying=0 completed=0 failed=0; %s/target stage=Queued retry=1: CollectorLost: collector Pod disappeared before its result was recorded", ns),
	}
	if events := recordedEvents(recorder); !reflect.DeepEqual(events, want) {
		t.Fatalf("retry summary events=%v want=%v", events, want)
	}
}

func TestLostCollectorWithNoRetriesEmitsOnlyTerminalFailureEvents(t *testing.T) {
	cm := newConfigMap("collector:v1")
	cm.Data[nodeagent.ConfigKey] = "image: collector:v1\nmaxRetries: 0\n"
	f := newFixture(t, newJob(), newTargetPod(true), newPVC(), cm)
	f.start()
	recorder := record.NewFakeRecorder(100)
	f.r.Recorder = recorder
	if err := f.client.Delete(f.ctx, f.collectorFor(0)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		f.reconcile()
	}
	j := f.expectPhase(profilingv1alpha1.PhaseFailed)
	if target := j.Status.Targets[0]; target.Reason != "CollectorLost" || target.Stage != profilingv1alpha1.StageFailed || target.RetryCount != 0 {
		t.Fatalf("unexpected exhausted retry state: %+v", j.Status)
	}
	collectorName := j.Status.Targets[0].CollectorPodName
	want := []string{
		fmt.Sprintf("Warning CollectorsUpdated profiling round 1 collectors: queued=0 collecting=0 retrying=1 completed=0 failed=0; %s/target stage=Retrying retry=0 collector=%s/%s: CollectorLost: collector Pod disappeared before its result was recorded", ns, ns, collectorName),
		"Warning Failed profiling round 1 Failed: completed=0 failed=1",
		fmt.Sprintf("Warning CollectorsUpdated profiling round 1 collectors: queued=0 collecting=0 retrying=0 completed=0 failed=1; %s/target stage=Failed retry=0 collector=%s/%s: CollectorLost: collector Pod disappeared before its result was recorded", ns, ns, collectorName),
	}
	if events := recordedEvents(recorder); !reflect.DeepEqual(events, want) {
		t.Fatalf("exhausted retry summary events=%v want=%v", events, want)
	}
}

func TestNonRetryableInvalidArguments(t *testing.T) {
	f := baseFixture(t)
	f.start()
	f.complete(0, collectorprotocol.Failed(collectorprotocol.ReasonInvalidArgs, "invalid mode"))
	f.reconcile()
	target := f.job().Status.Targets[0]
	if target.Stage != profilingv1alpha1.StageFailed || target.RetryAt != nil || target.Reason != collectorprotocol.ReasonInvalidArgs || target.Message != "invalid mode" {
		t.Fatalf("failure not persisted before cleanup: %+v", target)
	}
	f.reconcile()
	f.reconcile()
	j := f.expectPhase(profilingv1alpha1.PhaseFailed)
	if j.Status.Targets[0].RetryCount != 0 || len(f.collectors()) != 0 {
		t.Fatal("invalid args retried or collector not cleaned")
	}
}

func TestCollectorsUpdatedBatchesMultiTargetLifecycle(t *testing.T) {
	second := newTargetPod(true)
	second.Name, second.UID = "target-2", "target-2-uid"
	job, pvc := newJob(), newPVC()
	job.Spec.PersistentVolumeClaimNamespace, pvc.Namespace = "storage-ns", "storage-ns"
	f := newFixture(t, job, newTargetPod(true), second, pvc, newConfigMap("collector:v1"))
	recorder := record.NewFakeRecorder(100)
	f.r.Recorder = recorder
	f.reconcile()
	f.reconcile()
	if got := eventReasons(recorder); !reflect.DeepEqual(got, map[string]int{"RoundCreated": 1, "TargetsSelected": 1}) {
		t.Fatalf("initial target selection must not emit a collector summary: %v", got)
	}
	f.reconcile()
	if events := recordedEvents(recorder); len(events) != 0 || len(f.collectors()) != 0 {
		t.Fatalf("persisting collector names alone emitted events or created Pods: %v", events)
	}
	f.reconcile()
	j := f.job()
	first, peer := j.Status.Targets[0].CollectorPodName, j.Status.Targets[1].CollectorPodName
	if len(f.collectors()) != 2 || j.Status.Targets[0].Stage != profilingv1alpha1.StageCollecting || j.Status.Targets[1].Stage != profilingv1alpha1.StageCollecting {
		t.Fatalf("both collectors must be created in the same reconcile: %+v", j.Status)
	}
	want := []string{fmt.Sprintf("Normal CollectorsUpdated profiling round 1 collectors: queued=0 collecting=2 retrying=0 completed=0 failed=0; inference/target stage=Collecting retry=0 collector=storage-ns/%s: CollectorCreated: collector Pod storage-ns/%s created on node node-1; inference/target-2 stage=Collecting retry=0 collector=storage-ns/%s: CollectorCreated: collector Pod storage-ns/%s created on node node-1", first, first, peer, peer)}
	if events := recordedEvents(recorder); !reflect.DeepEqual(events, want) {
		t.Fatalf("creation must emit exactly one detailed summary: %v want %v", events, want)
	}
	f.running(0, f.now)
	f.running(1, f.now)
	f.reconcile()
	want = []string{
		"Normal CollectionStarted profiling round 1 started writing to /inference/job/run-1",
		fmt.Sprintf("Normal CollectorsUpdated profiling round 1 collectors: queued=0 collecting=2 retrying=0 completed=0 failed=0; inference/target stage=Collecting retry=0 collector=storage-ns/%s: CollectorRunning: collector Pod storage-ns/%s is running; inference/target-2 stage=Collecting retry=0 collector=storage-ns/%s: CollectorRunning: collector Pod storage-ns/%s is running", first, first, peer, peer),
	}
	if events := recordedEvents(recorder); !reflect.DeepEqual(events, want) {
		t.Fatalf("running must retain the lifecycle event and one summary: %v want %v", events, want)
	}
	f.now = f.now.Add(time.Minute)
	for i := 0; i < 2; i++ {
		res := successResult()
		res.Message = fmt.Sprintf("target %d collected", i)
		res.Containers[0].OutputFiles = []string{fmt.Sprintf("target-%d.log", i)}
		f.complete(i, res)
	}
	f.reconcile()
	j = f.expectPhase(profilingv1alpha1.PhaseCompleted)
	for i, target := range j.Status.Targets {
		if target.Stage != profilingv1alpha1.StageCompleted || target.RetryCount != 0 || target.CollectedDuration != 60000 || !reflect.DeepEqual(target.OutputFiles, []string{fmt.Sprintf("/inference/job/run-1/target-%d.log", i)}) {
			t.Fatalf("completion lost target %d results: %+v", i, target)
		}
	}
	want = []string{
		"Normal Completed profiling round 1 Completed: queued=0 collecting=0 retrying=0 completed=2 failed=0",
		fmt.Sprintf("Normal CollectorsUpdated profiling round 1 collectors: queued=0 collecting=0 retrying=0 completed=2 failed=0; inference/target stage=Completed retry=0 collector=storage-ns/%s: target 0 collected elapsed=1m0s outputs=/inference/job/run-1/target-0.log; inference/target-2 stage=Completed retry=0 collector=storage-ns/%s: target 1 collected elapsed=1m0s outputs=/inference/job/run-1/target-1.log", first, peer),
	}
	if events := recordedEvents(recorder); !reflect.DeepEqual(events, want) {
		t.Fatalf("completion must emit exactly one summary with elapsed/output details: %v want %v", events, want)
	}
	f.restart()
	f.reconcile()
	f.reconcile()
	if events := recordedEvents(recorder); len(events) != 0 {
		t.Fatalf("terminal cleanup/restart repeated collector events: %v", events)
	}
	f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) { s.RunIndex = 2 })
	f.reconcile()
	f.reconcile()
	if got := eventReasons(recorder); !reflect.DeepEqual(got, map[string]int{"RoundCreated": 1, "TargetsSelected": 1}) {
		t.Fatalf("round rollover must not compare collectors across runIndex: %v", got)
	}
	if lastRunIndex(&f.job().Status) != 2 || len(f.job().Status.History) != 1 {
		t.Fatalf("new round did not retain history: %+v", f.job().Status)
	}
}

func TestCollectorsUpdatedMixedSuccessFailureWarning(t *testing.T) {
	second := newTargetPod(true)
	second.Name, second.UID = "target-2", "target-2-uid"
	f := newFixture(t, newJob(), newTargetPod(true), second, newPVC(), newConfigMap("collector:v1"))
	f.start()
	f.running(0, f.now)
	f.running(1, f.now)
	f.reconcile()
	recorder := record.NewFakeRecorder(100)
	f.r.Recorder = recorder
	first, peer := f.collectorFor(0).Name, f.collectorFor(1).Name
	res := successResult()
	res.Message, res.Containers[0].OutputFiles = "collection saved", []string{"success.log"}
	f.complete(0, res)
	f.complete(1, collectorprotocol.Failed(collectorprotocol.ReasonStreamFailed, "peer stream broken"))
	f.reconcile()
	j := f.expectPhase(profilingv1alpha1.PhaseFailed)
	if j.Status.Targets[0].Stage != profilingv1alpha1.StageCompleted || j.Status.Targets[1].Stage != profilingv1alpha1.StageFailed || j.Status.Targets[1].RetryAt != nil || j.Status.Targets[1].RetryCount != 0 {
		t.Fatalf("mixed terminal results must not retry: %+v", j.Status)
	}
	want := []string{
		"Warning Failed profiling round 1 Failed: completed=1 failed=1",
		fmt.Sprintf("Warning CollectorsUpdated profiling round 1 collectors: queued=0 collecting=0 retrying=0 completed=1 failed=1; inference/target stage=Completed retry=0 collector=inference/%s: collection saved elapsed=1m0s outputs=/inference/job/run-1/success.log; inference/target-2 stage=Failed retry=0 collector=inference/%s: %s: peer stream broken", first, peer, collectorprotocol.ReasonStreamFailed),
	}
	if events := recordedEvents(recorder); !reflect.DeepEqual(events, want) {
		t.Fatalf("mixed results must share one Warning summary: %v want %v", events, want)
	}
}

func TestCollectorsUpdatedStableReconcileAndRestart(t *testing.T) {
	f := baseFixture(t)
	f.start()
	f.running(0, f.now)
	recorder := record.NewFakeRecorder(100)
	f.r.Recorder = recorder
	f.reconcile()
	collectorName := f.collectorFor(0).Name
	want := []string{
		"Normal CollectionStarted profiling round 1 started writing to /inference/job/run-1",
		fmt.Sprintf("Normal CollectorsUpdated profiling round 1 collectors: queued=0 collecting=1 retrying=0 completed=0 failed=0; inference/target stage=Collecting retry=0 collector=inference/%s: CollectorRunning: collector Pod inference/%s is running", collectorName, collectorName),
	}
	if events := recordedEvents(recorder); !reflect.DeepEqual(events, want) {
		t.Fatalf("initial running summary=%v want=%v", events, want)
	}
	before := f.job().Status.DeepCopy()
	for i := 0; i < 3; i++ {
		f.reconcile()
		f.now = f.now.Add(time.Second)
		f.restart()
		f.reconcile()
		j := f.expectPhase(profilingv1alpha1.PhaseRunning)
		if j.Status.CollectedDuration != int64(i+1)*1000 || !reflect.DeepEqual(j.Status.Targets, before.Targets) || !reflect.DeepEqual(j.Status.StartTime, before.StartTime) || len(f.collectors()) != 1 {
			t.Fatalf("stable reconcile/restart changed target or stopped updating wall time: %+v", j.Status)
		}
		if events := recordedEvents(recorder); len(events) != 0 {
			t.Fatalf("stable reconcile/restart repeated collector events: %v", events)
		}
	}
}

func TestCollectorsUpdatedIgnoresMessageOnlyChanges(t *testing.T) {
	f := baseFixture(t)
	f.start()
	p := f.collectorFor(0)
	p.Status.Phase = corev1.PodPending
	p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "collector", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff", Message: "first pull error"}}}}
	if err := f.client.Status().Update(f.ctx, p); err != nil {
		t.Fatal(err)
	}
	recorder := record.NewFakeRecorder(100)
	f.r.Recorder = recorder
	f.reconcile()
	want := []string{fmt.Sprintf("Normal CollectorsUpdated profiling round 1 collectors: queued=0 collecting=1 retrying=0 completed=0 failed=0; inference/target stage=Collecting retry=0 collector=inference/%s: ImagePullBackOff: first pull error", p.Name)}
	if events := recordedEvents(recorder); !reflect.DeepEqual(events, want) {
		t.Fatalf("initial pending summary=%v want=%v", events, want)
	}
	before := f.job().Status.Targets[0]
	for _, message := range []string{"second pull error", "latest pull error"} {
		p = f.collectorFor(0)
		p.Status.ContainerStatuses[0].State.Waiting.Message = message
		if err := f.client.Status().Update(f.ctx, p); err != nil {
			t.Fatal(err)
		}
		f.now = f.now.Add(time.Second)
		f.restart()
		f.reconcile()
		wantTarget := before
		wantTarget.Message = message
		if target := f.job().Status.Targets[0]; !reflect.DeepEqual(target, wantTarget) {
			t.Fatalf("message-only diagnostic change must persist without changing identity/retry: %+v want %+v", target, wantTarget)
		}
		if events := recordedEvents(recorder); len(events) != 0 {
			t.Fatalf("message-only change repeated collector events: %v", events)
		}
	}
}

func TestCollectorsUpdatedStatusComparison(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*profilingv1alpha1.ProfilingJob, *profilingv1alpha1.ProfilingJobStatus)
		want   string
	}{
		{"stage", func(j *profilingv1alpha1.ProfilingJob, _ *profilingv1alpha1.ProfilingJobStatus) {
			j.Status.Targets[0].Stage = profilingv1alpha1.StageRetrying
		}, "Warning CollectorsUpdated profiling round 1 collectors: queued=0 collecting=0 retrying=1 completed=0 failed=0; inference/target stage=Retrying retry=2 collector=inference/collector: CollectorRunning: running"},
		{"reason", func(j *profilingv1alpha1.ProfilingJob, _ *profilingv1alpha1.ProfilingJobStatus) {
			j.Status.Targets[0].Reason = "CollectorPendingTimeout"
		}, "Warning CollectorsUpdated profiling round 1 collectors: queued=0 collecting=1 retrying=0 completed=0 failed=0; inference/target stage=Collecting retry=2 collector=inference/collector: CollectorPendingTimeout: running"},
		{"retry count", func(j *profilingv1alpha1.ProfilingJob, _ *profilingv1alpha1.ProfilingJobStatus) {
			j.Status.Targets[0].RetryCount++
		}, "Normal CollectorsUpdated profiling round 1 collectors: queued=0 collecting=1 retrying=0 completed=0 failed=0; inference/target stage=Collecting retry=3 collector=inference/collector: CollectorRunning: running"},
		{"collector UID", func(j *profilingv1alpha1.ProfilingJob, _ *profilingv1alpha1.ProfilingJobStatus) {
			j.Status.Targets[0].CollectorPodUID = "replacement-uid"
		}, "Normal CollectorsUpdated profiling round 1 collectors: queued=0 collecting=1 retrying=0 completed=0 failed=0; inference/target stage=Collecting retry=2 collector=inference/collector: CollectorRunning: running"},
		{"message only", func(j *profilingv1alpha1.ProfilingJob, _ *profilingv1alpha1.ProfilingJobStatus) {
			j.Status.Targets[0].Message = "new diagnostic"
		}, ""},
		{"wall clock only", func(j *profilingv1alpha1.ProfilingJob, _ *profilingv1alpha1.ProfilingJobStatus) {
			j.Status.CollectedDuration = 1000
			j.Status.Targets[0].CollectedDuration = 1000
			j.Status.Targets[0].CollectorCreatedAt = ptr.To(metav1.NewTime(time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)))
		}, ""},
		{"no previous targets", func(j *profilingv1alpha1.ProfilingJob, before *profilingv1alpha1.ProfilingJobStatus) {
			before.Targets = nil
			j.Status.Targets[0].Stage = profilingv1alpha1.StageFailed
		}, ""},
		{"different runIndex", func(j *profilingv1alpha1.ProfilingJob, _ *profilingv1alpha1.ProfilingJobStatus) {
			j.Status.LastRunIndex = ptr.To(int64(2))
			j.Status.Targets[0].Stage = profilingv1alpha1.StageFailed
		}, ""},
		{"canceled", func(j *profilingv1alpha1.ProfilingJob, _ *profilingv1alpha1.ProfilingJobStatus) {
			j.Status.Phase = profilingv1alpha1.PhaseCanceled
			j.Status.Targets[0].Stage = profilingv1alpha1.StageCanceled
		}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			j := newJob()
			j.Status = profilingv1alpha1.ProfilingJobStatus{
				Phase: profilingv1alpha1.PhaseRunning, LastRunIndex: ptr.To(int64(1)),
				Targets: []profilingv1alpha1.TargetStatus{{TargetNamespace: ns, TargetName: "target", TargetUID: "target-uid", Stage: profilingv1alpha1.StageCollecting, RetryCount: 2, CollectorPodName: "collector", CollectorPodUID: "collector-uid", Reason: "CollectorRunning", Message: "running"}},
			}
			before := j.Status.DeepCopy()
			tc.mutate(j, before)
			recorder := record.NewFakeRecorder(10)
			r := &ProfilingJobReconciler{Recorder: recorder}
			r.reportCollectorChanges(j, before)
			var want []string
			if tc.want != "" {
				want = []string{tc.want}
			}
			if events := recordedEvents(recorder); !reflect.DeepEqual(events, want) {
				t.Fatalf("status comparison events=%v want=%v", events, want)
			}
		})
	}
}

func TestPVCCompatibility(t *testing.T) {
	for _, mode := range []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce, corev1.ReadWriteOncePod, corev1.ReadOnlyMany} {
		t.Run(string(mode), func(t *testing.T) {
			p := newTargetPod(true)
			p.Name, p.UID, p.Spec.NodeName = "other", "other-uid", "node-2"
			pvc := newPVC()
			pvc.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{mode}
			f := newFixture(t, newJob(), newTargetPod(true), p, pvc, newConfigMap("collector:v1"))
			f.reconcile()
			f.reconcile()
			f.expectPhase(profilingv1alpha1.PhaseFailed)
			if len(f.collectors()) != 0 {
				t.Fatal("incompatible PVC started collectors")
			}
		})
	}
	pvc := newPVC()
	pvc.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	f := newFixture(t, newJob(), newTargetPod(true), pvc, newConfigMap("collector:v1"))
	f.start()
}

func TestCollectorRecreationUsesLatestConfigAndFrozenRunInputs(t *testing.T) {
	f := baseFixture(t)
	f.start()
	old := f.collectorFor(0)
	frozen := f.job().Status.RunConfig.DeepCopy()
	f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) {
		s.Duration.Duration = 45 * time.Second
		s.ProfilingMode = profilingv1alpha1.ProfilingModeLight
		s.ProfilingRules = "next-round"
		s.RunIndex = 2
	})
	cm := newConfigMap("collector:v2")
	cm.Data[nodeagent.ConfigKey] = "image: collector:v2\npollInterval: 7s\nmaxRetries: 1\nretryBackoff: 2s\nmaxRetryBackoff: 4s\n"
	if err := f.client.Update(f.ctx, cm); err != nil {
		t.Fatal(err)
	}
	if err := f.client.Delete(f.ctx, old); err != nil {
		t.Fatal(err)
	}
	f.reconcile()
	j := f.job()
	target := j.Status.Targets[0]
	if target.Stage != profilingv1alpha1.StageRetrying || target.RetryCount != 0 || !timeEqual(target.RetryAt, f.now.Add(2*time.Second)) || !reflect.DeepEqual(j.Status.RunConfig, frozen) {
		t.Fatalf("collector loss did not use current retry backoff with frozen run inputs: %+v", j.Status)
	}
	f.now = f.now.Add(time.Second)
	f.restart()
	if res := f.reconcile(); res.RequeueAfter != time.Second || len(f.collectors()) != 0 || f.job().Status.Targets[0].RetryCount != 0 {
		t.Fatalf("retry ignored latest backoff after restart: %+v", res)
	}
	f.now = f.now.Add(time.Second)
	f.start()
	j = f.job()
	p := f.collectorFor(0)
	args := strings.Join(p.Spec.Containers[0].Args, " ")
	if lastRunIndex(&j.Status) != 1 || j.Status.Targets[0].RetryCount != 1 || p.Name == old.Name || p.Namespace != j.Spec.PersistentVolumeClaimNamespace || p.Spec.Containers[0].Image != "collector:v2" || !reflect.DeepEqual(j.Status.RunConfig, frozen) || len(j.Status.History) != 0 {
		t.Fatalf("collector recreation changed active round or ignored latest config: %+v pod=%+v", j.Status, p.Spec)
	}
	for _, arg := range []string{"--duration=10s", "--mode=full"} {
		if !strings.Contains(args, arg) {
			t.Fatalf("recreated collector missing frozen input %s: %s", arg, args)
		}
	}
	if strings.Contains(args, "--poll-interval") || strings.Contains(args, "--max-retries") {
		t.Fatalf("recreated collector must not receive timeout or retry arguments: %s", args)
	}
	if err := f.client.Delete(f.ctx, p); err != nil {
		t.Fatal(err)
	}
	f.reconcile()
	if target := f.job().Status.Targets[0]; target.Stage != profilingv1alpha1.StageRetrying || target.RetryAt != nil {
		t.Fatalf("current maxRetries=1 was not enforced: %+v", target)
	}
	f.reconcile()
	j = f.expectPhase(profilingv1alpha1.PhaseFailed)
	if lastRunIndex(&j.Status) != 1 || j.Status.Targets[0].RetryCount != 1 || j.Status.Targets[0].Reason != "CollectorLost" || len(f.collectors()) != 0 || !reflect.DeepEqual(j.Status.RunConfig, frozen) {
		t.Fatalf("current retry budget did not finish the active round: %+v", j.Status)
	}
}

func TestCanceledPeerWithoutCollectorDoesNotBlock(t *testing.T) {
	for _, tc := range []struct {
		name    string
		phase   profilingv1alpha1.ProfilingPhase
		newRun  bool
		cleaned bool
	}{
		{"initial cancel runIndex 0", "", false, false},
		{"unobserved cancel on pending peer", profilingv1alpha1.PhasePending, false, false},
		{"unobserved cancel on running peer", profilingv1alpha1.PhaseRunning, false, false},
		{"canceled cleanup pending", profilingv1alpha1.PhaseCanceled, false, false},
		{"canceled cleanup complete", profilingv1alpha1.PhaseCanceled, false, true},
		{"canceled queued higher runIndex", profilingv1alpha1.PhaseCanceled, true, false},
		{"completed queued higher runIndex", profilingv1alpha1.PhaseCompleted, true, false},
		{"failed queued higher runIndex", profilingv1alpha1.PhaseFailed, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			older := namedJob("older", "older-uid", time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC))
			older.Spec.Cancel = true
			older.Status.Phase = tc.phase
			if tc.phase == "" {
				older.Spec.RunIndex = 0
			} else {
				older.Status.LastRunIndex = ptr.To(int64(1))
				older.Status.RunConfig = &profilingv1alpha1.RunConfiguration{Duration: older.Spec.Duration, ProfilingMode: older.Spec.ProfilingMode}
				stage := profilingv1alpha1.StageCanceled
				if tc.phase == profilingv1alpha1.PhasePending || tc.phase == profilingv1alpha1.PhaseRunning {
					stage = profilingv1alpha1.StageCollecting
				}
				older.Status.Targets = []profilingv1alpha1.TargetStatus{{TargetNamespace: ns, TargetName: "target", TargetUID: "target-uid", Stage: stage}}
			}
			if tc.newRun {
				older.Spec.RunIndex = 3
			}
			if tc.phase == profilingv1alpha1.PhaseCanceled {
				reason := "CleanupPending"
				if tc.cleaned {
					reason = "CollectorsRemoved"
				}
				older.Status.Message = "Canceled: canceled" + cleanupMessageSeparator + reason + ": previous cleanup"
			}
			f := newFixture(t, newJob(), older, newTargetPod(true), newPVC(), newConfigMap("collector:v1"))
			before := f.namedJob("older").Status.DeepCopy()
			f.r.Client = collectorBlindCache{f.client}
			f.start()
			f.restart()
			f.reconcile()
			pods := f.collectors()
			if len(pods) != 1 || pods[0].Labels[nodeagent.LabelJobUID] != "job-uid" || !reflect.DeepEqual(f.namedJob("older").Status.DeepCopy(), before) {
				t.Fatalf("canceled peer without collector blocked target or consumed its round: collectors=%+v older=%+v", pods, f.namedJob("older").Status)
			}
		})
	}
}

func TestCanceledPeerCollectorBlocksUntilExit(t *testing.T) {
	for _, deleting := range []bool{false, true} {
		t.Run(map[bool]string{false: "active", true: "deleting succeeded pod"}[deleting], func(t *testing.T) {
			older := newJob()
			older.Spec.Cancel = true
			younger := namedJob("younger", "younger-uid", older.CreationTimestamp.Add(time.Hour))
			younger.Namespace = "other-jobs"
			younger.Spec.TargetSelector = profilingv1alpha1.TargetSelector{PodName: "target", PodNamespace: ns}
			younger.Spec.PersistentVolumeClaimNamespace = "storage-ns"
			pvc := newPVC()
			pvc.Namespace = "storage-ns"
			f := newFixture(t, older, younger, newTargetPod(true), newPVC(), pvc, newConfigMap("collector:v1"))
			f.r.Client = collectorBlindCache{f.client}
			f.waitForCanceledCleanup()
			youngerKey := client.ObjectKeyFromObject(younger)
			for i := 0; i < 6; i++ {
				f.reconcileKey(youngerKey)
			}
			pods := f.collectors()
			if len(pods) != 1 || pods[0].Labels[nodeagent.LabelJobUID] != "younger-uid" || pods[0].Namespace != "storage-ns" {
				t.Fatalf("younger collector not established: %+v", pods)
			}
			pod := pods[0].DeepCopy()
			pod.Finalizers = []string{"test/hold"}
			if err := f.client.Update(f.ctx, pod); err != nil {
				t.Fatal(err)
			}
			pod.Status.Phase = corev1.PodRunning
			if err := f.client.Status().Update(f.ctx, pod); err != nil {
				t.Fatal(err)
			}
			if err := f.client.Get(f.ctx, youngerKey, younger); err != nil {
				t.Fatal(err)
			}
			younger.Spec.Cancel = true
			if err := f.client.Update(f.ctx, younger); err != nil {
				t.Fatal(err)
			}
			f.reconcileKey(youngerKey)
			if err := f.client.Get(f.ctx, client.ObjectKeyFromObject(pod), pod); err != nil || !pod.DeletionTimestamp.IsZero() {
				t.Fatalf("first cancellation deleted younger collector: %v", err)
			}
			if deleting {
				f.reconcileKey(youngerKey)
				if err := f.client.Get(f.ctx, client.ObjectKeyFromObject(pod), pod); err != nil || pod.DeletionTimestamp.IsZero() {
					t.Fatalf("younger collector not deleting: %v", err)
				}
				pod.Status.Phase = corev1.PodSucceeded
				if err := f.client.Status().Update(f.ctx, pod); err != nil {
					t.Fatal(err)
				}
			}
			f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) { s.Cancel = false })
			for i := 0; i < 6; i++ {
				f.restart()
				f.reconcile()
			}
			j := f.job()
			if len(f.collectors()) != 1 || len(j.Status.Targets) != 1 || j.Status.Targets[0].Stage != profilingv1alpha1.StageQueued || j.Status.Targets[0].Reason != "Queued" || !strings.Contains(j.Status.Targets[0].Message, "storage-ns/"+pod.Name) {
				t.Fatalf("older job bypassed younger canceled collector: status=%+v collectors=%+v", j.Status, f.collectors())
			}
			f.reconcileKey(youngerKey)
			if err := f.client.Get(f.ctx, client.ObjectKeyFromObject(pod), pod); err != nil {
				t.Fatal(err)
			}
			pod.Finalizers = nil
			if err := f.client.Update(f.ctx, pod); err != nil {
				t.Fatal(err)
			}
			f.reconcileKey(youngerKey)
			if err := f.client.Get(f.ctx, youngerKey, younger); err != nil || younger.Status.Phase != profilingv1alpha1.PhaseCanceled || !strings.Contains(younger.Status.Message, cleanupMessageSeparator+"CollectorsRemoved: ") {
				t.Fatalf("younger cleanup not completed: error=%v status=%+v", err, younger.Status)
			}
			f.restart()
			f.start()
			if pods := f.collectors(); len(pods) != 1 || pods[0].Labels[nodeagent.LabelJobUID] != "job-uid" {
				t.Fatalf("older job did not proceed after authoritative cleanup: %+v", pods)
			}
		})
	}
}

func TestPendingCollectorTimeoutOnlyWarns(t *testing.T) {
	for _, tc := range []struct {
		reason     string
		maxRetries int32
	}{
		{"ContainerCreating", 3}, {"ErrImagePull", 3}, {"ImagePullBackOff", 3},
		{"Unschedulable", 3}, {"ImagePullBackOff", 0},
	} {
		t.Run(fmt.Sprintf("%s/retries-%d", tc.reason, tc.maxRetries), func(t *testing.T) {
			cm := newConfigMap("collector:v1")
			cm.Data[nodeagent.ConfigKey] = fmt.Sprintf("image: collector:v1\nmaxRetries: %d\npendingTimeout: 30s\n", tc.maxRetries)
			f := newFixture(t, newJob(), newTargetPod(true), newPVC(), cm)
			f.start()
			p := f.collectorFor(0)
			key, uid := client.ObjectKeyFromObject(p), p.UID
			p.Status.Phase = corev1.PodPending
			if tc.reason == "Unschedulable" {
				p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: tc.reason, Message: "waiting details"}}
			} else {
				p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "collector", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: tc.reason, Message: "waiting details"}}}}
			}
			if err := f.client.Status().Update(f.ctx, p); err != nil {
				t.Fatal(err)
			}
			recorder := record.NewFakeRecorder(100)
			f.r.Recorder = recorder
			f.now = f.now.Add(29 * time.Second)
			if res := f.reconcile(); res.RequeueAfter != pollInterval {
				t.Fatalf("pending requeue: %+v", res)
			}
			if target := f.job().Status.Targets[0]; target.Reason != tc.reason {
				t.Fatalf("warning before threshold: %+v", target)
			}
			want := []string{fmt.Sprintf("Normal CollectorsUpdated profiling round 1 collectors: queued=0 collecting=1 retrying=0 completed=0 failed=0; %s/target stage=Collecting retry=0 collector=%s: %s: waiting details", ns, key, tc.reason)}
			if events := recordedEvents(recorder); !reflect.DeepEqual(events, want) {
				t.Fatalf("events before threshold=%v want=%v", events, want)
			}
			f.now = f.now.Add(time.Second)
			f.reconcile()
			warned := f.job().Status.DeepCopy()
			target := warned.Targets[0]
			if target.Stage != profilingv1alpha1.StageCollecting || target.Reason != "CollectorPendingTimeout" || !strings.Contains(target.Message, tc.reason) || !strings.Contains(target.Message, "waiting details") || target.RetryAt != nil || target.RetryCount != 0 {
				t.Fatalf("pending timeout must only warn: %+v", target)
			}
			want = []string{fmt.Sprintf("Warning CollectorsUpdated profiling round 1 collectors: queued=0 collecting=1 retrying=0 completed=0 failed=0; %s/target stage=Collecting retry=0 collector=%s: CollectorPendingTimeout: collector did not start within 30s; continuing to wait: %s: waiting details", ns, key, tc.reason)}
			if events := recordedEvents(recorder); !reflect.DeepEqual(events, want) {
				t.Fatalf("pending timeout summary=%v want=%v", events, want)
			}
			for i := 0; i < 3; i++ {
				f.now = f.now.Add(time.Hour)
				f.restart()
				if res := f.reconcile(); res.RequeueAfter != pollInterval {
					t.Fatalf("warning must keep polling: %+v", res)
				}
				if !reflect.DeepEqual(f.job().Status, *warned) {
					t.Fatal("repeated warning changed status or retry state")
				}
				if err := f.client.Get(f.ctx, key, p); err != nil || p.UID != uid || !p.DeletionTimestamp.IsZero() || p.Spec.ActiveDeadlineSeconds != nil || len(f.collectors()) != 1 {
					t.Fatalf("pending collector deleted/replaced or given a deadline: %v", err)
				}
			}
			if reasons := eventReasons(recorder); len(reasons) != 0 {
				t.Fatalf("repeated polling/restart emitted duplicate events: %v", reasons)
			}
			f.running(0, f.now)
			f.reconcile()
			if target := f.job().Status.Targets[0]; target.Reason != "CollectorRunning" || target.Stage != profilingv1alpha1.StageCollecting || target.RetryCount != 0 {
				t.Fatalf("late-starting collector did not recover: %+v", target)
			}
			f.complete(0, successResult())
			f.reconcile()
			f.expectPhase(profilingv1alpha1.PhaseCompleted)
		})
	}
}

// A collector pinned to a node cannot start once its target is gone, so polling
// must stop waiting instead of reporting Pending forever.
func TestPendingCollectorFailsWhenTargetInvalidated(t *testing.T) {
	for _, tc := range []struct {
		name, reason string
		invalidate   func(*fixture, *corev1.Pod)
	}{
		{"deleted", "TargetLost", func(f *fixture, pod *corev1.Pod) {
			if err := f.client.Delete(f.ctx, pod); err != nil {
				f.t.Fatal(err)
			}
		}},
		{"rescheduled", "TargetReplaced", func(f *fixture, pod *corev1.Pod) {
			if err := f.client.Delete(f.ctx, pod); err != nil {
				f.t.Fatal(err)
			}
			replacement := newTargetPod(true)
			replacement.UID = "target-uid-2"
			replacement.Spec.NodeName = "node-2"
			if err := f.client.Create(f.ctx, replacement); err != nil {
				f.t.Fatal(err)
			}
			if err := f.client.Status().Update(f.ctx, replacement); err != nil {
				f.t.Fatal(err)
			}
		}},
		{"terminated", "TargetTerminated", func(f *fixture, pod *corev1.Pod) {
			pod.Status.Phase = corev1.PodFailed
			if err := f.client.Status().Update(f.ctx, pod); err != nil {
				f.t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cm := newConfigMap("collector:v1")
			cm.Data[nodeagent.ConfigKey] = "image: collector:v1\nmaxRetries: 3\npendingTimeout: 30s\n"
			target := newTargetPod(true)
			f := newFixture(t, newJob(), target, newPVC(), cm)
			f.start()
			p := f.collectorFor(0)
			p.Status.Phase = corev1.PodPending
			p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: "Unschedulable", Message: "no node matches"}}
			if err := f.client.Status().Update(f.ctx, p); err != nil {
				t.Fatal(err)
			}
			f.reconcile()
			tc.invalidate(f, target)
			for i := 0; i < 5; i++ {
				f.now = f.now.Add(time.Minute)
				f.reconcile()
			}
			j := f.expectPhase(profilingv1alpha1.PhaseFailed)
			if got := j.Status.Targets[0]; got.Stage != profilingv1alpha1.StageFailed || got.Reason != tc.reason || got.RetryCount != 0 {
				t.Fatalf("invalidated target kept waiting: %+v", got)
			}
			if pods := f.collectors(); len(pods) != 0 {
				t.Fatalf("pending collector was not released: %+v", pods)
			}
		})
	}
}

func TestFailedCollectorRecordsFailureBeforeDeletionAndBlocksNewRound(t *testing.T) {
	cm := newConfigMap("collector:v1")
	cm.Data[nodeagent.ConfigKey] = "image: collector:v1\nmaxRetries: 0\npendingTimeout: 1s\n"
	f := newFixture(t, newJob(), newTargetPod(true), newPVC(), cm)
	f.start()
	p := f.collectorFor(0)
	p.Finalizers = []string{"test/hold"}
	if err := f.client.Update(f.ctx, p); err != nil {
		t.Fatal(err)
	}
	f.now = f.now.Add(time.Minute)
	f.reconcile()
	if target := f.job().Status.Targets[0]; target.Stage != profilingv1alpha1.StageCollecting || target.RetryCount != 0 {
		t.Fatalf("pending warning consumed retries: %+v", target)
	}
	// 失败状态立即保存；旧 Pod 未消失仍阻止新轮次启动。
	f.complete(0, collectorprotocol.Failed("StartTimeout", "profiling session did not start"))
	f.reconcile()
	f.reconcile()
	j := f.expectPhase(profilingv1alpha1.PhaseFailed)
	if target := j.Status.Targets[0]; target.Stage != profilingv1alpha1.StageFailed || target.RetryAt != nil || target.RetryCount != 0 || len(f.collectors()) != 1 {
		t.Fatalf("清理不应阻塞失败记录: %+v", target)
	}
	j.Spec.RunIndex++
	if err := f.client.Update(f.ctx, j); err != nil {
		t.Fatal(err)
	}
	f.restart()
	f.reconcile()
	if lastRunIndex(&f.job().Status) != 1 || len(f.collectors()) != 1 {
		t.Fatal("旧 Pod 未删除时启动了新轮次")
	}
	if err := f.client.Get(f.ctx, client.ObjectKeyFromObject(p), p); err != nil {
		t.Fatal(err)
	}
	if p.DeletionTimestamp.IsZero() {
		t.Fatal("failed collector was not deleted")
	}
	p.Finalizers = nil
	if err := f.client.Update(f.ctx, p); err != nil {
		t.Fatal(err)
	}
	f.reconcile()
	if lastRunIndex(&f.job().Status) != 2 || len(f.collectors()) != 0 {
		t.Fatal("旧 Pod 消失后未开始新轮次")
	}
}

func TestAdoptsCollectorAfterStatusWasNotRecorded(t *testing.T) {
	f := baseFixture(t)
	f.start()
	j := f.job()
	name := j.Status.Targets[0].CollectorPodName
	j.Status.Targets[0].Stage = profilingv1alpha1.StageQueued
	j.Status.Targets[0].CollectorPodUID = ""
	j.Status.Targets[0].CollectorCreatedAt = nil
	if err := f.client.Status().Update(f.ctx, j); err != nil {
		t.Fatal(err)
	}
	p := newTargetPod(true)
	if err := f.client.Delete(f.ctx, p); err != nil {
		t.Fatal(err)
	}
	f.reconcile()
	target := f.job().Status.Targets[0]
	if target.Stage != profilingv1alpha1.StageCollecting || target.CollectorPodName != name || len(f.collectors()) != 1 {
		t.Fatalf("existing collector not adopted: %+v", target)
	}
}

func TestInvalidStageTimeoutFailsBeforeCreatingCollectors(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		t.Run(fmt.Sprintf("targetEnabled=%t", enabled), func(t *testing.T) {
			cm := newConfigMap("collector:v1")
			cm.Data[nodeagent.ConfigKey] += "finishTimeout: 2562047h47m16s\n"
			f := newFixture(t, newJob(), newTargetPod(enabled), newPVC(), cm)
			f.reconcile()
			f.reconcile()
			j := f.expectPhase(profilingv1alpha1.PhaseFailed)
			if messageCategory(j.Status.Message) != "InvalidDuration" || j.Status.Targets != nil || len(f.collectors()) != 0 {
				t.Fatalf("stage timeout must fail before target selection and creation: %+v", j.Status)
			}
		})
	}
}

func TestDurationTruncatedToModeLimit(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode profilingv1alpha1.ProfilingMode
		want time.Duration
	}{
		{"full", profilingv1alpha1.ProfilingModeFull, profilingv1alpha1.MaxDurationFull},
		{"light", profilingv1alpha1.ProfilingModeLight, profilingv1alpha1.MaxDurationLight},
	} {
		t.Run(tc.name, func(t *testing.T) {
			j := newJob()
			j.Spec.ProfilingMode = tc.mode
			j.Spec.Duration = metav1.Duration{Duration: time.Hour}
			f := newFixture(t, j, newTargetPod(true), newPVC(), newConfigMap("collector:v1"))
			recorder := record.NewFakeRecorder(100)
			f.r.Recorder = recorder
			f.start()
			run := f.job().Status.RunConfig
			args := strings.Join(f.collectorFor(0).Spec.Containers[0].Args, " ")
			if run.Duration.Duration != tc.want || !strings.Contains(args, "--duration="+tc.want.String()) {
				t.Fatalf("duration not truncated: run=%+v args=%s", run, args)
			}
			if run.ProfilingMode != tc.mode || !strings.Contains(args, "--mode="+tc.name) {
				t.Fatalf("冻结状态与 collector 参数未使用预期模式：run=%+v args=%s", run, args)
			}
			if reasons := eventReasons(recorder); reasons["DurationTruncated"] != 1 {
				t.Fatalf("truncation not reported: %v", reasons)
			}
		})
	}
}

type collectorCleanupErrorClient struct {
	client.Client
	failDelete, failList    bool
	deletes, lists, creates int
}

func (c *collectorCleanupErrorClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	if _, ok := obj.(*corev1.Pod); ok && obj.GetLabels()[nodeagent.LabelComponent] == nodeagent.ComponentCollector {
		c.deletes++
		if c.failDelete {
			return apierrors.NewServiceUnavailable("collector Delete failed")
		}
	}
	return c.Client.Delete(ctx, obj, opts...)
}

func (c *collectorCleanupErrorClient) List(ctx context.Context, obj client.ObjectList, opts ...client.ListOption) error {
	if _, ok := obj.(*corev1.PodList); ok {
		c.lists++
		if c.failList {
			return apierrors.NewServiceUnavailable("collector List failed")
		}
	}
	return c.Client.List(ctx, obj, opts...)
}

func (c *collectorCleanupErrorClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if _, ok := obj.(*corev1.Pod); ok && obj.GetLabels()[nodeagent.LabelComponent] == nodeagent.ComponentCollector {
		c.creates++
	}
	return c.Client.Create(ctx, obj, opts...)
}

func TestCanceledCleanupErrorsPersistAndRecoverAfterRestart(t *testing.T) {
	for _, tc := range []struct {
		name     string
		list     bool
		newRound bool
	}{
		{"Delete failure then resume", false, false},
		{"Delete failure then latest new round", false, true},
		{"List failure then resume", true, false},
		{"List failure then latest new round", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := baseFixture(t)
			f.start()
			old := f.collectorFor(0)
			old.Finalizers = []string{"test/hold"}
			if err := f.client.Update(f.ctx, old); err != nil {
				t.Fatal(err)
			}
			frozen := f.job().Status.RunConfig.DeepCopy()
			c := &collectorCleanupErrorClient{Client: f.client, failDelete: !tc.list, failList: tc.list}
			// The cache hides Pods; cleanup reads and injected List errors must come from the APIReader.
			f.r.Client, f.r.APIReader = collectorBlindCache{c}, c
			f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) { s.Cancel = true })
			if res := f.reconcile(); res.RequeueAfter != time.Millisecond {
				t.Fatalf("cancel did not persist before cleanup: %+v", res)
			}
			j := f.expectCleanup("CleanupPending")
			ended := j.Status.EndTime.DeepCopy()
			if ended == nil || c.deletes != 0 || c.lists != 0 || c.creates != 0 {
				t.Fatalf("first cancel performed cleanup: deletes=%d lists=%d creates=%d status=%+v", c.deletes, c.lists, c.creates, j.Status)
			}
			for i := 0; i < 3; i++ {
				if i == 1 {
					f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) {
						s.Cancel = false
						s.Duration.Duration = 45 * time.Second
						s.ProfilingMode = profilingv1alpha1.ProfilingModeLight
						if tc.newRound {
							s.RunIndex = 2
						}
					})
					if err := f.client.Update(f.ctx, newConfigMap("collector:v2")); err != nil {
						t.Fatal(err)
					}
				}
				if i == 2 && tc.newRound {
					f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) { s.RunIndex = 3 })
				}
				f.now = f.now.Add(time.Minute)
				f.restart()
				res, err := f.r.Reconcile(f.ctx, ctrl.Request{NamespacedName: client.ObjectKey{Namespace: ns, Name: "job"}})
				if !apierrors.IsServiceUnavailable(err) || res != (ctrl.Result{}) {
					t.Fatalf("cleanup failure did not return error for runtime retry: result=%+v error=%v", res, err)
				}
				j = f.expectCleanup("CleanupFailed")
				if !strings.Contains(j.Status.Message, err.Error()) || j.Status.Targets[0].Stage != profilingv1alpha1.StageCanceled || j.Status.Targets[0].Reason != "Canceled" || !reflect.DeepEqual(j.Status.EndTime, ended) || !reflect.DeepEqual(j.Status.RunConfig, frozen) || lastRunIndex(&j.Status) != 1 || len(j.Status.History) != 0 || c.creates != 0 {
					t.Fatalf("cleanup failure was not persisted or started another collection: status=%+v creates=%d error=%v", j.Status, c.creates, err)
				}
				if err := f.client.Get(f.ctx, client.ObjectKeyFromObject(old), old); err != nil || !old.DeletionTimestamp.IsZero() || len(f.collectors()) != 1 {
					t.Fatalf("failed cleanup changed old collector: %v", err)
				}
			}
			if c.lists == 0 || (!tc.list && c.deletes == 0) || (tc.list && c.deletes != 0) {
				t.Fatalf("failure injection missed cleanup: lists=%d deletes=%d", c.lists, c.deletes)
			}
			c.failDelete, c.failList = false, false
			f.restart()
			if res := f.reconcile(); res.RequeueAfter != pollInterval {
				t.Fatalf("restarted cleanup did not poll held Pod: %+v", res)
			}
			j = f.expectCleanup("CleanupPending")
			if !reflect.DeepEqual(j.Status.EndTime, ended) || c.creates != 0 {
				t.Fatal("successful Delete refinalized cancel or started new collector")
			}
			if err := f.client.Get(f.ctx, client.ObjectKeyFromObject(old), old); err != nil || old.DeletionTimestamp.IsZero() {
				t.Fatalf("restart failed to continue collector deletion: %v", err)
			}
			f.restart()
			f.reconcile()
			f.expectCleanup("CleanupPending")
			if c.creates != 0 || len(f.collectors()) != 1 {
				t.Fatal("deleting Pod did not block recovery")
			}
			old.Finalizers = nil
			if err := f.client.Update(f.ctx, old); err != nil {
				t.Fatal(err)
			}
			f.restart()
			f.reconcile()
			j = f.expectPhase(profilingv1alpha1.PhasePending)
			if c.creates != 0 || len(f.collectors()) != 0 || j.Status.Message != "" || j.Status.EndTime != nil {
				t.Fatalf("recovery retained cleanup or created collector too early: %+v", j.Status)
			}
			if tc.newRound {
				if lastRunIndex(&j.Status) != 3 || j.Status.Message != "" || len(j.Status.History) != 1 || j.Status.History[0].Phase != profilingv1alpha1.PhaseCanceled || !reflect.DeepEqual(j.Status.History[0].EndTime, ended) {
					t.Fatalf("cleanup retry consumed the wrong round: %+v", j.Status)
				}
			} else if lastRunIndex(&j.Status) != 1 || !reflect.DeepEqual(j.Status.RunConfig, frozen) || len(j.Status.History) != 0 {
				t.Fatalf("cleanup retry lost frozen resume: %+v", j.Status)
			}
			f.restart()
			f.start()
			args := strings.Join(f.collectorFor(0).Spec.Containers[0].Args, " ")
			wantDuration, wantMode := "--duration=10s", "--mode=full"
			if tc.newRound {
				wantDuration, wantMode = "--duration=45s", "--mode=light"
			}
			if c.creates != 1 || len(f.collectors()) != 1 || (f.collectorFor(0).Name != old.Name) != tc.newRound || f.collectorFor(0).Spec.Containers[0].Image != "collector:v2" || !strings.Contains(args, wantDuration) || !strings.Contains(args, wantMode) {
				t.Fatalf("collection did not recover exactly once with latest config and correct frozen inputs: creates=%d args=%s", c.creates, args)
			}
		})
	}
}

type collectorBlindCache struct{ client.Client }

func (c collectorBlindCache) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if err := c.Client.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	if _, ok := obj.(*corev1.Pod); ok && obj.GetLabels()[nodeagent.LabelComponent] == nodeagent.ComponentCollector {
		return apierrors.NewNotFound(corev1.Resource("pods"), key.Name)
	}
	return nil
}

func (c collectorBlindCache) List(ctx context.Context, obj client.ObjectList, opts ...client.ListOption) error {
	if err := c.Client.List(ctx, obj, opts...); err != nil {
		return err
	}
	if pods, ok := obj.(*corev1.PodList); ok {
		var visible []corev1.Pod
		for _, pod := range pods.Items {
			if pod.Labels[nodeagent.LabelComponent] != nodeagent.ComponentCollector {
				visible = append(visible, pod)
			}
		}
		pods.Items = visible
	}
	return nil
}

type collectorListCounter struct {
	client.Reader
	lists int
}

func (c *collectorListCounter) List(ctx context.Context, obj client.ObjectList, opts ...client.ListOption) error {
	if _, ok := obj.(*corev1.PodList); ok {
		lo := (&client.ListOptions{}).ApplyOptions(opts)
		if lo.LabelSelector != nil && lo.LabelSelector.Matches(labels.Set{nodeagent.LabelComponent: nodeagent.ComponentCollector}) {
			c.lists++
		}
	}
	return c.Reader.List(ctx, obj, opts...)
}

func TestQueuedTargetsShareOneCollectorList(t *testing.T) {
	p := newTargetPod(true)
	p.Name, p.UID, p.Spec.NodeName = "target-2", "target-2-uid", "node-2"
	f := newFixture(t, newJob(), newTargetPod(true), p, newPVC(), newConfigMap("collector:v1"))
	counter := &collectorListCounter{Reader: f.client}
	f.r.APIReader = counter
	for i := 0; i < 8 && len(f.collectors()) < 2; i++ {
		before := counter.lists
		f.reconcile()
		if n := counter.lists - before; n > 1 {
			t.Fatalf("reconcile listed collector Pods %d times", n)
		}
	}
	if len(f.collectors()) != 2 || counter.lists == 0 {
		t.Fatalf("collectors=%d lists=%d", len(f.collectors()), counter.lists)
	}
}

func TestCollectorCacheLagCannotReleaseCleanupBarrier(t *testing.T) {
	f := baseFixture(t)
	f.r.Client = collectorBlindCache{f.client}
	f.start()
	f.reconcile()
	if f.job().Status.Targets[0].Stage != profilingv1alpha1.StageCollecting {
		t.Fatal("cached absence caused collector loss")
	}
	p := f.collectorFor(0)
	p.Finalizers = []string{"test/hold"}
	if err := f.client.Update(f.ctx, p); err != nil {
		t.Fatal(err)
	}
	f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) { s.Cancel = true })
	f.reconcile()
	j := f.expectCleanup("CleanupPending")
	ended := j.Status.EndTime.DeepCopy()
	if err := f.client.Get(f.ctx, client.ObjectKeyFromObject(p), p); err != nil || !p.DeletionTimestamp.IsZero() {
		t.Fatalf("first cancel deleted authoritative collector: %v", err)
	}
	for i := 0; i < 3; i++ {
		f.now = f.now.Add(time.Minute)
		f.restart()
		if res := f.reconcile(); res.RequeueAfter != pollInterval {
			t.Fatalf("hidden deleting collector requeue=%+v", res)
		}
		j = f.expectCleanup("CleanupPending")
		if !reflect.DeepEqual(j.Status.EndTime, ended) || j.Status.Targets[0].Stage != profilingv1alpha1.StageCanceled || len(f.collectors()) != 1 {
			t.Fatalf("cached absence released cleanup barrier or refinalized cancel: %+v", j.Status)
		}
	}
	if err := f.client.Get(f.ctx, client.ObjectKeyFromObject(p), p); err != nil || p.DeletionTimestamp.IsZero() {
		t.Fatalf("authoritative collector not deleted: %v", err)
	}
	p.Finalizers = nil
	if err := f.client.Update(f.ctx, p); err != nil {
		t.Fatal(err)
	}
	f.restart()
	j = f.waitForCanceledCleanup()
	if !reflect.DeepEqual(j.Status.EndTime, ended) {
		t.Fatal("cleanup completion changed cancellation end time")
	}
}

func TestTerminalJobCleansLateCollector(t *testing.T) {
	f := baseFixture(t)
	f.start()
	j := f.job()
	j.Status.Phase = profilingv1alpha1.PhaseFailed
	if err := f.client.Status().Update(f.ctx, j); err != nil {
		t.Fatal(err)
	}
	f.reconcile()
	f.expectPhase(profilingv1alpha1.PhaseFailed)
	if len(f.collectors()) != 0 {
		t.Fatal("terminal job ignored live collector")
	}
}

func TestDeletionUpdateIsReconciledWithoutGenerationChange(t *testing.T) {
	old := newJob()
	updated := old.DeepCopy()
	now := metav1.Now()
	updated.DeletionTimestamp = &now
	if !jobUpdateRelevant(event.UpdateEvent{ObjectOld: old, ObjectNew: updated}) {
		t.Fatal("deletion timestamp transition filtered out")
	}
	updated = old.DeepCopy()
	updated.Status.Phase = profilingv1alpha1.PhaseCompleted
	if jobUpdateRelevant(event.UpdateEvent{ObjectOld: old, ObjectNew: updated}) {
		t.Fatal("status-only update not filtered")
	}
	updated.Generation++
	if !jobUpdateRelevant(event.UpdateEvent{ObjectOld: old, ObjectNew: updated}) {
		t.Fatal("spec generation transition filtered out")
	}
}

type statusWriteFailureClient struct{ client.Client }

func (c *statusWriteFailureClient) Status() client.SubResourceWriter {
	return &statusWriteFailureWriter{SubResourceWriter: c.Client.Status()}
}

type statusWriteFailureWriter struct{ client.SubResourceWriter }

func (w *statusWriteFailureWriter) Update(_ context.Context, _ client.Object, _ ...client.SubResourceUpdateOption) error {
	return apierrors.NewServiceUnavailable("status persistence failed")
}

func TestStatusWriteFailureDoesNotCreateCollector(t *testing.T) {
	for _, tc := range []struct {
		name       string
		reconciles int
	}{
		{"round config", 0},
		{"frozen targets", 1},
		{"collector identity", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := baseFixture(t)
			for i := 0; i < tc.reconciles; i++ {
				f.reconcile()
			}
			before := f.job().Status.DeepCopy()
			f.r.Client = &statusWriteFailureClient{Client: f.client}
			_, err := f.r.Reconcile(f.ctx, ctrl.Request{NamespacedName: client.ObjectKey{Namespace: ns, Name: "job"}})
			if !apierrors.IsServiceUnavailable(err) || len(f.collectors()) != 0 || !reflect.DeepEqual(before, f.job().Status.DeepCopy()) {
				t.Fatalf("failed status write started collection or changed persisted state: error=%v status=%+v", err, f.job().Status)
			}
			f.r.Client = f.client
			f.start()
			if len(f.collectors()) != 1 || lastRunIndex(&f.job().Status) != 1 {
				t.Fatal("collection did not recover after status persistence succeeded")
			}
		})
	}
}

func TestCollectorsUpdatedAfterStatusWriteRecovery(t *testing.T) {
	for _, tc := range []struct {
		name      string
		prepare   func(*fixture)
		stage     profilingv1alpha1.TargetStage
		summary   string
		detail    string
		lifecycle []string
	}{
		{
			name: "creation",
			prepare: func(f *fixture) {
				f.reconcile()
				f.reconcile()
				f.reconcile()
			},
			stage:   profilingv1alpha1.StageCollecting,
			summary: "queued=0 collecting=1 retrying=0 completed=0 failed=0",
		},
		{
			name: "running",
			prepare: func(f *fixture) {
				f.start()
				f.running(0, f.now)
			},
			stage:     profilingv1alpha1.StageCollecting,
			summary:   "queued=0 collecting=1 retrying=0 completed=0 failed=0",
			detail:    ": CollectorRunning: collector Pod inference/%s is running",
			lifecycle: []string{"Normal CollectionStarted profiling round 1 started writing to /inference/job/run-1"},
		},
		{
			name: "completion",
			prepare: func(f *fixture) {
				f.start()
				f.running(0, f.now)
				f.reconcile()
				res := successResult()
				res.Containers[0].OutputFiles = []string{"result.log"}
				f.complete(0, res)
			},
			stage:     profilingv1alpha1.StageCompleted,
			summary:   "queued=0 collecting=0 retrying=0 completed=1 failed=0",
			detail:    " elapsed=1m0s outputs=/inference/job/run-1/result.log",
			lifecycle: []string{"Normal Completed profiling round 1 Completed: queued=0 collecting=0 retrying=0 completed=1 failed=0"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := baseFixture(t)
			tc.prepare(f)
			before := f.job().Status.DeepCopy()
			collectorName := before.Targets[0].CollectorPodName
			recorder := record.NewFakeRecorder(100)
			f.r.Recorder = recorder
			f.r.Client = &statusWriteFailureClient{Client: f.client}
			_, err := f.r.Reconcile(f.ctx, ctrl.Request{NamespacedName: client.ObjectKey{Namespace: ns, Name: "job"}})
			if !apierrors.IsServiceUnavailable(err) || !reflect.DeepEqual(f.job().Status.DeepCopy(), before) || len(f.collectors()) != 1 {
				t.Fatalf("failed status write must not persist target progress: error=%v status=%+v", err, f.job().Status)
			}
			if events := recordedEvents(recorder); !reflect.DeepEqual(events, tc.lifecycle) {
				t.Fatalf("failed status write emitted a collector summary: %v want only lifecycle %v", events, tc.lifecycle)
			}
			f.r.Client = f.client
			f.restart()
			f.reconcile()
			j := f.job()
			if target := j.Status.Targets[0]; target.Stage != tc.stage || target.RetryCount != 0 || target.CollectorPodName != collectorName || len(f.collectors()) != 1 {
				t.Fatalf("recovery duplicated collector or lost progress: %+v", j.Status)
			}
			detail := tc.detail
			if tc.name == "running" {
				detail = fmt.Sprintf(detail, collectorName)
			}
			want := append([]string(nil), tc.lifecycle...)
			want = append(want, fmt.Sprintf("Normal CollectorsUpdated profiling round 1 collectors: %s; inference/target stage=%s retry=0 collector=inference/%s%s", tc.summary, tc.stage, collectorName, detail))
			if events := recordedEvents(recorder); !reflect.DeepEqual(events, want) {
				t.Fatalf("successful retry must emit exactly one collector summary: %v want %v", events, want)
			}
		})
	}
}

func TestDifferentTargetsCanCollectConcurrently(t *testing.T) {
	a := newJob()
	b := namedJob("b", "b-uid", time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC))
	a.Spec.TargetSelector = profilingv1alpha1.TargetSelector{PodName: "target", PodNamespace: ns}
	b.Spec.TargetSelector = profilingv1alpha1.TargetSelector{PodName: "target-2", PodNamespace: ns}
	second := newTargetPod(true)
	second.Name, second.UID = "target-2", "target-2-uid"
	f := newFixture(t, a, b, newTargetPod(true), second, newPVC(), newConfigMap("collector:v1"))
	for i := 0; i < 6; i++ {
		f.reconcile()
		f.reconcileJob("b")
	}
	pods := f.collectors()
	if len(pods) != 2 || f.job().Status.Targets[0].Stage != profilingv1alpha1.StageCollecting || f.namedJob("b").Status.Targets[0].Stage != profilingv1alpha1.StageCollecting {
		t.Fatalf("different target UIDs blocked each other: %+v", pods)
	}
	if pods[0].Annotations[nodeagent.AnnotationTargetUID] == pods[1].Annotations[nodeagent.AnnotationTargetUID] {
		t.Fatal("collectors unexpectedly share a target")
	}
}

func TestSuccessNeedsValidReport(t *testing.T) {
	f := baseFixture(t)
	f.start()
	p := f.collectorFor(0)
	p.Status.Phase = corev1.PodSucceeded
	p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "collector", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, Message: "invalid json"}}}}
	if err := f.client.Status().Update(f.ctx, p); err != nil {
		t.Fatal(err)
	}
	f.reconcile()
	if f.job().Status.Targets[0].Stage == profilingv1alpha1.StageCompleted {
		t.Fatal("invalid report marked success")
	}
}

func TestQueuedTerminalTargetDoesNotBlockRound(t *testing.T) {
	for _, phase := range []corev1.PodPhase{corev1.PodSucceeded, corev1.PodFailed} {
		t.Run(string(phase), func(t *testing.T) {
			peer := newTargetPod(true)
			peer.Name, peer.UID = "target-2", "target-2-uid"
			f := newFixture(t, newJob(), newTargetPod(true), peer, newPVC(), newConfigMap("collector:v1"))
			f.reconcile()
			f.reconcile()
			if len(f.job().Status.Targets) != 2 || len(f.collectors()) != 0 {
				t.Fatal("targets must be frozen before creating collectors")
			}
			pod := newTargetPod(true)
			if err := f.client.Get(f.ctx, client.ObjectKeyFromObject(pod), pod); err != nil {
				t.Fatal(err)
			}
			pod.Status.Phase = phase
			if err := f.client.Status().Update(f.ctx, pod); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 4; i++ {
				f.reconcile()
			}
			j := f.job()
			if j.Status.Targets[0].Stage != profilingv1alpha1.StageFailed || j.Status.Targets[0].Reason != "TargetTerminated" || j.Status.Targets[0].RetryCount != 0 {
				t.Fatalf("terminal target kept waiting: %+v", j.Status.Targets[0])
			}
			if len(f.collectors()) != 1 || j.Status.Targets[1].Stage != profilingv1alpha1.StageCollecting {
				t.Fatal("terminal target blocked peer or created a collector")
			}
			f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) { s.RunIndex = 2 })
			f.complete(1, successResult())
			f.reconcile()
			f.expectPhase(profilingv1alpha1.PhaseFailed)
			f.reconcile()
			f.reconcile()
			if lastRunIndex(&f.job().Status) != 2 {
				t.Fatal("terminal target blocked the next round")
			}
		})
	}
}

func TestTerminalTargetFailsPreflight(t *testing.T) {
	for _, phase := range []corev1.PodPhase{corev1.PodSucceeded, corev1.PodFailed} {
		t.Run(string(phase), func(t *testing.T) {
			pod := newTargetPod(true)
			pod.Status.Phase = phase
			f := newFixture(t, newJob(), pod, newPVC(), newConfigMap("collector:v1"))
			f.reconcile()
			f.reconcile()
			j := f.expectPhase(profilingv1alpha1.PhaseFailed)
			if messageCategory(j.Status.Message) != "TargetTerminated" || len(f.collectors()) != 0 {
				t.Fatalf("terminal target did not fail preflight: %+v", j.Status)
			}
		})
	}
}

func TestQueuedPendingTargetCanRecover(t *testing.T) {
	f := baseFixture(t)
	f.reconcile()
	f.reconcile()
	pod := newTargetPod(true)
	if err := f.client.Get(f.ctx, client.ObjectKeyFromObject(pod), pod); err != nil {
		t.Fatal(err)
	}
	pod.Status.Phase = corev1.PodPending
	if err := f.client.Status().Update(f.ctx, pod); err != nil {
		t.Fatal(err)
	}
	res := f.reconcile()
	if res.RequeueAfter != waitRequeueInterval || f.job().Status.Targets[0].Stage != profilingv1alpha1.StageQueued || len(f.collectors()) != 0 {
		t.Fatal("recoverable target was failed or started prematurely")
	}
	pod.Status.Phase = corev1.PodRunning
	if err := f.client.Status().Update(f.ctx, pod); err != nil {
		t.Fatal(err)
	}
	f.start()
}

func TestInvalidCollectorConfigIsNotFrozen(t *testing.T) {
	for _, invalid := range []string{
		"imagePullPolicy: never\n",
		"resources:\n  requests:\n    cpu: 2\n  limits:\n    cpu: 1\n",
	} {
		t.Run(invalid, func(t *testing.T) {
			cm := newConfigMap("collector:v1")
			cm.Data[nodeagent.ConfigKey] += invalid
			f := newFixture(t, newJob(), newTargetPod(true), newPVC(), cm)
			f.reconcile()
			f.reconcile()
			j := f.expectPhase(profilingv1alpha1.PhasePending)
			wantRun := &profilingv1alpha1.RunConfiguration{Duration: j.Spec.Duration, ProfilingMode: j.Spec.ProfilingMode, ProfilingRules: j.Spec.ProfilingRules}
			if !reflect.DeepEqual(j.Status.RunConfig, wantRun) || f.r.lastGood != nil || j.Status.Targets != nil || len(f.collectors()) != 0 {
				t.Fatalf("invalid configuration changed run inputs, became last-good or passed preflight: %+v", j.Status)
			}
			frozen := j.Status.RunConfig.DeepCopy()
			if messageCategory(j.Status.Message) != "CollectorConfigInvalid" {
				t.Fatalf("missing invalid configuration diagnostic: %+v", j.Status)
			}
			if err := f.client.Get(f.ctx, client.ObjectKeyFromObject(cm), cm); err != nil {
				t.Fatal(err)
			}
			cm.Data[nodeagent.ConfigKey] = "image: collector:v2\nimagePullPolicy: Never\n"
			if err := f.client.Update(f.ctx, cm); err != nil {
				t.Fatal(err)
			}
			f.restart()
			f.start()
			if f.collectorFor(0).Spec.Containers[0].Image != "collector:v2" || !reflect.DeepEqual(f.job().Status.RunConfig, frozen) || f.r.lastGood == nil || f.r.lastGood.Image != "collector:v2" {
				t.Fatal("corrected configuration was not used without changing frozen run inputs")
			}
		})
	}
}

func TestInvalidCollectorConfigPreservesLastGood(t *testing.T) {
	f := baseFixture(t)
	cfg, err := f.r.collectorConfig(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	cm := newConfigMap("collector:v2")
	if err := f.client.Get(f.ctx, client.ObjectKeyFromObject(cm), cm); err != nil {
		t.Fatal(err)
	}
	cm.Data[nodeagent.ConfigKey] = "image: collector:v2\nimagePullPolicy: never\n"
	if err := f.client.Update(f.ctx, cm); err != nil {
		t.Fatal(err)
	}
	got, err := f.r.collectorConfig(f.ctx)
	if err != nil || got != cfg || f.r.lastGood != cfg || f.r.lastBadRV == "" {
		t.Fatalf("invalid configuration replaced last-good: %+v, %v", got, err)
	}
	f.start()
	if f.collectorFor(0).Spec.Containers[0].Image != "collector:v1" {
		t.Fatal("new collector did not use the in-memory last-good configuration")
	}
	f.running(0, f.now)
	f.reconcile()
	old := f.collectorFor(0)
	frozen := f.job().Status.RunConfig.DeepCopy()
	cm.Data[nodeagent.ConfigKey] = "image: collector:v3\nmaxRetries: -1\n"
	if err := f.client.Update(f.ctx, cm); err != nil {
		t.Fatal(err)
	}
	f.reconcile()
	j := f.expectPhase(profilingv1alpha1.PhaseRunning)
	if f.r.lastGood != cfg || !reflect.DeepEqual(f.collectorFor(0), old) || !reflect.DeepEqual(j.Status.RunConfig, frozen) {
		t.Fatal("a later invalid configuration stalled or changed the running collector or run inputs")
	}
	cm.Data[nodeagent.ConfigKey] = "image: collector:v3\npollInterval: 7s\n"
	if err := f.client.Update(f.ctx, cm); err != nil {
		t.Fatal(err)
	}
	f.reconcile()
	if f.r.lastGood == nil || f.r.lastGood.Image != "collector:v3" || f.r.lastGood == cfg || !reflect.DeepEqual(f.collectorFor(0), old) || !reflect.DeepEqual(f.job().Status.RunConfig, frozen) {
		t.Fatal("valid correction was not reloaded independently of existing collector identity")
	}
}

type collectorCreateErrorClient struct {
	client.Client
	err   error
	calls int
}

func (c *collectorCreateErrorClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if pod, ok := obj.(*corev1.Pod); ok && pod.Annotations[nodeagent.AnnotationTargetUID] == "target-uid" {
		c.calls++
		return c.err
	}
	return c.Client.Create(ctx, obj, opts...)
}

func TestPermanentCollectorCreateErrorFailsTarget(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"invalid", apierrors.NewInvalid(corev1.SchemeGroupVersion.WithKind("Pod").GroupKind(), "collector", nil)},
		{"bad request", apierrors.NewBadRequest("collector rejected by admission")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peer := newTargetPod(true)
			peer.Name, peer.UID = "target-2", "target-2-uid"
			f := newFixture(t, newJob(), newTargetPod(true), peer, newPVC(), newConfigMap("collector:v1"))
			c := &collectorCreateErrorClient{Client: f.client, err: tc.err}
			f.r.Client = c
			for i := 0; i < 6; i++ {
				f.reconcile()
			}
			j := f.job()
			if j.Status.Targets[0].Stage != profilingv1alpha1.StageFailed || j.Status.Targets[0].Reason != "CollectorCreateRejected" || !strings.Contains(j.Status.Targets[0].Message, tc.err.Error()) || c.calls != 1 {
				t.Fatalf("permanent error did not fail target: %+v, creates=%d", j.Status, c.calls)
			}
			if j.Status.Targets[1].Stage != profilingv1alpha1.StageCollecting || len(f.collectors()) != 1 {
				t.Fatal("permanent create error blocked peer")
			}
			f.complete(1, successResult())
			f.reconcile()
			f.expectPhase(profilingv1alpha1.PhaseFailed)
		})
	}
}

func TestTransientCollectorCreateErrorCanRecover(t *testing.T) {
	f := baseFixture(t)
	f.reconcile()
	f.reconcile()
	f.reconcile()
	c := &collectorCreateErrorClient{Client: f.client, err: apierrors.NewServiceUnavailable("apiserver temporarily unavailable")}
	f.r.Client = c
	_, err := f.r.Reconcile(f.ctx, ctrl.Request{NamespacedName: client.ObjectKey{Namespace: ns, Name: "job"}})
	if !apierrors.IsServiceUnavailable(err) || f.job().Status.Targets[0].Stage != profilingv1alpha1.StageQueued {
		t.Fatalf("transient create error classified as permanent: %v, %+v", err, f.job().Status)
	}
	f.r.Client = f.client
	f.start()
}

func TestTargetContainerIDsSortedAndExcludeCPUSidecar(t *testing.T) {
	pod := newTargetPod(true)
	pod.Spec.Containers[0].Name = "trainer"
	worker := pod.Spec.Containers[0].DeepCopy()
	worker.Name = "worker"
	pod.Spec.Containers = append(pod.Spec.Containers, *worker, corev1.Container{Name: "cpu-sidecar"})
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{
		runningContainer("cpu-sidecar", "containerd://cpu"),
		runningContainer("worker", "containerd://aaa"),
		runningContainer("trainer", "containerd://zzz"),
	}
	f := newFixture(t, newJob(), pod, newPVC(), newConfigMap("collector:v1"))
	f.start()
	target := f.job().Status.Targets[0]
	if strings.Join(target.TargetContainerIds, ",") != "containerd://aaa,containerd://zzz" {
		t.Fatalf("GPU 容器 ID 未排序或包含 CPU sidecar：%+v", target)
	}
	args := strings.Join(f.collectorFor(0).Spec.Containers[0].Args, " ")
	if !strings.Contains(args, "--container-ids=containerd://aaa,containerd://zzz") {
		t.Fatalf("collector received unsorted IDs: %s", args)
	}
}

func TestGPUContainersFollowConfiguredResources(t *testing.T) {
	limits := func(name string) corev1.ResourceRequirements {
		return corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceName(name): resource.MustParse("1")}}
	}
	pod := newTargetPod(true)
	pod.Spec.Containers = nil
	pod.Status.ContainerStatuses = nil
	for i, res := range []string{"aliyun.com/gpu-memory", "aliyun.com/gpu-core", "aliyun.com/gpu-slice", "aliyun.com/gpu", "nvidia.com/gpu", "nvidia.com/mig-1g.10gb", "amd.com/gpu", "example.com/vgpu"} {
		name := fmt.Sprintf("c%d", i)
		pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{Name: name, Resources: limits(res)})
		pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, runningContainer(name, "containerd://"+name))
	}
	pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{Name: "zero", Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("0")}}})
	pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, runningContainer("zero", "containerd://zero"))

	for _, tc := range []struct {
		name, config, want string
	}{
		{"defaults", "", "containerd://c0,containerd://c1,containerd://c2,containerd://c3,containerd://c4,containerd://c5"},
		{"override", "gpuResources:\n  resources.nvidia: example.com/vgpu\n  resources.migPrefix: example.com/mig-\n", "containerd://c0,containerd://c1,containerd://c2,containerd://c3,containerd://c7"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cm := newConfigMap("collector:v1")
			cm.Data[nodeagent.ConfigKey] += tc.config
			f := newFixture(t, newJob(), pod.DeepCopy(), newPVC(), cm)
			f.start()
			if got := strings.Join(f.job().Status.Targets[0].TargetContainerIds, ","); got != tc.want {
				t.Fatalf("GPU containers = %s, want %s", got, tc.want)
			}
		})
	}
}
func TestTargetContainerIDsRefreshBeforeRetry(t *testing.T) {
	pod := newTargetPod(true)
	pod.Spec.Containers[0].Name = "trainer"
	worker := pod.Spec.Containers[0].DeepCopy()
	worker.Name = "worker"
	pod.Spec.Containers = append(pod.Spec.Containers, *worker)
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{
		runningContainer("trainer", "containerd://aaa"),
		runningContainer("worker", "containerd://mmm"),
	}
	f := newFixture(t, newJob(), pod, newPVC(), newConfigMap("collector:v1"))
	f.start()
	if err := f.client.Delete(f.ctx, f.collectorFor(0)); err != nil {
		t.Fatal(err)
	}
	f.reconcile()
	if err := f.client.Get(f.ctx, client.ObjectKeyFromObject(pod), pod); err != nil {
		t.Fatal(err)
	}
	pod.Status.ContainerStatuses[0].ContainerID = "containerd://zzz"
	if err := f.client.Status().Update(f.ctx, pod); err != nil {
		t.Fatal(err)
	}
	f.now = f.now.Add(10 * time.Second)
	f.start()
	target := f.job().Status.Targets[0]
	if target.RetryCount != 1 || strings.Join(target.TargetContainerIds, ",") != "containerd://mmm,containerd://zzz" {
		t.Fatalf("重试前未刷新并排序 GPU 容器 ID：%+v", target)
	}
	args := strings.Join(f.collectorFor(0).Spec.Containers[0].Args, " ")
	if !strings.Contains(args, "--container-ids=containerd://mmm,containerd://zzz") {
		t.Fatalf("collector 收到过期容器 ID：%s", args)
	}
}

// Kubelet keeps the dead instance ID on a GPU container that waits for a restart.
func TestNotRunningGPUContainerBlocksStaleContainerID(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		state      corev1.ContainerState
	}{
		{"crash-loop", "waiting: CrashLoopBackOff", corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}},
		{"exited", "terminated: Error", corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := newTargetPod(true)
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "inference", ContainerID: "containerd://dead", State: tc.state}}
			f := newFixture(t, newJob(), pod, newPVC(), newConfigMap("collector:v1"))
			for i := 0; i < 3; i++ {
				f.reconcile()
			}
			j := f.expectPhase(profilingv1alpha1.PhasePending)
			if len(j.Status.Targets) != 0 || len(f.collectors()) != 0 || messageCategory(j.Status.Message) != "TargetsNotReady" || !strings.Contains(j.Status.Message, tc.want) {
				t.Fatalf("stale container ID was accepted: %+v", j.Status)
			}
			if err := f.client.Get(f.ctx, client.ObjectKeyFromObject(pod), pod); err != nil {
				t.Fatal(err)
			}
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{runningContainer("inference", "containerd://restarted")}
			if err := f.client.Status().Update(f.ctx, pod); err != nil {
				t.Fatal(err)
			}
			f.start()
			if args := strings.Join(f.collectorFor(0).Spec.Containers[0].Args, " "); !strings.Contains(args, "--container-ids=containerd://restarted") {
				t.Fatalf("recovered container did not refresh the ID: %s", args)
			}
		})
	}
}

// The pre-create recheck must not reuse the frozen ID once the container stops running.
func TestNotRunningGPUContainerBlocksCollectorRecreate(t *testing.T) {
	pod := newTargetPod(true)
	f := newFixture(t, newJob(), pod, newPVC(), newConfigMap("collector:v1"))
	f.start()
	if err := f.client.Delete(f.ctx, f.collectorFor(0)); err != nil {
		t.Fatal(err)
	}
	f.reconcile()
	if err := f.client.Get(f.ctx, client.ObjectKeyFromObject(pod), pod); err != nil {
		t.Fatal(err)
	}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "inference", ContainerID: "containerd://gpu-container",
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}}}
	if err := f.client.Status().Update(f.ctx, pod); err != nil {
		t.Fatal(err)
	}
	f.now = f.now.Add(10 * time.Second)
	for i := 0; i < 4; i++ {
		f.reconcile()
	}
	target := f.job().Status.Targets[0]
	if len(f.collectors()) != 0 || target.Stage != profilingv1alpha1.StageQueued || target.Reason != "TargetNotReady" || !strings.Contains(target.Message, "CrashLoopBackOff") {
		t.Fatalf("retry profiled a container that stopped running: %+v", target)
	}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{runningContainer("inference", "containerd://restarted")}
	if err := f.client.Status().Update(f.ctx, pod); err != nil {
		t.Fatal(err)
	}
	f.start()
	if args := strings.Join(f.collectorFor(0).Spec.Containers[0].Args, " "); !strings.Contains(args, "--container-ids=containerd://restarted") {
		t.Fatalf("retry kept the stale container ID: %s", args)
	}
}

func TestForbiddenCollectorCreateErrorHasBoundedRetries(t *testing.T) {
	f := baseFixture(t)
	c := &collectorCreateErrorClient{Client: f.client, err: apierrors.NewForbidden(corev1.Resource("pods"), "collector", nil)}
	f.r.Client = c
	for i := 0; i < 30 && !f.job().Status.Phase.IsTerminal(); i++ {
		f.reconcile()
		f.now = f.now.Add(10 * time.Second)
	}
	j := f.expectPhase(profilingv1alpha1.PhaseFailed)
	if j.Status.Targets[0].Reason != "CollectorCreateRejected" || c.calls != 4 || len(f.collectors()) != 0 {
		t.Fatalf("forbidden creates retried without a bound: %+v, creates=%d", j.Status, c.calls)
	}
}

func TestFirstReconciledJobWinsTarget(t *testing.T) {
	older := namedJob("older", "older-uid", time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC))
	f := newFixture(t, newJob(), older, newTargetPod(true), newPVC(), newConfigMap("collector:v1"))
	f.start()
	winner := f.collectorFor(0)
	winner.Finalizers = []string{"test/hold"}
	if err := f.client.Update(f.ctx, winner); err != nil {
		t.Fatal(err)
	}
	assertQueued := func(stage string) {
		t.Helper()
		for i := 0; i < 6; i++ {
			f.reconcileJob("older")
		}
		o := f.namedJob("older")
		if pods := f.collectors(); len(pods) != 1 || pods[0].Name != winner.Name || len(o.Status.Targets) != 1 || o.Status.Targets[0].Stage != profilingv1alpha1.StageQueued ||
			o.Status.Targets[0].Reason != "Queued" || !strings.Contains(o.Status.Targets[0].Message, winner.Name) {
			t.Fatalf("%s: older job did not wait for the first collector: status=%+v collectors=%+v", stage, o.Status, pods)
		}
	}
	assertQueued("active")
	f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) { s.Cancel = true })
	f.reconcile()
	f.reconcile()
	if err := f.client.Get(f.ctx, client.ObjectKeyFromObject(winner), winner); err != nil || winner.DeletionTimestamp.IsZero() {
		t.Fatalf("canceled collector not deleting: %v", err)
	}
	assertQueued("deleting")
	winner.Finalizers = nil
	if err := f.client.Update(f.ctx, winner); err != nil {
		t.Fatal(err)
	}
	f.waitForCanceledCleanup()
	f.r.Client = collectorBlindCache{f.client}
	for i := 0; i < 6; i++ {
		f.reconcileJob("older")
	}
	if pods := f.collectors(); len(pods) != 1 || pods[0].Labels[nodeagent.LabelJobUID] != "older-uid" || f.namedJob("older").Status.Targets[0].Stage != profilingv1alpha1.StageCollecting {
		t.Fatalf("older job did not start after the collector exited: %+v", pods)
	}
}

func TestMismatchedCollectorIsRecreatedThroughRetry(t *testing.T) {
	for _, adopt := range []bool{false, true} {
		t.Run(map[bool]string{false: "poll", true: "adopt"}[adopt], func(t *testing.T) {
			f := baseFixture(t)
			f.start()
			old := f.collectorFor(0)
			old.Annotations[nodeagent.AnnotationRetryCount] = "9"
			if err := f.client.Update(f.ctx, old); err != nil {
				t.Fatal(err)
			}
			if adopt {
				j := f.job()
				j.Status.Targets[0].Stage, j.Status.Targets[0].CollectorPodUID, j.Status.Targets[0].CollectorCreatedAt = profilingv1alpha1.StageQueued, "", nil
				if err := f.client.Status().Update(f.ctx, j); err != nil {
					t.Fatal(err)
				}
			}
			f.reconcile()
			target := f.job().Status.Targets[0]
			if target.Stage != profilingv1alpha1.StageRetrying || target.Reason != "CollectorMismatch" || target.RetryAt == nil {
				t.Fatalf("mismatch did not enter retry: %+v", target)
			}
			f.reconcile()
			if len(f.collectors()) != 0 {
				t.Fatal("mismatched collector was not deleted")
			}
			f.now = f.now.Add(2 * time.Second)
			for i := 0; i < 8 && f.job().Status.Targets[0].Stage != profilingv1alpha1.StageCollecting; i++ {
				f.reconcile()
			}
			j := f.job()
			pod := f.collectorFor(0)
			args := strings.Join(pod.Spec.Containers[0].Args, " ")
			if j.Status.Targets[0].Stage != profilingv1alpha1.StageCollecting || j.Status.Targets[0].RetryCount != 1 || pod.Name == old.Name || len(f.collectors()) != 1 ||
				pod.Annotations[nodeagent.AnnotationRetryCount] != "1" || !strings.Contains(args, "--output-dir=/data/inference/job/run-1") {
				t.Fatalf("mismatched collector not recreated: status=%+v pod=%+v", j.Status, pod)
			}
		})
	}
}

func TestRunningCollectorSkipsPVCCheck(t *testing.T) {
	f := baseFixture(t)
	f.start()
	f.running(0, f.now)
	if err := f.client.Delete(f.ctx, newPVC()); err != nil {
		t.Fatal(err)
	}
	f.reconcile()
	j := f.expectPhase(profilingv1alpha1.PhaseRunning)
	if j.Status.Targets[0].Stage != profilingv1alpha1.StageCollecting || j.Status.Targets[0].Reason != "CollectorRunning" || messageCategory(j.Status.Message) != "InProgress" {
		t.Fatalf("running collector rechecked the PVC: %+v", j.Status)
	}
}

func TestMultiTargetTimingUsesEarliestCollectorStart(t *testing.T) {
	for _, resumed := range []bool{false, true} {
		for _, observation := range []string{"same-reconcile", "later-reconcile", "terminated", "missing-start-time"} {
			t.Run(fmt.Sprintf("resumed=%t/%s", resumed, observation), func(t *testing.T) {
				peer := newTargetPod(true)
				peer.Name, peer.UID = "target-2", "target-2-uid"
				f := newFixture(t, newJob(), newTargetPod(true), peer, newPVC(), newConfigMap("collector:v1"))
				f.start()
				roundStarted := f.now
				if resumed {
					f.running(0, roundStarted)
					f.running(1, roundStarted)
					f.reconcile()
					f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) { s.Cancel = true })
					f.waitForCanceledCleanup()
					f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) { s.Cancel = false })
					f.now = f.now.Add(5 * time.Minute)
					f.start()
				}
				recorder := record.NewFakeRecorder(100)
				f.r.Recorder = recorder
				earlier, later := f.now.Add(5*time.Second), f.now.Add(20*time.Second)
				f.now = f.now.Add(40 * time.Second)
				f.running(0, later)
				if observation == "missing-start-time" {
					f.running(0, time.Time{})
				}
				if observation == "later-reconcile" || observation == "missing-start-time" {
					f.reconcile()
					f.restart()
					f.r.Recorder = recorder
					f.running(0, later)
				}
				f.running(1, earlier)
				if observation == "terminated" {
					f.complete(1, successResult())
				}
				f.reconcile()
				j := f.job()
				if !timeEqual(j.Status.StartTime, earlier) || j.Status.CollectedDuration != 35000 || j.Status.OutputLocation != "/inference/job/run-1" {
					t.Fatalf("timing depends on observation order: %+v", j.Status)
				}
				f.restart()
				f.r.Recorder = recorder
				f.reconcile()
				if got := eventReasons(recorder)["CollectionStarted"]; got != 1 {
					t.Fatalf("CollectionStarted count=%d want=1", got)
				}
				f.now = f.now.Add(25 * time.Second)
				f.complete(0, successResult())
				if observation != "terminated" {
					f.complete(1, successResult())
				}
				f.reconcile()
				j = f.expectPhase(profilingv1alpha1.PhaseCompleted)
				if !timeEqual(j.Status.StartTime, earlier) || j.Status.CollectedDuration != 60000 {
					t.Fatalf("terminal timing lost earliest start: %+v", j.Status)
				}
				f.now = f.now.Add(time.Hour)
				f.reconcile()
				if got := f.job().Status.CollectedDuration; got != 60000 {
					t.Fatalf("terminal duration changed: %d", got)
				}
			})
		}
	}
}

func TestMultiTargetPreflightWaitsForAllPods(t *testing.T) {
	for _, tc := range []struct {
		name, message string
		mutate        func(*corev1.Pod)
	}{
		{"pending", "not running on a node", func(p *corev1.Pod) { p.Status.Phase = corev1.PodPending }},
		{"unknown", "not running on a node", func(p *corev1.Pod) { p.Status.Phase = corev1.PodUnknown }},
		{"empty-phase", "not running on a node", func(p *corev1.Pod) { p.Status.Phase = "" }},
		{"no-node", "not running on a node", func(p *corev1.Pod) { p.Spec.NodeName = "" }},
		{"deleting", "not running on a node", func(p *corev1.Pod) {
			p.Finalizers = []string{"test/hold"}
			p.DeletionTimestamp = ptr.To(metav1.NewTime(time.Date(2026, 10, 21, 11, 59, 0, 0, time.UTC)))
		}},
		{"gpu-waiting", "waiting: CrashLoopBackOff", func(p *corev1.Pod) {
			p.Status.ContainerStatuses[0].State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}
		}},
		{"gpu-terminated", "terminated: Error", func(p *corev1.Pod) {
			p.Status.ContainerStatuses[0].State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error"}}
		}},
		{"no-container-id", "no runtime container ID yet", func(p *corev1.Pod) { p.Status.ContainerStatuses[0].ContainerID = "" }},
		{"no-gpu-resource", "no container requests a configured GPU resource", func(p *corev1.Pod) {
			p.Spec.Containers[0].Resources = corev1.ResourceRequirements{}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ready, waiting := newTargetPod(true), newTargetPod(true)
			ready.Name, ready.UID = "a-ready", "ready-uid"
			waiting.Name, waiting.UID = "z-waiting", "waiting-uid"
			tc.mutate(waiting)
			f := newFixture(t, newJob(), ready, waiting, newPVC(), newConfigMap("collector:v1"))
			f.reconcile()
			frozen := f.job().Status.RunConfig.DeepCopy()
			for i := 0; i < 3; i++ {
				f.restart()
				res := f.reconcile()
				j := f.expectPhase(profilingv1alpha1.PhasePending)
				if res.RequeueAfter != waitRequeueInterval || j.Status.Targets != nil || len(f.collectors()) != 0 ||
					messageCategory(j.Status.Message) != "TargetsNotReady" || !strings.Contains(j.Status.Message, tc.message) ||
					!reflect.DeepEqual(j.Status.RunConfig, frozen) || j.Status.StartTime != nil || j.Status.EndTime != nil {
					t.Fatalf("partially ready group passed preflight or stopped waiting: %+v, result=%+v", j.Status, res)
				}
				f.now = f.now.Add(time.Hour)
			}
			if err := f.client.Get(f.ctx, client.ObjectKeyFromObject(waiting), waiting); err != nil {
				t.Fatal(err)
			}
			if tc.name == "deleting" || tc.name == "no-node" || tc.name == "no-gpu-resource" {
				if tc.name == "deleting" {
					waiting.Finalizers = nil
					if err := f.client.Update(f.ctx, waiting); err != nil {
						t.Fatal(err)
					}
				} else if err := f.client.Delete(f.ctx, waiting); err != nil {
					t.Fatal(err)
				}
				waiting = newTargetPod(true)
				waiting.Name, waiting.UID = "z-waiting", "recovered-uid"
				if err := f.client.Create(f.ctx, waiting); err != nil {
					t.Fatal(err)
				}
			}
			waiting.Status = newTargetPod(true).Status
			waiting.Status.ContainerStatuses[0].ContainerID = "containerd://recovered"
			if err := f.client.Status().Update(f.ctx, waiting); err != nil {
				t.Fatal(err)
			}
			f.start()
			j := f.expectPhase(profilingv1alpha1.PhaseRunning)
			if len(j.Status.Targets) != 2 || len(f.collectors()) != 2 || j.Status.Targets[1].TargetUID != string(waiting.UID) ||
				!reflect.DeepEqual(j.Status.Targets[1].TargetContainerIds, []string{"containerd://recovered"}) || !reflect.DeepEqual(j.Status.RunConfig, frozen) {
				t.Fatalf("recovered group did not start with current identities: %+v", j.Status)
			}
		})
	}
}

func TestMultiTargetPreflightReadinessPrecedesLaterTerminalPod(t *testing.T) {
	for _, phase := range []corev1.PodPhase{corev1.PodSucceeded, corev1.PodFailed} {
		t.Run(string(phase), func(t *testing.T) {
			waiting, ended := newTargetPod(true), newTargetPod(true)
			waiting.Name, waiting.UID, waiting.Status.Phase = "a-waiting", "waiting-uid", corev1.PodPending
			ended.Name, ended.UID, ended.Status.Phase = "z-ended", "ended-uid", phase
			f := newFixture(t, newJob(), waiting, ended, newPVC(), newConfigMap("collector:v1"))
			f.reconcile()
			res := f.reconcile()
			j := f.expectPhase(profilingv1alpha1.PhasePending)
			if res.RequeueAfter != waitRequeueInterval || j.Status.Targets != nil || len(f.collectors()) != 0 || !strings.Contains(j.Status.Message, "a-waiting is not running") {
				t.Fatalf("preflight did not stop at the first readiness issue: %+v", j.Status)
			}
			waiting.Status.Phase = corev1.PodRunning
			if err := f.client.Status().Update(f.ctx, waiting); err != nil {
				t.Fatal(err)
			}
			f.restart()
			f.reconcile()
			j = f.expectPhase(profilingv1alpha1.PhaseFailed)
			if j.Status.Targets != nil || len(f.collectors()) != 0 || messageCategory(j.Status.Message) != "TargetTerminated" || !strings.Contains(j.Status.Message, "z-ended is "+string(phase)) {
				t.Fatalf("later terminal Pod did not fail the whole preflight: %+v", j.Status)
			}
		})
	}
}

func TestRunningGPUContainersDoNotRequirePodReady(t *testing.T) {
	pod, peer := newTargetPod(true), newTargetPod(true)
	peer.Name, peer.UID = "target-2", "target-2-uid"
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
	pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{Name: "cpu-sidecar"})
	pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, corev1.ContainerStatus{Name: "cpu-sidecar", ContainerID: "containerd://dead-sidecar", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}})
	f := newFixture(t, newJob(), pod, peer, newPVC(), newConfigMap("collector:v1"))
	f.start()
	j := f.expectPhase(profilingv1alpha1.PhaseRunning)
	if len(j.Status.Targets) != 2 || len(f.collectors()) != 2 || !reflect.DeepEqual(j.Status.Targets[0].TargetContainerIds, []string{"containerd://gpu-container"}) || j.Status.StartTime != nil {
		t.Fatalf("CPU sidecar or Pod readiness blocked running GPU containers: %+v", j.Status)
	}
	for i := range j.Status.Targets {
		collectorPod := f.collectorFor(i)
		collectorPod.Status.Phase = corev1.PodPending
		if err := f.client.Status().Update(f.ctx, collectorPod); err != nil {
			t.Fatal(err)
		}
	}
	f.now = f.now.Add(time.Hour)
	f.reconcile()
	j = f.expectPhase(profilingv1alpha1.PhaseRunning)
	if j.Status.StartTime != nil || j.Status.CollectedDuration != 0 || j.Status.EndTime != nil {
		t.Fatalf("Pending collectors must not imply sampling has started: %+v", j.Status)
	}
}

func TestLabelSelectionRechecksBeforeFreezeAndOnNextRound(t *testing.T) {
	f := newFixture(t, newJob(), newPVC(), newConfigMap("collector:v1"))
	f.reconcile()
	f.reconcile()
	j := f.expectPhase(profilingv1alpha1.PhasePending)
	if j.Status.Targets != nil || !strings.Contains(j.Status.Message, "no Pods match") || len(f.collectors()) != 0 {
		t.Fatalf("empty selection did not wait: %+v", j.Status)
	}
	first := newTargetPod(true)
	first.Status.Phase = corev1.PodPending
	if err := f.client.Create(f.ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := f.client.Status().Update(f.ctx, first); err != nil {
		t.Fatal(err)
	}
	f.restart()
	f.reconcile()
	j = f.expectPhase(profilingv1alpha1.PhasePending)
	if j.Status.Targets != nil || !strings.Contains(j.Status.Message, "target is not running") {
		t.Fatalf("new matching Pod was not discovered: %+v", j.Status)
	}
	peer := newTargetPod(true)
	peer.Name, peer.UID = "target-2", "target-2-uid"
	if err := f.client.Create(f.ctx, peer); err != nil {
		t.Fatal(err)
	}
	if err := f.client.Status().Update(f.ctx, peer); err != nil {
		t.Fatal(err)
	}
	first.Status.Phase = corev1.PodRunning
	if err := f.client.Status().Update(f.ctx, first); err != nil {
		t.Fatal(err)
	}
	f.reconcile()
	selected := f.job().Status.Targets
	if len(selected) != 2 || len(f.collectors()) != 0 {
		t.Fatalf("current matching group was not persisted before creation: %+v", selected)
	}
	for _, identity := range []struct{ name, namespace, app string }{
		{"target-3", ns, "inference"},
		{"outside-namespace", "other", "inference"},
		{"outside-labels", ns, "other"},
	} {
		pod := newTargetPod(true)
		pod.Name, pod.Namespace, pod.UID = identity.name, identity.namespace, types.UID(identity.name+"-uid")
		pod.Labels["app"] = identity.app
		if err := f.client.Create(f.ctx, pod); err != nil {
			t.Fatal(err)
		}
		if err := f.client.Status().Update(f.ctx, pod); err != nil {
			t.Fatal(err)
		}
	}
	f.restart()
	f.start()
	j = f.expectPhase(profilingv1alpha1.PhaseRunning)
	if len(j.Status.Targets) != 2 || len(f.collectors()) != 2 || j.Status.Targets[0].TargetUID != selected[0].TargetUID || j.Status.Targets[1].TargetUID != selected[1].TargetUID {
		t.Fatalf("new Pods changed the frozen target set: %+v", j.Status)
	}
	f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) { s.RunIndex++ })
	f.reconcile()
	if lastRunIndex(&f.job().Status) != 1 || len(f.job().Status.Targets) != 2 {
		t.Fatal("increasing runIndex replaced the still-active round")
	}
	f.complete(0, successResult())
	f.complete(1, successResult())
	f.reconcile()
	f.expectPhase(profilingv1alpha1.PhaseCompleted)
	f.restart()
	f.start()
	j = f.expectPhase(profilingv1alpha1.PhaseRunning)
	if lastRunIndex(&j.Status) != 2 || len(j.Status.Targets) != 3 || len(f.collectors()) != 3 || j.Status.Targets[2].TargetUID != "target-3-uid" || len(j.Status.History) != 1 {
		t.Fatalf("next round did not reselect namespace-local matching Pods: %+v", j.Status)
	}
}

func TestQueuedTargetInvalidationDoesNotFailPeer(t *testing.T) {
	for _, invalidation := range []string{"lost", "replaced", "deleting", "Succeeded", "Failed"} {
		t.Run(invalidation, func(t *testing.T) {
			pod, peer := newTargetPod(true), newTargetPod(true)
			peer.Name, peer.UID = "target-2", "target-2-uid"
			f := newFixture(t, newJob(), pod, peer, newPVC(), newConfigMap("collector:v1"))
			f.reconcile()
			f.reconcile()
			reason := "TargetTerminated"
			switch invalidation {
			case "lost", "replaced":
				if err := f.client.Delete(f.ctx, pod); err != nil {
					t.Fatal(err)
				}
				reason = "TargetLost"
				if invalidation == "replaced" {
					reason = "TargetReplaced"
					replacement := newTargetPod(true)
					replacement.UID = "replacement-uid"
					if err := f.client.Create(f.ctx, replacement); err != nil {
						t.Fatal(err)
					}
				}
			case "deleting":
				reason = "TargetReplaced"
				pod.Finalizers = []string{"test/hold"}
				if err := f.client.Update(f.ctx, pod); err != nil {
					t.Fatal(err)
				}
				if err := f.client.Delete(f.ctx, pod); err != nil {
					t.Fatal(err)
				}
			default:
				pod.Status.Phase = corev1.PodPhase(invalidation)
				if err := f.client.Status().Update(f.ctx, pod); err != nil {
					t.Fatal(err)
				}
			}
			f.restart()
			for i := 0; i < 4; i++ {
				f.reconcile()
			}
			j := f.expectPhase(profilingv1alpha1.PhaseRunning)
			if len(f.collectors()) != 1 || j.Status.Targets[0].Stage != profilingv1alpha1.StageFailed || j.Status.Targets[0].Reason != reason ||
				j.Status.Targets[0].TargetUID != "target-uid" || j.Status.Targets[0].RetryCount != 0 || j.Status.Targets[1].Stage != profilingv1alpha1.StageCollecting {
				t.Fatalf("invalid target was adopted, retried or blocked its peer: %+v", j.Status)
			}
			f.complete(1, successResult())
			f.reconcile()
			j = f.expectPhase(profilingv1alpha1.PhaseFailed)
			if j.Status.Targets[1].Stage != profilingv1alpha1.StageCompleted || len(j.Status.Targets[1].OutputFiles) == 0 || messageCategory(j.Status.Message) != "TargetsFailed" {
				t.Fatalf("mixed final result lost the successful peer: %+v", j.Status)
			}
		})
	}
}

func TestRunningCollectorsUseReportsAfterTargetStateChanges(t *testing.T) {
	for _, invalidation := range []string{"Pending", "Succeeded", "Failed", "gpu-waiting", "lost", "replaced"} {
		for _, succeeds := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/succeeds=%t", invalidation, succeeds), func(t *testing.T) {
				pod, peer := newTargetPod(true), newTargetPod(true)
				peer.Name, peer.UID = "target-2", "target-2-uid"
				f := newFixture(t, newJob(), pod, peer, newPVC(), newConfigMap("collector:v1"))
				f.start()
				f.running(0, f.now)
				f.running(1, f.now)
				f.reconcile()
				original := f.collectorFor(0).DeepCopy()
				if invalidation == "lost" || invalidation == "replaced" {
					if err := f.client.Delete(f.ctx, pod); err != nil {
						t.Fatal(err)
					}
					if invalidation == "replaced" {
						replacement := newTargetPod(true)
						replacement.UID = "replacement-uid"
						if err := f.client.Create(f.ctx, replacement); err != nil {
							t.Fatal(err)
						}
					}
				} else {
					if invalidation == "gpu-waiting" {
						pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}
					} else {
						pod.Status.Phase = corev1.PodPhase(invalidation)
					}
					if err := f.client.Status().Update(f.ctx, pod); err != nil {
						t.Fatal(err)
					}
				}
				f.restart()
				f.reconcile()
				j := f.expectPhase(profilingv1alpha1.PhaseRunning)
				if j.Status.Targets[0].Stage != profilingv1alpha1.StageCollecting || j.Status.Targets[0].Reason != "CollectorRunning" ||
					j.Status.Targets[0].TargetUID != "target-uid" || len(f.collectors()) != 2 || !reflect.DeepEqual(f.collectorFor(0), original) {
					t.Fatalf("workload phase replaced the running collector's result: %+v", j.Status)
				}
				result, wantStage, wantPhase := successResult(), profilingv1alpha1.StageCompleted, profilingv1alpha1.PhaseCompleted
				wantDuration := int64(60000)
				if !succeeds {
					result = collectorprotocol.Failed(collectorprotocol.ReasonStreamFailed, "target stream failed")
					result.ElapsedMillis, wantDuration = 321, 321
					wantStage, wantPhase = profilingv1alpha1.StageFailed, profilingv1alpha1.PhaseFailed
				}
				f.complete(0, result)
				f.reconcile()
				j = f.expectPhase(profilingv1alpha1.PhaseRunning)
				if j.Status.Targets[0].Stage != wantStage || j.Status.Targets[0].Reason != result.Reason || j.Status.Targets[0].Message != result.Message ||
					j.Status.Targets[0].CollectedDuration != wantDuration || (len(j.Status.Targets[0].OutputFiles) > 0) != succeeds ||
					j.Status.Targets[1].Stage != profilingv1alpha1.StageCollecting || j.Status.EndTime != nil {
					t.Fatalf("report was lost or prematurely ended the round: %+v", j.Status)
				}
				f.complete(1, successResult())
				f.reconcile()
				f.expectPhase(wantPhase)
			})
		}
	}
}

func TestMultiTargetStageAggregation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		before   profilingv1alpha1.ProfilingPhase
		stages   []profilingv1alpha1.TargetStage
		phase    profilingv1alpha1.ProfilingPhase
		category string
	}{
		{"all-queued-before-start", profilingv1alpha1.PhasePending, []profilingv1alpha1.TargetStage{profilingv1alpha1.StageQueued, profilingv1alpha1.StageQueued}, profilingv1alpha1.PhasePending, "Queued"},
		{"all-queued-after-start", profilingv1alpha1.PhaseRunning, []profilingv1alpha1.TargetStage{profilingv1alpha1.StageQueued, profilingv1alpha1.StageQueued}, profilingv1alpha1.PhaseRunning, "InProgress"},
		{"collecting-queued", profilingv1alpha1.PhasePending, []profilingv1alpha1.TargetStage{profilingv1alpha1.StageCollecting, profilingv1alpha1.StageQueued}, profilingv1alpha1.PhaseRunning, "InProgress"},
		{"retrying-queued", profilingv1alpha1.PhaseRunning, []profilingv1alpha1.TargetStage{profilingv1alpha1.StageRetrying, profilingv1alpha1.StageQueued}, profilingv1alpha1.PhaseRunning, "InProgress"},
		{"completed-queued", profilingv1alpha1.PhaseRunning, []profilingv1alpha1.TargetStage{profilingv1alpha1.StageCompleted, profilingv1alpha1.StageQueued}, profilingv1alpha1.PhaseRunning, "InProgress"},
		{"failed-queued-before-start", profilingv1alpha1.PhasePending, []profilingv1alpha1.TargetStage{profilingv1alpha1.StageFailed, profilingv1alpha1.StageQueued}, profilingv1alpha1.PhaseRunning, "InProgress"},
		{"failed-retrying", profilingv1alpha1.PhaseRunning, []profilingv1alpha1.TargetStage{profilingv1alpha1.StageFailed, profilingv1alpha1.StageRetrying}, profilingv1alpha1.PhaseRunning, "InProgress"},
		{"completed-collecting", profilingv1alpha1.PhaseRunning, []profilingv1alpha1.TargetStage{profilingv1alpha1.StageCompleted, profilingv1alpha1.StageCollecting}, profilingv1alpha1.PhaseRunning, "InProgress"},
		{"all-completed", profilingv1alpha1.PhaseRunning, []profilingv1alpha1.TargetStage{profilingv1alpha1.StageCompleted, profilingv1alpha1.StageCompleted}, profilingv1alpha1.PhaseCompleted, "Completed"},
		{"completed-failed", profilingv1alpha1.PhaseRunning, []profilingv1alpha1.TargetStage{profilingv1alpha1.StageCompleted, profilingv1alpha1.StageFailed}, profilingv1alpha1.PhaseFailed, "TargetsFailed"},
		{"all-failed", profilingv1alpha1.PhasePending, []profilingv1alpha1.TargetStage{profilingv1alpha1.StageFailed, profilingv1alpha1.StageFailed}, profilingv1alpha1.PhaseFailed, "TargetsFailed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := baseFixture(t)
			job := newJob()
			f.r.startNewRound(job)
			job.Status.Phase = tc.before
			for i, stage := range tc.stages {
				job.Status.Targets = append(job.Status.Targets, profilingv1alpha1.TargetStatus{
					TargetNamespace: ns, TargetName: fmt.Sprintf("target-%d", i), Stage: stage,
				})
			}
			f.r.summarize(job)
			if job.Status.Phase != tc.phase || messageCategory(job.Status.Message) != tc.category ||
				(job.Status.EndTime != nil) != tc.phase.IsTerminal() || job.Status.StartTime != nil {
				t.Fatalf("stages=%v yielded wrong CR state: %+v", tc.stages, job.Status)
			}
			for i, stage := range tc.stages {
				if job.Status.Targets[i].Stage != stage {
					t.Fatalf("aggregation changed target %d stage", i)
				}
			}
		})
	}
}

func TestMultiTargetMixedStatesWaitForAllResults(t *testing.T) {
	pods := []*corev1.Pod{newTargetPod(true), newTargetPod(true), newTargetPod(true), newTargetPod(true)}
	objs := []client.Object{newJob(), newPVC(), newConfigMap("collector:v1")}
	for i, pod := range pods {
		pod.Name, pod.UID = fmt.Sprintf("target-%d", i), types.UID(fmt.Sprintf("target-%d-uid", i))
		objs = append(objs, pod)
	}
	f := newFixture(t, objs...)
	f.reconcile()
	f.reconcile()
	pods[3].Status.Phase = corev1.PodPending
	if err := f.client.Status().Update(f.ctx, pods[3]); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		f.reconcile()
	}
	j := f.expectPhase(profilingv1alpha1.PhaseRunning)
	if len(f.collectors()) != 3 || j.Status.Targets[3].Stage != profilingv1alpha1.StageQueued {
		t.Fatalf("queued peer blocked ready targets after selection: %+v", j.Status)
	}
	f.complete(0, successResult())
	f.complete(1, collectorprotocol.Failed(collectorprotocol.ReasonStreamFailed, "broken stream"))
	if err := f.client.Delete(f.ctx, f.collectorFor(2)); err != nil {
		t.Fatal(err)
	}
	f.reconcile()
	j = f.expectPhase(profilingv1alpha1.PhaseRunning)
	want := []profilingv1alpha1.TargetStage{profilingv1alpha1.StageCompleted, profilingv1alpha1.StageFailed, profilingv1alpha1.StageRetrying, profilingv1alpha1.StageQueued}
	for i, stage := range want {
		if j.Status.Targets[i].Stage != stage {
			t.Fatalf("target %d stage=%s want=%s: %+v", i, j.Status.Targets[i].Stage, stage, j.Status)
		}
	}
	files := append([]string(nil), j.Status.Targets[0].OutputFiles...)
	if j.Status.EndTime != nil || len(files) == 0 || !strings.Contains(j.Status.Message, "queued=1 collecting=0 retrying=1 completed=1 failed=1") {
		t.Fatalf("mixed states prematurely finalized or lost progress: %+v", j.Status)
	}
	f.restart()
	f.reconcile()
	f.expectPhase(profilingv1alpha1.PhaseRunning)
	pods[3].Status.Phase = corev1.PodRunning
	if err := f.client.Status().Update(f.ctx, pods[3]); err != nil {
		t.Fatal(err)
	}
	f.now = f.now.Add(10 * time.Second)
	for i := 0; i < 6; i++ {
		f.reconcile()
	}
	j = f.expectPhase(profilingv1alpha1.PhaseRunning)
	if j.Status.Targets[2].Stage != profilingv1alpha1.StageCollecting || j.Status.Targets[2].RetryCount != 1 ||
		j.Status.Targets[3].Stage != profilingv1alpha1.StageCollecting || !reflect.DeepEqual(j.Status.Targets[0].OutputFiles, files) {
		t.Fatalf("retry or queued recovery disturbed terminal targets: %+v", j.Status)
	}
	f.complete(2, successResult())
	f.reconcile()
	f.expectPhase(profilingv1alpha1.PhaseRunning)
	f.complete(3, successResult())
	f.reconcile()
	j = f.expectPhase(profilingv1alpha1.PhaseFailed)
	if !strings.Contains(j.Status.Message, "completed=3 failed=1") || j.Status.EndTime == nil || !reflect.DeepEqual(j.Status.Targets[0].OutputFiles, files) {
		t.Fatalf("final aggregate must preserve successes and one failure: %+v", j.Status)
	}
}

func TestResumeResetsStartTimeAndDuration(t *testing.T) {
	for _, previouslyRunning := range []bool{false, true} {
		t.Run(fmt.Sprintf("previouslyRunning=%t", previouslyRunning), func(t *testing.T) {
			f := baseFixture(t)
			f.start()
			if previouslyRunning {
				f.running(0, f.now)
				f.reconcile()
			}
			f.now = f.now.Add(20 * time.Second)
			f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) { s.Cancel = true })
			canceled := f.waitForCanceledCleanup()
			wantCanceledDuration := int64(0)
			if previouslyRunning {
				wantCanceledDuration = 20000
			}
			if canceled.Status.CollectedDuration != wantCanceledDuration {
				t.Fatalf("取消时的耗时错误：%+v", canceled.Status)
			}
			f.now = f.now.Add(time.Hour)
			f.patchSpec(func(s *profilingv1alpha1.ProfilingJobSpec) { s.Cancel = false })
			f.restart()
			f.reconcile()
			j := f.expectPhase(profilingv1alpha1.PhasePending)
			if j.Status.StartTime != nil || j.Status.EndTime != nil || j.Status.CollectedDuration != 0 {
				t.Fatalf("恢复未清空本次采集的计时：%+v", j.Status)
			}
			recorder := record.NewFakeRecorder(100)
			f.r.Recorder = recorder
			f.start()
			if j = f.job(); j.Status.StartTime != nil || j.Status.CollectedDuration != 0 {
				t.Fatalf("collector 尚未运行就开始计时：%+v", j.Status)
			}
			started := f.now.Add(10 * time.Second)
			f.running(0, started)
			f.now = started.Add(3 * time.Second)
			f.reconcile()
			f.restart()
			f.r.Recorder = recorder
			f.now = started.Add(7 * time.Second)
			f.reconcile()
			j = f.job()
			if !timeEqual(j.Status.StartTime, started) || j.Status.CollectedDuration != 7000 {
				t.Fatalf("恢复后的耗时包含此前采集或等待时间：%+v", j.Status)
			}
			f.complete(0, successResult())
			f.reconcile()
			j = f.expectPhase(profilingv1alpha1.PhaseCompleted)
			if !timeEqual(j.Status.StartTime, started) || !timeEqual(j.Status.EndTime, f.now) || j.Status.CollectedDuration != 7000 {
				t.Fatalf("终态未保留本次采集的计时：%+v", j.Status)
			}
			f.now = f.now.Add(time.Hour)
			f.restart()
			f.r.Recorder = recorder
			f.reconcile()
			if j = f.job(); !timeEqual(j.Status.StartTime, started) || j.Status.CollectedDuration != 7000 {
				t.Fatalf("终态在重启或清理后改变计时：%+v", j.Status)
			}
			if got := eventReasons(recorder)["CollectionStarted"]; got != 1 {
				t.Fatalf("恢复后 CollectionStarted 次数=%d，期望 1", got)
			}
		})
	}
}

func TestCollectionTimingOutputLocationAndEvents(t *testing.T) {
	f := baseFixture(t)
	recorder := record.NewFakeRecorder(100)
	f.r.Recorder = recorder
	f.start()
	j := f.expectPhase(profilingv1alpha1.PhaseRunning)
	if j.Status.StartTime != nil || j.Status.CollectedDuration != 0 || j.Status.OutputLocation != "" {
		t.Fatalf("timing started before the collector ran: %+v", j.Status)
	}
	started := f.now.Add(5 * time.Second)
	f.running(0, started)
	f.now = started.Add(30 * time.Second)
	f.reconcile()
	j = f.job()
	if !timeEqual(j.Status.StartTime, started) || j.Status.CollectedDuration != 30000 || j.Status.OutputLocation != "/inference/job/run-1" {
		t.Fatalf("running timing/location: %+v", j.Status)
	}
	for i := 0; i < 3; i++ {
		f.now = f.now.Add(10 * time.Second)
		f.reconcile()
	}
	if j = f.job(); j.Status.CollectedDuration != 60000 {
		t.Fatalf("collected duration did not follow wall time: %d", j.Status.CollectedDuration)
	}
	f.now = f.now.Add(15 * time.Second)
	f.complete(0, successResult())
	f.reconcile()
	j = f.expectPhase(profilingv1alpha1.PhaseCompleted)
	wantFiles := []string{
		"/inference/job/run-1/inference_target_gpu-container_lastkernels.log",
		"/inference/job/run-1/inference_target_gpu-container_kerneltime.log",
	}
	if j.Status.CollectedDuration != 75000 || j.Status.Targets[0].CollectedDuration != 60000 || !reflect.DeepEqual(j.Status.Targets[0].OutputFiles, wantFiles) {
		t.Fatalf("finished timing/location: %+v", j.Status)
	}
	f.now = f.now.Add(time.Hour)
	f.reconcile()
	if j = f.job(); j.Status.CollectedDuration != 75000 {
		t.Fatalf("terminal collected duration changed: %d", j.Status.CollectedDuration)
	}
	want := map[string]int{"RoundCreated": 1, "TargetsSelected": 1, "CollectorsUpdated": 3, "CollectionStarted": 1, "Completed": 1}
	if got := eventReasons(recorder); !reflect.DeepEqual(got, want) {
		t.Fatalf("events=%v want=%v", got, want)
	}
}
