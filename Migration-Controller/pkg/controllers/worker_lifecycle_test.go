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

package controllers

import (
	"context"
	"testing"
	"time"

	"github.com/container-migration/controller/api/v1beta1"
	"github.com/container-migration/controller/pkg/util"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// TestWorkerConfigHash_StableAndSensitive pins two properties of the
// fingerprint: constant for the same (template, port); changing either
// template or port must change the fingerprint.
func TestWorkerConfigHash_StableAndSensitive(t *testing.T) {
	base := workerConfigHash(testWorkerTemplate, 50051)
	if base != workerConfigHash(testWorkerTemplate, 50051) {
		t.Fatal("hash not stable for identical input")
	}
	if base == workerConfigHash(testWorkerTemplate, 50052) {
		t.Error("hash must change when port changes")
	}
	if base == workerConfigHash(testWorkerTemplate+"\n", 50051) {
		t.Error("hash must change when template changes")
	}
	if len(base) != 16 {
		t.Errorf("hash length = %d, want 16 hex chars (label-safe)", len(base))
	}
}

func TestFormatWorkerBlockedMessage(t *testing.T) {
	cases := []struct {
		name  string
		node  string
		users []string
		want  string
	}{
		{"single", "10.0.0.1", []string{"ns/a"}, "node=10.0.0.1 blockedBy=ns/a"},
		{"exactly3", "n", []string{"a", "b", "c"}, "node=n blockedBy=a,b,c"},
		{"capped", "n", []string{"a", "b", "c", "d", "e"}, "node=n blockedBy=a,b,c (+2 more)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := FormatWorkerBlockedMessage(tc.node, tc.users); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestPodRunningReady(t *testing.T) {
	cases := []struct {
		name string
		pod  *corev1.Pod
		want bool
	}{
		{"running+ready", readyPod("w", "h"), true},
		{"pending", &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodPending}}, false},
		{"running not ready", &corev1.Pod{Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}},
		}}, false},
		{"running no ready cond", &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodRunning}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := podRunningReady(tc.pod); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIsTerminalJobPhase(t *testing.T) {
	terminal := []v1beta1.JobPhase{
		v1beta1.JobPhaseSucceeded, v1beta1.JobPhasePartiallySucceeded,
		v1beta1.JobPhaseFailed, v1beta1.JobPhaseTerminated,
	}
	for _, p := range terminal {
		if !isTerminalJobPhase(p) {
			t.Errorf("phase %q should be terminal", p)
		}
	}
	nonTerminal := []v1beta1.JobPhase{
		v1beta1.JobPhasePending, v1beta1.JobPhaseCheckpointing, v1beta1.JobPhaseRestoring, "",
	}
	for _, p := range nonTerminal {
		if isTerminalJobPhase(p) {
			t.Errorf("phase %q should not be terminal", p)
		}
	}
}

// checkpointCR builds a CR using nodeIP on the checkpoint side; invoked
// controls whether it has been dispatched. phase is checkpointStatus.Phase,
// used to distinguish "checkpointing" from "checkpoint done".
func checkpointCR(uid, node string, phase v1beta1.JobPhase, invoked bool) *v1beta1.ContainerStateMigrationJobRequest {
	cs := &v1beta1.JobCheckpointStatus{SourceNodeIP: node, Phase: phase}
	if invoked {
		cs.Conditions = []metav1.Condition{{
			Type:   v1beta1.RequestConditionCheckpointInvoked,
			Status: metav1.ConditionTrue,
		}}
	}
	return &v1beta1.ContainerStateMigrationJobRequest{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: uid, UID: types.UID(uid)},
		Status:     v1beta1.ContainerStateMigrationJobRequestStatus{Phase: v1beta1.JobPhaseCheckpointing, CheckpointStatus: cs},
	}
}

// restoreCR builds a CR with one replica on nodeIP in the given phase.
func restoreCR(uid, node string, phase v1beta1.JobPhase) *v1beta1.ContainerStateMigrationJobRequest {
	return &v1beta1.ContainerStateMigrationJobRequest{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: uid, UID: types.UID(uid)},
		Status: v1beta1.ContainerStateMigrationJobRequestStatus{
			Phase: v1beta1.JobPhaseRestoring,
			RestoreStatus: &v1beta1.JobRestoreStatus{
				Phase:        v1beta1.JobPhaseRestoring,
				Destinations: []v1beta1.JobDestinationStatus{{NodeIP: node, Phase: phase}},
			},
		},
	}
}

func TestCrUsesNode(t *testing.T) {
	const node = "10.0.0.1"
	cases := []struct {
		name string
		cr   *v1beta1.ContainerStateMigrationJobRequest
		want bool
	}{
		{"checkpointing on node", checkpointCR("a", node, v1beta1.JobPhaseCheckpointing, true), true},
		{"checkpoint not invoked", checkpointCR("a", node, v1beta1.JobPhaseCheckpointing, false), false},
		{"checkpoint done (succeeded)", checkpointCR("a", node, v1beta1.JobPhaseSucceeded, true), false},
		{"checkpoint other node", checkpointCR("a", "10.0.0.2", v1beta1.JobPhaseCheckpointing, true), false},
		{"restore scheduled", restoreCR("a", node, v1beta1.DestPhaseScheduled), true},
		{"restore xpu", restoreCR("a", node, v1beta1.DestPhaseXPURestoring), true},
		{"restore snapshot prepared", restoreCR("a", node, v1beta1.DestPhaseSnapshotPrepared), true},
		{"restore init released", restoreCR("a", node, v1beta1.DestPhaseInitReleased), true},
		{"restore pending (not dispatched)", restoreCR("a", node, v1beta1.DestPhasePending), false},
		{"restore succeeded (done)", restoreCR("a", node, v1beta1.DestPhaseSucceeded), false},
		{"restore other node", restoreCR("a", "10.0.0.2", v1beta1.DestPhaseScheduled), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := crUsesNode(tc.cr, node); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestWorkerInUseRegistry(t *testing.T) {
	sc := newTestContext(t, newFakeClient())
	const node = "10.0.0.1"

	if got := sc.registeredWorkerUsers(node); got != nil {
		t.Fatalf("expected empty registry, got %v", got)
	}
	sc.MarkWorkerInUse(node, "job1")
	sc.MarkWorkerInUse(node, "job2")
	sc.MarkWorkerInUse(node, "job1") // idempotent
	if got := sc.registeredWorkerUsers(node); len(got) != 2 {
		t.Fatalf("expected 2 users, got %v", got)
	}
	sc.ReleaseWorkerUse(node, "job1")
	if got := sc.registeredWorkerUsers(node); len(got) != 1 {
		t.Fatalf("expected 1 user after release, got %v", got)
	}
	sc.ReleaseWorkerUse(node, "job2")
	if got := sc.registeredWorkerUsers(node); got != nil {
		t.Fatalf("expected registry key removed when empty, got %v", got)
	}
	// Empty arguments should be ignored, no empty entries created.
	sc.MarkWorkerInUse("", "job")
	sc.MarkWorkerInUse(node, "")
	if got := sc.registeredWorkerUsers(node); got != nil {
		t.Fatalf("empty args must be ignored, got %v", got)
	}
}

func TestWorkerBusyUsers_UnionAndReverseLeak(t *testing.T) {
	const node = "10.0.0.1"
	fc := newFakeClient()
	// CR busy1 explicitly occupies this node in the CR list.
	fc.crs = append(fc.crs, restoreCR("busy1", node, v1beta1.DestPhaseScheduled))
	// CR term1 is terminal and must not be counted.
	term := restoreCR("term1", node, v1beta1.DestPhaseScheduled)
	term.Status.Phase = v1beta1.JobPhaseSucceeded
	fc.crs = append(fc.crs, term)

	sc := newTestContext(t, fc)
	// The CR of registry entry reg1 does not exist (reverse leak), must be cleaned up and not counted.
	sc.MarkWorkerInUse(node, "reg1")

	users, err := sc.WorkerBusyUsers(context.Background(), node)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(users) != 1 || users[0] != "ns/busy1" {
		t.Fatalf("expected [ns/busy1], got %v", users)
	}
	// The reverse-leak entry should have been cleaned up.
	if got := sc.registeredWorkerUsers(node); got != nil {
		t.Errorf("stale registry entry should be dropped, got %v", got)
	}
}

func TestWorkerBusyUsers_RegistryCoversUnpersistedWindow(t *testing.T) {
	const node = "10.0.0.1"
	fc := newFakeClient()
	// The CR exists and is non-terminal, but its status has not yet reflected occupying this node (just dispatched, not persisted).
	fc.crs = append(fc.crs, restoreCR("job1", node, v1beta1.DestPhasePending))

	sc := newTestContext(t, fc)
	sc.MarkWorkerInUse(node, "job1")

	users, err := sc.WorkerBusyUsers(context.Background(), node)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(users) != 1 || users[0] != "ns/job1" {
		t.Fatalf("registry should cover unpersisted window, got %v", users)
	}
	// Registry entries of non-terminal CRs must be kept.
	if got := sc.registeredWorkerUsers(node); len(got) != 1 {
		t.Errorf("registry entry for live CR must be kept, got %v", got)
	}
}

func TestWorkerBusyUsers_CacheThrottle(t *testing.T) {
	const node = "10.0.0.1"
	fc := newFakeClient()
	fc.crs = append(fc.crs, restoreCR("busy1", node, v1beta1.DestPhaseScheduled))
	sc := newTestContext(t, fc)

	if users, _ := sc.WorkerBusyUsers(context.Background(), node); len(users) != 1 {
		t.Fatalf("first scan expected busy, got %v", users)
	}
	// Clear the underlying data; within the unexpired cache the previous verdict should still be returned.
	fc.crs = nil
	if users, _ := sc.WorkerBusyUsers(context.Background(), node); len(users) != 1 {
		t.Errorf("cached verdict should persist within poll interval, got %v", users)
	}
	// Manually expire the cache; a rescan should find it idle.
	sc.invalidateDrainVerdict(node)
	if users, _ := sc.WorkerBusyUsers(context.Background(), node); len(users) != 0 {
		t.Errorf("after cache invalidation should rescan idle, got %v", users)
	}
}

func TestWorkerBusyUsers_ListErrorPropagates(t *testing.T) {
	fc := newFakeClient()
	fc.listErr = context.DeadlineExceeded
	sc := newTestContext(t, fc)
	if _, err := sc.WorkerBusyUsers(context.Background(), "n"); err == nil {
		t.Fatal("expected error when List fails")
	}
}

// TestWorkerBusyUsers_RestoreOnlyDoesNotSelfBlock regression: a restore-only
// CR inherits a Succeeded checkpointStatus whose SourceNodeIP equals the
// restore target node in single-machine scenarios. This CR must not be judged
// as occupying that node (otherwise it would block its own restore and
// self-deadlock).
func TestWorkerBusyUsers_RestoreOnlyDoesNotSelfBlock(t *testing.T) {
	const node = "33.254.24.200"
	fc := newFakeClient()
	// Inherited checkpointStatus: Succeeded + SourceNodeIP==node +
	// CheckpointInvoked=True, the restore replica still Pending (not
	// dispatched).
	cr := restoreCR("restore-only", node, v1beta1.DestPhasePending)
	cr.Status.CheckpointStatus = &v1beta1.JobCheckpointStatus{
		Phase:        v1beta1.JobPhaseSucceeded,
		SourceNodeIP: node,
		Conditions: []metav1.Condition{{
			Type:   v1beta1.RequestConditionCheckpointInvoked,
			Status: metav1.ConditionTrue,
		}},
	}
	fc.crs = append(fc.crs, cr)
	sc := newTestContext(t, fc)

	users, err := sc.WorkerBusyUsers(context.Background(), node)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(users) != 0 {
		t.Fatalf("restore-only CR must not be judged busy on its own restore target node, got %v", users)
	}
}

func TestEnsureWorkerUpToDate_CreatesWhenAbsent(t *testing.T) {
	fc := newFakeClient()
	putWorkerConfigMap(fc, "50051")
	sc := newTestContext(t, fc)

	res := sc.EnsureWorkerUpToDate(context.Background(), "node-a", "10.0.0.1", "job1", true)
	if res.State != WorkerWaiting {
		t.Fatalf("expected Waiting, got %v (%s)", res.State, res.Reason)
	}
	if _, ok := fc.pods[nsName(metav1.NamespaceSystem, util.GenerateMigrationWorkerName("node-a"))]; !ok {
		t.Fatal("worker pod should have been created")
	}
}

func TestEnsureWorkerUpToDate_ReadyWhenHashMatches(t *testing.T) {
	fc := newFakeClient()
	putWorkerConfigMap(fc, "50051")
	hash := workerConfigHash(testWorkerTemplate, 50051)
	fc.pods[nsName(metav1.NamespaceSystem, util.GenerateMigrationWorkerName("node-a"))] =
		readyPod(util.GenerateMigrationWorkerName("node-a"), hash)
	sc := newTestContext(t, fc)

	res := sc.EnsureWorkerUpToDate(context.Background(), "node-a", "10.0.0.1", "job1", true)
	if res.State != WorkerReady {
		t.Fatalf("expected Ready, got %v (%s)", res.State, res.Reason)
	}
}

func TestEnsureWorkerUpToDate_WaitsWhenHashMatchesNotReady(t *testing.T) {
	fc := newFakeClient()
	putWorkerConfigMap(fc, "50051")
	hash := workerConfigHash(testWorkerTemplate, 50051)
	p := readyPod(util.GenerateMigrationWorkerName("node-a"), hash)
	p.Status.Phase = corev1.PodPending
	fc.pods[nsName(metav1.NamespaceSystem, util.GenerateMigrationWorkerName("node-a"))] = p
	sc := newTestContext(t, fc)

	res := sc.EnsureWorkerUpToDate(context.Background(), "node-a", "10.0.0.1", "job1", true)
	if res.State != WorkerWaiting || res.Blocked {
		t.Fatalf("expected non-blocked Waiting, got state=%v blocked=%v", res.State, res.Blocked)
	}
}

func TestEnsureWorkerUpToDate_ReplacesWhenDriftAndIdle(t *testing.T) {
	fc := newFakeClient()
	putWorkerConfigMap(fc, "50051")
	podName := util.GenerateMigrationWorkerName("node-a")
	fc.pods[nsName(metav1.NamespaceSystem, podName)] = readyPod(podName, "stalehash")
	sc := newTestContext(t, fc)

	res := sc.EnsureWorkerUpToDate(context.Background(), "node-a", "10.0.0.1", "job1", true)
	if res.State != WorkerWaiting || res.Blocked {
		t.Fatalf("expected non-blocked Waiting (replacing), got state=%v blocked=%v", res.State, res.Blocked)
	}
	if _, ok := fc.pods[nsName(metav1.NamespaceSystem, podName)]; ok {
		t.Fatal("stale worker pod should have been deleted")
	}
}

func TestEnsureWorkerUpToDate_BlocksWhenDriftAndBusy(t *testing.T) {
	fc := newFakeClient()
	putWorkerConfigMap(fc, "50051")
	podName := util.GenerateMigrationWorkerName("node-a")
	fc.pods[nsName(metav1.NamespaceSystem, podName)] = readyPod(podName, "stalehash")
	// This node has an in-flight restore task.
	fc.crs = append(fc.crs, restoreCR("busy1", "10.0.0.1", v1beta1.DestPhaseScheduled))
	sc := newTestContext(t, fc)

	res := sc.EnsureWorkerUpToDate(context.Background(), "node-a", "10.0.0.1", "job1", true)
	if res.State != WorkerWaiting || !res.Blocked {
		t.Fatalf("expected blocked Waiting, got state=%v blocked=%v", res.State, res.Blocked)
	}
	if len(res.Users) != 1 || res.Users[0] != "ns/busy1" {
		t.Errorf("expected blockedBy [ns/busy1], got %v", res.Users)
	}
	// Never delete the old worker while busy.
	if _, ok := fc.pods[nsName(metav1.NamespaceSystem, podName)]; !ok {
		t.Fatal("busy node worker must NOT be deleted")
	}
}

func TestEnsureWorkerUpToDate_NoReplaceWhenNotAllowed(t *testing.T) {
	fc := newFakeClient()
	putWorkerConfigMap(fc, "50051")
	podName := util.GenerateMigrationWorkerName("node-a")
	fc.pods[nsName(metav1.NamespaceSystem, podName)] = readyPod(podName, "stalehash")
	sc := newTestContext(t, fc)

	// allowReplace=false: no replacement even on drift; the worker is ready so let through.
	res := sc.EnsureWorkerUpToDate(context.Background(), "node-a", "10.0.0.1", "job1", false)
	if res.State != WorkerReady {
		t.Fatalf("expected Ready (replace deferred), got %v (%s)", res.State, res.Reason)
	}
	if _, ok := fc.pods[nsName(metav1.NamespaceSystem, podName)]; !ok {
		t.Fatal("worker must not be deleted when replace not allowed")
	}
}

func TestEnsureWorkerUpToDate_WaitsWhenTerminating(t *testing.T) {
	fc := newFakeClient()
	putWorkerConfigMap(fc, "50051")
	podName := util.GenerateMigrationWorkerName("node-a")
	p := readyPod(podName, "stalehash")
	now := metav1.NewTime(time.Now())
	p.DeletionTimestamp = &now
	fc.pods[nsName(metav1.NamespaceSystem, podName)] = p
	sc := newTestContext(t, fc)

	res := sc.EnsureWorkerUpToDate(context.Background(), "node-a", "10.0.0.1", "job1", true)
	if res.State != WorkerWaiting || res.Blocked {
		t.Fatalf("expected non-blocked Waiting (terminating), got state=%v blocked=%v", res.State, res.Blocked)
	}
	// Never force-delete a terminating pod.
	if _, ok := fc.pods[nsName(metav1.NamespaceSystem, podName)]; !ok {
		t.Fatal("terminating pod must not be force deleted")
	}
}

func TestEnsureWorkerUpToDate_FailsOnInvalidConfig(t *testing.T) {
	fc := newFakeClient()
	// Missing template -> SyncConfigMap fails -> buildWorkerPod fails.
	fc.cms[nsName(metav1.NamespaceSystem, "lj-runtimekit-migration-controller-config")] = &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: metav1.NamespaceSystem, Name: "lj-runtimekit-migration-controller-config"},
		Data:       map[string]string{"grpcPort": "50051"},
	}
	sc := newTestContext(t, fc)

	res := sc.EnsureWorkerUpToDate(context.Background(), "node-a", "10.0.0.1", "job1", true)
	if res.State != WorkerFailed || res.Err == nil {
		t.Fatalf("expected Failed with error, got state=%v err=%v", res.State, res.Err)
	}
}
