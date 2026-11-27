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
	"testing"
	"time"

	"github.com/container-migration/controller/api/v1beta1"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestInitContainerState covers the four state parsing cases of initContainerState and restartCount passthrough.
func TestInitContainerState(t *testing.T) {
	now := metav1.Now()
	code := int32(137)
	pod := &corev1.Pod{Status: corev1.PodStatus{InitContainerStatuses: []corev1.ContainerStatus{
		{Name: "other-init", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: now}}, RestartCount: 0},
		{Name: InitContainerName, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: now}}, RestartCount: 2},
	}}}

	running, exitCode, restartCount := initContainerState(pod, InitContainerName)
	if !running || exitCode != nil || restartCount != 2 {
		t.Fatalf("Running case: got running=%v exitCode=%v restartCount=%d", running, exitCode, restartCount)
	}

	pod.Status.InitContainerStatuses[1].State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: code, FinishedAt: now}}
	pod.Status.InitContainerStatuses[1].RestartCount = 3
	running, exitCode, restartCount = initContainerState(pod, InitContainerName)
	if running || exitCode == nil || *exitCode != 137 || restartCount != 3 {
		t.Fatalf("Terminated case: got running=%v exitCode=%v restartCount=%d", running, exitCode, restartCount)
	}

	// Waiting (no Running/Terminated state) + backoff restartCount after a crash.
	pod.Status.InitContainerStatuses[1].State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}
	pod.Status.InitContainerStatuses[1].RestartCount = 4
	running, exitCode, restartCount = initContainerState(pod, InitContainerName)
	if running || exitCode != nil || restartCount != 4 {
		t.Fatalf("Waiting case: got running=%v exitCode=%v restartCount=%d", running, exitCode, restartCount)
	}

	// The container has not appeared in status yet (not started).
	pod.Status.InitContainerStatuses = nil
	running, exitCode, restartCount = initContainerState(pod, InitContainerName)
	if running || exitCode != nil || restartCount != 0 {
		t.Fatalf("Absent case: got running=%v exitCode=%v restartCount=%d", running, exitCode, restartCount)
	}
}

// TestSyncInitRestartStatus covers the create/update/History-only-grows gauge of restartCount sync.
func TestSyncInitRestartStatus(t *testing.T) {
	dest := &v1beta1.JobDestinationStatus{}

	if added := syncInitRestartStatus(dest, "InitContainerFailed", "msg", 0, 2); !added {
		t.Fatal("first sync should add history")
	}
	entry := retryStatusOf(dest, v1beta1.RetryPhaseInitRestart)
	if entry == nil || entry.Attempts != 1 || entry.MaxRetry != 2 || len(entry.History) != 1 {
		t.Fatalf("first sync entry wrong: %+v", entry)
	}

	// Re-sync with the same restartCount: attempts synced, History not appended.
	if added := syncInitRestartStatus(dest, "InitContainerFailed", "msg", 0, 2); added {
		t.Fatal("same restartCount should not add history")
	}
	entry = retryStatusOf(dest, v1beta1.RetryPhaseInitRestart)
	if entry == nil || len(entry.History) != 1 {
		t.Fatalf("history should stay at 1: %+v", entry)
	}

	// restartCount grows to 3: History backfilled to 4 entries, attempts=4.
	if added := syncInitRestartStatus(dest, "InitContainerCrashLoop", "crash", 3, 2); !added {
		t.Fatal("restartCount growth should add history")
	}
	entry = retryStatusOf(dest, v1beta1.RetryPhaseInitRestart)
	if entry == nil || entry.Attempts != 4 || len(entry.History) != 4 || entry.History[3].Reason != "InitContainerCrashLoop" {
		t.Fatalf("growth entry wrong: %+v", entry)
	}
}

// TestDestInInitReleaseBackoff covers the exec failure backoff gate: not
// effective outside SnapshotPrepared, not effective without a record,
// effective within the 3s window, expired outside it.
func TestDestInInitReleaseBackoff(t *testing.T) {
	dest := &v1beta1.JobDestinationStatus{Phase: v1beta1.DestPhaseSnapshotPrepared}
	if destInInitReleaseBackoff(dest) {
		t.Fatal("no InitRelease entry should not be in backoff")
	}

	recordAttempt(dest, v1beta1.RetryPhaseInitRelease, "InitReleaseFailed", "exec failed", initReleaseMaxRetry)
	if !destInInitReleaseBackoff(dest) {
		t.Fatal("fresh exec failure should enter backoff")
	}

	entry := retryStatusOf(dest, v1beta1.RetryPhaseInitRelease)
	entry.History[0].Time = metav1.NewTime(time.Now().Add(-time.Hour))
	if destInInitReleaseBackoff(dest) {
		t.Fatal("backoff window passed should not be in backoff")
	}

	// Not SnapshotPrepared (e.g. already InitReleased / ColdStarting): not effective.
	dest2 := &v1beta1.JobDestinationStatus{Phase: v1beta1.DestPhaseInitReleased}
	recordAttempt(dest2, v1beta1.RetryPhaseInitRelease, "InitReleaseFailed", "exec failed", initReleaseMaxRetry)
	if dest2.Phase == v1beta1.DestPhaseSnapshotPrepared {
		t.Fatal("test setup: dest2 should not be SnapshotPrepared")
	}
	if destInInitReleaseBackoff(dest2) {
		t.Fatal("backoff gate should only apply to SnapshotPrepared dests")
	}
}

// TestInitReleaseAttemptBudget verifies the fixed 3-attempt budget of the
// exec failure line: no cold start within 3 attempts, the 4th (attempts >
// initReleaseMaxRetry) triggers the cold-start verdict.
func TestInitReleaseAttemptBudget(t *testing.T) {
	dest := &v1beta1.JobDestinationStatus{Phase: v1beta1.DestPhaseSnapshotPrepared}

	attempts := 0
	for i := 0; i < 4; i++ {
		attempts = recordAttempt(dest, v1beta1.RetryPhaseInitRelease, "InitReleaseFailed",
			"exec failed", initReleaseMaxRetry)
	}
	if attempts != 4 {
		t.Fatalf("attempts=%d, want 4", attempts)
	}
	if attempts <= initReleaseMaxRetry {
		t.Fatal("attempts should exceed the fixed budget of 3")
	}

	entry := retryStatusOf(dest, v1beta1.RetryPhaseInitRelease)
	if entry == nil || entry.MaxRetry != initReleaseMaxRetry || len(entry.History) != 4 {
		t.Fatalf("InitRelease entry wrong: %+v", entry)
	}
	// The InitRestart and InitRelease lines do not interfere: restartCount sync does not touch the exec count.
	syncInitRestartStatus(dest, "InitContainerFailed", "msg", 1, 2)
	if e := retryStatusOf(dest, v1beta1.RetryPhaseInitRelease); e == nil || e.Attempts != 4 {
		t.Fatalf("InitRelease attempts changed by restart sync: %+v", e)
	}
	if e := retryStatusOf(dest, v1beta1.RetryPhaseInitRestart); e == nil || e.Attempts != 2 {
		t.Fatalf("InitRestart entry wrong: %+v", e)
	}
}
