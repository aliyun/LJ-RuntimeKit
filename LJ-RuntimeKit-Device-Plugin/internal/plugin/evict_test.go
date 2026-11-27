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

package plugin

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lj-runtimekit/device-plugin/internal/utils"

	xpugrpc "github.com/lj-runtimekit/device-plugin/internal/grpc"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

const evictTestNode = "evict-node"

// shrinkEvictWaits shortens the bounded eviction waits so tests run fast; restored on cleanup.
func shrinkEvictWaits(t *testing.T) {
	t.Helper()
	origGraceWait, origForceWait, origPoll := evictGraceWait, evictForceWait, evictPollInterval
	evictGraceWait = 120 * time.Millisecond
	evictForceWait = 60 * time.Millisecond
	evictPollInterval = 10 * time.Millisecond
	t.Cleanup(func() {
		evictGraceWait = origGraceWait
		evictForceWait = origForceWait
		evictPollInterval = origPoll
	})
}

// evictDiagPod builds a detection Pod pinned to the given minor on the eviction test node.
func evictDiagPod(name, minor string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   "default",
			Labels:      map[string]string{utils.LabelActiveMonitoring: "true"},
			Annotations: map[string]string{utils.AnnoActiveMonitoringTargetMinor: minor},
		},
		Spec: v1.PodSpec{NodeName: evictTestNode},
	}
}

// TestEvictActiveMonitoringPodsOnMinors_DeletesMatchingPod verifies a detection Pod pinned to
// an allocated minor is deleted so the business Pod can take the card.
func TestEvictActiveMonitoringPodsOnMinors_DeletesMatchingPod(t *testing.T) {
	shrinkEvictWaits(t)
	utils.SetNodeName(evictTestNode)
	client := k8sfake.NewSimpleClientset(evictDiagPod("diag-1", "1"))
	dp := &XpuDevicePlugin{K8sClient: client}

	assert.NoError(t, dp.evictActiveMonitoringPodsOnMinors(context.Background(), []int{1}))

	// Exactly two Lists (fast-path snapshot + one poll iteration) — pins that no re-scan was added.
	lists := 0
	for _, a := range client.Actions() {
		if a.GetVerb() == "list" && a.GetResource().Resource == "pods" {
			lists++
		}
	}
	assert.Equal(t, 2, lists)

	_, err := client.CoreV1().Pods("default").Get(context.Background(), "diag-1", metav1.GetOptions{})
	assert.True(t, apierrors.IsNotFound(err))
}

// TestEvictActiveMonitoringPodsOnMinors_NoMatchIsNoop verifies a detection Pod on a different
// minor is left untouched.
func TestEvictActiveMonitoringPodsOnMinors_NoMatchIsNoop(t *testing.T) {
	shrinkEvictWaits(t)
	utils.SetNodeName(evictTestNode)
	client := k8sfake.NewSimpleClientset(evictDiagPod("diag-1", "1"))
	dp := &XpuDevicePlugin{K8sClient: client}

	// minor 2 has no detection Pod pinned to it → nothing to evict, the Pod survives.
	assert.NoError(t, dp.evictActiveMonitoringPodsOnMinors(context.Background(), []int{2}))

	_, err := client.CoreV1().Pods("default").Get(context.Background(), "diag-1", metav1.GetOptions{})
	assert.NoError(t, err)
}

// TestEvictActiveMonitoringPodsOnMinors_EmptyMinorsIsNoop verifies the early return: with no
// minors there is nothing to guard, list, or delete.
func TestEvictActiveMonitoringPodsOnMinors_EmptyMinorsIsNoop(t *testing.T) {
	utils.SetNodeName(evictTestNode)
	client := k8sfake.NewSimpleClientset(evictDiagPod("diag-1", "1"))
	dp := &XpuDevicePlugin{K8sClient: client}

	assert.NoError(t, dp.evictActiveMonitoringPodsOnMinors(context.Background(), nil))
	assert.Empty(t, client.Actions())
}

// TestEvictActiveMonitoringPodsOnMinors_FailClosed verifies fail-closed: a swallowed delete means
// the Pod cannot be confirmed gone, so the DP errors rather than allocating onto an occupied card.
func TestEvictActiveMonitoringPodsOnMinors_FailClosed(t *testing.T) {
	shrinkEvictWaits(t)
	utils.SetNodeName(evictTestNode)
	client := k8sfake.NewSimpleClientset(evictDiagPod("diag-1", "1"))
	// Swallow every delete so the detection Pod never actually leaves the apiserver.
	client.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, nil
	})
	dp := &XpuDevicePlugin{K8sClient: client}

	err := dp.evictActiveMonitoringPodsOnMinors(context.Background(), []int{1})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "refusing to allocate")

	// The Pod is still there because its delete was swallowed.
	_, getErr := client.CoreV1().Pods("default").Get(context.Background(), "diag-1", metav1.GetOptions{})
	assert.NoError(t, getErr)
}

// TestEvictActiveMonitoringPodsOnMinors_ConcurrentPerMinorLock exercises overlapping minor sets
// evicted concurrently: the global per-minor locks must serialize with no race (-race) or deadlock.
func TestEvictActiveMonitoringPodsOnMinors_ConcurrentPerMinorLock(t *testing.T) {
	shrinkEvictWaits(t)
	utils.SetNodeName(evictTestNode)

	sets := [][]int{{1, 2}, {2, 3}, {3, 1}, {1}, {2, 3}, {1, 2, 3}}
	var wg sync.WaitGroup
	for _, minors := range sets {
		minors := minors
		client := k8sfake.NewSimpleClientset(evictDiagPod("diag", strconv.Itoa(minors[0])))
		dp := &XpuDevicePlugin{K8sClient: client}
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(t, dp.evictActiveMonitoringPodsOnMinors(context.Background(), minors))
		}()
	}
	wg.Wait()
}

// TestLockMinor_SerializesSameMinor verifies the per-minor lock gives mutual exclusion (many
// goroutines on one minor never overlap) and all finish (no deadlock).
func TestLockMinor_SerializesSameMinor(t *testing.T) {
	const minor = 1 << 20 // an unlikely minor, so this test's lock does not collide with others
	var guard sync.Mutex
	inCritical, maxConcurrent := 0, 0

	done := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			unlock := lockMinor(minor)
			defer unlock()
			guard.Lock()
			inCritical++
			if inCritical > maxConcurrent {
				maxConcurrent = inCritical
			}
			guard.Unlock()
			time.Sleep(time.Millisecond)
			guard.Lock()
			inCritical--
			guard.Unlock()
		}()
	}
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("lockMinor deadlocked on the same minor")
	}
	assert.Equal(t, 1, maxConcurrent, "per-minor lock must serialize the critical section")
}

// TestEvictActiveMonitoringPodsOnMinors_StuckMinorDoesNotHoldHealthyMinorLock verifies per-minor
// lock release: a healthy minor's lock frees as soon as the poll sees it clear, despite a stuck peer.
func TestEvictActiveMonitoringPodsOnMinors_StuckMinorDoesNotHoldHealthyMinorLock(t *testing.T) {
	shrinkEvictWaits(t)
	utils.SetNodeName(evictTestNode)
	const stuckMinor, healthyMinor = 1, 2

	// Caller A targets {1,2}: minor 1's delete is swallowed (never clears), minor 2 has none (the
	// poll clears it first). The force delete is the 2nd delete call, after minor 2's lock freed.
	clientA := k8sfake.NewSimpleClientset(evictDiagPod("diag-stuck", strconv.Itoa(stuckMinor)))
	dpA := &XpuDevicePlugin{K8sClient: clientA}
	forceEntered := make(chan struct{})
	releaseForce := make(chan struct{})
	var deletes int32
	clientA.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if atomic.AddInt32(&deletes, 1) == 2 { // the force delete, after the grace poll ran
			close(forceEntered)
			<-releaseForce
		}
		return true, nil, nil // swallow: the detection Pod never leaves the apiserver
	})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Fail-closed: minor 1 never clears, so the whole call errors.
		assert.Error(t, dpA.evictActiveMonitoringPodsOnMinors(context.Background(), []int{stuckMinor, healthyMinor}))
	}()

	// Wait until A is inside minor 1's force delete: minor 2's lock is released, minor 1's still held.
	<-forceEntered

	// minor 2's lock must be free now; an all-held-until-return design would block B against the 2s guard.
	bAcquired := make(chan struct{})
	go func() {
		unlock := lockMinor(healthyMinor)
		unlock()
		close(bAcquired)
	}()
	select {
	case <-bAcquired:
	case <-time.After(2 * time.Second):
		close(releaseForce)
		wg.Wait()
		t.Fatal("healthy minor lock was held hostage by the stuck minor's eviction")
	}

	close(releaseForce) // let A finish its (failing) force-delete + poll on minor 1
	wg.Wait()
}

// TestEvictActiveMonitoringPodsOnMinors_BatchedPollListLoadIsIndependentOfMinorCount verifies the
// shared poll issues ONE batched List per interval regardless of minor count, not one per minor.
func TestEvictActiveMonitoringPodsOnMinors_BatchedPollListLoadIsIndependentOfMinorCount(t *testing.T) {
	shrinkEvictWaits(t)
	utils.SetNodeName(evictTestNode)

	// Four minors, each pinned by a detection Pod whose delete is swallowed, so all stay pinned.
	minors := []int{1, 2, 3, 4}
	objs := make([]runtime.Object, 0, len(minors))
	for _, m := range minors {
		objs = append(objs, evictDiagPod("diag-"+strconv.Itoa(m), strconv.Itoa(m)))
	}
	client := k8sfake.NewSimpleClientset(objs...)
	client.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, nil // swallow: no detection Pod ever clears
	})
	dp := &XpuDevicePlugin{K8sClient: client}

	assert.Error(t, dp.evictActiveMonitoringPodsOnMinors(context.Background(), minors))

	lists := 0
	for _, a := range client.Actions() {
		if a.GetVerb() == "list" && a.GetResource().Resource == "pods" {
			lists++
		}
	}
	// One shared sequence (snapshot + grace polls + force polls), independent of len(minors); a
	// per-minor loop would ~4x it. The bound sits above one sequence, below len(minors) sequences.
	assert.Less(t, lists, 40, "shared poll must not multiply List load by the minor count")
}

// TestEvictActiveMonitoringPodsOnMinors_ListFailClosed verifies fail-closed on the read path: if
// the DP cannot list detection Pods it cannot confirm a minor is clear, so it errors.
func TestEvictActiveMonitoringPodsOnMinors_ListFailClosed(t *testing.T) {
	shrinkEvictWaits(t)
	utils.SetNodeName(evictTestNode)
	client := k8sfake.NewSimpleClientset(evictDiagPod("diag-1", "1"))
	// Fail every list so the DP cannot confirm whether a detection Pod holds the minor.
	client.PrependReactor("list", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("apiserver unavailable")
	})
	dp := &XpuDevicePlugin{K8sClient: client}

	err := dp.evictActiveMonitoringPodsOnMinors(context.Background(), []int{1})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "list activemonitoring pods failed")
}

// TestSortedUnique verifies the dedup + ascending sort that underpins the lock ordering.
func TestSortedUnique(t *testing.T) {
	assert.Equal(t, []int{0, 1, 2}, sortedUnique([]int{2, 0, 1, 1, 2}))
	assert.Empty(t, sortedUnique(nil))
}

// TestTargetMinorsFor verifies the eviction target mirrors xdmAllocate's source: an lj-scheduled
// Pod's IDX_LIST is authoritative, while a community Pod derives minors from the kubelet device ids.
func TestTargetMinorsFor(t *testing.T) {
	// lj-scheduled Pod: ALIYUN_COM_GPU_IDX_LIST=2,3 is authoritative regardless of kubelet device ids.
	ljPod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:        "biz-lj",
		Namespace:   "default",
		Annotations: map[string]string{utils.AnnotationDeviceIndex: "2,3"},
	}}
	dp := &XpuDevicePlugin{} // deviceMeta unused on the IDX_LIST branch
	minors, err := dp.targetMinorsFor(ljPod, []string{"GPU-unrelated"})
	assert.NoError(t, err)
	assert.Equal(t, []int{2, 3}, minors)

	// community Pod: no IDX_LIST, so minors come from the kubelet device ids via deviceMeta.
	dm := utils.NewDeviceMeta()
	dm.Replace(map[int]*xpugrpc.Device{5: {Id: "GPU-abc"}})
	dpCommunity := &XpuDevicePlugin{deviceMeta: dm}
	communityPod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "biz-community", Namespace: "default"}}
	minors, err = dpCommunity.targetMinorsFor(communityPod, []string{"GPU-abc"})
	assert.NoError(t, err)
	assert.Equal(t, []int{5}, minors)
}
