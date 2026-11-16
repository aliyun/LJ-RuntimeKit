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

package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	xpugrpc "github.com/lj-runtimekit/device-plugin/internal/grpc"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

// fakeXdmReleaser records every Release call and returns the configured result.
type fakeXdmReleaser struct {
	mu      sync.Mutex
	calls   []string
	err     error
	status  int32
	message string
}

func (f *fakeXdmReleaser) Release(ctx context.Context, in *xpugrpc.ReleaseRequest, opts ...grpc.CallOption) (*xpugrpc.ReleaseResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, in.GetId())
	return &xpugrpc.ReleaseResponse{Status: f.status, Message: f.message}, f.err
}

func (f *fakeXdmReleaser) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func newTestGC(releaser XdmReleaser, runningPods map[string]struct{}) *CheckpointGC {
	gc := &CheckpointGC{releaser: releaser, interval: CheckpointGCInterval, firstMissing: make(map[string]time.Time)}
	gc.fetchPods = func() (map[string]struct{}, error) { return runningPods, nil }
	gc.fetchKubeletState = func() (*KubeletCheckpointState, error) {
		return &KubeletCheckpointState{
			PodUIDs:         map[string]struct{}{},
			AssignedDevices: map[string]map[string]struct{}{},
		}, nil
	}
	return gc
}

// loadRecordIDs returns the allocate ids currently persisted in the checkpoint.
func loadRecordIDs(t *testing.T) map[string]struct{} {
	t.Helper()
	content, err := os.ReadFile(DpCheckpointPath)
	assert.NoError(t, err)
	var data struct {
		Records map[string]*AllocateRecord `json:"records"`
	}
	assert.NoError(t, json.Unmarshal(content, &data))
	ids := make(map[string]struct{}, len(data.Records))
	for id := range data.Records {
		ids[id] = struct{}{}
	}
	return ids
}

// TestCheckpointGCDoneRecordDeletedPod verifies that a done record whose pod
// is gone triggers the xdm Release rpc and the record is dropped afterwards.
func TestCheckpointGCDoneRecordDeletedPod(t *testing.T) {
	setupDpCheckpoint(t)
	stale := time.Now().UnixNano() - int64(CheckpointGCGracePeriod) - int64(time.Hour)

	assert.NoError(t, SaveAllocateRecord(&AllocateRecord{
		AllocateID: "alloc-done",
		PodUID:     "uid-deleted",
		Status:     CheckpointStatusDone,
		CreateTime: stale,
		UpdateTime: stale,
	}))
	// record of a pod that still exists must survive
	assert.NoError(t, SaveAllocateRecord(&AllocateRecord{
		AllocateID: "alloc-alive",
		PodUID:     "uid-alive",
		Status:     CheckpointStatusDone,
		CreateTime: stale,
		UpdateTime: stale,
	}))

	releaser := &fakeXdmReleaser{}
	gc := newTestGC(releaser, map[string]struct{}{"uid-alive": {}})
	assert.NoError(t, gc.RunOnce())

	assert.Equal(t, []string{"uid-deleted"}, releaser.calls)
	ids := loadRecordIDs(t)
	_, hasDeleted := ids["alloc-done"]
	assert.False(t, hasDeleted)
	_, hasAlive := ids["alloc-alive"]
	assert.True(t, hasAlive)
}

// TestCheckpointGCAbsentFromBothSourcesDeletesImmediately verifies that a
// record missing from both pod list and checkpoint is released immediately.
func TestCheckpointGCAbsentFromBothSourcesDeletesImmediately(t *testing.T) {
	setupDpCheckpoint(t)
	now := time.Now().UnixNano()

	// fresh done record within the grace period: still deleted immediately
	assert.NoError(t, SaveAllocateRecord(&AllocateRecord{
		AllocateID: "alloc-fresh-done",
		PodUID:     "uid-deleted",
		Status:     CheckpointStatusDone,
		CreateTime: now,
		UpdateTime: now,
	}))
	// fresh in-flight record: same, an in-flight allocation of a pod that is
	// provably gone must be released as well
	assert.NoError(t, SaveAllocateRecord(&AllocateRecord{
		AllocateID: "alloc-fresh-prestart",
		PodUID:     "uid-deleted",
		Status:     CheckpointStatusPreStart,
		CreateTime: now,
		UpdateTime: now,
	}))
	// record without any timestamp: age unknown, but the pod is provably
	// gone, so the age does not matter
	assert.NoError(t, SaveAllocateRecord(&AllocateRecord{
		AllocateID: "alloc-notime",
		PodUID:     "uid-deleted",
		Status:     CheckpointStatusDone,
	}))

	releaser := &fakeXdmReleaser{}
	gc := newTestGC(releaser, map[string]struct{}{})
	assert.NoError(t, gc.RunOnce())

	assert.ElementsMatch(t, []string{"uid-deleted", "uid-deleted", "uid-deleted"}, releaser.calls)
	assert.Empty(t, loadRecordIDs(t))
}

// TestCheckpointGCSkipsRecordAdvancedAfterSnapshot verifies that a record
// updated between the snapshot and the release is neither released in xdm nor
// dropped: the decision was taken on a stale snapshot.
func TestCheckpointGCSkipsRecordAdvancedAfterSnapshot(t *testing.T) {
	setupDpCheckpoint(t)
	now := time.Now().UnixNano()

	assert.NoError(t, SaveAllocateRecord(&AllocateRecord{
		AllocateID: "alloc-raced",
		PodUID:     "uid-deleted",
		Status:     CheckpointStatusDone,
		CreateTime: now,
		UpdateTime: now,
	}))

	releaser := &fakeXdmReleaser{}
	gc := newTestGC(releaser, map[string]struct{}{})
	gc.fetchKubeletState = func() (*KubeletCheckpointState, error) {
		// another stage advances the record right after the GC snapshot
		assert.NoError(t, UpdateAllocateRecord("alloc-raced", func(r *AllocateRecord) {
			r.Status = CheckpointStatusPreStart
		}))
		return &KubeletCheckpointState{
			PodUIDs:         map[string]struct{}{},
			AssignedDevices: map[string]map[string]struct{}{},
		}, nil
	}
	assert.NoError(t, gc.RunOnce())

	assert.Equal(t, 0, releaser.callCount())
	_, kept := loadRecordIDs(t)["alloc-raced"]
	assert.True(t, kept)
}

// TestCheckpointGCSkipsRecordVanishedAfterSnapshot verifies that a record
// removed between the snapshot and the release is not released again.
func TestCheckpointGCSkipsRecordVanishedAfterSnapshot(t *testing.T) {
	setupDpCheckpoint(t)
	now := time.Now().UnixNano()

	assert.NoError(t, SaveAllocateRecord(&AllocateRecord{
		AllocateID: "alloc-vanished",
		PodUID:     "uid-deleted",
		Status:     CheckpointStatusDone,
		CreateTime: now,
		UpdateTime: now,
	}))

	releaser := &fakeXdmReleaser{}
	gc := newTestGC(releaser, map[string]struct{}{})
	gc.fetchKubeletState = func() (*KubeletCheckpointState, error) {
		snapshot, err := LoadAllocateRecord("alloc-vanished")
		assert.NoError(t, err)
		_, _, err = DeleteUnchangedAllocateRecords([]*AllocateRecord{snapshot})
		assert.NoError(t, err)
		return &KubeletCheckpointState{
			PodUIDs:         map[string]struct{}{},
			AssignedDevices: map[string]map[string]struct{}{},
		}, nil
	}
	assert.NoError(t, gc.RunOnce())

	assert.Equal(t, 0, releaser.callCount())
	assert.Empty(t, loadRecordIDs(t))
}

// TestCheckpointGCKeepsRecordStillInKubeletCheckpoint verifies a record
// still in the checkpoint but missing from the pod list arms the timer and
// is not released immediately.
func TestCheckpointGCKeepsRecordStillInKubeletCheckpoint(t *testing.T) {
	setupDpCheckpoint(t)
	stale := time.Now().UnixNano() - int64(CheckpointGCGracePeriod) - int64(time.Hour)

	assert.NoError(t, SaveAllocateRecord(&AllocateRecord{
		AllocateID: "alloc-restarting",
		PodUID:     "uid-restarting",
		Status:     CheckpointStatusDone,
		CreateTime: stale,
		UpdateTime: stale,
	}))

	releaser := &fakeXdmReleaser{}
	gc := newTestGC(releaser, map[string]struct{}{})
	gc.fetchKubeletState = func() (*KubeletCheckpointState, error) {
		return &KubeletCheckpointState{
			PodUIDs:         map[string]struct{}{"uid-restarting": {}},
			AssignedDevices: map[string]map[string]struct{}{},
		}, nil
	}
	assert.NoError(t, gc.RunOnce())

	assert.Equal(t, 0, releaser.callCount())
	_, kept := loadRecordIDs(t)["alloc-restarting"]
	assert.True(t, kept)
	_, armed := gc.firstMissing["alloc-restarting"]
	assert.True(t, armed)
}

// TestCheckpointGCKeepsResidualCheckpointEntry verifies that a record whose
// pod is missing from the pod list but still present in the kubelet checkpoint
// is never reclaimed: the two sources disagree, so the pod cannot be proven
// gone.
func TestCheckpointGCKeepsResidualCheckpointEntry(t *testing.T) {
	setupDpCheckpoint(t)
	now := time.Now().UnixNano()

	assert.NoError(t, SaveAllocateRecord(&AllocateRecord{
		AllocateID: "alloc-residual",
		PodUID:     "uid-deleted",
		Status:     CheckpointStatusDone,
		CreateTime: now,
		UpdateTime: now,
	}))

	releaser := &fakeXdmReleaser{}
	gc := newTestGC(releaser, map[string]struct{}{})
	gc.fetchKubeletState = func() (*KubeletCheckpointState, error) {
		return &KubeletCheckpointState{
			PodUIDs:         map[string]struct{}{"uid-deleted": {}},
			AssignedDevices: map[string]map[string]struct{}{},
		}, nil
	}

	// round 1: the absence is not proven, the record survives
	assert.NoError(t, gc.RunOnce())
	assert.Equal(t, 0, releaser.callCount())
	_, kept := loadRecordIDs(t)["alloc-residual"]
	assert.True(t, kept)
	_, armed := gc.firstMissing["alloc-residual"]
	assert.True(t, armed)

	// no amount of waiting turns missing evidence into a deletion
	gc.firstMissing["alloc-residual"] = time.Now().Add(-CheckpointGCGracePeriod - time.Hour)
	assert.NoError(t, gc.RunOnce())
	assert.Equal(t, 0, releaser.callCount())
	_, kept = loadRecordIDs(t)["alloc-residual"]
	assert.True(t, kept)
}

// TestCheckpointGCReappearanceResetsAbsenceTimer verifies a pod reappearing
// in the pod list resets the absence timer.
func TestCheckpointGCReappearanceResetsAbsenceTimer(t *testing.T) {
	setupDpCheckpoint(t)
	now := time.Now().UnixNano()

	assert.NoError(t, SaveAllocateRecord(&AllocateRecord{
		AllocateID: "alloc-restarting",
		PodUID:     "uid-restarting",
		Status:     CheckpointStatusDone,
		CreateTime: now,
		UpdateTime: now,
	}))

	releaser := &fakeXdmReleaser{}
	gc := newTestGC(releaser, map[string]struct{}{})
	gc.fetchKubeletState = func() (*KubeletCheckpointState, error) {
		return &KubeletCheckpointState{
			PodUIDs:         map[string]struct{}{"uid-restarting": {}},
			AssignedDevices: map[string]map[string]struct{}{},
		}, nil
	}

	// round 1: pod absent from the list, timer armed
	assert.NoError(t, gc.RunOnce())
	assert.Equal(t, 0, releaser.callCount())

	// round 2: pod back in the list, timer reset
	gc.fetchPods = func() (map[string]struct{}, error) {
		return map[string]struct{}{"uid-restarting": {}}, nil
	}
	assert.NoError(t, gc.RunOnce())
	assert.Equal(t, 0, releaser.callCount())
	assert.Empty(t, gc.firstMissing)
	_, kept := loadRecordIDs(t)["alloc-restarting"]
	assert.True(t, kept)
}

// TestCheckpointGCKeepsRecordWhenStateUnavailable verifies that an unreadable
// kubelet checkpoint never leads to a deletion: the pod list alone cannot
// prove that a pod is gone.
func TestCheckpointGCKeepsRecordWhenStateUnavailable(t *testing.T) {
	setupDpCheckpoint(t)
	now := time.Now().UnixNano()

	assert.NoError(t, SaveAllocateRecord(&AllocateRecord{
		AllocateID: "alloc-stale",
		PodUID:     "uid-deleted",
		Status:     CheckpointStatusDone,
		CreateTime: now - int64(CheckpointGCGracePeriod) - int64(time.Hour),
		UpdateTime: now - int64(CheckpointGCGracePeriod) - int64(time.Hour),
	}))

	releaser := &fakeXdmReleaser{}
	gc := newTestGC(releaser, map[string]struct{}{})
	gc.fetchKubeletState = func() (*KubeletCheckpointState, error) {
		return nil, fmt.Errorf("read kubelet checkpoint failed")
	}
	gc.firstMissing["alloc-stale"] = time.Now().Add(-CheckpointGCGracePeriod - time.Hour)
	assert.NoError(t, gc.RunOnce())

	assert.Equal(t, 0, releaser.callCount())
	_, kept := loadRecordIDs(t)["alloc-stale"]
	assert.True(t, kept)
}

// TestCheckpointGCKeepsRecordOnDeleteFailure verifies that the record is kept
// when the xdm Release rpc fails so the next round can retry it.
func TestCheckpointGCKeepsRecordOnReleaseFailure(t *testing.T) {
	setupDpCheckpoint(t)

	for name, releaser := range map[string]*fakeXdmReleaser{
		"rpc error":  {err: fmt.Errorf("connection refused")},
		"bad status": {status: 1, message: "device busy"},
	} {
		t.Run(name, func(t *testing.T) {
			setupDpCheckpoint(t)
			stale := time.Now().UnixNano() - int64(CheckpointGCGracePeriod) - int64(time.Hour)
			assert.NoError(t, SaveAllocateRecord(&AllocateRecord{
				AllocateID: "alloc-retry",
				PodUID:     "uid-deleted",
				Status:     CheckpointStatusDone,
				CreateTime: stale,
				UpdateTime: stale,
			}))

			gc := newTestGC(releaser, map[string]struct{}{})
			assert.NoError(t, gc.RunOnce())

			assert.Equal(t, 1, releaser.callCount())
			_, kept := loadRecordIDs(t)["alloc-retry"]
			assert.True(t, kept)
		})
	}
}

// TestCheckpointGCInFlightRecordKeptWithoutProof verifies an in-flight record
// of a pod missing from the pod list is kept while the kubelet checkpoint is
// unreadable.
func TestCheckpointGCInFlightRecordKeptWithoutProof(t *testing.T) {
	setupDpCheckpoint(t)
	now := time.Now().UnixNano()

	assert.NoError(t, SaveAllocateRecord(&AllocateRecord{
		AllocateID: "alloc-stale",
		PodUID:     "uid-deleted",
		Status:     CheckpointStatusAllocated,
		CreateTime: now - int64(CheckpointGCGracePeriod) - int64(time.Hour),
		UpdateTime: now - int64(CheckpointGCGracePeriod) - int64(time.Hour),
	}))

	releaser := &fakeXdmReleaser{}
	gc := newTestGC(releaser, map[string]struct{}{})
	gc.fetchKubeletState = func() (*KubeletCheckpointState, error) {
		return nil, fmt.Errorf("read kubelet checkpoint failed")
	}
	gc.firstMissing["alloc-stale"] = time.Now().Add(-CheckpointGCGracePeriod - time.Hour)
	assert.NoError(t, gc.RunOnce())

	assert.Equal(t, 0, releaser.callCount())
	_, kept := loadRecordIDs(t)["alloc-stale"]
	assert.True(t, kept)
}

// TestCheckpointGCRecordWithoutPodUID verifies records without pod identity:
// fresh ones are kept, aged-out ones with devices no longer assigned are
// released.
func TestCheckpointGCRecordWithoutPodUID(t *testing.T) {
	setupDpCheckpoint(t)

	assert.NoError(t, SaveAllocateRecord(&AllocateRecord{
		AllocateID: "alloc-nouid-fresh",
		Status:     CheckpointStatusAllocated,
		CreateTime: time.Now().UnixNano(),
	}))
	stale := time.Now().UnixNano() - int64(CheckpointGCGracePeriod) - int64(time.Hour)
	assert.NoError(t, SaveAllocateRecord(&AllocateRecord{
		AllocateID: "alloc-nouid-stale",
		Status:     CheckpointStatusAllocated,
		CreateTime: stale,
		UpdateTime: stale,
	}))

	releaser := &fakeXdmReleaser{}
	gc := newTestGC(releaser, map[string]struct{}{})
	gc.firstMissing["alloc-nouid-stale"] = time.Now().Add(-CheckpointGCGracePeriod - time.Hour)
	assert.NoError(t, gc.RunOnce())

	assert.Equal(t, []string{"alloc-nouid-stale"}, releaser.calls)
	ids := loadRecordIDs(t)
	_, hasFresh := ids["alloc-nouid-fresh"]
	assert.True(t, hasFresh)
	_, hasStale := ids["alloc-nouid-stale"]
	assert.False(t, hasStale)
}

// TestCheckpointGCNoUIDDeviceStillAssigned verifies a record without pod
// identity is kept while kubelet still allocates its devices.
func TestCheckpointGCNoUIDDeviceStillAssigned(t *testing.T) {
	setupDpCheckpoint(t)
	stale := time.Now().UnixNano() - int64(CheckpointGCGracePeriod) - int64(time.Hour)

	assert.NoError(t, SaveAllocateRecord(&AllocateRecord{
		AllocateID:   "alloc-pulling",
		Status:       CheckpointStatusAllocated,
		ResourceName: "gpu/xpu",
		DeviceIDs:    []string{"dev-1", "dev-2"},
		CreateTime:   stale,
		UpdateTime:   stale,
	}))

	releaser := &fakeXdmReleaser{}
	gc := newTestGC(releaser, map[string]struct{}{})
	gc.fetchKubeletState = func() (*KubeletCheckpointState, error) {
		return &KubeletCheckpointState{
			PodUIDs: map[string]struct{}{},
			AssignedDevices: map[string]map[string]struct{}{
				"gpu/xpu": {"dev-1": {}, "dev-2": {}},
			},
		}, nil
	}
	assert.NoError(t, gc.RunOnce())

	assert.Equal(t, 0, releaser.callCount())
	_, kept := loadRecordIDs(t)["alloc-pulling"]
	assert.True(t, kept)
}

// TestCheckpointGCNoUIDCheckpointUnreadable verifies that records without
// pod identity are left untouched when the kubelet checkpoint cannot be
// read: without the deterministic oracle nothing may be deleted.
func TestCheckpointGCNoUIDCheckpointUnreadable(t *testing.T) {
	setupDpCheckpoint(t)
	stale := time.Now().UnixNano() - int64(CheckpointGCGracePeriod) - int64(time.Hour)

	assert.NoError(t, SaveAllocateRecord(&AllocateRecord{
		AllocateID: "alloc-nouid-stale",
		Status:     CheckpointStatusAllocated,
		CreateTime: stale,
		UpdateTime: stale,
	}))

	releaser := &fakeXdmReleaser{}
	gc := newTestGC(releaser, map[string]struct{}{})
	gc.fetchKubeletState = func() (*KubeletCheckpointState, error) {
		return nil, fmt.Errorf("read kubelet checkpoint failed")
	}
	assert.NoError(t, gc.RunOnce())

	assert.Equal(t, 0, releaser.callCount())
	_, kept := loadRecordIDs(t)["alloc-nouid-stale"]
	assert.True(t, kept)
}

// TestCheckpointGCFetchPodsFailure verifies that a failing pod fetch aborts
// the whole round without touching any record.
func TestCheckpointGCFetchPodsFailure(t *testing.T) {
	setupDpCheckpoint(t)

	assert.NoError(t, SaveAllocateRecord(&AllocateRecord{
		AllocateID: "alloc-keep",
		PodUID:     "uid-deleted",
		Status:     CheckpointStatusDone,
		CreateTime: time.Now().UnixNano(),
	}))

	releaser := &fakeXdmReleaser{}
	gc := &CheckpointGC{releaser: releaser, interval: CheckpointGCInterval}
	gc.fetchPods = func() (map[string]struct{}, error) {
		return nil, fmt.Errorf("kubelet unreachable")
	}
	assert.Error(t, gc.RunOnce())

	assert.Equal(t, 0, releaser.callCount())
	_, kept := loadRecordIDs(t)["alloc-keep"]
	assert.True(t, kept)
}

// TestCheckpointGCFetchPodsFailureDiscardsAbsenceTimers verifies a failing
// pod fetch discards accumulated absence timers.
func TestCheckpointGCFetchPodsFailureDiscardsAbsenceTimers(t *testing.T) {
	setupDpCheckpoint(t)

	releaser := &fakeXdmReleaser{}
	gc := newTestGC(releaser, nil)
	gc.firstMissing["alloc-x"] = time.Now().Add(-CheckpointGCGracePeriod - time.Minute)
	gc.fetchPods = func() (map[string]struct{}, error) {
		return nil, fmt.Errorf("kubelet unreachable")
	}
	assert.Error(t, gc.RunOnce())

	assert.Equal(t, 0, releaser.callCount())
	assert.Empty(t, gc.firstMissing)
}

// TestCheckpointGCReleaseFailureRetriedNextRound verifies that a failed
// Release keeps the record of a proven-gone pod so the next round retries it.
func TestCheckpointGCReleaseFailureRetriedNextRound(t *testing.T) {
	setupDpCheckpoint(t)
	now := time.Now().UnixNano()

	assert.NoError(t, SaveAllocateRecord(&AllocateRecord{
		AllocateID: "alloc-retry",
		PodUID:     "uid-deleted",
		Status:     CheckpointStatusDone,
		CreateTime: now,
		UpdateTime: now,
	}))

	releaser := &fakeXdmReleaser{err: fmt.Errorf("xdm unavailable")}
	gc := newTestGC(releaser, map[string]struct{}{})

	// release fails: the record must survive for the next round
	assert.NoError(t, gc.RunOnce())
	assert.Equal(t, 1, releaser.callCount())
	_, kept := loadRecordIDs(t)["alloc-retry"]
	assert.True(t, kept)

	// next round retries immediately and drops the record
	releaser.err = nil
	assert.NoError(t, gc.RunOnce())
	assert.Equal(t, []string{"uid-deleted", "uid-deleted"}, releaser.calls)
	_, kept = loadRecordIDs(t)["alloc-retry"]
	assert.False(t, kept)
	assert.Empty(t, gc.firstMissing)
}

// TestResolveKubeletPodsEndpoint verifies the endpoint resolution precedence:
// full url override > host ip from downward api > loopback fallback.
func TestResolveKubeletPodsEndpoint(t *testing.T) {
	t.Setenv(EnvKubeletPodsEndpoint, "")
	t.Setenv(EnvHostIP, "")
	assert.Equal(t, "http://127.0.0.1:10255/pods", resolveKubeletPodsEndpoint())

	t.Setenv(EnvHostIP, "192.168.1.10")
	assert.Equal(t, "http://192.168.1.10:10255/pods", resolveKubeletPodsEndpoint())

	t.Setenv(EnvKubeletPodsEndpoint, "http://10.0.0.1:10250/pods")
	assert.Equal(t, "http://10.0.0.1:10250/pods", resolveKubeletPodsEndpoint())
}

// TestFetchRunningPodUIDsFromKubelet verifies parsing of the kubelet 10255
// /pods response against a local test server.
func TestFetchRunningPodUIDsFromKubelet(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"items":[{"metadata":{"uid":"uid-1"}},{"metadata":{"uid":"uid-2"}}]}`)
	}))
	defer server.Close()

	origEndpoint := KubeletPodsEndpoint
	KubeletPodsEndpoint = server.URL
	t.Cleanup(func() { KubeletPodsEndpoint = origEndpoint })

	pods, err := fetchRunningPodUIDsFromKubelet()
	assert.NoError(t, err)
	assert.Len(t, pods, 2)
	_, has1 := pods["uid-1"]
	assert.True(t, has1)
	_, has2 := pods["uid-2"]
	assert.True(t, has2)
}

// TestFetchRunningPodUIDsFromKubeletEmptyListRejected verifies that an empty
// pod list is rejected as unreliable: the device plugin pod itself always
// runs on this node, so a genuinely empty list cannot happen.
func TestFetchRunningPodUIDsFromKubeletEmptyListRejected(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"items":[]}`)
	}))
	defer server.Close()

	origEndpoint := KubeletPodsEndpoint
	KubeletPodsEndpoint = server.URL
	t.Cleanup(func() { KubeletPodsEndpoint = origEndpoint })

	pods, err := fetchRunningPodUIDsFromKubelet()
	assert.Error(t, err)
	assert.Nil(t, pods)
}

// TestCheckKubeletPodsEndpoint verifies the pre-check against both a healthy
// and a broken endpoint.
func TestCheckKubeletPodsEndpoint(t *testing.T) {
	origEndpoint := KubeletPodsEndpoint
	t.Cleanup(func() { KubeletPodsEndpoint = origEndpoint })

	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"items":[]}`)
	}))
	defer healthy.Close()
	KubeletPodsEndpoint = healthy.URL
	assert.NoError(t, checkKubeletPodsEndpoint())

	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer broken.Close()
	KubeletPodsEndpoint = broken.URL
	assert.Error(t, checkKubeletPodsEndpoint())
}
