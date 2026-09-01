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
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	xpugrpc "github.com/lj-runtimekit/device-plugin/internal/grpc"

	"github.com/stretchr/testify/assert"
)

func setupDpCheckpoint(t *testing.T) {
	t.Helper()
	origPath := DpCheckpointPath
	DpCheckpointPath = filepath.Join(t.TempDir(), "deviceplugin_internal_checkpoint")
	t.Cleanup(func() { DpCheckpointPath = origPath })
}

func TestGenerateAllocateIDUnique(t *testing.T) {
	seen := make(map[string]struct{})
	for i := 0; i < 100; i++ {
		id := GenerateAllocateID()
		assert.NotEmpty(t, id)
		_, dup := seen[id]
		assert.False(t, dup)
		seen[id] = struct{}{}
	}
}

func TestSaveAndFindRecord(t *testing.T) {
	setupDpCheckpoint(t)

	record := &AllocateRecord{
		AllocateID:   "alloc-1",
		ResourceName: ResourceCore,
		DeviceIDs:    []string{"COMP-0-_0", "COMP-0-_1", "COMP-0-_2"},
		Status:       CheckpointStatusAllocated,
	}
	assert.NoError(t, SaveAllocateRecord(record))

	// full match
	found, err := FindRecordByDeviceIDs(ResourceCore, []string{"COMP-0-_0", "COMP-0-_1", "COMP-0-_2"})
	assert.NoError(t, err)
	assert.Equal(t, "alloc-1", found.AllocateID)

	// subset match (PreStartContainer is invoked per container)
	found, err = FindRecordByDeviceIDs(ResourceCore, []string{"COMP-0-_1"})
	assert.NoError(t, err)
	assert.Equal(t, "alloc-1", found.AllocateID)

	// wrong resource name
	_, err = FindRecordByDeviceIDs(ResourceMemory, []string{"COMP-0-_0"})
	assert.Error(t, err)

	// unknown device id
	_, err = FindRecordByDeviceIDs(ResourceCore, []string{"COMP-9-_0"})
	assert.Error(t, err)
}

// TestFindRecordPrefersNewest verifies that when device uuids are reused
// across allocations and several records match, the newest one (in-flight
// allocation) wins over a stale record of a previous allocation.
func TestFindRecordPrefersNewest(t *testing.T) {
	setupDpCheckpoint(t)

	now := time.Now().UnixNano()
	// stale record of a previous allocation of the same devices
	assert.NoError(t, SaveAllocateRecord(&AllocateRecord{
		AllocateID:   "alloc-old",
		ResourceName: ResourceCore,
		DeviceIDs:    []string{"COMP-0-_0"},
		Status:       CheckpointStatusDone,
		CreateTime:   now - int64(time.Hour),
	}))
	// in-flight record of the current allocation
	assert.NoError(t, SaveAllocateRecord(&AllocateRecord{
		AllocateID:   "alloc-new",
		ResourceName: ResourceCore,
		DeviceIDs:    []string{"COMP-0-_0"},
		Status:       CheckpointStatusAllocated,
		CreateTime:   now,
	}))

	found, err := FindRecordByDeviceIDs(ResourceCore, []string{"COMP-0-_0"})
	assert.NoError(t, err)
	assert.Equal(t, "alloc-new", found.AllocateID)
}

// TestPruneExpiredRecords verifies that SaveAllocateRecord drops finished
// records older than the done TTL and any record older than the max age
// leak guard, while keeping fresh records.
func TestPruneExpiredRecords(t *testing.T) {
	setupDpCheckpoint(t)

	now := time.Now().UnixNano()
	// expired done record
	assert.NoError(t, SaveAllocateRecord(&AllocateRecord{
		AllocateID:   "alloc-done-expired",
		ResourceName: ResourceCore,
		DeviceIDs:    []string{"COMP-0-_0"},
		Status:       CheckpointStatusDone,
		CreateTime:   now - int64(CheckpointDoneRecordTTL) - int64(time.Hour),
	}))
	// leaked allocated record far beyond the max age guard
	assert.NoError(t, SaveAllocateRecord(&AllocateRecord{
		AllocateID:   "alloc-leaked",
		ResourceName: ResourceCore,
		DeviceIDs:    []string{"COMP-1-_0"},
		Status:       CheckpointStatusAllocated,
		CreateTime:   now - int64(CheckpointMaxRecordAge) - int64(time.Hour),
	}))
	// fresh record: the save of this one triggers the pruning
	assert.NoError(t, SaveAllocateRecord(&AllocateRecord{
		AllocateID:   "alloc-fresh",
		ResourceName: ResourceCore,
		DeviceIDs:    []string{"COMP-2-_0"},
		Status:       CheckpointStatusAllocated,
		CreateTime:   now,
	}))

	content, err := os.ReadFile(DpCheckpointPath)
	assert.NoError(t, err)
	var data struct {
		Records map[string]*AllocateRecord `json:"records"`
	}
	assert.NoError(t, json.Unmarshal(content, &data))
	assert.Len(t, data.Records, 1)
	_, ok := data.Records["alloc-fresh"]
	assert.True(t, ok)
}

func TestUpdateAllocateRecord(t *testing.T) {
	setupDpCheckpoint(t)

	record := &AllocateRecord{
		AllocateID:   "alloc-2",
		ResourceName: ResourceMemory,
		DeviceIDs:    []string{"MEM-1-_0"},
		Status:       CheckpointStatusAllocated,
	}
	assert.NoError(t, SaveAllocateRecord(record))

	err := UpdateAllocateRecord("alloc-2", func(r *AllocateRecord) {
		r.PodUID = "uid-123"
		r.PodNamespace = "default"
		r.PodName = "test-pod"
		r.Status = CheckpointStatusPreStart
	})
	assert.NoError(t, err)

	found, err := FindRecordByDeviceIDs(ResourceMemory, []string{"MEM-1-_0"})
	assert.NoError(t, err)
	assert.Equal(t, "uid-123", found.PodUID)
	assert.Equal(t, "default", found.PodNamespace)
	assert.Equal(t, "test-pod", found.PodName)
	assert.Equal(t, CheckpointStatusPreStart, found.Status)

	// updating a missing record must fail
	assert.Error(t, UpdateAllocateRecord("alloc-missing", func(r *AllocateRecord) {}))
}

func TestTryMarkRecordStatus(t *testing.T) {
	setupDpCheckpoint(t)

	record := &AllocateRecord{
		AllocateID:   "alloc-mark",
		ResourceName: ResourceCore,
		DeviceIDs:    []string{"COMP-0-_0"},
		Status:       CheckpointStatusAllocated,
	}
	assert.NoError(t, SaveAllocateRecord(record))

	// the first caller wins the allocated -> prestart transition
	acquired, err := TryMarkRecordStatus("alloc-mark", CheckpointStatusAllocated, CheckpointStatusPreStart)
	assert.NoError(t, err)
	assert.True(t, acquired)

	// a concurrent/restarted caller must lose the race
	acquired, err = TryMarkRecordStatus("alloc-mark", CheckpointStatusAllocated, CheckpointStatusPreStart)
	assert.NoError(t, err)
	assert.False(t, acquired)

	// same after the record reached done
	assert.NoError(t, UpdateAllocateRecord("alloc-mark", func(r *AllocateRecord) {
		r.Status = CheckpointStatusDone
	}))
	acquired, err = TryMarkRecordStatus("alloc-mark", CheckpointStatusAllocated, CheckpointStatusPreStart)
	assert.NoError(t, err)
	assert.False(t, acquired)

	// unknown record
	_, err = TryMarkRecordStatus("alloc-missing", CheckpointStatusAllocated, CheckpointStatusPreStart)
	assert.Error(t, err)
}

func TestConcurrentCheckpointWrites(t *testing.T) {
	setupDpCheckpoint(t)

	const writers = 8
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			record := &AllocateRecord{
				AllocateID:   GenerateAllocateID(),
				ResourceName: ResourceCore,
				DeviceIDs:    []string{"COMP-0-_0"},
				Status:       CheckpointStatusAllocated,
			}
			assert.NoError(t, SaveAllocateRecord(record))
		}(i)
	}
	wg.Wait()

	// all records should survive the concurrent writes
	content, err := os.ReadFile(DpCheckpointPath)
	assert.NoError(t, err)
	var data struct {
		Records map[string]*AllocateRecord `json:"records"`
	}
	assert.NoError(t, json.Unmarshal(content, &data))
	assert.Len(t, data.Records, writers)
}

func TestFindPodInKubeletCheckpointV1(t *testing.T) {
	origPath := KubeletCheckpointPath
	KubeletCheckpointPath = filepath.Join(t.TempDir(), "kubelet_internal_checkpoint")
	defer func() { KubeletCheckpointPath = origPath }()

	// format v1 (k8s 1.13 ~ 1.19): entry per pod/container/resource,
	// DeviceIDs is a flat list, only PodUID is kept (no pod name/namespace)
	kubeletData := map[string]interface{}{
		"Data": map[string]interface{}{
			"PodDeviceEntries": []map[string]interface{}{
				{
					"PodUID":        "uid-old",
					"ContainerName": "c1",
					"ResourceName":  ResourceCore,
					"DeviceIDs":     []string{"COMP-1-_0"},
				},
				{
					"PodUID":        "uid-new",
					"ContainerName": "c1",
					"ResourceName":  ResourceCore,
					"DeviceIDs":     []string{"COMP-0-_0", "COMP-0-_1"},
				},
			},
		},
		"Checksum": 0,
	}
	content, err := json.Marshal(kubeletData)
	assert.NoError(t, err)
	assert.NoError(t, os.WriteFile(KubeletCheckpointPath, content, 0644))

	entry, err := FindPodInKubeletCheckpoint(ResourceCore, []string{"COMP-0-_0", "COMP-0-_1"})
	assert.NoError(t, err)
	assert.Equal(t, "uid-new", entry.PodUID)
	assert.Equal(t, "c1", entry.ContainerName)
	assert.Empty(t, entry.PodName)
	assert.Empty(t, entry.PodNamespace)

	// no matching entry
	_, err = FindPodInKubeletCheckpoint(ResourceCore, []string{"COMP-7-_0"})
	assert.Error(t, err)
}

func TestFindPodInKubeletCheckpointV2(t *testing.T) {
	origPath := KubeletCheckpointPath
	KubeletCheckpointPath = filepath.Join(t.TempDir(), "kubelet_internal_checkpoint")
	defer func() { KubeletCheckpointPath = origPath }()

	// format v2 (k8s >= 1.20, NUMA-aware): one entry per resource, DeviceIDs
	// keyed by NUMA node id, no pod name/namespace
	kubeletData := map[string]interface{}{
		"Data": map[string]interface{}{
			"PodDeviceEntries": []map[string]interface{}{
				{
					"PodUID":        "uid-other",
					"ContainerName": "c1",
					"ResourceName":  ResourceMemory,
					"DeviceIDs":     map[string][]string{"0": {"MEM-1-_0"}},
				},
				{
					"PodUID":        "uid-new",
					"ContainerName": "c1",
					"ResourceName":  ResourceCore,
					"DeviceIDs":     map[string][]string{"0": {"COMP-0-_0"}, "-1": {"COMP-0-_1"}},
				},
			},
		},
		"Checksum": 0,
	}
	content, err := json.Marshal(kubeletData)
	assert.NoError(t, err)
	assert.NoError(t, os.WriteFile(KubeletCheckpointPath, content, 0644))

	// device ids spread over several NUMA keys must all be flattened and matched
	entry, err := FindPodInKubeletCheckpoint(ResourceCore, []string{"COMP-0-_0", "COMP-0-_1"})
	assert.NoError(t, err)
	assert.Equal(t, "uid-new", entry.PodUID)
	assert.Equal(t, "c1", entry.ContainerName)
	assert.Empty(t, entry.PodName)
	assert.Empty(t, entry.PodNamespace)
	assert.ElementsMatch(t, []string{"COMP-0-_0", "COMP-0-_1"}, entry.DeviceIDs)

	// entry of another resource must not match
	_, err = FindPodInKubeletCheckpoint(ResourceCore, []string{"MEM-1-_0"})
	assert.Error(t, err)
}

func TestLoadKubeletCheckpointState(t *testing.T) {
	origPath := KubeletCheckpointPath
	KubeletCheckpointPath = filepath.Join(t.TempDir(), "kubelet_internal_checkpoint")
	defer func() { KubeletCheckpointPath = origPath }()

	// a mix of flat (v1) and NUMA (v2) entries across two resources
	kubeletData := map[string]interface{}{
		"Data": map[string]interface{}{
			"PodDeviceEntries": []map[string]interface{}{
				{
					"PodUID":        "uid-a",
					"ContainerName": "c1",
					"ResourceName":  ResourceCore,
					"DeviceIDs":     []string{"COMP-0-_0", "COMP-0-_1"},
				},
				{
					"PodUID":        "uid-b",
					"ContainerName": "c1",
					"ResourceName":  ResourceCore,
					"DeviceIDs":     map[string][]string{"0": {"COMP-1-_0"}},
				},
				{
					"PodUID":        "uid-c",
					"ContainerName": "c1",
					"ResourceName":  ResourceMemory,
					"DeviceIDs":     map[string][]string{"0": {"MEM-0-_0"}, "1": {"MEM-1-_0"}},
				},
			},
		},
		"Checksum": 0,
	}
	content, err := json.Marshal(kubeletData)
	assert.NoError(t, err)
	assert.NoError(t, os.WriteFile(KubeletCheckpointPath, content, 0644))

	state, err := LoadKubeletCheckpointState()
	assert.NoError(t, err)
	assert.Equal(t, map[string]struct{}{
		"uid-a": {}, "uid-b": {}, "uid-c": {},
	}, state.PodUIDs)
	assert.Equal(t, map[string]struct{}{
		"COMP-0-_0": {}, "COMP-0-_1": {}, "COMP-1-_0": {},
	}, state.AssignedDevices[ResourceCore])
	assert.Equal(t, map[string]struct{}{
		"MEM-0-_0": {}, "MEM-1-_0": {},
	}, state.AssignedDevices[ResourceMemory])

	// a missing checkpoint file is an error, callers must treat it as
	// "unknown" rather than "nothing assigned"
	KubeletCheckpointPath = filepath.Join(t.TempDir(), "missing")
	_, err = LoadKubeletCheckpointState()
	assert.Error(t, err)
}

// newTestDeviceMeta builds a DeviceMeta cache as filled by GrpcListAndWatch:
// minor -> xdm reported device.
func newTestDeviceMeta(devices map[int]string) *DeviceMeta {
	meta := NewDeviceMeta()
	for minor, id := range devices {
		meta.Put(minor, &xpugrpc.Device{Id: id})
	}
	return meta
}

func TestGetDeviceMinorFromID(t *testing.T) {
	// minors are resolved by reverse-looking-up the device ids reported in
	// ListAndWatch, so opaque (non-numeric) xdm device ids work as well
	meta := newTestDeviceMeta(map[int]string{
		0:  "0",
		15: "15",
		2:  "GPU-8f3a-bc12",
		1:  "GPU-11aa",
		3:  "GPU-mig3",
		4:  "GPU-poss4",
		7:  "GPU-f4e2d1c0-abcd-1234-5678-90abcdef0000",
	})
	tests := []struct {
		name     string
		deviceID string
		expected int
		hasError bool
	}{
		{name: "raw id", deviceID: "0", expected: 0},
		{name: "raw id multi digit", deviceID: "15", expected: 15},
		{name: "fake core id with uuid", deviceID: "COMP-GPU-8f3a-bc12-_37", expected: 2},
		{name: "fake memory id with uuid", deviceID: "MEM-GPU-11aa-_5", expected: 1},
		{name: "fake mig id with uuid", deviceID: "MIG-GPU-mig3-_-4_1", expected: 3},
		{name: "fake possibility id with uuid", deviceID: "POSSI-GPU-poss4-_9", expected: 4},
		{name: "fake numeric id", deviceID: "COMP-0-_0", expected: 0},
		// default branch of initialize reports the real gpu uuid as-is
		{name: "raw real gpu uuid", deviceID: "GPU-f4e2d1c0-abcd-1234-5678-90abcdef0000", expected: 7},
		{name: "unknown device id", deviceID: "COMP-GPU-missing-_0", hasError: true},
		{name: "unknown raw id", deviceID: "GPU-abc", hasError: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			minor, err := GetDeviceMinorFromID(meta, tt.deviceID)
			if tt.hasError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, minor)
		})
	}
}

func TestGetDeviceMinorsFromIDsDedup(t *testing.T) {
	meta := newTestDeviceMeta(map[int]string{
		0: "GPU-uuid-0",
		1: "GPU-uuid-1",
	})
	// several core shares on GPU 0 and one on GPU 1
	minors, err := GetDeviceMinorsFromIDs(meta, []string{"COMP-GPU-uuid-0-_0", "COMP-GPU-uuid-0-_1", "COMP-GPU-uuid-0-_2", "COMP-GPU-uuid-1-_0"})
	assert.NoError(t, err)
	assert.Equal(t, []int{0, 1}, minors)

	minors, err = GetDeviceMinorsFromIDs(meta, nil)
	assert.NoError(t, err)
	assert.Empty(t, minors)
}
