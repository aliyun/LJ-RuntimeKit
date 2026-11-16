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
	"syscall"
	"testing"
	"time"

	xpugrpc "github.com/lj-runtimekit/device-plugin/internal/grpc"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupDpCheckpoint(t *testing.T) {
	t.Helper()
	origPath := DpCheckpointPath
	DpCheckpointPath = filepath.Join(t.TempDir(), "deviceplugin_internal_checkpoint")
	t.Cleanup(func() { DpCheckpointPath = origPath })
}

func setupCheckpointMigration(t *testing.T) {
	t.Helper()
	oldPath, newPath := OldDpCheckpointPath, DpCheckpointPath
	root := t.TempDir()
	OldDpCheckpointPath = filepath.Join(root, "old", "deviceplugin_internal_checkpoint")
	DpCheckpointPath = filepath.Join(root, "new", "deviceplugin_internal_checkpoint")
	require.NoError(t, os.MkdirAll(filepath.Dir(OldDpCheckpointPath), 0755))
	t.Cleanup(func() {
		OldDpCheckpointPath, DpCheckpointPath = oldPath, newPath
	})
}

func TestMigrateDpCheckpoint(t *testing.T) {
	// 混合生命周期与未知字段，迁移须逐字节保留，不能改时间或丢记录。
	legacy := `{"records":{"a":{"allocateID":"a","status":"allocated","createTime":1},"b":{"allocateID":"b","status":"prestart","podUID":"pod-b"},"c":{"allocateID":"c","status":"done","updateTime":2}},"futureField":"preserved"}`
	fresh := `{"records":{"new":{"allocateID":"new","status":"done"}}}`
	for _, tc := range []struct {
		name       string
		oldExists  bool
		oldContent string
		newExists  bool
		newContent string
		oldLock    bool
		wantError  bool
	}{
		{name: "迁移完整旧账本", oldExists: true, oldContent: legacy, oldLock: true},
		{name: "无旧锁也能迁移", oldExists: true, oldContent: legacy},
		{name: "空旧账本", oldExists: true},
		{name: "首次启动无账本"},
		{name: "只有旧锁时创建新账本并清理旧锁", oldLock: true},
		{name: "仅有新账本保持不变", newExists: true, newContent: fresh},
		{name: "复制后中断可重试", oldExists: true, oldContent: legacy, newExists: true, newContent: legacy, oldLock: true},
		{name: "新旧都存在备份旧账本", oldExists: true, oldContent: legacy, newExists: true, newContent: fresh, oldLock: true},
		{name: "新账本存在时原样备份损坏的旧文件", oldExists: true, oldContent: "{broken", newExists: true, newContent: fresh, oldLock: true},
		{name: "旧账本损坏保留原件", oldExists: true, oldContent: "{broken", oldLock: true, wantError: true},
		{name: "新账本损坏禁止启动", newExists: true, newContent: "{broken", wantError: true},
		{name: "清理迁移后残留的旧锁", newExists: true, newContent: fresh, oldLock: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setupCheckpointMigration(t)
			if tc.oldExists {
				require.NoError(t, os.WriteFile(OldDpCheckpointPath, []byte(tc.oldContent), 0644))
			}
			if tc.oldLock {
				require.NoError(t, os.WriteFile(OldDpCheckpointPath+".lock", nil, 0644))
			}
			if tc.newExists {
				require.NoError(t, os.MkdirAll(filepath.Dir(DpCheckpointPath), 0755))
				require.NoError(t, os.WriteFile(DpCheckpointPath, []byte(tc.newContent), 0644))
			}

			err := MigrateDpCheckpoint()
			if tc.wantError {
				require.Error(t, err)
				if tc.oldExists {
					content, err := os.ReadFile(OldDpCheckpointPath)
					require.NoError(t, err)
					assert.Equal(t, tc.oldContent, string(content))
					assert.FileExists(t, OldDpCheckpointPath+".lock")
				}
				if tc.newExists {
					content, err := os.ReadFile(DpCheckpointPath)
					require.NoError(t, err)
					assert.Equal(t, tc.newContent, string(content))
				} else {
					assert.NoFileExists(t, DpCheckpointPath)
				}
				return
			}
			require.NoError(t, err)
			assert.NoFileExists(t, OldDpCheckpointPath)
			assert.NoFileExists(t, OldDpCheckpointPath+".lock")
			assert.FileExists(t, DpCheckpointPath+".lock")
			want := `{"records":{}}`
			if tc.newExists {
				want = tc.newContent
			} else if tc.oldExists {
				want = tc.oldContent
			}
			content, err := os.ReadFile(DpCheckpointPath)
			require.NoError(t, err)
			assert.Equal(t, want, string(content))
			backups, err := filepath.Glob(DpCheckpointPath + ".bak-*")
			require.NoError(t, err)
			if tc.oldExists && tc.newExists {
				require.Len(t, backups, 1)
				backup, err := os.ReadFile(backups[0])
				require.NoError(t, err)
				assert.Equal(t, tc.oldContent, string(backup))
			} else {
				assert.Empty(t, backups)
			}
			// 后续分配必须继续写入新目录，重启不应恢复旧记录。
			require.NoError(t, SaveAllocateRecord(&AllocateRecord{AllocateID: "after-migration", Status: CheckpointStatusAllocated}))
			before, err := os.ReadFile(DpCheckpointPath)
			require.NoError(t, err)
			require.NoError(t, MigrateDpCheckpoint())
			after, err := os.ReadFile(DpCheckpointPath)
			require.NoError(t, err)
			assert.Equal(t, before, after)
			assert.NoFileExists(t, OldDpCheckpointPath)
			assert.NoFileExists(t, OldDpCheckpointPath+".lock")
		})
	}
}

func TestMigrateDpCheckpointKeepsExistingBackups(t *testing.T) {
	setupCheckpointMigration(t)
	require.NoError(t, SaveAllocateRecord(&AllocateRecord{AllocateID: "active", Status: CheckpointStatusDone}))
	active, err := os.ReadFile(DpCheckpointPath)
	require.NoError(t, err)

	// 模拟两次升级均带来旧账本，各次备份不得互相覆盖。
	for _, content := range []string{"legacy-one", "legacy-two"} {
		require.NoError(t, os.WriteFile(OldDpCheckpointPath, []byte(content), 0644))
		require.NoError(t, MigrateDpCheckpoint())
		assert.NoFileExists(t, OldDpCheckpointPath)
	}
	backups, err := filepath.Glob(DpCheckpointPath + ".bak-*")
	require.NoError(t, err)
	require.Len(t, backups, 2)
	var contents []string
	for _, backup := range backups {
		// 后缀必须包含可解析的日期、时分秒及九位小数。
		suffix := backup[len(DpCheckpointPath+".bak-"):]
		require.Len(t, suffix, len("20060102-150405.000000000"))
		_, err := time.Parse("20060102-150405.000000000", suffix)
		require.NoError(t, err)
		content, err := os.ReadFile(backup)
		require.NoError(t, err)
		contents = append(contents, string(content))
	}
	assert.ElementsMatch(t, []string{"legacy-one", "legacy-two"}, contents)
	after, err := os.ReadFile(DpCheckpointPath)
	require.NoError(t, err)
	assert.Equal(t, active, after)
	records, err := ListAllRecords()
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, "active", records[0].AllocateID)
}

func TestMigrateDpCheckpointOldLockBusy(t *testing.T) {
	setupCheckpointMigration(t)
	content := []byte(`{"records":{}}`)
	require.NoError(t, os.WriteFile(OldDpCheckpointPath, content, 0644))
	oldLock, err := acquireCheckpointLock(OldDpCheckpointPath+".lock", syscall.LOCK_EX)
	require.NoError(t, err)
	defer oldLock.Close()

	require.Error(t, MigrateDpCheckpoint())
	assert.FileExists(t, OldDpCheckpointPath+".lock")
	assert.NoFileExists(t, DpCheckpointPath)
	remaining, err := os.ReadFile(OldDpCheckpointPath)
	require.NoError(t, err)
	assert.Equal(t, content, remaining)

	releaseLock(oldLock)
	require.NoError(t, MigrateDpCheckpoint())
	assert.NoFileExists(t, OldDpCheckpointPath)
}

func TestMigrateDpCheckpointDestinationFailure(t *testing.T) {
	for _, tc := range []string{"父路径是文件", "目标路径是目录"} {
		t.Run(tc, func(t *testing.T) {
			setupCheckpointMigration(t)
			content := []byte(`{"records":{}}`)
			require.NoError(t, os.WriteFile(OldDpCheckpointPath, content, 0644))
			require.NoError(t, os.WriteFile(OldDpCheckpointPath+".lock", nil, 0644))
			if tc == "父路径是文件" {
				require.NoError(t, os.WriteFile(filepath.Dir(DpCheckpointPath), nil, 0644))
			} else {
				require.NoError(t, os.MkdirAll(DpCheckpointPath, 0755))
			}
			require.Error(t, MigrateDpCheckpoint())
			remaining, err := os.ReadFile(OldDpCheckpointPath)
			require.NoError(t, err)
			assert.Equal(t, content, remaining)
			assert.FileExists(t, OldDpCheckpointPath+".lock")
		})
	}
}

func TestMigrateDpCheckpointConcurrent(t *testing.T) {
	setupCheckpointMigration(t)
	content := []byte(`{"records":{"a":{"allocateID":"a","status":"done"}}}`)
	require.NoError(t, os.WriteFile(OldDpCheckpointPath, content, 0644))
	var wg sync.WaitGroup
	errors := make(chan error, 8)
	for i := 0; i < cap(errors); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errors <- MigrateDpCheckpoint()
		}()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	migrated, err := os.ReadFile(DpCheckpointPath)
	require.NoError(t, err)
	assert.Equal(t, content, migrated)
	assert.NoFileExists(t, OldDpCheckpointPath)
	assert.NoFileExists(t, OldDpCheckpointPath+".lock")
}

func TestCopyCheckpointPublicationFailure(t *testing.T) {
	setupCheckpointMigration(t)
	content := []byte(`{"records":{}}`)
	require.NoError(t, os.WriteFile(OldDpCheckpointPath, content, 0644))
	// 目标是目录时，临时文件已写入并落盘，但原子发布必然失败。
	require.NoError(t, os.MkdirAll(DpCheckpointPath, 0755))
	require.Error(t, copyCheckpointLocked(DpCheckpointPath, content))
	remaining, err := os.ReadFile(OldDpCheckpointPath)
	require.NoError(t, err)
	assert.Equal(t, content, remaining)
	assert.DirExists(t, DpCheckpointPath)
	temps, err := filepath.Glob(filepath.Join(filepath.Dir(DpCheckpointPath), ".checkpoint-migration-*"))
	require.NoError(t, err)
	assert.Empty(t, temps)
}

func TestMigrateDpCheckpointSamePath(t *testing.T) {
	setupCheckpointMigration(t)
	OldDpCheckpointPath = DpCheckpointPath
	require.NoError(t, SaveAllocateRecord(&AllocateRecord{AllocateID: "same-path"}))
	before, err := os.ReadFile(DpCheckpointPath)
	require.NoError(t, err)
	require.NoError(t, MigrateDpCheckpoint())
	after, err := os.ReadFile(DpCheckpointPath)
	require.NoError(t, err)
	assert.Equal(t, before, after)
	assert.FileExists(t, DpCheckpointPath+".lock")
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

// TestSaveAllocateRecordKeepsAllRecords verifies that saving a record never
// drops existing ones, whatever their status or age: a record may only be
// removed by the GC after the pod behind it is proven gone.
func TestSaveAllocateRecordKeepsAllRecords(t *testing.T) {
	setupDpCheckpoint(t)

	now := time.Now().UnixNano()
	old := now - int64(30*24*time.Hour)
	// long finished record of a running pod
	assert.NoError(t, SaveAllocateRecord(&AllocateRecord{
		AllocateID:   "alloc-done-old",
		ResourceName: ResourceCore,
		DeviceIDs:    []string{"COMP-0-_0"},
		Status:       CheckpointStatusDone,
		CreateTime:   old,
		UpdateTime:   old,
	}))
	// long leaked in-flight record
	assert.NoError(t, SaveAllocateRecord(&AllocateRecord{
		AllocateID:   "alloc-leaked",
		ResourceName: ResourceCore,
		DeviceIDs:    []string{"COMP-1-_0"},
		Status:       CheckpointStatusAllocated,
		CreateTime:   old,
		UpdateTime:   old,
	}))
	// fresh record: the save of this one must not drop the two above
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
	assert.Len(t, data.Records, 3)
	for _, id := range []string{"alloc-done-old", "alloc-leaked", "alloc-fresh"} {
		_, ok := data.Records[id]
		assert.True(t, ok, "record %s must survive", id)
	}
}

// TestDeleteUnchangedAllocateRecords verifies the conditional delete: records
// that still match the snapshot are dropped, while the ones another writer
// advanced (or already removed) survive.
func TestDeleteUnchangedAllocateRecords(t *testing.T) {
	setupDpCheckpoint(t)

	for _, id := range []string{"alloc-same", "alloc-advanced", "alloc-removed"} {
		assert.NoError(t, SaveAllocateRecord(&AllocateRecord{
			AllocateID: id,
			Status:     CheckpointStatusAllocated,
		}))
	}
	snapSame, err := LoadAllocateRecord("alloc-same")
	assert.NoError(t, err)
	snapAdvanced, err := LoadAllocateRecord("alloc-advanced")
	assert.NoError(t, err)
	snapRemoved, err := LoadAllocateRecord("alloc-removed")
	assert.NoError(t, err)

	// after the snapshot one record is advanced by PreStart, one is removed
	assert.NoError(t, UpdateAllocateRecord("alloc-advanced", func(r *AllocateRecord) {
		r.PodUID = "uid-1"
		r.Status = CheckpointStatusDone
	}))
	_, _, err = DeleteUnchangedAllocateRecords([]*AllocateRecord{snapRemoved})
	assert.NoError(t, err)

	deleted, kept, err := DeleteUnchangedAllocateRecords([]*AllocateRecord{snapSame, snapAdvanced, snapRemoved})
	assert.NoError(t, err)
	assert.Equal(t, 1, deleted)
	assert.Equal(t, []string{"alloc-advanced"}, kept)

	records, err := ListAllRecords()
	assert.NoError(t, err)
	assert.Len(t, records, 1)
	assert.Equal(t, "alloc-advanced", records[0].AllocateID)

	// a missing record is reported as an error, not as an empty record
	_, err = LoadAllocateRecord("alloc-same")
	assert.Error(t, err)
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
