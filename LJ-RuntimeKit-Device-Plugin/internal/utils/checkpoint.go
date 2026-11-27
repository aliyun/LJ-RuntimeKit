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
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"k8s.io/klog/v2"
)

const (
	// record lifecycle: allocated (Allocate stage) -> prestart (PreStartContainer stage) -> done (xdm allocate succeeded)
	CheckpointStatusAllocated = "allocated"
	CheckpointStatusPreStart  = "prestart"
	CheckpointStatusDone      = "done"
)

var (
	// DpCheckpointPath is the device plugin checkpoint file. Declared as var
	// so tests can override it.
	OldDpCheckpointPath = "/var/lib/kubelet/device-plugins/deviceplugin_internal_checkpoint"
	DpCheckpointPath    = "/var/lib/kubelet/device-plugins/checkpoint/deviceplugin_internal_checkpoint"
)

// AllocateRecord describes one Allocate request context.
// The Allocate stage fills AllocateID/ResourceName/DeviceIDs,
// and the PreStartContainer stage fills pod identity and status.
type AllocateRecord struct {
	AllocateID   string   `json:"allocateID"`
	ResourceName string   `json:"resourceName"`
	DeviceIDs    []string `json:"deviceIDs"`
	PodUID       string   `json:"podUID,omitempty"`
	PodNamespace string   `json:"podNamespace,omitempty"`
	PodName      string   `json:"podName,omitempty"`
	Status       string   `json:"status"`
	CreateTime   int64    `json:"createTime"`
	UpdateTime   int64    `json:"updateTime"`
}

type dpCheckpointData struct {
	Records map[string]*AllocateRecord `json:"records"`
}

// processLock guards against concurrent access from multiple plugin
// instances within the same process; flock guards across processes.
var processLock sync.Mutex

// GenerateAllocateID returns a unique id for one Allocate request.
func GenerateAllocateID() string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("alloc-%d", time.Now().UnixNano())
	}
	return fmt.Sprintf("alloc-%d-%s", time.Now().UnixNano(), hex.EncodeToString(buf))
}

// acquireLock takes an exclusive file lock on the dedicated lock file.
// A dedicated lock file is used because the checkpoint file itself is
// replaced atomically via rename, which would invalidate locks held on it.
func acquireLock() (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(DpCheckpointPath), 0755); err != nil {
		return nil, fmt.Errorf("create checkpoint directory failed: %w", err)
	}
	return acquireCheckpointLock(DpCheckpointPath+".lock", syscall.LOCK_EX)
}

// acquireCheckpointLock 也用于迁移时锁定旧账本，防止复制期间被修改。
func acquireCheckpointLock(path string, operation int) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open checkpoint lock file %s failed: %w", path, err)
	}
	if err := syscall.Flock(int(f.Fd()), operation); err != nil {
		f.Close()
		return nil, fmt.Errorf("flock checkpoint lock file failed: %w", err)
	}
	return f, nil
}

func releaseLock(f *os.File) {
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_UN); err != nil {
		klog.Warningf("unlock checkpoint lock file failed: %v", err)
	}
	f.Close()
}

// MigrateDpCheckpoint 在启动阶段迁移旧账本。旧版本进程须已停止；迁移失败
// 必须终止启动，不能丢弃历史记录后继续服务。锁文件只重建，不复制锁状态。
func MigrateDpCheckpoint() error {
	processLock.Lock()
	defer processLock.Unlock()

	lockFile, err := acquireLock()
	if err != nil {
		return err
	}
	defer releaseLock(lockFile)

	if filepath.Clean(OldDpCheckpointPath) == filepath.Clean(DpCheckpointPath) {
		return initializeCheckpointLocked()
	}

	_, newErr := os.Stat(DpCheckpointPath)
	if newErr != nil && !os.IsNotExist(newErr) {
		return fmt.Errorf("stat new checkpoint failed: %w", newErr)
	}
	newExists := newErr == nil
	if newExists {
		// 已有新账本始终是程序使用的账本，先确认它可正常读取。
		if _, err := loadCheckpointLocked(); err != nil {
			return err
		}
	}

	_, checkpointErr := os.Stat(OldDpCheckpointPath)
	if checkpointErr != nil && !os.IsNotExist(checkpointErr) {
		return fmt.Errorf("stat old checkpoint failed: %w", checkpointErr)
	}
	_, lockErr := os.Stat(OldDpCheckpointPath + ".lock")
	if lockErr != nil && !os.IsNotExist(lockErr) {
		return fmt.Errorf("stat old checkpoint lock failed: %w", lockErr)
	}
	if os.IsNotExist(checkpointErr) && os.IsNotExist(lockErr) {
		return initializeCheckpointLocked()
	}

	// 非阻塞获取旧锁，旧进程正在使用账本时保留文件并拒绝迁移。
	oldLock, err := acquireCheckpointLock(OldDpCheckpointPath+".lock", syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		return fmt.Errorf("lock old checkpoint for migration failed: %w", err)
	}
	defer releaseLock(oldLock)

	content, err := os.ReadFile(OldDpCheckpointPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("read old checkpoint failed: %w", err)
	}
	if err == nil {
		destination := DpCheckpointPath
		if newExists {
			// 新旧都存在：旧文件仅作独立备份，不覆盖新账本或已有备份。
			destination += ".bak-" + time.Now().Format("20060102-150405.000000000")
		} else {
			// 仅旧账本存在：校验后原样复制，保留所有记录及未知字段。
			var data dpCheckpointData
			if len(content) > 0 {
				if err := json.Unmarshal(content, &data); err != nil {
					return fmt.Errorf("unmarshal old checkpoint failed: %w", err)
				}
			}
		}
		if err := copyCheckpointLocked(destination, content); err != nil {
			return err
		}
		if err := os.Remove(OldDpCheckpointPath); err != nil {
			return fmt.Errorf("remove migrated old checkpoint failed: %w", err)
		}
		klog.Infof("copied old dp checkpoint from %s to %s", OldDpCheckpointPath, destination)
	} else {
		// 旧账本已删除但锁还在：确保新账本存在，然后清理旧锁。
		if err := initializeCheckpointLocked(); err != nil {
			return err
		}
	}
	if err := os.Remove(OldDpCheckpointPath + ".lock"); err != nil {
		return fmt.Errorf("remove old checkpoint lock failed: %w", err)
	}
	return syncCheckpointDirectory(filepath.Dir(OldDpCheckpointPath))
}

// initializeCheckpointLocked 保留已有账本；没有账本则创建空记录文件。
func initializeCheckpointLocked() error {
	if _, err := loadCheckpointLocked(); err != nil {
		return err
	}
	if _, err := os.Stat(DpCheckpointPath); os.IsNotExist(err) {
		return copyCheckpointLocked(DpCheckpointPath, []byte(`{"records":{}}`))
	} else {
		return err
	}
}

// copyCheckpointLocked 先写入新目录的临时文件并 fsync，再原子发布。
// 目标文件和目录落盘前，调用方不会删除旧账本。
func copyCheckpointLocked(destination string, content []byte) error {
	dir := filepath.Dir(destination)
	tmp, err := os.CreateTemp(dir, ".checkpoint-migration-*")
	if err != nil {
		return fmt.Errorf("create migration temp file failed: %w", err)
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()
	if err := tmp.Chmod(0644); err != nil {
		return fmt.Errorf("chmod migration temp file failed: %w", err)
	}
	if _, err := tmp.Write(content); err != nil {
		return fmt.Errorf("write migration temp file failed: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync migration temp file failed: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close migration temp file failed: %w", err)
	}
	if err := os.Rename(tmp.Name(), destination); err != nil {
		return fmt.Errorf("publish migrated checkpoint failed: %w", err)
	}
	return syncCheckpointDirectory(dir)
}

func syncCheckpointDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open checkpoint directory failed: %w", err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync checkpoint directory failed: %w", err)
	}
	return nil
}

func loadCheckpointLocked() (*dpCheckpointData, error) {
	data := &dpCheckpointData{Records: make(map[string]*AllocateRecord)}
	content, err := os.ReadFile(DpCheckpointPath)
	if err != nil {
		if os.IsNotExist(err) {
			return data, nil
		}
		return nil, fmt.Errorf("read checkpoint file failed: %w", err)
	}
	if len(content) == 0 {
		return data, nil
	}
	if err := json.Unmarshal(content, data); err != nil {
		return nil, fmt.Errorf("unmarshal checkpoint file failed: %w", err)
	}
	if data.Records == nil {
		data.Records = make(map[string]*AllocateRecord)
	}
	return data, nil
}

// saveCheckpointLocked writes the checkpoint atomically (temp file + rename).
func saveCheckpointLocked(data *dpCheckpointData) error {
	content, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal checkpoint data failed: %w", err)
	}
	tmpPath := DpCheckpointPath + ".tmp"
	if err := os.WriteFile(tmpPath, content, 0644); err != nil {
		return fmt.Errorf("write temp checkpoint file failed: %w", err)
	}
	if err := os.Rename(tmpPath, DpCheckpointPath); err != nil {
		return fmt.Errorf("rename checkpoint file failed: %w", err)
	}
	return nil
}

// SaveAllocateRecord appends one Allocate record into the checkpoint file.
// It never drops existing records: a record is the only proof that an
// allocation still exists for xdm, so it may only be removed by the GC after
// the pod behind it is proven gone.
func SaveAllocateRecord(record *AllocateRecord) error {
	processLock.Lock()
	defer processLock.Unlock()

	lockFile, err := acquireLock()
	if err != nil {
		return err
	}
	defer releaseLock(lockFile)

	data, err := loadCheckpointLocked()
	if err != nil {
		return err
	}
	data.Records[record.AllocateID] = record
	return saveCheckpointLocked(data)
}

// UpdateAllocateRecord applies updateFn to the record of the given
// allocateID under the file lock.
func UpdateAllocateRecord(allocateID string, updateFn func(record *AllocateRecord)) error {
	processLock.Lock()
	defer processLock.Unlock()

	lockFile, err := acquireLock()
	if err != nil {
		return err
	}
	defer releaseLock(lockFile)

	data, err := loadCheckpointLocked()
	if err != nil {
		return err
	}
	record, ok := data.Records[allocateID]
	if !ok {
		return fmt.Errorf("allocate record %s not found in checkpoint", allocateID)
	}
	updateFn(record)
	record.UpdateTime = time.Now().UnixNano()
	return saveCheckpointLocked(data)
}

// TryMarkRecordStatus transitions a record from expected to target under the
// file lock. Returns true when this caller won; used to make PreStartContainer
// idempotent across kubelet retries.
func TryMarkRecordStatus(allocateID string, expected string, target string) (bool, error) {
	processLock.Lock()
	defer processLock.Unlock()

	lockFile, err := acquireLock()
	if err != nil {
		return false, err
	}
	defer releaseLock(lockFile)

	data, err := loadCheckpointLocked()
	if err != nil {
		return false, err
	}
	record, ok := data.Records[allocateID]
	if !ok {
		return false, fmt.Errorf("allocate record %s not found in checkpoint", allocateID)
	}
	if record.Status != expected {
		return false, nil
	}
	record.Status = target
	record.UpdateTime = time.Now().UnixNano()
	return true, saveCheckpointLocked(data)
}

// FindRecordByDeviceIDs returns the newest matching record for the given
// resource and device id subset. Device uuids are reused across allocations.
func FindRecordByDeviceIDs(resourceName string, deviceIDs []string) (*AllocateRecord, error) {
	processLock.Lock()
	defer processLock.Unlock()

	lockFile, err := acquireLock()
	if err != nil {
		return nil, err
	}
	defer releaseLock(lockFile)

	data, err := loadCheckpointLocked()
	if err != nil {
		return nil, err
	}

	var best *AllocateRecord
	for _, record := range data.Records {
		if record.ResourceName != resourceName {
			continue
		}
		if !containsAll(record.DeviceIDs, deviceIDs) {
			continue
		}
		if best == nil || record.CreateTime > best.CreateTime {
			best = record
		}
	}
	if best != nil {
		return best, nil
	}
	return nil, fmt.Errorf("no allocate record matched resource %s with device ids %v", resourceName, deviceIDs)
}

func containsAll(all []string, subset []string) bool {
	set := make(map[string]struct{}, len(all))
	for _, id := range all {
		set[id] = struct{}{}
	}
	for _, id := range subset {
		if _, ok := set[id]; !ok {
			return false
		}
	}
	return true
}

// ListAllRecords returns a snapshot of every record currently stored in the
// checkpoint file. It is used by the checkpoint GC to reconcile the records
// against the pods that are still present on the node.
func ListAllRecords() ([]*AllocateRecord, error) {
	processLock.Lock()
	defer processLock.Unlock()

	lockFile, err := acquireLock()
	if err != nil {
		return nil, err
	}
	defer releaseLock(lockFile)

	data, err := loadCheckpointLocked()
	if err != nil {
		return nil, err
	}
	records := make([]*AllocateRecord, 0, len(data.Records))
	for _, record := range data.Records {
		records = append(records, record)
	}
	return records, nil
}

// LoadAllocateRecord returns the record of the given allocateID. A missing
// record is reported as an error so callers can tell it apart from a live one.
func LoadAllocateRecord(allocateID string) (*AllocateRecord, error) {
	processLock.Lock()
	defer processLock.Unlock()

	lockFile, err := acquireLock()
	if err != nil {
		return nil, err
	}
	defer releaseLock(lockFile)

	data, err := loadCheckpointLocked()
	if err != nil {
		return nil, err
	}
	record, ok := data.Records[allocateID]
	if !ok || record == nil {
		return nil, fmt.Errorf("allocate record %s not found in checkpoint", allocateID)
	}
	return record, nil
}

// sameRecordVersion reports whether the stored record still equals a snapshot
// taken earlier. PodUID, Status and UpdateTime are the fields any later stage
// rewrites, so a difference means the allocation moved on and a decision taken
// on the snapshot is stale.
func sameRecordVersion(snapshot *AllocateRecord, current *AllocateRecord) bool {
	if snapshot == nil || current == nil {
		return false
	}
	return snapshot.PodUID == current.PodUID &&
		snapshot.Status == current.Status &&
		snapshot.UpdateTime == current.UpdateTime
}

// DeleteUnchangedAllocateRecords deletes the given snapshot records and skips
// the ones another stage advanced in the meantime, so a decision taken on a
// stale snapshot can never drop a live allocation. It returns the deleted
// count and the ids kept because they changed.
func DeleteUnchangedAllocateRecords(expected []*AllocateRecord) (int, []string, error) {
	if len(expected) == 0 {
		return 0, nil, nil
	}
	processLock.Lock()
	defer processLock.Unlock()

	lockFile, err := acquireLock()
	if err != nil {
		return 0, nil, err
	}
	defer releaseLock(lockFile)

	data, err := loadCheckpointLocked()
	if err != nil {
		return 0, nil, err
	}
	deleted := 0
	var kept []string
	for _, snapshot := range expected {
		if snapshot == nil {
			continue
		}
		current, ok := data.Records[snapshot.AllocateID]
		if !ok {
			// already removed by another writer, nothing left to do
			continue
		}
		if !sameRecordVersion(snapshot, current) {
			kept = append(kept, snapshot.AllocateID)
			continue
		}
		delete(data.Records, snapshot.AllocateID)
		deleted++
	}
	if deleted == 0 {
		return 0, kept, nil
	}
	klog.Infof("deleted %d allocate record(s) from checkpoint", deleted)
	return deleted, kept, saveCheckpointLocked(data)
}
