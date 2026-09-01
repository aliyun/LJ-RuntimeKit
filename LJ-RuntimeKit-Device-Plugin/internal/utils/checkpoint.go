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

	// CheckpointDoneRecordTTL bounds how long a finished (done) record is
	// kept. It must cover the window in which xdm still consumes the
	// allocateID <-> pod mapping of a running pod.
	CheckpointDoneRecordTTL = 24 * time.Hour
	// CheckpointMaxRecordAge is the last-resort leak guard: no allocation
	// flow legitimately stays in allocated/prestart for days, so any record
	// older than this is dropped to keep the checkpoint file bounded.
	CheckpointMaxRecordAge = 7 * 24 * time.Hour
)

var (
	// DpCheckpointPath is the device plugin checkpoint file. Declared as var
	// so tests can override it.
	DpCheckpointPath = "/var/lib/kubelet/device-plugins/deviceplugin_internal_checkpoint"
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
	f, err := os.OpenFile(DpCheckpointPath+".lock", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open checkpoint lock file failed: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
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
// Expired records are pruned on the fly so the file stays bounded.
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
	if pruned := pruneExpiredRecordsLocked(data); pruned > 0 {
		klog.Infof("pruned %d expired allocate record(s) from checkpoint", pruned)
	}
	data.Records[record.AllocateID] = record
	return saveCheckpointLocked(data)
}

// pruneExpiredRecordsLocked drops done records older than their TTL and any
// record older than the max age leak guard. Returns the prune count.
func pruneExpiredRecordsLocked(data *dpCheckpointData) int {
	now := time.Now().UnixNano()
	pruned := 0
	for id, record := range data.Records {
		refTime := record.UpdateTime
		if refTime == 0 {
			refTime = record.CreateTime
		}
		if refTime == 0 {
			// no timestamp at all: keep the record instead of guessing its age
			continue
		}
		age := time.Duration(now - refTime)
		if (record.Status == CheckpointStatusDone && age > CheckpointDoneRecordTTL) || age > CheckpointMaxRecordAge {
			delete(data.Records, id)
			pruned++
		}
	}
	return pruned
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

// DeleteAllocateRecords removes the given records from the checkpoint.
// Missing ids are ignored to tolerate concurrent cleanup.
func DeleteAllocateRecords(allocateIDs []string) error {
	if len(allocateIDs) == 0 {
		return nil
	}
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
	deleted := 0
	for _, id := range allocateIDs {
		if _, ok := data.Records[id]; ok {
			delete(data.Records, id)
			deleted++
		}
	}
	if deleted == 0 {
		return nil
	}
	klog.Infof("deleted %d allocate record(s) from checkpoint", deleted)
	return saveCheckpointLocked(data)
}
