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

package controller

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/container-migration/worker/cmd/migration-worker/options"
	migrationworker "github.com/container-migration/worker/pkg/apis"

	"k8s.io/klog/v2"
)

// Manager is the gRPC server for the worker side of the
// container_state_migration service. It wires the RPC handlers
// to the scheduler that owns the actual task queues.
type Manager struct {
	ControllerScheduler *Scheduler
	migrationworker.UnimplementedContainerStateMigrationServiceServer
}

// NewManager builds a Manager and the underlying Scheduler.
func NewManager(opts *options.Options) (*Manager, error) {
	controllerScheduler, err := NewScheduler(opts)
	if err != nil {
		return nil, err
	}
	return &Manager{ControllerScheduler: controllerScheduler}, nil
}

// CheckpointToStorage enqueues an asynchronous checkpoint job.
// Idempotent: the controller re-enters its CheckpointHandler whenever the
// status write-back loses the race against the next reconcile (informer
// cache lag), so the same requestID can arrive twice within milliseconds.
// The first item keeps running; a re-enqueue would shadow it in the
// scheduler and freeze the request in WaitingState forever.
func (m *Manager) CheckpointToStorage(_ context.Context, in *migrationworker.CheckpointToStorageRequest) (*migrationworker.BaseResponse, error) {
	klog.V(4).Infof("[checkpoint][request=%s] enqueue requested, containers=%d, storage=%s",
		in.GetRequestID(), len(in.GetPolicy()), in.GetStorageType())
	// Idempotent: same node may receive duplicate triggers from controller retry.
	if _, ok := m.ControllerScheduler.GetCheckpointItemFromQueue(in.GetRequestID()); ok {
		klog.V(4).Infof("[checkpoint][request=%s] task already exists; duplicate enqueue skipped", in.GetRequestID())
		return respGrpc(nil, in.GetRequestID()), nil
	}
	err := m.ControllerScheduler.AddCheckpointItemToQueue(in)
	if err != nil {
		klog.Errorf("[checkpoint][request=%s] failed to enqueue task: %v", in.RequestID, err)
	}
	return respGrpc(err, in.GetRequestID()), nil
}

// QueryCheckpointToStorageStatus returns progress / final result of a
// CheckpointToStorage task.
func (m *Manager) QueryCheckpointToStorageStatus(_ context.Context, in *migrationworker.QueryCheckpointToStorageStatusRequest) (*migrationworker.QueryCheckpointToStorageStatusResponse, error) {
	migrationItem, ok := m.ControllerScheduler.GetCheckpointItemFromQueue(in.RequestID)
	if !ok {
		status := &migrationworker.QueryCheckpointToStorageStatusResponse{RequestID: in.RequestID, Status: StatusNotFound}
		klog.V(4).Infof("[checkpoint][request=%s] status query result: %+v", in.RequestID, status)
		return status, nil
	}
	// Snapshot once: the item's status maps are being written by the upload
	// sampler goroutine, so neither klog nor the gRPC marshaller may touch the
	// live message.
	status := migrationItem.SnapshotStatus()

	waitingTaskNum, ok := m.ControllerScheduler.GetWaitingCheckpointTaskNum(in.RequestID)
	if ok {
		checkpointStatus := &migrationworker.QueryCheckpointToStorageStatusResponse{
			RequestID: status.RequestID,
			Status:    status.Status,
			Message:   status.Message,
			Data:      make(map[string]*migrationworker.CheckpointAttributes),
		}
		for _, policy := range migrationItem.CheckpointRequest.Policy {
			checkpointStatus.Data[policy.ContainerName] = &migrationworker.CheckpointAttributes{
				Extras: map[string]string{"waitingTaskNum": fmt.Sprintf("%d", waitingTaskNum)},
			}
		}
		klog.V(4).Infof("[checkpoint][request=%s] status query result: %+v", in.RequestID, checkpointStatus)
		return checkpointStatus, nil
	}
	klog.V(4).Infof("[checkpoint][request=%s] status query result: %+v", in.RequestID, status)
	return status, nil
}

// RestoreFromStorage enqueues an asynchronous restore job.
// Idempotent: if a task with the same requestID already exists, returns OK
// without re-enqueueing.
func (m *Manager) RestoreFromStorage(_ context.Context, in *migrationworker.RestoreFromStorageRequest) (*migrationworker.BaseResponse, error) {
	klog.V(4).Infof("[restore][request=%s] enqueue requested, containers=%d, storage=%s",
		in.GetRequestID(), len(in.GetContainerCheckpointPath()), in.GetStorageType())
	// Idempotent: same node may receive duplicate triggers from controller retry.
	if _, ok := m.ControllerScheduler.GetRestoreItemFromQueue(in.GetRequestID()); ok {
		klog.V(4).Infof("[restore][request=%s] task already exists; duplicate enqueue skipped", in.GetRequestID())
		return respGrpc(nil, in.GetRequestID()), nil
	}
	err := m.ControllerScheduler.AddRestoreItemToQueue(in)
	if err != nil {
		klog.Errorf("[restore][request=%s] failed to enqueue task: %v", in.RequestID, err)
	}
	return respGrpc(err, in.GetRequestID()), nil
}

// QueryRestoreFromStorageStatus returns progress / final result of a
// RestoreFromStorage task.
func (m *Manager) QueryRestoreFromStorageStatus(_ context.Context, in *migrationworker.QueryRestoreFromStorageStatusRequest) (*migrationworker.QueryRestoreFromStorageStatusResponse, error) {
	migrationItem, ok := m.ControllerScheduler.GetRestoreItemFromQueue(in.RequestID)
	if !ok {
		status := &migrationworker.QueryRestoreFromStorageStatusResponse{RequestID: in.RequestID, Status: StatusNotFound}
		klog.V(4).Infof("[restore][request=%s] status query result: %+v", in.RequestID, status)
		return status, nil
	}
	waitingTaskNum, ok := m.ControllerScheduler.GetWaitingRestoreTaskNum(in.RequestID)
	if ok {
		status := migrationItem.SnapshotStatus()
		restoreStatus := &migrationworker.QueryRestoreFromStorageStatusResponse{
			RequestID: status.RequestID,
			Status:    status.Status,
			Message:   status.Message,
			Data:      make(map[string]*migrationworker.CheckpointAttributes),
		}
		for containerName := range migrationItem.RestoreRequest.ContainerCheckpointPath {
			restoreStatus.Data[containerName] = &migrationworker.CheckpointAttributes{
				Extras: map[string]string{"waitingTaskNum": fmt.Sprintf("%d", waitingTaskNum)},
			}
		}
		klog.V(4).Infof("[restore][request=%s] status query result: %+v", in.RequestID, restoreStatus)
		return restoreStatus, nil
	}
	// Data is already extracted; on each subsequent poll, re-scan the evidence
	// file so the controller can confirm xpu-container-runtime really intercepted.
	m.reportRestoreEvidence(migrationItem)
	// Snapshot after the evidence write, and only hand the copy to klog and the
	// gRPC marshaller: the live message keeps being mutated by the download
	// sampler goroutine.
	status := migrationItem.SnapshotStatus()
	klog.V(4).Infof("[restore][request=%s] status query result: %+v", in.RequestID, status)
	return status, nil
}

// reportRestoreEvidence puts "restoreAttempted" into each container's Extras.
//
// Why: a Running container looks identical whether it was truly restored from
// checkpoint or simply started from scratch. If the restore annotations never
// reached the OCI spec (containerd pass-through misconfigured, runtime not
// installed) the migration silently does nothing. The runtime drops an evidence
// file next to the checkpoint directory; the worker is the only node-side actor
// that can observe it.
func (m *Manager) reportRestoreEvidence(item *RestoreItem) {
	for containerName := range item.RestoreRequest.ContainerCheckpointPath {
		item.UpdateExtrasOfStatus(containerName, map[string]string{
			ExtraKeyRestoreAttempted: fmt.Sprintf("%t", item.RestoreAttempted(containerName)),
		}, nil)
	}
}

// RestoreXPUOnNode synchronously restores the XPU device state of the given
// containers by forwarding each one to the node-local XDM (which runs cr-tool).
//
// It is issued by the controller after it observed the restored pod Running, so
// `runc restore` has finished reading memory/ and the pids are stable. The call
// is fully synchronous: the response is only produced once every container has
// been processed.
//
// Semantics: all containers must succeed. On the first failure the remaining
// containers are skipped and the error is reported with the failing container
// id — the worker itself does not retry: retries (re-running cr-tool, which is
// safe against the same live process) are driven by the controller, which
// counts attempts and falls back to cold start once retryCount is exhausted.
// CPU-only containers (no xpu/ under the checkpoint dir) succeed without side
// effects, decided inside XDM.
func (m *Manager) RestoreXPUOnNode(ctx context.Context, in *migrationworker.RestoreXPUOnNodeRequest) (*migrationworker.BaseResponse, error) {
	klog.V(4).Infof("[restore-xpu][request=%s] requested, containers=%d",
		in.GetRequestID(), len(in.GetContainerCheckpointDir()))

	if len(in.GetContainerCheckpointDir()) == 0 {
		return respGrpc(fmt.Errorf("containerCheckpointDir is empty"), in.GetRequestID()), nil
	}

	// Deterministic order for reproducible logs / failure reporting.
	containerIDs := make([]string, 0, len(in.GetContainerCheckpointDir()))
	for id := range in.GetContainerCheckpointDir() {
		containerIDs = append(containerIDs, id)
	}
	sort.Strings(containerIDs)

	for _, containerID := range containerIDs {
		checkpointDir := in.GetContainerCheckpointDir()[containerID]
		containerStartedAt := time.Now()
		if err := m.ControllerScheduler.Runtime.RestoreXPU(ctx, containerID, checkpointDir); err != nil {
			klog.Errorf("[restore-xpu][request=%s][container=%s] status=failed, checkpointDir=%s, took=%s: %v",
				in.GetRequestID(), containerID, checkpointDir,
				time.Since(containerStartedAt).Truncate(time.Millisecond), err)
			return respGrpc(fmt.Errorf("container %s: %w", containerID, err), in.GetRequestID()), nil
		}
		klog.Infof("[restore-xpu][request=%s][container=%s] status=completed, checkpointDir=%s, took=%s",
			in.GetRequestID(), containerID, checkpointDir,
			time.Since(containerStartedAt).Truncate(time.Millisecond))
	}

	return respGrpc(nil, in.GetRequestID()), nil
}

// CleanTask cancels an in-flight task (if any) and reclaims staging resources.
func (m *Manager) CleanTask(_ context.Context, in *migrationworker.CleanTaskRequest) (*migrationworker.BaseResponse, error) {
	klog.V(4).Infof("[task][request=%s] cleanup requested, cleanData=%t, checkpointDirs=%d",
		in.GetRequestID(), in.GetCleanData(), len(in.GetCheckpointDirs()))
	var err error
	// Whichever cleanup path is taken, remove the in-memory task record from the queue to prevent records from only growing.
	defer m.ControllerScheduler.RemoveItemFromQueue(in.RequestID)

	// Explicit path cleanup: when cleanData=true with checkpointDirs, delete
	// memory/xpu by path directly (keep rootfs), without relying on the in-memory
	// record of requestID - after worker restart/replacement the record is gone,
	// yet data can still be cleaned along with the CR deletion.
	if in.GetCleanData() && len(in.GetCheckpointDirs()) > 0 {
		for _, dir := range in.GetCheckpointDirs() {
			if dir == "" {
				continue
			}
			if derr := deleteCheckpointNonRootfsData(dir); derr != nil {
				klog.Errorf("[task][request=%s] failed to clean checkpoint dir %s: %v", in.GetRequestID(), dir, derr)
				err = derr
			} else {
				klog.V(4).Infof("[task][request=%s] cleaned memory/xpu under %s (rootfs preserved)", in.GetRequestID(), dir)
			}
		}
		return respGrpc(err, in.GetRequestID()), nil
	}

	// Original behavior: clean via the in-memory task record looked up by requestID (when checkpointDirs is empty).
	checkpointItem, checkpointOk := m.ControllerScheduler.GetCheckpointItemFromQueue(in.RequestID)
	if checkpointOk {
		err = checkpointItem.CleanTask(in.CleanData)
	}
	restoreItem, restoreOk := m.ControllerScheduler.GetRestoreItemFromQueue(in.RequestID)
	if restoreOk {
		if rerr := restoreItem.CleanTask(in.CleanData); rerr != nil {
			err = rerr
		}
	}
	if !checkpointOk && !restoreOk {
		klog.V(4).Infof("Task %s is already cleaned, returning directly", in.RequestID)
	}
	return respGrpc(err, in.GetRequestID()), nil
}
