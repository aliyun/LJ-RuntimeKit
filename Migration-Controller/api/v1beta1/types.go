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

package v1beta1

import (
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// ===================== MigrationJob related types =====================

// JobPhase represents the task phase of a ContainerStateMigrationJobRequest.
type JobPhase string

const (
	JobPhasePending            = JobPhase("Pending")
	JobPhaseCheckpointing      = JobPhase("Checkpointing")
	JobPhaseRestoring          = JobPhase("Restoring")
	JobPhaseSucceeded          = JobPhase("Succeeded")
	JobPhasePartiallySucceeded = JobPhase("PartiallySucceeded")
	JobPhaseFailed             = JobPhase("Failed")
	JobPhaseTerminated         = JobPhase("Terminated")
)

// Legal values of spec.storageType. Pod annotations and hand-crafted CRs share
// the same set of values, so they live in the api package to avoid each
// controller defining its own literals.
const (
	StorageTypeLocal     = "local"
	StorageTypeNFS       = "nfs"
	StorageTypeOSS       = "oss"
	StorageTypeDragonfly = "dragonfly"
	StorageTypeEFC       = "efc"
	StorageTypePVC       = "pvc"
)

const (
	// JobConditionCheckpointed: checkpoint stage completed.
	JobConditionCheckpointed = "Checkpointed"
	// JobConditionPodsCreated: all replica pods created and Running.
	JobConditionPodsCreated = "PodsCreated"
	// JobConditionRestored: all replicas restored.
	JobConditionRestored = "Restored"
	// JobConditionCleaned: checkpoint data cleaned up.
	JobConditionCleaned = "Cleaned"
	// JobConditionWorkerBlocked: the task is blocked because the worker config
	// on the target node is stale but the node still has in-flight tasks: the
	// controller is waiting for the old tasks to drain before replacing the
	// worker and dispatching this task.
	JobConditionWorkerBlocked = "WorkerBlocked"
	// JobConditionColdStartFallback: a replica only succeeded via the
	// cold-start fallback (pod deleted and recreated after retries exhausted).
	JobConditionColdStartFallback = "ColdStartFallback"

	// RequestConditionCheckpointInvoked indicates the checkpoint API has been called
	RequestConditionCheckpointInvoked = "CheckpointInvoked"
	// RequestConditionRestoreInvoked indicates the restore API has been called
	RequestConditionRestoreInvoked = "RestoreInvoked"
)

// DestPhase represents the fine-grained restore phase of a single destination.
const (
	// DestPhasePending: pod created, waiting for scheduling.
	DestPhasePending = JobPhase("Pending")
	// DestPhaseScheduled: pod scheduled to a node.
	DestPhaseScheduled = JobPhase("Scheduled")
	// DestPhaseSnapshotPrepared: checkpoint data ready on the target node.
	DestPhaseSnapshotPrepared = JobPhase("SnapshotPrepared")
	// DestPhaseInitReleased: init container released.
	DestPhaseInitReleased = JobPhase("InitReleased")
	// DestPhaseXPURestoring: container Running and confirmed restored from
	// checkpoint (restoreAttempted), XPU device state restore in progress: the
	// controller calls XDM RestoreXPU via the worker to run cr-tool.
	DestPhaseXPURestoring = JobPhase("XPURestoring")
	// DestPhaseColdStarting: a stage of this replica exhausted its retries,
	// falling back to cold start: delete the failed restore pod and rebuild a
	// normal pod without the restore annotation from the source template.
	DestPhaseColdStarting = JobPhase("ColdStarting")
	// DestPhaseSucceeded: restore complete (container Running and XPU state
	// restored, or all containers exited normally with no device state to
	// restore; a successful cold-start fallback also counts).
	DestPhaseSucceeded = JobPhase("Succeeded")
	// DestPhaseFailed: failed.
	DestPhaseFailed = JobPhase("Failed")
)

// ===================== Shared types =====================

// CheckpointAttributes records the attributes of checkpoint data.
type CheckpointAttributes struct {
	// Path is the path.
	Path string `json:"path,omitempty"`
	// Size is the file size in bytes.
	Size int64 `json:"size,omitempty"`
	// FileCount is the number of files.
	FileCount int64 `json:"fileCount,omitempty"`
	// TotalDuration is the total elapsed time.
	TotalDuration time.Duration `json:"totalDuration,omitempty"`
	// Extras are other attributes.
	Extras map[string]string `json:"extras,omitempty"`
}

// ContainerMigrationStatus is the container migration status.
type ContainerMigrationStatus struct {
	Name                 string                `json:"name,omitempty"`
	ContainerID          string                `json:"containerID,omitempty"`
	CheckpointAttributes *CheckpointAttributes `json:"checkpointAttributes,omitempty"`
	// SnapshotPath is the checkpoint data root directory (containing the
	// rootfs/memory/xpu subdirectories); xpu-container-runtime reads the pod
	// annotation migration.io/restore-data-path, this is only a status record.
	SnapshotPath string             `json:"snapshotPath,omitempty"`
	Conditions   []metav1.Condition `json:"conditions,omitempty"`
}

// SimplePodInfo is the brief info of a pod.
type SimplePodInfo struct {
	Name      string    `json:"name,omitempty"`
	Namespace string    `json:"namespace,omitempty"`
	UID       types.UID `json:"uid,omitempty"`
	IP        string    `json:"ip,omitempty"`
	NodeName  string    `json:"nodeName,omitempty"`
}

// String returns a human-readable string representation
func (s *SimplePodInfo) String() string {
	if s == nil {
		return "nil"
	}
	return fmt.Sprintf("%s/%s", s.Namespace, s.Name)
}
