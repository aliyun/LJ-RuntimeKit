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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ===================== Spec types =====================

// CheckpointPolicy defines the checkpoint policy of a single container.
type CheckpointPolicy struct {
	// ExcludeFilepath excludes file/directory paths (absolute paths inside the container).
	ExcludeFilepath []string `json:"excludeFilepath,omitempty"`
	// AdvanceOptions advanced options.
	AdvanceOptions map[string]string `json:"advanceOptions,omitempty"`
}

// CheckpointSpec defines the configuration of the checkpoint stage.
type CheckpointSpec struct {
	// SourcePodName is the source pod name.
	SourcePodName string `json:"sourcePodName"`
	// SourcePodNamespace is the namespace of the source pod.
	SourcePodNamespace string `json:"sourcePodNamespace"`
	// SourcePodUID is the K8s UID of the source pod.
	SourcePodUID string `json:"sourcePodUID"`
	// Containers lists the container names to checkpoint. Required, and only one
	// container is allowed at present: the list type is kept for future
	// multi-container support, but worker / XDM are single-container only, so
	// filling zero or more than one is rejected by InitHandler validation.
	Containers []string `json:"containers,omitempty"`
	// TimeoutInMinute is the checkpoint timeout in minutes, passed through to
	// cr-tool as --timeout <minutes> when explicitly set. Unset (nil) means
	// --timeout is not passed to cr-tool at all. An explicit 0 is passed
	// through as --timeout 0 (cr-tool semantics: wait-then-abort), except on
	// auto-created CRs (pod-annotation path) where an explicit 0 blocks the
	// checkpoint stage with a warning (see InitHandler). A negative value is
	// rejected by InitHandler and written back to status.message. It does not
	// control any controller-side deadline: the checkpoint stage waits
	// indefinitely and cr-tool enforces the budget itself.
	TimeoutInMinute *int `json:"timeoutInMinute,omitempty"`
	// EnableCheckpointAutoMode controls whether cr-tool runs in auto mode
	// (--enable-auto-mode): the system automatically detects the steady-state
	// point of the model service and triggers the checkpoint. Unset (nil)
	// defaults to true. When false, cr-tool runs without --enable-auto-mode;
	// combined with an unset timeoutInMinute the checkpoint stage is blocked
	// with a warning (see InitHandler).
	EnableCheckpointAutoMode *bool `json:"enableCheckpointAutoMode,omitempty"`
	// ContainerCheckpointPolicy is the fine-grained checkpoint policy keyed by
	// container name.
	ContainerCheckpointPolicy map[string]*CheckpointPolicy `json:"containerCheckpointPolicy,omitempty"`
}

// RestorePolicy defines the restore policy of a single container.
//
// Reserved for future extensions, currently no consumer: the restore path
// (proto RestoreFromStorageRequest -> worker -> XDM) has no passthrough
// channel for it yet, filling it has no effect. It is kept to reserve the slot
// for "per-container restore options" and to stay symmetric with
// CheckpointPolicy.
//
// Do not delete it as dead code just because no references are found.
type RestorePolicy struct {
	// AdvanceOptions advanced options, reserved for future passthrough to
	// worker / XDM.
	AdvanceOptions map[string]string `json:"advanceOptions,omitempty"`
}

// RestoreSpec defines the configuration of the restore stage.
type RestoreSpec struct {
	// Replicas is the number of replica pods to restore.
	Replicas int `json:"replicas"`
	// CheckpointUID is the UID of the referenced checkpoint CRD.
	// Required when checkpoint is nil; left empty when checkpoint exists.
	CheckpointUID string `json:"checkpointUID,omitempty"`
	// RetryCount is the max retry count after a stage failure, >=0; 0=no retry.
	// Download data / runc restore / restore XPU are three independently
	// counted stages; when any stage exhausts its retries, the replica falls
	// back to cold start (brought up normally from the source template).
	// Retry counting and the "exhausted -> cold start" decision are owned by
	// the controller: download and XPU retries are driven by the controller;
	// runc restore retries are driven by kubelet in-place restarts (the
	// controller counts restartCount). The cold-start fallback is always on;
	// retryCount only controls the number of retries before cold start. The
	// restore stage has no timeout.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=0
	RetryCount int `json:"retryCount,omitempty"`
	// DisableColdStartFallback turns off the cold-start fallback: replicas that
	// exhaust restore retries stay in ColdStarting instead of being replaced by
	// a cold-started pod. The failing restore pod is kept in place - useful for
	// debugging cr-tool failures on the live pod. Off by default (fallback on).
	DisableColdStartFallback bool `json:"disableColdStartFallback,omitempty"`
	// ContainerRestorePolicy is the fine-grained restore policy keyed by
	// container name.
	//
	// Reserved for future extensions, currently disabled, see RestorePolicy;
	// do not delete.
	ContainerRestorePolicy map[string]*RestorePolicy `json:"containerRestorePolicy,omitempty"`
}

// JobLocal defines the local storage backend.
type JobLocal struct {
	// Path is the local storage path, the root directory for checkpoint data.
	Path string `json:"path,omitempty"`
}

// JobOSS defines the OSS storage backend.
type JobOSS struct {
	MountPoint string            `json:"mountPoint,omitempty"`
	Endpoint   string            `json:"endpoint,omitempty"`
	Options    map[string]string `json:"options,omitempty"`
}

// JobNFS defines the NFS storage backend.
type JobNFS struct {
	Server  string            `json:"server,omitempty"`
	Path    string            `json:"path,omitempty"`
	Options map[string]string `json:"options,omitempty"`
}

// JobDragonfly defines the Dragonfly P2P storage backend.
type JobDragonfly struct {
	// Endpoint is the dfdaemon gRPC address, e.g. "127.0.0.1:65000".
	Endpoint string `json:"endpoint,omitempty"`
	// OSSMountPoint is the OSS mount point (for direct upload to OSS).
	OSSMountPoint string `json:"ossMountPoint,omitempty"`
	// OSSEndpoint is the OSS endpoint (for direct upload to OSS).
	OSSEndpoint string `json:"ossEndpoint,omitempty"`
	// Options are additional options (including OSS accessKeyId/accessKeySecret
	// etc.).
	Options map[string]string `json:"options,omitempty"`
	// PreheatJobName is the name of the associated PreheatJob CR (optional;
	// preheat disabled when empty).
	PreheatJobName string `json:"preheatJobName,omitempty"`
	// PreheatJobNamespace is the namespace of the associated PreheatJob CR.
	PreheatJobNamespace string `json:"preheatJobNamespace,omitempty"`
}

// JobEFC defines the EFC storage backend.
//
// EFC mounting and origin fetching are handled by the node-side runtimekit; the
// controller only passes through the bucket holding the checkpoint and the
// associated PreheatJob CR (so the worker can obtain preheat info through it).
type JobEFC struct {
	// BucketName is the bucket holding the checkpoint.
	BucketName string `json:"bucketName,omitempty"`
	// PreheatJobName is the name of the associated PreheatJob CR (optional;
	// preheat disabled when empty).
	PreheatJobName string `json:"preheatJobName,omitempty"`
	// PreheatJobNamespace is the namespace of the associated PreheatJob CR.
	PreheatJobNamespace string `json:"preheatJobNamespace,omitempty"`
	// Options are additional options.
	Options map[string]string `json:"options,omitempty"`
}

// JobPVC defines the PVC direct-attach storage backend.
//
// The controller mounts the PVC when creating the task-level worker (fixed
// mount point /mnt/pv); checkpoint data is written directly into the mounted
// PVC and restore reads directly from the mounted PVC, no remote storage such
// as OSS/EFC involved. Which preheat path (Dragonfly or EFC) the restore reads
// take is decided by the worker command-line flag --pvc-preheat-backend,
// unrelated to the CR.
type JobPVC struct {
	// PvcName is the name of the PVC to mount.
	PvcName string `json:"pvcName,omitempty"`
	// PvcNamespace is the namespace of the PVC.
	PvcNamespace string `json:"pvcNamespace,omitempty"`
}

// JobStorage defines the storage backend (CRD-owned, no external Storage
// dependency).
type JobStorage struct {
	Local     *JobLocal     `json:"local,omitempty"`
	NFS       *JobNFS       `json:"nfs,omitempty"`
	OSS       *JobOSS       `json:"oss,omitempty"`
	Dragonfly *JobDragonfly `json:"dragonfly,omitempty"`
	EFC       *JobEFC       `json:"efc,omitempty"`
	PVC       *JobPVC       `json:"pvc,omitempty"`
}

// ContainerStateMigrationJobRequestSpec defines the desired state
type ContainerStateMigrationJobRequestSpec struct {
	// Checkpoint is the checkpoint configuration; the checkpoint stage is
	// skipped when nil.
	Checkpoint *CheckpointSpec `json:"checkpoint,omitempty"`
	// Restore is the restore configuration; the restore stage is skipped when
	// nil.
	Restore *RestoreSpec `json:"restore,omitempty"`
	// StorageType is the storage type: local, nfs, oss, dragonfly, efc, pvc.
	StorageType string `json:"storageType"`
	// Storage is the storage backend configuration.
	Storage *JobStorage `json:"storage"`
	// Cancel is the cancellation flag.
	Cancel bool `json:"cancel,omitempty"`
}

// ===================== Status types =====================

// JobCheckpointStatus is the checkpoint sub-task status.
type JobCheckpointStatus struct {
	// Phase is the current phase: Checkpointing -> Succeeded / Failed.
	Phase JobPhase `json:"phase,omitempty"`
	// Message is the status description.
	Message string `json:"message,omitempty"`
	// ContainerMigrationStatuses is the per-container checkpoint status
	// (including results).
	ContainerMigrationStatuses []ContainerMigrationStatus `json:"containerMigrationStatuses,omitempty"`
	// SourcePodTemplate is the reproducible configuration of the source pod, the
	// only basis for rebuilding the target pod in the restore stage.
	//
	// Stored as a whole unvalidated object: the expanded PodSpec OpenAPI schema
	// has seven thousand lines; putting it into the CRD would force the
	// apiserver to run a full structural validation on every read/write, the
	// cost is not worth the benefit. Schemaless + PreserveUnknownFields also
	// keeps the embedded metadata from being pruned, so the name / namespace /
	// labels / annotations needed by the target pod are read directly from the
	// ObjectMeta here instead of keeping a separate copy.
	// +kubebuilder:validation:Type=object
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:pruning:PreserveUnknownFields
	SourcePodTemplate *corev1.PodTemplateSpec `json:"sourcePodTemplate,omitempty"`
	// SourceNodeIP is the node IP of the source pod. The source pod may be
	// deleted any time after checkpoint completes, after which the node to
	// finish worker tasks on can no longer be looked up; it must be pinned in
	// the checkpoint stage.
	SourceNodeIP string `json:"sourceNodeIP,omitempty"`
	// Conditions are the checkpoint sub-status conditions.
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// JobDestinationStatus describes the restore status of a single replica pod.
type JobDestinationStatus struct {
	// NodeIP is the node IP the pod is scheduled to.
	NodeIP string `json:"nodeIP,omitempty"`
	// Pod is the pod info.
	Pod *SimplePodInfo `json:"pod,omitempty"`
	// Phase is the restore phase of this pod.
	Phase JobPhase `json:"phase,omitempty"`
	// StartupMode records whether this replica came up "from checkpoint
	// restore" or "cold-start fallback"; only meaningful after the replica
	// reaches a terminal state.
	StartupMode StartupMode `json:"startupMode,omitempty"`
	// PhaseRetries is the per-stage retry counts and failure history (visible
	// via describe).
	PhaseRetries []PhaseRetryStatus `json:"phaseRetries,omitempty"`
	// ColdStart is the cold-start fallback info (filled only on fallback).
	ColdStart *ColdStartStatus `json:"coldStart,omitempty"`
	// ContainerMigrationStatuses is the per-container restore status of this
	// pod.
	ContainerMigrationStatuses []ContainerMigrationStatus `json:"containerMigrationStatuses,omitempty"`
	// Message is the status description.
	Message string `json:"message,omitempty"`
}

// StartupMode is the final startup mode of a replica.
type StartupMode string

const (
	// StartupModeRestored: restored from checkpoint successfully (via runc
	// restore + XPU restore).
	StartupModeRestored StartupMode = "Restored"
	// StartupModeColdStart: cold-start fallback succeeded after retries
	// exhausted (brought up by a normal cold start).
	StartupModeColdStart StartupMode = "ColdStart"
)

// RetryPhase identifies a retryable stage.
type RetryPhase string

const (
	// RetryPhaseDownload is the checkpoint data download stage.
	RetryPhaseDownload RetryPhase = "Download"
	// RetryPhaseRuncRestore is the runc restore (container restore) stage.
	RetryPhaseRuncRestore RetryPhase = "RuncRestore"
	// RetryPhaseXPU is the restore XPU (cr-tool) stage.
	RetryPhaseXPU RetryPhase = "XPU"
	// RetryPhaseInitRestart is the kubelet restart line of the init container's
	// own non-zero exit: restartCount is counted synchronously (the kubelet
	// authoritative source), MaxRetry is spec.restore.retryCount.
	RetryPhaseInitRestart RetryPhase = "InitRestart"
	// RetryPhaseInitRelease is the controller counting line of init container
	// release exec/touch failures: an exec failure does not trigger a kubelet
	// restart (the container did not exit, restartCount does not grow), only
	// the controller's own counting can be the fallback, with a fixed cap
	// initReleaseMaxRetry=3 (independent of retryCount).
	RetryPhaseInitRelease RetryPhase = "InitRelease"
)

// RestoreAttempt is the detail of one failure.
type RestoreAttempt struct {
	// Reason is the failure classification (e.g. "RestoreFromStorage rejected").
	Reason string `json:"reason,omitempty"`
	// Message is the failure detail.
	Message string `json:"message,omitempty"`
	// Time is the failure time.
	Time metav1.Time `json:"time,omitempty"`
}

// PhaseRetryStatus is the retry count and history of a single stage.
type PhaseRetryStatus struct {
	// Phase identifies the stage.
	Phase RetryPhase `json:"phase"`
	// Attempts is the number of failures of this stage so far (excluding the
	// attempt in progress).
	Attempts int `json:"attempts"`
	// MaxRetry is the snapshot of retryCount at that time.
	MaxRetry int `json:"maxRetry"`
	// History is the detail of each failure (chronological).
	History []RestoreAttempt `json:"history,omitempty"`
}

// ColdStartStatus is the cold-start fallback info.
type ColdStartStatus struct {
	// Stage is the cold-start sub-stage: DeletingPod (waiting for the old pod
	// to disappear) / CreatingPod (creating the cold-start pod) /
	// WaitingRunning (waiting for the cold-start pod to come up).
	Stage string `json:"stage,omitempty"`
	// Reason is the reason triggering cold start (which stage, which attempt
	// failed).
	Reason string `json:"reason,omitempty"`
	// StartedAt is the moment cold start was entered.
	StartedAt metav1.Time `json:"startedAt,omitempty"`
}

// JobRestoreStatus is the restore sub-task status.
type JobRestoreStatus struct {
	// Phase is the current phase: Pending (replica pods to be created) ->
	// Restoring (replicas created, pulling data / waiting for containers to
	// settle) -> Succeeded / PartiallySucceeded / Failed.
	Phase JobPhase `json:"phase,omitempty"`
	// Message is the status description.
	Message string `json:"message,omitempty"`
	// Destinations is the restore status of each replica pod.
	Destinations []JobDestinationStatus `json:"destinations,omitempty"`
	// SucceededCount is the number of successfully restored pods.
	SucceededCount int `json:"succeededCount,omitempty"`
	// FailedCount is the number of failed restore pods.
	FailedCount int `json:"failedCount,omitempty"`
	// Conditions are the restore sub-status conditions.
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// ContainerStateMigrationJobRequestStatus defines the observed state
type ContainerStateMigrationJobRequestStatus struct {
	// Phase is the overall task phase.
	Phase JobPhase `json:"phase,omitempty"`
	// RequestID is the request ID (UUID).
	RequestID string `json:"requestID,omitempty"`
	// Message is the overall status description.
	Message string `json:"message,omitempty"`
	// CheckpointStatus is the checkpoint sub-task status.
	CheckpointStatus *JobCheckpointStatus `json:"checkpointStatus,omitempty"`
	// RestoreStatus is the restore sub-task status.
	RestoreStatus *JobRestoreStatus `json:"restoreStatus,omitempty"`
	// Conditions are the overall conditions.
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=csmjr
// +kubebuilder:printcolumn:name="phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="age",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:printcolumn:name="message",type=string,JSONPath=`.status.message`

// ContainerStateMigrationJobRequest is the Schema for the containerstatemigrationjobrequests API
type ContainerStateMigrationJobRequest struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ContainerStateMigrationJobRequestSpec   `json:"spec,omitempty"`
	Status ContainerStateMigrationJobRequestStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ContainerStateMigrationJobRequestList contains a list of ContainerStateMigrationJobRequest
type ContainerStateMigrationJobRequestList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ContainerStateMigrationJobRequest `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ContainerStateMigrationJobRequest{}, &ContainerStateMigrationJobRequestList{})
}
