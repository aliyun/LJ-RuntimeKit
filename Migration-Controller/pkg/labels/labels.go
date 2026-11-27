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

package labels

const (
	// LabelComponent is a generic label key for component identification
	LabelComponent = "component"

	// LabelContainerStateMigrationConfigMap identifies ConfigMaps that contain migration configuration
	LabelContainerStateMigrationConfigMap = "migration.io/container-state-migration-config"

	// MigrationWorkerComponentName is the component name for migration worker pods
	MigrationWorkerComponentName = "lj-runtimekit-migration-agent"

	// DriverName is the CSI driver name for checkpoint/migration
	DriverName = "checkpoint.csi.migration.io"

	// DriverVersion is the CSI driver version
	DriverVersion = "1.0.0"

	// MigrationRestartEnvName is the env var name for restore version tracking
	MigrationRestartEnvName = "CONTAINER_STATE_RESTORE_VERSION"

	// CheckpointID is the pouch-compatible checkpoint identifier
	CheckpointID = "checkpoint"

	// SuperAdmin is the reserved admin user name
	SuperAdmin = "admin"

	// AnnotationIgnoreWorkerCheck skips worker pod health checks when set to "true"
	AnnotationIgnoreWorkerCheck = "migration.io/ignore-worker-check"
)

// The automatic checkpoint protocol: declared on business pod annotations,
// read by the pod controller, used to auto-create a
// ContainerStateMigrationJobRequest that runs the checkpoint stage and, when
// restore-replicas is declared as a positive integer, the restore stage as
// well.
const (
	// AnnotationCheckpointEnable is the only admission entry; "true" auto-creates the checkpoint CR.
	AnnotationCheckpointEnable = "migration.io/container-state-checkpoint-enable"

	// AnnotationStorageType is the storage type: local / nfs / oss / dragonfly / efc / pvc.
	AnnotationStorageType = "migration.io/storage-type"

	// AnnotationStorageLocalPath is the storage root directory of the local backend (absolute path).
	AnnotationStorageLocalPath = "migration.io/storage-local-path"

	// AnnotationStorageNFSServer is the server address of the nfs backend.
	AnnotationStorageNFSServer = "migration.io/storage-nfs-server"
	// AnnotationStorageNFSPath is the export path of the nfs backend.
	AnnotationStorageNFSPath = "migration.io/storage-nfs-path"
	// AnnotationStorageNFSOptions are the nfs mount options, format k1=v1,k2=v2.
	AnnotationStorageNFSOptions = "migration.io/storage-nfs-options"

	// AnnotationStorageOSSEndpoint is the endpoint of the oss backend.
	AnnotationStorageOSSEndpoint = "migration.io/storage-oss-endpoint"
	// AnnotationStorageOSSMountPoint is the oss mount point, format oss://bucket/path.
	AnnotationStorageOSSMountPoint = "migration.io/storage-oss-mount-point"
	// AnnotationStorageOSSOptions are extra oss options, format k1=v1,k2=v2, including accessKeyId / accessKeySecret.
	AnnotationStorageOSSOptions = "migration.io/storage-oss-options"

	// AnnotationStorageDragonflyOSSEndpoint is the endpoint of the dragonfly origin OSS.
	AnnotationStorageDragonflyOSSEndpoint = "migration.io/storage-dragonfly-oss-endpoint"
	// AnnotationStorageDragonflyOSSMountPoint is the dragonfly origin OSS mount point, format oss://bucket/path.
	AnnotationStorageDragonflyOSSMountPoint = "migration.io/storage-dragonfly-oss-mount-point"
	// AnnotationStorageDragonflyOptions are extra dragonfly options, format k1=v1,k2=v2, including accessKeyId / accessKeySecret.
	AnnotationStorageDragonflyOptions = "migration.io/storage-dragonfly-options"

	// AnnotationStorageEFCBucketName is the bucket holding the checkpoint under efc.
	AnnotationStorageEFCBucketName = "migration.io/storage-efc-bucket-name"
	// AnnotationStorageEFCOptions are extra efc options, format k1=v1,k2=v2.
	AnnotationStorageEFCOptions = "migration.io/storage-efc-options"

	// AnnotationStoragePVCName is the PVC name of the pvc backend.
	AnnotationStoragePVCName = "migration.io/storage-pvc-name"
	// AnnotationStoragePVCNamespace is the namespace the PVC lives in.
	AnnotationStoragePVCNamespace = "migration.io/storage-pvc-namespace"

	// AnnotationCheckpointContainers is the list of container names to checkpoint, format c1,c2.
	AnnotationCheckpointContainers = "migration.io/checkpoint-containers"
	// AnnotationCheckpointTimeoutMinutes is the checkpoint timeout in minutes,
	// passed through to cr-tool as --timeout. Only read when declared: unset
	// means no --timeout is passed to cr-tool at all. An explicit 0 blocks the
	// checkpoint stage on the auto-created CR with a warning; negative or
	// non-integer values are reported on the CR's status.message.
	AnnotationCheckpointTimeoutMinutes = "migration.io/checkpoint-timeout-minutes"
	// AnnotationCheckpointAutoModeEnable controls whether cr-tool runs in auto
	// mode with --enable-auto-mode, detecting the steady-state point of the
	// model service to trigger the checkpoint; true / false, defaults to true
	// when undeclared.
	AnnotationCheckpointAutoModeEnable = "migration.io/checkpoint-automode-enable"
	// AnnotationCheckpointPolicy is the JSON content of containerCheckpointPolicy.
	AnnotationCheckpointPolicy = "migration.io/checkpoint-policy"

	// AnnotationRestoreReplicas is the replica count of the auto-created CR's
	// restore stage: a positive integer triggers restore after checkpoint, 0
	// or undeclared keeps the CR checkpoint-only.
	AnnotationRestoreReplicas = "migration.io/restore-replicas"
	// AnnotationRestoreRetryCount is the max retry count of the restore stage,
	// same semantics as spec.restore.retryCount; only read when
	// restore-replicas is a positive integer.
	AnnotationRestoreRetryCount = "migration.io/restore-retry-count"
	// AnnotationRestoreDisableColdStartFallback turns off the cold-start
	// fallback of the auto-created CR's restore stage (mirrors
	// spec.restore.disableColdStartFallback); only "true" disables it, and it
	// is only read when restore-replicas is a positive integer.
	AnnotationRestoreDisableColdStartFallback = "migration.io/restore-disable-cold-start-fallback"

	// AnnotationAutoCreated marks that this CR was auto-created by the pod controller.
	AnnotationAutoCreated = "migration.io/auto-created"
	// AnnotationAutoCheckpointInvalidTimeout carries the original illegal
	// checkpoint-timeout-minutes annotation value (non-integer) onto the
	// auto-created CR, so InitHandler can report it on status.message - the
	// int field cannot express the raw input.
	AnnotationAutoCheckpointInvalidTimeout = "migration.io/auto-checkpoint-invalid-timeout"
	// AnnotationAutoCheckpointInvalidAutoMode carries the original illegal
	// checkpoint-automode-enable annotation value (not true or false) onto
	// the auto-created CR, so InitHandler can report it on status.message -
	// the bool field cannot express the raw input.
	AnnotationAutoCheckpointInvalidAutoMode = "migration.io/auto-checkpoint-invalid-automode"
	// AnnotationRestoreCreated marks that this pod was created by this project's restore, used to block the cascade checkpoint.
	AnnotationRestoreCreated = "migration.io/restore-created"

	// OSSAccessKeyID / OSSAccessKeySecret are the credential key names of the
	// storage backend, case-sensitive and must match the Migration-Worker
	// side.
	OSSAccessKeyID     = "accessKeyId"
	OSSAccessKeySecret = "accessKeySecret"

	// MigrationWorkerComponentNameAlias is the component value actually used in
	// the worker pod template, used together with MigrationWorkerComponentName
	// to identify worker pods.
	MigrationWorkerComponentNameAlias = "lj-runtimekit-migration-worker"

	// LabelWorkerConfigHash marks the fingerprint of the worker config (template
	// + grpcPort) the worker pod was created from. The controller compares it
	// with the current config fingerprint before dispatching a task; a mismatch
	// means config drift.
	LabelWorkerConfigHash = "migration.io/worker-config-hash"
	// AnnotationWorkerConfigSource records the ConfigMap coordinates and
	// version that produced this worker config, format
	// <namespace>/<name>@<resourceVersion>, only for troubleshooting which
	// config the worker is using.
	AnnotationWorkerConfigSource = "migration.io/worker-config-source"

	// LabelPreheatSourceJobUID is the dedicated marker of the preheat source
	// pod, valued with the owning PreheatJob's UID. The preheat source Service
	// has been changed to selector-less with a manual EndpointSlice (the
	// backend is declared by the registrant writing the slice itself); this
	// label no longer takes part in addressing and is only used to identify the
	// preheat source pod (e.g. port / residue troubleshooting).
	LabelPreheatSourceJobUID = "migration.io/preheat-source-job-uid"
)
