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

package handler

// Migration-related pod annotations / container conventions.
//
// restore-enable / restore-data-path are passed through to the OCI spec by
// containerd via pod_annotations; xpu-container-runtime intercepts `runc
// create` and rewrites it into `runc restore` accordingly, and calls XDM to
// complete the restoration of rootfs / memory / xpu first.
const (
	// AnnotationRestoreEnable marks that this pod's containers must go through the restore flow.
	AnnotationRestoreEnable = "migration.io/restore-enable"
	// AnnotationRestoreDataPath is the root directory of the checkpoint data on
	// the target node, layered by container name underneath; each container
	// directory contains the rootfs/ memory/ xpu/ three copies of data.
	AnnotationRestoreDataPath = "migration.io/restore-data-path"
	// AnnotationRestoreFlatContainer names the sole restore target container in
	// flat layout. In flat layout restore-data-path points directly at
	// <hostPath>/data (with rootfs/ memory/ xpu/ underneath and no
	// <container>/checkpoint middle layer). Since restore-data-path is a
	// pod-level annotation seen by both the sandbox and the injected init
	// container, flat layout lacks the implicit "per-container layering"
	// distinction, so the target container must be named explicitly.
	AnnotationRestoreFlatContainer = "migration.io/restore-flat-container"
	// AnnotationRestoreSucceeded is patched onto the pod by the controller after
	// the replica fully restored (container Running + restoreAttempted + XPU
	// state restored). When kubelet restarts this pod afterwards,
	// xpu-container-runtime sees it and adds restore_xpu=true to the RestoreCPU
	// options, letting XDM drive cr-tool to re-restore on its own (the
	// controller no longer observes this pod by then). The value must match the
	// annotationRestoreSucceeded constant on the xpu-container-runtime side
	// exactly.
	AnnotationRestoreSucceeded = "migration.io/restore-succeeded"
	// AnnotationRestoreSucceededAt is patched in the same patch as
	// AnnotationRestoreSucceeded, carrying the RFC3339 timestamp of the first
	// successful restore, purely for human troubleshooting (the runtime does
	// not read it). Later re-restores bypass the controller and never update
	// it.
	AnnotationRestoreSucceededAt = "migration.io/restore-succeeded-at"
	// AnnotationCheckpointJobUID records the UID of the CR that created this pod, for reverse lookup in troubleshooting.
	AnnotationCheckpointJobUID = "migration.io/migration-job-uid"
	// AnnotationColdStartFallback marks this pod as the cold-start fallback pod
	// after retries were exhausted - it carries no restore annotation, no
	// migration-injected env, and starts normally; purely a marker (for
	// identification and troubleshooting).
	AnnotationColdStartFallback = "migration.io/cold-start-fallback"
)

// Migration injection switch env vars carried by the source pod. Cold-start
// fallback pods do not participate in migration (no checkpoint session will
// release the migration stack's steady-state hang), so they must be stripped
// when building, otherwise the app hangs at the migration stack's steady-state
// point at startup and never serves (health=000, no port listening observed in
// practice). Only affects the cold-start path: restore pods keep all migration
// env, migration capability unaffected.
const (
	// EnvKubergpuMigrationEnable triggers harp migration stack injection (libhggc_cr.so etc.).
	EnvKubergpuMigrationEnable = "KUBERGPU_MIGRATION_ENABLE"
	// EnvAmpVgpuEnable is the vGPU allocation mode switch, injected together with the migration stack.
	EnvAmpVgpuEnable = "AMP_VGPU_ENABLE"
)

// LabelMigrationJobUID identifies the migration task a replica pod belongs to.
// The target pod carries the source pod's labels as-is (so it keeps being
// selected by the original Service); labels alone cannot tell which pods were
// migrated, so this extra marker is applied.
const LabelMigrationJobUID = "migration.io/migration-job-uid"

// extraKeyRestoreAttempted is the key the worker reports in the Extras of
// QueryRestoreFromStorageStatus to state "whether xpu-container-runtime really
// took over the creation of this container".
//
// This is the only positive proof of whether the restore truly took effect -
// a Running container cannot tell whether it was restored from the checkpoint
// or started from scratch. The value must match controller.ExtraKeyRestoreAttempted
// on the migration-worker side.
const extraKeyRestoreAttempted = "restoreAttempted"

// The init container blocking mechanism. Community K8s does not allow
// modifying a pod's container env / command after creation, so during the
// restore data preparation stage a blocking init container holds back the main
// container creation; once the data is ready the controller writes the signal
// file via pods/exec to release it.
const (
	// InitContainerName is the name of the blocking init container.
	InitContainerName = "migration-init-signal"
	// SignalVolumeName is the emptyDir carrying the signal file.
	SignalVolumeName = "init-signal"
	// SignalMountPath is the signal file mount point.
	SignalMountPath = "/signal"
	// SignalFileName is the signal file name; its presence means data is ready.
	SignalFileName = "ready"
)

// maxPodNameLength is the pod name (DNS-1123 subdomain) length limit.
const maxPodNameLength = 253

// MigrationJobFinalizer guarantees the CR gets a chance to clean up the
// process data on both node sides before deletion. Cleanup is best-effort: no
// failure blocks deletion, otherwise one bad node could leave the CR stuck in
// Terminating forever.
const MigrationJobFinalizer = "migration.io/cleanup"

// advanceOptKeyCheckpointHostPath is passed through policy.advanceOptions to
// worker -> XDM as the migration working directory hostPath; it must match
// optCheckpointHostPath on the X-Device-Manager side.
const advanceOptKeyCheckpointHostPath = "checkpoint_host_path"

// advanceOptKeyCheckpointTimeout is passed through policy.advanceOptions to
// worker -> XDM as the cr-tool timeout in minutes; it must match
// optCheckpointTimeout on the X-Device-Manager side. It is injected only when
// spec.checkpoint.timeoutInMinute is explicitly set (0 included); when unset
// the key is absent and XDM omits --timeout from the cr-tool command line.
const advanceOptKeyCheckpointTimeout = "checkpoint_timeout"

// advanceOptKeyCheckpointAutoMode is passed through policy.advanceOptions to
// worker -> XDM as the cr-tool auto-mode switch; it must match
// optCheckpointAutoMode on the X-Device-Manager side. Injected as "true" when
// spec.checkpoint.enableCheckpointAutoMode is unset (defaults to true) or
// true; absent when explicitly false, and XDM omits --enable-auto-mode from
// the cr-tool command line.
const advanceOptKeyCheckpointAutoMode = "checkpoint_auto_mode"

// runtimekitMigrationVolumeName is the volume name of the migration-dedicated hostPath mount on the source pod.
const runtimekitMigrationVolumeName = "lj-runtimekit-migration"
