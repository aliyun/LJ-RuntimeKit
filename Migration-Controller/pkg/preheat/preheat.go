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

// Package preheat defines the abstract model of the preheat backends: the
// interface, request/status types and all status constants. This is the single
// source of truth for preheat semantics, independent of any concrete backend;
// concrete implementations (dragonfly, EFC, copy...) reference this package
// and align with it - the dragonfly package's TaskState / TaskCacheState are
// aliases of the types here.
package preheat

import (
	"context"
	"errors"
	"strings"

	"github.com/container-migration/controller/api/v1beta1"
)

const (
	// OptionEFCEnable is the option key that selects the preheat backend: the
	// value "true" (case-insensitive) uses EFC preheat; anything else (absent /
	// empty / any other value) falls back to the default Dragonfly preheat.
	// Deliberately not a CRD enum validation: the target cluster is 1.23, and
	// Options is a free map interpreted by the controller at reconcile time.
	OptionEFCEnable = "efc-enable"

	// OptionTargetPvcName / OptionTargetPvcNamespace are the target PVC
	// coordinates of copy preheat: filling both in options (non-empty values)
	// selects the copy backend, which copies the source PVC's checkpoint
	// directory to the same path on the target PVC (see copyBackend). Filling
	// only one is rejected by the controller in the Pending phase instead of
	// silently falling back to Dragonfly. Mutually exclusive with efc-enable.
	OptionTargetPvcName      = "target-pvc-name"
	OptionTargetPvcNamespace = "target-pvc-namespace"

	// BackendDragonfly / BackendEFC / BackendCopy are the names of the three
	// preheat backends, used in logs and status.message so users can tell which
	// path the task actually took.
	BackendDragonfly = "dragonfly"
	BackendEFC       = "efc"
	BackendCopy      = "copy"
)

// StartPreheatRequest describes one preheat start. The preheat source is one
// of RemoteURL (an http directory) or Pvc (a path inside a PVC mounted in the
// pod); how the directory is preheated is decided by the backend itself:
// dragonfly expands the directory into per-part preheats, EFC preheats the
// whole directory, copy copies the source PVC directory wholesale to the
// target PVC.
type StartPreheatRequest struct {
	// Job is the PreheatJob CR that triggered this preheat. The dragonfly
	// backend uses its UID to build the BIO idempotency key and passes
	// spec.options through to the preheat task; the EFC backend uses its
	// spec.checkpointUID (when non-empty, preferred over RemoteURL to re-resolve
	// the preheat directory); the copy backend reads the target PVC coordinates
	// from spec.options.
	Job *v1beta1.PreheatJob
	// RemoteURL is the full http(s) address of the checkpoint **directory**.
	// The dragonfly backend expands it into per-part preheats; the EFC backend
	// preheats the whole directory directly.
	RemoteURL string
	// Pvc is the PVC source coordinates (filled only for PVC-source preheat:
	// spec.pvc filled directly or resolved from checkpointUID, with PvcPath
	// normalized). The dragonfly / EFC backends do not read it; the copy backend
	// creates the copy pod from it (source PVC at /mnt/src, target PVC at
	// /mnt/dst).
	Pvc *v1beta1.PreheatJobPVC
	// AccessKeyID / AccessKeySecret are the origin-fetch credentials, used
	// only by backends whose RequiresStorageCredentials() returns true.
	AccessKeyID     string
	AccessKeySecret string
}

// TaskState is the state of a preheat task.
type TaskState string

const (
	// TaskStatePending: waiting to be scheduled.
	TaskStatePending TaskState = "Pending"
	// TaskStateRunning: in progress.
	TaskStateRunning TaskState = "Running"
	// TaskStateSuccess: succeeded.
	TaskStateSuccess TaskState = "Success"
	// TaskStateFailure: failed.
	TaskStateFailure TaskState = "Failure"
)

// Task is the status snapshot of a preheat task.
type Task struct {
	ID      string
	State   TaskState
	Message string
	// TotalSize is the total bytes of the target path, filled only by the EFC
	// backend (the TotalSize from stat, i.e. the total size of the target on
	// OSS); always 0 for the dragonfly backend.
	TotalSize int64
}

// CacheState is the cache state of the preheated content on a probed target node.
type CacheState string

const (
	// CacheStateNotFound: the task is not registered on the target node at all (never preheated or evicted).
	CacheStateNotFound CacheState = "NotFound"
	// CacheStateRunning: the task is registered and the download is still in progress.
	CacheStateRunning CacheState = "Running"
	// CacheStateFailed: the download failed (e.g. origin 403), nothing was cached.
	CacheStateFailed CacheState = "Failed"
	// CacheStateCached: the content is fully downloaded and served from the local cache.
	CacheStateCached CacheState = "Cached"
)

// ErrNoProbeTargets is returned by ProbeCache when the backend has no
// probeable target node at the moment (e.g. no seed peer in the cluster). The
// caller skips this round without updating status, instead of misjudging the
// task as Stale.
var ErrNoProbeTargets = errors.New("no probe targets available")

// PreheatBackend abstracts the full capability of a preheat backend so the
// PreheatJob controller can drive different preheat services with the same
// state machine (Pending -> Running -> Probing -> Ready/Stale).
//
// Backend differences are fully encapsulated in the implementations: dragonfly
// needs origin credentials, first expands the directory probe into parts and
// then creates a task with a BIO idempotency key for each part; EFC preheats
// the whole directory with one task, needing neither credentials nor BIO; copy
// copies the source PVC's checkpoint directory wholesale to the target PVC via
// a one-shot pod and polls the pod phase, with no cache to probe. The main
// controller flow only faces this interface.
type PreheatBackend interface {
	// Name returns the backend name (dragonfly / efc / copy), used in logs and failure reasons.
	Name() string
	// RequiresStorageCredentials reports whether the backend needs
	// accessKeyId / accessKeySecret to preheat. When true the controller
	// enforces credential validation in the Pending phase and allows backfill
	// from the checkpoint CR; when false (EFC) credentials are neither
	// collected nor validated.
	RequiresStorageCredentials() bool
	// StartPreheat starts preheating the whole checkpoint directory and
	// returns the created task ID list (dragonfly: one per part, EFC: one for
	// the whole directory). How many tasks to split into and how idempotency is
	// guaranteed is decided by the backend; the returned error is written by
	// the controller directly as the failure reason / retry log, so
	// implementations must make the context clear.
	StartPreheat(ctx context.Context, req StartPreheatRequest) ([]string, error)
	// GetPreheatTask queries the current state of a preheat task.
	GetPreheatTask(ctx context.Context, taskID string) (Task, error)
	// TaskLabel returns the readable label of the idx-th task in the list
	// returned by StartPreheat, used in polling logs and failure reasons
	// (dragonfly: part name, EFC: the whole directory, copy: always "copy").
	TaskLabel(idx int) string
	// ProbeCache probes the cache state of the job's preheated content and
	// returns the info of every probed target node plus the aggregated cache
	// state. The preheat source is taken from job.Spec.CheckpointUID /
	// job.Spec.RemoteURL; the caller guarantees at least one is usable (when
	// only checkpointUID is filled the caller has already resolved it back into
	// the in-memory copy). When there is no probeable target at the moment,
	// ErrNoProbeTargets is returned and the caller skips this round without
	// updating status. The copy backend has no cache semantics and always
	// returns ErrNoProbeTargets (normally never called: copy mode skips
	// Probing, and the ProbeController entry skips too).
	ProbeCache(ctx context.Context, job *v1beta1.PreheatJob) ([]v1beta1.PreheatPeerInfo, []CacheState, error)
}

// IsEFCEnabled reports whether the PreheatJob explicitly selected the EFC backend via spec.options.
func IsEFCEnabled(job *v1beta1.PreheatJob) bool {
	return strings.EqualFold(strings.TrimSpace(job.Spec.Options[OptionEFCEnable]), "true")
}

// IsCopyMode reports whether the PreheatJob selected copy preheat (any
// target-pvc coordinate key appears in options). Any key present means copy
// mode: filling only one is rejected by the controller in the Pending phase
// instead of silently falling back to Dragonfly.
func IsCopyMode(job *v1beta1.PreheatJob) bool {
	return TargetPvcName(job) != "" || TargetPvcNamespace(job) != ""
}

// TargetPvcName returns the copy preheat target PVC name (trimmed).
func TargetPvcName(job *v1beta1.PreheatJob) string {
	return strings.TrimSpace(job.Spec.Options[OptionTargetPvcName])
}

// TargetPvcNamespace returns the copy preheat target PVC namespace (trimmed).
func TargetPvcNamespace(job *v1beta1.PreheatJob) string {
	return strings.TrimSpace(job.Spec.Options[OptionTargetPvcNamespace])
}

// SelectBackend selects the preheat backend by job's spec.options: any
// target-pvc coordinate present -> copy; efc-enable=true -> EFC; everything
// else falls back to the default Dragonfly. The reconciler and ProbeController
// share this routing rule.
func SelectBackend(job *v1beta1.PreheatJob, df, efc, cp PreheatBackend) PreheatBackend {
	if IsCopyMode(job) {
		return cp
	}
	if IsEFCEnabled(job) {
		return efc
	}
	return df
}
