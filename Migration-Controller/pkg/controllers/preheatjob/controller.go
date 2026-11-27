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

// Package preheatjob provides the controller for PreheatJob resources.
//
// The controller watches PreheatJob CRs and drives the preheat lifecycle.
// The backend is selected from spec.options: target-pvc coordinates -> copy,
// efc-enable=true -> EFC, otherwise the default Dragonfly. All three backends
// share one state machine (see preheat.PreheatBackend in pkg/preheat and the
// implementations in backend.go / efc_backend.go / copy_backend.go).
package preheatjob

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/container-migration/controller/api/v1beta1"
	"github.com/container-migration/controller/pkg/controllers"
	"github.com/container-migration/controller/pkg/dragonfly"
	"github.com/container-migration/controller/pkg/preheat"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// DefaultTaskPollInterval is the default poll interval for preheat task status.
	DefaultTaskPollInterval = 3 * time.Second

	// OptionAccessKeyID is the OSS accessKeyId key in options.
	OptionAccessKeyID = "accessKeyId"
	// OptionAccessKeySecret is the OSS accessKeySecret key in options.
	OptionAccessKeySecret = "accessKeySecret"

	// MaxCreateRetries is the max retry count for creating preheat tasks.
	MaxCreateRetries = 5
	// MaxPollRetries is the max retry count for polling preheat task status.
	MaxPollRetries = 10

	// createRetryAnnotation records the retry count of preheat task creation.
	createRetryAnnotation = "preheatjob.migration.io/create-retries"
	// pollRetryAnnotation records the retry count of preheat task status polling.
	pollRetryAnnotation = "preheatjob.migration.io/poll-retries"

	// taskIDSeparator separates the packed per-part task IDs in status.TaskID.
	taskIDSeparator = ","

	// preheatJobFinalizer deletes leftover preheat pods on CR deletion.
	preheatJobFinalizer = "preheatjob.migration.io/finalizer"

	// sourcePodNamespaceAnnotation records the namespace of the preheat source pod
	// (= the source PVC namespace); markFailed and CR deletion locate it via this.
	sourcePodNamespaceAnnotation = "preheatjob.migration.io/source-pod-namespace"

	// podWaitRetryAnnotation records the retry count while waiting for the preheat
	// source pod to become ready (bounded by MaxPreheatSourcePodRetries).
	podWaitRetryAnnotation = "preheatjob.migration.io/pod-wait-retries"

	// MaxPreheatSourcePodRetries is the retry budget for waiting on the preheat source pod.
	MaxPreheatSourcePodRetries = 30

	// partRetryAnnotation records per-part failure retry counts, encoded as
	// "idx:count,idx2:count2" (part index <-> taskIDs position).
	partRetryAnnotation = "preheatjob.migration.io/part-retries"

	// MaxPartRetries is the max retry count per failed part. Transient source-side
	// failures recover on retry and must not fail the whole job.
	MaxPartRetries = 2

	// remoteURLCredsAnnotation pins the OSS credentials actually used for preheat
	// as "ak:sk". When spec only fills checkpointUID the credentials are resolved
	// from the checkpoint CR and not written back to spec; part retries in Running
	// phase need them.
	remoteURLCredsAnnotation = "preheatjob.migration.io/remote-url-creds"

	// efcExpectedSizeAnnotation pins the data-integrity baseline for EFC preheat
	// (the total checkpoint size recorded by the checkpoint CR). In Running phase
	// it is compared with the warmup stat TotalSize; a mismatch means the ossfs
	// async upload has not finished and the cache holds incomplete data.
	efcExpectedSizeAnnotation = "preheatjob.migration.io/efc-expected-size"

	// minRetryInterval is the minimum real-time gap between two retry counts.
	// Writing an annotation triggers a watch event and a new reconcile immediately;
	// without this gap the retry budget would be exhausted in milliseconds.
	minRetryInterval = 5 * time.Second
)

// PreheatJobReconciler reconciles a PreheatJob object
type PreheatJobReconciler struct {
	scheme        *runtime.Scheme
	sharedContext *controllers.SharedContext

	// The three backends, selected by backendFor from spec.options.
	dragonfly preheat.PreheatBackend
	efc       preheat.PreheatBackend
	copy      preheat.PreheatBackend

	// lastLoggedPhase records the phase when the START log was last emitted per
	// PreheatJob. ProbeController probes ready tasks every 10s and writes status,
	// waking this controller each time while the phase stays put; this map limits
	// logging to real phase transitions.
	//
	// With MaxConcurrentReconciles>1 different objects may enter Reconcile
	// concurrently, so the shared map is guarded by a lock; controller-runtime
	// never reconciles the same object concurrently.
	lastLoggedPhase     map[types.NamespacedName]v1beta1.PreheatJobPhase
	lastLoggedPhaseLock sync.Mutex
}

// NewPreheatJobReconciler creates a new PreheatJobReconciler instance.
func NewPreheatJobReconciler(scheme *runtime.Scheme, sharedContext *controllers.SharedContext, df dragonfly.Client) *PreheatJobReconciler {
	return &PreheatJobReconciler{
		scheme:        scheme,
		sharedContext: sharedContext,
		// The reconciler only drives task lifecycle; probe dependencies
		// (seedPeerSource/prober) live in the other backend instance held by
		// ProbeController.
		dragonfly: dragonflyBackend{Client: df},
		// The EFC backend talks to the per-node warmup HTTP endpoint and only
		// needs the K8s client (list nodes + resolve checkpointUID).
		efc: newEFCBackend(sharedContext.Client),
		// The copy backend creates/polls/deletes one-shot copy pods via SharedContext.
		copy:            copyBackend{sharedContext: sharedContext},
		lastLoggedPhase: make(map[types.NamespacedName]v1beta1.PreheatJobPhase),
	}
}

// backendFor selects the backend from spec.options: target-pvc coordinates ->
// copy, efc-enable=true -> EFC, everything else falls back to Dragonfly.
func (r *PreheatJobReconciler) backendFor(job *v1beta1.PreheatJob) preheat.PreheatBackend {
	return preheat.SelectBackend(job, r.dragonfly, r.efc, r.copy)
}

// GetName returns the name of this controller.
func (r *PreheatJobReconciler) GetName() string {
	return "PreheatJobController"
}

// +kubebuilder:rbac:groups=migration.io,resources=preheatjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=migration.io,resources=preheatjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=migration.io,resources=preheatjobs/finalizers,verbs=update
// +kubebuilder:rbac:groups=migration.io,resources=containerstatemigrationjobrequests,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop.
func (r *PreheatJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	ctxLog := log.FromContext(ctx)

	job := &v1beta1.PreheatJob{}
	err := r.sharedContext.Client.Get(ctx, req.NamespacedName, job)
	if errors.IsNotFound(err) {
		// Object deleted: forget its phase to keep the map bounded (a recreated
		// CR with the same name gets a fresh START log).
		r.forgetPhase(req.NamespacedName)
		return ctrl.Result{}, nil
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	mutatedJob := job.DeepCopy()

	// CR deletion: best-effort delete the leftover preheat source pod / copy pod
	// before removing the finalizer (both pods mount the user's PVC; a leftover
	// pod holds the PVC mount indefinitely, and the source pod also holds the
	// http port).
	if !mutatedJob.DeletionTimestamp.IsZero() {
		if preheat.IsCopyMode(mutatedJob) {
			// Copy mode never creates a preheat source pod, only a copy pod.
			if ns := preheat.TargetPvcNamespace(mutatedJob); ns != "" {
				if err := r.sharedContext.DeletePreheatCopyPod(ctx, string(mutatedJob.UID), ns); err != nil {
					ctxLog.Error(err, "failed to delete preheat copy pod during PreheatJob deletion")
					return ctrl.Result{}, err
				}
			}
		} else if ns := r.sourcePodNamespace(mutatedJob); ns != "" {
			if err := r.sharedContext.DeletePreheatSourcePod(ctx, string(mutatedJob.UID), ns); err != nil {
				ctxLog.Error(err, "failed to delete preheat source pod during PreheatJob deletion")
				return ctrl.Result{}, err
			}
		}
		if controllerutil.ContainsFinalizer(mutatedJob, preheatJobFinalizer) {
			controllerutil.RemoveFinalizer(mutatedJob, preheatJobFinalizer)
			if err := r.sharedContext.Client.Update(ctx, mutatedJob); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Only log START when the phase changed since the last logged observation.
	// ProbeController wakes this controller every 10s; in steady state the phase
	// is unchanged and no log is emitted, only real transitions log a line.
	phaseChanged := r.observePhaseForLog(req.NamespacedName, mutatedJob.Status.Phase)
	if phaseChanged {
		ctxLog.Info(fmt.Sprintf("=== Reconcile START %s phase=%q ===", req.NamespacedName, mutatedJob.Status.Phase))
	}

	// Init: a new CR defaults to Pending and gets the finalizer so that deletion
	// can clean up the preheat source pod.
	if mutatedJob.Status.Phase == "" {
		if !controllerutil.ContainsFinalizer(mutatedJob, preheatJobFinalizer) {
			controllerutil.AddFinalizer(mutatedJob, preheatJobFinalizer)
			if err := r.sharedContext.Client.Update(ctx, mutatedJob); err != nil {
				ctxLog.Error(err, "Failed to add finalizer to PreheatJob")
				return ctrl.Result{}, err
			}
		}
		if err := r.updateStatus(ctx, mutatedJob, func(j *v1beta1.PreheatJob) {
			j.Status.Phase = v1beta1.PreheatJobPhasePending
		}); err != nil {
			ctxLog.Error(err, "Failed to initialize PreheatJob phase to Pending")
			return ctrl.Result{}, err
		}
		ctxLog.Info(fmt.Sprintf("Initialized PreheatJob %s phase to Pending", req.NamespacedName))
		return ctrl.Result{}, nil
	}

	switch mutatedJob.Status.Phase {
	case v1beta1.PreheatJobPhasePending:
		return r.handlePending(ctx, mutatedJob)
	case v1beta1.PreheatJobPhaseRunning:
		return r.handleRunning(ctx, mutatedJob)
	case v1beta1.PreheatJobPhaseProbing:
		// Probing -> Ready/Stale is driven by ProbeController probing the actual
		// cache state; the main controller only checks that a TaskID exists.
		if mutatedJob.Status.TaskID == "" {
			return r.markFailed(ctx, mutatedJob, "taskID is missing in Probing state")
		}
		if phaseChanged {
			ctxLog.Info(fmt.Sprintf("PreheatJob %s phase=%s, waiting for ProbeController", req.NamespacedName, mutatedJob.Status.Phase))
		}
		return ctrl.Result{}, nil
	case v1beta1.PreheatJobPhaseStale:
		// Stale is recovered to Ready or kept Stale by ProbeController based on
		// cache state; the main controller only fails the job if TaskID is lost.
		if mutatedJob.Status.TaskID == "" {
			return r.markFailed(ctx, mutatedJob, "taskID is missing in Stale state")
		}
		if phaseChanged {
			ctxLog.Info(fmt.Sprintf("PreheatJob %s phase=%s, managed by ProbeController", req.NamespacedName, mutatedJob.Status.Phase))
		}
		return ctrl.Result{}, nil
	case v1beta1.PreheatJobPhaseReady:
		if preheat.IsCopyMode(mutatedJob) {
			// Copy mode: data is already on the target PVC; delete the copy pod
			// to release the mount of both PVCs (copy mode builds no preheat
			// source pod and no Service).
			if ns := preheat.TargetPvcNamespace(mutatedJob); ns != "" {
				if err := r.sharedContext.DeletePreheatCopyPod(ctx, string(mutatedJob.UID), ns); err != nil {
					ctxLog.Error(err, "failed to delete preheat copy pod after Ready")
					return ctrl.Result{}, err
				}
			}
		} else if mutatedJob.Spec.Pvc != nil || mutatedJob.Spec.CheckpointUID != "" {
			// Release the preheat source pod after Ready (idempotent): during
			// restore, source reachability is provided by the restore workers
			// double-hosting the source (same PVC mount, self-registered
			// EndpointSlice), so the source pod is no longer needed; its
			// EndpointSlice is GC'd via ownerReference, the Service stays.
			if ns := r.sourcePodNamespace(mutatedJob); ns != "" {
				if err := r.sharedContext.DeletePreheatSourcePod(ctx, string(mutatedJob.UID), ns); err != nil {
					ctxLog.Error(err, "failed to delete preheat source pod after Ready")
					return ctrl.Result{}, err
				}
			}
		}
		if phaseChanged {
			ctxLog.Info(fmt.Sprintf("PreheatJob %s is Ready", req.NamespacedName))
		}
		return ctrl.Result{}, nil
	default:
		if phaseChanged {
			ctxLog.Info(fmt.Sprintf("PreheatJob %s phase=%s, skip processing", req.NamespacedName, mutatedJob.Status.Phase))
		}
		return ctrl.Result{}, nil
	}
}

// observePhaseForLog records the observed phase and reports whether it changed
// since the last logged observation. The first sight of an object counts as a
// change (worth a START log).
func (r *PreheatJobReconciler) observePhaseForLog(key types.NamespacedName, phase v1beta1.PreheatJobPhase) bool {
	r.lastLoggedPhaseLock.Lock()
	defer r.lastLoggedPhaseLock.Unlock()
	if prev, ok := r.lastLoggedPhase[key]; ok && prev == phase {
		return false
	}
	r.lastLoggedPhase[key] = phase
	return true
}

// forgetPhase drops the record after the object is deleted.
func (r *PreheatJobReconciler) forgetPhase(key types.NamespacedName) {
	r.lastLoggedPhaseLock.Lock()
	defer r.lastLoggedPhaseLock.Unlock()
	delete(r.lastLoggedPhase, key)
}

// handlePending validates the spec, resolves the preheat source, starts the
// preheat for the whole checkpoint directory via the selected backend, and
// moves the phase to Running.
//
// spec.remoteURL points to a checkpoint **directory** (containing part-00.tar
// ...), not a single object. How the directory is preheated is up to the
// backend: dragonfly fans it out into N independent tasks (requires AK/SK
// origin credentials, idempotent via BIO, see dragonflyBackend.StartPreheat);
// EFC preheats the whole directory as one task, no credentials, no BIO; copy
// uses a one-shot pod to copy the source PVC directory to the target PVC as a
// whole (single task, no credentials, no BIO, see copyBackend).
//
// spec may fill only checkpointUID: the missing remoteURL (and OSS credentials
// for backends that need them) is then resolved from the checkpoint CR at
// reconcile time, used in memory only, never written back to spec.
func (r *PreheatJobReconciler) handlePending(ctx context.Context, job *v1beta1.PreheatJob) (ctrl.Result, error) {
	ctxLog := log.FromContext(ctx)

	// The preheat source is exactly one of: remoteURL / checkpointUID / pvc.
	sources := 0
	if job.Spec.RemoteURL != "" {
		sources++
	}
	if job.Spec.CheckpointUID != "" {
		sources++
	}
	if job.Spec.Pvc != nil {
		sources++
	}
	if sources == 0 {
		return r.markFailed(ctx, job, "one of remoteURL, checkpointUID or pvc is required")
	}
	if sources > 1 {
		return r.markFailed(ctx, job, "remoteURL, checkpointUID and pvc are mutually exclusive, fill exactly one of them")
	}

	backend := r.backendFor(job)

	remoteURL := job.Spec.RemoteURL
	accessKeyID := job.Spec.Options[OptionAccessKeyID]
	accessKeySecret := job.Spec.Options[OptionAccessKeySecret]
	var pvcSrc *v1beta1.PreheatJobPVC
	// pvcPath is the PVC-internal path normalized by validRelPath: dragonfly
	// composes the remoteURL from it, the copy backend passes it verbatim to the
	// copy pod script; normalization keeps both sides consistent on "same path".
	pvcPath := ""

	// With checkpointUID filled, resolve the preheat source from the checkpoint
	// CR: storageType=pvc resolves the three PVC fields; oss/dragonfly resolves
	// remoteURL and OSS credentials. The result is never written back to spec.
	if job.Spec.CheckpointUID != "" {
		src, err := resolveSourceFromCheckpoint(ctx, r.sharedContext.Client, job)
		if err != nil {
			// A missing checkpoint, no data path yet, or a storage backend that
			// cannot compose a URL are all user-input problems: fail immediately
			// with the reason in the message, no wait-and-retry.
			return r.markFailed(ctx, job, err.Error())
		}
		if src.IsPVC() {
			pvcSrc = src.PVC()
			// Data-integrity baseline for the EFC backend: total size recorded by
			// the checkpoint CR vs warmup stat TotalSize. Pinned in an annotation
			// for the Running phase.
			if src.checkpointSize > 0 {
				if err := r.recordEFCExpectedSize(ctx, job, src.checkpointSize); err != nil {
					ctxLog.Error(err, "Failed to record EFC expected size annotation")
					return ctrl.Result{}, err
				}
			}
			ctxLog.Info(fmt.Sprintf("PreheatJob %s/%s resolved PVC source from checkpoint %s",
				job.Namespace, job.Name, job.Spec.CheckpointUID),
				"pvc", fmt.Sprintf("%s/%s", pvcSrc.PvcNamespace, pvcSrc.PvcName), "pvcPath", pvcSrc.PvcPath)
		} else {
			if remoteURL == "" {
				remoteURL = src.remoteURL
			}
			if backend.RequiresStorageCredentials() {
				if accessKeyID == "" {
					accessKeyID = src.accessKeyID
				}
				if accessKeySecret == "" {
					accessKeySecret = src.accessKeySecret
				}
			}
			ctxLog.Info(fmt.Sprintf("PreheatJob %s/%s resolved remoteURL from checkpoint %s",
				job.Namespace, job.Name, job.Spec.CheckpointUID),
				"remoteURL", remoteURL)
		}
	}
	if job.Spec.Pvc != nil {
		pvcSrc = job.Spec.Pvc
	}

	// PVC source: the dragonfly backend ensures the preheat source pod is ready
	// and composes the http remoteURL (a plain http address, no AK/SK needed -
	// CreatePreheatTask does not sign non-OSS URLs); the EFC backend builds no
	// pod; the copy backend builds neither pod nor URL (its copy pod is created
	// by StartPreheat).
	if pvcSrc != nil {
		if pvcSrc.PvcName == "" || pvcSrc.PvcNamespace == "" {
			return r.markFailed(ctx, job, "pvc.pvcName and pvc.pvcNamespace are required")
		}
		// Structural validation of pvcPath: the value flows into the preheat
		// source pod shell script, the http remoteURL, the EFC warmup
		// target_path and the copy pod copy script. "."/".." segments, empty
		// segments and backslashes are rejected at the entry (both sources -
		// spec.pvc and checkpointUID resolution - funnel through here). Any
		// other character is legal inside a segment: checkpoint data may be
		// moved to user directories; escaping is handled at each consumer (see
		// validRelPath).
		var err error
		pvcPath, err = validRelPath(pvcSrc.PvcPath)
		if err != nil {
			return r.markFailed(ctx, job, fmt.Sprintf("pvc.pvcPath %q is invalid: %v", pvcSrc.PvcPath, err))
		}
		if pvcPath == "" {
			return r.markFailed(ctx, job, "pvc.pvcPath is required")
		}
		// RWX check of the preheat source PVC differs per mode: dragonfly/EFC
		// preheat data serves multi-node restores, so RWX is mandatory; a copy
		// source is read by a single copy pod, RWO is fine (same rule as the
		// single-node checkpoint write).
		pvc := &corev1.PersistentVolumeClaim{}
		if err := r.sharedContext.Client.Get(ctx, client.ObjectKey{
			Namespace: pvcSrc.PvcNamespace,
			Name:      pvcSrc.PvcName,
		}, pvc); err != nil {
			if errors.IsNotFound(err) {
				return r.markFailed(ctx, job, fmt.Sprintf("PVC %s/%s not found",
					pvcSrc.PvcNamespace, pvcSrc.PvcName))
			}
			return ctrl.Result{}, err
		}
		if backend.Name() != preheat.BackendCopy && !hasReadWriteMany(pvc.Spec.AccessModes) {
			return r.markFailed(ctx, job, fmt.Sprintf(
				"PVC %s/%s is not RWX (ReadWriteMany): preheat requires a ReadWriteMany PVC so preheat data can serve restore replicas on multiple nodes",
				pvcSrc.PvcNamespace, pvcSrc.PvcName))
		}
		// Only the dragonfly backend needs a preheat source pod (to produce the
		// http remoteURL): the EFC backend's target_path is derived from the PVC
		// (PV.path prefix + pvcPath) and goes straight through the warmup HTTP
		// endpoint; the copy backend's pod is created by StartPreheat, and copy
		// mode deliberately builds no Service - restore falls back to reading
		// the target PVC directly when no Service exists.
		if backend.Name() == preheat.BackendDragonfly {
			serviceIP, ready, err := r.sharedContext.EnsurePreheatSourcePod(ctx,
				string(job.UID), pvcSrc.PvcName, pvcSrc.PvcNamespace, pvcPath)
			if err != nil {
				return r.retryOrFail(ctx, job, createRetryAnnotation, MaxCreateRetries, err.Error())
			}
			if !ready {
				return r.retryOrFail(ctx, job, podWaitRetryAnnotation, MaxPreheatSourcePodRetries,
					"waiting for preheat source pod to become ready (PVC mount may be blocked on the scheduled node)")
			}
			// The preheat source pod uses pod networking + a fixed port; the
			// remoteURL uses the persistent Service ClusterIP (hashed from PVC
			// coordinates + path, shared by the preheat and restore phases).
			// pvcPath may contain spaces, "?", "#", "%" etc. (users may move
			// checkpoint data to their own directories); naive string
			// concatenation would break the URL structure. Building with
			// url.URL lets String() escape per encodePath rules while the
			// server-side http.FileServer sees the decoded original value - a
			// lossless round trip.
			remoteURL = (&url.URL{
				Scheme: "http",
				Host:   net.JoinHostPort(serviceIP, strconv.Itoa(controllers.PreheatSourceHTTPPort)),
				Path:   "/" + pvcPath,
			}).String()
			// Record the preheat source pod namespace so markFailed, the Ready
			// release and CR deletion (finalizer) can locate it; the source pod is
			// deleted after phase=Ready (restore workers double-host the source).
			if err := r.recordSourcePodNamespace(ctx, job, pvcSrc.PvcNamespace); err != nil {
				ctxLog.Error(err, "Failed to record preheat source pod namespace")
				return ctrl.Result{}, err
			}
			// Pre-read the PVC data once the source pod is ready: when the data is
			// cold this moves the slow OSS cold reads out of the dragonfly download
			// phase (8-way random concurrency, slow and prone to per-part timeouts)
			// into a sequential bulk pre-read phase (see warmupPreheatSourcePod).
			// Pre-read failure never blocks preheat.
			r.warmupPreheatSourcePod(ctx, job, pvcSrc)
		}
	}

	// Copy preheat (options containing target-pvc-*): dedicated entry validation,
	// after which the copy backend creates the copy pod in StartPreheat.
	if preheat.IsCopyMode(job) {
		if handled, result, err := r.validateCopyPreheat(ctx, job, pvcSrc); handled {
			return result, err
		}
	}

	// The EFC backend composes target_path from the PVC, the copy backend reads
	// the source PVC directly inside the copy pod; neither needs remoteURL.
	if remoteURL == "" && !(pvcSrc != nil && (backend.Name() == preheat.BackendEFC || backend.Name() == preheat.BackendCopy)) {
		return r.markFailed(ctx, job, "remoteURL is required")
	}
	needCreds := backend.RequiresStorageCredentials() && pvcSrc == nil
	if needCreds && (accessKeyID == "" || accessKeySecret == "") {
		return r.markFailed(ctx, job,
			fmt.Sprintf("%s and %s are required in spec.options or in the referenced checkpoint storage options",
				OptionAccessKeyID, OptionAccessKeySecret))
	}
	// Pin the OSS credentials actually used for preheat: in checkpointUID
	// resolution mode credentials are not written back to spec, and part retries
	// (retryPart) in Running phase must read them back from the annotation. The
	// copy backend has neither credentials nor part retries, so pinning is
	// skipped (only dragonfly reads the annotation).
	if backend.Name() != preheat.BackendCopy {
		if err := r.recordRemoteURLCreds(ctx, job, accessKeyID, accessKeySecret); err != nil {
			ctxLog.Error(err, "Failed to record preheat remote URL creds")
			return ctrl.Result{}, err
		}
	}

	// How many preheat tasks to fan out and how idempotency is guaranteed are up
	// to the backend; its error message goes straight to retryOrFail for retry
	// counting, and the job is failed when the budget is exhausted.
	req := preheat.StartPreheatRequest{
		Job:             job,
		RemoteURL:       remoteURL,
		AccessKeyID:     accessKeyID,
		AccessKeySecret: accessKeySecret,
	}
	if pvcSrc != nil {
		// Pass the PVC source coordinates to the backend: the copy backend uses
		// them to create the copy pod (pvcPath already normalized); the
		// dragonfly/EFC backends ignore this field.
		req.Pvc = &v1beta1.PreheatJobPVC{PvcName: pvcSrc.PvcName, PvcNamespace: pvcSrc.PvcNamespace, PvcPath: pvcPath}
	}
	taskIDs, err := backend.StartPreheat(ctx, req)
	if err != nil {
		return r.retryOrFail(ctx, job, createRetryAnnotation, MaxCreateRetries, err.Error())
	}

	// The EFC backend's taskID is the target_path itself; write it back to
	// status.efcPath for the restore-side worker (mirrors status.remoteURL in
	// dragonfly mode).
	efcPath := ""
	if backend.Name() == preheat.BackendEFC && len(taskIDs) > 0 {
		efcPath = taskIDs[0]
	}

	return r.transitionToRunning(ctx, job, remoteURL, efcPath, taskIDs)
}

// validateCopyPreheat performs the dedicated entry validation of copy preheat,
// called after the PVC source validation (existence + validRelPath + per-mode
// source accessMode check). Returns handled=true when it already returned
// (validation failure -> Failed / environment error -> requeue), and the caller
// must return the result directly; handled=false means validation passed and
// the main flow continues.
func (r *PreheatJobReconciler) validateCopyPreheat(ctx context.Context, job *v1beta1.PreheatJob, pvcSrc *v1beta1.PreheatJobPVC) (bool, ctrl.Result, error) {
	targetName := preheat.TargetPvcName(job)
	targetNamespace := preheat.TargetPvcNamespace(job)

	// fail wraps the whole-job Failed return in this function's three-value
	// signature.
	fail := func(msg string) (bool, ctrl.Result, error) {
		result, err := r.markFailed(ctx, job, msg)
		return true, result, err
	}

	// 1. Both keys must be set together (IsCopyMode interprets either presence
	// as copy mode).
	if targetName == "" || targetNamespace == "" {
		return fail("target-pvc-name and target-pvc-namespace must be set together in spec.options to enable copy preheat")
	}
	// 2. Mutually exclusive with the EFC backend (conflicting semantics).
	if preheat.IsEFCEnabled(job) {
		return fail("copy preheat (target-pvc-*) and efc-enable are mutually exclusive")
	}
	// 3. The source must be a PVC: a remoteURL source has nothing to copy.
	if pvcSrc == nil {
		return fail("copy preheat requires a PVC source: fill spec.pvc or a checkpointUID resolving to pvc, remoteURL is not supported")
	}
	// 4. K8s constraint: a single pod can only mount PVCs from its own namespace.
	if pvcSrc.PvcNamespace != targetNamespace {
		return fail(fmt.Sprintf(
			"source PVC %s/%s and target PVC %s/%s must be in the same namespace (a pod can only mount PVCs from its own namespace)",
			pvcSrc.PvcNamespace, pvcSrc.PvcName, targetNamespace, targetName))
	}
	// 5. Copying onto itself makes no sense.
	if pvcSrc.PvcName == targetName {
		return fail(fmt.Sprintf(
			"target PVC %s/%s is the same as the source PVC, copy preheat makes no sense",
			targetNamespace, targetName))
	}
	// 7/8. The target PVC must exist and be RWX (restore reads the target from
	// multiple nodes; rejecting early beats failing at restore time).
	targetPvc := &corev1.PersistentVolumeClaim{}
	if err := r.sharedContext.Client.Get(ctx, client.ObjectKey{Namespace: targetNamespace, Name: targetName}, targetPvc); err != nil {
		if errors.IsNotFound(err) {
			return fail(fmt.Sprintf("target PVC %s/%s not found", targetNamespace, targetName))
		}
		return true, ctrl.Result{}, err
	}
	if !hasReadWriteMany(targetPvc.Spec.AccessModes) {
		return fail(fmt.Sprintf(
			"target PVC %s/%s is not RWX (ReadWriteMany): restore replicas read the target PVC across multiple nodes",
			targetNamespace, targetName))
	}
	// 10. Copy onto the same target cannot run concurrently: rm -rf + whole
	// directory copy would corrupt each other, and dragonfly's BIO idempotency
	// does not apply here. Reject any non-terminal copy job targeting the same
	// PVC at the entry.
	jobs := &v1beta1.PreheatJobList{}
	if err := r.sharedContext.Client.List(ctx, jobs); err != nil {
		return true, ctrl.Result{}, err
	}
	for i := range jobs.Items {
		other := &jobs.Items[i]
		if other.UID == job.UID || !preheat.IsCopyMode(other) {
			continue
		}
		if other.Status.Phase != "" && other.Status.Phase != v1beta1.PreheatJobPhasePending &&
			other.Status.Phase != v1beta1.PreheatJobPhaseRunning {
			continue
		}
		if preheat.TargetPvcName(other) == targetName && preheat.TargetPvcNamespace(other) == targetNamespace {
			return fail(fmt.Sprintf(
				"another copy PreheatJob %s/%s targeting PVC %s/%s is still active, copy preheat cannot run concurrently",
				other.Namespace, other.Name, targetNamespace, targetName))
		}
	}
	return false, ctrl.Result{}, nil
}

// recordSourcePodNamespace pins the preheat source pod namespace into an
// annotation (idempotent).
func (r *PreheatJobReconciler) recordSourcePodNamespace(ctx context.Context, job *v1beta1.PreheatJob, ns string) error {
	if job.Annotations != nil && job.Annotations[sourcePodNamespaceAnnotation] == ns {
		return nil
	}
	if job.Annotations == nil {
		job.Annotations = map[string]string{}
	}
	job.Annotations[sourcePodNamespaceAnnotation] = ns
	return r.sharedContext.Client.Update(ctx, job)
}

// recordEFCExpectedSize pins the EFC data-integrity baseline (total size
// recorded by the checkpoint CR) into an annotation (idempotent); the Running
// phase compares it with the warmup stat TotalSize.
func (r *PreheatJobReconciler) recordEFCExpectedSize(ctx context.Context, job *v1beta1.PreheatJob, size int64) error {
	expected := strconv.FormatInt(size, 10)
	if job.Annotations != nil && job.Annotations[efcExpectedSizeAnnotation] == expected {
		return nil
	}
	if job.Annotations == nil {
		job.Annotations = map[string]string{}
	}
	job.Annotations[efcExpectedSizeAnnotation] = expected
	return r.sharedContext.Client.Update(ctx, job)
}

// sourcePodNamespace returns the preheat source pod namespace recorded in the
// annotation.
func (r *PreheatJobReconciler) sourcePodNamespace(job *v1beta1.PreheatJob) string {
	if job.Annotations == nil {
		return ""
	}
	return job.Annotations[sourcePodNamespaceAnnotation]
}

// encodeTaskIDs / decodeTaskIDs pack the fanned-out task ids into the single
// status.TaskID string.
//
// The field is kept as-is on purpose: this change must not touch the CRD schema.
// Note that status.TaskID is now purely informational - idempotency of the
// dragonfly path is anchored on the BIO, see dragonflyPartBIO.
func encodeTaskIDs(ids []string) string {
	return strings.Join(ids, taskIDSeparator)
}

func decodeTaskIDs(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, taskIDSeparator)
}

// handleRunning polls the preheat task status of all parts and advances to
// Probing or Failed based on the result.
//
// Only when **all** parts succeed is the preheat complete: missing any one of
// them means the restore side falls back to origin download for that part,
// negating the multi-path parallelism (the slowest path wins).
func (r *PreheatJobReconciler) handleRunning(ctx context.Context, job *v1beta1.PreheatJob) (ctrl.Result, error) {
	ctxLog := log.FromContext(ctx)

	taskIDs := decodeTaskIDs(job.Status.TaskID)
	if len(taskIDs) == 0 {
		return r.markFailed(ctx, job, "taskID is missing in Running state")
	}

	backend := r.backendFor(job)
	pending := 0
	for i, taskID := range taskIDs {
		task, err := backend.GetPreheatTask(ctx, taskID)
		if err != nil {
			return r.retryOrFail(ctx, job, pollRetryAnnotation, MaxPollRetries,
				fmt.Sprintf("failed to get %s preheat task %s (%s): %v", backend.Name(), taskID, backend.TaskLabel(i), err))
		}
		switch task.State {
		case preheat.TaskStateSuccess:
			// EFC backend: after completion, compare the stat TotalSize with the
			// baseline recorded by the checkpoint CR; a mismatch means the ossfs
			// async upload has not finished (the cache holds incomplete data),
			// fail the job; a retry after the upload completes reloads and fills
			// the cache.
			if backend.Name() == preheat.BackendEFC {
				if expected := getInt64Annotation(job, efcExpectedSizeAnnotation); expected > 0 && task.TotalSize != expected {
					return r.markFailed(ctx, job, fmt.Sprintf(
						"efc preheat data incomplete: warmup TotalSize=%d does not match checkpoint size=%d (ossfs upload may still be in progress, retry later)",
						task.TotalSize, expected))
				}
			}
		case preheat.TaskStateFailure:
			// Per-part retry: on a single part failure re-create that part's
			// preheat task (with a new BIO) instead of failing the whole job;
			// markFailed only when the budget is exhausted.
			if retried, result, err := r.retryPart(ctx, job, backend, i, taskIDs); retried {
				return result, err
			}
			return r.markFailed(ctx, job,
				fmt.Sprintf("%s preheat of %s failed: %s", backend.Name(), backend.TaskLabel(i), task.Message))
		default:
			pending++
		}
	}

	// Clear the retry count when polling succeeds.
	if job.Annotations != nil && job.Annotations[pollRetryAnnotation] != "" {
		delete(job.Annotations, pollRetryAnnotation)
		if err := r.sharedContext.Client.Update(ctx, job); err != nil {
			ctxLog.Error(err, "Failed to clear PreheatJob poll retry annotation")
		}
	}

	if pending > 0 {
		ctxLog.Info(fmt.Sprintf("PreheatJob %s/%s has %d/%d parts still preheating, requeue after %s",
			job.Namespace, job.Name, pending, len(taskIDs), DefaultTaskPollInterval))
		return ctrl.Result{RequeueAfter: DefaultTaskPollInterval}, nil
	}

	// The copy backend goes straight to Ready once all its tasks succeed: copy
	// has no cache to probe, the Probing semantics (probing seed peer / warmup
	// stat) do not apply, and ProbeController skips copy jobs.
	if backend.Name() == preheat.BackendCopy {
		if err := r.updateStatus(ctx, job, func(j *v1beta1.PreheatJob) {
			j.Status.Phase = v1beta1.PreheatJobPhaseReady
			j.Status.Message = "copy preheat completed"
			j.Status.PeerList = nil
		}); err != nil {
			ctxLog.Error(err, "Failed to update PreheatJob status to Ready")
			return ctrl.Result{}, err
		}
		ctxLog.Info(fmt.Sprintf("PreheatJob %s/%s copy completed, phase -> Ready",
			job.Namespace, job.Name))
		return ctrl.Result{}, nil
	}

	// Not Ready directly after all parts complete backend-side; go to Probing
	// instead. ProbeController performs the first actual probe of each seed
	// peer's cache state and then decides Ready or Stale based on PeerList,
	// avoiding the "just succeeded but immediately marked Stale" flapping.
	//
	// The preheat source pod is deleted after phase=Ready (see the Ready branch
	// of Reconcile): during restore, source reachability is provided by the
	// restore workers double-hosting the source (same PVC mount, self-registered
	// EndpointSlice).

	if err := r.updateStatus(ctx, job, func(j *v1beta1.PreheatJob) {
		j.Status.Phase = v1beta1.PreheatJobPhaseProbing
		j.Status.Message = "waiting for first cache probe"
		j.Status.PeerList = nil
	}); err != nil {
		ctxLog.Error(err, "Failed to update PreheatJob status to Probing")
		return ctrl.Result{}, err
	}
	ctxLog.Info(fmt.Sprintf("PreheatJob %s/%s all %d part(s) succeeded, phase -> Probing",
		job.Namespace, job.Name, len(taskIDs)))
	return ctrl.Result{}, nil
}

// retryPart re-creates the preheat task for a failed part. Returns
// retried=true when it took over (the caller must return the result directly);
// false means no retry (budget exhausted / backend unsupported / no URL) and
// the caller keeps its markFailed logic.
//
// The new task must use a new BIO (with a retry sequence number): dragonfly
// jobs are terminal, the same BIO would only find the same failed job. The
// restore-side task id is derived from the URL, independent of the BIO, so
// changing the BIO does not affect P2P hits.
func (r *PreheatJobReconciler) retryPart(ctx context.Context, job *v1beta1.PreheatJob,
	backend preheat.PreheatBackend, partIdx int, taskIDs []string) (bool, ctrl.Result, error) {
	ctxLog := log.FromContext(ctx)

	// Only the dragonfly backend fans out N independent tasks, so per-part
	// retry only makes sense there; an EFC single-task failure fails the whole
	// job directly.
	df, ok := backend.(dragonflyBackend)
	if !ok {
		return false, ctrl.Result{}, nil
	}

	count := r.partRetryCount(job, partIdx)
	if count >= MaxPartRetries {
		ctxLog.Info(fmt.Sprintf("PreheatJob %s/%s part %s exhausted %d retries, will fail",
			job.Namespace, job.Name, backend.TaskLabel(partIdx), MaxPartRetries))
		return false, ctrl.Result{}, nil
	}

	// Anti hot-loop: same minimum-interval semantics as retryOrFail, to avoid
	// the watch event triggered by the annotation write exhausting the retry
	// budget in milliseconds.
	if last, ok := job.Annotations[partRetryAnnotation+".last-attempt"]; ok {
		if t, err := time.Parse(time.RFC3339Nano, last); err == nil &&
			time.Since(t) < minRetryInterval {
			return true, ctrl.Result{RequeueAfter: minRetryInterval}, nil
		}
	}

	remoteURL := job.Status.RemoteURL
	if remoteURL == "" {
		ctxLog.Info("PreheatJob has no remoteURL recorded, cannot retry part",
			"job", fmt.Sprintf("%s/%s", job.Namespace, job.Name), "part", backend.TaskLabel(partIdx))
		return false, ctrl.Result{}, nil
	}
	accessKeyID, accessKeySecret := r.remoteURLCreds(job)

	// New BIO = <CR-UID>#<partIdx>#r<seq>: finds the retry job by the new BIO
	// and never confuses it with the failed job of the first creation
	// (<CR-UID>#<partIdx>).
	bio := fmt.Sprintf("%s#r%d", dragonflyPartBIO(job, partIdx), count+1)
	partURL := dragonfly.PartURL(remoteURL, partIdx)
	taskID, err := df.Client.CreatePreheatTask(ctx, dragonfly.PreheatRequest{
		URL:             partURL,
		BIO:             bio,
		AccessKeyID:     accessKeyID,
		AccessKeySecret: accessKeySecret,
		Options:         job.Spec.Options,
	})
	if err != nil {
		// Creating the retry task failing also goes through the retry budget, so
		// the job is not failed immediately for the same reason the first
		// creation failed.
		return r.retryPartWithCount(ctx, job, backend, partIdx, taskIDs, count, err.Error())
	}

	// Persist the retry count and last-attempt before updating status.TaskID.
	// Order matters: updateStatus re-Gets the latest copy and its
	// Status().Update bumps the server-side resourceVersion; updating with the
	// stale local job afterwards would 409, the retry count would never land ->
	// infinite retries on the same BIO. Consistent with markFailed's
	// update-then-updateStatus order.
	if job.Annotations == nil {
		job.Annotations = map[string]string{}
	}
	job.Annotations[partRetryAnnotation+".last-attempt"] = time.Now().Format(time.RFC3339Nano)
	job.Annotations[partRetryAnnotation] = encodePartRetries(setPartRetry(job.Annotations[partRetryAnnotation], partIdx, count+1))
	if err := r.sharedContext.Client.Update(ctx, job); err != nil {
		ctxLog.Error(err, "Failed to update PreheatJob part retry annotation")
		return true, ctrl.Result{}, err
	}

	// Replace the part taskID then land the status: by now the annotation is on
	// the server, and updateStatus re-Gets the latest object, so there is no
	// resourceVersion conflict with the previous step.
	taskIDs[partIdx] = taskID
	if err := r.updateStatus(ctx, job, func(j *v1beta1.PreheatJob) {
		j.Status.TaskID = encodeTaskIDs(taskIDs)
	}); err != nil {
		ctxLog.Error(err, "Failed to update PreheatJob taskID after part retry")
		return true, ctrl.Result{}, err
	}
	ctxLog.Info(fmt.Sprintf("PreheatJob %s/%s part %s failed, retry %d/%d created new task %s (bio %s)",
		job.Namespace, job.Name, backend.TaskLabel(partIdx), count+1, MaxPartRetries, taskID, bio))
	return true, ctrl.Result{RequeueAfter: DefaultTaskPollInterval}, nil
}

// retryPartWithCount bumps the retry count and backs off when creating the
// part retry task fails (a temporarily unavailable dragonfly is recoverable).
func (r *PreheatJobReconciler) retryPartWithCount(ctx context.Context, job *v1beta1.PreheatJob,
	backend preheat.PreheatBackend, partIdx int, taskIDs []string, count int, reason string) (bool, ctrl.Result, error) {
	ctxLog := log.FromContext(ctx)
	if job.Annotations == nil {
		job.Annotations = map[string]string{}
	}
	job.Annotations[partRetryAnnotation+".last-attempt"] = time.Now().Format(time.RFC3339Nano)
	job.Annotations[partRetryAnnotation] = encodePartRetries(setPartRetry(job.Annotations[partRetryAnnotation], partIdx, count+1))
	if err := r.sharedContext.Client.Update(ctx, job); err != nil {
		ctxLog.Error(err, "Failed to update PreheatJob part retry annotation")
		return true, ctrl.Result{}, err
	}
	if count+1 >= MaxPartRetries {
		ctxLog.Info(fmt.Sprintf("PreheatJob %s/%s part %s retry create failed and budget exhausted: %s",
			job.Namespace, job.Name, backend.TaskLabel(partIdx), reason))
		return false, ctrl.Result{}, nil
	}
	ctxLog.Info(fmt.Sprintf("PreheatJob %s/%s part %s retry create failed (%s), attempt %d/%d, requeue after %s",
		job.Namespace, job.Name, backend.TaskLabel(partIdx), reason, count+1, MaxPartRetries, DefaultTaskPollInterval))
	return true, ctrl.Result{RequeueAfter: DefaultTaskPollInterval}, nil
}

// recordRemoteURLCreds pins the OSS credentials actually used for preheat into
// an annotation (idempotent) for part retries in Running phase. The
// credentials come from the same source as spec.options (plaintext), no new
// exposure surface is introduced.
func (r *PreheatJobReconciler) recordRemoteURLCreds(ctx context.Context, job *v1beta1.PreheatJob, accessKeyID, accessKeySecret string) error {
	value := accessKeyID + ":" + accessKeySecret
	if job.Annotations != nil && job.Annotations[remoteURLCredsAnnotation] == value {
		return nil
	}
	if job.Annotations == nil {
		job.Annotations = map[string]string{}
	}
	job.Annotations[remoteURLCredsAnnotation] = value
	return r.sharedContext.Client.Update(ctx, job)
}

// remoteURLCreds reads the pinned OSS credentials back from the annotation.
func (r *PreheatJobReconciler) remoteURLCreds(job *v1beta1.PreheatJob) (string, string) {
	if job.Annotations == nil {
		return "", ""
	}
	parts := strings.SplitN(job.Annotations[remoteURLCredsAnnotation], ":", 2)
	if len(parts) != 2 {
		return "", ""
	}
	return parts[0], parts[1]
}

// partRetryCount returns the current retry count of a part.
func (r *PreheatJobReconciler) partRetryCount(job *v1beta1.PreheatJob, partIdx int) int {
	if job.Annotations == nil {
		return 0
	}
	return getPartRetry(job.Annotations[partRetryAnnotation], partIdx)
}

// encodePartRetries / getPartRetry / setPartRetry maintain the "idx:count,..."
// retry-count table.
func encodePartRetries(entries map[int]int) string {
	parts := make([]string, 0, len(entries))
	for idx, count := range entries {
		parts = append(parts, fmt.Sprintf("%d:%d", idx, count))
	}
	return strings.Join(parts, ",")
}

func getPartRetry(encoded string, partIdx int) int {
	for _, entry := range strings.Split(encoded, ",") {
		kv := strings.SplitN(entry, ":", 2)
		if len(kv) != 2 {
			continue
		}
		idx, err := strconv.Atoi(kv[0])
		if err != nil || idx != partIdx {
			continue
		}
		count, err := strconv.Atoi(kv[1])
		if err != nil {
			return 0
		}
		return count
	}
	return 0
}

func setPartRetry(encoded string, partIdx, count int) map[int]int {
	entries := map[int]int{}
	for _, entry := range strings.Split(encoded, ",") {
		kv := strings.SplitN(entry, ":", 2)
		if len(kv) != 2 {
			continue
		}
		idx, err1 := strconv.Atoi(kv[0])
		c, err2 := strconv.Atoi(kv[1])
		if err1 == nil && err2 == nil {
			entries[idx] = c
		}
	}
	entries[partIdx] = count
	return entries
}

// warmupHotThresholdMBps is the pre-read speed probe threshold: reading the
// first part faster than this means the OSS/FUSE cache is already hot, skip
// the remaining full pre-read to avoid wasting time.
const warmupHotThresholdMBps = 300

// preheatWarmupTimeout is the total pre-read timeout. A cold full sequential
// read takes ~10 minutes at worst; after the timeout the pre-read is given up
// (preheat proceeds, just back on the cold download path).
const preheatWarmupTimeout = 20 * time.Minute

// warmupPreheatSourcePod pre-reads the PVC preheat source pod data
// (best-effort, failures only log).
//
// Why pre-read: the preheat source pod is newly created per preheat round, its
// ossfs FUSE page cache starts from zero. Cold data reads OSS origin at a
// throttled rate (~103MB/s measured), and dragonfly's 8-way concurrent
// download is slow on cold reads and prone to per-part timeouts; hot data
// (already cached OSS-side) reaches ~600MB/s. The pre-read moves cold reads
// out of the dragonfly download phase (8-way random concurrency) into a
// sequential bulk pre-read phase, and lets the subsequent downloads all hit
// the page cache.
//
// Adaptive: read the first part to probe speed - hot means skip (pre-read cost
// ~10s); cold means sequentially pre-read all part-*.tar. A dedicated context
// is used: the pre-read may last minutes, the reconcile request context cannot
// survive that.
func (r *PreheatJobReconciler) warmupPreheatSourcePod(ctx context.Context, job *v1beta1.PreheatJob, pvcSrc *v1beta1.PreheatJobPVC) {
	ctxLog := log.FromContext(ctx)
	podName := controllers.GeneratePreheatSourcePodName(string(job.UID))
	ctxLog.Info("Warming up preheat source pod data",
		"job", fmt.Sprintf("%s/%s", job.Namespace, job.Name), "pod", podName)

	// Derive from the reconcile ctx (not context.Background): the cancel signal
	// on manager shutdown must propagate to the exec stream so graceful
	// shutdown is not blocked by the pre-read for up to 20 minutes. Warmup is a
	// best-effort optimization, interruption is harmless. The reconcile ctx is
	// only cancelled on manager shutdown (controller-runtime does not time out
	// a single Reconcile), so no unexpected interruption is introduced.
	warmCtx, cancel := context.WithTimeout(ctx, preheatWarmupTimeout)
	defer cancel()

	dir := filepath.Join(controllers.PVCMountPoint, pvcSrc.PvcPath)
	// dir is passed as a positional argument ("$1"), never interpolated into the
	// script text: pvcPath may contain spaces, "$", ";" etc., interpolation
	// would be parsed as shell syntax - this pod reuses the worker template
	// (privileged + hostPath), an injection is node root. validRelPath at the
	// entry is the first line of defense, the positional argument the second
	// (against callers bypassing the validation). Likewise the trailing full
	// pre-read uses while-read line by line, avoiding cat $files word-splitting
	// paths containing spaces.
	script := fmt.Sprintf(
		`dir="$1"; files=$(ls "$dir"/part-*.tar 2>/dev/null); [ -z "$files" ] && exit 0; first=$(echo "$files" | head -n1); S=$(date +%%s); cat "$first" > /dev/null 2>&1; E=$(date +%%s); elapsed=$((E-S)); [ $elapsed -lt 1 ] && elapsed=1; size=$(stat -c %%s "$first" 2>/dev/null || echo 0); speed=$((size/1048576/elapsed)); if [ $speed -gt %d ]; then echo "warm skip speed=$speed MB/s"; exit 0; fi; echo "cold speed=$speed MB/s, warming up rest"; echo "$files" | while IFS= read -r f; do cat "$f" > /dev/null 2>&1; done; echo "warmup done"`,
		warmupHotThresholdMBps)
	if err := r.sharedContext.ExecInPod(warmCtx, pvcSrc.PvcNamespace, podName, "",
		[]string{"sh", "-c", script, "sh", dir}); err != nil {
		ctxLog.Info("Preheat source pod warmup failed, continue preheat without warmup",
			"job", fmt.Sprintf("%s/%s", job.Namespace, job.Name), "error", err)
		return
	}
	ctxLog.Info("Preheat source pod warmup completed",
		"job", fmt.Sprintf("%s/%s", job.Namespace, job.Name))
}

// hasReadWriteMany reports whether accessModes contains ReadWriteMany (RWX).
func hasReadWriteMany(modes []corev1.PersistentVolumeAccessMode) bool {
	for _, m := range modes {
		if m == corev1.ReadWriteMany {
			return true
		}
	}
	return false
}

// markFailed marks the job Failed and updates the status. The preheat source
// pod / copy pod are deleted alongside to release the PVC mount: on the normal
// path the pod is already deleted after Ready (see the Ready branch of
// Reconcile), this is the fallback cleanup for pods possibly leftover on
// failure paths.
func (r *PreheatJobReconciler) markFailed(ctx context.Context, job *v1beta1.PreheatJob, message string) (ctrl.Result, error) {
	ctxLog := log.FromContext(ctx)

	// Clear retry-count annotations when entering the terminal state.
	if job.Annotations != nil {
		delete(job.Annotations, createRetryAnnotation)
		delete(job.Annotations, pollRetryAnnotation)
		delete(job.Annotations, podWaitRetryAnnotation)
		delete(job.Annotations, partRetryAnnotation)
		delete(job.Annotations, partRetryAnnotation+".last-attempt")
		delete(job.Annotations, remoteURLCredsAnnotation)
		if err := r.sharedContext.Client.Update(ctx, job); err != nil {
			ctxLog.Error(err, "Failed to clear PreheatJob retry annotations when marking failed")
		}
	}

	// The preheat source pod / copy pod may still exist when preheat fails
	// (e.g. part probe failure); delete them to release the PVC mount. The two
	// modes are mutually exclusive, delete per mode.
	if preheat.IsCopyMode(job) {
		if ns := preheat.TargetPvcNamespace(job); ns != "" {
			if err := r.sharedContext.DeletePreheatCopyPod(ctx, string(job.UID), ns); err != nil {
				ctxLog.Error(err, "Failed to delete preheat copy pod when marking failed")
			}
		}
	} else if ns := r.sourcePodNamespace(job); ns != "" {
		if err := r.sharedContext.DeletePreheatSourcePod(ctx, string(job.UID), ns); err != nil {
			ctxLog.Error(err, "Failed to delete preheat source pod when marking failed")
		}
	}

	if err := r.updateStatus(ctx, job, func(j *v1beta1.PreheatJob) {
		j.Status.Phase = v1beta1.PreheatJobPhaseFailed
		j.Status.Message = message
	}); err != nil {
		ctxLog.Error(err, "Failed to update PreheatJob status to Failed")
		return ctrl.Result{}, err
	}
	ctxLog.Info(fmt.Sprintf("PreheatJob %s/%s phase -> Failed: %s", job.Namespace, job.Name, message))
	return ctrl.Result{}, nil
}

// transitionToRunning moves the job to Running, writes all part taskIDs, and
// pins the remoteURL actually used for preheat into the status (probing
// depends on it when spec only fills pvc/checkpointUID).
func (r *PreheatJobReconciler) transitionToRunning(ctx context.Context, job *v1beta1.PreheatJob, remoteURL, efcPath string, taskIDs []string) (ctrl.Result, error) {
	ctxLog := log.FromContext(ctx)
	encoded := encodeTaskIDs(taskIDs)
	if err := r.updateStatus(ctx, job, func(j *v1beta1.PreheatJob) {
		j.Status.Phase = v1beta1.PreheatJobPhaseRunning
		j.Status.TaskID = encoded
		j.Status.RemoteURL = remoteURL
		j.Status.EFCPath = efcPath
		j.Status.Message = ""
	}); err != nil {
		ctxLog.Error(err, "Failed to update PreheatJob status to Running")
		return ctrl.Result{}, err
	}
	ctxLog.Info(fmt.Sprintf("PreheatJob %s/%s preheat tasks %s, phase -> Running",
		job.Namespace, job.Name, encoded))
	// Requeue immediately to poll the preheat task status at the earliest
	// opportunity, rather than relying on a single status update event.
	return ctrl.Result{RequeueAfter: 0}, nil
}

// retryOrFail bumps the retry count, returns a jittered backoff requeue while
// under the limit, otherwise marks Failed.
func (r *PreheatJobReconciler) retryOrFail(ctx context.Context, job *v1beta1.PreheatJob, retryKey string, maxRetries int, message string) (ctrl.Result, error) {
	ctxLog := log.FromContext(ctx)

	// Anti hot-loop: writing an annotation triggers a watch event and an
	// immediate new reconcile; without a minimum interval the retry count would
	// be exhausted in milliseconds (the preheat source pod would never get a
	// chance to start). When the gap since the last attempt is below
	// minRetryInterval, do not bump the count, just requeue to wait for real
	// time to pass.
	//
	// A second '/' is not allowed in an annotation key, use the '.last-attempt'
	// suffix.
	if last, ok := job.Annotations[retryKey+".last-attempt"]; ok {
		if t, err := time.Parse(time.RFC3339Nano, last); err == nil &&
			time.Since(t) < minRetryInterval {
			return ctrl.Result{RequeueAfter: minRetryInterval}, nil
		}
	}

	retries := getIntAnnotation(job, retryKey) + 1
	setIntAnnotation(job, retryKey, retries)
	if job.Annotations == nil {
		job.Annotations = map[string]string{}
	}
	job.Annotations[retryKey+".last-attempt"] = time.Now().Format(time.RFC3339Nano)
	if err := r.sharedContext.Client.Update(ctx, job); err != nil {
		ctxLog.Error(err, "Failed to update PreheatJob retry annotation", "key", retryKey)
		return ctrl.Result{}, err
	}

	if retries > maxRetries {
		return r.markFailed(ctx, job, message)
	}

	// The retry reason must stay visible: otherwise the concrete error of
	// creation/polling failure is swallowed and only guesswork remains.
	ctxLog.Info("PreheatJob retry scheduled", "job", fmt.Sprintf("%s/%s", job.Namespace, job.Name),
		"key", retryKey, "attempt", fmt.Sprintf("%d/%d", retries, maxRetries), "reason", message)

	delay := backoff(retries)
	ctxLog.Info(fmt.Sprintf("PreheatJob %s/%s %s attempt %d/%d, requeue after %s",
		job.Namespace, job.Name, retryKey, retries, maxRetries, delay))
	return ctrl.Result{RequeueAfter: delay}, nil
}

// backoff returns a jittered exponential backoff duration.
func backoff(retry int) time.Duration {
	const (
		base = 500 * time.Millisecond
		max  = 30 * time.Second
	)
	if retry <= 0 {
		return base
	}
	d := base * time.Duration(1<<(retry-1))
	if d > max {
		d = max
	}
	// At most 50% jitter.
	jitter := time.Duration(rand.Int63n(int64(d / 2)))
	return d + jitter
}

func getIntAnnotation(job *v1beta1.PreheatJob, key string) int {
	if job.Annotations == nil {
		return 0
	}
	v, err := strconv.Atoi(job.Annotations[key])
	if err != nil {
		return 0
	}
	return v
}

// getInt64Annotation reads an int64 value from an annotation (0 when missing
// or unparseable).
func getInt64Annotation(job *v1beta1.PreheatJob, key string) int64 {
	if job.Annotations == nil {
		return 0
	}
	v, err := strconv.ParseInt(job.Annotations[key], 10, 64)
	if err != nil {
		return 0
	}
	return v
}

func setIntAnnotation(job *v1beta1.PreheatJob, key string, value int) {
	if job.Annotations == nil {
		job.Annotations = map[string]string{}
	}
	job.Annotations[key] = strconv.Itoa(value)
}

// updateStatus updates the PreheatJob status, re-fetching the latest object
// and retrying on conflict.
func (r *PreheatJobReconciler) updateStatus(ctx context.Context, job *v1beta1.PreheatJob, mutate func(*v1beta1.PreheatJob)) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &v1beta1.PreheatJob{}
		if err := r.sharedContext.Client.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: job.Name}, latest); err != nil {
			return err
		}
		mutate(latest)
		return r.sharedContext.Client.Status().Update(ctx, latest)
	})
}

// SetupWithManager sets up the controller with the Manager.
func (r *PreheatJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named(r.GetName()).
		For(&v1beta1.PreheatJob{}).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: r.sharedContext.Config.MaxConcurrentReconciles,
			RateLimiter:             workqueue.DefaultControllerRateLimiter(),
		}).
		Complete(r)
}
