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

package preheatjob

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/container-migration/controller/api/v1beta1"
	"github.com/container-migration/controller/pkg/controllers"
	"github.com/container-migration/controller/pkg/dragonfly"
	"github.com/container-migration/controller/pkg/preheat"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// DefaultProbeInterval is the default interval between probe reconciliations.
const (
	DefaultProbeInterval = 30 * time.Second
	// DefaultProbeTimeout is the per-RPC timeout for a single worker Probe call.
	DefaultProbeTimeout = 5 * time.Second
)

// ProbeController periodically probes every Ready, Stale or Probing PreheatJob
// to determine whether the preheated data is still cached.
//
// The probing itself is provided by PreheatBackend.ProbeCache: the dragonfly
// backend discovers all seed peers via the scheduler ListHosts and probes the
// cache state of each part task directly against each seed peer's own dfdaemon
// gRPC endpoint; the EFC backend queries the whole directory's cache state via
// the node-side runtimekit's warmup/stat interface.
// This controller only owns the probe period, the preheat source resolution
// (resolved by checkpointUID when spec.remoteURL is missing) and writing probe
// results back to status; it knows nothing about backend-specific details.
type ProbeController struct {
	sharedContext *controllers.SharedContext
	// The dragonfly / efc / copy probe backends, selected by backendFor per
	// spec.options, with routing rules identical to PreheatJobReconciler (the
	// copy backend is skipped at the Reconcile entry and never actually
	// probed).
	dragonfly     preheat.PreheatBackend
	efc           preheat.PreheatBackend
	copy          preheat.PreheatBackend
	probeInterval time.Duration
}

// NewProbeController creates a new ProbeController.
//
// seedPeerSource / prober are the dependencies dragonfly probing needs; the
// constructor signature stays unchanged so main.go's assembly is unaffected;
// EFC probing only needs the K8s client (list nodes + resolve
// checkpointUID).
func NewProbeController(
	sharedContext *controllers.SharedContext,
	seedPeerSource dragonfly.SeedPeerSource,
	prober dragonfly.SeedPeerProber,
	probeInterval time.Duration,
) *ProbeController {
	if probeInterval <= 0 {
		probeInterval = DefaultProbeInterval
	}
	return &ProbeController{
		sharedContext: sharedContext,
		dragonfly: dragonflyBackend{
			seedPeerSource: seedPeerSource,
			prober:         prober,
		},
		efc:           newEFCBackend(sharedContext.Client),
		copy:          copyBackend{sharedContext: sharedContext},
		probeInterval: probeInterval,
	}
}

// backendFor selects the probe backend by the job's spec.options.
func (p *ProbeController) backendFor(job *v1beta1.PreheatJob) preheat.PreheatBackend {
	return preheat.SelectBackend(job, p.dragonfly, p.efc, p.copy)
}

// NeedLeaderElection ensures the probe loop only runs on the elected leader.
func (p *ProbeController) NeedLeaderElection() bool {
	return true
}

// Start starts the periodic probe loop. It blocks until ctx is cancelled.
// It implements sigs.k8s.io/controller-runtime/pkg/manager.Runnable.
func (p *ProbeController) Start(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("ProbeController")
	logger.Info("starting probe controller", "interval", p.probeInterval)

	ticker := time.NewTicker(p.probeInterval)
	defer ticker.Stop()

	// Run once immediately so users don't have to wait a full interval.
	if err := p.Reconcile(ctx); err != nil {
		logger.Error(err, "initial probe reconciliation failed")
	}

	for {
		select {
		case <-ctx.Done():
			logger.Info("stopping probe controller")
			return nil
		case <-ticker.C:
			if err := p.Reconcile(ctx); err != nil {
				logger.Error(err, "probe reconciliation failed")
			}
		}
	}
}

// Reconcile lists all Ready, Stale or Probing PreheatJobs and probes them via
// the selected backend, then updates the job status. The phase is driven by
// the peer list: if at least one peer reports Cached=true the job becomes
// Ready, otherwise it becomes Stale. Probing jobs are promoted to Ready/Stale
// after their first successful probe.
func (p *ProbeController) Reconcile(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("ProbeController")

	jobs := &v1beta1.PreheatJobList{}
	if err := p.sharedContext.Client.List(ctx, jobs, &client.ListOptions{
		LabelSelector: labels.Everything(),
	}); err != nil {
		return fmt.Errorf("list preheat jobs: %w", err)
	}

	for i := range jobs.Items {
		job := &jobs.Items[i]
		if job.Status.Phase != v1beta1.PreheatJobPhaseReady &&
			job.Status.Phase != v1beta1.PreheatJobPhaseStale &&
			job.Status.Phase != v1beta1.PreheatJobPhaseProbing {
			continue
		}
		if job.Status.TaskID == "" {
			continue
		}
		// Copy preheat has no cache to probe: data lands permanently on the
		// target PVC (until the user deletes it), Ready is terminal, skip probing
		// - otherwise it would be misjudged Stale (ProbeCache semantics do not
		// apply).
		if preheat.IsCopyMode(job) {
			continue
		}

		if err := p.probeJob(ctx, job); err != nil {
			logger.Error(err, "failed to probe job", "namespace", job.Namespace, "name", job.Name)
		}
	}
	return nil
}

// probeJob resolves the preheat source, delegates to the selected backend for probing, then writes the result back to status.
func (p *ProbeController) probeJob(ctx context.Context, job *v1beta1.PreheatJob) error {
	logger := log.FromContext(ctx).WithName("ProbeController")

	if len(decodeTaskIDs(job.Status.TaskID)) == 0 {
		return fmt.Errorf("preheat job %s/%s has no task id recorded", job.Namespace, job.Name)
	}

	// The backend probe only recognizes job.Spec.RemoteURL: prefer the one
	// filled in spec; when spec is empty use status.remoteURL (the address
	// pinned after handlePending resolved/generated it - a PVC source only
	// fills the pvc field in spec, the address only exists in status); when
	// neither exists but checkpointUID is filled, resolve from the checkpoint
	// CR; the result is only written into the in-memory copy, never back to
	// spec.
	if job.Spec.RemoteURL == "" && job.Status.RemoteURL != "" {
		job = job.DeepCopy()
		job.Spec.RemoteURL = job.Status.RemoteURL
	}
	backend := p.backendFor(job)
	if job.Spec.RemoteURL == "" && job.Spec.CheckpointUID != "" {
		src, err := resolveSourceFromCheckpoint(ctx, p.sharedContext.Client, job)
		if err != nil {
			return fmt.Errorf("resolve remoteURL from checkpoint %s: %w", job.Spec.CheckpointUID, err)
		}
		if src.IsPVC() {
			// The EFC backend needs no remoteURL: ProbeCache internally resolves
			// the EFC target_path from spec.pvc/checkpointUID for probing; the
			// dragonfly backend relies on the preheat source pod generating
			// remoteURL (pinned into status at handlePending); when neither can
			// obtain one, error out.
			if backend.Name() != preheat.BackendEFC {
				return fmt.Errorf("preheat job %s/%s resolved a PVC source but status.remoteURL is empty, cannot probe",
					job.Namespace, job.Name)
			}
		} else {
			job = job.DeepCopy()
			job.Spec.RemoteURL = src.remoteURL
		}
	}
	if job.Spec.RemoteURL == "" && backend.Name() != preheat.BackendEFC {
		return fmt.Errorf("preheat job %s/%s has no remoteURL to probe", job.Namespace, job.Name)
	}

	newPeerList, states, err := backend.ProbeCache(log.IntoContext(ctx, logger), job)
	if errors.Is(err, preheat.ErrNoProbeTargets) {
		logger.Info("no probe targets found, skipping probe",
			"backend", backend.Name(), "namespace", job.Namespace, "name", job.Name)
		return nil
	}
	if err != nil {
		return err
	}

	// Retry on conflict: re-fetch the latest object and apply the probe
	// result, avoiding a stale resourceVersion failing the status update and
	// leaving the task stuck in Probing for a long time.
	ns, name := job.Namespace, job.Name
	updateErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &v1beta1.PreheatJob{}
		if err := p.sharedContext.Client.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, latest); err != nil {
			return err
		}
		latest.Status.PeerList = newPeerList
		if anyPeerCached(newPeerList) {
			latest.Status.Phase = v1beta1.PreheatJobPhaseReady
			latest.Status.Message = ""
		} else {
			latest.Status.Phase = v1beta1.PreheatJobPhaseStale
			latest.Status.Message = staleMessage(states)
		}
		return p.sharedContext.Client.Status().Update(ctx, latest)
	})
	if updateErr != nil {
		return fmt.Errorf("update preheat job status: %w", updateErr)
	}
	return nil
}

// anyPeerCached reports whether at least one peer in the list has Cached=true.
func anyPeerCached(peerList []v1beta1.PreheatPeerInfo) bool {
	for _, p := range peerList {
		if p.Cached {
			return true
		}
	}
	return false
}

// staleMessage builds a human readable reason for the Stale phase from the
// probed cache states, so users can tell a failed download apart from a cache
// eviction or a still-running preheat.
func staleMessage(states []preheat.CacheState) string {
	failed, running := 0, 0
	for _, s := range states {
		switch s {
		case preheat.CacheStateFailed:
			failed++
		case preheat.CacheStateRunning:
			running++
		}
	}
	switch {
	case failed > 0:
		return fmt.Sprintf("preheat download failed on %d seed peer(s), the file is not cached (check the backend URL and credentials)", failed)
	case running > 0:
		return fmt.Sprintf("preheat task is still downloading on %d seed peer(s), the file is not cached yet", running)
	default:
		return "no seed peer currently caches the preheated file"
	}
}
