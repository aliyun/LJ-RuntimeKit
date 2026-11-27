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
	"fmt"
	"net"
	"strconv"
	"sync"

	"github.com/container-migration/controller/api/v1beta1"
	"github.com/container-migration/controller/pkg/dragonfly"
	"github.com/container-migration/controller/pkg/preheat"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// The backend-agnostic preheat model (the PreheatBackend interface, TaskState
// / CacheState, StartPreheatRequest etc.) lives in pkg/preheat: that is the
// single source of truth for preheat semantics, and the dragonfly package's
// state types are its aliases. This file keeps only the dragonfly backend's
// concrete implementation; see efc_backend.go for the EFC backend and
// copy_backend.go for the copy backend.

// ============================================================================
// The dragonfly backend.
//
// StartPreheat is the part-expansion loop of the original handlePending and
// ProbeCache is the probe loop of the original probeJob, both moved verbatim:
// the same DiscoverParts probing, the same BIO idempotency recovery, the same
// per-part creation / probing and error wording; the Dragonfly path (OSS
// signing, Range GET part probing, ListHosts + StatLocalTask probing) stays
// byte-for-byte identical.
// ============================================================================

// dragonflyBackend adapts the various dragonfly clients into PreheatBackend.
//
// Client serves the task lifecycle (StartPreheat / GetPreheatTask);
// seedPeerSource and prober serve cache probing (ProbeCache). The reconciler
// and ProbeController each build one on demand; unused dependencies can stay
// nil.
type dragonflyBackend struct {
	dragonfly.Client
	seedPeerSource dragonfly.SeedPeerSource
	prober         dragonfly.SeedPeerProber
}

var _ preheat.PreheatBackend = dragonflyBackend{}

func (dragonflyBackend) Name() string { return preheat.BackendDragonfly }

// RequiresStorageCredentials: Dragonfly fetches OSS origin via presigned URLs, AK/SK is mandatory.
func (dragonflyBackend) RequiresStorageCredentials() bool { return true }

// TaskLabel: one dragonfly task per part, the label is the part name.
func (dragonflyBackend) TaskLabel(idx int) string { return dragonfly.PartName(idx) }

// StartPreheat expands the checkpoint directory into N independent preheat
// tasks.
//
// They must be N independent tasks - the same object maps to the same task_id
// and dfdaemon would serve it as one task, so the restore side's multi-path
// parallelism would gain nothing.
//
// Idempotency: every part's task carries a BIO of the form
// <CR-UID>#<partIndex>. Even when the status write failed, the next reconcile
// can recover the existing taskIDs per BIO without creating duplicates. The
// idempotency key is deliberately in BIO rather than status.TaskID: the latter
// is a single string that cannot hold N taskIDs.
func (b dragonflyBackend) StartPreheat(ctx context.Context, req preheat.StartPreheatRequest) ([]string, error) {
	ctxLog := log.FromContext(ctx)

	indices, err := dragonfly.DiscoverParts(ctx, req.RemoteURL, req.AccessKeyID, req.AccessKeySecret)
	if err != nil {
		return nil, fmt.Errorf("failed to discover checkpoint parts under %s: %v", req.RemoteURL, err)
	}
	ctxLog.Info(fmt.Sprintf("PreheatJob %s/%s discovered %d checkpoint part(s) under %s",
		req.Job.Namespace, req.Job.Name, len(indices), req.RemoteURL), "backend", b.Name())

	taskIDs := make([]string, 0, len(indices))
	for _, idx := range indices {
		bio := dragonflyPartBIO(req.Job, idx)
		partURL := dragonfly.PartURL(req.RemoteURL, idx)

		existingTaskID, err := b.Client.FindPreheatTask(ctx, bio)
		if err != nil {
			return nil, fmt.Errorf("failed to find existing %s preheat task for %s: %v", b.Name(), dragonfly.PartName(idx), err)
		}
		if existingTaskID != "" {
			ctxLog.Info(fmt.Sprintf("PreheatJob %s/%s found existing %s task %s for %s by BIO",
				req.Job.Namespace, req.Job.Name, b.Name(), existingTaskID, dragonfly.PartName(idx)))
			taskIDs = append(taskIDs, existingTaskID)
			continue
		}

		taskID, err := b.Client.CreatePreheatTask(ctx, dragonfly.PreheatRequest{
			URL:             partURL,
			BIO:             bio,
			AccessKeyID:     req.AccessKeyID,
			AccessKeySecret: req.AccessKeySecret,
			Options:         req.Job.Spec.Options,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create %s preheat task for %s: %v", b.Name(), dragonfly.PartName(idx), err)
		}
		taskIDs = append(taskIDs, taskID)
	}
	return taskIDs, nil
}

func (b dragonflyBackend) GetPreheatTask(ctx context.Context, taskID string) (preheat.Task, error) {
	task, err := b.Client.GetPreheatTask(ctx, taskID)
	if err != nil {
		return preheat.Task{}, err
	}
	// dragonfly.TaskState is an alias of preheat.TaskState, passed through directly.
	return preheat.Task{ID: task.ID, State: task.State, Message: task.Message}, nil
}

// ProbeCache discovers all seed peers via the Dragonfly scheduler ListHosts,
// then probes the cache state of each part task directly against each seed
// peer's own dfdaemon gRPC endpoint.
//
// ListHosts is the first-hand registry of Rust-client seed peers: dfdaemon
// self-reports to the scheduler at startup and deregisters on exit, so a seed
// peer that can be listed is alive and can take part in P2P distribution; the
// ip:port carried in the entry is exactly dfdaemon's gRPC endpoint, so probing
// hits this instance directly, with no pod discovery or node-side worker
// involved.
func (b dragonflyBackend) ProbeCache(ctx context.Context, job *v1beta1.PreheatJob) ([]v1beta1.PreheatPeerInfo, []preheat.CacheState, error) {
	logger := log.FromContext(ctx)
	now := metav1.Now()

	seedPeers, err := b.seedPeerSource.GetSeedPeers(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("list seed peers: %w", err)
	}
	if len(seedPeers) == 0 {
		return nil, nil, preheat.ErrNoProbeTargets
	}

	// dfdaemons key cached tasks by the scheduler generated task id, not by the
	// manager preheat job id kept in status.taskID, so recompute it from the
	// preheated URL.
	//
	// spec.remoteURL points at the checkpoint *directory* and one task id exists
	// per part, so a peer only counts as holding this checkpoint when it caches
	// **every** part: a peer missing one part cannot serve that part's download,
	// which is exactly the slowest lane of the parallel restore.
	partCount := len(decodeTaskIDs(job.Status.TaskID))
	if partCount == 0 {
		return nil, nil, fmt.Errorf("preheat job %s/%s has no task id recorded", job.Namespace, job.Name)
	}
	dfTaskIDs := make([]string, 0, partCount)
	for i := 0; i < partCount; i++ {
		dfTaskIDs = append(dfTaskIDs, dragonfly.TaskIDFromURL(dragonfly.PartURL(job.Spec.RemoteURL, i)))
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	newPeerList := make([]v1beta1.PreheatPeerInfo, 0, len(seedPeers))
	states := make([]preheat.CacheState, 0, len(seedPeers))

	for _, peer := range seedPeers {
		if peer.IP == "" || peer.Port == 0 {
			continue
		}
		peer := peer
		// The scheduler announces the dfdaemon grpc endpoint of the seed peer.
		addr := net.JoinHostPort(peer.IP, strconv.Itoa(int(peer.Port)))

		wg.Add(1)
		go func() {
			defer wg.Done()

			// worstState is the aggregate across all parts: cached only if every
			// part is cached, otherwise the first non-cached state is reported so
			// staleMessage still explains what went wrong.
			worstState := preheat.CacheStateCached
			for i, dfTaskID := range dfTaskIDs {
				probeCtx, cancel := context.WithTimeout(ctx, DefaultProbeTimeout)
				state, err := b.prober.ProbeTask(probeCtx, addr, dfTaskID)
				cancel()
				if err != nil {
					logger.Error(err, "probe seed peer dfdaemon failed",
						"seedPeer", peer.Hostname, "addr", addr, "part", dragonfly.PartName(i),
						"taskID", dfTaskID, "jobID", job.Status.TaskID)
				}
				if state != preheat.CacheStateCached {
					worstState = state
					break
				}
			}

			info := v1beta1.PreheatPeerInfo{
				IP:       peer.IP,
				NodeName: peer.Hostname,
				State:    peer.State,
				Cached:   worstState == preheat.CacheStateCached,
				LastSeen: &now,
			}

			mu.Lock()
			newPeerList = append(newPeerList, info)
			states = append(states, worstState)
			mu.Unlock()
		}()
	}
	wg.Wait()

	return newPeerList, states, nil
}

// dragonflyPartBIO returns the idempotency key of one part's Dragonfly job.
//
// It carries the part index because a PreheatJob now fans out into N Dragonfly
// tasks and FindPreheatTask looks tasks up by BIO — a bare CR UID would only
// ever find one of them back.
func dragonflyPartBIO(job *v1beta1.PreheatJob, partIdx int) string {
	return fmt.Sprintf("%s#%d", job.UID, partIdx)
}
