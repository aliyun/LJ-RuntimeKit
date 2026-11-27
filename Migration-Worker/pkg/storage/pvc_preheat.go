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

package storage

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/container-migration/worker/cmd/migration-worker/options"
	migrationworker "github.com/container-migration/worker/pkg/apis"

	"k8s.io/klog/v2"
)

// newPVCWithDragonflyPreheat builds the combined storage that prefers Dragonfly with PVC fallback (default path).
//
// Restore no longer depends on the PreheatJob CR: on every Restore, the worker
// derives the long-running Service name from (pvcNamespace, pvcName,
// checkpointDir) and takes its ClusterIP to rebuild the preheat URL (see
// derivePreheatSourceURL); shards are discovered from the local mount
// /mnt/pv/<checkpointDir>. Construction only creates the PVC fallback LocalFS;
// everything else is deferred to the first Restore: both the URL and shards
// depend on checkpointDir (one task may restore multiple container dirs), and
// the co-hosted source serves restore only, never started during checkpoint.

// Parameters for the EFC read progress-aware timeout. var instead of const so unit tests can shrink the time window.
var (
	// efcProgressCheckInterval is the granularity for checking EFC read progress.
	efcProgressCheckInterval = 1 * time.Minute
	// efcNoProgressTimeout is the window for declaring a hang: if the byte counter
	// of EFC read/extract stays flat for this long, the FUSE read is considered
	// permanently blocked (hung) and we fall back to PVC.
	efcNoProgressTimeout = 10 * time.Minute
)

// newLocalFSWithPath constructs and validates a LocalFS instance for reuse by combined storages.
func newLocalFSWithPath(opts *options.Options, path string) (*LocalFS, error) {
	iface, err := NewLocalFS(opts, &migrationworker.Storage{
		Local: &migrationworker.LocalStorage{Path: path},
	})
	if err != nil {
		return nil, err
	}
	lf, ok := iface.(*LocalFS)
	if !ok {
		return nil, fmt.Errorf("unexpected local storage type %T", iface)
	}
	return lf, nil
}

// PVCWithDerivedEFC is the combined storage for PVC mode with --pvc-preheat-backend=efc:
// the worker derives the relative path prefix under the EFC mount from the PVC/PV
// (resolvePVCEFCPrefix); Restore prefers /mnt/efc/<prefix>/<checkpointDir>, and any
// failure or hang falls back to reading the mounted PVC (/mnt/pv) directly.
type PVCWithDerivedEFC struct {
	primary  *LocalFS // reads /mnt/efc
	fallback *LocalFS // reads /mnt/pv
	prefix   string   // EFC relative path prefix derived from the PV (may be empty)
}

// newPVCWithDerivedEFC constructs the default combined storage: EFC first, PVC fallback.
//
// EFC is only an accelerator: if primary (/mnt/efc) fails to construct - node
// has no EFC deployed, read-only root filesystem, or no permission to create
// directories (NewLocalFS calls os.MkdirAll) - we always degrade to plain PVC
// direct read (/mnt/pv) with a Warning and never return an error; checkpoint
// uploads share the same construction path and do not depend on EFC at all, so
// they must not be rejected just because the accelerator dir cannot be created.
// Only failure to construct the PVC fallback itself returns an error.
func newPVCWithDerivedEFC(opts *options.Options, storageInfo *migrationworker.Storage) (StorageInterface, error) {
	primary, err := newLocalFSWithPath(opts, efcMountRoot)
	if err != nil {
		klog.Warningf("efc primary storage unavailable (%v), degrading to PVC direct read only", err)
		return newLocalFSWithPath(opts, pvcMountPoint)
	}
	fallback, err := newLocalFSWithPath(opts, pvcMountPoint)
	if err != nil {
		return nil, fmt.Errorf("create pvc fallback local storage: %w", err)
	}
	// Empty when prefix derivation fails: EFC read uses checkpointDir directly, and a miss quickly falls back.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	prefix := resolvePVCEFCPrefix(ctx, storageInfo.Pvc.PvcNamespace, storageInfo.Pvc.PvcName)
	return &PVCWithDerivedEFC{primary: primary, fallback: fallback, prefix: prefix}, nil
}

func (p *PVCWithDerivedEFC) Name() string {
	return "pvc-with-derived-efc"
}

// Upload always writes checkpoint data to the mounted PVC; EFC only accelerates restore reads.
func (p *PVCWithDerivedEFC) Upload(reader io.Reader, dstTarFile string) (int64, error) {
	return p.fallback.Upload(reader, dstTarFile)
}

// Restore prefers EFC (path = prefix + checkpointDir); any error, hang or ctx
// cancellation falls back to reading the mounted PVC directly (see
// restoreFromEFCWithFallback).
func (p *PVCWithDerivedEFC) Restore(ctx context.Context, checkpointDir string, dstDir string) (*RestoreInfo, error) {
	return restoreFromEFCWithFallback(ctx, p.primary, efcRelPath(p.prefix, checkpointDir),
		p.fallback, checkpointDir, dstDir)
}

func (p *PVCWithDerivedEFC) CleanUpload(path string) error {
	return p.fallback.CleanUpload(path)
}

func (p *PVCWithDerivedEFC) GetUploadInfo() (*UploadInfo, error) {
	return p.fallback.GetUploadInfo()
}

func (p *PVCWithDerivedEFC) GetDownloadInfo() (*RestoreInfo, error) {
	return mergeDownloadInfo(p.primary, p.fallback)
}

// restoreFromEFCWithFallback is the shared implementation for EFC-first reads
// (specific to PVCWithDerivedEFC): a goroutine runs primary.Restore (full dir
// listing plus shard streaming extract on the EFC mount), while the main
// goroutine monitors progress - every efcProgressCheckInterval it reads the
// DownloadReader counters of primary, and efcNoProgressTimeout of zero growth
// declares the FUSE read permanently blocked (hung); on error or hang it falls
// back to reading the PVC directly with a log entry; task-level ctx cancellation
// terminates immediately (item.Ctx has no deadline, Done means the task was
// abandoned upstream, nobody consumes fallback results, and a cancelled ctx makes
// fallback fail at the first UnpackPart checkpoint anyway).
//
// Progress-aware instead of a hard timeout: a cold EFC read (backed by OSS/NAS)
// or a large dataset can be slow but still advancing; a hard timeout would kill
// such "slow but alive" reads and waste time. The window keeps renewing while
// the counter grows, and only a true hang (including a flat zero counter during
// dir listing) triggers fallback.
//
// The primary and fallback paths share dstDir, so their commit and cleanup must
// be isolated from each other:
//   - primary uses a derived ctx; when fallback triggers, cancel makes it exit
//     as soon as it hits a ctx-aware operation (ineffective for a goroutine stuck
//     in FUSE read - an acceptable leak: the process is long-running and only
//     this restore is affected);
//   - fallback passes a non-empty tmpSuffix via restoreWith, so the two temp
//     dirs are separate and one side's RemoveAll never deletes the dir the other
//     side is writing;
//   - primary passes abandonCheck via restoreWith: even if primary recovers from
//     the hang after fallback started and extracts successfully, seeing itself
//     abandoned before commit it only cleans up its own tmp and never touches
//     dstDir - otherwise primary's RemoveAll(dstDir)+rename would delete what
//     fallback already committed.
//
// Isolation parameters are passed per call instead of shared on the instance:
// concurrent Restores of a multi-container task each hold their own (own
// abandoned flag, own tmp suffix), with no cross interference.
func restoreFromEFCWithFallback(ctx context.Context, primary *LocalFS, efcRel string,
	fallback *LocalFS, checkpointDir, dstDir string) (*RestoreInfo, error) {
	type result struct {
		info *RestoreInfo
		err  error
	}
	// primary uses a derived ctx: when fallback triggers, cancel makes it exit
	// quickly at inter-shard checkpoints; the abandoned flag prevents primary from
	// committing even if it recovers (see the abandon check in restoreIntoTemp).
	// fallback passes the "-fallback" suffix to keep its tmp separate from primary.
	pCtx, pCancel := context.WithCancel(ctx)
	defer pCancel()
	var abandoned atomic.Bool

	ch := make(chan result, 1)
	go func() {
		// Parameterized isolation: the abandonCheck closure captures this
		// call's abandoned flag, so concurrent Restore calls of other
		// containers (each holding their own) never interfere.
		info, err := primary.restoreWith(pCtx, efcRel, dstDir, "", func() bool { return abandoned.Load() })
		ch <- result{info: info, err: err}
	}()

	lastSize := primary.DownloadReader.Size()
	noProgressSince := time.Now()
	ticker := time.NewTicker(efcProgressCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case r := <-ch:
			if r.err == nil {
				return r.info, nil
			}
			klog.Warningf("efc read failed for %s, falling back to PVC direct read: %v", checkpointDir, r.err)
		case <-ticker.C:
			if size := primary.DownloadReader.Size(); size > lastSize {
				lastSize = size
				noProgressSince = time.Now()
				continue
			}
			if time.Since(noProgressSince) >= efcNoProgressTimeout {
				klog.Warningf("efc read made no progress for %s (checkpoint %s), falling back to PVC direct read",
					efcNoProgressTimeout, checkpointDir)
			} else {
				continue
			}
		case <-ctx.Done():
			// Task-level cancellation: terminate directly without fallback.
			// A cancelled ctx makes fallback fail immediately at the first
			// UnpackPart ctx checkpoint (untar canceled), rendering fallback
			// useless; and item.Ctx has no deadline - Done means the task was
			// abandoned upstream, so nobody consumes fallback results. pCtx is
			// auto-cancelled by ctx derivation; primary either exits at a
			// checkpoint or stays stuck in a bare read (error cleanup only
			// touches its own tmp, no race).
			return nil, ctx.Err()
		}
		// Every fallback path first makes primary abandon the commit, then
		// starts fallback: primary may recover from the hang and finish
		// extracting; without abandoning, it would delete fallback results
		// at commit time.
		pCancel()
		abandoned.Store(true)
		return fallback.restoreWith(ctx, checkpointDir, dstDir, "-fallback", nil)
	}
}

// PVCWithPreheat is the combined storage for PVC mode with Dragonfly preheat enabled:
//
//   - Upload goes through the local plugin (checkpoint writes the mounted PVC
//     /mnt/pv directly, same as without preheat);
//   - Restore first pulls via the local dfdaemon - a P2P cache hit is delivered
//     from local disk, a miss goes P2P from a nearby seed peer; when a shard's P2P
//     download fails (retries exhausted), that shard falls back per-part to
//     reading the mounted PVC directly; overall extract failure still falls back
//     to full direct read (local fallback).
//
// Fallback must be implemented by the worker: dfdaemon's origin fetch only knows
// remoteURL (http sources), not PVC; and the preheat source pod is deleted once
// preheat becomes Ready, so restore cannot depend on it being long-lived - origin
// reachability is guaranteed by this worker co-hosting the source (same PVC mount,
// self-registered EndpointSlice, see pvc_preheat_source.go).
type PVCWithPreheat struct {
	opts     *options.Options
	pvc      *migrationworker.PVC // (namespace, name) needed to derive the long-running Service name
	fallback *LocalFS             // /mnt/pv direct-read fallback

	// sourceOnce ensures the co-hosted source starts at most once (lazily on the
	// first Restore, never during checkpoint). Start failure only logs, no retry:
	// registration failure is an environment issue, retrying would likely fail too.
	sourceOnce sync.Once
	// sourcePort is the actual listening port of the co-hosted source (0 = not started), used for EndpointSlice registration.
	sourcePort int
	sourceErr  error

	// primary is the Dragonfly instance built by the most recent Restore, used
	// only by GetDownloadInfo to aggregate progress (merged with the fallback
	// counters under the same metric).
	mu      sync.Mutex
	primary *Dragonfly
}

// newPVCWithDragonflyPreheat constructs the combined storage. Construction only
// creates the PVC fallback LocalFS (primary depends on checkpointDir and is
// deferred to the first Restore): preheat anomalies must not block the restore
// main path.
func newPVCWithDragonflyPreheat(opts *options.Options, storageInfo *migrationworker.Storage) (StorageInterface, error) {
	fallback, err := newLocalFSWithPath(opts, pvcMountPoint)
	if err != nil {
		return nil, fmt.Errorf("create pvc fallback local storage: %w", err)
	}
	return &PVCWithPreheat{opts: opts, pvc: storageInfo.Pvc, fallback: fallback}, nil
}

func (p *PVCWithPreheat) Name() string {
	return "pvc-with-dragonfly-preheat"
}

// Upload always writes checkpoint data to the mounted PVC; preheat does not change the upload path.
func (p *PVCWithPreheat) Upload(reader io.Reader, dstTarFile string) (int64, error) {
	return p.fallback.Upload(reader, dstTarFile)
}

// ensureSourceOnce lazily starts the co-hosted source: an http file server on a
// random port. Failure only warns - the co-hosted source is an accelerator; when
// the origin is unreachable, a miss falls back to per-part direct PVC read, so
// correctness is unaffected.
func (p *PVCWithPreheat) ensureSourceOnce() {
	p.sourceOnce.Do(func() {
		src, err := startPreheatSourceServer()
		if err != nil {
			p.sourceErr = err
			klog.Warningf("failed to start preheat source http server: %v", err)
			return
		}
		p.sourcePort = src.port
		klog.V(2).Infof("preheat source http server listening on 0.0.0.0:%d", src.port)
	})
}

// ensurePreheatSource registers this worker's EndpointSlice (serviceName derived
// from checkpointDir, idempotent create-or-update; repeated Restores of multiple
// containers have no side effects). A not-started source or registration failure
// only warns and never blocks restore.
func (p *PVCWithPreheat) ensurePreheatSource(ctx context.Context, checkpointDir string) {
	if p.sourceErr != nil || p.sourcePort == 0 {
		return // co-hosted source not started, nothing to register (start failure already warned)
	}
	serviceName := preheatSourceServiceName(p.pvc.PvcNamespace, p.pvc.PvcName, checkpointDir)
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := registerPreheatSourceEndpointSlice(ctx, p.pvc.PvcNamespace, serviceName, p.sourcePort); err != nil {
		klog.Warningf("failed to register preheat source endpointslice %s: %v", serviceName, err)
	}
}

// buildPrimary rebuilds the preheat read path from checkpointDir: derive URL ->
// local shard discovery -> construct the http-source Dragonfly. Any failure
// (Service missing / no kube config / no shards in the dir) returns an error and
// the caller degrades to direct PVC read.
func (p *PVCWithPreheat) buildPrimary(ctx context.Context, checkpointDir string) (*Dragonfly, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	sourceURL, err := derivePreheatSourceURL(ctx, p.pvc.PvcNamespace, p.pvc.PvcName, checkpointDir)
	if err != nil {
		return nil, err
	}
	// Shard discovery: the local mount dir (one-to-one with preheat shards).
	// Discovery failure (checkpoint dir not ready yet / moved) degrades to direct
	// read without constructing Dragonfly.
	indices, err := DiscoverLocalParts(filepath.Join(pvcMountPoint, checkpointDir))
	if err != nil {
		return nil, fmt.Errorf("discover checkpoint parts under %s: %w", checkpointDir, err)
	}
	return newDragonflyWithHTTPSource(p.opts, sourceURL, indices)
}

// Restore first pulls via dfdaemon, falling back to reading the mounted PVC directly.
//
// Dragonfly's Restore extracts into <dstDir>-tmp, atomically renames on success,
// and cleans up tmp on failure (the unified restoreIntoTemp semantics), so dstDir
// is never polluted by partial data when falling back.
func (p *PVCWithPreheat) Restore(ctx context.Context, checkpointDir string, dstDir string) (*RestoreInfo, error) {
	// Co-hosted source lazy start + EndpointSlice registration (serviceName derived
	// from checkpointDir registered here; repeated Restores of the same (PVC,path)
	// are deduplicated by ensurePreheatSource).
	p.ensureSourceOnce()
	p.ensurePreheatSource(ctx, checkpointDir)

	primary, err := p.buildPrimary(ctx, checkpointDir)
	if err != nil {
		klog.Warningf("dragonfly preheat path unavailable for %s, falling back to PVC direct read: %v", checkpointDir, err)
		return p.fallback.Restore(ctx, checkpointDir, dstDir)
	}
	// Per-part fallback: when a shard's P2P download fails after retries are
	// exhausted, read the same shard directly from /mnt/pv while the remaining
	// shards keep going P2P - so a single failure does not drag the whole restore
	// back to slow direct read. Direct-read bytes go into the fallback counters,
	// sharing the transfer metric with P2P downloads.
	primary.fallbackPartDir = pvcMountPoint
	primary.fallbackCounter = p.fallback.DownloadReader

	p.mu.Lock()
	p.primary = primary
	p.mu.Unlock()

	info, err := primary.Restore(ctx, checkpointDir, dstDir)
	if err == nil {
		return info, nil
	}
	klog.Warningf("dragonfly download failed for %s, falling back to PVC direct read: %v", checkpointDir, err)
	return p.fallback.Restore(ctx, checkpointDir, dstDir)
}

func (p *PVCWithPreheat) CleanUpload(path string) error {
	return p.fallback.CleanUpload(path)
}

func (p *PVCWithPreheat) GetUploadInfo() (*UploadInfo, error) {
	return p.fallback.GetUploadInfo()
}

func (p *PVCWithPreheat) GetDownloadInfo() (*RestoreInfo, error) {
	p.mu.Lock()
	primary := p.primary
	p.mu.Unlock()
	if primary == nil {
		// Restore not started yet: only the fallback counter (0).
		return p.fallback.GetDownloadInfo()
	}
	return mergeDownloadInfo(primary, p.fallback)
}

// mergeDownloadInfo merges the primary and fallback counters: during fallback,
// primary's counter is necessarily frozen (EFC hang or overall dragonfly
// failure); reporting only primary would make the upper progress ticker keep
// reporting a frozen bytesTransferred with zero rate, looking like a stalled
// restore. Summing the two lets the real fallback progress show up. During EFC,
// the fallback counter is always 0, so the sum is the real EFC progress; for the
// Dragonfly combination, per-part fallback bytes are already counted in the
// fallback counter, so summing also covers progress missed on the normal path.
func mergeDownloadInfo(primary, fallback StorageInterface) (*RestoreInfo, error) {
	pi, err := primary.GetDownloadInfo()
	if err != nil {
		return nil, err
	}
	fi, err := fallback.GetDownloadInfo()
	if err != nil {
		return nil, err
	}
	return &RestoreInfo{
		FileCount: pi.FileCount + fi.FileCount,
		FileSize:  pi.FileSize + fi.FileSize,
	}, nil
}
