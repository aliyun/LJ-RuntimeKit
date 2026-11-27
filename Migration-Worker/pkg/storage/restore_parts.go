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
	"os"
	"path/filepath"
	"sync"
	"time"

	migutil "github.com/container-migration/worker/pkg/util"
	migarchive "github.com/container-migration/worker/pkg/util/archive"

	"k8s.io/klog/v2"
)

// reaperSentinelDirName is the sentinel dir name pre-created before download.
//
// XDM's rootfs reaper decides
// whether a checkpoint may still be in use by one criterion alone: "does a
// memory/ sit next to rootfs/". That criterion was originally built on "in a
// single tar memory/ precedes rootfs/, so when streaming extract sees rootfs,
// memory must already exist".
//
// After sharding, N workers extract out of order and that ordering guarantee is
// gone entirely: the reaper could delete the whole checkpoint dir after rootfs
// lands but before memory finishes downloading. So here we pre-create an empty
// memory/ **before** unpacking: the reaper only does os.Lstat existence checks,
// and an empty dir is enough to make it skip.
//
// That way the XDM-side reaper needs zero code changes.
//
// ⚠️ Conversely, this sentinel is not optional defense - it is now the only
// support for that criterion: deleting it would let the reaper delete the
// landed rootfs/ mid-download. The XDM side's reaperMemoryDirName comment
// records the same contract; both sides change together or not at all.
const reaperSentinelDirName = "memory"

// restoreIntoTemp owns the full lifecycle of <dstDir>-tmp.
//
// ⚠️ The order of these three steps cannot change; the 4 storage implementations share this so none of them misses one:
//
//  1. Clean tmp - once only. The unpack functions must **never** clean the
//     target dir again, otherwise the N workers would delete each other's
//     just-landed data (this is exactly why the os.RemoveAll(dest) at the head
//     of UnTarWithReader cannot be reused on the parallel path).
//  2. Pre-create the memory/ sentinel - must happen before unpack, see
//     reaperSentinelDirName.
//  3. On failure delete the whole tmp, **including rootfs/**.
//
// Point 3 is strictly separated from the cleanup of a failed runc restore:
//   - download/extract failure: the data is still in tmp and by definition
//     cannot be referenced by any overlay mount (XDM only mounts lowerdir from
//     the final path; data first becomes referenceable at the Rename), so
//     deleting the whole thing is safe without checking "is any pod using it".
//   - runc restore failure: goes through deleteCheckpointNonRootfsData, which
//     deletes only memory/ and xpu/, **keeping rootfs/** - because another pod
//     may have already mounted the same data successfully.
//
// This also closes an existing leak: the Restore failure paths of the four
// storages never cleaned tmp before; the leftover memory/ made the reaper skip
// forever, so tens of GB could never be reclaimed.
//
// tmpSuffix separates the temp dirs of concurrent multi-path restores
// (<dstDir><tmpSuffix>-tmp): in the EFC-first + PVC-fallback combined read the
// primary and fallback paths share dstDir and each must use its own tmp,
// otherwise one side's RemoveAll deletes the dir the other side is writing;
// plain single-path calls pass "".
//
// abandoned is the commit-abandon check (nil for single-path calls): called
// after extract completes, before deleting the old dst and renaming. Returning
// true means this path has been abandoned (e.g. the EFC primary path hung and
// timed out, and the fallback path took over dstDir); then only clean up our
// own tmp and never touch dstDir - otherwise we would delete what the fallback
// path already committed.
func restoreIntoTemp(dstDir string, tmpSuffix string, abandoned func() bool, unpack func(tmpDir string) error) error {
	tmpDir := fmt.Sprintf("%s%s-tmp", dstDir, tmpSuffix)

	if err := os.RemoveAll(tmpDir); err != nil {
		return fmt.Errorf("clean stale temp dir %s: %w", tmpDir, err)
	}
	if err := os.MkdirAll(filepath.Join(tmpDir, reaperSentinelDirName), 0700); err != nil {
		return fmt.Errorf("create reaper sentinel under %s: %w", tmpDir, err)
	}

	if err := unpack(tmpDir); err != nil {
		if rmErr := os.RemoveAll(tmpDir); rmErr != nil {
			klog.Errorf("restore failed and cleanup of %s also failed: %v (original error: %v)", tmpDir, rmErr, err)
		} else {
			klog.Infof("restore failed, removed incomplete checkpoint data at %s (including rootfs)", tmpDir)
		}
		return err
	}

	if abandoned != nil && abandoned() {
		if rmErr := os.RemoveAll(tmpDir); rmErr != nil {
			klog.Errorf("abandoned restore and cleanup of %s also failed: %v", tmpDir, rmErr)
		}
		return fmt.Errorf("restore result abandoned, fallback path owns %s", dstDir)
	}

	if err := os.RemoveAll(dstDir); err != nil {
		return fmt.Errorf("remove previous checkpoint data at %s: %w", dstDir, err)
	}
	if err := os.Rename(tmpDir, dstDir); err != nil {
		return fmt.Errorf("rename %s to %s: %w", tmpDir, dstDir, err)
	}
	return nil
}

// partOpener opens the idx-th shard and returns its byte stream.
//
// Provided by each storage implementation: Dragonfly uses dfdaemon's
// growingFileReader (download and extract in one stream), OSS uses the SDK
// download stream, localfs / nfs just os.Open. cleanup is called after this
// shard finishes extracting, to reclaim temp files and the like.
type partOpener func(ctx context.Context, idx int) (r io.Reader, cleanup func(), err error)

// unpackPartsConcurrently extracts the N shards into the same dir concurrently.
//
// This is where the whole sharding scheme pays off: each worker independently
// downloads and extracts its own shard, so total time is roughly max(per-worker
// time) instead of sum. Every shard carries the full dir skeleton, and each
// file (or each range of the same file) belongs to exactly one shard, so there
// are no ordering dependencies between workers.
//
// Note runc restore still waits for all shards to land (it reads the complete
// memory/), so this barrier is required - parallelism compresses the data
// landing phase only.
func unpackPartsConcurrently(ctx context.Context, indices []int, dest string,
	open partOpener, opts *migarchive.TarOptions) error {

	extractStartedAt := time.Now()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		firstEr error
		splits  []migarchive.SplitFileInfo
	)

	for _, idx := range indices {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			r, cleanup, err := open(ctx, idx)
			if cleanup != nil {
				defer cleanup()
			}
			if err != nil {
				recordErr(&mu, &firstEr, cancel, fmt.Errorf("open checkpoint part %s: %w", PartName(idx), err))
				return
			}

			partSplits, err := migarchive.UnpackPart(ctx, r, dest, opts)
			if err != nil {
				recordErr(&mu, &firstEr, cancel, fmt.Errorf("unpack checkpoint part %s: %w", PartName(idx), err))
				return
			}

			mu.Lock()
			splits = append(splits, partSplits...)
			mu.Unlock()
			klog.V(4).Infof("[restore][dest=%s] part=%s extracted", dest, PartName(idx))
		}(idx)
	}
	wg.Wait()

	if firstEr != nil {
		klog.Errorf("[restore][dest=%s] stage=transfer-extract status=failed, parts=%d, took=%s: %v",
			dest, len(indices), time.Since(extractStartedAt).Truncate(time.Millisecond), firstEr)
		return firstEr
	}

	// Must be after the barrier: a split large file is pwritten by multiple
	// workers, and any worker's write bumps mtime to now, so the final metadata can
	// only be settled after all writes. This also verifies that the union of ranges
	// exactly covers the original file length, catching "a shard is missing or
	// truncated".
	if err := migarchive.ApplySplitFileMetadata(splits); err != nil {
		klog.Errorf("[restore][dest=%s] stage=transfer-extract status=failed, parts=%d, took=%s: %v",
			dest, len(indices), time.Since(extractStartedAt).Truncate(time.Millisecond), err)
		return err
	}

	klog.V(2).Infof("[restore][dest=%s] stage=transfer-extract status=completed, parts=%d, took=%s",
		dest, len(indices), time.Since(extractStartedAt).Truncate(time.Millisecond))
	return nil
}

func recordErr(mu *sync.Mutex, dst *error, cancel context.CancelFunc, err error) {
	mu.Lock()
	if *dst == nil {
		*dst = err
	}
	mu.Unlock()
	// One failure cancels the rest: no point downloading data doomed to be deleted wholesale.
	cancel()
}

// restoreInfoOf aggregates the extract result, reused by each storage's Restore return value.
func restoreInfoOf(dstDir string) (int64, error) {
	fileCount, _, err := migutil.GetDirData(dstDir)
	if err != nil {
		return 0, fmt.Errorf("error getting file %s attribute: %w", dstDir, err)
	}
	if fileCount == 0 {
		return 0, fmt.Errorf("restore extracted 0 files to %s — the checkpoint parts may be empty", dstDir)
	}
	return fileCount, nil
}
