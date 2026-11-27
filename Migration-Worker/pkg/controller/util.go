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

package controller

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	migrationworker "github.com/container-migration/worker/pkg/apis"

	"k8s.io/klog/v2"
)

// respGrpc converts an internal Go error into a BaseResponse with the
// canonical worker status codes used by every gRPC handler.
func respGrpc(err error, requestID string) *migrationworker.BaseResponse {
	if err != nil {
		return &migrationworker.BaseResponse{
			Status:    StatusInternalError,
			Message:   err.Error(),
			RequestID: requestID,
		}
	}
	return &migrationworker.BaseResponse{
		Status:    StatusOK,
		RequestID: requestID,
	}
}

// deleteRestoreDir removes path if it exists. It is a no-op if path
// does not exist, so callers can use it for both cleanup-on-cancel and
// cleanup-on-success without prior stat checks.
func deleteRestoreDir(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil
	}
	return os.RemoveAll(path)
}

// deleteRestoreDirUnder removes path and then any ancestor directory that became
// empty, stopping at homeDir. Without this the per-task layout
// <homeDir>/<podUID>/<version>/<containerName>/ leaves three empty directories
// behind for every migration, which piles up on long-lived nodes.
func deleteRestoreDirUnder(homeDir, path string) error {
	if err := deleteRestoreDir(path); err != nil {
		return err
	}
	pruneEmptyDirsUpTo(homeDir, filepath.Dir(path))
	return nil
}

// pruneEmptyDirsUpTo walks from dir towards homeDir removing empty directories.
// It never touches homeDir itself, and stops at the first non-empty directory so
// that data belonging to other tasks is left alone.
func pruneEmptyDirsUpTo(homeDir, dir string) {
	home := filepath.Clean(homeDir)
	cur := filepath.Clean(dir)

	for cur != home && strings.HasPrefix(cur, home+string(os.PathSeparator)) {
		entries, err := os.ReadDir(cur)
		if err != nil || len(entries) > 0 {
			return
		}
		if err := os.Remove(cur); err != nil {
			klog.V(4).Infof("Skip removing empty dir %s: %v", cur, err)
			return
		}
		klog.V(4).Infof("Removed empty dir %s", cur)
		cur = filepath.Dir(cur)
	}
}

// Extras keys carrying the transfer speed of an in-flight checkpoint upload or
// restore download. Both are MB/s rendered with two decimals.
const (
	// ExtraKeyAvgTransferSpeed is the average speed since the task started,
	// i.e. bytes transferred so far divided by the elapsed time.
	ExtraKeyAvgTransferSpeed = "avgTransferSpeed"
	// ExtraKeyInstantTransferSpeed is the speed observed between the two most
	// recent samples, which the status ticker takes once per second.
	ExtraKeyInstantTransferSpeed = "instantTransferSpeed"
)

// minSpeedSampleInterval guards the instant speed against a division by a
// near-zero interval: two samples taken back to back would turn a handful of
// bytes into an absurd rate.
const minSpeedSampleInterval = 10 * time.Millisecond

// speedSampler turns a monotonically increasing byte counter into a speed over
// the interval between two consecutive samples ("instant" speed), as opposed to
// the average over the whole task.
//
// It is not safe for concurrent use: every checkpoint/restore task keeps its own
// sampler, touched only by that task's status ticker goroutine.
type speedSampler struct {
	lastBytes int64
	lastTime  time.Time
	lastSpeed float64
}

// newSpeedSampler starts a sampler at the task's start time with no bytes
// transferred yet. That makes the very first sample cover the whole elapsed
// time, so it equals the average speed — which is exactly what should be
// reported before a second sample exists.
func newSpeedSampler(start time.Time) *speedSampler {
	return &speedSampler{lastTime: start}
}

// SampleMBPerSec records the current cumulative byte count and returns the
// speed in MB/s over the interval since the previous sample.
//
// It keeps the previous result when the interval is too short to measure, and
// clamps a counter that went backwards to zero, so callers can report the
// returned value unconditionally.
func (s *speedSampler) SampleMBPerSec(totalBytes int64) float64 {
	now := time.Now()
	elapsed := now.Sub(s.lastTime)
	if elapsed < minSpeedSampleInterval {
		return s.lastSpeed
	}

	delta := totalBytes - s.lastBytes
	if delta < 0 {
		delta = 0
	}
	s.lastBytes = totalBytes
	s.lastTime = now
	s.lastSpeed = float64(delta) / 1024 / 1024 / elapsed.Seconds()
	return s.lastSpeed
}

// avgSpeedMBPerSec returns totalBytes over elapsed in MB/s. A non-positive
// elapsed yields 0 rather than an infinity.
func avgSpeedMBPerSec(totalBytes int64, elapsed time.Duration) float64 {
	if elapsed <= 0 {
		return 0
	}
	return float64(totalBytes) / 1024 / 1024 / elapsed.Seconds()
}

// formatSpeed renders a MB/s value the way the Extras fields expect it.
func formatSpeed(mbPerSec float64) string {
	return fmt.Sprintf("%.2f", mbPerSec)
}

// extrasMissingFrom returns the entries of snapshot whose keys final does not
// already carry.
//
// It exists because a finished task and its progress ticker report overlapping
// keys. The ticker snapshot must survive for the keys only it knows
// (instantTransferSpeed, ossUpload*, waitingTaskNum, ...), while the values the
// completed task computed from the final size and duration must win over the
// ticker's last, up-to-a-second-stale sample.
func extrasMissingFrom(final *migrationworker.CheckpointAttributes, snapshot map[string]string) map[string]string {
	if len(snapshot) == 0 {
		return nil
	}
	if final == nil || len(final.Extras) == 0 {
		return snapshot
	}
	out := make(map[string]string, len(snapshot))
	for k, v := range snapshot {
		if _, ok := final.Extras[k]; ok {
			continue
		}
		out[k] = v
	}
	return out
}

// checkpointRootfsSubdir is the subdirectory of a per-container checkpoint dir
// that xpu-container-runtime / XDM keeps mounted as an overlay lowerdir on
// every restored pod. It MUST NOT be removed by CleanTask; a node-side reaper
// handles it once every pod referencing it is gone.
const checkpointRootfsSubdir = "rootfs"

// deleteCheckpointNonRootfsData removes everything directly under baseDir
// except the rootfs/ subdirectory. Missing baseDir is treated as success so
// callers can invoke it unconditionally after a restore.
//
// baseDir is the per-container checkpoint directory returned by
// containerDistCheckpointDir: for the flat layout it is <hostPath>/data; for
// the default layout it is <homeDir>/<podUID>/<version>/<container>/checkpoint.
// In both layouts memory/ and xpu/ (and any future non-rootfs entries) are
// safe to delete once restore completes.
func deleteCheckpointNonRootfsData(baseDir string) error {
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read checkpoint dir %s: %w", baseDir, err)
	}
	for _, e := range entries {
		if e.Name() == checkpointRootfsSubdir {
			continue
		}
		target := filepath.Join(baseDir, e.Name())
		if err := os.RemoveAll(target); err != nil {
			return fmt.Errorf("remove %s: %w", target, err)
		}
	}
	return nil
}
