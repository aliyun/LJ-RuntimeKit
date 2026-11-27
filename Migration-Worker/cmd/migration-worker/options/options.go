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

package options

import (
	"time"

	"github.com/spf13/pflag"
)

// Legal values of PvcPreheatBackend. PvcPreheatBackendDragonfly is the default:
// the restore side derives the long-running Service name from (pvcNamespace,
// pvcName, pvcPath), takes its ClusterIP to rebuild the preheat URL, pulls via
// the local dfdaemon (P2P hit) and co-hosts the origin; PvcPreheatBackendEFC
// switches to EFC-first + PVC fallback (no co-hosted source). Other values are
// rejected by main at startup.
const (
	PvcPreheatBackendDragonfly = "dragonfly"
	PvcPreheatBackendEFC       = "efc"
)

// Options holds every runtime-tunable parameter of the migration-worker
// process. Defaults can be overridden either via CLI flags (see Flags)
// or via the deployment manifest.
type Options struct {
	// GrpcPort is the TCP port on which the worker exposes the
	// container-state-migration gRPC service.
	GrpcPort int

	// SaveDir is the root directory under which the worker stages
	// migration data (downloaded checkpoints, extracted bundles).
	SaveDir string

	// XDMSocket is the path to the X-Device-Manager gRPC unix domain socket.
	// Used by the runtime to call Checkpoint. If not set in the deployment
	// manifest, the default value below is used.
	XDMSocket string

	// CheckpointParallelism is the number of concurrent checkpoint
	// jobs the worker is willing to run.
	CheckpointParallelism int

	// RestoreParallelism is the number of concurrent restore jobs
	// the worker is willing to run.
	RestoreParallelism int

	// HttpClientTimeout caps any HTTP-style transfer the worker
	// performs (e.g. talking to remote storage).
	HttpClientTimeout time.Duration

	// PrometheusPort is the TCP port on which /metrics is served.
	PrometheusPort int

	// ServeHTTP enables the standalone http file server mode used by the PVC
	// preheat source pod: it serves the PVC mount point (/mnt/pv) as static
	// files (with Range support) so Dragonfly seed peers can pull checkpoint
	// data from it as the origin. In this mode the worker does not start the
	// gRPC server, the prometheus endpoint or the controller scheduler.
	ServeHTTP bool

	// HTTPPort is the TCP port for the http file server in serve-http mode.
	// Fixed at 50090 by the preheat controller to keep preheat URLs stable.
	HTTPPort int

	// PvcPreheatBackend selects which preheat link the PVC restore path
	// prefers: "dragonfly" (default) derives the per-(pvcNamespace,pvcName,
	// pvcPath) resident Service name, reads its ClusterIP to rebuild the
	// preheat URL and pulls through dfdaemon with a direct /mnt/pv fallback;
	// "efc" reads /mnt/efc first with a direct /mnt/pv fallback. Any other
	// value is rejected at startup.
	PvcPreheatBackend string

	// OssMultiPartSize is the per-part size (MB) used by the OSS
	// multi-part upload path.
	OssMultiPartSize int64

	// OssUploadThreadsCount is the worker count used by the OSS
	// multi-part upload path.
	OssUploadThreadsCount int

	// DfdaemonEndpoint is the default dfdaemon gRPC address for
	// Dragonfly2 storage backend. Can be a Unix socket path
	// (e.g., "/var/run/dragonfly/dfdaemon.sock" or "unix:///var/run/...")
	// or a TCP address (e.g., "127.0.0.1:65000").
	// Can be overridden per-request via the Dragonfly protobuf config.
	DfdaemonEndpoint string

	// DragonflyPieceSize is the chunk size (bytes) for Dragonfly2
	// upload/download piece splitting.
	//
	// MUST stay 0 unless you also control the preheat side. dfdaemon folds
	// piece_length into the task id (IDGenerator::task_id hashes
	// url + tag + application + piece_length + task type), and the preheat job
	// created by the controller goes through the manager OAPI without any
	// piece_length. Sending one here therefore yields a different task id than
	// the preheated one, so the download misses the warm cache entirely and
	// falls back to the OSS origin.
	//
	// A fixed value also disables dfdaemon's OptimizeByFileLength strategy,
	// which scales the piece up to MAX_PIECE_LENGTH (64 MiB) for multi-GiB
	// tars. Pinning it to 4 MiB shrinks the in-flight window to
	// concurrentPieceCount * 4 MiB and throttles the download pipeline.
	DragonflyPieceSize int32

	// DragonflyReorderBufferMB caps the in-memory reorder buffer used when
	// streaming a Dragonfly P2P download into tar extraction. dfdaemon returns
	// pieces out of order (concurrentPieceCount), so late pieces are buffered in
	// RAM until their predecessors arrive. This cap bounds that buffer to avoid
	// OOM on very large checkpoints; 0 means unlimited (legacy behavior).
	DragonflyReorderBufferMB int

	// DragonflyPieceStallTimeout is how long the reassembler waits for the
	// next expected piece before re-fetching it directly with a ranged
	// request (hedged request). dfdaemon may silently drop a piece when it
	// fails to push it into the gRPC stream ("send DownloadPieceFinished
	// Response ... failed: Timeout"), in which case waiting forever would
	// stall the whole download. Keep this below dfdaemon's pieceTimeout so
	// the gap is repaired before the stream ends. 0 disables hedged
	// re-fetch (end-of-stream repair still applies).
	DragonflyPieceStallTimeout time.Duration

	// DragonflySpillLimitMB is a HARD cap on the amount of un-reassembled
	// data buffered on disk. Once reached, the stream consumer stops calling
	// Recv() until the reassembler drains some of it (i.e. real backpressure).
	//
	// A cap is essential: dfdaemon can deliver ~100 MiB/s from its local cache
	// while tar extraction only sustains ~38 MiB/s, so an unbounded spill grows
	// to the full task size (observed: 34+ GiB for a 47 GiB task) and its
	// write+read traffic competes for the very disk the extraction needs.
	//
	// Blocking Recv() is safe here because the piece the reassembler is
	// actually waiting for is always handed over immediately, bypassing this
	// cap; only pieces further ahead in the stream are held back. 0 disables
	// the cap (unbounded, not recommended).
	DragonflySpillLimitMB int
}

// Default returns Options pre-filled with the values used by the
// reference deployment.
func Default() *Options {
	return &Options{
		GrpcPort:                 50051,
		SaveDir:                  "/home/admin/t4/migration",
		XDMSocket:                "/run/amp/eKitC/xdm/sockets/dp2mgr.sock",
		CheckpointParallelism:    2,
		RestoreParallelism:       6,
		HttpClientTimeout:        2 * time.Minute,
		PrometheusPort:           9416,
		OssMultiPartSize:         30,
		OssUploadThreadsCount:    10,
		DfdaemonEndpoint:         "/var/run/dragonfly/dfdaemon.sock",
		DragonflyPieceSize:       0,
		DragonflyReorderBufferMB: 512,

		DragonflyPieceStallTimeout: 90 * time.Second,
		DragonflySpillLimitMB:      2048,
		ServeHTTP:                  false,
		HTTPPort:                   50090,
		PvcPreheatBackend:          PvcPreheatBackendDragonfly,
	}
}

// Flags binds Options fields to CLI flags. The returned FlagSet
// is consumed by the cobra command in cmd/migration-worker.
func (o *Options) Flags() *pflag.FlagSet {
	fs := pflag.NewFlagSet("migration-worker", pflag.ContinueOnError)
	fs.IntVar(&o.GrpcPort, "grpc-port", o.GrpcPort, "Port on which the gRPC service listens")
	fs.StringVar(&o.SaveDir, "save-dir", o.SaveDir, "Root directory for staged migration data")
	fs.StringVar(&o.XDMSocket, "xdm-socket", o.XDMSocket, "Path to the X-Device-Manager gRPC unix domain socket")
	fs.IntVar(&o.CheckpointParallelism, "checkpoint-parallelism", o.CheckpointParallelism, "Number of parallel checkpoint workers")
	fs.IntVar(&o.RestoreParallelism, "restore-parallelism", o.RestoreParallelism, "Number of parallel restore workers")
	fs.DurationVar(&o.HttpClientTimeout, "http-client-timeout", o.HttpClientTimeout, "Timeout for HTTP-style transfers")
	fs.IntVar(&o.PrometheusPort, "prometheus-port", o.PrometheusPort, "Port on which Prometheus /metrics is served")
	fs.BoolVar(&o.ServeHTTP, "serve-http", o.ServeHTTP,
		"Serve the PVC mount point (/mnt/pv) as a static http file server (preheat source mode); disables gRPC/prometheus/scheduler")
	fs.IntVar(&o.HTTPPort, "http-port", o.HTTPPort, "TCP port for the http file server in serve-http mode")
	fs.StringVar(&o.PvcPreheatBackend, "pvc-preheat-backend", o.PvcPreheatBackend,
		"Preheat link preferred by the PVC restore path: dragonfly (default) or efc; any other value is rejected at startup")
	fs.Int64Var(&o.OssMultiPartSize, "oss-part-size", o.OssMultiPartSize, "Per-part size for OSS multi-part upload, unit: MB")
	fs.IntVar(&o.OssUploadThreadsCount, "oss-upload-threads", o.OssUploadThreadsCount, "Number of threads for OSS multi-part upload")
	fs.StringVar(&o.DfdaemonEndpoint, "dfdaemon-endpoint", o.DfdaemonEndpoint, "Default dfdaemon gRPC endpoint for Dragonfly2 storage")
	fs.Int32Var(&o.DragonflyPieceSize, "dragonfly-piece-size", o.DragonflyPieceSize,
		"Piece size in bytes for Dragonfly2 upload/download; 0 lets dfdaemon pick it (required for preheat cache hits)")
	fs.IntVar(&o.DragonflyReorderBufferMB, "dragonfly-reorder-buffer", o.DragonflyReorderBufferMB, "Max in-memory reorder buffer (MB) for Dragonfly streaming download; 0 = unlimited")
	fs.DurationVar(&o.DragonflyPieceStallTimeout, "dragonfly-piece-stall-timeout", o.DragonflyPieceStallTimeout,
		"How long to wait for the next expected piece before re-fetching it with a ranged request; 0 disables hedged re-fetch")
	fs.IntVar(&o.DragonflySpillLimitMB, "dragonfly-spill-limit", o.DragonflySpillLimitMB,
		"Hard cap (MB) on un-reassembled data buffered on disk; the download applies backpressure once reached. 0 = unbounded")
	return fs
}
