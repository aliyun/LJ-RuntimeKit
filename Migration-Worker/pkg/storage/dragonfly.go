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
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/container-migration/worker/cmd/migration-worker/options"
	migrationworker "github.com/container-migration/worker/pkg/apis"
	oss_sdk "github.com/container-migration/worker/pkg/storage/oss-sdk"
	migutil "github.com/container-migration/worker/pkg/util"
	migarchive "github.com/container-migration/worker/pkg/util/archive"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

// dfOutputPollInterval is how often Restore polls for dfdaemon's output file
// to show up before it can open and tail it.
const dfOutputPollInterval = 50 * time.Millisecond

// Dragonfly implements StorageInterface using a hybrid approach:
//   - Upload: direct OSS SDK upload (same as Oss plugin). Unavailable in
//     http-source mode (PVC preheat), which is download-only — see OssClient.
//   - Restore/Download: P2P download via dfdaemon gRPC API
type Dragonfly struct {
	// dfdaemon gRPC endpoint
	Endpoint string
	// OSS configuration (for upload and for dfdaemon download source)。
	// OssClient is nil in http-source mode (PVC preheat): that mode only downloads,
	// never uploads. nil does not mean "never called" - the controller-side
	// convertJobStorageToGrpcStorage is shared by checkpoint and restore, and
	// storageType=dragonfly can carry a PreheatJob; if that is a PVC source, it
	// enters the checkpoint flow with a nil OssClient. So every dereference site
	// must have its own nil guard (see Upload / GetUploadInfo).
	BucketName      string
	BasePath        string
	OssEndpoint     string
	AccessKeyId     string
	AccessKeySecret string
	OssClient       *oss_sdk.Client
	// dfdaemon gRPC client
	DfClient DfDaemonClient
	// Dragonfly2 settings
	PieceSize int32
	// Rate limiting and metrics
	UploadReader   *migutil.CountingReader
	DownloadReader *migutil.CountingReader
	StartTime      time.Time
	// PreheatJob mode: when the CR names a PreheatJob, the download source object is
	// uniquely determined by that CR's spec.remoteURL - no longer BasePath + tarFile.
	// It must exactly match the URL used at preheat time, otherwise the task_id
	// computed by dfdaemon mismatches and misses the P2P cache.
	usePreheatJob        bool
	preheatJobObjectName string
	// httpSourceURL PVC preheat source mode: the dir URL is a plain http address (the
	// ClusterIP of the long-running preheat Service, derived independently by the
	// preheat and restore sides from (pvcNamespace, pvcName, pvcPath)); shard URLs
	// are appended to it directly, no OSS signing.
	httpSourceURL string
	// httpSourceParts is the ascending shard list in http-source mode: discovered
	// from the local dir (DiscoverLocalParts) when the same PVC is mounted; when a
	// storageType=dragonfly job carries a PVC-source PreheatJob CR, still inferred
	// from the length of status.taskID. Empty means not http-source.
	httpSourceParts []int
	// fallbackPartDir is the per-part direct-read fallback dir after P2P retries are
	// exhausted (PVC combined mode passes /mnt/pv). Empty means no local dir
	// fallback (pure dragonfly/OSS mode has no PVC mounted).
	fallbackPartDir string
	// fallbackCounter counts fallback direct-read bytes, sharing the transfer metric
	// with P2P downloads (set by pvc_preheat.go in PVC combined mode; pure
	// dragonfly mode reads DownloadReader directly).
	fallbackCounter *migutil.CountingReader
	// fallbackHTTPClient is the per-part fallback direct-read client for pure
	// dragonfly/OSS mode: after P2P retries are exhausted it re-signs the URL and
	// streams directly from OSS, symmetric with the dir read of PVC combined mode.
	// No http.Client.Timeout (a fixed timeout covers the whole body read and would
	// cut slow reads into truncated tar streams mid-way); hang protection is handled
	// by openFallbackPart's progress-aware timeout.
	fallbackHTTPClient *http.Client
}

func NewDragonfly(opt *options.Options, storageInfo *migrationworker.Storage) (StorageInterface, error) {
	if storageInfo == nil {
		return nil, fmt.Errorf("storageInfo is nil")
	}
	if storageInfo.Dragonfly == nil {
		return nil, fmt.Errorf("Dragonfly storage info is nil")
	}
	dfConfig := storageInfo.Dragonfly

	// Determine dfdaemon endpoint: per-request config overrides CLI default
	endpoint := dfConfig.Endpoint
	if endpoint == "" {
		endpoint = opt.DfdaemonEndpoint
	}

	// Prefer the PreheatJob: when the CR gives name/namespace, the preheat task
	// itself carries a verified remoteURL and credentials, so the worker uses them
	// directly and skips the oss_mount_point / endpoint / options config. On resolve
	// failure (not ready / CR not found) degrade to the legacy path - a preheat
	// anomaly must not stall the migration.
	var (
		bucketName      string
		basePath        string
		ossEndpoint     string
		accessKeyId     string
		accessKeySecret string
		usePreheatJob   bool
		preheatObjName  string
		httpSourceURL   string
		httpSourceParts []int
	)
	if dfConfig.PreheatJobName != "" && dfConfig.PreheatJobNamespace != "" {
		resolved, objName, err := resolveFromPreheatJob(dfConfig)
		if err != nil {
			klog.Warningf("failed to resolve Dragonfly storage from PreheatJob %s/%s, "+
				"falling back to oss_mount_point: %v",
				dfConfig.PreheatJobNamespace, dfConfig.PreheatJobName, err)
		} else {
			bucketName = resolved.bucketName
			basePath = resolved.basePath
			ossEndpoint = resolved.ossEndpoint
			accessKeyId = resolved.accessKeyId
			accessKeySecret = resolved.accessKeySecret
			usePreheatJob = true
			preheatObjName = objName
			httpSourceURL = resolved.httpSourceURL
			httpSourceParts = resolved.parts
		}
	}

	// Parse OSS config from Dragonfly config (for upload)
	if !usePreheatJob {
		if dfConfig.OssMountPoint == "" {
			return nil, fmt.Errorf("Dragonfly oss_mount_point is empty")
		}
		var err error
		bucketName, basePath, err = extractOSSBucketAndObjectName(dfConfig.OssMountPoint)
		if err != nil {
			return nil, fmt.Errorf("failed to parse oss_mount_point: %w", err)
		}

		if dfConfig.OssEndpoint == "" {
			return nil, fmt.Errorf("Dragonfly oss_endpoint is empty")
		}
		if dfConfig.Options == nil {
			return nil, fmt.Errorf("Dragonfly options are empty")
		}
		var ok bool
		accessKeyId, ok = dfConfig.Options[OSS_ACCESS_KEY_ID]
		if !ok {
			return nil, fmt.Errorf("Dragonfly options missing %s", OSS_ACCESS_KEY_ID)
		}
		accessKeySecret, ok = dfConfig.Options[OSS_ACCESS_KEY_SECRET]
		if !ok {
			return nil, fmt.Errorf("Dragonfly options missing %s", OSS_ACCESS_KEY_SECRET)
		}
		ossEndpoint = dfConfig.OssEndpoint
	}

	// Create OSS client for upload. http-source mode (PVC preheat) only downloads,
	// so the OSS config is a placeholder and the client is nil. nil is safe only if
	// callers never touch upload: Upload and GetUploadInfo each hold their own nil
	// guard, otherwise the checkpoint flow would nil-deref inside the progress
	// goroutine - a detached goroutine that no recover can catch, taking down the
	// whole worker process.
	var wrappedOSSClient *oss_sdk.Client
	if httpSourceURL == "" {
		chunkSize := opt.OssMultiPartSize * 1024 * 1024
		parallelism := opt.OssUploadThreadsCount
		provider := credentials.NewStaticCredentialsProvider(accessKeyId, accessKeySecret)
		cfg := oss.LoadDefaultConfig().
			WithSignatureVersion(oss.SignatureVersionV1).
			WithCredentialsProvider(provider).
			WithEndpoint(ossEndpoint)
		ossClient := oss.NewClient(cfg)
		wrappedOSSClient = oss_sdk.NewClient(ossClient, chunkSize, parallelism, 3)
	}

	// Create dfdaemon gRPC client for P2P download
	dfClient, err := newDfDaemonClient(endpoint, opt)
	if err != nil {
		return nil, err
	}

	// The per-part fallback direct-read client for pure dragonfly/OSS mode: no
	// http.Client.Timeout - that timeout covers the whole body read (in Go the timer
	// keeps running after Do returns and would cut Body reads), so a 6GB shard read
	// on a degraded network would be cut mid-way into a truncated tar stream; hang
	// protection is handled by openFallbackPart's progress-aware timeout instead
	// (cancel only after continuous zero growth, slow reads never killed).
	// However, the phase from connection establishment to response headers is not
	// covered by the progress monitor (watchFallbackProgress starts only after Do
	// returns): a gateway that accepts but never sends headers would hang forever,
	// so the Transport limits header waiting only - ResponseHeaderTimeout semantics
	// conveniently exclude body reads.
	// PVC combined mode falls back to dir reads (/mnt/pv) and does not need this
	// client.
	var fallbackHTTPClient *http.Client
	if httpSourceURL == "" {
		tr := http.DefaultTransport.(*http.Transport).Clone()
		tr.ResponseHeaderTimeout = 3 * time.Minute
		fallbackHTTPClient = &http.Client{Transport: tr}
	}

	return &Dragonfly{
		Endpoint:             endpoint,
		BucketName:           bucketName,
		BasePath:             basePath,
		OssEndpoint:          ossEndpoint,
		AccessKeyId:          accessKeyId,
		AccessKeySecret:      accessKeySecret,
		OssClient:            wrappedOSSClient,
		DfClient:             dfClient,
		PieceSize:            opt.DragonflyPieceSize,
		UploadReader:         migutil.NewCountingReader(),
		DownloadReader:       migutil.NewCountingReader(),
		usePreheatJob:        usePreheatJob,
		preheatJobObjectName: preheatObjName,
		httpSourceURL:        httpSourceURL,
		httpSourceParts:      httpSourceParts,
		fallbackHTTPClient:   fallbackHTTPClient,
	}, nil
}

// newDfDaemonClient creates the dfdaemon gRPC client, shared by NewDragonfly and
// newDragonflyWithHTTPSource.
func newDfDaemonClient(endpoint string, opt *options.Options) (DfDaemonClient, error) {
	dfClient, err := NewGrpcDfDaemonClient(endpoint,
		int64(opt.DragonflyReorderBufferMB)*1024*1024,
		opt.DragonflyPieceStallTimeout,
		int64(opt.DragonflySpillLimitMB)*1024*1024)
	if err != nil {
		return nil, fmt.Errorf("failed to create dfdaemon client: %w", err)
	}
	return dfClient, nil
}

// newDragonflyWithHTTPSource constructs the Dragonfly for http-source mode (PVC
// combined): shard URLs = sourceURL (the dir URL derived from the long-running
// preheat Service ClusterIP) + shard name; shard indices are discovered locally
// by the caller (from the dir of the mounted PVC). This mode only downloads
// (OssClient=nil, Upload/GetUploadInfo hold nil guards); per-part fallback
// direct reads are handled by fallbackPartDir, no fallbackHTTPClient needed.
func newDragonflyWithHTTPSource(opt *options.Options, sourceURL string, indices []int) (*Dragonfly, error) {
	dfClient, err := newDfDaemonClient(opt.DfdaemonEndpoint, opt)
	if err != nil {
		return nil, err
	}
	return &Dragonfly{
		Endpoint:        opt.DfdaemonEndpoint,
		DfClient:        dfClient,
		PieceSize:       opt.DragonflyPieceSize,
		UploadReader:    migutil.NewCountingReader(),
		DownloadReader:  migutil.NewCountingReader(),
		httpSourceURL:   sourceURL,
		httpSourceParts: indices,
	}, nil
}

func (d *Dragonfly) Name() string {
	return "dragonfly"
}

// errHTTPSourceNoUpload is the error for uploads in http-source mode (PVC
// preheat). The message must point out the root cause - storageType=dragonfly
// carrying a PVC-source PreheatJob: checkpoint writes to OSS, but the http
// source only serves downloads, the two semantics are mutually exclusive.
// "client is nil" alone would leave users no way to self-diagnose.
var errHTTPSourceNoUpload = errors.New(
	"dragonfly storage resolved a http-source PreheatJob (PVC preheat), which is download-only " +
		"and cannot upload checkpoints; use an OSS-source PreheatJob, or storageType=pvc " +
		"(preheat is then composed with a PVC fallback) for the checkpoint stage")

// Upload pushes checkpoint data directly to OSS via the OSS SDK.
// This is identical to the Oss plugin's upload flow.
//
// In http-source mode OssClient is nil and upload is unsupported, so error out
// instead of dereferencing nil. The guard can only live at the method level:
// NewDragonfly has no direction info, and the same storage config is reused by
// restore (restore only calls Restore/GetDownloadInfo, for which the http source
// is legitimate), so rejecting at construction would kill restore-only.
func (d *Dragonfly) Upload(reader io.Reader, dstTarFile string) (int64, error) {
	if d.OssClient == nil {
		return 0, errHTTPSourceNoUpload
	}
	d.UploadReader.SetReader(reader)
	realObjectName, err := filepath.Rel("/", filepath.Join(d.BasePath, dstTarFile))
	if err != nil {
		return 0, err
	}

	return d.OssClient.UploadWithReader(d.UploadReader, d.BucketName, realObjectName)
}

// Restore downloads every checkpoint part through the dfdaemon P2P network and
// extracts them into the destination directory in parallel.
//
// checkpointDir is a directory (no longer a single tar): it holds part-00.tar,
// part-01.tar, ... Each part is a complete tar carrying the full directory
// skeleton, so all N goroutines can extract into the same target with no
// ordering dependency between them. Download dominates the restore latency, so
// the speedup is roughly totalBytes/largestPartBytes.
//
// Per part the flow is unchanged:
//  1. Build a signed OSS URL with HMAC-SHA1 authentication
//  2. DownloadToFile lets dfdaemon write the tar straight to a temp file, so
//     piece content never crosses the gRPC stream into this process
//  3. A growingFileReader tails that temp file while it is still being written,
//     so tar extraction overlaps the download instead of waiting for it
//
// The atomic rename into the final destination happens once, after every part
// has landed — see restoreIntoTemp.
//
// ⚠ PieceSize must stay 0. task_id is sha256 over URL/tag/application/revision
// **and piece_length**; preheat does not pass it and lets dfdaemon derive it from
// the object length. Setting it explicitly here would make every part miss the
// preheated cache and silently fall back to origin download — slower, no error.
func (d *Dragonfly) Restore(ctx context.Context, checkpointDir string, dstDir string) (*RestoreInfo, error) {
	d.StartTime = time.Now()

	// http preheat source (PVC preheat): the shard dir is not OSS and the worker
	// cannot probe the remote side; shard indices come from local dir discovery (PVC
	// combined mode with the same PVC mounted) or from the length of status.taskID
	// (storageType=dragonfly carrying a PVC-source CR).
	var indices []int
	if d.httpSourceURL != "" && d.httpSourceParts != nil {
		indices = d.httpSourceParts
		klog.V(2).Infof("dragonfly restore: %d preheat part(s) (http source)", len(indices))
	} else {
		var err error
		indices, err = ProbeRemoteParts(ctx, func(idx int) string {
			return d.buildSignedOSSURL(d.partObjectName(checkpointDir, idx))
		})
		if err != nil {
			return nil, fmt.Errorf("discover checkpoint parts under %s: %w", checkpointDir, err)
		}
	}
	klog.V(4).Infof("dragonfly restore: %d checkpoint part(s) under %s", len(indices), checkpointDir)

	if err := restoreIntoTemp(dstDir, "", nil, func(tmpDir string) error {
		return unpackPartsConcurrently(ctx, indices, tmpDir, d.openPart(dstDir, checkpointDir),
			&migarchive.TarOptions{Compression: migarchive.Uncompressed})
	}); err != nil {
		return nil, err
	}

	fileCount, err := restoreInfoOf(dstDir)
	if err != nil {
		return nil, err
	}

	return &RestoreInfo{
		FileCount: fileCount,
		FileSize:  d.DownloadReader.Size(),
	}, nil
}

// partObjectName resolves the OSS object name of one part.
//
// In PreheatJob mode the CR's remoteURL points at the checkpoint *directory*, so
// preheatJobObjectName is that directory and the part name is appended to it.
// The object path must match the one the controller preheated byte for byte,
// otherwise dfdaemon computes a different task_id and the P2P cache is missed.
func (d *Dragonfly) partObjectName(checkpointDir string, idx int) string {
	base := filepath.Join(d.BasePath, checkpointDir)
	if d.usePreheatJob && d.preheatJobObjectName != "" {
		base = d.preheatJobObjectName
	}
	name, err := filepath.Rel("/", filepath.Join("/", base, PartName(idx)))
	if err != nil {
		return filepath.Join(base, PartName(idx))
	}
	return name
}

// maxDownloadRetries bounds how many times a failed P2P download is retried
// before falling back (or failing). Only transient errors are retried, see
// isTransientDownloadError.
const maxDownloadRetries = 3

// isTransientDownloadError reports whether a dfdaemon download error is worth
// retrying: connection-level failures (socket not ready, connection reset,
// deadline exceeded) are transient; deterministic failures (not found,
// permission denied, backend source errors) are not.
func isTransientDownloadError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch st.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted:
		return true
	default:
		return false
	}
}

// downloadWithRetry runs DownloadToFile with up to maxDownloadRetries retries
// and exponential backoff (1s, 2s, 4s). Deterministic errors are returned
// immediately; transient errors are retried until the budget runs out.
func (d *Dragonfly) downloadWithRetry(ctx context.Context, ossURL, tmpTar string) error {
	var lastErr error
	for attempt := 0; attempt <= maxDownloadRetries; attempt++ {
		if attempt > 0 {
			if !isTransientDownloadError(lastErr) {
				return lastErr
			}
			backoff := time.Duration(1<<uint(attempt-1)) * time.Second
			klog.Warningf("[dragonfly] retrying P2P download of %s after transient error (attempt %d/%d, backoff %v): %v",
				ossURL, attempt, maxDownloadRetries, backoff, lastErr)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
			// The output file of the last failed attempt may hold partial data; remove it
			// before retrying. A growingFileReader tailing the file notices the inode swap
			// and switches to the new file (same URL, same content, seamless continuation).
			_ = os.Remove(tmpTar)
		}
		if err := d.DfClient.DownloadToFile(ctx, ossURL, tmpTar, nil, uint64(d.PieceSize)); err == nil {
			return nil
		} else {
			lastErr = err
		}
	}
	return lastErr
}

// openFallbackPart opens the part directly from the fallback source when a P2P
// download has failed: the mounted PVC directory in combined mode, or a freshly
// signed OSS URL in pure dragonfly mode. It returns an error when no fallback
// is configured.
func (d *Dragonfly) openFallbackPart(ctx context.Context, checkpointDir string, idx int) (io.Reader, func(), error) {
	// PVC combined mode: read the same shard directly from the mounted dir.
	if d.fallbackPartDir != "" {
		srcDir := filepath.Join(d.fallbackPartDir, checkpointDir)
		p := filepath.Join(srcDir, PartName(idx))
		f, err := os.Open(p)
		if err != nil {
			return nil, nil, fmt.Errorf("open checkpoint part %s: %w", p, err)
		}
		var r io.Reader = f
		if d.fallbackCounter != nil {
			r = d.fallbackCounter.Wrap(f)
		}
		return r, func() { f.Close() }, nil
	}
	// Pure dragonfly/OSS mode: re-sign and read OSS directly, so one shard's P2P
	// failure does not drag the whole restore into cold-start fallback (far slower
	// than a single-part direct read).
	if d.fallbackHTTPClient == nil {
		return nil, nil, fmt.Errorf("no part-level fallback configured")
	}
	url := d.buildSignedOSSURL(d.partObjectName(checkpointDir, idx))

	// Hang protection: progress-aware timeout. The request and body read share
	// fallbackCtx - the monitor goroutine (watchFallbackProgress) cancels it after
	// fallbackStallChecks consecutive zero-growth checks; on a silent network hang
	// (headers arrived, body stalled; neither TCP nor HTTP times out on its own) the
	// read returns an error immediately; any observed growth resets the timer, so
	// slow reads are never killed. No http.Client.Timeout: it covers the whole body
	// read and would hard-cut slow reads into truncated tar streams.
	fallbackCtx, cancelFallback := context.WithCancel(ctx)
	req, err := http.NewRequestWithContext(fallbackCtx, http.MethodGet, url, nil)
	if err != nil {
		cancelFallback()
		return nil, nil, fmt.Errorf("build fallback request for %s: %w", url, err)
	}
	resp, err := d.fallbackHTTPClient.Do(req)
	if err != nil {
		cancelFallback()
		return nil, nil, fmt.Errorf("fallback GET %s: %w", url, err)
	}
	if resp.StatusCode != http.StatusOK {
		cancelFallback()
		resp.Body.Close()
		return nil, nil, fmt.Errorf("fallback GET %s: unexpected status %d", url, resp.StatusCode)
	}

	// Per-part independent counter for monitoring: a shared fallbackCounter /
	// DownloadReader would let other shards' progress mask this shard's hang; bytes
	// are also chained into the shared metric, so the metric stays unchanged.
	prog := migutil.NewCountingReader()
	var r io.Reader = prog.Wrap(resp.Body)
	if d.fallbackCounter != nil {
		r = d.fallbackCounter.Wrap(r)
	} else {
		r = d.DownloadReader.Wrap(r)
	}
	watchStop := make(chan struct{})
	go watchFallbackProgress(prog, cancelFallback, watchStop)
	return r, func() {
		close(watchStop)
		cancelFallback()
		resp.Body.Close()
	}, nil
}

// Parameters for the fallback direct-read progress-aware timeout. var instead of const so unit tests can shrink the time window.
var (
	// fallbackProgressCheckInterval is the progress check granularity: decoupled
	// from the tolerance window, hang detection time = interval x fallbackStallChecks
	// (default 1 minute x 10 = 10 minutes).
	fallbackProgressCheckInterval = time.Minute
	// fallbackStallChecks is the max number of consecutive zero-growth checks before declaring a hang.
	fallbackStallChecks = 10
)

// watchFallbackProgress monitors fallback direct-read progress, implementing the
// progress-aware timeout: every fallbackProgressCheckInterval it reads the
// counter's cumulative bytes, and after fallbackStallChecks consecutive
// zero-growth checks it cancels the request context. On a silent network hang
// (headers arrived, body stalled) neither HTTP nor TCP times out on its own;
// without cancellation restore would hang forever. Any observed growth resets
// the count, so slow reads (e.g. a 6GB shard taking tens of minutes on a
// degraded network) are never killed. After cancel this goroutine exits; stop is
// only closed by the caller's cleanup.
func watchFallbackProgress(counter *migutil.CountingReader, cancel context.CancelFunc, stop <-chan struct{}) {
	last := counter.Size()
	stalls := 0
	ticker := time.NewTicker(fallbackProgressCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			cur := counter.Size()
			if cur != last {
				last = cur
				stalls = 0
				continue
			}
			stalls++
			if stalls >= fallbackStallChecks {
				cancel()
				return
			}
		}
	}
}

// openPart hands back a reader that tails the part's tar while dfdaemon is still
// writing it, so extraction overlaps the download.
//
// Whether that overlap actually happens depends on the filesystem layout:
// when the dragonfly cache dir and the output dir are on the same device,
// dfdaemon hard-links instead of copying, the temp tar is complete instantly and
// the behaviour degenerates to "download, then extract" (which is in fact faster
// overall — it skips a full data copy). Check with:
//
//	stat -c '%d %n' /var/lib/dragonfly <output dir>
func (d *Dragonfly) openPart(dstDir, checkpointDir string) partOpener {
	return func(ctx context.Context, idx int) (io.Reader, func(), error) {
		ossURL := d.buildSignedOSSURL(d.partObjectName(checkpointDir, idx))

		// Per-part temp tar: the parts are downloaded concurrently, so they must
		// not share one output path.
		tmpTar := fmt.Sprintf("%s.%s.tmp", dstDir, PartName(idx))
		_ = os.Remove(tmpTar)
		if err := os.MkdirAll(filepath.Dir(tmpTar), 0755); err != nil {
			return nil, nil, fmt.Errorf("create temp tar parent directory %s: %w", filepath.Dir(tmpTar), err)
		}

		dlCtx, cancelDownload := context.WithCancel(ctx)
		downloadDone := make(chan struct{})
		var downloadErr error
		go func() {
			defer close(downloadDone)
			downloadErr = d.downloadWithRetry(dlCtx, ossURL, tmpTar)
		}()

		cleanup := func() {
			cancelDownload()
			<-downloadDone
			_ = os.Remove(tmpTar)
		}

		// Wait for dfdaemon to create the output file — there is nothing to tail
		// until it exists.
		//
		// Reaching the downloadDone branch does not mean failure: in hardlink mode
		// (cache dir and output dir on the same device), dfdaemon creates the output
		// file once at the very end of the download; the file appears almost
		// simultaneously with the download goroutine returning, so the poll loop has no
		// chance to see it at the loop head. This branch must stat again, otherwise a
		// successful fast download (especially a P2P cache hit) would be misjudged as
		// "no output file".
	waitOutput:
		for {
			if _, statErr := os.Stat(tmpTar); statErr == nil {
				break
			}
			select {
			case <-downloadDone:
				if downloadErr != nil {
					// Per-part fallback: this shard's P2P download failed after
					// retries were exhausted; read the same shard directly from
					// the fallback source (PVC combined mode reads the mounted
					// dir, pure OSS mode re-signs and reads OSS), while the
					// remaining shards keep going P2P - so one failure does not
					// drag the whole restore back to slow direct read or cold
					// start.
					r, fcleanup, ferr := d.openFallbackPart(ctx, checkpointDir, idx)
					if ferr == nil {
						klog.Warningf("[dragonfly] P2P download of %s failed after retries (%v), reading it directly from fallback source",
							PartName(idx), downloadErr)
						return r, func() { fcleanup(); cleanup() }, nil
					}
					// Fallback also failed: ferr must reach the log and the
					// returned error, otherwise production only shows the P2P
					// failure with no hint why fallback missed (PVC dir missing,
					// expired OSS signature, network down, no fallback configured
					// etc.), leaving no clue to diagnose. downloadErr keeps the %w
					// main chain (to avoid changing upstream error classification),
					// ferr is appended with %v.
					klog.Warningf("[dragonfly] P2P download of %s failed after retries (%v), and part-level fallback failed too: %v",
						PartName(idx), downloadErr, ferr)
					cleanup()
					return nil, nil, fmt.Errorf("dragonfly P2P download of %s failed: %w (part-level fallback also failed: %v)",
						PartName(idx), downloadErr, ferr)
				}
				if _, statErr := os.Stat(tmpTar); statErr != nil {
					cleanup()
					return nil, nil, fmt.Errorf("dfdaemon finished but produced no output file %s: %w", tmpTar, statErr)
				}
				// The file appears only at the download tail; by now the data is
				// complete, hand it to the reader below to read to EOF in one go.
				break waitOutput
			case <-dlCtx.Done():
				cleanup()
				return nil, nil, dlCtx.Err()
			case <-time.After(dfOutputPollInterval):
			}
		}

		f, err := os.Open(tmpTar)
		if err != nil {
			cleanup()
			return nil, nil, fmt.Errorf("open temp tar %s failed: %w", tmpTar, err)
		}

		reader := &growingFileReader{path: tmpTar, f: f, done: downloadDone, ctx: dlCtx}
		// The counter wraps growingFileReader: it counts bytes actually consumed by the
		// extract, not the idle wait for dfdaemon. growingFileReader swaps inodes when a
		// download retry recreates the output file, so cleanup must use reader.Close()
		// to close the current FD, not the one opened here (which may have been replaced
		// by refresh).
		return d.DownloadReader.Wrap(reader), func() {
			reader.Close()
			cleanup()
		}, nil
	}
}

// buildSignedOSSURL constructs a pre-signed OSS URL with embedded
// authentication credentials, see presignOSSGetURL for the details of the
// stable-expiry / task_id relationship. PVC preheat sources (httpSourceURL, the
// dir URL derived from the long-running preheat Service ClusterIP) need no
// signing: append the shard name directly.
func (d *Dragonfly) buildSignedOSSURL(objectName string) string {
	if d.httpSourceURL != "" {
		// httpSourceURL already contains the checkpoint dir path (derived from the
		// long-running preheat Service ClusterIP); objectName is the full path built by
		// partObjectName, so take the file name.
		return d.httpSourceURL + "/" + filepath.Base(objectName)
	}
	return presignOSSGetURL(d.BucketName, d.OssEndpoint, objectName, d.AccessKeyId, d.AccessKeySecret)
}

func (d *Dragonfly) CleanUpload(path string) error {
	return nil
}

// GetUploadInfo is a pure progress/metric query. In http-source mode OssClient
// is nil; the only field depending on it, ReadDuration (OSS upload read
// duration), is meaningless then, so leave it zero and do NOT return an error:
// the caller checkpointItem.go's progress goroutine polls every second, and
// between it and Upload sits partStream.Next() (the first part may block for
// minutes) - an error would flood the same warning for minutes. The real config
// error fails fast at Upload on the first part and is written into the CR
// status.
func (d *Dragonfly) GetUploadInfo() (*UploadInfo, error) {
	var readDuration time.Duration
	if d.OssClient != nil {
		readDuration = d.OssClient.GetPouchReadInfo()
	}
	return &UploadInfo{
		UploadedSize: d.UploadReader.Size(),
		ChunkSize:    int64(d.PieceSize),
		Parallelism:  1, // dfdaemon manages download parallelism internally
		ReadDuration: readDuration,
	}, nil
}

func (d *Dragonfly) GetDownloadInfo() (*RestoreInfo, error) {
	return &RestoreInfo{
		FileSize: d.DownloadReader.Size(),
	}, nil
}

func init() {
	RegisterStoragePlugin("dragonfly", NewDragonfly)
}
