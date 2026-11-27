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
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	commonv2 "d7y.io/api/v2/pkg/apis/common/v2"
	dfdaemonv2 "d7y.io/api/v2/pkg/apis/dfdaemon/v2"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/klog/v2"
)

// DfDaemonClient abstracts the dfdaemon gRPC service interaction.
// This isolates the Dragonfly2 SDK dependency and enables testing
// with a mock implementation.
type DfDaemonClient interface {
	// DownloadToFile downloads a task from P2P network to a local file.
	// It uses dfdaemon's OutputPath feature to let dfdaemon write the
	// downloaded content directly to disk, avoiding in-process buffering.
	// The call blocks until the download is complete or the context is cancelled.
	DownloadToFile(ctx context.Context, url string, outputPath string, headers map[string]string, pieceLength uint64) error

	// DownloadStream downloads a task from P2P network and returns a reader
	// that streams the content as it arrives. Uses io.Pipe internally to
	// bridge the gRPC stream's piece content into an io.Reader, so no
	// intermediate file or FIFO is needed. The caller MUST read the returned
	// ReadCloser to EOF (or Close it) to avoid blocking the download
	// goroutine. Close() returns the download goroutine's error.
	DownloadStream(ctx context.Context, url string, headers map[string]string, pieceLength uint64) (io.ReadCloser, error)

	// Close releases the underlying gRPC connection.
	Close() error
}

// defaultDragonflyTag / defaultDragonflyApplication are deliberately empty
// strings, not nil: dragonfly-client v1.3.0's seed-client passes Some("") at
// preheat, and both sides must take the same value, otherwise the computed
// task_id differs and the download misses the preheated cache.
var (
	defaultDragonflyTag         = ""
	defaultDragonflyApplication = ""
)

// defaultFilteredQueryParams matches the OSS signing params filtered at seed-client preheat.
//
// These three params change on every re-signing; without filtering, a new
// task_id would be computed each time. But note some dfdaemon versions take the
// filtered URL straight to origin fetch, causing OSS to return 403.
// buildSignedOSSURL already uses a fixed expiry so the URL of the same object is
// constant within its validity period, still hitting the cache even if this
// item does not apply; if origin-fetch 403 shows up while investigating preheat
// hit rate, suspect this first.
var defaultFilteredQueryParams = []string{
	"OSSAccessKeyId",
	"Expires",
	"Signature",
}

// grpcDfDaemonClient implements DfDaemonClient via the dfdaemon v2 gRPC API.
type grpcDfDaemonClient struct {
	conn   *grpc.ClientConn
	client dfdaemonv2.DfdaemonDownloadClient
	// reorderBufferBytes caps the in-memory reorder buffer in DownloadStream.
	// 0 means unlimited (legacy behavior).
	reorderBufferBytes int64
	// pieceStallTimeout triggers a ranged re-fetch when the next expected
	// piece has not arrived for this long. 0 disables hedged re-fetch.
	pieceStallTimeout time.Duration
	// spillLimitBytes is a soft cap on the on-disk reorder spill file;
	// exceeding it forces a gap re-fetch. 0 means unlimited.
	spillLimitBytes int64
}

// maxGapRefetchAttempts bounds how many times a single missing piece is
// re-fetched before the download is declared failed.
const maxGapRefetchAttempts = 3

// dfdaemonSocketReadyTimeout / dfdaemonSocketProbeInterval control the worker's
// startup readiness wait on the dfdaemon unix socket: right after a client pod
// is recreated the socket is not yet bound, and downloading immediately would
// fail with "no such file or directory", dragging the whole restore into
// direct-read fallback.
const (
	dfdaemonSocketReadyTimeout  = 60 * time.Second
	dfdaemonSocketProbeInterval = time.Second
)

// NewGrpcDfDaemonClient creates a new dfdaemon gRPC client connected to the
// given endpoint. The endpoint can be:
//   - A Unix socket path: "/var/run/dragonfly/dfdaemon.sock" or "unix:///var/run/dragonfly/dfdaemon.sock"
//   - A TCP address: "127.0.0.1:65000"
//
// The connection is established lazily (no blocking dial).
func NewGrpcDfDaemonClient(endpoint string, reorderBufferBytes int64,
	pieceStallTimeout time.Duration, spillLimitBytes int64) (*grpcDfDaemonClient, error) {
	// Detect Unix socket: path starts with "/" or "unix://"
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// Piece data (up to 4MB) + protobuf overhead exceeds the default
		// 4MB gRPC message limit. 128MB leaves room for the largest piece
		// sizes dfdaemon may negotiate for multi-GB checkpoint tars.
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(128 * 1024 * 1024)),
	}

	if strings.HasPrefix(endpoint, "unix://") {
		socketPath := strings.TrimPrefix(endpoint, "unix://")
		dialOpts = append(dialOpts, grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
		}))
		endpoint = "unix:" + socketPath
		waitForDfdaemonSocket(socketPath)
	} else if strings.HasPrefix(endpoint, "/") {
		socketPath := endpoint
		dialOpts = append(dialOpts, grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
		}))
		endpoint = "unix:" + socketPath
		waitForDfdaemonSocket(socketPath)
	}

	conn, err := grpc.Dial(endpoint, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to dial dfdaemon at %s: %w", endpoint, err)
	}
	return &grpcDfDaemonClient{
		conn:               conn,
		client:             dfdaemonv2.NewDfdaemonDownloadClient(conn),
		reorderBufferBytes: reorderBufferBytes,
		pieceStallTimeout:  pieceStallTimeout,
		spillLimitBytes:    spillLimitBytes,
	}, nil
}

func (c *grpcDfDaemonClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// waitForDfdaemonSocket blocks until the unix socket accepts connections or
// the readiness timeout expires. It never fails: the gRPC connection stays
// lazy, so a still-missing socket surfaces later as the usual dial error
// (which the per-part download retry then absorbs).
func waitForDfdaemonSocket(socketPath string) {
	deadline := time.Now().Add(dfdaemonSocketReadyTimeout)
	for {
		conn, err := net.DialTimeout("unix", socketPath, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		if time.Now().After(deadline) {
			klog.Warningf("[dragonfly] dfdaemon socket %s not ready after %v: %v",
				socketPath, dfdaemonSocketReadyTimeout, err)
			return
		}
		klog.V(2).Infof("[dragonfly] dfdaemon socket %s not ready yet, waiting: %v", socketPath, err)
		time.Sleep(dfdaemonSocketProbeInterval)
	}
}

// DownloadToFile downloads a task from the P2P network using dfdaemon's
// OutputPath feature. In the v2.1.x API, DownloadTask is a server-streaming
// RPC: it returns a stream of DownloadTaskResponse messages. The stream
// must be consumed to completion (until io.EOF) for the download to finish.
func (c *grpcDfDaemonClient) DownloadToFile(ctx context.Context, url string, outputPath string, headers map[string]string, pieceLength uint64) error {
	download := &commonv2.Download{
		Url:                 url,
		OutputPath:          &outputPath,
		Tag:                 &defaultDragonflyTag,
		Application:         &defaultDragonflyApplication,
		FilteredQueryParams: defaultFilteredQueryParams,
	}

	if len(headers) > 0 {
		download.RequestHeader = headers
	}
	if pieceLength > 0 {
		download.PieceLength = &pieceLength
	}

	req := &dfdaemonv2.DownloadTaskRequest{
		Download: download,
	}

	// DownloadTask returns a server stream. Consume all responses
	// until EOF to ensure the download completes.
	stream, err := c.client.DownloadTask(ctx, req)
	if err != nil {
		return fmt.Errorf("dfdaemon DownloadTask failed: %w", err)
	}
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("dfdaemon DownloadTask stream error: %w", err)
		}
	}

	// Verify the output file exists and is non-empty
	info, err := os.Stat(outputPath)
	if err != nil {
		return fmt.Errorf("downloaded file not found at %s: %w", outputPath, err)
	}
	if info.Size() == 0 {
		return fmt.Errorf("downloaded file at %s is empty", outputPath)
	}

	return nil
}

// DownloadStream downloads a task from the P2P network and returns an
// io.ReadCloser that streams the content as pieces arrive from dfdaemon.
//
// This method sets NeedPieceContent=true so dfdaemon returns piece data
// directly in the gRPC stream (DownloadPieceFinishedResponse → Piece.Content).
// The piece content is written into an io.Pipe, and the read end is returned
// to the caller.
//
// This approach is pure Go — no filesystem dependencies (no named pipes, no
// temp files), no risk of FIFO open blocking.
//
//   - Writer goroutine: consumes the gRPC stream, writes piece content to pipe
//   - Caller (reader): reads from the returned ReadCloser → pipe read end
func (c *grpcDfDaemonClient) DownloadStream(ctx context.Context, url string, headers map[string]string, pieceLength uint64) (io.ReadCloser, error) {
	pr, pw := io.Pipe()

	download := &commonv2.Download{
		Url:                 url,
		NeedPieceContent:    true, // request piece data in stream responses
		Tag:                 &defaultDragonflyTag,
		Application:         &defaultDragonflyApplication,
		FilteredQueryParams: defaultFilteredQueryParams,
	}
	if len(headers) > 0 {
		download.RequestHeader = headers
	}
	if pieceLength > 0 {
		download.PieceLength = &pieceLength
	}

	req := &dfdaemonv2.DownloadTaskRequest{Download: download}

	// The stream runs under a derived context so the reassembler can shut the
	// consumer down the moment reassembly is over. Without this, a failed
	// download would keep pulling the remaining (possibly tens of GB) data,
	// and the consumer would go on writing into the spill file that the
	// reassembler has already closed.
	streamCtx, cancelStream := context.WithCancel(ctx)

	// Start the gRPC stream. If this fails, the error is immediate
	// and we can return without spawning any goroutines.
	stream, err := c.client.DownloadTask(streamCtx, req)
	if err != nil {
		cancelStream()
		return nil, fmt.Errorf("dfdaemon DownloadTask failed: %w", err)
	}

	// P2P downloads return pieces out of order. We need to reassemble them
	// before writing to the pipe so the tar reader sees sequential data.
	//
	// Responsibilities are split so that stream.Recv() is never blocked:
	//   - consumer goroutine: Recv() a piece, park it either in the bounded
	//     memory budget or on the spill file, record its metadata in
	//     reorderState, then immediately go back to Recv().
	//   - reassembler goroutine: pop consecutive pieces out of reorderState and
	//     write them to the pipe, repairing gaps via ranged re-fetch.
	//
	// IMPORTANT: the consumer must NEVER block. Blocking stops stream.Recv(),
	// gRPC flow control pushes back on dfdaemon, and dfdaemon then silently
	// drops the piece it could not hand over ("send DownloadPieceFinished
	// Response ... failed: Timeout(..)" / "Closed(..)" in its log) while still
	// ending the stream with a normal EOF. The download then either deadlocks
	// (observed as "stuck at downloaded=NNN MiB") or fails late with "stream
	// ended with N pieces out of order". Hence:
	//   - no memory slot available -> spill the piece to disk
	//   - reassembler blocked      -> metadata still lands in reorderState,
	//     which is intentionally unbounded (~48 bytes per piece, so even a
	//     100k-piece task costs a few MB)
	maxPieces := 0
	if c.reorderBufferBytes > 0 {
		pl := pieceLength
		if pl == 0 {
			pl = 4 * 1024 * 1024
		}
		maxPieces = int(c.reorderBufferBytes / int64(pl))
		if maxPieces < 1 {
			maxPieces = 1
		}
		klog.Infof("[dragonfly] reorder buffer capped at %d pieces (~%d MiB, pieceLen=%d), stallTimeout=%v, spillHardCap=%d MiB",
			maxPieces, c.reorderBufferBytes/(1024*1024), pl, c.pieceStallTimeout, c.spillLimitBytes/(1024*1024))
	}

	st := newReorderState(maxPieces, c.spillLimitBytes)
	// Disk spill for out-of-order pieces that don't fit in the memory budget.
	if maxPieces > 0 {
		spill, err := newPieceSpill()
		if err != nil {
			cancelStream()
			return nil, fmt.Errorf("failed to create reorder spill file: %w", err)
		}
		st.spill = spill
	}

	// waker lets the reassembler re-evaluate its stall deadline even while no
	// piece arrives at all (sync.Cond offers no timed Wait).
	wakerStop := make(chan struct{})
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				st.cond.Broadcast()
			case <-wakerStop:
				return
			}
		}
	}()

	// Stream consumer goroutine: drains the gRPC stream into reorderState.
	// It must never block — see the note above.
	go func() {
		totalResponses := 0
		skippedResponses := 0
		emptyContentResponses := 0
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				klog.Infof("[dragonfly] stream EOF: total=%d responses, pieces=%d, skipped=%d, empty_content=%d",
					totalResponses, totalResponses-skippedResponses-emptyContentResponses, skippedResponses, emptyContentResponses)
				st.finish(nil)
				return
			}
			if err != nil {
				// A cancelled stream means the reassembler is already done (either
				// successfully, possibly before EOF, or with a failure it has
				// already reported). Not an error worth surfacing.
				if streamCtx.Err() != nil {
					klog.V(4).Infof("[dragonfly] stream closed by reassembler after %d responses", totalResponses)
					st.finish(nil)
					return
				}
				klog.Errorf("[dragonfly] stream Recv error after %d responses: %v", totalResponses, err)
				st.finish(err)
				return
			}
			totalResponses++

			// The task-started response carries the total content length. It
			// tells the reassembler how many pieces to expect (so it knows when
			// it is done) and bounds the offsets used by ranged re-fetches.
			if started := resp.GetDownloadTaskStartedResponse(); started != nil {
				st.setContentLength(started.ContentLength, pieceLength)
				skippedResponses++
				continue
			}

			pieceResp := resp.GetDownloadPieceFinishedResponse()
			if pieceResp == nil || pieceResp.Piece == nil {
				skippedResponses++
				continue
			}
			piece := pieceResp.Piece
			if len(piece.Content) == 0 {
				emptyContentResponses++
				klog.Warningf("[dragonfly] piece #%d (number=%d) has empty content (piece.Length=%d)",
					totalResponses, piece.Number, piece.Length)
				continue
			}
			// put() parks the piece in RAM or on disk and never blocks.
			if err := st.put(piece.Number, piece.Offset, piece.Content, piece.Digest); err != nil {
				// Writes failing because the reassembler closed the spill file
				// are expected during shutdown, not a real error.
				if streamCtx.Err() != nil {
					return
				}
				klog.Errorf("[dragonfly] failed to buffer piece %d: %v", piece.Number, err)
				st.finish(err)
				return
			}
		}
	}()

	// Reassembler goroutine: writes pieces to the pipe in ascending order.
	// A missing piece is repaired with a ranged re-fetch instead of stalling
	// forever or failing the whole 30+ minute download.
	errCh := make(chan error, 1)
	go func() {
		defer pw.Close()
		defer close(wakerStop)
		if st.spill != nil {
			defer st.spill.Close()
		}
		// Runs first (LIFO): stop the consumer before tearing anything down, and
		// release it in case it is parked on the spill cap.
		defer cancelStream()
		defer st.abortWaiters()

		var (
			expected    uint32
			totalBytes  int64
			written     int
			refetched   int
			spillReads  int
			gapAttempts int
		)

		fail := func(err error) {
			pw.CloseWithError(err)
			errCh <- err
		}

		// repairGap re-fetches the currently expected piece with a ranged
		// request. It returns false once the attempt budget is exhausted (the
		// download has then already been failed).
		//
		// The byte range is derived from what has actually been written rather
		// than from number*pieceLength: pieces are emitted in strict order, so
		// totalBytes IS the missing piece's offset. dfdaemon treats the
		// requested pieceLength as a hint only, so assuming a uniform piece
		// size could otherwise fetch a misaligned (silently wrong) range.
		repairGap := func(reason string) bool {
			if gapAttempts >= maxGapRefetchAttempts {
				fail(fmt.Errorf("piece %d unavailable after %d ranged re-fetch attempts (%s)",
					expected, gapAttempts, reason))
				return false
			}
			gapAttempts++

			start := uint64(totalBytes)
			length := pieceLength
			if length == 0 {
				length = 4 * 1024 * 1024
			}
			// If the successor is already buffered, its offset gives the exact
			// length of the hole instead of an assumed piece size.
			if succ, ok := st.offsetOf(expected + 1); ok && succ > start {
				length = succ - start
			}
			if cl := st.contentLen(); cl > 0 {
				if start >= cl {
					fail(fmt.Errorf("piece %d starts at %d which is beyond content length %d (%s)",
						expected, start, cl, reason))
					return false
				}
				if start+length > cl {
					length = cl - start
				}
			}

			klog.Warningf("[dragonfly] piece %d missing (%s), re-fetching bytes [%d,%d) via ranged request (attempt %d/%d)",
				expected, reason, start, start+length, gapAttempts, maxGapRefetchAttempts)
			content, err := c.refetchRange(ctx, url, headers, pieceLength, start, length)
			if err != nil {
				klog.Errorf("[dragonfly] ranged re-fetch of piece %d failed: %v", expected, err)
				// Keep retrying until the attempt budget runs out.
				return true
			}
			st.putRefetched(expected, start, content)
			refetched++
			klog.Infof("[dragonfly] piece %d recovered via ranged re-fetch (%d bytes)", expected, len(content))
			return true
		}

		for !st.complete(totalBytes) {
			pd, ok, reason, err := st.await(expected, c.pieceStallTimeout)
			if err != nil {
				fail(fmt.Errorf("dfdaemon stream recv error: %w", err))
				return
			}
			if !ok {
				if !repairGap(reason) {
					return
				}
				continue
			}

			content := pd.content
			if content == nil {
				if st.spill == nil {
					fail(fmt.Errorf("piece %d has no payload and no spill file is configured", expected))
					return
				}
				content, err = st.spill.read(pd.spillOff, pd.spillLen)
				if err != nil {
					fail(fmt.Errorf("reorder spill read error: %w", err))
					return
				}
				// Only now are the bytes really free, which is what lets a
				// consumer blocked on the spill cap make progress.
				st.releaseSpill(pd.spillLen)
				spillReads++
			}

			// Integrity check: a corrupted piece is treated exactly like a
			// missing one, so the same ranged re-fetch path repairs it.
			if derr := verifyPieceDigest(content, pd.digest); derr != nil {
				klog.Errorf("[dragonfly] piece %d failed integrity check: %v", expected, derr)
				if !repairGap(reasonDigestMismatch) {
					return
				}
				continue
			}

			if _, err := pw.Write(content); err != nil {
				fail(fmt.Errorf("pipe write error: %w", err))
				return
			}
			totalBytes += int64(len(content))
			written++
			expected++
			gapAttempts = 0
		}

		// A transport error reported after the last byte was written is not
		// fatal: contentLength is dfdaemon's authoritative task size, so the
		// tar stream is already complete (and when contentLength is unknown,
		// complete() only returns true with err == nil anyway). Failing here
		// would throw away a finished multi-GB download.
		if err := st.streamError(); err != nil {
			klog.Warningf("[dragonfly] stream reported %v after all %d bytes were written; treating the download as complete",
				err, totalBytes)
		}

		if written == 0 || totalBytes == 0 {
			fail(fmt.Errorf("dfdaemon returned %d pieces with %d total bytes — download is empty", written, totalBytes))
			return
		}

		if spilled, peakLive, total, waits := st.spillStats(); spilled > 0 {
			klog.Infof("[dragonfly] reorder spill: %d pieces spilled, %d read back, peak live %.2f MiB on disk, %.2f MiB written in total, %d backpressure waits",
				spilled, spillReads, float64(peakLive)/(1024*1024), float64(total)/(1024*1024), waits)
			if waits > 0 {
				klog.Infof("[dragonfly] download was throttled by tar extraction (%d waits); this keeps the disk footprint bounded", waits)
			}
		}
		if urgent := st.urgentStats(); urgent > 0 {
			klog.Infof("[dragonfly] %d piece(s) handed over directly while the reassembler was waiting on them", urgent)
		}
		if refetched > 0 {
			klog.Infof("[dragonfly] recovered %d piece(s) via ranged re-fetch", refetched)
		}
		klog.Infof("[dragonfly] reassembly done: %d pieces, %.2f MiB written", written, float64(totalBytes)/(1024*1024))

		errCh <- nil
	}()

	// Return a pipeReadCloser that wraps the pipe reader and propagates
	// the writer goroutine's error on Close.
	return &pipeReadCloser{
		Reader: pr,
		errCh:  errCh,
	}, nil
}

// Reasons a piece is considered unavailable and must be re-fetched.
const (
	reasonStallTimeout   = "stall timeout"
	reasonStreamEnded    = "stream ended with gap"
	reasonDigestMismatch = "digest mismatch"
)

// pieceData describes one buffered piece. The payload is either held in
// memory (content != nil) or spilled to the temp file (content == nil, with
// spillOff/spillLen valid).
type pieceData struct {
	number uint32
	// offset is the piece's byte offset within the task, as reported by
	// dfdaemon. It is used to size a re-fetch precisely instead of assuming
	// every piece has the same length.
	offset   uint64
	content  []byte
	spillOff int64
	spillLen int64
	digest   string
	// inBudget marks pieces that consumed one in-memory slot, so await()
	// releases exactly what put() acquired. Re-fetched pieces bypass the
	// budget and must therefore not release a slot.
	inBudget bool
}

// reorderState is the hand-off point between the stream consumer and the
// reassembler. It exists to guarantee that the consumer never blocks:
//
//   - the piece index (`pieces`) is unbounded, because each entry is only
//     metadata (~64 bytes); even a 100k-piece task costs a few MB;
//   - the payload is what is bounded: `freeSlots` pieces may stay in RAM,
//     everything else goes to `spill` on disk.
//
// A blocked reassembler (slow tar extraction) therefore can never stop
// stream.Recv(), which is exactly what made dfdaemon drop pieces before.
type reorderState struct {
	mu   sync.Mutex
	cond *sync.Cond

	pieces map[uint32]pieceData

	// consumed is the low-water mark: every piece below it has already been
	// written to the pipe. Late duplicates below this mark must be dropped,
	// otherwise they would sit in the index forever (nobody asks for them
	// again) and leak both memory and a budget slot. This happens routinely
	// once hedged re-fetch is enabled: the re-fetch wins, then the original
	// piece finally shows up.
	consumed uint32

	// freeSlots is the remaining in-memory piece budget. unbounded==true
	// disables the budget entirely (legacy behaviour, no spill file).
	freeSlots int
	unbounded bool

	spill *pieceSpill
	// spillLimit caps the *un-reassembled* bytes held on disk. Beyond it the
	// consumer waits instead of spilling more, which is what keeps the disk
	// footprint at a few hundred MB instead of the whole task.
	spillLimit int64
	// spillWritten / spillConsumed track bytes appended to and read back from
	// the spill file. Their difference is the in-flight (live) amount, which
	// is what the cap applies to; spillConsumed is only advanced after a
	// successful read, so the live figure never understates what is needed.
	spillWritten  int64
	spillConsumed int64
	spillPeakLive int64
	spillTotal    int64
	spilled       int
	spillWaits    int
	// urgentPasses counts pieces handed straight to the reassembler because it
	// was blocked on them. A high number simply means extraction is the
	// bottleneck and the buffers are staying full.
	urgentPasses int

	done bool
	err  error
	// abort is set when the reassembler exits, so a consumer parked on the
	// spill cap is released instead of waiting for a drain that will never
	// come.
	abort bool

	contentLength uint64
}

func newReorderState(maxPieces int, spillLimit int64) *reorderState {
	st := &reorderState{
		pieces:     make(map[uint32]pieceData),
		freeSlots:  maxPieces,
		unbounded:  maxPieces <= 0,
		spillLimit: spillLimit,
	}
	st.cond = sync.NewCond(&st.mu)
	return st
}

// setContentLength records the task size announced by dfdaemon. Completion is
// judged on bytes written, not on a piece count, so that a piece length
// different from the one we requested cannot truncate the stream.
func (st *reorderState) setContentLength(contentLength, pieceLength uint64) {
	if contentLength == 0 {
		return
	}
	st.mu.Lock()
	st.contentLength = contentLength
	st.cond.Broadcast()
	st.mu.Unlock()
	pl := pieceLength
	if pl == 0 {
		pl = 4 * 1024 * 1024
	}
	klog.Infof("[dragonfly] task content length %d bytes (%.2f MiB), ~%d pieces at %d bytes",
		contentLength, float64(contentLength)/(1024*1024), (contentLength+pl-1)/pl, pl)
}

// put parks a piece received from the stream.
//
// Backpressure: if the in-memory budget is exhausted AND the on-disk spill is
// at its cap, this blocks until the reassembler drains some spill. That is a
// deliberate reversal of the original "never block Recv()" rule — without it
// a fast dfdaemon (~100 MiB/s from local cache) against slow tar extraction
// (~38 MiB/s) spills the entire task to disk and starves the extraction of
// disk bandwidth.
//
// The piece the reassembler is currently waiting for (number == consumed) is
// NEVER subject to that wait — see the fast path below. Holding it back is
// what turns backpressure into a livelock: put() is serial, so a parked piece
// blocks the whole stream, and if that piece happens to be the one being
// awaited, the reassembler can only get it via the 90s stall timeout plus a
// ranged re-fetch. Every piece then costs ~91s (observed: 176 consecutive
// "stall timeout" re-fetches at ~0.05 MiB/s) while the copy already in hand
// gets discarded as "already consumed".
func (st *reorderState) put(number uint32, offset uint64, content []byte, digest string) error {
	st.mu.Lock()
	if number < st.consumed {
		// Already written out (a re-fetch beat this delivery). Dropping it is
		// mandatory: nobody would ever take it out of the index again.
		st.mu.Unlock()
		return nil
	}
	if _, dup := st.pieces[number]; dup {
		st.mu.Unlock()
		return nil
	}

	// Fast path: this is exactly the piece the reassembler is blocked on, so
	// hand it over immediately, bypassing both the memory budget and the spill
	// cap. inBudget stays false because no slot was taken.
	//
	// This costs at most one extra piece of memory (put() is serial, so only
	// one can be in flight) and it degrades gracefully: when everything is
	// full and pieces arrive in order, every piece takes this path and is
	// consumed right away, i.e. plain streaming with no buffering at all.
	if number == st.consumed {
		st.pieces[number] = pieceData{
			number: number, offset: offset, content: content, digest: digest,
		}
		st.urgentPasses++
		st.cond.Broadcast()
		st.mu.Unlock()
		return nil
	}

	// Pick a destination, waiting if neither is available. Both options are
	// re-evaluated on every wake-up: the reassembler may free a memory slot
	// (piece served from RAM) or spill capacity (piece read back from disk),
	// and parking on only one of them could deadlock.
	needed := int64(len(content))
	useMemory := false
	for !st.abort {
		if st.unbounded || st.freeSlots > 0 {
			useMemory = true
			break
		}
		if st.spillLimit <= 0 || (st.spillWritten-st.spillConsumed)+needed <= st.spillLimit {
			break
		}
		st.spillWaits++
		// Woken by await()/releaseSpill()/abortWaiters(), or by the 1s waker.
		// Safe to park here: this piece is ahead of what the reassembler needs,
		// so draining can always proceed without it.
		st.cond.Wait()
	}
	if st.abort {
		st.mu.Unlock()
		return nil
	}
	// The wait may have lasted long enough for this piece to become the one
	// being awaited (or to be re-fetched outright); re-check before spilling.
	if number < st.consumed {
		st.mu.Unlock()
		return nil
	}
	if _, dup := st.pieces[number]; dup {
		st.mu.Unlock()
		return nil
	}
	if number == st.consumed {
		st.pieces[number] = pieceData{
			number: number, offset: offset, content: content, digest: digest,
		}
		st.urgentPasses++
		st.cond.Broadcast()
		st.mu.Unlock()
		return nil
	}

	if useMemory {
		if !st.unbounded {
			st.freeSlots--
		}
		st.pieces[number] = pieceData{
			number: number, offset: offset, content: content,
			digest: digest, inBudget: !st.unbounded,
		}
		st.cond.Broadcast()
		st.mu.Unlock()
		return nil
	}

	// Whenever the spill is fully drained there is no live piece left on disk,
	// so the file can be rewound instead of growing forever. Both the rewind
	// and the append below run on the consumer goroutine, so they cannot race
	// with each other.
	rewind := st.spillWritten > 0 && st.spillWritten == st.spillConsumed
	st.mu.Unlock()

	if rewind {
		if err := st.spill.reset(); err != nil {
			return fmt.Errorf("reorder spill reset failed: %w", err)
		}
		st.mu.Lock()
		st.spillWritten = 0
		st.spillConsumed = 0
		st.mu.Unlock()
	}

	// Append outside the lock so a slow disk never delays the reassembler.
	// Only this goroutine writes the spill file.
	off, err := st.spill.write(content)
	if err != nil {
		return fmt.Errorf("reorder spill write failed: %w", err)
	}

	st.mu.Lock()
	st.spillWritten = off + needed
	st.spillTotal += needed
	if live := st.spillWritten - st.spillConsumed; live > st.spillPeakLive {
		st.spillPeakLive = live
	}
	// Re-check: the piece may have been re-fetched and written out while the
	// payload was going to disk. The spilled bytes are simply abandoned, but
	// the index must stay clean.
	if number >= st.consumed {
		if _, dup := st.pieces[number]; !dup {
			st.pieces[number] = pieceData{
				number: number, offset: offset,
				spillOff: off, spillLen: needed, digest: digest,
			}
			st.spilled++
		} else {
			// Nobody will read these bytes back, so release them right away.
			st.spillConsumed += needed
		}
	} else {
		st.spillConsumed += needed
	}
	st.cond.Broadcast()
	st.mu.Unlock()
	return nil
}

// releaseSpill accounts bytes that have been read back off the spill file,
// freeing capacity for the consumer.
func (st *reorderState) releaseSpill(n int64) {
	st.mu.Lock()
	st.spillConsumed += n
	st.cond.Broadcast()
	st.mu.Unlock()
}

// abortWaiters releases a consumer parked on the spill cap. Called by the
// reassembler on its way out.
func (st *reorderState) abortWaiters() {
	st.mu.Lock()
	st.abort = true
	st.cond.Broadcast()
	st.mu.Unlock()
}

// putRefetched stores a piece recovered by a ranged re-fetch. It is held in
// memory and deliberately bypasses the budget (inBudget stays false) because
// the reassembler consumes it on the very next iteration.
//
// Unlike put(), this ignores the `consumed` low-water mark: the caller IS the
// consumer and knows it still needs this piece. That matters for the digest
// mismatch path, where await() has already taken the piece out (raising
// consumed past it) and the repaired copy must still be accepted.
func (st *reorderState) putRefetched(number uint32, offset uint64, content []byte) {
	st.mu.Lock()
	if _, dup := st.pieces[number]; !dup {
		// No digest: dfdaemon serves a ranged request as an independent
		// task, so its piece digests describe that task's own layout.
		st.pieces[number] = pieceData{number: number, offset: offset, content: content}
	}
	st.cond.Broadcast()
	st.mu.Unlock()
}

// finish marks the stream as ended (err != nil on transport failure).
func (st *reorderState) finish(err error) {
	st.mu.Lock()
	st.done = true
	if err != nil && err != io.EOF {
		st.err = err
	}
	st.cond.Broadcast()
	st.mu.Unlock()
}

// await blocks until piece `number` is available, and removes it from the
// index. It returns ok=false when the piece must be re-fetched instead: the
// stall deadline expired, or the stream already ended without delivering it.
//
// Note there is no spill-capacity check here: capacity is enforced on the
// producer side (put), which is the only place that can actually stop the
// growth. The previous soft-cap check lived in this function and was
// effectively dead code, because pieces normally arrive in order and this
// function returns on the first branch.
func (st *reorderState) await(number uint32, stallTimeout time.Duration) (pieceData, bool, string, error) {
	started := time.Now()
	var deadline time.Time
	if stallTimeout > 0 {
		deadline = started.Add(stallTimeout)
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	for {
		if pd, ok := st.pieces[number]; ok {
			delete(st.pieces, number)
			// Release exactly what put() acquired.
			if pd.inBudget {
				st.freeSlots++
			}
			// Raise the low-water mark so late duplicates get dropped.
			if number+1 > st.consumed {
				st.consumed = number + 1
			}
			// A freed memory slot may unblock a consumer parked on the spill cap.
			st.cond.Broadcast()
			return pd, true, "", nil
		}
		if st.err != nil {
			return pieceData{}, false, "", st.err
		}
		if st.done {
			return pieceData{}, false, reasonStreamEnded, nil
		}
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			return pieceData{}, false, reasonStallTimeout, nil
		}
		// Woken either by an incoming piece or by the 1s waker.
		st.cond.Wait()
	}
}

// complete reports whether the whole task has been written out. It compares
// bytes rather than piece counts so that a piece length different from the
// requested one cannot cut the stream short.
func (st *reorderState) complete(writtenBytes int64) bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.contentLength > 0 {
		return writtenBytes >= int64(st.contentLength)
	}
	// Content length unknown: fall back to "stream ended and nothing left".
	return st.done && st.err == nil && len(st.pieces) == 0
}

// offsetOf returns the recorded offset of a buffered piece, if present.
func (st *reorderState) offsetOf(number uint32) (uint64, bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	pd, ok := st.pieces[number]
	if !ok {
		return 0, false
	}
	return pd.offset, true
}

func (st *reorderState) streamError() error {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.err
}

func (st *reorderState) contentLen() uint64 {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.contentLength
}

// spillStats reports how the disk buffer behaved: pieces spilled, peak live
// bytes on disk, total bytes ever written, and how many times the consumer
// had to wait on the cap (a high count means extraction is the bottleneck).
func (st *reorderState) spillStats() (pieces int, peakLive, total int64, waits int) {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.spilled, st.spillPeakLive, st.spillTotal, st.spillWaits
}

// urgentStats returns how many pieces skipped the buffers because the
// reassembler was already waiting for them.
func (st *reorderState) urgentStats() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.urgentPasses
}

// refetchRange downloads a byte range directly with a ranged request.
//
// This is the recovery path for pieces dfdaemon dropped while pushing them
// into the stream: it logs "send DownloadPieceFinishedResponse ... failed:
// Timeout(..)" and then ends the stream with a normal EOF, so the client is
// the only party that can notice and repair the gap.
//
// dfdaemon serves a ranged request as an independent task whose piece
// numbering restarts at 0, so every returned chunk is concatenated in
// ascending order and the total length is verified before returning.
func (c *grpcDfDaemonClient) refetchRange(ctx context.Context, url string, headers map[string]string,
	pieceLength, start, length uint64) ([]byte, error) {
	if length == 0 {
		return nil, fmt.Errorf("refusing to re-fetch an empty range at offset %d", start)
	}

	download := &commonv2.Download{
		Url:                 url,
		NeedPieceContent:    true,
		Range:               &commonv2.Range{Start: start, Length: length},
		Tag:                 &defaultDragonflyTag,
		Application:         &defaultDragonflyApplication,
		FilteredQueryParams: defaultFilteredQueryParams,
	}
	if len(headers) > 0 {
		download.RequestHeader = headers
	}
	if pieceLength > 0 {
		pl := pieceLength
		download.PieceLength = &pl
	}

	stream, err := c.client.DownloadTask(ctx, &dfdaemonv2.DownloadTaskRequest{Download: download})
	if err != nil {
		return nil, fmt.Errorf("ranged DownloadTask failed: %w", err)
	}

	chunks := make(map[uint32][]byte)
	for {
		resp, rerr := stream.Recv()
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return nil, fmt.Errorf("ranged stream error: %w", rerr)
		}
		pieceResp := resp.GetDownloadPieceFinishedResponse()
		if pieceResp == nil || pieceResp.Piece == nil || len(pieceResp.Piece.Content) == 0 {
			continue
		}
		chunks[pieceResp.Piece.Number] = pieceResp.Piece.Content
	}
	if len(chunks) == 0 {
		return nil, fmt.Errorf("ranged request for [%d,%d) returned no content", start, start+length)
	}

	nums := make([]uint32, 0, len(chunks))
	for n := range chunks {
		nums = append(nums, n)
	}
	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })
	out := make([]byte, 0, length)
	for _, n := range nums {
		out = append(out, chunks[n]...)
	}
	// A short or over-long reply would silently corrupt the tar stream, so it
	// is rejected rather than written out.
	if uint64(len(out)) != length {
		return nil, fmt.Errorf("ranged request for [%d,%d) returned %d bytes, want %d",
			start, start+length, len(out), length)
	}
	return out, nil
}

// unsupportedDigestOnce keeps the "algorithm not verifiable" notice to a
// single log line per process instead of one per piece.
var unsupportedDigestOnce sync.Once

// verifyPieceDigest validates content against dfdaemon's piece digest,
// formatted as "<algo>:<hex>".
//
// Only algorithms available in the standard library are checked. blake3 has
// no vendored implementation, and crc32's polynomial is not pinned by the
// protocol, so both are skipped rather than guessed — a wrong guess would
// trigger endless bogus re-fetches. An empty digest is also accepted, since
// the field is optional in the protobuf.
func verifyPieceDigest(content []byte, digest string) error {
	if digest == "" {
		return nil
	}
	idx := strings.IndexByte(digest, ':')
	if idx <= 0 || idx == len(digest)-1 {
		return nil
	}
	algo, want := digest[:idx], digest[idx+1:]

	var h hash.Hash
	switch algo {
	case "md5":
		h = md5.New()
	case "sha1":
		h = sha1.New()
	case "sha256":
		h = sha256.New()
	case "sha512":
		h = sha512.New()
	default:
		unsupportedDigestOnce.Do(func() {
			klog.Warningf("[dragonfly] piece digest algorithm %q cannot be verified here, integrity checks are skipped", algo)
		})
		return nil
	}
	h.Write(content)
	if got := hex.EncodeToString(h.Sum(nil)); !strings.EqualFold(got, want) {
		return fmt.Errorf("%s mismatch: got %s, want %s", algo, got, want)
	}
	return nil
}

// pieceSpill is an unlinked temp file used to buffer out-of-order pieces
// on disk when the in-memory reorder buffer is full. The file is removed
// from the filesystem immediately after creation (anonymous file), so the
// space is reclaimed automatically when the fd is closed, even on crash.
// Writes happen only from the stream consumer goroutine (sequential
// appends via WriteAt); reads happen only from the reassembler goroutine
// (ReadAt). WriteAt/ReadAt on distinct offsets are safe concurrently.
type pieceSpill struct {
	f   *os.File
	off int64
}

func newPieceSpill() (*pieceSpill, error) {
	f, err := os.CreateTemp("", "dragonfly-reorder-spill-*")
	if err != nil {
		return nil, err
	}
	// Unlink immediately: the fd keeps the data alive; disk space is
	// released as soon as the file is closed (or the process dies).
	os.Remove(f.Name())
	return &pieceSpill{f: f}, nil
}

// write appends b to the spill file and returns its offset.
func (s *pieceSpill) write(b []byte) (int64, error) {
	off := s.off
	if _, err := s.f.WriteAt(b, off); err != nil {
		return 0, err
	}
	s.off += int64(len(b))
	return off, nil
}

// read loads n bytes at offset off from the spill file.
func (s *pieceSpill) read(off, n int64) ([]byte, error) {
	buf := make([]byte, n)
	if _, err := s.f.ReadAt(buf, off); err != nil {
		return nil, err
	}
	return buf, nil
}

// reset rewinds the file to zero length, releasing its disk blocks. It is only
// safe when no spilled piece is still awaiting read-back; the caller enforces
// that (spillWritten == spillConsumed) and is the same goroutine that appends.
func (s *pieceSpill) reset() error {
	if err := s.f.Truncate(0); err != nil {
		return err
	}
	s.off = 0
	return nil
}

func (s *pieceSpill) Close() error {
	return s.f.Close()
}

// pipeReadCloser wraps an io.PipeReader and propagates the download
// goroutine's error when Close() is called.
type pipeReadCloser struct {
	io.Reader
	errCh chan error
	dlErr error
	done  bool
}

// Close closes the pipe read end and waits for the download goroutine
// to finish, returning its error (nil on success). Safe to call multiple
// times; subsequent calls return the cached error.
func (p *pipeReadCloser) Close() error {
	if !p.done {
		p.done = true
		// Close the reader first — this signals the writer goroutine
		// to stop if it's still writing.
		if closer, ok := p.Reader.(io.Closer); ok {
			closer.Close()
		}
		// Drain the error channel.
		if p.errCh != nil {
			p.dlErr = <-p.errCh
		}
	}
	return p.dlErr
}

// growingFilePollInterval is how long growingFileReader sleeps at EOF before
// checking the file for newly appended bytes again.
const growingFilePollInterval = 5 * time.Millisecond

// growingFileReader reads from a file that another goroutine is still writing.
// When it hits EOF it waits for more data to be appended, for the writer to
// finish, or for the context to be cancelled. This lets tar extraction overlap
// with dfdaemon's DownloadToFile instead of waiting for it to complete.
//
// Retrying with a recreated file can also continue reading seamlessly:
// downloadWithRetry os.Removes the output file before each retry and the next
// attempt lands on a new inode. This reader, during EOF waiting, checks whether
// the inode path points to differs from the current FD (os.SameFile), and if so
// reopens the new file and continues from the current offset. The retry
// downloads the same URL -> same task -> byte-identical content (checkpoint
// shards are immutable), so switching inodes introduces no byte gap and the tar
// extractor is none the wiser.
type growingFileReader struct {
	path   string // output file path: used to detect whether a retry replaced it with a new inode
	f      *os.File
	done   <-chan struct{}
	ctx    context.Context
	offset int64
}

// Read returns the next chunk of the file. ReadAt with an explicit offset is
// used instead of the file's own cursor so a short read at EOF does not leave
// the position ambiguous.
func (r *growingFileReader) Read(p []byte) (n int, err error) {
	for {
		n, err = r.f.ReadAt(p, r.offset)
		if n > 0 {
			r.offset += int64(n)
			return n, nil
		}
		if err != nil && err != io.EOF {
			return 0, err
		}

		// EOF: first check whether the output file was replaced by a retry (os.Remove
		// then recreate -> new inode); if so reopen the new file and return to the loop
		// head immediately - the new file may already be written past the offset.
		if r.refresh() {
			continue
		}

		// EOF: wait for the writer to produce more data, finish, or be cancelled.
		select {
		case <-r.done:
			// Writer is done. The last retry may have recreated the file right before
			// done, so after done also probe once before the final read of the current
			// file.
			if r.refresh() {
				continue
			}
			n, err = r.f.ReadAt(p, r.offset)
			if n > 0 {
				r.offset += int64(n)
				return n, nil
			}
			if err != nil {
				return 0, err
			}
			return 0, io.EOF
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		case <-time.After(growingFilePollInterval):
		}
	}
}

// refresh switches the reader to the new file when a retry recreates the output
// file: compare stat(path)'s inode with the current FD's (os.SameFile); if
// different, reopen path and close the old FD. When path does not exist yet
// (after Remove, before recreate) or the open fails, return false and wait for
// the next poll round.
//
// The new and old files are byte-identical (same URL -> same task); the
// correctness of continuation relies on this assumption. Even if the content
// really changed, the tar integrity check would error instead of silently
// producing bad data.
func (r *growingFileReader) refresh() bool {
	st, err := os.Stat(r.path)
	if err != nil {
		return false
	}
	cur, err := r.f.Stat()
	if err != nil {
		return false
	}
	if os.SameFile(cur, st) {
		return false
	}
	nf, err := os.Open(r.path)
	if err != nil {
		return false
	}
	_ = r.f.Close()
	r.f = nf
	return true
}

// Close releases the currently held FD. Callers must use it instead of closing
// the file they opened - refresh swaps FDs, so a self-held one is already
// stale.
func (r *growingFileReader) Close() error {
	return r.f.Close()
}
