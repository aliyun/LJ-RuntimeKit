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

package collector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	pb "github.com/lj-runtimekit/exporter/api/rtkit/profiling"
)

const (
	statusOK             = 0
	statusNotFound       = 1004
	statusBusy           = 1006
	statusAlreadyRunning = 1007
)

// These defaults must stay compatible with JobController's collectorprotocol package.
const (
	DefaultSocketPath        = "/run/amp/eKitC/xdm/sockets/dp2mgr.sock"
	DefaultPollInterval      = 2 * time.Second
	DefaultRPCTimeout        = 15 * time.Second
	DefaultStartTimeout      = 2 * time.Minute
	DefaultStreamIdleTimeout = 5 * time.Minute
	DefaultFinishTimeout     = 5 * time.Minute
	DefaultStopTimeout       = 15 * time.Second
	DefaultMaxRetries        = 3
	DefaultRetryBackoff      = 10 * time.Second
	DefaultMaxRetryBackoff   = 2 * time.Minute
	DefaultStartBusyRetries  = 5
	DefaultStartBusyBackoff  = 15 * time.Second
)

var (
	pathComponent  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,252}$`)
	streamFileName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,254}$`)
	errStreamIdle  = errors.New("stream idle timeout")
)

type Config struct {
	SocketPath   string
	PodNamespace string
	PodName      string
	PodUID       string
	ContainerIDs []string
	Mode         pb.ProfilingMode
	Duration     time.Duration
	Options      map[string]string
	OutputDir    string
	PollInterval time.Duration
	RPCTimeout   time.Duration
	StartTimeout time.Duration
	// StreamIdleTimeout bounds the gap between stream progress, not the transfer total.
	StreamIdleTimeout time.Duration
	FinishTimeout     time.Duration
	StopTimeout       time.Duration
	MaxRetries        int
	RetryBackoff      time.Duration
	MaxRetryBackoff   time.Duration
	// StartBusyRetries 与 StartBusyBackoff 是 Start 返回 1006（daemon 无空闲 worker）时的进程内等待策略。
	// 1006 表示 daemon 未创建任何会话，重发 Start 幂等，因此既不 Stop 也不占用 MaxRetries。
	StartBusyRetries int
	StartBusyBackoff time.Duration
	// CleanupLeftovers 在 CLI 中默认开启：controller 保证同一目标串行且旧 Pod 已退出后才创建新 Pod，
	// 新 Pod 必须先清掉 daemon 里的残留会话；手工排查确认无并行采集时应显式关闭。
	CleanupLeftovers bool
}

// Validate checks that c is complete and every path component is safe.
func (c Config) Validate() error {
	var missing []string
	for _, f := range []struct{ name, value string }{
		{"socket", c.SocketPath},
		{"pod-namespace", c.PodNamespace},
		{"pod-name", c.PodName},
		{"pod-uid", c.PodUID},
		{"output-dir", c.OutputDir},
	} {
		if f.value == "" {
			missing = append(missing, f.name)
		}
	}
	if len(c.ContainerIDs) == 0 {
		missing = append(missing, "container-ids")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required flags: %s", strings.Join(missing, ", "))
	}
	if !filepath.IsAbs(c.SocketPath) {
		return fmt.Errorf("socket path must be absolute")
	}
	if !filepath.IsAbs(c.OutputDir) || filepath.Clean(c.OutputDir) == string(filepath.Separator) {
		return fmt.Errorf("output directory must be an absolute subdirectory")
	}
	for _, f := range []struct{ name, value string }{
		{"pod-namespace", c.PodNamespace}, {"pod-name", c.PodName}, {"pod-uid", c.PodUID},
	} {
		if !pathComponent.MatchString(f.value) {
			return fmt.Errorf("invalid %s: %q", f.name, f.value)
		}
	}
	seen := make(map[string]bool, len(c.ContainerIDs))
	filenameIDs := make(map[string]bool, len(c.ContainerIDs))
	for _, rawID := range c.ContainerIDs {
		id := NormalizeContainerID(rawID)
		if !pathComponent.MatchString(id) {
			return fmt.Errorf("invalid container ID: %q", rawID)
		}
		if seen[id] {
			return fmt.Errorf("duplicate container ID: %q", id)
		}
		filenameID := containerFilenameID(id)
		if filenameIDs[filenameID] {
			return fmt.Errorf("container IDs share filename prefix %q", filenameID)
		}
		seen[id], filenameIDs[filenameID] = true, true
	}
	for _, f := range []struct {
		name  string
		value time.Duration
	}{
		{"duration", c.Duration}, {"poll-interval", c.PollInterval}, {"rpc-timeout", c.RPCTimeout},
		{"start-timeout", c.StartTimeout}, {"stream-idle-timeout", c.StreamIdleTimeout},
		{"finish-timeout", c.FinishTimeout}, {"stop-timeout", c.StopTimeout},
	} {
		if f.value <= 0 {
			return fmt.Errorf("%s must be positive, got %s", f.name, f.value)
		}
	}
	if c.MaxRetries < 0 || c.MaxRetries > 10 {
		return fmt.Errorf("max-retries must be between 0 and 10")
	}
	if c.StartBusyRetries < 0 || c.StartBusyRetries > 20 {
		return fmt.Errorf("start-busy-retries must be between 0 and 20")
	}
	if c.StartBusyRetries > 0 && c.StartBusyBackoff <= 0 {
		return fmt.Errorf("start-busy-backoff must be positive when start-busy-retries is positive")
	}
	// 直接构造的零值配置不重试；CLI 仍必须提供有效退避参数。
	if c.MaxRetries != 0 || c.RetryBackoff != 0 || c.MaxRetryBackoff != 0 {
		if c.RetryBackoff <= 0 || c.MaxRetryBackoff < c.RetryBackoff {
			return fmt.Errorf("retry-backoff must be positive and max-retry-backoff must be >= retry-backoff")
		}
	}
	if c.Duration+c.FinishTimeout <= 0 {
		return fmt.Errorf("overflowing completion wait duration")
	}
	if _, ok := pb.ProfilingMode_name[int32(c.Mode)]; !ok {
		return fmt.Errorf("invalid profiling mode: %d", c.Mode)
	}
	return nil
}

type Collector struct {
	cfg        Config
	client     pb.XpuDeviceManagerServiceClient
	log        *slog.Logger
	openOutput func(string, string) (*outputFile, error)
}

func Run(ctx context.Context, cfg Config, log *slog.Logger) Result {
	return collectWithOutput(ctx, cfg, log, func() Result { return run(ctx, cfg, log) })
}

func run(ctx context.Context, cfg Config, log *slog.Logger) Result {
	socketCtx, cancel := context.WithTimeout(ctx, cfg.RPCTimeout)
	err := cfg.retry(socketCtx, log, "socket", "", func(err error) bool {
		return errors.Is(err, os.ErrNotExist) || retryableFileError(err) || errors.Is(err, syscall.ECONNREFUSED)
	}, func() error {
		info, err := os.Stat(cfg.SocketPath)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSocket == 0 {
			return fmt.Errorf("runtime kit socket path is not a unix socket")
		}
		return nil
	})
	cancel()
	if err != nil {
		if ctx.Err() != nil {
			return Failed(ReasonInterrupted, err.Error())
		}
		return Failed(ReasonSocketUnavailable, fmt.Sprintf("runtime kit socket: %v", err))
	}
	conn, err := grpc.DialContext(ctx, "unix://"+cfg.SocketPath, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry())
	if err != nil {
		return Failed(ReasonSocketUnavailable, fmt.Sprintf("dial runtime kit socket: %v", err))
	}
	defer conn.Close()

	return New(cfg, pb.NewXpuDeviceManagerServiceClient(conn), log).collect(ctx)
}

// New returns a Collector using client; cfg must pass Validate.
func New(cfg Config, client pb.XpuDeviceManagerServiceClient, log *slog.Logger) *Collector {
	if log == nil {
		log = slog.Default()
	}
	return &Collector{cfg: cfg, client: client, log: log}
}

func (c *Collector) Collect(ctx context.Context) Result {
	return collectWithOutput(ctx, c.cfg, c.log, func() Result { return c.collect(ctx) })
}

func (c *Collector) collect(ctx context.Context) Result {
	if c.cfg.CleanupLeftovers {
		// Finish all retry cleanup before starting any new session. The controller
		// must ensure the prior collector pod has exited before enabling this.
		for _, rawID := range c.cfg.ContainerIDs {
			if err := c.cleanupLeftover(ctx, NormalizeContainerID(rawID)); err != nil {
				if ctx.Err() != nil {
					return Failed(ReasonInterrupted, err.Error())
				}
				return Failed(ReasonCleanupFailed, err.Error())
			}
		}
	}
	if ctx.Err() != nil {
		return Failed(ReasonInterrupted, context.Cause(ctx).Error())
	}
	results := make([]ContainerResult, len(c.cfg.ContainerIDs))
	var wg sync.WaitGroup
	for i, id := range c.cfg.ContainerIDs {
		wg.Add(1)
		go func(i int, id string) {
			defer wg.Done()
			results[i] = c.collectOne(ctx, id)
		}(i, id)
	}
	wg.Wait()

	res := Result{Phase: PhaseSucceeded, PodNamespace: c.cfg.PodNamespace, PodName: c.cfg.PodName, PodUID: c.cfg.PodUID, Containers: results}
	var failed, stopErrors []string
	for _, r := range results {
		res.ElapsedMillis = max(res.ElapsedMillis, r.ElapsedMillis)
		if r.StopError != "" {
			stopErrors = append(stopErrors, fmt.Sprintf("%s: %s", shortID(r.ContainerID), r.StopError))
		}
		if r.Phase != PhaseFailed {
			continue
		}
		if res.Reason == "" {
			res.Reason = r.Reason
		}
		failed = append(failed, fmt.Sprintf("%s: %s", shortID(r.ContainerID), r.Message))
	}
	if len(failed) > 0 {
		res.Phase = PhaseFailed
		res.Message = fmt.Sprintf("%d/%d container(s) failed: %s", len(failed), len(results), strings.Join(failed, "; "))
	}
	res.StopError = strings.Join(stopErrors, "; ")
	return res
}

type failure struct {
	reason string
	err    error
}

func (f *failure) Error() string { return f.err.Error() }
func (f *failure) Unwrap() error { return f.err }

func fail(reason string, format string, args ...any) error {
	return &failure{reason: reason, err: fmt.Errorf(format, args...)}
}

func (c *Collector) collectOne(ctx context.Context, rawID string) (res ContainerResult) {
	id := NormalizeContainerID(rawID)
	res = ContainerResult{ContainerID: id, Phase: PhaseSucceeded}
	log := c.log.With("containerID", id)

	defer func() {
		if res.Phase == PhaseFailed {
			log.Error("collection failed", "reason", res.Reason, "message", res.Message, "stopError", res.StopError)
		} else {
			log.Info("collection succeeded", "files", res.OutputFiles, "bytes", res.Bytes, "elapsedMillis", res.ElapsedMillis)
		}
	}()

	err := c.profile(ctx, log, id, &res)
	if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		return res
	}
	res.Phase = PhaseFailed
	res.Message = err.Error()
	var f *failure
	switch {
	case ctx.Err() != nil:
		res.Reason = ReasonInterrupted
		res.Message = fmt.Sprintf("interrupted (%v): %v", context.Cause(ctx), err)
	case errors.As(err, &f):
		res.Reason = f.reason
	default:
		res.Reason = ReasonProfilingFailed
	}
	return res
}

func (c *Collector) profile(ctx context.Context, log *slog.Logger, id string, res *ContainerResult) (err error) {
	log.Info("starting profiling", "mode", c.cfg.Mode, "duration", c.cfg.Duration)
	request := &pb.StartProfilingRequest{
		Id: id, Mode: c.cfg.Mode.Enum(), Duration: durationpb.New(c.cfg.Duration), Options: c.cfg.Options,
	}
	var startResp *pb.StartProfilingResponse
	var startErr error
	replaced, busy := false, 0
	// 1007 只允许“Stop 成功后按原参数重发一次”；1006 表示 daemon 仍无空闲 worker 且未创建会话，
	// 等待后重发即可，绝不触发 Stop。两者互不复用次数预算。
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		rpcCtx, cancel := context.WithTimeout(ctx, c.cfg.RPCTimeout)
		startResp, startErr = c.client.StartProfiling(rpcCtx, request, grpc.WaitForReady(true))
		cancel()
		if startErr == nil && startResp == nil {
			startErr = fmt.Errorf("StartProfiling returned nil response")
		}
		if startErr != nil {
			break
		}
		status := startResp.GetStatus()
		if status == statusBusy && busy < c.cfg.StartBusyRetries {
			busy++
			log.Warn("runtime kit has no idle worker; waiting before retrying start", "retry", busy, "startBusyRetries", c.cfg.StartBusyRetries, "backoff", c.cfg.StartBusyBackoff, "status", status, "message", startResp.GetMessages())
			if err := sleepCtx(ctx, c.cfg.StartBusyBackoff); err != nil {
				return err
			}
			continue
		}
		if status == statusAlreadyRunning && !replaced {
			replaced = true
			log.Warn("container already has a profiling task; stopping it before restarting", "status", status, "message", startResp.GetMessages())
			if err := c.stop(ctx, id); err != nil {
				res.StopError = err.Error()
				return fail(ReasonStopFailed, "stop existing profiling task before restart: %w", err)
			}
			continue
		}
		break
	}
	if startErr == nil && startResp.GetStatus() == statusBusy {
		return fail(ReasonStartBusy, "StartProfiling returned status %d after %d retries: %s", statusBusy, busy, startResp.GetMessages())
	}
	if startErr == nil && startResp.GetStatus() != statusOK {
		return fail(ReasonStartFailed, "StartProfiling returned status %d: %s", startResp.GetStatus(), startResp.GetMessages())
	}
	var stopOnce sync.Once
	var stopErr error
	cleanup := func() { stopOnce.Do(func() { stopErr = c.stop(ctx, id) }) }
	// A canceled Job must stop the daemon even if a PVC write is blocked.
	stopWatcher := context.AfterFunc(ctx, cleanup)
	defer func() {
		stopWatcher()
		cleanup()
		if stopErr != nil {
			res.StopError = stopErr.Error()
			if err == nil {
				err = fail(ReasonStopFailed, "%w", stopErr)
			}
		}
	}()
	if startErr != nil {
		return fail(ReasonStartFailed, "StartProfiling: %w", startErr)
	}

	if err := c.waitForStatus(ctx, id, c.cfg.StartTimeout, ReasonStartTimeout, pb.ProfilingStatus_PROFILING_STATUS_RUNNING, res); err != nil {
		return err
	}
	log.Info("waiting for profiling completion")
	if err := c.waitForStatus(ctx, id, c.cfg.Duration+c.cfg.FinishTimeout, ReasonFinishTimeout, pb.ProfilingStatus_PROFILING_STATUS_COMPLETED, res); err != nil {
		return err
	}
	log.Info("profiling completed, streaming data")

	outputs, err := c.stream(ctx, id, res)
	if err != nil {
		return err
	}
	defer func() {
		for _, output := range outputs {
			output.dir.Close()
		}
	}()

	if err := ctx.Err(); err != nil {
		return err
	}
	// Only a stopped session may publish final files; a failed Stop keeps .partial.
	cleanup()
	if stopErr != nil {
		return fail(ReasonStopFailed, "%w", stopErr)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	commitCtx, commitCancel := context.WithTimeout(ctx, c.cfg.RPCTimeout)
	defer commitCancel()
	for i, output := range outputs {
		if err := c.cfg.retry(commitCtx, c.log, "Rename", id, retryableFileError, output.finalize); err != nil {
			return fail(ReasonOutputFailed, "finalize output file: %w", err)
		}
		res.OutputFiles[i] = output.relative
	}
	return nil
}

func (c *Collector) waitForStatus(ctx context.Context, id string, timeout time.Duration, timeoutReason string, want pb.ProfilingStatus, res *ContainerResult) error {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(c.cfg.PollInterval)
	defer ticker.Stop()

	var lastStatus pb.ProfilingStatus
	for {
		resp, err := c.query(waitCtx, id)
		if err == nil && resp.GetStatus() == statusOK {
			if elapsed := resp.GetElapsedDuration(); elapsed != nil && elapsed.CheckValid() == nil && elapsed.AsDuration() >= 0 {
				res.ElapsedMillis = elapsed.AsDuration().Milliseconds()
			}
		}
		if waitCtx.Err() != nil {
			return fail(timeoutReason, "waiting for %s: %w (last status: %s)", want, waitCtx.Err(), lastStatus)
		}
		if err != nil {
			return fail(ReasonQueryFailed, "QueryProfilingStatus: %w", err)
		}
		if resp.GetStatus() != statusOK {
			return fail(ReasonQueryFailed, "QueryProfilingStatus returned status %d: %s", resp.GetStatus(), resp.GetMessages())
		}
		lastStatus = resp.GetProfilingStatus()
		// Short sessions can complete between polls without RUNNING being observed.
		if lastStatus == want || (want == pb.ProfilingStatus_PROFILING_STATUS_RUNNING && lastStatus == pb.ProfilingStatus_PROFILING_STATUS_COMPLETED) {
			return nil
		}
		switch lastStatus {
		case pb.ProfilingStatus_PROFILING_STATUS_FAILED:
			return fail(ReasonProfilingFailed, "runtime kit reported FAILED: %s", resp.GetMessages())
		case pb.ProfilingStatus_PROFILING_STATUS_CANCELED:
			return fail(ReasonProfilingCanceled, "runtime kit reported CANCELED: %s", resp.GetMessages())
		case pb.ProfilingStatus_PROFILING_STATUS_PENDING, pb.ProfilingStatus_PROFILING_STATUS_RUNNING:
		default:
			return fail(ReasonUnexpectedStatus, "expected %s, got %s", want, lastStatus)
		}
		select {
		case <-waitCtx.Done():
			return fail(timeoutReason, "waiting for %s: %w (last status: %s)", want, waitCtx.Err(), lastStatus)
		case <-ticker.C:
		}
	}
}

// All files stay partial until the complete stream and StopProfiling succeed.
// Profile size is unpredictable, so the stream has no total deadline; it fails
// only when StreamIdleTimeout passes without a received message or finished write.
func (c *Collector) stream(parent context.Context, id string, res *ContainerResult) (_ []*outputFile, err error) {
	// gRPC binds the stream deadline at creation, so idleness cancels instead.
	ctx, cancelStream := context.WithCancelCause(parent)
	defer cancelStream(nil)
	idle := time.AfterFunc(c.cfg.StreamIdleTimeout, func() { cancelStream(errStreamIdle) })
	defer idle.Stop()
	progress := func() { idle.Reset(c.cfg.StreamIdleTimeout) }
	defer func() {
		if err == nil || !errors.Is(context.Cause(ctx), errStreamIdle) {
			return
		}
		reason := ReasonStreamFailed
		var f *failure
		if errors.As(err, &f) {
			reason = f.reason
		}
		err = fail(reason, "no stream progress for %s after %d bytes: %w", c.cfg.StreamIdleTimeout, res.Bytes, err)
	}()

	var outputs []*outputFile
	var current *outputFile
	var currentName string
	seen := map[string]bool{}
	keepDirs := false
	defer func() {
		if current != nil {
			current.file.Close()
		}
		if !keepDirs {
			for _, output := range outputs {
				output.dir.Close()
			}
		}
	}()

	s, err := c.client.StreamProfilingData(ctx, &pb.StreamProfilingDataRequest{Id: id}, grpc.WaitForReady(true))
	if err != nil {
		return nil, fail(ReasonStreamFailed, "StreamProfilingData: %w", err)
	}
	for {
		msg, err := s.Recv()
		if errors.Is(err, io.EOF) {
			if current != nil {
				return nil, fail(ReasonStreamFailed, "stream ended before file_end for %q", currentName)
			}
			if len(outputs) == 0 {
				return nil, fail(ReasonStreamFailed, "stream returned no files")
			}
			keepDirs = true
			return outputs, nil
		}
		if err != nil {
			return nil, fail(ReasonStreamFailed, "receive profiling data after %d bytes: %w", res.Bytes, err)
		}
		progress()
		if msg.GetId() != id {
			return nil, fail(ReasonStreamFailed, "stream container ID mismatch: expected %q, got %q", id, msg.GetId())
		}
		name := msg.GetFileName()
		if !streamFileName.MatchString(name) {
			return nil, fail(ReasonStreamFailed, "invalid stream file name %q", name)
		}
		if msg.GetFileEnd() && len(msg.GetRawData()) != 0 {
			return nil, fail(ReasonStreamFailed, "file_end for %q contains data", name)
		}
		if current != nil && name != currentName {
			return nil, fail(ReasonStreamFailed, "file changed from %q to %q before file_end", currentName, name)
		}
		if current == nil {
			outputName := OutputFileName(c.cfg.PodNamespace, c.cfg.PodName, id, name)
			if seen[outputName] {
				return nil, fail(ReasonStreamFailed, "duplicate stream file name %q", name)
			}
			// A final name must not overwrite another file's .partial.
			if seen[outputName+partialSuffix] || (strings.HasSuffix(outputName, partialSuffix) && seen[strings.TrimSuffix(outputName, partialSuffix)]) {
				return nil, fail(ReasonStreamFailed, "stream file name %q conflicts with a partial file", name)
			}
			prepareCtx, cancel := context.WithTimeout(ctx, c.cfg.RPCTimeout)
			if c.openOutput == nil {
				current, err = c.createOutputWithRetry(prepareCtx, id, name)
			} else {
				err = c.cfg.retry(prepareCtx, c.log, "CreateOutput", id, retryableFileError, func() error {
					var err error
					current, err = c.openOutput(id, name)
					return err
				})
			}
			cancel()
			if err != nil {
				return nil, fail(ReasonOutputFailed, "create output file: %w", err)
			}
			currentName = name
			outputs = append(outputs, current)
			res.OutputFiles = append(res.OutputFiles, current.relative)
			actualName := strings.TrimSuffix(current.name, partialSuffix)
			if actualName != outputName && (seen[actualName] || seen[actualName+partialSuffix] || (strings.HasSuffix(actualName, partialSuffix) && seen[strings.TrimSuffix(actualName, partialSuffix)])) {
				return nil, fail(ReasonStreamFailed, "stream file name %q conflicts with an output file", name)
			}
			seen[outputName], seen[actualName] = true, true
		}
		if err := c.writeOutput(ctx, id, current.file, msg.GetRawData(), res); err != nil {
			return nil, fail(ReasonOutputFailed, "write output file: %w", err)
		}
		// Slow storage that still completes writes is progress, not idleness.
		progress()
		if msg.GetFileEnd() {
			if err := c.cfg.retry(ctx, c.log, "Sync", id, retryableFileError, current.file.Sync); err != nil {
				return nil, fail(ReasonOutputFailed, "sync output file: %w", err)
			}
			progress()
			if err := ctx.Err(); err != nil {
				return nil, fail(ReasonOutputFailed, "close output file: %w", err)
			}
			err := current.file.Close()
			current = nil
			if err != nil {
				return nil, fail(ReasonOutputFailed, "close output file: %w", err)
			}
		}
	}
}

func (c *Collector) cleanupLeftover(ctx context.Context, id string) error {
	resp, err := c.query(ctx, id)
	if err != nil {
		return fmt.Errorf("cleanup %s: QueryProfilingStatus: %w", id, err)
	}
	if resp.GetStatus() == statusNotFound {
		return nil
	}
	if resp == nil || resp.GetStatus() != statusOK {
		return fmt.Errorf("cleanup %s: QueryProfilingStatus returned status %d: %s", id, resp.GetStatus(), resp.GetMessages())
	}
	switch resp.GetProfilingStatus() {
	case pb.ProfilingStatus_PROFILING_STATUS_PENDING, pb.ProfilingStatus_PROFILING_STATUS_RUNNING:
		if err := c.stop(ctx, id); err != nil {
			return fmt.Errorf("cleanup %s: %w", id, err)
		}
	case pb.ProfilingStatus_PROFILING_STATUS_COMPLETED, pb.ProfilingStatus_PROFILING_STATUS_FAILED, pb.ProfilingStatus_PROFILING_STATUS_CANCELED:
	default:
		return fmt.Errorf("cleanup %s: unexpected status %s", id, resp.GetProfilingStatus())
	}
	return nil
}

func (c *Collector) stop(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.cfg.StopTimeout)
	defer cancel()
	err := c.cfg.retry(ctx, c.log, "StopProfiling", id, retryableRPCError, func() error {
		resp, err := c.client.StopProfiling(ctx, &pb.StopProfilingRequest{Id: id}, grpc.WaitForReady(true))
		if err != nil {
			return err
		}
		if resp == nil || resp.GetStatus() != statusOK {
			return fmt.Errorf("StopProfiling returned invalid response or status %d: %s", resp.GetStatus(), resp.GetMessages())
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("StopProfiling: %w", err)
	}
	return nil
}

func (c *Collector) query(ctx context.Context, id string) (*pb.QueryProfilingStatusResponse, error) {
	rpcCtx, cancel := context.WithTimeout(ctx, c.cfg.RPCTimeout)
	defer cancel()
	var resp *pb.QueryProfilingStatusResponse
	err := c.cfg.retry(rpcCtx, c.log, "QueryProfilingStatus", id, retryableRPCError, func() error {
		var err error
		resp, err = c.client.QueryProfilingStatus(rpcCtx, &pb.QueryProfilingStatusRequest{Id: id}, grpc.WaitForReady(true))
		if err == nil && resp == nil {
			return fmt.Errorf("QueryProfilingStatus returned nil response")
		}
		return err
	})
	return resp, err
}

// 每个调用独立计数，整个操作共享原 deadline，不能按尝试重置预算。
func (cfg Config) retry(ctx context.Context, log *slog.Logger, operation, id string, retryable func(error) bool, call func() error) error {
	backoff := cfg.RetryBackoff
	for retries := 0; ; retries++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := call()
		if err == nil || retries >= cfg.MaxRetries || !retryable(err) {
			return err
		}
		if ctx.Err() != nil {
			return errors.Join(err, ctx.Err())
		}
		if log == nil {
			log = slog.Default()
		}
		log.Warn("操作重试", "operation", operation, "containerID", id, "retry", retries+1, "backoff", backoff, "err", err)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(err, ctx.Err())
		case <-timer.C:
		}
		if backoff > cfg.MaxRetryBackoff/2 {
			backoff = cfg.MaxRetryBackoff
		} else {
			backoff *= 2
		}
	}
}

// sleepCtx 等待 d，Job 取消时立即返回，不把等待拖到超时。
func sleepCtx(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func retryableRPCError(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
		return true
	default:
		return false
	}
}

func retryableFileError(err error) bool {
	for _, code := range []syscall.Errno{syscall.EINTR, syscall.EAGAIN, syscall.ETIMEDOUT, syscall.EBUSY, syscall.EIO, syscall.ENOSPC, syscall.EDQUOT} {
		if errors.Is(err, code) {
			return true
		}
	}
	return false
}

func (c *Collector) writeOutput(ctx context.Context, id string, file io.Writer, data []byte, res *ContainerResult) error {
	return c.cfg.retry(ctx, c.log, "Write", id, retryableFileError, func() error {
		for len(data) > 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
			n, err := file.Write(data)
			if n < 0 || n > len(data) {
				return fmt.Errorf("invalid Write count %d", n)
			}
			res.Bytes += int64(n)
			data = data[n:]
			if err != nil {
				return err
			}
			if n == 0 {
				return io.ErrShortWrite
			}
		}
		return nil
	})
}

// NormalizeContainerID strips a runtime scheme such as "containerd://".
func NormalizeContainerID(id string) string {
	if i := strings.Index(id, "://"); i >= 0 {
		return id[i+3:]
	}
	return id
}

func shortID(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}
