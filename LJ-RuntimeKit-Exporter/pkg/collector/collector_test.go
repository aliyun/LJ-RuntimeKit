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
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	pb "github.com/lj-runtimekit/exporter/api/rtkit/profiling"
	"github.com/lj-runtimekit/exporter/pkg/safefs"
)

// fakeRuntimeKit simulates a daemon session per container: PENDING on the
// first query, RUNNING on the second, then COMPLETED before data can be streamed.
type fakeRuntimeKit struct {
	pb.UnimplementedXpuDeviceManagerServiceServer

	startStatus        map[string]int32
	startResponse      func(context.Context, *pb.StartProfilingRequest, int) (*pb.StartProfilingResponse, error)
	startAttempts      map[string]int
	failStatus         map[string]pb.ProfilingStatus
	stayPending        bool
	stayRunning        bool
	completedEarly     bool
	queryCode          codes.Code
	queryStatus        int32
	blockQuery         bool
	queryResponse      func(context.Context, int) (*pb.QueryProfilingStatusResponse, error)
	leftoverStatus     map[string]pb.ProfilingStatus
	chunks             [][]byte
	streamMessages     []*pb.ProfilingData // nil uses the default result.json stream; an empty slice sends no files.
	streamErrAt        int
	chunkDelay         time.Duration
	blockStream        bool
	blockStart         bool
	startResponseLost  bool
	blockStop          bool
	cancelOnStop       context.CancelFunc
	mismatchedStreamID bool
	stopStatus         int32
	stopResponse       func(context.Context, int) (*pb.StopProfilingResponse, error)
	streamStarted      chan struct{}
	stopCalled         chan string

	mu        sync.Mutex
	started   map[string]*pb.StartProfilingRequest
	queries   map[string]int
	completed map[string]bool
	stopped   map[string]int
	calls     map[string][]string
}

func newFake() *fakeRuntimeKit {
	return &fakeRuntimeKit{
		startStatus:    map[string]int32{},
		startAttempts:  map[string]int{},
		failStatus:     map[string]pb.ProfilingStatus{},
		leftoverStatus: map[string]pb.ProfilingStatus{},
		chunks:         [][]byte{[]byte(`{"traceEvents":[`), []byte(`{"name":"k"}`), []byte(`]}`)},
		streamErrAt:    -1,
		streamStarted:  make(chan struct{}, 8),
		stopCalled:     make(chan string, 128),
		started:        map[string]*pb.StartProfilingRequest{},
		queries:        map[string]int{},
		completed:      map[string]bool{},
		stopped:        map[string]int{},
		calls:          map[string][]string{},
	}
}

func (f *fakeRuntimeKit) StartProfiling(ctx context.Context, req *pb.StartProfilingRequest) (*pb.StartProfilingResponse, error) {
	if f.blockStart {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	f.mu.Lock()
	f.calls[req.Id] = append(f.calls[req.Id], "start")
	f.started[req.Id] = req
	f.queries[req.Id], f.completed[req.Id] = 0, false
	f.startAttempts[req.Id]++
	attempt := f.startAttempts[req.Id]
	f.mu.Unlock()
	if f.startResponse != nil {
		return f.startResponse(ctx, req, attempt)
	}
	if f.startResponseLost {
		return nil, status.Error(codes.Unavailable, "start accepted but response lost")
	}
	if s := f.startStatus[req.Id]; s != 0 {
		return &pb.StartProfilingResponse{Status: s, Messages: "no profiling env in container"}, nil
	}
	return &pb.StartProfilingResponse{}, nil
}

func (f *fakeRuntimeKit) QueryProfilingStatus(ctx context.Context, req *pb.QueryProfilingStatusRequest) (*pb.QueryProfilingStatusResponse, error) {
	if f.blockQuery {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	f.mu.Lock()
	f.calls[req.Id] = append(f.calls[req.Id], "query")
	f.queries[req.Id]++
	if f.queryResponse != nil {
		query := f.queries[req.Id]
		f.mu.Unlock()
		resp, err := f.queryResponse(ctx, query)
		if err == nil && resp.GetStatus() == 0 && resp.GetProfilingStatus() == pb.ProfilingStatus_PROFILING_STATUS_COMPLETED {
			f.mu.Lock()
			f.completed[req.Id] = true
			f.mu.Unlock()
		}
		return resp, err
	}
	defer f.mu.Unlock()
	if f.queryCode != codes.OK {
		return nil, status.Error(f.queryCode, "query unavailable")
	}
	resp := &pb.QueryProfilingStatusResponse{Status: f.queryStatus, ElapsedDuration: durationpb.New(60 * time.Second)}
	switch {
	case f.started[req.Id] == nil:
		resp.ProfilingStatus = f.leftoverStatus[req.Id]
	case f.failStatus[req.Id] != 0:
		resp.ProfilingStatus, resp.Messages = f.failStatus[req.Id], "cupti error"
	case f.completedEarly:
		resp.ProfilingStatus = pb.ProfilingStatus_PROFILING_STATUS_COMPLETED
	case f.stayPending || f.queries[req.Id] == 1:
		resp.ProfilingStatus = pb.ProfilingStatus_PROFILING_STATUS_PENDING
	case f.queries[req.Id] >= 3 && !f.stayRunning:
		resp.ProfilingStatus = pb.ProfilingStatus_PROFILING_STATUS_COMPLETED
	default:
		resp.ProfilingStatus = pb.ProfilingStatus_PROFILING_STATUS_RUNNING
	}
	if resp.Status == 0 && resp.ProfilingStatus == pb.ProfilingStatus_PROFILING_STATUS_COMPLETED {
		f.completed[req.Id] = true
	}
	return resp, nil
}

func (f *fakeRuntimeKit) StreamProfilingData(req *pb.StreamProfilingDataRequest, s pb.XpuDeviceManagerService_StreamProfilingDataServer) error {
	f.mu.Lock()
	f.calls[req.Id] = append(f.calls[req.Id], "stream")
	completed := f.completed[req.Id]
	f.mu.Unlock()
	if !completed {
		return status.Error(codes.FailedPrecondition, "stream requested before COMPLETED was observed")
	}
	f.streamStarted <- struct{}{}
	id := req.Id
	if f.mismatchedStreamID {
		id = "another-container"
	}
	messages := f.streamMessages
	if messages == nil {
		messages = make([]*pb.ProfilingData, 0, len(f.chunks)+1)
		for _, chunk := range f.chunks {
			messages = append(messages, &pb.ProfilingData{Id: id, FileName: "result.json", RawData: chunk})
		}
		messages = append(messages, &pb.ProfilingData{Id: id, FileName: "result.json", FileEnd: true})
	}
	for i, msg := range messages {
		if i == f.streamErrAt {
			return status.Error(codes.Internal, "ring buffer overflow")
		}
		if f.chunkDelay > 0 {
			select {
			case <-s.Context().Done():
				return s.Context().Err()
			case <-time.After(f.chunkDelay):
			}
		}
		if err := s.Send(msg); err != nil {
			return err
		}
	}
	if f.streamErrAt == len(messages) {
		return status.Error(codes.Internal, "ring buffer overflow")
	}
	if f.blockStream {
		<-s.Context().Done()
		return s.Context().Err()
	}
	return nil
}

func (f *fakeRuntimeKit) StopProfiling(ctx context.Context, req *pb.StopProfilingRequest) (*pb.StopProfilingResponse, error) {
	f.mu.Lock()
	f.stopped[req.Id]++
	f.calls[req.Id] = append(f.calls[req.Id], "stop")
	attempt := f.stopped[req.Id]
	f.mu.Unlock()
	f.stopCalled <- req.Id
	if f.stopResponse != nil {
		return f.stopResponse(ctx, attempt)
	}
	if f.blockStop {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if f.cancelOnStop != nil {
		f.cancelOnStop()
	}
	return &pb.StopProfilingResponse{Status: f.stopStatus, Messages: "stop response"}, nil
}

func (f *fakeRuntimeKit) stopCount(id string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stopped[id]
}

// serve starts f on a unix socket; macOS caps socket paths at 104 bytes, so
// avoid t.TempDir's long paths.
func serve(t *testing.T, f *fakeRuntimeKit, opts ...grpc.ServerOption) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "pjc")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "rk.sock")
	lis, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(opts...)
	pb.RegisterXpuDeviceManagerServiceServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return sock
}

func testConfig(t *testing.T, sock string, ids ...string) Config {
	return Config{
		SocketPath:        sock,
		PodNamespace:      "inference",
		PodName:           "vllm-inference-0",
		PodUID:            "0b5e5d2e-uid",
		ContainerIDs:      ids,
		Mode:              pb.ProfilingMode_PROFILING_MODE_FULL,
		Duration:          60 * time.Second,
		Options:           map[string]string{"k": "v"},
		OutputDir:         realTempDir(t),
		PollInterval:      10 * time.Millisecond,
		RPCTimeout:        time.Second,
		StartTimeout:      2 * time.Second,
		StartBusyRetries:  2,
		StartBusyBackoff:  time.Millisecond,
		StreamIdleTimeout: time.Minute,
		FinishTimeout:     2 * time.Second,
		StopTimeout:       time.Second,
	}
}

func realTempDir(t *testing.T) string {
	t.Helper()
	path, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func assertOnlyProfilingFiles(t *testing.T, dir string, names ...string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != len(names) {
		t.Fatalf("output directory must contain only profiling files %v: %v, %v", names, entries, err)
	}
	want := make(map[string]bool, len(names))
	for _, name := range names {
		want[name] = true
	}
	for _, entry := range entries {
		if !want[entry.Name()] || !entry.Type().IsRegular() {
			t.Fatalf("unexpected output entry %q (type %v)", entry.Name(), entry.Type())
		}
	}
}

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

func enableRetries(cfg *Config) {
	cfg.MaxRetries = 2
	cfg.RetryBackoff, cfg.MaxRetryBackoff = time.Millisecond, 2*time.Millisecond
}

func testCollector(t *testing.T, cfg Config) *Collector {
	t.Helper()
	conn, err := grpc.DialContext(context.Background(), "unix://"+cfg.SocketPath, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return New(cfg, pb.NewXpuDeviceManagerServiceClient(conn), discard)
}

func TestRunRPCPathContract(t *testing.T) {
	// Hard-code the daemon's wire contract, independent of generated method constants.
	if got := pb.XpuDeviceManagerService_ServiceDesc.ServiceName; got != "XpuDeviceManagerService" {
		t.Fatalf("service name = %q, want XpuDeviceManagerService", got)
	}
	var mu sync.Mutex
	var methods []string
	record := func(method string) {
		mu.Lock()
		defer mu.Unlock()
		methods = append(methods, method)
	}
	sock := serve(t, newFake(),
		grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			record(info.FullMethod)
			return handler(ctx, req)
		}),
		grpc.StreamInterceptor(func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			record(info.FullMethod)
			return handler(srv, stream)
		}),
	)
	cfg := testConfig(t, sock, "c1")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if res := Run(ctx, cfg, discard); res.Phase != PhaseSucceeded {
		t.Fatalf("normal Unix RPC collection failed: %+v", res)
	}
	want := []string{
		"/XpuDeviceManagerService/StartProfiling",
		"/XpuDeviceManagerService/QueryProfilingStatus",
		"/XpuDeviceManagerService/QueryProfilingStatus",
		"/XpuDeviceManagerService/QueryProfilingStatus",
		"/XpuDeviceManagerService/StreamProfilingData",
		"/XpuDeviceManagerService/StopProfiling",
	}
	mu.Lock()
	defer mu.Unlock()
	if len(methods) != len(want) {
		t.Fatalf("RPC paths = %v, want %v", methods, want)
	}
	for i, method := range methods {
		if method != want[i] {
			t.Errorf("RPC %d path = %q, want %q", i, method, want[i])
		}
	}
}

func TestQueryOperationRetries(t *testing.T) {
	for _, code := range []codes.Code{codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted} {
		t.Run(code.String(), func(t *testing.T) {
			f := newFake()
			f.queryResponse = func(_ context.Context, call int) (*pb.QueryProfilingStatusResponse, error) {
				if call == 1 || call == 3 {
					return nil, status.Error(code, "临时失败")
				}
				return &pb.QueryProfilingStatusResponse{ProfilingStatus: pb.ProfilingStatus_PROFILING_STATUS_COMPLETED}, nil
			}
			cfg := testConfig(t, serve(t, f), "c1")
			enableRetries(&cfg)
			cfg.MaxRetries = 1
			r := Run(context.Background(), cfg, discard)
			f.mu.Lock()
			calls := strings.Join(f.calls["c1"], ",")
			f.mu.Unlock()
			if r.Phase != PhaseSucceeded || calls != "start,query,query,query,query,stream,stop" {
				t.Fatalf("两个 Query 操作应独立重试且不重采: %+v calls=%s", r, calls)
			}
		})
	}
}

func TestQueryRetryExhaustionAndPermanentFailures(t *testing.T) {
	for _, tc := range []struct {
		name             string
		code             codes.Code
		business         int32
		state            pb.ProfilingStatus
		retries, queries int
		reason           string
	}{
		{"耗尽", codes.Unavailable, 0, 0, 2, 3, ReasonQueryFailed},
		{"禁用", codes.Unavailable, 0, 0, 0, 1, ReasonQueryFailed},
		{"永久传输错误", codes.PermissionDenied, 0, 0, 2, 1, ReasonQueryFailed},
		{"业务拒绝", codes.OK, 7, 0, 2, 1, ReasonQueryFailed},
		{"FAILED", codes.OK, 0, pb.ProfilingStatus_PROFILING_STATUS_FAILED, 2, 1, ReasonProfilingFailed},
		{"CANCELED", codes.OK, 0, pb.ProfilingStatus_PROFILING_STATUS_CANCELED, 2, 1, ReasonProfilingCanceled},
		{"非法状态", codes.OK, 0, 99, 2, 1, ReasonUnexpectedStatus},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake()
			f.queryCode, f.queryStatus = tc.code, tc.business
			f.failStatus["c1"] = tc.state
			cfg := testConfig(t, serve(t, f), "c1")
			enableRetries(&cfg)
			cfg.MaxRetries = tc.retries
			r := Run(context.Background(), cfg, discard)
			f.mu.Lock()
			queries := f.queries["c1"]
			f.mu.Unlock()
			if r.Reason != tc.reason || queries != tc.queries || f.stopCount("c1") != 1 {
				t.Fatalf("错误分类或次数不正确: %+v queries=%d", r, queries)
			}
		})
	}
}

func TestStopOperationRetriesWithinIndependentBudget(t *testing.T) {
	for _, tc := range []struct {
		name     string
		code     codes.Code
		business int32
		attempts int
		success  bool
	}{
		{"恢复", codes.Unavailable, 0, 2, true},
		{"DeadlineExceeded", codes.DeadlineExceeded, 0, 2, true},
		{"ResourceExhausted", codes.ResourceExhausted, 0, 2, true},
		{"耗尽", codes.Unavailable, 0, 3, false},
		{"业务拒绝", codes.OK, 9, 1, false},
		{"永久错误", codes.PermissionDenied, 0, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake()
			f.stopResponse = func(_ context.Context, call int) (*pb.StopProfilingResponse, error) {
				if tc.success && call >= 2 {
					return &pb.StopProfilingResponse{}, nil
				}
				if tc.code != codes.OK {
					return nil, status.Error(tc.code, "停止失败")
				}
				return &pb.StopProfilingResponse{Status: tc.business}, nil
			}
			cfg := testConfig(t, serve(t, f), "c1")
			enableRetries(&cfg)
			r := Run(context.Background(), cfg, discard)
			if (r.Phase == PhaseSucceeded) != tc.success || f.stopCount("c1") != tc.attempts {
				t.Fatalf("Stop 重试结果错误: %+v stops=%d", r, f.stopCount("c1"))
			}
			if !tc.success && (r.Reason != ReasonStopFailed || !strings.HasSuffix(r.Containers[0].OutputFiles[0], ".partial")) {
				t.Fatalf("Stop 未明确成功不得提交文件: %+v", r)
			}
		})
	}
	f := newFake()
	var firstDeadline time.Time
	var deadlineMu sync.Mutex
	f.stopResponse = func(ctx context.Context, _ int) (*pb.StopProfilingResponse, error) {
		deadlineMu.Lock()
		defer deadlineMu.Unlock()
		deadline, _ := ctx.Deadline()
		if firstDeadline.IsZero() {
			firstDeadline = deadline
			// gRPC 服务端由相对超时重建 deadline，允许传输舍入误差。
		} else if delta := deadline.Sub(firstDeadline); delta > 5*time.Millisecond || delta < -5*time.Millisecond {
			return nil, fmt.Errorf("Stop deadline 被重置")
		}
		return nil, status.Error(codes.Unavailable, "停止失败")
	}
	cfg := testConfig(t, serve(t, f), "c1")
	enableRetries(&cfg)
	cfg.StopTimeout, cfg.RetryBackoff, cfg.MaxRetryBackoff = 40*time.Millisecond, 30*time.Millisecond, 30*time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	err := testCollector(t, cfg).stop(ctx, "c1")
	if !errors.Is(err, context.DeadlineExceeded) || f.stopCount("c1") != 2 || time.Since(start) > 500*time.Millisecond {
		t.Fatalf("取消后 Stop 必须使用一个独立预算: %v", err)
	}
}

func TestCleanupQueryRetriesBeforeStart(t *testing.T) {
	f := newFake()
	f.queryResponse = func(_ context.Context, call int) (*pb.QueryProfilingStatusResponse, error) {
		if call == 1 {
			return nil, status.Error(codes.Unavailable, "残留 Query 暂时失败")
		}
		return &pb.QueryProfilingStatusResponse{ProfilingStatus: pb.ProfilingStatus_PROFILING_STATUS_COMPLETED}, nil
	}
	cfg := testConfig(t, serve(t, f), "c1")
	enableRetries(&cfg)
	cfg.CleanupLeftovers = true
	r := Run(context.Background(), cfg, discard)
	f.mu.Lock()
	calls := strings.Join(f.calls["c1"], ",")
	f.mu.Unlock()
	if r.Phase != PhaseSucceeded || calls != "query,query,start,query,query,query,stream,stop" {
		t.Fatalf("清理 Query 应在 Start 前完成重试: %+v calls=%s", r, calls)
	}
}

func TestRetryDoesNotExtendStartPhaseOrRPCTimeout(t *testing.T) {
	for _, phaseDeadline := range []bool{true, false} {
		f := newFake()
		f.queryCode = codes.Unavailable
		cfg := testConfig(t, serve(t, f), "c1")
		enableRetries(&cfg)
		cfg.RetryBackoff, cfg.MaxRetryBackoff = time.Second, time.Second
		want := ReasonQueryFailed
		if phaseDeadline {
			cfg.StartTimeout = 40 * time.Millisecond
			want = ReasonStartTimeout
		} else {
			cfg.RPCTimeout = 40 * time.Millisecond
		}
		start := time.Now()
		r := Run(context.Background(), cfg, discard)
		if r.Reason != want || time.Since(start) > 500*time.Millisecond {
			t.Fatalf("重试延长了已有阶段预算: %+v", r)
		}
	}
}

func TestRetryBackoffCancellationAndErrorClassification(t *testing.T) {
	cfg := Config{MaxRetries: 3, RetryBackoff: time.Hour, MaxRetryBackoff: time.Hour}
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	start := time.Now()
	err := cfg.retry(ctx, discard, "Write", "c1", retryableFileError, func() error {
		calls++
		cancel()
		return syscall.EIO
	})
	if !errors.Is(err, context.Canceled) || calls != 1 || time.Since(start) > time.Second {
		t.Fatalf("退避取消不及时: %v calls=%d", err, calls)
	}
	for _, code := range []syscall.Errno{syscall.EINTR, syscall.EAGAIN, syscall.ETIMEDOUT, syscall.EBUSY, syscall.EIO, syscall.ENOSPC, syscall.EDQUOT, syscall.EACCES, syscall.EPERM, syscall.EEXIST, syscall.EBADF, syscall.ELOOP, syscall.EINVAL} {
		t.Run(code.Error(), func(t *testing.T) {
			cfg := Config{MaxRetries: 2, RetryBackoff: time.Millisecond, MaxRetryBackoff: time.Millisecond}
			calls := 0
			err := cfg.retry(context.Background(), discard, "PrepareOutputDir", "", retryableFileError, func() error {
				calls++
				return &os.PathError{Op: "mkdir", Path: "output", Err: code}
			})
			want := 1
			if retryableFileError(code) {
				want = 3
			}
			if !errors.Is(err, code) || calls != want {
				t.Fatalf("文件错误分类错误: %v calls=%d", err, calls)
			}
		})
	}
}

func TestSocketAppearanceAndRetryBudget(t *testing.T) {
	f := newFake()
	old := serve(t, f)
	cfg := testConfig(t, old+"-late", "c1")
	enableRetries(&cfg)
	cfg.MaxRetries, cfg.RetryBackoff, cfg.MaxRetryBackoff = 10, 10*time.Millisecond, 10*time.Millisecond
	done := make(chan Result, 1)
	go func() { done <- Run(context.Background(), cfg, discard) }()
	time.Sleep(25 * time.Millisecond)
	if err := os.Rename(old, cfg.SocketPath); err != nil {
		t.Fatal(err)
	}
	if r := <-done; r.Phase != PhaseSucceeded {
		t.Fatalf("迟到 socket 未恢复: %+v", r)
	}
	for _, retries := range []int{0, 10} {
		cfg := testConfig(t, old+"-missing", "c1")
		enableRetries(&cfg)
		cfg.MaxRetries, cfg.RPCTimeout = retries, 40*time.Millisecond
		cfg.RetryBackoff, cfg.MaxRetryBackoff = time.Second, time.Second
		start := time.Now()
		r := Run(context.Background(), cfg, discard)
		if r.Reason != ReasonSocketUnavailable || time.Since(start) > 500*time.Millisecond {
			t.Fatalf("socket 重试无界: %+v", r)
		}
		if retries == 0 && time.Since(start) > 30*time.Millisecond {
			t.Fatal("0 次重试仍等待退避")
		}
	}
}

type faultyOutput struct {
	outputHandle
	writeCalls, syncCalls, closeCalls int
	writeError, syncError, closeError error
	shortWrite                        bool
}

func (f *faultyOutput) Write(data []byte) (int, error) {
	f.writeCalls++
	if f.writeCalls == 1 {
		if f.writeError != nil {
			n, err := f.outputHandle.Write(data[:min(2, len(data))])
			if err != nil {
				return n, err
			}
			return n, f.writeError
		}
		if f.shortWrite {
			return f.outputHandle.Write(data[:min(2, len(data))])
		}
	}
	return f.outputHandle.Write(data)
}
func (f *faultyOutput) Sync() error {
	f.syncCalls++
	if f.syncCalls == 1 && f.syncError != nil {
		return f.syncError
	}
	return f.outputHandle.Sync()
}
func (f *faultyOutput) Close() error {
	f.closeCalls++
	err := f.outputHandle.Close()
	if f.closeError != nil {
		return f.closeError
	}
	return err
}

func TestFileOperationRetriesPreserveBytesAndSingleSession(t *testing.T) {
	for _, operation := range []string{"Create", "Write", "ShortWrite", "Sync", "Rename", "Close", "Permission", "StreamDeadline", "CommitDeadline"} {
		t.Run(operation, func(t *testing.T) {
			f := newFake()
			f.chunks = [][]byte{[]byte("abcdef")}
			f.completedEarly = true
			cfg := testConfig(t, serve(t, f), "c1")
			enableRetries(&cfg)
			if operation == "StreamDeadline" || operation == "CommitDeadline" {
				cfg.RetryBackoff, cfg.MaxRetryBackoff = time.Second, time.Second
				cfg.StreamIdleTimeout, cfg.RPCTimeout = 40*time.Millisecond, 40*time.Millisecond
			}
			c := testCollector(t, cfg)
			var handle *faultyOutput
			creates, renames := 0, 0
			c.openOutput = func(id, fileName string) (*outputFile, error) {
				creates++
				if operation == "Create" && creates == 1 {
					return nil, syscall.ENOSPC
				}
				out, err := c.createOutput(id, fileName)
				if err != nil {
					return nil, err
				}
				handle = &faultyOutput{outputHandle: out.file, shortWrite: operation == "ShortWrite"}
				switch operation {
				case "Write", "StreamDeadline":
					handle.writeError = syscall.EIO
				case "Sync":
					handle.syncError = syscall.EINTR
				case "Close":
					handle.closeError = syscall.EIO
				case "Permission":
					handle.writeError = syscall.EACCES
				}
				out.file = handle
				out.rename = func(dir *os.File, from, to string) error {
					renames++
					if (operation == "Rename" || operation == "CommitDeadline") && renames == 1 {
						return syscall.EBUSY
					}
					return safefs.RenameAt(dir, from, to)
				}
				return out, nil
			}
			start := time.Now()
			r := c.Collect(context.Background())
			wantSuccess := operation != "Close" && operation != "Permission" && operation != "StreamDeadline" && operation != "CommitDeadline"
			if (r.Phase == PhaseSucceeded) != wantSuccess || handle.closeCalls != 1 || f.stopCount("c1") != 1 {
				t.Fatalf("操作重试或关闭次数错误: %+v handle=%+v", r, handle)
			}
			data, err := os.ReadFile(filepath.Join(cfg.OutputDir, r.Containers[0].OutputFiles[0]))
			want := "abcdef"
			if operation == "Permission" || operation == "StreamDeadline" {
				want = "ab"
			}
			if err != nil || string(data) != want || r.Containers[0].Bytes != int64(len(want)) {
				t.Fatalf("部分写入重复或字节数错误: %q %+v err=%v", data, r, err)
			}
			if !wantSuccess && (r.Reason != ReasonOutputFailed || !strings.HasSuffix(r.Containers[0].OutputFiles[0], ".partial")) {
				t.Fatalf("失败应保留 partial: %+v", r)
			}
			if (operation == "StreamDeadline" || operation == "CommitDeadline") && time.Since(start) > 500*time.Millisecond {
				t.Fatal("文件重试延长了阶段预算")
			}
			if operation == "Create" && creates != 2 || operation == "Rename" && renames != 2 {
				t.Fatalf("操作未恢复: creates=%d renames=%d", creates, renames)
			}
			f.mu.Lock()
			calls := strings.Join(f.calls["c1"], ",")
			f.mu.Unlock()
			if calls != "start,query,query,stream,stop" {
				t.Fatalf("重试不能重采会话: %s", calls)
			}
		})
	}
}

type nilResponseClient struct {
	pb.XpuDeviceManagerServiceClient
	queries, stops int
}

func (c *nilResponseClient) QueryProfilingStatus(context.Context, *pb.QueryProfilingStatusRequest, ...grpc.CallOption) (*pb.QueryProfilingStatusResponse, error) {
	c.queries++
	return nil, nil
}
func (c *nilResponseClient) StopProfiling(context.Context, *pb.StopProfilingRequest, ...grpc.CallOption) (*pb.StopProfilingResponse, error) {
	c.stops++
	return nil, nil
}

func TestNilRPCResponsesAreNotRetried(t *testing.T) {
	cfg := testConfig(t, "/missing/rk.sock", "c1")
	enableRetries(&cfg)
	client := &nilResponseClient{}
	c := New(cfg, client, discard)
	_, queryErr := c.query(context.Background(), "c1")
	stopErr := c.stop(context.Background(), "c1")
	if queryErr == nil || stopErr == nil || client.queries != 1 || client.stops != 1 {
		t.Fatalf("非法响应不能视为成功或重试: query=%v stop=%v client=%+v", queryErr, stopErr, client)
	}
}

func TestCancellationDuringStopRetryUsesSingleCleanup(t *testing.T) {
	f := newFake()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.stopResponse = func(_ context.Context, call int) (*pb.StopProfilingResponse, error) {
		if call == 1 {
			cancel()
			return nil, status.Error(codes.Unavailable, "临时停止失败")
		}
		return &pb.StopProfilingResponse{}, nil
	}
	cfg := testConfig(t, serve(t, f), "c1")
	enableRetries(&cfg)
	r := Run(ctx, cfg, discard)
	if r.Reason != ReasonInterrupted || f.stopCount("c1") != 2 || r.StopError != "" || !strings.HasSuffix(r.Containers[0].OutputFiles[0], ".partial") {
		t.Fatalf("AfterFunc 与正常清理不能重复整组 Stop: %+v stops=%d", r, f.stopCount("c1"))
	}
}

type writeFunc func([]byte) (int, error)

func (f writeFunc) Write(data []byte) (int, error) { return f(data) }

func TestWriteZeroProgressAndExhaustion(t *testing.T) {
	cfg := testConfig(t, "/missing/rk.sock", "c1")
	enableRetries(&cfg)
	c := New(cfg, nil, discard)
	for _, transient := range []bool{true, false} {
		calls := 0
		var res ContainerResult
		err := c.writeOutput(context.Background(), "c1", writeFunc(func([]byte) (int, error) {
			calls++
			if transient {
				return 0, syscall.EIO
			}
			return 0, nil
		}), []byte("data"), &res)
		want := 1
		if transient {
			want = 3
		}
		if err == nil || calls != want || res.Bytes != 0 {
			t.Fatalf("零进展/耗尽必须失败且不能虚增 Bytes: %v calls=%d bytes=%d", err, calls, res.Bytes)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	cfg.RetryBackoff, cfg.MaxRetryBackoff = time.Hour, time.Hour
	calls := 0
	start := time.Now()
	err := cfg.retry(ctx, discard, "Write", "c1", retryableFileError, func() error { calls++; return syscall.EIO })
	if !errors.Is(err, context.DeadlineExceeded) || calls != 1 || time.Since(start) > 500*time.Millisecond {
		t.Fatalf("退避等待期间应响应取消: %v calls=%d", err, calls)
	}
}

func TestStartAndStreamAreNotReplayedWithRetriesEnabled(t *testing.T) {
	for _, startLost := range []bool{true, false} {
		f := newFake()
		f.startResponseLost = startLost
		f.streamErrAt = 1
		cfg := testConfig(t, serve(t, f), "c1")
		enableRetries(&cfg)
		r := Run(context.Background(), cfg, discard)
		f.mu.Lock()
		calls := strings.Join(f.calls["c1"], ",")
		f.mu.Unlock()
		want := "start,query,query,query,stream,stop"
		reason := ReasonStreamFailed
		if startLost {
			want, reason = "start,stop", ReasonStartFailed
		}
		if r.Reason != reason || calls != want {
			t.Fatalf("Start/Stream 被不安全重放: %+v calls=%s", r, calls)
		}
	}
}

func assertOutputFilename(t *testing.T, name, namespace, pod, container, fileName string) {
	t.Helper()
	want := fmt.Sprintf("%s_%s_%s_%s", namespace, pod, container, fileName)
	if name != want || filepath.Base(name) != name || len(name) > 255 {
		t.Fatalf("output filename %q, want %q within basename limits", name, want)
	}
}

func TestCollectOutputFilenames(t *testing.T) {
	for _, partial := range []bool{false, true} {
		for _, longNames := range []bool{false, true} {
			t.Run(fmt.Sprintf("partial=%t/long=%t", partial, longNames), func(t *testing.T) {
				f := newFake()
				if partial {
					f.streamErrAt = 2
				}
				cfg := testConfig(t, serve(t, f), "containerd://runtime-id")
				namespace, pod, container := cfg.PodNamespace, cfg.PodName, "runtime-id"
				if longNames {
					cfg.PodNamespace, cfg.PodName = strings.Repeat("n", 63), strings.Repeat("p", 253)
					cfg.ContainerIDs = []string{"containerd://" + strings.Repeat("c", 253)}
					podHash := sha256.Sum256([]byte(cfg.PodName))
					namespace = cfg.PodNamespace
					pod = fmt.Sprintf("%s-%x", cfg.PodName[:47], podHash[:8])
					container = strings.Repeat("c", 12)
				}
				if err := cfg.Validate(); err != nil {
					t.Fatal(err)
				}
				res := Run(context.Background(), cfg, discard)
				phase, fileName := PhaseSucceeded, "result.json"
				if partial {
					phase, fileName = PhaseFailed, "result.json.partial"
				}
				if res.Phase != phase || len(res.Containers) != 1 || len(res.Containers[0].OutputFiles) != 1 || (partial && res.Reason != ReasonStreamFailed) {
					t.Fatalf("unexpected result %+v", res)
				}
				name := res.Containers[0].OutputFiles[0]
				assertOutputFilename(t, name, namespace, pod, container, fileName)
				if !partial && name != OutputFileName(cfg.PodNamespace, cfg.PodName, cfg.ContainerIDs[0], "result.json") {
					t.Fatalf("OutputFileName disagrees with written file %q", name)
				}
				info, err := os.Stat(filepath.Join(cfg.OutputDir, name))
				if err != nil || info.Mode().Perm() != 0o600 {
					t.Fatalf("output file info = %v, %v", info, err)
				}
			})
		}
	}
}

func TestCollectRemovesPreviousTargetOutputs(t *testing.T) {
	f := newFake()
	cfg := testConfig(t, serve(t, f), "containerd://new-id")
	stale := []string{
		"inference_vllm-inference-0_new-id_result.json",
		"inference_vllm-inference-0_new-id_result.json.partial",
		"inference_vllm-inference-0_old-id_trace.json",
		"inference_vllm-inference-0_old-id_trace.json.partial",
		"inference_vllm-inference-0_old-id_events.pb",
		"inference_vllm-inference-0_old-id_events.pb.partial",
		"inference_vllm-inference-0_old-id_metrics.csv",
		"inference_vllm-inference-0_old-id_no-extension",
		"inference_vllm-inference-0_old-id_daemon.partial.partial",
		"inference_vllm-inference-0_old__id_trace.json",
	}
	kept := []string{
		"inference_vllm-inference-1_old-id_metrics.csv",
		"inference_vllm-inference-1_old-id_metrics.csv.partial",
		"inference_vllm-inference-00_old-id_metrics.csv",
		"inference_vllm-inference-0-extra_old-id_metrics.csv.partial",
		"other_vllm-inference-0_old-id_result.json",
		"other_vllm-inference-0_old-id_result.json.partial",
		"inference-extra_vllm-inference-0_old-id_result.json",
		"inference_vllm-inference-0_result.json",
		"inference_vllm-inference-0__result.json",
		"inference_vllm-inference-0_old-id_",
		"inference_vllm-inference-0_old-id_.partial",
		"inference_vllm-inference-0_old-id_.hidden",
		"inference_vllm-inference-0_old-id_bad$name",
		"inference_vllm-inference-0_" + strings.Repeat("c", 13) + "_trace.json",
		"result.json",
	}
	for _, name := range append(append([]string{}, stale...), kept...) {
		if err := os.WriteFile(filepath.Join(cfg.OutputDir, name), []byte("old"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	res := Run(context.Background(), cfg, discard)
	if res.Phase != PhaseSucceeded {
		t.Fatalf("unexpected result %+v", res)
	}
	assertOnlyProfilingFiles(t, cfg.OutputDir, append(kept, "inference_vllm-inference-0_new-id_result.json")...)
	for _, name := range kept {
		if data, err := os.ReadFile(filepath.Join(cfg.OutputDir, name)); err != nil || string(data) != "old" {
			t.Fatalf("unrelated output %q changed: %q, %v", name, data, err)
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if got := strings.Join(f.calls["new-id"], ","); got != "start,query,query,query,stream,stop" {
		t.Fatalf("output removal must not require leftover RPC cleanup: %s", got)
	}
}

func TestPreviousOutputRemovalFailureStartsNoSession(t *testing.T) {
	f := newFake()
	cfg := testConfig(t, serve(t, f), "containerd://new-id")
	blocker := filepath.Join(cfg.OutputDir, "inference_vllm-inference-0_old-id_result.json")
	if err := os.Mkdir(blocker, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(blocker, "keep"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	res := Run(context.Background(), cfg, discard)
	if res.Phase != PhaseFailed || res.Reason != ReasonOutputFailed || !strings.Contains(res.Message, "remove previous target output") {
		t.Fatalf("unexpected result %+v", res)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.started) != 0 {
		t.Fatalf("sessions started despite cleanup failure: %v", f.started)
	}
}

func streamChunk(fileName, data string) *pb.ProfilingData {
	return &pb.ProfilingData{Id: "c1", FileName: fileName, RawData: []byte(data)}
}

func streamFileEnd(fileName string) *pb.ProfilingData {
	return &pb.ProfilingData{Id: "c1", FileName: fileName, FileEnd: true}
}

func assertStreamOutputs(t *testing.T, cfg Config, res Result, fileNames, contents []string, partial bool) {
	t.Helper()
	if len(res.Containers) != 1 || len(fileNames) != len(contents) {
		t.Fatalf("invalid result or expected files: %+v, %v, %v", res, fileNames, contents)
	}
	container := res.Containers[0]
	if len(container.OutputFiles) != len(fileNames) {
		t.Fatalf("output file count = %d, want %d: %+v", len(container.OutputFiles), len(fileNames), res)
	}
	var total int64
	for i, source := range fileNames {
		want := OutputFileName(cfg.PodNamespace, cfg.PodName, container.ContainerID, source)
		if partial {
			want += partialSuffix
		}
		if container.OutputFiles[i] != want || filepath.Base(want) != want || len(want) > 255 {
			t.Fatalf("output %d = %q, want basename %q", i, container.OutputFiles[i], want)
		}
		path := filepath.Join(cfg.OutputDir, want)
		data, err := os.ReadFile(path)
		if err != nil || string(data) != contents[i] {
			t.Fatalf("output %q = %q, want %q: %v", want, data, contents[i], err)
		}
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != 0o600 {
			t.Fatalf("output permissions for %q: %v, %v", want, info, err)
		}
		total += int64(len(contents[i]))
	}
	if container.Bytes != total {
		t.Fatalf("total bytes = %d, want %d", container.Bytes, total)
	}
	assertOnlyProfilingFiles(t, cfg.OutputDir, container.OutputFiles...)
}

func TestCollectMultipleFilesPreservesChunkBoundaries(t *testing.T) {
	f := newFake()
	f.streamMessages = []*pb.ProfilingData{
		streamChunk("z-first.trace.json", "first-"),
		streamChunk("z-first.trace.json", ""),
		streamChunk("z-first.trace.json", "payload"),
		streamFileEnd("z-first.trace.json"),
		streamChunk("a-second.pb", "\x00\x01"),
		streamChunk("a-second.pb", "\xfflast"),
		streamFileEnd("a-second.pb"),
	}
	cfg := testConfig(t, serve(t, f), "containerd://c1")
	res := Run(context.Background(), cfg, discard)
	if res.Phase != PhaseSucceeded || f.stopCount("c1") != 1 {
		t.Fatalf("unexpected multi-file result: %+v", res)
	}
	assertStreamOutputs(t, cfg, res, []string{"z-first.trace.json", "a-second.pb"}, []string{"first-payload", "\x00\x01\xfflast"}, false)
}

func TestCollectEmptyFiles(t *testing.T) {
	for _, tc := range []struct {
		name     string
		messages []*pb.ProfilingData
		files    []string
		contents []string
	}{
		{"default empty file", nil, []string{"result.json"}, []string{""}},
		{"single end", []*pb.ProfilingData{streamFileEnd("empty")}, []string{"empty"}, []string{""}},
		{"empty before and after data", []*pb.ProfilingData{
			streamFileEnd("empty.log"), streamChunk("data", "payload"), streamFileEnd("data"), streamFileEnd("source.partial"),
		}, []string{"empty.log", "data", "source.partial"}, []string{"", "payload", ""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake()
			f.chunks, f.streamMessages = nil, tc.messages
			cfg := testConfig(t, serve(t, f), "c1")
			res := Run(context.Background(), cfg, discard)
			if res.Phase != PhaseSucceeded || f.stopCount("c1") != 1 {
				t.Fatalf("empty files must be valid: %+v", res)
			}
			assertStreamOutputs(t, cfg, res, tc.files, tc.contents, false)
		})
	}
}

func TestStreamFileProtocolErrorsKeepPartials(t *testing.T) {
	for _, tc := range []struct {
		name     string
		messages []*pb.ProfilingData
		files    []string
		contents []string
	}{
		{"no files", []*pb.ProfilingData{}, nil, nil},
		{"missing end", []*pb.ProfilingData{streamChunk("first.log", "first")}, []string{"first.log"}, []string{"first"}},
		{"missing end on second file", []*pb.ProfilingData{
			streamChunk("first.log", "first"), streamFileEnd("first.log"), streamChunk("second.log", "second"),
		}, []string{"first.log", "second.log"}, []string{"first", "second"}},
		{"switch before end", []*pb.ProfilingData{streamChunk("first.log", "first"), streamChunk("second.log", "second")}, []string{"first.log"}, []string{"first"}},
		{"end switches name", []*pb.ProfilingData{streamChunk("first.log", "first"), streamFileEnd("second.log")}, []string{"first.log"}, []string{"first"}},
		{"duplicate name", []*pb.ProfilingData{
			streamChunk("first.log", "first"), streamFileEnd("first.log"), streamChunk("first.log", "duplicate"),
		}, []string{"first.log"}, []string{"first"}},
		{"repeated end", []*pb.ProfilingData{streamFileEnd("empty.log"), streamFileEnd("empty.log")}, []string{"empty.log"}, []string{""}},
		{"final aliases previous partial", []*pb.ProfilingData{
			streamChunk("first.log", "first"), streamFileEnd("first.log"), streamChunk("first.log.partial", "rejected"),
		}, []string{"first.log"}, []string{"first"}},
		{"partial aliases previous final", []*pb.ProfilingData{
			streamChunk("first.log.partial", "first"), streamFileEnd("first.log.partial"), streamChunk("first.log", "rejected"),
		}, []string{"first.log.partial"}, []string{"first"}},
		{"reuse name after another file", []*pb.ProfilingData{
			streamFileEnd("empty.log"), streamChunk("second.log", "second"), streamFileEnd("second.log"), streamChunk("empty.log", "duplicate"),
		}, []string{"empty.log", "second.log"}, []string{"", "second"}},
		{"data attached to end", []*pb.ProfilingData{streamChunk("first.log", "first"), {Id: "c1", FileName: "first.log", RawData: []byte("rejected"), FileEnd: true}}, []string{"first.log"}, []string{"first"}},
		{"first message has data and end", []*pb.ProfilingData{{Id: "c1", FileName: "first.log", RawData: []byte("rejected"), FileEnd: true}}, nil, nil},
		{"missing chunk name", []*pb.ProfilingData{streamChunk("first.log", "first"), streamChunk("", "rejected")}, []string{"first.log"}, []string{"first"}},
		{"missing end name", []*pb.ProfilingData{streamChunk("first.log", "first"), streamFileEnd("")}, []string{"first.log"}, []string{"first"}},
		{"wrong chunk ID", []*pb.ProfilingData{streamChunk("first.log", "first"), {Id: "other", FileName: "first.log", RawData: []byte("rejected")}}, []string{"first.log"}, []string{"first"}},
		{"wrong end ID", []*pb.ProfilingData{streamChunk("first.log", "first"), {Id: "other", FileName: "first.log", FileEnd: true}}, []string{"first.log"}, []string{"first"}},
		{"empty end ID", []*pb.ProfilingData{{FileName: "empty.log", FileEnd: true}}, nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake()
			f.streamMessages = tc.messages
			cfg := testConfig(t, serve(t, f), "c1")
			enableRetries(&cfg)
			res := Run(context.Background(), cfg, discard)
			if res.Phase != PhaseFailed || res.Reason != ReasonStreamFailed || res.Containers[0].Reason != ReasonStreamFailed || f.stopCount("c1") != 1 {
				t.Fatalf("protocol error must fail and stop once: %+v", res)
			}
			assertStreamOutputs(t, cfg, res, tc.files, tc.contents, true)
			f.mu.Lock()
			calls := strings.Join(f.calls["c1"], ",")
			f.mu.Unlock()
			if calls != "start,query,query,query,stream,stop" {
				t.Fatalf("protocol error replayed the stream: %s", calls)
			}
		})
	}
}

func TestStreamRejectsInvalidBasenames(t *testing.T) {
	for _, fileName := range []string{"", ".", "..", "../escape", "/tmp/escape", "dir/file", "dir\\file", "bad\x00name", "bad\nname", "bad name", strings.Repeat("x", 256)} {
		for _, end := range []bool{false, true} {
			t.Run(fmt.Sprintf("%q/end=%t", truncate(fileName, 64), end), func(t *testing.T) {
				f := newFake()
				invalid := streamChunk(fileName, "rejected")
				if end {
					invalid = streamFileEnd(fileName)
				}
				f.streamMessages = []*pb.ProfilingData{streamChunk("first.log", "first"), streamFileEnd("first.log"), invalid}
				cfg := testConfig(t, serve(t, f), "c1")
				res := Run(context.Background(), cfg, discard)
				if res.Phase != PhaseFailed || res.Reason != ReasonStreamFailed || f.stopCount("c1") != 1 {
					t.Fatalf("invalid basename accepted: %+v", res)
				}
				assertStreamOutputs(t, cfg, res, []string{"first.log"}, []string{"first"}, true)
			})
		}
	}
}

func TestMultiFileStreamInterruptionKeepsEveryPartial(t *testing.T) {
	for _, failAt := range []int{3, 4, 5} {
		t.Run(fmt.Sprintf("message-%d", failAt), func(t *testing.T) {
			f := newFake()
			f.streamMessages = []*pb.ProfilingData{
				streamChunk("first.log", "first"), streamFileEnd("first.log"),
				streamChunk("second.log", "second-"), streamChunk("second.log", "tail"), streamFileEnd("second.log"),
			}
			f.streamErrAt = failAt
			cfg := testConfig(t, serve(t, f), "c1")
			enableRetries(&cfg)
			res := Run(context.Background(), cfg, discard)
			if res.Phase != PhaseFailed || res.Reason != ReasonStreamFailed || !strings.Contains(res.Message, "ring buffer overflow") || f.stopCount("c1") != 1 {
				t.Fatalf("stream failure was not preserved: %+v", res)
			}
			second := "second-"
			if failAt >= 4 {
				second += "tail"
			}
			assertStreamOutputs(t, cfg, res, []string{"first.log", "second.log"}, []string{"first", second}, true)
		})
	}
}

func TestMultiFileStopFailureKeepsEveryPartial(t *testing.T) {
	for _, failure := range []string{"business", "transport", "timeout"} {
		t.Run(failure, func(t *testing.T) {
			f := newFake()
			f.streamMessages = []*pb.ProfilingData{
				streamChunk("first.log", "first"), streamFileEnd("first.log"), streamFileEnd("empty.log"),
				streamChunk("second.pb", "second"), streamFileEnd("second.pb"),
			}
			cfg := testConfig(t, serve(t, f), "c1")
			switch failure {
			case "business":
				f.stopStatus = 9
			case "transport":
				f.stopResponse = func(context.Context, int) (*pb.StopProfilingResponse, error) {
					return nil, status.Error(codes.PermissionDenied, "stop failed")
				}
			case "timeout":
				f.blockStop, cfg.StopTimeout = true, 40*time.Millisecond
			}
			res := Run(context.Background(), cfg, discard)
			if res.Phase != PhaseFailed || res.Reason != ReasonStopFailed || res.StopError == "" || res.Containers[0].StopError == "" || f.stopCount("c1") != 1 {
				t.Fatalf("Stop failure must prevent publishing every file: %+v", res)
			}
			assertStreamOutputs(t, cfg, res, []string{"first.log", "empty.log", "second.pb"}, []string{"first", "", "second"}, true)
		})
	}
}

func TestMultiFileRenameRetriesPublishInStreamOrder(t *testing.T) {
	f := newFake()
	f.streamMessages = []*pb.ProfilingData{streamChunk("z-first.log", "first"), streamFileEnd("z-first.log"), streamChunk("a-second.pb", "second"), streamFileEnd("a-second.pb")}
	cfg := testConfig(t, serve(t, f), "c1")
	enableRetries(&cfg)
	c := testCollector(t, cfg)
	var created, renamed []string
	handles := map[string]*faultyOutput{}
	f.stopResponse = func(context.Context, int) (*pb.StopProfilingResponse, error) {
		// Even a completed first file must remain partial until Stop succeeds.
		for _, fileName := range []string{"z-first.log", "a-second.pb"} {
			partial := OutputFileName(cfg.PodNamespace, cfg.PodName, "c1", fileName) + partialSuffix
			if _, err := os.Stat(filepath.Join(cfg.OutputDir, partial)); err != nil {
				return nil, fmt.Errorf("partial missing before Stop: %w", err)
			}
		}
		return &pb.StopProfilingResponse{}, nil
	}
	c.openOutput = func(id, fileName string) (*outputFile, error) {
		created = append(created, fileName)
		out, err := c.createOutput(id, fileName)
		if err != nil {
			return nil, err
		}
		handle := &faultyOutput{outputHandle: out.file}
		handles[fileName], out.file = handle, handle
		calls := 0
		out.rename = func(dir *os.File, from, to string) error {
			calls++
			renamed = append(renamed, fileName)
			want := OutputFileName(cfg.PodNamespace, cfg.PodName, id, fileName)
			if f.stopCount(id) != 1 || handle.closeCalls != 1 || from != want+partialSuffix || to != want {
				return fmt.Errorf("rename occurred before Stop/close or changed actual name: %s -> %s", from, to)
			}
			if calls == 1 {
				return syscall.EBUSY
			}
			return safefs.RenameAt(dir, from, to)
		}
		return out, nil
	}
	res := c.Collect(context.Background())
	if res.Phase != PhaseSucceeded || strings.Join(created, ",") != "z-first.log,a-second.pb" || strings.Join(renamed, ",") != "z-first.log,z-first.log,a-second.pb,a-second.pb" {
		t.Fatalf("create/rename must preserve stream order and retry only the operation: %+v created=%v renamed=%v", res, created, renamed)
	}
	for name, handle := range handles {
		if handle.syncCalls != 1 || handle.closeCalls != 1 {
			t.Fatalf("file %q not synced and closed exactly once: %+v", name, handle)
		}
	}
	assertStreamOutputs(t, cfg, res, []string{"z-first.log", "a-second.pb"}, []string{"first", "second"}, false)
}

func TestMultiFileOutputFailureKeepsEveryReceivedPartial(t *testing.T) {
	for _, operation := range []string{"CreateSecond", "WriteSecond", "SyncSecond", "CloseSecond", "RenameFirst"} {
		t.Run(operation, func(t *testing.T) {
			f := newFake()
			f.streamMessages = []*pb.ProfilingData{streamChunk("first.log", "first"), streamFileEnd("first.log"), streamChunk("second.pb", "second"), streamFileEnd("second.pb")}
			cfg := testConfig(t, serve(t, f), "c1")
			enableRetries(&cfg)
			c := testCollector(t, cfg)
			handles := map[string]*faultyOutput{}
			c.openOutput = func(id, fileName string) (*outputFile, error) {
				if operation == "CreateSecond" && fileName == "second.pb" {
					return nil, syscall.EACCES
				}
				out, err := c.createOutput(id, fileName)
				if err != nil {
					return nil, err
				}
				handle := &faultyOutput{outputHandle: out.file}
				handles[fileName], out.file = handle, handle
				if fileName == "second.pb" {
					switch operation {
					case "WriteSecond":
						handle.writeError = syscall.EACCES
					case "SyncSecond":
						handle.syncError = syscall.EACCES
					case "CloseSecond":
						handle.closeError = syscall.EIO
					}
				}
				if operation == "RenameFirst" && fileName == "first.log" {
					out.rename = func(*os.File, string, string) error { return syscall.EACCES }
				}
				return out, nil
			}
			res := c.Collect(context.Background())
			if res.Phase != PhaseFailed || res.Reason != ReasonOutputFailed || f.stopCount("c1") != 1 {
				t.Fatalf("multi-file output failure lost cleanup: %+v", res)
			}
			files, contents := []string{"first.log", "second.pb"}, []string{"first", "second"}
			if operation == "CreateSecond" {
				files, contents = files[:1], contents[:1]
			} else if operation == "WriteSecond" {
				contents[1] = "se"
			}
			assertStreamOutputs(t, cfg, res, files, contents, true)
			for name, handle := range handles {
				if handle.closeCalls != 1 {
					t.Fatalf("file %q must close exactly once after failure: %+v", name, handle)
				}
			}
		})
	}
}

func TestMultiFileCancellationAfterStreamEOFKeepsEveryPartial(t *testing.T) {
	f := newFake()
	f.streamMessages = []*pb.ProfilingData{streamChunk("first.log", "first"), streamFileEnd("first.log"), streamFileEnd("empty.log"), streamChunk("second.pb", "second"), streamFileEnd("second.pb")}
	cfg := testConfig(t, serve(t, f), "c1")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := testCollector(t, cfg)
	c.client = cancelAtEOFClient{XpuDeviceManagerServiceClient: c.client, cancel: cancel}
	res := c.Collect(ctx)
	if res.Phase != PhaseFailed || res.Reason != ReasonInterrupted || f.stopCount("c1") != 1 {
		t.Fatalf("cancellation at EOF finalized files or lost cleanup: %+v", res)
	}
	assertStreamOutputs(t, cfg, res, []string{"first.log", "empty.log", "second.pb"}, []string{"first", "", "second"}, true)
}

func TestOutputFileNameUsesRemainingByteBudget(t *testing.T) {
	for _, longIdentity := range []bool{false, true} {
		namespace, pod, id := "ns", "pod", "containerd://c1"
		if longIdentity {
			namespace, pod, id = strings.Repeat("n", 253), strings.Repeat("p", 253), "containerd://"+strings.Repeat("c", 253)
		}
		prefix := fmt.Sprintf("%s_%s_%s_", filenameComponent(namespace), filenameComponent(pod), containerFilenameID(id))
		budget := 255 - len(prefix) - len(partialSuffix)
		for _, length := range []int{budget - 1, budget, budget + 1, 253, 255} {
			source := strings.Repeat("s", length)
			got := OutputFileName(namespace, pod, id, source)
			want := prefix + source
			if length > budget {
				hash := sha256.Sum256([]byte(source))
				want = prefix + fmt.Sprintf("%s-%x", source[:budget-17], hash[:8])
			}
			if got != want || len(got+partialSuffix) > 255 || filepath.Base(got) != got || OutputFileName(namespace, pod, id, source) != got {
				t.Fatalf("filename budget or deterministic hash violated: %q, want %q", got, want)
			}
			if length > budget && got == OutputFileName(namespace, pod, id, source[:length-1]+"t") {
				t.Fatalf("distinct truncated source names collided: %q", got)
			}
		}
	}
}

func TestCollectLongSourceNamesFitFinalAndPartial(t *testing.T) {
	for _, length := range []int{253, 255} {
		for _, partial := range []bool{false, true} {
			t.Run(fmt.Sprintf("length=%d/partial=%t", length, partial), func(t *testing.T) {
				f := newFake()
				source := strings.Repeat("s", length-4) + ".log"
				f.streamMessages = []*pb.ProfilingData{streamChunk(source, "payload"), streamFileEnd(source)}
				if partial {
					f.streamErrAt = len(f.streamMessages)
				}
				cfg := testConfig(t, serve(t, f), "c1")
				cfg.PodNamespace, cfg.PodName = strings.Repeat("n", 63), strings.Repeat("p", 253)
				res := Run(context.Background(), cfg, discard)
				if (res.Phase == PhaseFailed) != partial || (partial && res.Reason != ReasonStreamFailed) {
					t.Fatalf("unexpected long-source result: %+v", res)
				}
				assertStreamOutputs(t, cfg, res, []string{source}, []string{"payload"}, partial)
			})
		}
	}
}

func TestStreamRequestWireContract(t *testing.T) {
	var req pb.StreamProfilingDataRequest
	wire := []byte{0x0a, 0x02, 'c', '1', 0x12, 0x06, 0x0a, 0x01, 'k', 0x12, 0x01, 'v'}
	if err := proto.Unmarshal(wire, &req); err != nil {
		t.Fatal(err)
	}
	fields := req.ProtoReflect().Descriptor().Fields()
	options := fields.ByName("options")
	if req.GetId() != "c1" || req.GetOptions()["k"] != "v" || options == nil || options.Number() != 2 || !options.IsMap() || fields.ByName("format") != nil {
		t.Fatalf("stream request does not match XDM id=1/options=2: %v", &req)
	}
	data, err := proto.Marshal(&req)
	if err != nil || string(data) != string(wire) {
		t.Fatalf("stream request wire round trip: %x, %v", data, err)
	}
}

func TestContainerFilenameID(t *testing.T) {
	for _, length := range []int{1, 12, 13, 64, 253} {
		id := strings.Repeat("a", length)
		want := id
		if length > 12 {
			want = id[:12]
		}
		for _, prefix := range []string{"", "containerd://", "docker://"} {
			got := containerFilenameID(prefix + id)
			if got != want || len(got) > 12 {
				t.Fatalf("unexpected filename ID for %q: %q, want %q", prefix+id, got, want)
			}
		}
	}
	ids := []string{strings.Repeat("a", 64), strings.Repeat("b", 64)}
	f := newFake()
	cfg := testConfig(t, serve(t, f), ids...)
	res := Run(context.Background(), cfg, discard)
	if res.Phase != PhaseSucceeded || len(res.Containers) != 2 || res.Containers[0].OutputFiles[0] == res.Containers[1].OutputFiles[0] {
		t.Fatalf("distinct container prefixes did not produce independent outputs: %+v", res)
	}
	for i, id := range ids {
		f.mu.Lock()
		req := f.started[id]
		f.mu.Unlock()
		if res.Containers[i].ContainerID != id || req == nil || req.GetId() != id {
			t.Fatalf("RPC/report container ID must remain full length: %+v, request %v", res, req)
		}
		assertOutputFilename(t, res.Containers[i].OutputFiles[0], cfg.PodNamespace, cfg.PodName, id[:12], "result.json")
	}
}

func TestContainerFilenameIDCollisionRejected(t *testing.T) {
	for _, firstLength := range []int{12, 64} {
		t.Run(fmt.Sprint(firstLength), func(t *testing.T) {
			f := newFake()
			cfg := testConfig(t, serve(t, f), "containerd://"+strings.Repeat("a", firstLength), "docker://"+strings.Repeat("a", 63)+"b")
			if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "share filename prefix") {
				t.Fatalf("filename prefix collision was not rejected: %v", err)
			}
			res := Run(context.Background(), cfg, discard)
			if res.Phase != PhaseFailed || res.Reason != ReasonInvalidArgs || !strings.Contains(res.Message, "share filename prefix") {
				t.Fatalf("unexpected result for colliding container prefixes: %+v", res)
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if len(f.calls) != 0 {
				t.Fatalf("colliding prefixes must fail before RPCs: %v", f.calls)
			}
			assertOnlyProfilingFiles(t, cfg.OutputDir)
		})
	}
}

func TestCollectFilenameLengthFallback(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprintf("partial=%t", partial), func(t *testing.T) {
			f := newFake()
			source := strings.Repeat("s", 250) + ".json"
			f.streamMessages = []*pb.ProfilingData{streamChunk(source, "payload"), streamFileEnd(source)}
			if partial {
				f.streamErrAt = len(f.streamMessages)
			}
			cfg := testConfig(t, serve(t, f), "c1")
			cfg.PodNamespace, cfg.PodName = strings.Repeat("n", 63), strings.Repeat("p", 253)
			c := testCollector(t, cfg)
			var names []string
			c.openOutput = func(id, fileName string) (*outputFile, error) {
				dir, err := openOutputDir(cfg.OutputDir)
				if err != nil {
					return nil, err
				}
				out, err := c.createOutputAt(dir, id, fileName, func(dir *os.File, name string, perm uint32) (*os.File, error) {
					names = append(names, name)
					if len(name) > 128 {
						return nil, &os.PathError{Op: "open", Path: name, Err: syscall.ENAMETOOLONG}
					}
					return safefs.CreateExclAt(dir, name, perm)
				})
				if err != nil {
					dir.Close()
				}
				return out, err
			}
			res := c.Collect(context.Background())
			if (res.Phase == PhaseFailed) != partial || len(names) != 2 || len(names[0]) <= 128 || len(names[0]) > 255 || len(names[1]) > 128 || names[0] == names[1] || f.stopCount("c1") != 1 {
				t.Fatalf("filename fallback failed: %+v names=%v", res, names)
			}
			want := names[1]
			if !partial {
				want = strings.TrimSuffix(want, partialSuffix)
			}
			if res.Containers[0].OutputFiles[0] != want {
				t.Fatalf("fallback filename not reported: %+v, want %s", res, want)
			}
			data, err := os.ReadFile(filepath.Join(cfg.OutputDir, want))
			if err != nil || string(data) != "payload" {
				t.Fatalf("fallback output corrupted: %q, %v", data, err)
			}
			assertOnlyProfilingFiles(t, cfg.OutputDir, want)
			stale := []string{
				OutputFileName(cfg.PodNamespace, cfg.PodName, "old-id", source),
				OutputFileName(cfg.PodNamespace, cfg.PodName, "old-id", source) + partialSuffix,
				outputFileName(cfg.PodNamespace, cfg.PodName, "old-id", source, 24, 128),
				outputFileName(cfg.PodNamespace, cfg.PodName, "old-id", source, 24, 128) + partialSuffix,
			}
			kept := []string{
				outputFileName("other", cfg.PodName, "c1", source, 24, 128),
				outputFileName("other", cfg.PodName, "c1", source, 24, 128) + partialSuffix,
				outputFileName(cfg.PodNamespace, cfg.PodName+"-other", "c1", source, 24, 128),
				outputFileName(cfg.PodNamespace, cfg.PodName+"-other", "c1", source, 24, 128) + partialSuffix,
			}
			normalPrefix := filenameComponent(cfg.PodNamespace) + "_" + filenameComponent(cfg.PodName) + "_c1_"
			fallbackPrefix := boundedFilename(cfg.PodNamespace, 24) + "_" + boundedFilename(cfg.PodName, 24) + "_c1_"
			kept = append(kept,
				normalPrefix+strings.Repeat("d", 248-len(normalPrefix)),
				fallbackPrefix+strings.Repeat("d", 129-len(fallbackPrefix)),
			)
			for _, name := range append(append([]string{}, stale...), kept...) {
				if err := os.WriteFile(filepath.Join(cfg.OutputDir, name), []byte("keep"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			dir, err := openOutputDir(cfg.OutputDir)
			if err != nil {
				t.Fatal(err)
			}
			defer dir.Close()
			if err := removePreviousOutputs(dir, cfg); err != nil {
				t.Fatal(err)
			}
			assertOnlyProfilingFiles(t, cfg.OutputDir, kept...)
		})
	}
}

func TestFilenameFallbackDoesNotHideErrors(t *testing.T) {
	for _, code := range []syscall.Errno{syscall.ENAMETOOLONG, syscall.EACCES, syscall.ENOSPC, syscall.EEXIST} {
		t.Run(code.Error(), func(t *testing.T) {
			cfg := testConfig(t, "unused", "c1")
			c := New(cfg, nil, discard)
			calls := 0
			out, err := c.createOutputAt(nil, "c1", "result.json", func(_ *os.File, _ string, _ uint32) (*os.File, error) {
				calls++
				return nil, code
			})
			wantCalls := 1
			if code == syscall.ENAMETOOLONG {
				wantCalls = 2
			}
			if out != nil || !errors.Is(err, code) || calls != wantCalls {
				t.Fatalf("fallback must only handle name length: %+v, %v, calls=%d", out, err, calls)
			}
		})
	}
}

func TestOutputFilesJSONRoundTrip(t *testing.T) {
	files := []string{"ns_pod_c1_trace.json", "ns_pod_c1_empty.log.partial"}
	r := Result{Phase: PhaseFailed, Reason: ReasonStreamFailed, Containers: []ContainerResult{{ContainerID: "c1", Phase: PhaseFailed, OutputFiles: files, Bytes: 5}}}
	data := r.Marshal(TerminationMessageLimit)
	if !strings.Contains(string(data), `"outputFiles":[`) || strings.Contains(string(data), `"outputFile":`) {
		t.Fatalf("result must use only plural outputFiles: %s", data)
	}
	got, err := ParseResult(string(data))
	if err != nil || len(got.Containers) != 1 || strings.Join(got.Containers[0].OutputFiles, ",") != strings.Join(files, ",") || got.Containers[0].Bytes != 5 {
		t.Fatalf("outputFiles did not survive termination-message round trip: %+v, %v", got, err)
	}
}

func TestFilenameComponentShortening(t *testing.T) {
	for _, length := range []int{1, 64, 65, 253} {
		name := strings.Repeat("a", length)
		got := filenameComponent(name)
		if len(got) > 64 || !pathComponent.MatchString(got) {
			t.Fatalf("invalid shortened component %q", got)
		}
		if length <= 64 {
			if got != name {
				t.Fatalf("normal name changed: %q", got)
			}
			continue
		}
		hash := sha256.Sum256([]byte(name))
		want := fmt.Sprintf("%s-%x", name[:47], hash[:8])
		if got != want || filenameComponent(name) != got || got == filenameComponent(name[:len(name)-1]+"b") {
			t.Fatalf("shortening lost readable prefix, determinism or hash identity: %q, want %q", got, want)
		}
	}
}

func TestCollectSucceeds(t *testing.T) {
	f := newFake()
	cfg := testConfig(t, serve(t, f), "containerd://aaaaaaaaaaaaaaaa", "docker://bbbbbbbb")

	res := Run(context.Background(), cfg, discard)
	if res.Phase != PhaseSucceeded || len(res.Containers) != 2 || res.PodUID != cfg.PodUID {
		t.Fatalf("unexpected result %+v", res)
	}
	for i, id := range []string{"aaaaaaaaaaaaaaaa", "bbbbbbbb"} {
		f.mu.Lock()
		req, calls := f.started[id], strings.Join(f.calls[id], ",")
		f.mu.Unlock()
		if req == nil || req.Mode == nil || req.GetMode() != cfg.Mode || req.Duration.AsDuration() != cfg.Duration || req.Options["k"] != "v" {
			t.Fatalf("unexpected StartProfiling request for %s: %v", id, req)
		}
		if calls != "start,query,query,query,stream,stop" {
			t.Fatalf("unexpected RPC order: %s", calls)
		}
		if n := f.stopCount(id); n != 1 {
			t.Fatalf("StopProfiling called %d times for %s", n, id)
		}
		container := res.Containers[i]
		if container.ContainerID != id {
			t.Fatalf("container ID = %q, want %q", container.ContainerID, id)
		}
		assertOutputFilename(t, container.OutputFiles[0], cfg.PodNamespace, cfg.PodName, containerFilenameID(id), "result.json")
		data, err := os.ReadFile(filepath.Join(cfg.OutputDir, container.OutputFiles[0]))
		if err != nil || string(data) != `{"traceEvents":[{"name":"k"}]}` {
			t.Fatalf("output for %s = %q, %v", id, data, err)
		}
	}
	c := res.Containers[0]
	if c.ContainerID != "aaaaaaaaaaaaaaaa" || c.ElapsedMillis != 60000 || c.Bytes != 30 ||
		filepath.Dir(c.OutputFiles[0]) != "." || !strings.HasSuffix(c.OutputFiles[0], "result.json") {
		t.Fatalf("unexpected container result %+v", c)
	}
}

func TestStartExistingTaskStopsBeforeRestart(t *testing.T) {
	for _, retries := range []int{0, 3} {
		for _, mode := range []pb.ProfilingMode{pb.ProfilingMode_PROFILING_MODE_FULL, pb.ProfilingMode_PROFILING_MODE_LIGHT} {
			t.Run(fmt.Sprintf("retries=%d/mode=%s", retries, mode), func(t *testing.T) {
				f := newFake()
				id := strings.Repeat("a", 64)
				cfg := testConfig(t, serve(t, f), "containerd://"+id)
				cfg.Mode = mode
				cfg.MaxRetries, cfg.RetryBackoff, cfg.MaxRetryBackoff = retries, time.Millisecond, time.Millisecond
				var first *pb.StartProfilingRequest
				f.startResponse = func(_ context.Context, req *pb.StartProfilingRequest, attempt int) (*pb.StartProfilingResponse, error) {
					if attempt == 1 {
						first = proto.Clone(req).(*pb.StartProfilingRequest)
						return &pb.StartProfilingResponse{Status: 1007, Messages: "container already has a running task"}, nil
					}
					f.mu.Lock()
					stops, queries := f.stopped[id], f.queries[id]
					f.mu.Unlock()
					if attempt != 2 || stops != 1 || queries != 0 || !proto.Equal(first, req) || req.GetId() != id || req.GetMode() != mode || req.GetDuration().AsDuration() != cfg.Duration || req.GetOptions()["k"] != "v" {
						return nil, status.Error(codes.Internal, "restart violated Stop ordering or request identity")
					}
					return &pb.StartProfilingResponse{}, nil
				}
				res := Run(context.Background(), cfg, discard)
				f.mu.Lock()
				calls := strings.Join(f.calls[id], ",")
				f.mu.Unlock()
				if res.Phase != PhaseSucceeded || len(res.Containers) != 1 || res.Containers[0].ContainerID != id || calls != "start,stop,start,query,query,query,stream,stop" || f.stopCount(id) != 2 {
					t.Fatalf("existing task was not safely replaced: %+v calls=%s", res, calls)
				}
				assertStreamOutputs(t, cfg, res, []string{"result.json"}, []string{`{"traceEvents":[{"name":"k"}]}`}, false)
			})
		}
	}
}

func TestStartExistingTaskDoesNotRestartPeer(t *testing.T) {
	f := newFake()
	cfg := testConfig(t, serve(t, f), "containerd://c1", "docker://c2")
	f.startResponse = func(_ context.Context, req *pb.StartProfilingRequest, attempt int) (*pb.StartProfilingResponse, error) {
		if req.GetId() == "c1" && attempt == 1 {
			return &pb.StartProfilingResponse{Status: 1007}, nil
		}
		return &pb.StartProfilingResponse{}, nil
	}
	res := Run(context.Background(), cfg, discard)
	f.mu.Lock()
	first, peer := strings.Join(f.calls["c1"], ","), strings.Join(f.calls["c2"], ",")
	f.mu.Unlock()
	if res.Phase != PhaseSucceeded || first != "start,stop,start,query,query,query,stream,stop" || peer != "start,query,query,query,stream,stop" {
		t.Fatalf("existing-task recovery affected another container: %+v calls=%s peer=%s", res, first, peer)
	}
}

func TestStartExistingTaskFailurePaths(t *testing.T) {
	for _, tc := range []struct {
		name, reason, calls string
		secondStatus        int32
		secondCode          codes.Code
		stopStatus          int32
		stopCode            codes.Code
		cancelAfterStop     bool
	}{
		{name: "stop-rejected", stopStatus: 1005, reason: ReasonStopFailed, calls: "start,stop"},
		{name: "stop-not-found", stopStatus: 1004, reason: ReasonStopFailed, calls: "start,stop"},
		{name: "stop-transport-exhausted", stopCode: codes.Unavailable, reason: ReasonStopFailed, calls: "start,stop,stop,stop,stop"},
		{name: "second-still-existing", secondStatus: 1007, reason: ReasonStartFailed, calls: "start,stop,start"},
		{name: "second-busy", secondStatus: 1006, reason: ReasonStartBusy, calls: "start,stop,start"},
		{name: "second-error", secondStatus: 1005, reason: ReasonStartFailed, calls: "start,stop,start"},
		{name: "second-response-lost", secondCode: codes.Unavailable, reason: ReasonStartFailed, calls: "start,stop,start,stop"},
		{name: "canceled-after-stop", cancelAfterStop: true, reason: ReasonInterrupted, calls: "start,stop"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake()
			cfg := testConfig(t, serve(t, f), "c1")
			cfg.MaxRetries, cfg.RetryBackoff, cfg.MaxRetryBackoff = 3, time.Millisecond, time.Millisecond
			cfg.StartBusyRetries = 0 // 此表只覆盖一次替换，1006 等待重试在 TestStartBusy* 中校验
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			f.startResponse = func(_ context.Context, _ *pb.StartProfilingRequest, attempt int) (*pb.StartProfilingResponse, error) {
				if attempt == 1 {
					return &pb.StartProfilingResponse{Status: 1007, Messages: "existing task"}, nil
				}
				if tc.secondCode != codes.OK {
					return nil, status.Error(tc.secondCode, "start accepted but response lost")
				}
				return &pb.StartProfilingResponse{Status: tc.secondStatus, Messages: "restart rejected"}, nil
			}
			f.stopResponse = func(_ context.Context, _ int) (*pb.StopProfilingResponse, error) {
				if tc.cancelAfterStop {
					cancel()
				}
				if tc.stopCode != codes.OK {
					return nil, status.Error(tc.stopCode, "stop unavailable")
				}
				return &pb.StopProfilingResponse{Status: tc.stopStatus, Messages: "stop rejected"}, nil
			}
			res := Run(ctx, cfg, discard)
			f.mu.Lock()
			calls := strings.Join(f.calls["c1"], ",")
			f.mu.Unlock()
			if res.Phase != PhaseFailed || res.Reason != tc.reason || calls != tc.calls || len(res.Containers) != 1 || len(res.Containers[0].OutputFiles) != 0 || (tc.reason == ReasonStopFailed && res.StopError == "") {
				t.Fatalf("unexpected existing-task failure: %+v calls=%s, want %s", res, calls, tc.calls)
			}
			assertOnlyProfilingFiles(t, cfg.OutputDir)
		})
	}
}

func TestStartExistingTaskStopTransientRecovery(t *testing.T) {
	f := newFake()
	cfg := testConfig(t, serve(t, f), "c1")
	cfg.MaxRetries, cfg.RetryBackoff, cfg.MaxRetryBackoff = 3, time.Millisecond, time.Millisecond
	f.startResponse = func(_ context.Context, _ *pb.StartProfilingRequest, attempt int) (*pb.StartProfilingResponse, error) {
		if attempt == 1 {
			return &pb.StartProfilingResponse{Status: 1007}, nil
		}
		return &pb.StartProfilingResponse{}, nil
	}
	f.stopResponse = func(_ context.Context, attempt int) (*pb.StopProfilingResponse, error) {
		if attempt == 1 {
			return nil, status.Error(codes.Unavailable, "temporary stop failure")
		}
		return &pb.StopProfilingResponse{}, nil
	}
	res := Run(context.Background(), cfg, discard)
	f.mu.Lock()
	calls := strings.Join(f.calls["c1"], ",")
	f.mu.Unlock()
	if res.Phase != PhaseSucceeded || calls != "start,stop,stop,start,query,query,query,stream,stop" || res.StopError != "" {
		t.Fatalf("existing-task Stop retry did not recover: %+v calls=%s", res, calls)
	}
}

func TestStartExistingTaskStopTimeoutPreventsRestart(t *testing.T) {
	f := newFake()
	f.startStatus["c1"] = 1007
	f.blockStop = true
	cfg := testConfig(t, serve(t, f), "c1")
	cfg.StopTimeout = 20 * time.Millisecond
	res := Run(context.Background(), cfg, discard)
	f.mu.Lock()
	calls := strings.Join(f.calls["c1"], ",")
	f.mu.Unlock()
	if res.Phase != PhaseFailed || res.Reason != ReasonStopFailed || res.StopError == "" || !strings.Contains(res.StopError, "DeadlineExceeded") || calls != "start,stop" {
		t.Fatalf("timed-out existing-task Stop allowed restart: %+v calls=%s", res, calls)
	}
	assertOnlyProfilingFiles(t, cfg.OutputDir)
}

func TestStartBusyWaitsAndRetriesWithoutStop(t *testing.T) {
	f := newFake()
	f.startResponse = func(_ context.Context, _ *pb.StartProfilingRequest, attempt int) (*pb.StartProfilingResponse, error) {
		if attempt <= 2 {
			return &pb.StartProfilingResponse{Status: 1006, Messages: "no idle worker"}, nil
		}
		return &pb.StartProfilingResponse{}, nil
	}
	res := Run(context.Background(), testConfig(t, serve(t, f), "c1"), discard)
	f.mu.Lock()
	calls := strings.Join(f.calls["c1"], ",")
	f.mu.Unlock()
	if res.Phase != PhaseSucceeded || res.StopError != "" || calls != "start,start,start,query,query,query,stream,stop" || f.stopCount("c1") != 1 {
		t.Fatalf("worker exhaustion retry did not recover without extra Stop: %+v calls=%s", res, calls)
	}
}

func TestStartBusyExhaustedReportsStartBusy(t *testing.T) {
	f := newFake()
	f.startStatus["c1"] = 1006
	res := Run(context.Background(), testConfig(t, serve(t, f), "c1"), discard)
	f.mu.Lock()
	calls := strings.Join(f.calls["c1"], ",")
	f.mu.Unlock()
	if res.Phase != PhaseFailed || res.Reason != ReasonStartBusy || !strings.Contains(res.Message, "status 1006 after 2 retries") || calls != "start,start,start" {
		t.Fatalf("exhausted worker retries must fail without Stop or Query: %+v calls=%s", res, calls)
	}
	if n := f.stopCount("c1"); n != 0 {
		t.Fatalf("StopProfiling called %d times", n)
	}
}

func TestStartBusyWaitHonorsCancellation(t *testing.T) {
	f := newFake()
	cfg := testConfig(t, serve(t, f), "c1")
	cfg.StartBusyRetries, cfg.StartBusyBackoff = 5, time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.startResponse = func(_ context.Context, _ *pb.StartProfilingRequest, attempt int) (*pb.StartProfilingResponse, error) {
		if attempt == 1 {
			// 取消发生在收到 1006、进入等待之后，才能验证退避可中断。
			time.AfterFunc(20*time.Millisecond, cancel)
		}
		return &pb.StartProfilingResponse{Status: 1006, Messages: "no idle worker"}, nil
	}
	res := Run(ctx, cfg, discard)
	f.mu.Lock()
	calls := strings.Join(f.calls["c1"], ",")
	f.mu.Unlock()
	if res.Phase != PhaseFailed || res.Reason != ReasonInterrupted || calls != "start" || strings.Contains(res.Message, "StartProfiling") || f.stopCount("c1") != 0 {
		t.Fatalf("busy wait ignored cancellation: %+v calls=%s", res, calls)
	}
}

func TestStartRejectedSkipsStop(t *testing.T) {
	f := newFake()
	f.startStatus["c1"] = 3
	res := Run(context.Background(), testConfig(t, serve(t, f), "c1"), discard)
	if res.Phase != PhaseFailed || res.Reason != ReasonStartFailed || !strings.Contains(res.Message, "no profiling env") {
		t.Fatalf("unexpected result %+v", res)
	}
	if n := f.stopCount("c1"); n != 0 {
		t.Fatalf("StopProfiling called %d times", n)
	}
}

func TestProfilingFailedStops(t *testing.T) {
	f := newFake()
	f.failStatus["c1"] = pb.ProfilingStatus_PROFILING_STATUS_FAILED
	res := Run(context.Background(), testConfig(t, serve(t, f), "c1"), discard)
	if res.Reason != ReasonProfilingFailed || !strings.Contains(res.Message, "cupti error") || res.ElapsedMillis != 60000 || res.Containers[0].ElapsedMillis != 60000 {
		t.Fatalf("unexpected result %+v", res)
	}
	if n := f.stopCount("c1"); n != 1 {
		t.Fatalf("StopProfiling called %d times", n)
	}
}

func TestStreamErrorKeepsPartial(t *testing.T) {
	f := newFake()
	f.streamErrAt = 2
	cfg := testConfig(t, serve(t, f), "c1")
	res := Run(context.Background(), cfg, discard)
	if res.Reason != ReasonStreamFailed || !strings.Contains(res.Message, "ring buffer overflow") {
		t.Fatalf("unexpected result %+v", res)
	}
	partial := res.Containers[0].OutputFiles[0]
	if !strings.HasSuffix(partial, ".partial") {
		t.Fatalf("failed result must point to a partial file: %s", partial)
	}
	if _, err := os.Stat(filepath.Join(cfg.OutputDir, partial)); err != nil {
		t.Fatalf("partial file missing: %v", err)
	}
	if final, _ := filepath.Glob(filepath.Join(cfg.OutputDir, "*_result.json")); len(final) != 0 {
		t.Fatalf("final files should not exist: %v", final)
	}
	if n := f.stopCount("c1"); n != 1 {
		t.Fatalf("StopProfiling called %d times", n)
	}
}

func TestInterruptStillStops(t *testing.T) {
	f := newFake()
	f.blockStream = true
	cfg := testConfig(t, serve(t, f), "c1")
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-f.streamStarted
		cancel()
	}()
	res := Run(ctx, cfg, discard)
	if res.Reason != ReasonInterrupted {
		t.Fatalf("unexpected result %+v", res)
	}
	if n := f.stopCount("c1"); n != 1 {
		t.Fatalf("StopProfiling called %d times after interrupt", n)
	}
}

func TestOneContainerFailureDoesNotAbortOthers(t *testing.T) {
	f := newFake()
	f.startStatus["bad"] = 1
	res := Run(context.Background(), testConfig(t, serve(t, f), "good", "bad"), discard)
	if res.Phase != PhaseFailed || res.Reason != ReasonStartFailed || !strings.Contains(res.Message, "1/2 container(s) failed") {
		t.Fatalf("unexpected result %+v", res)
	}
	if res.Containers[0].Phase != PhaseSucceeded || res.Containers[1].Phase != PhaseFailed {
		t.Fatalf("unexpected container results %+v", res.Containers)
	}
}

func TestStartTimeout(t *testing.T) {
	f := newFake()
	f.stayPending = true
	cfg := testConfig(t, serve(t, f), "c1")
	cfg.StartTimeout = 100 * time.Millisecond
	res := Run(context.Background(), cfg, discard)
	if res.Reason != ReasonStartTimeout || !strings.Contains(res.Message, "PENDING") || res.ElapsedMillis != 60000 || res.Containers[0].ElapsedMillis != 60000 {
		t.Fatalf("unexpected result %+v", res)
	}
	if n := f.stopCount("c1"); n != 1 {
		t.Fatalf("StopProfiling called %d times", n)
	}
}

func TestSocketUnavailable(t *testing.T) {
	cfg := testConfig(t, "/nonexistent/rk.sock", "c1")
	if res := Run(context.Background(), cfg, discard); res.Reason != ReasonSocketUnavailable {
		t.Fatalf("unexpected result %+v", res)
	}
}

func TestInvalidConfig(t *testing.T) {
	cfg := testConfig(t, "/nonexistent/rk.sock")
	cfg.PodUID = ""
	res := Run(context.Background(), cfg, discard)
	if res.Reason != ReasonInvalidArgs || !strings.Contains(res.Message, "pod-uid") || !strings.Contains(res.Message, "container-ids") {
		t.Fatalf("unexpected result %+v", res)
	}
}

func TestResultFitsTerminationMessage(t *testing.T) {
	r := Result{Phase: PhaseFailed, Reason: ReasonStreamFailed, Message: strings.Repeat("x", 10000)}
	for i := 0; i < 16; i++ {
		r.Containers = append(r.Containers, ContainerResult{ContainerID: strings.Repeat("a", 64), Phase: PhaseFailed, Message: strings.Repeat("y", 2000)})
	}
	path := filepath.Join(t.TempDir(), "termination-log")
	if err := WriteTerminationMessage(path, r); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(path)
	if len(b) > TerminationMessageLimit {
		t.Fatalf("termination message is %d bytes", len(b))
	}
	got, err := ParseResult(string(b))
	if err != nil || got.Phase != PhaseFailed || got.Reason != ReasonStreamFailed {
		t.Fatalf("parse = %+v, %v", got, err)
	}
}

func assertNoProfilingData(t *testing.T, cfg Config, res Result, f *fakeRuntimeKit) {
	t.Helper()
	if len(res.Containers) != 1 || res.Containers[0].Bytes != 0 || len(res.Containers[0].OutputFiles) != 0 {
		t.Fatalf("collection transferred data before COMPLETED: %+v", res)
	}
	f.mu.Lock()
	calls := strings.Join(f.calls[res.Containers[0].ContainerID], ",")
	f.mu.Unlock()
	if strings.Contains(calls, "stream") {
		t.Fatalf("stream RPC called before COMPLETED: %s", calls)
	}
	assertOnlyProfilingFiles(t, cfg.OutputDir)
}

func TestCollectRequiresCompletedBeforeTransfer(t *testing.T) {
	for _, tc := range []struct {
		name       string
		terminal   pb.ProfilingStatus
		wantReason string
	}{
		{"failed", pb.ProfilingStatus_PROFILING_STATUS_FAILED, ReasonProfilingFailed},
		{"canceled", pb.ProfilingStatus_PROFILING_STATUS_CANCELED, ReasonProfilingCanceled},
		{"never completes", pb.ProfilingStatus_PROFILING_STATUS_RUNNING, ReasonFinishTimeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake()
			f.stayRunning = true
			if tc.terminal != pb.ProfilingStatus_PROFILING_STATUS_RUNNING {
				f.queryResponse = func(_ context.Context, query int) (*pb.QueryProfilingStatusResponse, error) {
					s := pb.ProfilingStatus_PROFILING_STATUS_PENDING
					if query == 2 {
						s = pb.ProfilingStatus_PROFILING_STATUS_RUNNING
					} else if query >= 3 {
						s = tc.terminal
					}
					return &pb.QueryProfilingStatusResponse{ProfilingStatus: s}, nil
				}
			}
			cfg := testConfig(t, serve(t, f), "c1")
			cfg.Duration, cfg.FinishTimeout = 80*time.Millisecond, 50*time.Millisecond
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			started := time.Now()
			res := Run(ctx, cfg, discard)
			if ctx.Err() != nil || res.Phase != PhaseFailed || res.Reason != tc.wantReason || f.stopCount("c1") != 1 {
				t.Fatalf("unexpected result or stop count: %+v", res)
			}
			assertNoProfilingData(t, cfg, res, f)
			f.mu.Lock()
			calls := strings.Join(f.calls["c1"], ",")
			f.mu.Unlock()
			if tc.wantReason == ReasonFinishTimeout {
				if elapsed := time.Since(started); elapsed < cfg.Duration+cfg.FinishTimeout {
					t.Fatalf("completion timed out after %s, before duration + finish timeout %s", elapsed, cfg.Duration+cfg.FinishTimeout)
				}
				// A query abandoned at the deadline may reach the fake server after stop.
				if !strings.HasPrefix(calls, "start,query,query,query,") || strings.Contains(calls, "stream") {
					t.Fatalf("unexpected RPC order: %s", calls)
				}
			} else if calls != "start,query,query,query,stop" {
				t.Fatalf("unexpected RPC order: %s", calls)
			}
		})
	}
}

func TestCompletionTimeoutIncludesCollectionDuration(t *testing.T) {
	f := newFake()
	cfg := testConfig(t, serve(t, f), "c1")
	cfg.Duration, cfg.FinishTimeout = 800*time.Millisecond, 50*time.Millisecond
	f.queryResponse = func(ctx context.Context, query int) (*pb.QueryProfilingStatusResponse, error) {
		if query <= 2 {
			return &pb.QueryProfilingStatusResponse{ProfilingStatus: pb.ProfilingStatus_PROFILING_STATUS_RUNNING}, nil
		}
		// Completion must outlast FinishTimeout without exhausting Duration + FinishTimeout.
		timer := time.NewTimer(3 * cfg.FinishTimeout)
		defer timer.Stop()
		select {
		case <-timer.C:
			return &pb.QueryProfilingStatusResponse{ProfilingStatus: pb.ProfilingStatus_PROFILING_STATUS_COMPLETED}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	res := Run(context.Background(), cfg, discard)
	if res.Phase != PhaseSucceeded || len(res.Containers) != 1 || res.Containers[0].Bytes != 30 || !strings.HasSuffix(res.Containers[0].OutputFiles[0], "result.json") || f.stopCount("c1") != 1 {
		t.Fatalf("collection did not receive Duration + FinishTimeout budget: %+v", res)
	}
	f.mu.Lock()
	calls := strings.Join(f.calls["c1"], ",")
	f.mu.Unlock()
	if calls != "start,query,query,query,stream,stop" {
		t.Fatalf("unexpected RPC order: %s", calls)
	}
}

func TestCancellationWhileWaitingForCompletedStillStops(t *testing.T) {
	f := newFake()
	waiting, queryCanceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(release)
	f.queryResponse = func(ctx context.Context, query int) (*pb.QueryProfilingStatusResponse, error) {
		if query <= 2 {
			return &pb.QueryProfilingStatusResponse{ProfilingStatus: pb.ProfilingStatus_PROFILING_STATUS_RUNNING}, nil
		}
		close(waiting)
		<-ctx.Done()
		close(queryCanceled)
		<-release // Stop must not wait for the canceled query handler to return.
		return nil, ctx.Err()
	}
	cfg := testConfig(t, serve(t, f), "c1")
	cfg.Duration, cfg.FinishTimeout = 200*time.Millisecond, 200*time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan Result, 1)
	go func() { done <- Run(ctx, cfg, discard) }()
	select {
	case <-waiting:
	case <-time.After(2 * time.Second):
		t.Fatal("collector never waited for COMPLETED")
	}
	cancel()
	select {
	case <-queryCanceled:
	case <-time.After(time.Second):
		t.Fatal("completion query was not canceled")
	}
	select {
	case id := <-f.stopCalled:
		if id != "c1" {
			t.Fatalf("stopped unexpected container %q", id)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not independently stop the session")
	}
	select {
	case res := <-done:
		if res.Phase != PhaseFailed || res.Reason != ReasonInterrupted || f.stopCount("c1") != 1 {
			t.Fatalf("unexpected interrupted result %+v", res)
		}
		assertNoProfilingData(t, cfg, res, f)
		f.mu.Lock()
		calls := strings.Join(f.calls["c1"], ",")
		f.mu.Unlock()
		if calls != "start,query,query,query,stop" {
			t.Fatalf("unexpected RPC order: %s", calls)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("collector did not finish after cancellation")
	}
}

func TestCollectPreservesLatestQueryElapsed(t *testing.T) {
	for _, tc := range []struct {
		name        string
		terminal    pb.ProfilingStatus
		elapsed     *durationpb.Duration
		queryCode   codes.Code
		queryStatus int32
		block       bool
		interrupt   bool
		wantReason  string
		wantElapsed int64
	}{
		{name: "completed", terminal: pb.ProfilingStatus_PROFILING_STATUS_COMPLETED, elapsed: durationpb.New(40 * time.Second), wantElapsed: 40000},
		{name: "failed", terminal: pb.ProfilingStatus_PROFILING_STATUS_FAILED, elapsed: durationpb.New(40 * time.Second), wantReason: ReasonProfilingFailed, wantElapsed: 40000},
		{name: "canceled", terminal: pb.ProfilingStatus_PROFILING_STATUS_CANCELED, elapsed: durationpb.New(40 * time.Second), wantReason: ReasonProfilingCanceled, wantElapsed: 40000},
		{name: "finish timeout", block: true, wantReason: ReasonFinishTimeout, wantElapsed: 30000},
		{name: "interrupted", block: true, interrupt: true, wantReason: ReasonInterrupted, wantElapsed: 30000},
		{name: "query transport error", queryCode: codes.Unavailable, elapsed: durationpb.New(90 * time.Second), wantReason: ReasonQueryFailed, wantElapsed: 30000},
		{name: "query status error", queryStatus: 9, elapsed: durationpb.New(90 * time.Second), wantReason: ReasonQueryFailed, wantElapsed: 30000},
		{name: "missing duration", terminal: pb.ProfilingStatus_PROFILING_STATUS_FAILED, wantReason: ReasonProfilingFailed, wantElapsed: 30000},
		{name: "invalid duration", terminal: pb.ProfilingStatus_PROFILING_STATUS_FAILED, elapsed: &durationpb.Duration{Nanos: 1000000000}, wantReason: ReasonProfilingFailed, wantElapsed: 30000},
		{name: "negative duration", terminal: pb.ProfilingStatus_PROFILING_STATUS_FAILED, elapsed: durationpb.New(-time.Second), wantReason: ReasonProfilingFailed, wantElapsed: 30000},
		{name: "latest not maximum", terminal: pb.ProfilingStatus_PROFILING_STATUS_FAILED, elapsed: durationpb.New(10 * time.Second), wantReason: ReasonProfilingFailed, wantElapsed: 10000},
		{name: "valid zero", terminal: pb.ProfilingStatus_PROFILING_STATUS_FAILED, elapsed: durationpb.New(0), wantReason: ReasonProfilingFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			releaseQuery := make(chan struct{})
			defer close(releaseQuery)
			f.queryResponse = func(rpcCtx context.Context, query int) (*pb.QueryProfilingStatusResponse, error) {
				switch query {
				case 1:
					return &pb.QueryProfilingStatusResponse{ProfilingStatus: pb.ProfilingStatus_PROFILING_STATUS_RUNNING, ElapsedDuration: durationpb.New(time.Second)}, nil
				case 2:
					return &pb.QueryProfilingStatusResponse{ProfilingStatus: pb.ProfilingStatus_PROFILING_STATUS_RUNNING, ElapsedDuration: durationpb.New(30 * time.Second)}, nil
				default:
					if tc.block {
						if tc.interrupt {
							cancel()
						}
						<-rpcCtx.Done()
						// Let the client deadline fire before the server can return a transport error.
						<-releaseQuery
						return nil, rpcCtx.Err()
					}
					if tc.queryCode != codes.OK {
						return nil, status.Error(tc.queryCode, "query unavailable")
					}
					return &pb.QueryProfilingStatusResponse{Status: tc.queryStatus, ProfilingStatus: tc.terminal, ElapsedDuration: tc.elapsed}, nil
				}
			}
			cfg := testConfig(t, serve(t, f), "c1")
			cfg.Duration, cfg.FinishTimeout = 50*time.Millisecond, 200*time.Millisecond
			res := Run(ctx, cfg, discard)
			wantPhase, wantCalls := PhaseFailed, "start,query,query,query,stop"
			if tc.wantReason == "" {
				wantPhase, wantCalls = PhaseSucceeded, "start,query,query,query,stream,stop"
			}
			if res.Phase != wantPhase || res.Reason != tc.wantReason || res.ElapsedMillis != tc.wantElapsed || len(res.Containers) != 1 {
				t.Fatalf("unexpected result %+v", res)
			}
			container := res.Containers[0]
			if container.Phase != wantPhase || container.Reason != tc.wantReason || container.ElapsedMillis != tc.wantElapsed {
				t.Fatalf("unexpected container result %+v", container)
			}
			if wantPhase == PhaseSucceeded {
				if container.Bytes != 30 || !strings.HasSuffix(container.OutputFiles[0], "result.json") {
					t.Fatalf("unexpected completed output %+v", container)
				}
				data, err := os.ReadFile(filepath.Join(cfg.OutputDir, container.OutputFiles[0]))
				if err != nil || string(data) != `{"traceEvents":[{"name":"k"}]}` {
					t.Fatalf("profiling output not preserved: %q, %v", data, err)
				}
			} else {
				assertNoProfilingData(t, cfg, res, f)
			}
			f.mu.Lock()
			calls := strings.Join(f.calls["c1"], ",")
			f.mu.Unlock()
			if calls != wantCalls || f.stopCount("c1") != 1 {
				t.Fatalf("unexpected RPC order or stop count: %s", calls)
			}
			bounded := res.Marshal(TerminationMessageLimit)
			if len(bounded) > TerminationMessageLimit {
				t.Fatalf("termination report is %d bytes", len(bounded))
			}
			got, err := ParseResult(string(bounded))
			if err != nil || got.Phase != wantPhase || got.Reason != tc.wantReason || got.ElapsedMillis != tc.wantElapsed || len(got.Containers) != 1 || got.Containers[0].ElapsedMillis != tc.wantElapsed {
				t.Fatalf("duration lost in termination report: %+v, %v", got, err)
			}
		})
	}
}

func TestStopFailureMakesJobFail(t *testing.T) {
	f := newFake()
	f.stopStatus = 1
	cfg := testConfig(t, serve(t, f), "c1")
	res := Run(context.Background(), cfg, discard)
	if res.Phase != PhaseFailed || res.Reason != ReasonStopFailed || res.Containers[0].StopError == "" || !strings.HasSuffix(res.Containers[0].OutputFiles[0], partialSuffix) {
		t.Fatalf("unexpected result %+v", res)
	}
	entries, err := os.ReadDir(cfg.OutputDir)
	if err != nil || len(entries) != 1 || entries[0].Name() != res.Containers[0].OutputFiles[0] {
		t.Fatalf("stop failure published a final file: %v, %v", entries, err)
	}
}

func TestPreservePrimaryErrorOnStopFailure(t *testing.T) {
	f := newFake()
	f.streamErrAt, f.stopStatus = 1, 1
	res := Run(context.Background(), testConfig(t, serve(t, f), "c1"), discard)
	if res.Reason != ReasonStreamFailed || res.Containers[0].StopError == "" {
		t.Fatalf("unexpected result %+v", res)
	}
}

func TestQueryErrors(t *testing.T) {
	for _, code := range []codes.Code{codes.OK, codes.Unavailable} {
		t.Run(code.String(), func(t *testing.T) {
			f := newFake()
			f.queryCode = code
			if code == codes.OK {
				f.queryStatus = 9
			}
			res := Run(context.Background(), testConfig(t, serve(t, f), "c1"), discard)
			if res.Reason != ReasonQueryFailed || f.stopCount("c1") != 1 {
				t.Fatalf("unexpected result %+v", res)
			}
		})
	}
}

func TestCompletedBeforeRunningAccepted(t *testing.T) {
	f := newFake()
	f.completedEarly = true
	cfg := testConfig(t, serve(t, f), "c1")
	cfg.Duration = 50 * time.Millisecond
	res := Run(context.Background(), cfg, discard)
	if res.Phase != PhaseSucceeded || res.Reason != "" || len(res.Containers) != 1 || f.stopCount("c1") != 1 {
		t.Fatalf("unexpected result %+v", res)
	}
	container := res.Containers[0]
	if container.Bytes != 30 || !strings.HasSuffix(container.OutputFiles[0], "result.json") {
		t.Fatalf("unexpected completed output %+v", container)
	}
	if data, err := os.ReadFile(filepath.Join(cfg.OutputDir, container.OutputFiles[0])); err != nil || string(data) != `{"traceEvents":[{"name":"k"}]}` {
		t.Fatalf("completed output = %q, %v", data, err)
	}
	f.mu.Lock()
	calls := strings.Join(f.calls["c1"], ",")
	f.mu.Unlock()
	if calls != "start,query,query,stream,stop" {
		t.Fatalf("unexpected RPC order: %s", calls)
	}
}

func TestCanceledStatus(t *testing.T) {
	f := newFake()
	f.failStatus["c1"] = pb.ProfilingStatus_PROFILING_STATUS_CANCELED
	res := Run(context.Background(), testConfig(t, serve(t, f), "c1"), discard)
	if res.Reason != ReasonProfilingCanceled || f.stopCount("c1") != 1 {
		t.Fatalf("unexpected result %+v", res)
	}
}

func TestStreamContainerIDMismatch(t *testing.T) {
	f := newFake()
	f.mismatchedStreamID = true
	res := Run(context.Background(), testConfig(t, serve(t, f), "c1"), discard)
	if res.Reason != ReasonStreamFailed || res.Containers[0].Bytes != 0 || f.stopCount("c1") != 1 {
		t.Fatalf("unexpected result %+v", res)
	}
}

func TestBoundedRPCsAndStream(t *testing.T) {
	for _, operation := range []string{"start", "stream", "stop"} {
		t.Run(operation, func(t *testing.T) {
			f := newFake()
			cfg := testConfig(t, serve(t, f), "c1")
			want := ""
			switch operation {
			case "start":
				f.blockStart, cfg.RPCTimeout, want = true, 50*time.Millisecond, ReasonStartFailed
			case "stream":
				f.blockStream, cfg.StreamIdleTimeout, want = true, 50*time.Millisecond, ReasonStreamFailed
			case "stop":
				f.blockStop, cfg.StopTimeout, want = true, 50*time.Millisecond, ReasonStopFailed
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			res := Run(ctx, cfg, discard)
			if ctx.Err() != nil || res.Reason != want {
				t.Fatalf("unexpected result %+v", res)
			}
		})
	}
}

func TestStreamIdleTimeoutOnlyStopsStalledTransfers(t *testing.T) {
	t.Run("continuous transfer outlives idle timeout", func(t *testing.T) {
		f := newFake()
		f.chunks = [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"), []byte("f")}
		f.chunkDelay = 40 * time.Millisecond
		cfg := testConfig(t, serve(t, f), "c1")
		cfg.StreamIdleTimeout = 100 * time.Millisecond
		start := time.Now()
		res := Run(context.Background(), cfg, discard)
		if res.Phase != PhaseSucceeded || res.Containers[0].Bytes != 6 {
			t.Fatalf("持续有数据的传输不应超时: %+v", res)
		}
		if elapsed := time.Since(start); elapsed < 2*cfg.StreamIdleTimeout {
			t.Fatalf("传输总时长 %s 未超过空闲超时，用例无效", elapsed)
		}
	})
	t.Run("stall after progress", func(t *testing.T) {
		f := newFake()
		f.blockStream = true
		cfg := testConfig(t, serve(t, f), "c1")
		cfg.StreamIdleTimeout = 100 * time.Millisecond
		res := Run(context.Background(), cfg, discard)
		container := res.Containers[0]
		if res.Reason != ReasonStreamFailed || !strings.Contains(res.Message, "no stream progress for 100ms after 30 bytes") ||
			container.Bytes != 30 || !strings.HasSuffix(container.OutputFiles[0], ".partial") || f.stopCount("c1") != 1 {
			t.Fatalf("停滞的传输应按空闲超时失败并保留 partial: %+v", res)
		}
	})
}

func TestOutputFailurePreventsSessionStart(t *testing.T) {
	f := newFake()
	cfg := testConfig(t, serve(t, f), "c1")
	file := filepath.Join(cfg.OutputDir, "not-a-directory")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg.OutputDir = file
	res := Run(context.Background(), cfg, discard)
	if res.Reason != ReasonOutputFailed || f.stopCount("c1") != 0 {
		t.Fatalf("unexpected result %+v", res)
	}
}

func TestRepeatedCollectionKeepsLatestTargetOutputs(t *testing.T) {
	f := newFake()
	cfg := testConfig(t, serve(t, f), "c1")
	r1 := Run(context.Background(), cfg, discard)
	f.mu.Lock()
	f.streamMessages = []*pb.ProfilingData{streamChunk("result.json", "latest"), streamFileEnd("result.json")}
	f.mu.Unlock()
	r2 := Run(context.Background(), cfg, discard)
	if r1.Phase != PhaseSucceeded || r2.Phase != PhaseSucceeded || r1.Containers[0].OutputFiles[0] != r2.Containers[0].OutputFiles[0] {
		t.Fatalf("unexpected results %+v, %+v", r1, r2)
	}
	name := r2.Containers[0].OutputFiles[0]
	assertOutputFilename(t, name, cfg.PodNamespace, cfg.PodName, "c1", "result.json")
	assertOnlyProfilingFiles(t, cfg.OutputDir, name)
	if data, err := os.ReadFile(filepath.Join(cfg.OutputDir, name)); err != nil || string(data) != "latest" {
		t.Fatalf("same-container output not replaced: %q, %v", data, err)
	}
	cfg.ContainerIDs = []string{"containerd://new-id"}
	f.mu.Lock()
	f.streamMessages = nil
	f.mu.Unlock()
	r3 := Run(context.Background(), cfg, discard)
	if r3.Phase != PhaseSucceeded {
		t.Fatalf("unexpected result %+v", r3)
	}
	assertOutputFilename(t, r3.Containers[0].OutputFiles[0], cfg.PodNamespace, cfg.PodName, "new-id", "result.json")
	assertOnlyProfilingFiles(t, cfg.OutputDir, r3.Containers[0].OutputFiles[0])
}

func TestRejectInvalidOutputNames(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		mutate     func(*Config)
	}{
		{"invalid container delimiter", "invalid container ID", func(c *Config) { c.ContainerIDs = []string{"containerd://c:1"} }},
		{"duplicate IDs", "duplicate container ID", func(c *Config) {
			c.ContainerIDs = []string{"c1", "containerd://c1"}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig(t, "/nonexistent/rk.sock", "c1", "c2")
			tc.mutate(&cfg)
			if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validation = %v, want %q", err, tc.want)
			}
			if res := Run(context.Background(), cfg, discard); res.Reason != ReasonInvalidArgs || !strings.Contains(res.Message, tc.want) {
				t.Fatalf("unexpected result %+v", res)
			}
		})
	}
}

func TestRejectInvalidPathsAndConfiguration(t *testing.T) {
	for _, mutate := range []func(*Config){
		func(c *Config) { c.PodName = "../../escape" },
		func(c *Config) { c.PodNamespace = "/etc" },
		func(c *Config) { c.PodUID = "../escape" },
		func(c *Config) { c.ContainerIDs = []string{"containerd://../../escape"} },
		func(c *Config) { c.ContainerIDs = []string{"c1", "containerd://c1"} },
		func(c *Config) { c.SocketPath = "relative.sock" },
		func(c *Config) { c.Duration = 0 },
		func(c *Config) { c.PollInterval = -time.Second },
		func(c *Config) { c.StartTimeout = 0 },
		func(c *Config) { c.StreamIdleTimeout = 0 },
		func(c *Config) { c.FinishTimeout = time.Duration(1<<63 - 1) },
		func(c *Config) { c.Mode = 99 },
	} {
		cfg := testConfig(t, "/nonexistent/rk.sock", "c1")
		mutate(&cfg)
		if res := Run(context.Background(), cfg, discard); res.Reason != ReasonInvalidArgs {
			t.Fatalf("unexpected result %+v", res)
		}
	}
}

func TestUnicodeTerminationResult(t *testing.T) {
	r := Failed(ReasonStreamFailed, strings.Repeat("采集失败\"\n", 5000))
	r.PodName = strings.Repeat("\x00", 5000)
	b := r.Marshal(TerminationMessageLimit)
	if len(b) > TerminationMessageLimit || !json.Valid(b) {
		t.Fatalf("invalid termination JSON (%d bytes)", len(b))
	}
	if s := truncate("采集失败", 4); !utf8.ValidString(s) || s != "采..." {
		t.Fatalf("invalid UTF-8 truncation: %q", s)
	}
}

func TestAcceptedStartWithLostResponseIsStopped(t *testing.T) {
	f := newFake()
	f.startResponseLost = true
	res := Run(context.Background(), testConfig(t, serve(t, f), "c1"), discard)
	if res.Reason != ReasonStartFailed || f.stopCount("c1") != 1 {
		t.Fatalf("unexpected result %+v", res)
	}
}

type cancelAtEOFClient struct {
	pb.XpuDeviceManagerServiceClient
	cancel context.CancelFunc
}

func (c cancelAtEOFClient) StreamProfilingData(ctx context.Context, req *pb.StreamProfilingDataRequest, opts ...grpc.CallOption) (pb.XpuDeviceManagerService_StreamProfilingDataClient, error) {
	stream, err := c.XpuDeviceManagerServiceClient.StreamProfilingData(ctx, req, opts...)
	if err != nil {
		return nil, err
	}
	return cancelAtEOFStream{XpuDeviceManagerService_StreamProfilingDataClient: stream, cancel: c.cancel}, nil
}

type cancelAtEOFStream struct {
	pb.XpuDeviceManagerService_StreamProfilingDataClient
	cancel context.CancelFunc
}

func (s cancelAtEOFStream) Recv() (*pb.ProfilingData, error) {
	msg, err := s.XpuDeviceManagerService_StreamProfilingDataClient.Recv()
	if err == io.EOF {
		s.cancel()
	}
	return msg, err
}

func TestCancellationAfterStreamEOFKeepsPartial(t *testing.T) {
	f := newFake()
	cfg := testConfig(t, serve(t, f), "c1")
	conn, err := grpc.DialContext(context.Background(), "unix://"+cfg.SocketPath, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := cancelAtEOFClient{XpuDeviceManagerServiceClient: pb.NewXpuDeviceManagerServiceClient(conn), cancel: cancel}
	res := New(cfg, client, discard).Collect(ctx)
	if res.Phase != PhaseFailed || res.Reason != ReasonInterrupted || f.stopCount("c1") != 1 || len(res.Containers) != 1 {
		t.Fatalf("unexpected interrupted result %+v", res)
	}
	container := res.Containers[0]
	if container.Bytes != 30 || !strings.HasSuffix(container.OutputFiles[0], ".partial") {
		t.Fatalf("interrupted transfer finalized output: %+v", container)
	}
	data, err := os.ReadFile(filepath.Join(cfg.OutputDir, container.OutputFiles[0]))
	if err != nil || string(data) != `{"traceEvents":[{"name":"k"}]}` {
		t.Fatalf("partial output = %q, %v", data, err)
	}
	if final, _ := filepath.Glob(filepath.Join(cfg.OutputDir, "*_result.json")); len(final) != 0 {
		t.Fatalf("interrupted transfer has final files: %v", final)
	}
}

func TestCancellationDuringCleanupCannotSucceed(t *testing.T) {
	f := newFake()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.cancelOnStop = cancel
	res := Run(ctx, testConfig(t, serve(t, f), "c1"), discard)
	if res.Phase != PhaseFailed || res.Reason != ReasonInterrupted || f.stopCount("c1") != 1 {
		t.Fatalf("unexpected result %+v", res)
	}
}

func TestPreviousOutputSymlinksAreUnlinkedWithoutFollowing(t *testing.T) {
	for _, directory := range []bool{false, true} {
		t.Run(fmt.Sprintf("directory=%t", directory), func(t *testing.T) {
			f := newFake()
			cfg := testConfig(t, serve(t, f), "c1")
			outside := t.TempDir()
			dataPath := filepath.Join(outside, "untouched")
			if err := os.WriteFile(dataPath, []byte("untouched"), 0o600); err != nil {
				t.Fatal(err)
			}
			target := dataPath
			if directory {
				target = outside
			}
			before, err := os.Lstat(target)
			if err != nil {
				t.Fatal(err)
			}
			stale := []string{
				OutputFileName(cfg.PodNamespace, cfg.PodName, "old-id", "trace.json"),
				OutputFileName(cfg.PodNamespace, cfg.PodName, "c1", "result.json") + partialSuffix,
			}
			for _, name := range stale {
				if err := os.Symlink(target, filepath.Join(cfg.OutputDir, name)); err != nil {
					t.Fatal(err)
				}
			}
			res := Run(context.Background(), cfg, discard)
			if res.Phase != PhaseSucceeded || len(res.Containers) != 1 {
				t.Fatalf("safe symlink unlink prevented collection: %+v", res)
			}
			assertOnlyProfilingFiles(t, cfg.OutputDir, res.Containers[0].OutputFiles...)
			after, err := os.Lstat(target)
			if err != nil || !os.SameFile(before, after) || before.Mode() != after.Mode() {
				t.Fatalf("symlink target changed: %v, %v", after, err)
			}
			if data, err := os.ReadFile(dataPath); err != nil || string(data) != "untouched" {
				t.Fatalf("symlink target data changed: %q, %v", data, err)
			}
			entries, err := os.ReadDir(outside)
			if err != nil || len(entries) != 1 || entries[0].Name() != "untouched" {
				t.Fatalf("symlink target directory changed: %v, %v", entries, err)
			}
		})
	}
}

func TestOutputSymlinkRejected(t *testing.T) {
	f := newFake()
	cfg := testConfig(t, serve(t, f), "c1")
	outside := t.TempDir()
	link := filepath.Join(cfg.OutputDir, "symlink")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	cfg.OutputDir = filepath.Join(link, "target")
	res := Run(context.Background(), cfg, discard)
	if res.Reason != ReasonOutputFailed || f.stopCount("c1") != 0 {
		t.Fatalf("unexpected result %+v", res)
	}
	entries, err := os.ReadDir(outside)
	if err != nil || len(entries) != 0 {
		t.Fatalf("symlink target changed: %v, %v", entries, err)
	}
}

func TestFinalizeAnchoredToOpenedDirectory(t *testing.T) {
	cfg := testConfig(t, "/nonexistent/rk.sock", "c1")
	c := &Collector{cfg: cfg}
	out, err := c.createOutput("c1", "result.json")
	if err != nil {
		t.Fatal(err)
	}
	defer out.dir.Close()
	if _, err := out.file.Write([]byte("trace")); err != nil {
		t.Fatal(err)
	}
	out.file.Close()
	dir := cfg.OutputDir
	if err := os.Rename(dir, dir+"-moved"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir + "-moved") })
	outside := t.TempDir()
	if err := os.Symlink(outside, dir); err != nil {
		t.Fatal(err)
	}
	if err := out.finalize(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir+"-moved", out.name))
	if err != nil || string(data) != "trace" {
		t.Fatalf("anchored output missing: %q, %v", data, err)
	}
	entries, err := os.ReadDir(outside)
	if err != nil || len(entries) != 0 {
		t.Fatalf("symlink target changed: %v, %v", entries, err)
	}
}

func TestCompactionPreservesCleanupError(t *testing.T) {
	r := Failed(ReasonStreamFailed, strings.Repeat("stream failed", 1000))
	r.StopError = strings.Repeat("StopProfiling failed", 1000)
	for i := 0; i < 32; i++ {
		r.Containers = append(r.Containers, ContainerResult{ContainerID: strings.Repeat("a", 64), Phase: PhaseFailed, OutputFiles: []string{strings.Repeat("p", 1000), strings.Repeat("q", 1000)}})
	}
	b := r.Marshal(TerminationMessageLimit)
	parsed, err := ParseResult(string(b))
	if err != nil || len(b) > TerminationMessageLimit || parsed.StopError == "" || parsed.OmittedContainers != 32 || parsed.Reason != ReasonStreamFailed {
		t.Fatalf("invalid compacted result: %+v, %v", parsed, err)
	}
}

func TestCompactionPreservesElapsedDuration(t *testing.T) {
	for _, phase := range []Phase{PhaseSucceeded, PhaseFailed} {
		t.Run(string(phase), func(t *testing.T) {
			r := Result{Phase: phase}
			for i := 0; i < 32; i++ {
				r.Containers = append(r.Containers, ContainerResult{ContainerID: strings.Repeat("a", 64), Phase: phase, ElapsedMillis: int64(i + 1), OutputFiles: []string{strings.Repeat("p", 1000), strings.Repeat("q", 1000)}})
			}
			b := r.Marshal(TerminationMessageLimit)
			parsed, err := ParseResult(string(b))
			if err != nil || len(b) > TerminationMessageLimit || parsed.Phase != PhaseFailed || parsed.ElapsedMillis != 32 || parsed.OmittedContainers != 32 {
				t.Fatalf("duration lost in compacted result: %+v, %v", parsed, err)
			}
			if phase == PhaseSucceeded && (parsed.Reason != ReasonOutputFailed || !strings.Contains(parsed.Message, "output file report exceeds")) {
				t.Fatalf("oversized success silently lost file paths: %+v", parsed)
			}
		})
	}
}

func TestCleanupLeftoversOnlyOnRetry(t *testing.T) {
	for _, cleanup := range []bool{false, true} {
		for _, state := range []pb.ProfilingStatus{
			pb.ProfilingStatus_PROFILING_STATUS_PENDING, pb.ProfilingStatus_PROFILING_STATUS_RUNNING,
			pb.ProfilingStatus_PROFILING_STATUS_COMPLETED, pb.ProfilingStatus_PROFILING_STATUS_FAILED,
			pb.ProfilingStatus_PROFILING_STATUS_CANCELED,
		} {
			t.Run(fmt.Sprintf("cleanup=%v/%s", cleanup, state), func(t *testing.T) {
				f := newFake()
				f.leftoverStatus["c1"] = state
				cfg := testConfig(t, serve(t, f), "containerd://c1")
				cfg.CleanupLeftovers = cleanup
				res := Run(context.Background(), cfg, discard)
				if res.Phase != PhaseSucceeded {
					t.Fatalf("unexpected result %+v", res)
				}
				prefix := ""
				stops := 1
				if cleanup {
					prefix = "query,"
					if state == pb.ProfilingStatus_PROFILING_STATUS_PENDING || state == pb.ProfilingStatus_PROFILING_STATUS_RUNNING {
						prefix += "stop,"
						stops++
					}
				}
				f.mu.Lock()
				calls := strings.Join(f.calls["c1"], ",")
				f.mu.Unlock()
				if calls != prefix+"start,query,query,query,stream,stop" || f.stopCount("c1") != stops {
					t.Fatalf("unexpected retry RPC order: %s", calls)
				}
			})
		}
	}
}

func TestCleanupLeftoversNotFound(t *testing.T) {
	for _, ids := range [][]string{{"containerd://c1"}, {"containerd://c1", "containerd://c2"}} {
		t.Run(strings.Join(ids, ","), func(t *testing.T) {
			f := newFake()
			f.queryResponse = func(_ context.Context, _ int) (*pb.QueryProfilingStatusResponse, error) {
				f.mu.Lock()
				started := len(f.started) > 0
				f.mu.Unlock()
				if !started {
					return &pb.QueryProfilingStatusResponse{Status: 1004, Messages: "profiling task not found"}, nil
				}
				return &pb.QueryProfilingStatusResponse{ProfilingStatus: pb.ProfilingStatus_PROFILING_STATUS_COMPLETED}, nil
			}
			cfg := testConfig(t, serve(t, f), ids...)
			cfg.CleanupLeftovers = true
			res := Run(context.Background(), cfg, discard)
			if res.Phase != PhaseSucceeded || len(res.Containers) != len(ids) {
				t.Fatalf("missing leftovers prevented collection: %+v", res)
			}
			for _, rawID := range ids {
				id := NormalizeContainerID(rawID)
				f.mu.Lock()
				calls := strings.Join(f.calls[id], ",")
				f.mu.Unlock()
				if calls != "query,start,query,query,stream,stop" || f.stopCount(id) != 1 {
					t.Fatalf("missing leftover must not be stopped: %s calls=%s", id, calls)
				}
			}
		})
	}
}

func TestQueryNotFoundAfterStartFails(t *testing.T) {
	f := newFake()
	f.queryStatus = 1004
	cfg := testConfig(t, serve(t, f), "c1")
	cfg.CleanupLeftovers = true
	res := Run(context.Background(), cfg, discard)
	if res.Phase != PhaseFailed || res.Reason != ReasonQueryFailed || f.stopCount("c1") != 1 {
		t.Fatalf("missing active session must fail: %+v", res)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if calls := strings.Join(f.calls["c1"], ","); calls != "query,start,query,stop" {
		t.Fatalf("unexpected active-session RPC order: %s", calls)
	}
}

func TestCleanupFailurePreventsAllStarts(t *testing.T) {
	for _, operation := range []string{"query transport", "query status", "query internal error", "query busy", "query timeout", "unknown state", "stop status", "stop timeout"} {
		t.Run(operation, func(t *testing.T) {
			f := newFake()
			cfg := testConfig(t, serve(t, f), "c1", "c2")
			cfg.CleanupLeftovers = true
			cfg.RPCTimeout, cfg.StopTimeout = 50*time.Millisecond, 50*time.Millisecond
			switch operation {
			case "query transport":
				f.queryCode = codes.Unavailable
			case "query status":
				f.queryStatus = 8
			case "query internal error":
				f.queryStatus = 1005
			case "query busy":
				f.queryStatus = 1006
			case "query timeout":
				f.blockQuery = true
			case "unknown state":
				f.leftoverStatus["c2"] = 99
			case "stop status":
				f.stopStatus = 9
			case "stop timeout":
				f.blockStop = true
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			res := Run(ctx, cfg, discard)
			if ctx.Err() != nil || res.Phase != PhaseFailed || res.Reason != ReasonCleanupFailed {
				t.Fatalf("unexpected cleanup result %+v", res)
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if len(f.started) != 0 {
				t.Fatalf("cleanup failure started new sessions: %v", f.started)
			}
		})
	}
}

func TestCancellationStopsWhileFileWriteBlocked(t *testing.T) {
	f := newFake()
	f.chunks = [][]byte{make([]byte, 1<<20)}
	cfg := testConfig(t, serve(t, f), "c1")
	conn, err := grpc.DialContext(context.Background(), "unix://"+cfg.SocketPath, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	c := New(cfg, pb.NewXpuDeviceManagerServiceClient(conn), discard)
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer writer.Close()
	c.openOutput = func(id, fileName string) (*outputFile, error) {
		out, err := c.createOutput(id, fileName)
		if err != nil {
			return nil, err
		}
		out.file.Close()
		out.file = writer // A filled pipe models a PVC Write stuck in the kernel.
		return out, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan Result, 1)
	go func() { done <- c.Collect(ctx) }()
	if err := reader.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var first [1]byte
	if _, err := reader.Read(first[:]); err != nil {
		t.Fatalf("file write did not begin: %v", err)
	}
	// The 1MiB Write cannot finish while the remainder of the pipe is unread.
	cancel()
	select {
	case id := <-f.stopCalled:
		if id != "c1" {
			t.Fatalf("stopped unexpected container %q", id)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not promptly stop the session during blocked Write")
	}
	select {
	case r := <-done:
		t.Fatalf("collection unexpectedly finished before Write was released: %+v", r)
	default:
	}
	reader.Close() // Release the blocked Write after proving independent Stop.
	select {
	case r := <-done:
		if r.Reason != ReasonInterrupted || r.Phase != PhaseFailed || f.stopCount("c1") != 1 || !strings.HasSuffix(r.Containers[0].OutputFiles[0], ".partial") {
			t.Fatalf("unexpected interrupted result %+v", r)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("collector did not finish after Write was released")
	}
}

func TestRunWritesOnlyProfilingData(t *testing.T) {
	for _, tc := range []struct {
		name, reason, fileName string
		phase                  Phase
	}{
		{"success", "", "result.json", PhaseSucceeded},
		{"start failure", ReasonStartFailed, "", PhaseFailed},
		{"stream failure", ReasonStreamFailed, "result.json.partial", PhaseFailed},
		{"canceled before start", ReasonInterrupted, "", PhaseFailed},
		{"canceled during stream", ReasonInterrupted, "result.json.partial", PhaseFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch tc.name {
			case "start failure":
				f.startStatus["c1"] = 3
			case "stream failure":
				f.streamErrAt = 2
			case "canceled before start":
				cancel()
			case "canceled during stream":
				f.blockStream = true
				go func() {
					select {
					case <-f.streamStarted:
						cancel()
					case <-ctx.Done():
					}
				}()
			}
			cfg := testConfig(t, serve(t, f), "c1")
			res := Run(ctx, cfg, discard)
			if res.Phase != tc.phase || res.Reason != tc.reason {
				t.Fatalf("unexpected result %+v", res)
			}
			if tc.fileName == "" {
				assertOnlyProfilingFiles(t, cfg.OutputDir)
				return
			}
			if len(res.Containers) != 1 {
				t.Fatalf("missing container result: %+v", res)
			}
			if tc.name == "canceled during stream" && len(res.Containers[0].OutputFiles) == 0 {
				// Cancellation may arrive before the first valid file message is received.
				if res.Containers[0].Bytes != 0 || f.stopCount("c1") != 1 {
					t.Fatalf("cancellation before file creation lost cleanup: %+v", res)
				}
				assertOnlyProfilingFiles(t, cfg.OutputDir)
				return
			}
			if len(res.Containers[0].OutputFiles) != 1 {
				t.Fatalf("unexpected output file count: %+v", res)
			}
			name := res.Containers[0].OutputFiles[0]
			assertOutputFilename(t, name, cfg.PodNamespace, cfg.PodName, "c1", tc.fileName)
			assertOnlyProfilingFiles(t, cfg.OutputDir, name)
			if f.stopCount("c1") != 1 {
				t.Fatalf("StopProfiling called %d times", f.stopCount("c1"))
			}
		})
	}
}

func TestCollectIgnoresExistingDiagnosticFileAndSymlink(t *testing.T) {
	for _, name := range []string{
		"result.json",
		"inference_vllm-inference-0_result.json",
		"inference_vllm-inference-0_old-id_.hidden",
		"other_vllm-inference-0_old-id_result.json",
	} {
		for _, symlink := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/symlink=%t", name, symlink), func(t *testing.T) {
				f := newFake()
				cfg := testConfig(t, serve(t, f), "c1")
				path := filepath.Join(cfg.OutputDir, name)
				dataPath := path
				if symlink {
					dataPath = filepath.Join(t.TempDir(), "report")
				}
				if err := os.WriteFile(dataPath, []byte("untouched"), 0o600); err != nil {
					t.Fatal(err)
				}
				if symlink {
					if err := os.Symlink(dataPath, path); err != nil {
						t.Fatal(err)
					}
				}
				before, err := os.Lstat(path)
				if err != nil {
					t.Fatal(err)
				}
				res := Run(context.Background(), cfg, discard)
				if res.Phase != PhaseSucceeded || len(res.Containers) != 1 || f.stopCount("c1") != 1 {
					t.Fatalf("existing diagnostic prevented collection: %+v", res)
				}
				after, err := os.Lstat(path)
				if err != nil || !os.SameFile(before, after) || before.Mode() != after.Mode() {
					t.Fatalf("existing diagnostic replaced or removed: %v, %v", after, err)
				}
				if data, err := os.ReadFile(dataPath); err != nil || string(data) != "untouched" {
					t.Fatalf("existing diagnostic or symlink target changed: %q, %v", data, err)
				}
				if symlink {
					if target, err := os.Readlink(path); err != nil || target != dataPath {
						t.Fatalf("existing symlink changed: %q, %v", target, err)
					}
				}
				entries, err := os.ReadDir(cfg.OutputDir)
				if err != nil || len(entries) != 2 {
					t.Fatalf("new diagnostics or temporary files created: %v, %v", entries, err)
				}
				for _, entry := range entries {
					if entry.Name() != name && entry.Name() != res.Containers[0].OutputFiles[0] {
						t.Fatalf("unexpected output entry %q", entry.Name())
					}
				}
			})
		}
	}
}

func TestContainersCollectConcurrently(t *testing.T) {
	f := newFake()
	f.blockStream = true
	cfg := testConfig(t, serve(t, f), "c1", "c2")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan Result, 1)
	go func() { done <- Run(ctx, cfg, discard) }()
	// Both streams must start before either is released; serial collection
	// would remain blocked in the first stream and never reach the second.
	for i := 0; i < 2; i++ {
		select {
		case <-f.streamStarted:
		case <-time.After(2 * time.Second):
			t.Fatal("containers did not stream concurrently")
		}
	}
	cancel()
	select {
	case r := <-done:
		if r.Phase != PhaseFailed || r.Reason != ReasonInterrupted || f.stopCount("c1") != 1 || f.stopCount("c2") != 1 {
			t.Fatalf("unexpected result %+v", r)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("concurrent collection did not stop")
	}
}

func TestOutputFailureAfterStartStillStops(t *testing.T) {
	f := newFake()
	cfg := testConfig(t, serve(t, f), "c1")
	conn, err := grpc.DialContext(context.Background(), "unix://"+cfg.SocketPath, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	c := New(cfg, pb.NewXpuDeviceManagerServiceClient(conn), discard)
	c.openOutput = func(string, string) (*outputFile, error) { return nil, io.ErrClosedPipe }
	r := c.Collect(context.Background())
	if r.Phase != PhaseFailed || r.Reason != ReasonOutputFailed || f.stopCount("c1") != 1 {
		t.Fatalf("unexpected result %+v", r)
	}
}
