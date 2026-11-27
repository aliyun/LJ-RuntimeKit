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

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	pb "github.com/lj-runtimekit/exporter/api/rtkit/profiling"
	"github.com/lj-runtimekit/exporter/pkg/collector"
)

const helperEnv = "COLLECTOR_COMMAND_HELPER"

func TestMainHelperProcess(t *testing.T) {
	if os.Getenv(helperEnv) != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg == "--" {
			os.Args = append([]string{os.Args[0]}, os.Args[i+1:]...)
			main()
			return
		}
	}
	t.Fatal("helper argument separator missing")
}

func command(t *testing.T, args ...string) (*exec.Cmd, *bytes.Buffer) {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, append([]string{"-test.run=^TestMainHelperProcess$", "--"}, args...)...)
	cmd.Env = append(os.Environ(), helperEnv+"=1")
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	return cmd, &output
}

func runCommand(t *testing.T, want int, args ...string) string {
	t.Helper()
	cmd, output := command(t, args...)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		_ = cmd.Process.Kill()
		<-done
		t.Fatalf("command timed out: %s", output.String())
	}
	if got := cmd.ProcessState.ExitCode(); got != want {
		t.Fatalf("exit = %d, want %d\n%s", got, want, output.String())
	}
	return output.String()
}

func readReport(t *testing.T, path string) collector.Result {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) > collector.TerminationMessageLimit {
		t.Fatalf("termination report is %d bytes", len(b))
	}
	var report collector.Result
	if err := json.Unmarshal(b, &report); err != nil {
		t.Fatalf("report %q: %v", b, err)
	}
	return report
}

func realTempDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return dir
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

type commandStreamFile struct {
	name string
	data []byte
}

type commandRuntimeKit struct {
	pb.UnimplementedXpuDeviceManagerServiceServer
	mu            sync.Mutex
	streamFiles   []commandStreamFile
	skipFileEnd   bool
	started       []*pb.StartProfilingRequest
	calls         []string
	queries       map[string]int
	completed     map[string]bool
	blockStream   bool
	streamError   string
	stopStatus    int32
	queryFailures int
	stopFailures  int
	stopHook      func()
	streamStarted chan struct{}
	stopped       chan struct{}
}

func (f *commandRuntimeKit) StartProfiling(_ context.Context, r *pb.StartProfilingRequest) (*pb.StartProfilingResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "start")
	f.started = append(f.started, r)
	f.queries[r.Id], f.completed[r.Id] = 0, false
	return &pb.StartProfilingResponse{}, nil
}

func (f *commandRuntimeKit) QueryProfilingStatus(_ context.Context, r *pb.QueryProfilingStatusRequest) (*pb.QueryProfilingStatusResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "query")
	count, known := f.queries[r.Id]
	if !known {
		// 与真实 daemon 一致：未经 Start 的容器没有会话，返回 1004 NotFound，
		// 因此默认开启的残留会话清理只是一次无副作用探测，不消耗注入的故障。
		return &pb.QueryProfilingStatusResponse{Status: 1004, Messages: "profiling task not found"}, nil
	}
	// 失败注入的尝试也计入查询次数，保持注入前后总调用数一致。
	count++
	f.queries[r.Id] = count
	if f.queryFailures > 0 {
		f.queryFailures--
		return nil, status.Error(codes.Unavailable, "临时 Query 失败")
	}
	s := pb.ProfilingStatus_PROFILING_STATUS_PENDING
	if count == 2 {
		s = pb.ProfilingStatus_PROFILING_STATUS_RUNNING
	} else if count >= 3 {
		s = pb.ProfilingStatus_PROFILING_STATUS_COMPLETED
		f.completed[r.Id] = true
	}
	return &pb.QueryProfilingStatusResponse{ProfilingStatus: s}, nil
}

func (f *commandRuntimeKit) StreamProfilingData(r *pb.StreamProfilingDataRequest, s pb.XpuDeviceManagerService_StreamProfilingDataServer) error {
	f.mu.Lock()
	f.calls = append(f.calls, "stream")
	completed, blockStream, streamError := f.completed[r.Id], f.blockStream, f.streamError
	files, skipFileEnd := f.streamFiles, f.skipFileEnd
	if files == nil {
		files = []commandStreamFile{{name: "result.json", data: []byte("trace")}}
		for _, started := range f.started {
			if started.Id == r.Id && started.GetMode() == pb.ProfilingMode_PROFILING_MODE_LIGHT {
				files = []commandStreamFile{{name: "lastkernels.log", data: []byte("trace")}, {name: "kerneltime.log", data: []byte("trace")}}
				break
			}
		}
	}
	f.mu.Unlock()
	if !completed {
		return status.Error(codes.FailedPrecondition, "stream requested before COMPLETED was observed")
	}
	for i, file := range files {
		if len(file.data) > 0 {
			if err := s.Send(&pb.ProfilingData{Id: r.Id, FileName: file.name, RawData: file.data}); err != nil {
				return err
			}
		}
		if i == 0 {
			f.streamStarted <- struct{}{}
			if blockStream {
				<-s.Context().Done()
				return s.Context().Err()
			}
		}
		if skipFileEnd && i == len(files)-1 {
			continue
		}
		if err := s.Send(&pb.ProfilingData{Id: r.Id, FileName: file.name, FileEnd: true}); err != nil {
			return err
		}
	}
	if streamError != "" {
		return status.Error(codes.Internal, streamError)
	}
	return nil
}

func (f *commandRuntimeKit) StopProfiling(_ context.Context, _ *pb.StopProfilingRequest) (*pb.StopProfilingResponse, error) {
	f.mu.Lock()
	f.calls = append(f.calls, "stop")
	if f.stopFailures > 0 {
		f.stopFailures--
		f.mu.Unlock()
		return nil, status.Error(codes.ResourceExhausted, "临时 Stop 失败")
	}
	stopHook, stopStatus := f.stopHook, f.stopStatus
	f.mu.Unlock()
	if stopHook != nil {
		stopHook()
	}
	f.stopped <- struct{}{}
	return &pb.StopProfilingResponse{Status: stopStatus, Messages: "stop failed"}, nil
}

func startRuntimeKit(t *testing.T) (*commandRuntimeKit, string) {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "cmd-collector-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "rk.sock")
	lis, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	f := &commandRuntimeKit{queries: make(map[string]int), completed: make(map[string]bool), streamStarted: make(chan struct{}, 8), stopped: make(chan struct{}, 8)}
	srv := grpc.NewServer()
	pb.RegisterXpuDeviceManagerServiceServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return f, socket
}

func collectArgs(socket, output, termination string) []string {
	return []string{"collect", "--socket=" + socket, "--pod-namespace=ns", "--pod-name=pod", "--pod-uid=uid", "--container-ids=containerd://c1", "--duration=1s", "--output-dir=" + output, "--poll-interval=1ms", "--rpc-timeout=1s", "--start-timeout=1s", "--stream-idle-timeout=1s", "--finish-timeout=1s", "--stop-timeout=1s", "--termination-message-path=" + termination}
}

func TestProfilingModeContract(t *testing.T) {
	for _, tc := range []struct {
		cli    string
		name   string
		number byte
	}{
		{"light", "PROFILING_MODE_LIGHT", 0},
		{"full", "PROFILING_MODE_FULL", 1},
	} {
		t.Run(tc.cli, func(t *testing.T) {
			mode, err := parseMode(tc.cli)
			if err != nil || int32(mode) != int32(tc.number) || mode.String() != tc.name {
				t.Fatalf("模式映射错误：mode=%v err=%v", mode, err)
			}
			number, ok := pb.ProfilingMode_value[tc.name]
			if !ok || number != int32(tc.number) || pb.ProfilingMode_name[number] != tc.name {
				t.Fatalf("枚举名称映射错误：name=%s number=%d", tc.name, number)
			}

			req := &pb.StartProfilingRequest{Mode: mode.Enum()}
			wire, err := proto.Marshal(req)
			// mode 仍为字段 2，显式 LIGHT=0 必须保留 optional 字段的存在性。
			if err != nil || !bytes.Equal(wire, []byte{0x10, tc.number}) {
				t.Fatalf("协议二进制编码改变：wire=%x err=%v", wire, err)
			}
			var decoded pb.StartProfilingRequest
			if err := proto.Unmarshal(wire, &decoded); err != nil || decoded.Mode == nil || decoded.GetMode() != mode {
				t.Fatalf("协议二进制解码错误：req=%v err=%v", &decoded, err)
			}

			data, err := protojson.Marshal(req)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]string
			if err := json.Unmarshal(data, &fields); err != nil || fields["mode"] != tc.name {
				t.Fatalf("protobuf JSON 枚举名称错误：data=%s err=%v", data, err)
			}
			var fromJSON pb.StartProfilingRequest
			if err := protojson.Unmarshal(data, &fromJSON); err != nil || fromJSON.Mode == nil || fromJSON.GetMode() != mode {
				t.Fatalf("protobuf JSON 解码错误：req=%v err=%v", &fromJSON, err)
			}
		})
	}
}

func TestCollectCommandSuccess(t *testing.T) {
	for _, tc := range []struct {
		mode      string
		wantMode  pb.ProfilingMode
		fileNames []string
	}{
		{"full", pb.ProfilingMode_PROFILING_MODE_FULL, []string{"result.json"}},
		{"light", pb.ProfilingMode_PROFILING_MODE_LIGHT, []string{"lastkernels.log", "kerneltime.log"}},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			f, socket := startRuntimeKit(t)
			dir := realTempDir(t)
			out, termination := filepath.Join(dir, "target"), filepath.Join(dir, "termination.json")
			args := append(collectArgs(socket, out, termination), "--mode="+tc.mode)
			runCommand(t, 0, args...)
			r := readReport(t, termination)
			if r.Phase != collector.PhaseSucceeded || r.PodNamespace != "ns" || r.PodName != "pod" || r.PodUID != "uid" || len(r.Containers) != 1 {
				t.Fatalf("report = %+v", r)
			}
			c := r.Containers[0]
			if c.ContainerID != "c1" || c.Bytes != int64(5*len(tc.fileNames)) || len(c.OutputFiles) != len(tc.fileNames) {
				t.Fatalf("container result = %+v", c)
			}
			for i, name := range tc.fileNames {
				if filepath.Dir(c.OutputFiles[i]) != "." || c.OutputFiles[i] != "ns_pod_c1_"+name {
					t.Fatalf("container output names = %q", c.OutputFiles)
				}
				if b, err := os.ReadFile(filepath.Join(out, c.OutputFiles[i])); err != nil || string(b) != "trace" {
					t.Fatalf("raw output = %q, %v", b, err)
				}
			}
			assertOnlyProfilingFiles(t, out, c.OutputFiles...)
			f.mu.Lock()
			defer f.mu.Unlock()
			if len(f.started) != 1 || f.started[0].Mode == nil || f.started[0].GetMode() != tc.wantMode || f.started[0].Duration.AsDuration() != time.Second || strings.Join(f.calls, ",") != "query,start,query,query,query,stream,stop" {
				t.Fatalf("RPC requests/calls = %v %v", f.started, f.calls)
			}
		})
	}
}

func TestCollectCommandEmptyFiles(t *testing.T) {
	for _, withData := range []bool{false, true} {
		name := "empty only"
		if withData {
			name = "empty and nonempty"
		}
		t.Run(name, func(t *testing.T) {
			f, socket := startRuntimeKit(t)
			files := []commandStreamFile{{name: "empty.log"}}
			var wantBytes int64
			if withData {
				files = append(files, commandStreamFile{name: "result.json", data: []byte("trace")})
				wantBytes = 5
			}
			f.mu.Lock()
			f.streamFiles = files
			f.mu.Unlock()
			dir := realTempDir(t)
			out, termination := filepath.Join(dir, "target"), filepath.Join(dir, "termination.json")
			runCommand(t, 0, collectArgs(socket, out, termination)...)
			r := readReport(t, termination)
			if r.Phase != collector.PhaseSucceeded || len(r.Containers) != 1 {
				t.Fatalf("empty file report = %+v", r)
			}
			c := r.Containers[0]
			if c.Phase != collector.PhaseSucceeded || c.Bytes != wantBytes || len(c.OutputFiles) != len(files) {
				t.Fatalf("empty file container = %+v", c)
			}
			for i, file := range files {
				if c.OutputFiles[i] != "ns_pod_c1_"+file.name {
					t.Fatalf("empty file not finalized: %+v", c)
				}
				if data, err := os.ReadFile(filepath.Join(out, c.OutputFiles[i])); err != nil || !bytes.Equal(data, file.data) {
					t.Fatalf("raw output = %q, %v", data, err)
				}
			}
			assertOnlyProfilingFiles(t, out, c.OutputFiles...)
		})
	}
}

func TestCollectCommandOperationRetries(t *testing.T) {
	for _, retries := range []string{"0", "2"} {
		t.Run(retries, func(t *testing.T) {
			f, socket := startRuntimeKit(t)
			f.mu.Lock()
			f.queryFailures, f.stopFailures = 1, 1
			f.mu.Unlock()
			dir := realTempDir(t)
			out, termination := filepath.Join(dir, "target"), filepath.Join(dir, "termination.json")
			args := append(collectArgs(socket, out, termination), "--max-retries="+retries, "--retry-backoff=1ms", "--max-retry-backoff=2ms")
			want := 0
			if retries == "0" {
				want = 1
			}
			output := runCommand(t, want, args...)
			r := readReport(t, termination)
			f.mu.Lock()
			calls := strings.Join(f.calls, ",")
			f.mu.Unlock()
			if retries == "2" {
				if r.Phase != collector.PhaseSucceeded || calls != "query,start,query,query,query,stream,stop,stop" || !strings.Contains(output, "操作重试") {
					t.Fatalf("CLI 未传递内部重试: %+v calls=%s output=%s", r, calls, output)
				}
			} else if r.Reason != collector.ReasonQueryFailed || calls != "query,start,query,stop" || r.StopError == "" {
				t.Fatalf("0 次重试应立即失败并清理一次: %+v calls=%s", r, calls)
			}
		})
	}
}

func TestCollectCommandMultipleContainers(t *testing.T) {
	f, socket := startRuntimeKit(t)
	dir := realTempDir(t)
	out, termination := filepath.Join(dir, "target"), filepath.Join(dir, "termination.json")
	args := append(collectArgs(socket, out, termination), "--container-ids=containerd://c2,docker://c1")
	runCommand(t, 0, args...)
	r := readReport(t, termination)
	if r.Phase != collector.PhaseSucceeded || len(r.Containers) != 2 {
		t.Fatalf("report = %+v", r)
	}
	var outputFiles []string
	for i, id := range []string{"c2", "c1"} {
		container := r.Containers[i]
		if container.ContainerID != id || len(container.OutputFiles) != 2 || container.Bytes != 10 {
			t.Fatalf("misaligned container result = %+v", container)
		}
		for j, name := range []string{"lastkernels.log", "kerneltime.log"} {
			if container.OutputFiles[j] != "ns_pod_"+id+"_"+name {
				t.Fatalf("misaligned container output files = %q", container.OutputFiles)
			}
			if data, err := os.ReadFile(filepath.Join(out, container.OutputFiles[j])); err != nil || string(data) != "trace" {
				t.Fatalf("output = %q, %v", data, err)
			}
		}
		outputFiles = append(outputFiles, container.OutputFiles...)
	}
	assertOnlyProfilingFiles(t, out, outputFiles...)
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.started) != 2 || !f.completed["c1"] || !f.completed["c2"] {
		t.Fatalf("runtime IDs = %v, completion = %v", f.started, f.completed)
	}
}

func TestCollectCommandInvalidArgsWritesReport(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		args       []string
	}{
		{"flag parse", "flag provided but not defined", []string{"--not-a-flag"}},
		{"mode", "invalid --mode", []string{"--mode=wrong"}},
		{"retired mode name", "invalid --mode", []string{"--mode=lightweight"}},
		{"obsolete format", "flag provided but not defined: -format", []string{"--format=json"}},
		{"version is not a collect flag", "flag provided but not defined: -version", []string{"--version"}},
		{"duration", "invalid value", []string{"--duration=bad"}},
		{"positive timeout", "stop-timeout must be positive", []string{"--stop-timeout=0"}},
		{"output path", "absolute subdirectory", []string{"--output-dir=relative"}},
		{"container IDs", "duplicate container ID", []string{"--container-ids=c1,containerd://c1"}},
		{"empty ID", "invalid container ID", []string{"--container-ids=c1,"}},
		{"removed attempt", "flag provided but not defined: -attempt", []string{"--attempt=1"}},
		{"cleanup bool", "invalid boolean value", []string{"--cleanup-leftovers=yes"}},
		{"negative retries", "max-retries must be between", []string{"--max-retries=-1"}},
		{"excessive retries", "max-retries must be between", []string{"--max-retries=11"}},
		{"zero backoff", "retry-backoff must be positive", []string{"--retry-backoff=0"}},
		{"negative backoff", "retry-backoff must be positive", []string{"--retry-backoff=-1s"}},
		{"backoff range", "max-retry-backoff must be", []string{"--retry-backoff=3s", "--max-retry-backoff=1s"}},
		{"positionals", "unexpected positional", []string{"unexpected"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := realTempDir(t)
			termination := filepath.Join(dir, "termination.json")
			args := collectArgs("/nonexistent/rk.sock", filepath.Join(dir, "target"), termination)
			args = append(args, tc.args...)
			// Repeat after malformed flags to exercise the termination-path pre-scan.
			args = append(args, "--termination-message-path", termination)
			runCommand(t, 2, args...)
			r := readReport(t, termination)
			if r.Phase != collector.PhaseFailed || r.Reason != collector.ReasonInvalidArgs || !strings.Contains(r.Message, tc.want) {
				t.Fatalf("report = %+v", r)
			}
			if _, err := os.Lstat(filepath.Join(dir, "target")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("invalid arguments created an output directory: %v", err)
			}
			entries, err := os.ReadDir(dir)
			if err != nil || len(entries) != 1 || entries[0].Name() != filepath.Base(termination) {
				t.Fatalf("invalid arguments wrote files other than the termination report: %v, %v", entries, err)
			}
		})
	}
	termination := filepath.Join(t.TempDir(), "termination.json")
	runCommand(t, 2, "collect", "--termination-message-path="+termination)
	if r := readReport(t, termination); r.Reason != collector.ReasonInvalidArgs || !strings.Contains(r.Message, "missing required flags") {
		t.Fatalf("report = %+v", r)
	}
}

func TestNoAndObsoleteSubcommands(t *testing.T) {
	for _, subcommand := range []string{"", "serve", "export", "unknown"} {
		termination := filepath.Join(t.TempDir(), "termination.json")
		args := []string{"--termination-message-path=" + termination}
		if subcommand != "" {
			args = append([]string{subcommand}, args...)
		}
		runCommand(t, 2, args...)
		if r := readReport(t, termination); r.Reason != collector.ReasonInvalidArgs {
			t.Fatalf("report = %+v", r)
		}
	}
	if path := reportPath(nil); path != "/dev/termination-log" {
		t.Fatalf("default termination path = %q", path)
	}
}

func TestCollectHelp(t *testing.T) {
	termination := filepath.Join(t.TempDir(), "termination.json")
	output := runCommand(t, 0, "collect", "--termination-message-path="+termination, "--help")
	if !strings.Contains(output, "Usage of profiling-collector collect") || strings.Contains(output, "-format") {
		t.Fatalf("help = %s", output)
	}
	if _, err := os.Stat(termination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("help wrote report: %v", err)
	}
}

func TestCollectFailuresWriteOnlyTerminationReport(t *testing.T) {
	for _, operation := range []string{"stream", "missing file end", "stop"} {
		t.Run(operation, func(t *testing.T) {
			f, socket := startRuntimeKit(t)
			dir := realTempDir(t)
			out, termination := filepath.Join(dir, "target"), filepath.Join(dir, "termination.json")
			want := collector.ReasonStreamFailed
			f.mu.Lock()
			switch operation {
			case "stream":
				f.streamError = strings.Repeat("采集失败", 2000)
			case "missing file end":
				f.skipFileEnd = true
			case "stop":
				f.stopStatus, want = 9, collector.ReasonStopFailed
			}
			f.mu.Unlock()
			args := append(collectArgs(socket, out, termination), "--mode=light")
			runCommand(t, 1, args...)
			r := readReport(t, termination)
			if r.Phase != collector.PhaseFailed || r.Reason != want || len(r.Containers) != 1 {
				t.Fatalf("report = %+v", r)
			}
			c := r.Containers[0]
			if c.Reason != want || len(c.OutputFiles) != 2 || c.Bytes != 10 {
				t.Fatalf("container failure or named outputs missing: %+v", c)
			}
			if operation == "stream" && !strings.Contains(r.Message, "采集失败") {
				t.Fatalf("stream diagnostic lost from termination report: %+v", r)
			}
			if operation == "missing file end" && !strings.Contains(r.Message, "file_end") {
				t.Fatalf("missing file end diagnostic lost: %+v", r)
			}
			if operation == "stop" && c.StopError == "" {
				t.Fatalf("stop diagnostic lost from termination report: %+v", r)
			}
			for i, name := range []string{"lastkernels.log", "kerneltime.log"} {
				if c.OutputFiles[i] != "ns_pod_c1_"+name+".partial" {
					t.Fatalf("failed stream or Stop finalized output: %+v", c)
				}
				if data, err := os.ReadFile(filepath.Join(out, c.OutputFiles[i])); err != nil || string(data) != "trace" {
					t.Fatalf("profiling data not preserved: %q, %v", data, err)
				}
			}
			assertOnlyProfilingFiles(t, out, c.OutputFiles...)
			f.mu.Lock()
			defer f.mu.Unlock()
			if strings.Join(f.calls, ",") != "query,start,query,query,query,stream,stop" {
				t.Fatalf("failure RPC order = %v", f.calls)
			}
		})
	}
}

func TestOversizedSuccessfulTerminationReportFails(t *testing.T) {
	res := collector.Result{Phase: collector.PhaseSucceeded}
	for i := 0; i < 16; i++ {
		res.Containers = append(res.Containers, collector.ContainerResult{
			ContainerID: fmt.Sprintf("container-%d", i), Phase: collector.PhaseSucceeded, ElapsedMillis: 1234,
			OutputFiles: []string{strings.Repeat("a", 240), strings.Repeat("b", 240)},
		})
	}
	termination := filepath.Join(realTempDir(t), "termination.json")
	if code := writeReport(termination, res, exitSucceeded); code != exitFailed {
		t.Fatalf("oversized file report returned success: %d", code)
	}
	data, err := os.ReadFile(termination)
	if err != nil {
		t.Fatal(err)
	}
	report, err := collector.ParseResult(string(data))
	if err != nil || len(data) > collector.TerminationMessageLimit || report.Phase != collector.PhaseFailed || report.Reason != collector.ReasonOutputFailed || report.ElapsedMillis != 1234 || !strings.Contains(report.Message, "retained on PVC") {
		t.Fatalf("oversized output report: %+v, %v", report, err)
	}
}

func TestCollectTerminationReportFailureCannotSucceed(t *testing.T) {
	f, socket := startRuntimeKit(t)
	dir := realTempDir(t)
	out, termination := filepath.Join(dir, "target"), filepath.Join(dir, "termination.json")
	hookDone := make(chan error, 1)
	f.mu.Lock()
	f.stopHook = func() {
		if err := os.Remove(termination); err != nil {
			hookDone <- err
			return
		}
		hookDone <- os.Mkdir(termination, 0o755)
	}
	f.mu.Unlock()
	args := append(collectArgs(socket, out, termination), "--mode=full")
	output := runCommand(t, 1, args...)
	select {
	case err := <-hookDone:
		if err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatal("termination report failure hook was not called")
	}
	if !strings.Contains(output, "write termination report") {
		t.Fatalf("termination write failure not reported: %s", output)
	}
	files, err := filepath.Glob(filepath.Join(out, "ns_pod_c1_result.json"))
	if err != nil || len(files) != 1 {
		t.Fatalf("completed profiling file missing: %v, %v", files, err)
	}
	if data, err := os.ReadFile(files[0]); err != nil || string(data) != "trace" {
		t.Fatalf("profiling data not preserved: %q, %v", data, err)
	}
	assertOnlyProfilingFiles(t, out, filepath.Base(files[0]))
	f.mu.Lock()
	defer f.mu.Unlock()
	if strings.Join(f.calls, ",") != "query,start,query,query,query,stream,stop" {
		t.Fatalf("termination failure RPC order = %v", f.calls)
	}
}

func TestCollectPreflightFailuresDoNotStart(t *testing.T) {
	for _, operation := range []string{"output", "termination", "socket"} {
		t.Run(operation, func(t *testing.T) {
			f, socket := startRuntimeKit(t)
			dir := realTempDir(t)
			out, termination := filepath.Join(dir, "target"), filepath.Join(dir, "termination.json")
			switch operation {
			case "output":
				if err := os.WriteFile(out, nil, 0o600); err != nil {
					t.Fatal(err)
				}
			case "termination":
				termination = dir
			case "socket":
				socket = "/nonexistent/rk.sock"
			}
			runCommand(t, 1, collectArgs(socket, out, termination)...)
			if operation == "socket" {
				assertOnlyProfilingFiles(t, out)
			} else if operation == "termination" {
				if _, err := os.Lstat(out); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("termination preflight created an output directory: %v", err)
				}
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if len(f.started) != 0 || len(f.calls) != 0 {
				t.Fatalf("preflight started profiling: %v", f.calls)
			}
			if operation != "termination" {
				r := readReport(t, termination)
				want := collector.ReasonOutputFailed
				if operation == "socket" {
					want = collector.ReasonSocketUnavailable
				}
				if r.Reason != want {
					t.Fatalf("report = %+v", r)
				}
			}
		})
	}
}

func TestCollectCleanupFlag(t *testing.T) {
	for _, setting := range []string{"default", "false", "true"} {
		t.Run(setting, func(t *testing.T) {
			f, socket := startRuntimeKit(t)
			dir := realTempDir(t)
			out := filepath.Join(dir, "target")
			if err := os.Mkdir(out, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(out, "ns_pod_old-id_trace.json.partial"), []byte("old"), 0o600); err != nil {
				t.Fatal(err)
			}
			args := collectArgs(socket, out, filepath.Join(dir, "termination.json"))
			if setting != "false" {
				// 模拟 daemon 中残留的 PENDING 会话：默认开启的清理应先 Stop 掉它。
				f.mu.Lock()
				f.queries["c1"] = 1
				f.mu.Unlock()
			} else {
				args = append(args, "--cleanup-leftovers=false")
			}
			runCommand(t, 0, args...)
			assertOnlyProfilingFiles(t, out, "ns_pod_c1_lastkernels.log", "ns_pod_c1_kerneltime.log")
			// 默认开启：未显式传入时也会先清理；只有显式 false 才跳过。
			want := "query,stop,start,query,query,query,stream,stop"
			if setting == "false" {
				want = "start,query,query,query,stream,stop"
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if strings.Join(f.calls, ",") != want {
				t.Fatalf("cleanup RPC order = %v, want %s", f.calls, want)
			}
		})
	}
}

func TestCollectCommandKeepsLatestTargetOutputs(t *testing.T) {
	f, socket := startRuntimeKit(t)
	f.mu.Lock()
	f.streamFiles = []commandStreamFile{{name: "first.log", data: []byte("first")}, {name: "empty.log"}}
	f.streamError = "first collection failed"
	f.mu.Unlock()
	dir := realTempDir(t)
	out, termination := filepath.Join(dir, "target"), filepath.Join(dir, "termination.json")
	args := append(collectArgs(socket, out, termination), "--mode=full")
	runCommand(t, 1, args...)
	assertOnlyProfilingFiles(t, out, "ns_pod_c1_first.log.partial", "ns_pod_c1_empty.log.partial")
	kept := []string{"ns_other-pod_c1_trace.json", "other-ns_pod_c1_trace.json.partial"}
	for _, name := range kept {
		if err := os.WriteFile(filepath.Join(out, name), []byte("keep"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	f.mu.Lock()
	f.streamFiles = []commandStreamFile{{name: "result.json", data: []byte("latest")}}
	f.streamError = ""
	f.mu.Unlock()
	runCommand(t, 0, args...)
	assertOnlyProfilingFiles(t, out, append(kept, "ns_pod_c1_result.json")...)
	runCommand(t, 0, append(args, "--container-ids=containerd://new-id")...)
	r := readReport(t, termination)
	if r.Phase != collector.PhaseSucceeded || len(r.Containers) != 1 || len(r.Containers[0].OutputFiles) != 1 || r.Containers[0].OutputFiles[0] != "ns_pod_new-id_result.json" {
		t.Fatalf("latest output report = %+v", r)
	}
	assertOnlyProfilingFiles(t, out, append(kept, "ns_pod_new-id_result.json")...)
	if data, err := os.ReadFile(filepath.Join(out, "ns_pod_new-id_result.json")); err != nil || string(data) != "latest" {
		t.Fatalf("latest output = %q, %v", data, err)
	}
	for _, name := range kept {
		if data, err := os.ReadFile(filepath.Join(out, name)); err != nil || string(data) != "keep" {
			t.Fatalf("other target output %q changed: %q, %v", name, data, err)
		}
	}
}

func TestCollectSignalsStopRuntimeKit(t *testing.T) {
	for _, sig := range []os.Signal{syscall.SIGTERM, syscall.SIGINT} {
		t.Run(sig.String(), func(t *testing.T) {
			f, socket := startRuntimeKit(t)
			f.mu.Lock()
			f.blockStream = true
			f.mu.Unlock()
			dir := realTempDir(t)
			out, termination := filepath.Join(dir, "target"), filepath.Join(dir, "termination.json")
			args := append(collectArgs(socket, out, termination), "--duration=1m", "--mode=full")
			cmd, output := command(t, args...)
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			t.Cleanup(func() {
				if cmd.ProcessState == nil {
					_ = cmd.Process.Kill()
					<-done
				}
			})
			select {
			case <-f.streamStarted:
			case <-time.After(10 * time.Second):
				t.Fatal("stream never started")
			}
			// Send only after the client has persisted the first chunk, not just
			// after the server has queued it on the gRPC transport.
			ticker := time.NewTicker(time.Millisecond)
			defer ticker.Stop()
			deadline := time.NewTimer(2 * time.Second)
			defer deadline.Stop()
			for {
				data, err := os.ReadFile(filepath.Join(out, "ns_pod_c1_result.json.partial"))
				if err == nil && string(data) == "trace" {
					break
				}
				select {
				case <-ticker.C:
				case <-deadline.C:
					t.Fatalf("first stream chunk not persisted: %q, %v", data, err)
				}
			}
			if err := cmd.Process.Signal(sig); err != nil {
				t.Fatal(err)
			}
			select {
			case <-f.stopped:
			case <-time.After(2 * time.Second):
				t.Fatal("signal did not promptly stop Runtime Kit")
			}
			select {
			case <-done:
				if cmd.ProcessState.ExitCode() != 1 {
					t.Fatalf("signal exit = %d\n%s", cmd.ProcessState.ExitCode(), output.String())
				}
			case <-time.After(5 * time.Second):
				t.Fatal("collector did not exit after signal")
			}
			r := readReport(t, termination)
			if r.Phase != collector.PhaseFailed || r.Reason != collector.ReasonInterrupted || len(r.Containers) != 1 || len(r.Containers[0].OutputFiles) != 1 || r.Containers[0].OutputFiles[0] != "ns_pod_c1_result.json.partial" {
				t.Fatalf("signal report = %+v", r)
			}
			assertOnlyProfilingFiles(t, out, r.Containers[0].OutputFiles...)
		})
	}
}

func TestVersionCommand(t *testing.T) {
	for _, arg := range []string{"version", "--version"} {
		t.Run(arg, func(t *testing.T) {
			termination := filepath.Join(t.TempDir(), "termination.json")
			cmd, stdout := command(t, arg, "--termination-message-path="+termination)
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			if err := cmd.Run(); err != nil {
				t.Fatalf("version command: %v: %s", err, stderr.String())
			}
			if stdout.String() != "dev\n" || stderr.Len() != 0 {
				t.Fatalf("version stdout=%q stderr=%q", stdout.String(), stderr.String())
			}
			if _, err := os.Stat(termination); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("version wrote termination report: %v", err)
			}
		})
	}
}

func TestBuildVersionInjection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "profiling-collector")
	build := exec.CommandContext(ctx, "go", "build", "-mod=readonly", "-ldflags=-X main.BuildVersion=v1.2.3-test", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build with version: %v\n%s", err, output)
	}
	for _, arg := range []string{"version", "--version"} {
		cmd := exec.CommandContext(ctx, binary, arg)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		output, err := cmd.Output()
		if err != nil || string(output) != "v1.2.3-test\n" || stderr.Len() != 0 {
			t.Fatalf("injected %s: stdout=%q stderr=%q err=%v", arg, output, stderr.String(), err)
		}
	}
}
