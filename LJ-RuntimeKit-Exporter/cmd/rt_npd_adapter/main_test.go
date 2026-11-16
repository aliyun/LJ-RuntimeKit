package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xdmgrpc "github.com/lj-runtimekit/exporter/pkg/grpc/xdm"
	"github.com/lj-runtimekit/exporter/pkg/npdadapter/exitcode"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type healthServer struct {
	xdmgrpc.UnimplementedXpuDeviceManagerServiceServer
	fetch      func(context.Context) (*xdmgrpc.GetAllHealthResponse, error)
	categories func(context.Context) (*xdmgrpc.GetCategoriesResponse, error)
}

func (s *healthServer) GetAllHealth(ctx context.Context, _ *xdmgrpc.GetAllHealthRequest) (*xdmgrpc.GetAllHealthResponse, error) {
	return s.fetch(ctx)
}

func (s *healthServer) GetCategories(ctx context.Context, _ *xdmgrpc.GetCategoriesRequest) (*xdmgrpc.GetCategoriesResponse, error) {
	if s.categories == nil {
		return nil, status.Error(codes.Unimplemented, "category catalog unavailable")
	}
	return s.categories(ctx)
}

func startServer(t *testing.T, fetch func(context.Context) (*xdmgrpc.GetAllHealthResponse, error)) string {
	t.Helper()
	return startHealthServer(t, &healthServer{fetch: fetch})
}

func startHealthServer(t *testing.T, handler *healthServer) string {
	t.Helper()
	listener, err := net.Listen("unix", filepath.Join(t.TempDir(), "health.sock"))
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	xdmgrpc.RegisterXpuDeviceManagerServiceServer(server, handler)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	return listener.Addr().String()
}

// Isolate health-only tests from configuration generation and its filesystem effects.
func runHealth(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	return run(ctx, append([]string{"--config-file=", "--cache-file="}, args...), stdout, stderr)
}

func TestRunValidatesFlags(t *testing.T) {
	for _, args := range [][]string{
		nil, {"--bogus"},
		{"--endpoint", "/run/health.sock"},
		{"--endpoint", "relative.sock", "--category", "x"},
		{"--endpoint", "", "--category", "x"},
		{"--endpoint", "  ", "--category", "x"},
		{"--endpoint", "unix://", "--category", "x"},
		{"--endpoint", "unix://relative.sock", "--category", "x"},
		{"--endpoint", "unix://localhost/run/health.sock", "--category", "x"},
		{"--endpoint", "127.0.0.1:1", "--category", "x"},
		{"--endpoint", "[::1]:50051", "--category", "x"},
		{"--endpoint", "dns:///localhost:50051", "--category", "x"},
		{"--endpoint", "tcp://127.0.0.1:50051", "--category", "x"},
		{"--endpoint", "/run/health.sock", "--category", "x", "--timeout", "0s"},
		{"--endpoint", "/run/health.sock", "--category", "x", "--cache-ttl", "-1s"},
		{"--endpoint", "/run/health.sock", "--category", "x", "--max-output-length", "0"},
		{"--endpoint", "/run/health.sock", "--category", "x", "extra"},
	} {
		var out, errOut bytes.Buffer
		if code := runHealth(context.Background(), args, &out, &errOut); code != exitcode.Error || out.Len() != 0 || errOut.Len() == 0 {
			t.Errorf("run(%v) = %d, stdout=%q, stderr=%q", args, code, out.String(), errOut.String())
		}
	}
	var out, errOut bytes.Buffer
	if code := runHealth(context.Background(), []string{"--help"}, &out, &errOut); code != 0 || out.Len() != 0 || errOut.Len() == 0 {
		t.Fatalf("help = %d, stdout=%q, stderr=%q", code, out.String(), errOut.String())
	}
}

func TestRunArgumentDiagnostics(t *testing.T) {
	for _, tt := range []struct {
		name string
		args []string
		want string
	}{
		{"endpoint required", []string{"--endpoint="}, "--endpoint is required"},
		{"invalid endpoint", []string{"--endpoint=relative.sock"}, "--endpoint must be an absolute Unix socket path"},
		{"endpoint NUL", []string{"--endpoint=/run/health\x00.sock"}, "without NUL bytes"},
		{"category required", []string{"--category=  "}, "--category is required for health checks"},
		{"timeout zero", []string{"--timeout=0s"}, "--timeout must be positive"},
		{"timeout negative", []string{"--timeout=-1s"}, "--timeout must be positive"},
		{"timeout format", []string{"--timeout=invalid"}, "timeout"},
		{"timeout missing", []string{"--timeout"}, "timeout"},
		{"cache TTL negative", []string{"--cache-ttl=-1s"}, "--cache-ttl must be nonnegative"},
		{"output zero", []string{"--max-output-length=0"}, "--max-output-length must be positive"},
		{"output format", []string{"--max-output-length=invalid"}, "max-output-length"},
		{"relative cache", []string{"--cache-file=relative.cache"}, "--cache-file must be an absolute path"},
		{"cache NUL", []string{"--cache-file=/run/cache\x00"}, "without NUL bytes"},
		{"cache endpoint conflict", []string{"--cache-file=/run/health.sock"}, "paths must be distinct"},
		{"normalized cache conflict", []string{"--endpoint=unix:///run/health.sock", "--cache-file=/run/./health.sock"}, "paths must be distinct"},
		{"cache lock endpoint conflict", []string{"--endpoint=/run/cache.lock", "--cache-file=/run/cache"}, "paths must be distinct"},
		{"generation destination", []string{"--generate-config"}, "--config-file is required with --generate-config"},
		{"generation bool", []string{"--generate-config=invalid"}, "generate-config"},
		{"unknown flag", []string{"--bogus"}, "bogus"},
		{"positional argument", []string{"extra"}, "positional arguments are not supported"},
		{"help is not a subcommand", []string{"help"}, "positional arguments are not supported"},
		{"argument separator", []string{"--", "extra"}, "positional arguments are not supported"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			args := append([]string{"--endpoint=/run/health.sock", "--category=dynamic"}, tt.args...)
			var out, errOut bytes.Buffer
			code := runHealth(context.Background(), args, &out, &errOut)
			if code != exitcode.Error || out.Len() != 0 || !strings.Contains(errOut.String(), tt.want) {
				t.Fatalf("code=%d, stdout=%q, stderr=%q; want diagnostic %q", code, out.String(), errOut.String(), tt.want)
			}
		})
	}
}

func TestRunHelp(t *testing.T) {
	for _, arg := range []string{"--help", "-h", "-help"} {
		t.Run(arg, func(t *testing.T) {
			var out, errOut bytes.Buffer
			if code := run(context.Background(), []string{arg}, &out, &errOut); code != exitcode.Healthy || out.Len() != 0 {
				t.Fatalf("help = %d, stdout=%q, stderr=%q", code, out.String(), errOut.String())
			}
			for _, text := range []string{
				"rt_npd_adapter --category CATEGORY",
				"rt_npd_adapter --generate-config [options]", "Exit codes:",
				"AMPERF_GRPC_PATH",
				"--endpoint", "--category", "--timeout", "--cache-file", "--cache-ttl",
				"--max-output-length", "--generate-config", "--config-file",
				"--invoke-interval", "--plugin-timeout", "--concurrency",
			} {
				if !strings.Contains(errOut.String(), text) {
					t.Errorf("help does not contain %q: %s", text, errOut.String())
				}
			}
		})
	}
}

func TestRunPreservesParentCancellation(t *testing.T) {
	var calls atomic.Int32
	endpoint := startServer(t, func(context.Context) (*xdmgrpc.GetAllHealthResponse, error) {
		calls.Add(1)
		return nil, status.Error(codes.Unavailable, "unexpected query")
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out, errOut bytes.Buffer
	code := runHealth(ctx, []string{"--endpoint", endpoint, "--category=dynamic"}, &out, &errOut)
	if code != exitcode.Unknown || out.String() != "health service unavailable" || !strings.Contains(errOut.String(), "context canceled") || calls.Load() != 0 {
		t.Fatalf("code=%d, stdout=%q, stderr=%q, calls=%d", code, out.String(), errOut.String(), calls.Load())
	}
}

func TestRunEndpointFromEnvironment(t *testing.T) {
	endpoint := startServer(t, func(context.Context) (*xdmgrpc.GetAllHealthResponse, error) {
		return &xdmgrpc.GetAllHealthResponse{Categories: []*xdmgrpc.CategoryHealth{
			{Category: "dynamic", Status: xdmgrpc.HealthStatus_HEALTHY},
		}}, nil
	})
	t.Setenv("AMPERF_GRPC_PATH", endpoint)
	var out, errOut bytes.Buffer
	code := runHealth(context.Background(), []string{"--category=dynamic"}, &out, &errOut)
	if code != exitcode.Healthy || out.Len() != 0 || errOut.Len() != 0 {
		t.Fatalf("code=%d, stdout=%q, stderr=%q", code, out.String(), errOut.String())
	}
}

func TestRunUnixEndpointFormsShareCache(t *testing.T) {
	var calls atomic.Int32
	endpoint := startServer(t, func(context.Context) (*xdmgrpc.GetAllHealthResponse, error) {
		calls.Add(1)
		return &xdmgrpc.GetAllHealthResponse{Categories: []*xdmgrpc.CategoryHealth{
			{Category: "x", Status: xdmgrpc.HealthStatus_HEALTHY},
		}}, nil
	})
	path := filepath.Join(t.TempDir(), "health.cache")
	for _, input := range []string{endpoint, "unix://" + endpoint, "  unix://" + endpoint + "  "} {
		var out, errOut bytes.Buffer
		code := runHealth(context.Background(), []string{"--endpoint", input, "--category", "x", "--cache-file", path, "--cache-ttl", "1m"}, &out, &errOut)
		if code != exitcode.Healthy || out.Len() != 0 || errOut.Len() != 0 {
			t.Fatalf("endpoint %q: code=%d, stdout=%q, stderr=%q", input, code, out.String(), errOut.String())
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("equivalent Unix endpoints must share the cache: calls=%d", calls.Load())
	}
}

func TestRunCacheEndpointConflict(t *testing.T) {
	var calls atomic.Int32
	endpoint := startHealthServer(t, &healthServer{
		fetch: func(context.Context) (*xdmgrpc.GetAllHealthResponse, error) {
			calls.Add(1)
			return &xdmgrpc.GetAllHealthResponse{Categories: []*xdmgrpc.CategoryHealth{
				{Category: "x", Status: xdmgrpc.HealthStatus_HEALTHY},
			}}, nil
		},
		categories: func(context.Context) (*xdmgrpc.GetCategoriesResponse, error) {
			calls.Add(1)
			return configCatalog(), nil
		},
	})
	before, err := os.Lstat(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"health only", "automatic config", "explicit config"} {
		t.Run(mode, func(t *testing.T) {
			configFile := ""
			if mode != "health only" {
				configFile = filepath.Join(t.TempDir(), "monitor.json")
			}
			for _, input := range []string{endpoint, "unix://" + endpoint} {
				args := []string{"--endpoint=" + input, "--category=x", "--cache-file=" + endpoint, "--config-file=" + configFile}
				if mode == "explicit config" {
					args = append(args, "--generate-config")
				}
				var out, errOut bytes.Buffer
				code := run(context.Background(), args, &out, &errOut)
				if code != exitcode.Error || out.Len() != 0 || !strings.Contains(errOut.String(), "paths must be distinct") {
					t.Fatalf("code=%d, stdout=%q, stderr=%q", code, out.String(), errOut.String())
				}
			}
		})
	}
	after, err := os.Lstat(endpoint)
	if err != nil || !os.SameFile(before, after) || after.Mode()&os.ModeSocket == 0 || calls.Load() != 0 {
		t.Fatalf("invalid arguments reached the service or modified its socket: %v, %v, calls=%d", after, err, calls.Load())
	}
	if _, err := os.Lstat(endpoint + ".lock"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid arguments created a cache lock: %v", err)
	}
	var out, errOut bytes.Buffer
	code := runHealth(context.Background(), []string{"--endpoint=" + endpoint, "--category=x"}, &out, &errOut)
	if code != exitcode.Healthy || out.Len() != 0 || errOut.Len() != 0 || calls.Load() != 1 {
		t.Fatalf("service no longer reachable: code=%d, stdout=%q, stderr=%q", code, out.String(), errOut.String())
	}
}

func TestRunMissingUnixSocket(t *testing.T) {
	endpoint := filepath.Join(t.TempDir(), "missing.sock")
	var out, errOut bytes.Buffer
	start := time.Now()
	code := runHealth(context.Background(), []string{"--endpoint", endpoint, "--category", "x", "--cache-file=", "--timeout", "50ms"}, &out, &errOut)
	if code != exitcode.Unknown || out.String() != "health service unavailable" || errOut.Len() == 0 || time.Since(start) > time.Second {
		t.Fatalf("code=%d, stdout=%q, stderr=%q", code, out.String(), errOut.String())
	}
}

func TestRunOutputAndCache(t *testing.T) {
	var calls atomic.Int32
	endpoint := startServer(t, func(context.Context) (*xdmgrpc.GetAllHealthResponse, error) {
		calls.Add(1)
		return &xdmgrpc.GetAllHealthResponse{
			NodeName: "node-a", SampledAtMs: time.Now().UnixMilli(), TtlMs: 60000,
			Categories: []*xdmgrpc.CategoryHealth{
				{Category: "healthy", Status: xdmgrpc.HealthStatus_HEALTHY, Reason: "ignored"},
				{Category: "unhealthy", Status: xdmgrpc.HealthStatus_UNHEALTHY, Reason: strings.Repeat("€€€€", 30)},
				{Category: "unknown", Status: xdmgrpc.HealthStatus_UNKNOWN, Reason: "unavailable"},
			},
		}, nil
	})
	path := filepath.Join(t.TempDir(), "health.cache")
	for _, tt := range []struct {
		category string
		code     int
		message  string
	}{
		{"healthy", 0, ""},
		{"unhealthy", 1, strings.Repeat("€€€€", 13) + "€"},
		{"unknown", 2, "unavailable"},
		{"missing", 2, "category not reported"},
	} {
		var out, errOut bytes.Buffer
		code := runHealth(context.Background(), []string{"--endpoint", endpoint, "--category", tt.category, "--cache-file", path, "--cache-ttl", "1m"}, &out, &errOut)
		if code != tt.code || errOut.Len() != 0 || len(out.String()) > 160 {
			t.Fatalf("%s: code=%d, stdout=%q, stderr=%q", tt.category, code, out.String(), errOut.String())
		}
		if out.String() != tt.message {
			t.Fatalf("%s: stdout=%q, want %q", tt.category, out.String(), tt.message)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("cache did not share the full snapshot: calls=%d", calls.Load())
	}
}

func TestRunOldSnapshotFromCache(t *testing.T) {
	var calls atomic.Int32
	endpoint := startServer(t, func(context.Context) (*xdmgrpc.GetAllHealthResponse, error) {
		calls.Add(1)
		return &xdmgrpc.GetAllHealthResponse{
			SampledAtMs: 1, TtlMs: 1,
			Categories: []*xdmgrpc.CategoryHealth{
				{Category: "healthy", Status: xdmgrpc.HealthStatus_HEALTHY},
				{Category: "unhealthy", Status: xdmgrpc.HealthStatus_UNHEALTHY, Reason: "device failure"},
			},
		}, nil
	})
	path := filepath.Join(t.TempDir(), "health.cache")
	for _, tt := range []struct {
		category string
		code     int
		message  string
	}{
		{"healthy", exitcode.Healthy, ""},
		{"unhealthy", exitcode.Unhealthy, "device failure"},
		{"missing", exitcode.Unknown, "category not reported"},
	} {
		var out, errOut bytes.Buffer
		code := runHealth(context.Background(), []string{"--endpoint", endpoint, "--category", tt.category, "--cache-file", path, "--cache-ttl", "1m"}, &out, &errOut)
		if code != tt.code || out.String() != tt.message || errOut.Len() != 0 {
			t.Fatalf("snapshot age overrode the reported status: code=%d, stdout=%q, stderr=%q", code, out.String(), errOut.String())
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("cache did not share the full snapshot: calls=%d", calls.Load())
	}
}

func TestRunExpiredCacheUnknownAndRecovery(t *testing.T) {
	for _, state := range []xdmgrpc.HealthStatus{xdmgrpc.HealthStatus_HEALTHY, xdmgrpc.HealthStatus_UNHEALTHY, xdmgrpc.HealthStatus_UNKNOWN} {
		t.Run(state.String(), func(t *testing.T) {
			var failure atomic.Int32
			endpoint := startServer(t, func(ctx context.Context) (*xdmgrpc.GetAllHealthResponse, error) {
				switch failure.Load() {
				case 1:
					return nil, status.Error(codes.Unavailable, "backend diagnostic detail")
				case 2:
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return &xdmgrpc.GetAllHealthResponse{
					SampledAtMs: 1, TtlMs: 1,
					Categories: []*xdmgrpc.CategoryHealth{{Category: "x", Status: state, Reason: "reported status"}},
				}, nil
			})
			path := filepath.Join(t.TempDir(), "health.cache")
			args := []string{"--endpoint=" + endpoint, "--category=x", "--cache-file=" + path, "--cache-ttl=1h", "--timeout=100ms"}
			var out, errOut bytes.Buffer
			wantCode, wantMessage := int(state)-1, "reported status"
			if state == xdmgrpc.HealthStatus_HEALTHY {
				wantMessage = ""
			}
			if code := runHealth(context.Background(), args, &out, &errOut); code != wantCode || out.String() != wantMessage || errOut.Len() != 0 {
				t.Fatalf("initial query: code=%d, stdout=%q, stderr=%q", code, out.String(), errOut.String())
			}
			failure.Store(1)
			out.Reset()
			errOut.Reset()
			if code := runHealth(context.Background(), args, &out, &errOut); code != wantCode || out.String() != wantMessage || errOut.Len() != 0 {
				t.Fatalf("valid cache was not reused: code=%d, stdout=%q, stderr=%q", code, out.String(), errOut.String())
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			beforeInfo, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			args = append(args, "--cache-ttl=1ns")
			for _, mode := range []int32{1, 2} {
				failure.Store(mode)
				out.Reset()
				errOut.Reset()
				start := time.Now()
				code := runHealth(context.Background(), args, &out, &errOut)
				if code != exitcode.Unknown || out.String() != "health service unavailable" || errOut.Len() == 0 || time.Since(start) > time.Second {
					t.Fatalf("expired cache: code=%d, stdout=%q, stderr=%q", code, out.String(), errOut.String())
				}
				after, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(before, after) {
					t.Fatalf("failed query cleared or changed the cache: %v", err)
				}
				info, err := os.Stat(path)
				if err != nil || !os.SameFile(beforeInfo, info) || !info.ModTime().Equal(beforeInfo.ModTime()) {
					t.Fatalf("failed query updated the cache: %v, %v", info, err)
				}
			}
			failure.Store(1)
			out.Reset()
			errOut.Reset()
			shortArgs := append(append([]string(nil), args...), "--max-output-length=8")
			if code := runHealth(context.Background(), shortArgs, &out, &errOut); code != exitcode.Unknown || out.String() != "health s" || !strings.Contains(errOut.String(), "backend diagnostic detail") {
				t.Fatalf("unknown output truncation: code=%d, stdout=%q, stderr=%q", code, out.String(), errOut.String())
			}
			out.Reset()
			errOut.Reset()
			disabledArgs := append(append([]string(nil), args...), "--cache-ttl=0s")
			if code := runHealth(context.Background(), disabledArgs, &out, &errOut); code != exitcode.Unknown || out.String() != "health service unavailable" {
				t.Fatalf("disabled cache changed failure mapping: code=%d, stdout=%q", code, out.String())
			}
			failure.Store(0)
			out.Reset()
			errOut.Reset()
			if code := runHealth(context.Background(), args, &out, &errOut); code != wantCode || out.String() != wantMessage || errOut.Len() != 0 {
				t.Fatalf("recovery: code=%d, stdout=%q, stderr=%q", code, out.String(), errOut.String())
			}
			info, err := os.Stat(path)
			if err != nil || os.SameFile(beforeInfo, info) {
				t.Fatalf("recovery did not replace the cache: %v, %v", info, err)
			}
		})
	}
}

func TestRunBackendFailure(t *testing.T) {
	for _, mode := range []string{"rpc error", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			endpoint := startServer(t, func(ctx context.Context) (*xdmgrpc.GetAllHealthResponse, error) {
				if mode == "timeout" {
					<-ctx.Done()
				}
				return nil, status.Error(codes.Unavailable, "backend diagnostic detail")
			})
			var out, errOut bytes.Buffer
			start := time.Now()
			code := runHealth(context.Background(), []string{"--endpoint", endpoint, "--category", "x", "--cache-file=", "--timeout", "50ms", "--max-output-length", "8"}, &out, &errOut)
			if code != exitcode.Unknown || out.String() != "health s" || errOut.Len() == 0 || time.Since(start) > time.Second {
				t.Fatalf("code=%d, stdout=%q, stderr=%q", code, out.String(), errOut.String())
			}
		})
	}
}

func TestAdapterUnavailableProcess(t *testing.T) {
	endpoint := startServer(t, func(context.Context) (*xdmgrpc.GetAllHealthResponse, error) {
		return nil, status.Error(codes.Unavailable, "backend unavailable")
	})
	path := filepath.Join(t.TempDir(), "health.cache")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := 0; i < 2; i++ {
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAdapterProcess$", "--",
			"--endpoint="+endpoint, "--category=x", "--config-file=", "--cache-file="+path)
		cmd.Env = append(os.Environ(), "RT_TEST_ADAPTER_PROCESS=1")
		var out, errOut bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &errOut
		var exitErr *exec.ExitError
		if err := cmd.Run(); !errors.As(err, &exitErr) {
			t.Fatalf("expected an unknown process exit: %v", err)
		}
		if cmd.ProcessState.ExitCode() != exitcode.Unknown || out.String() != "health service unavailable" || !strings.Contains(errOut.String(), "backend unavailable") {
			t.Fatalf("code=%d, stdout=%q, stderr=%q", cmd.ProcessState.ExitCode(), out.String(), errOut.String())
		}
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("failed query created a cache file: %v", err)
		}
	}
}

func TestRunCacheFailureOnlyLogsToStderr(t *testing.T) {
	endpoint := startServer(t, func(context.Context) (*xdmgrpc.GetAllHealthResponse, error) {
		return &xdmgrpc.GetAllHealthResponse{Categories: []*xdmgrpc.CategoryHealth{{Category: "x", Status: xdmgrpc.HealthStatus_UNHEALTHY, Reason: "€€€€"}}}, nil
	})
	path := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	code := runHealth(context.Background(), []string{"--endpoint", endpoint, "--category", "x", "--cache-file", filepath.Join(path, "cache"), "--max-output-length", "7"}, &out, &errOut)
	if code != 1 || out.String() != "€€" || errOut.Len() == 0 {
		t.Fatalf("code=%d, stdout=%q, stderr=%q", code, out.String(), errOut.String())
	}
}

// Run main in the test executable to verify process exit codes and the shared cache.
func TestAdapterProcess(t *testing.T) {
	if os.Getenv("RT_TEST_ADAPTER_PROCESS") != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg == "--" {
			os.Args = append([]string{os.Args[0]}, os.Args[i+1:]...)
			main()
			return
		}
	}
	t.Fatal("test child process is missing the argument separator")
}

func TestConcurrentAdapterProcesses(t *testing.T) {
	var calls atomic.Int32
	endpoint := startServer(t, func(context.Context) (*xdmgrpc.GetAllHealthResponse, error) {
		calls.Add(1)
		time.Sleep(30 * time.Millisecond)
		return &xdmgrpc.GetAllHealthResponse{Categories: []*xdmgrpc.CategoryHealth{
			{Category: "a", Status: xdmgrpc.HealthStatus_HEALTHY},
			{Category: "b", Status: xdmgrpc.HealthStatus_UNHEALTHY, Reason: "failure"},
		}}, nil
	})
	path := filepath.Join(t.TempDir(), "health.cache")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 9; i++ {
		category := []string{"a", "b", "missing"}[i%3]
		wantCode := i % 3
		wantMessage := []string{"", "failure", "category not reported"}[i%3]
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAdapterProcess$", "--", "--config-file=", "--endpoint", endpoint, "--category", category, "--cache-file", path, "--cache-ttl", "10s")
			cmd.Env = append(os.Environ(), "RT_TEST_ADAPTER_PROCESS=1")
			var out, errOut bytes.Buffer
			cmd.Stdout, cmd.Stderr = &out, &errOut
			err := cmd.Run()
			var exitErr *exec.ExitError
			if err != nil && !errors.As(err, &exitErr) {
				t.Errorf("failed to start adapter: %v", err)
				return
			}
			if cmd.ProcessState.ExitCode() != wantCode || out.String() != wantMessage || errOut.Len() != 0 {
				t.Errorf("%s: code=%d, stdout=%q, stderr=%q", category, cmd.ProcessState.ExitCode(), out.String(), errOut.String())
			}
		}()
	}
	close(start)
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("concurrent processes made %d RPC calls, want 1", calls.Load())
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestEmitWriteError(t *testing.T) {
	var errOut bytes.Buffer
	if code := emit(failingWriter{}, &errOut, exitcode.Unhealthy, "failure", 160); code != exitcode.Error || errOut.Len() == 0 {
		t.Fatalf("write error was not reported: code=%d, stderr=%q", code, errOut.String())
	}
}
