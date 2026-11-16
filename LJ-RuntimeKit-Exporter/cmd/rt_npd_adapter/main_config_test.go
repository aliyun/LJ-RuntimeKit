package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xdmgrpc "github.com/lj-runtimekit/exporter/pkg/grpc/xdm"
	npdconfig "github.com/lj-runtimekit/exporter/pkg/npdadapter/config"
	"github.com/urfave/cli/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func parseConfig(t *testing.T, args []string) *Config {
	t.Helper()
	app := newApp(io.Discard)
	var config *Config
	app.Action = func(c *cli.Context) (err error) {
		config, err = contextToConfig(c)
		return err
	}
	if err := app.Run(append([]string{app.Name}, args...)); err != nil {
		t.Fatal(err)
	}
	if config == nil {
		t.Fatal("configuration was not parsed")
	}
	return config
}

func TestContextToConfigDefaults(t *testing.T) {
	t.Setenv("AMPERF_GRPC_PATH", "")
	if err := os.Unsetenv("AMPERF_GRPC_PATH"); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	got := parseConfig(t, []string{"--category=dynamic"})
	want := Config{
		Category: "dynamic",
		Options: npdconfig.Options{
			Executable: executable, Endpoint: "/run/amp/eKitC/xdm/sockets/dp2mgr.sock",
			File:      filepath.Join(filepath.Dir(executable), npdconfig.FileName),
			CacheFile: filepath.Join(filepath.Dir(executable), "health.cache"),
			Timeout:   3 * time.Second, CacheTTL: 10 * time.Second, MaxOutput: 160,
			InvokeInterval: 30 * time.Second, PluginTimeout: 5 * time.Second, Concurrency: 3,
		},
	}
	if *got != want {
		t.Fatalf("config = %+v, want %+v", *got, want)
	}
}

func TestContextToConfigEndpointPrecedence(t *testing.T) {
	for _, tt := range []struct {
		name    string
		env     string
		args    []string
		want    string
		wantErr string
	}{
		{"environment", "/run/environment.sock", nil, "/run/environment.sock", ""},
		{"normalized environment", "  unix:///run/environment.sock  ", nil, "/run/environment.sock", ""},
		{"explicit override", "/run/environment.sock", []string{"--endpoint=/run/explicit.sock"}, "/run/explicit.sock", ""},
		{"explicit overrides empty environment", "", []string{"--endpoint=/run/explicit.sock"}, "/run/explicit.sock", ""},
		{"empty environment", "", nil, "", "--endpoint is required"},
		{"empty explicit value", "/run/environment.sock", []string{"--endpoint="}, "", "--endpoint is required"},
		{"invalid environment", "relative.sock", nil, "", "--endpoint must be an absolute Unix socket path"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("AMPERF_GRPC_PATH", tt.env)
			app := newApp(io.Discard)
			var config *Config
			app.Action = func(c *cli.Context) (err error) {
				config, err = contextToConfig(c)
				return err
			}
			err := app.Run(append([]string{app.Name, "--category=dynamic"}, tt.args...))
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if config == nil || config.Options.Endpoint != tt.want {
				t.Fatalf("config = %+v, want endpoint %q", config, tt.want)
			}
		})
	}
}

func TestContextToConfigOverrides(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	flags := [][2]string{
		{"generate-config", "true"}, {"endpoint", "  unix:///run/custom.sock  "},
		{"category", "custom.category"}, {"timeout", "250ms"}, {"max-output-length", "64"},
		{"cache-file", "/opt/custom npd/snapshot"}, {"cache-ttl", "100ms"},
		{"config-file", "/opt/custom npd/monitor.json"}, {"invoke-interval", "2s"},
		{"plugin-timeout", "500ms"}, {"concurrency", "4"},
	}
	want := Config{
		GenerateConfig: true, Category: "custom.category",
		Options: npdconfig.Options{
			Executable: executable, Endpoint: "/run/custom.sock",
			File: "/opt/custom npd/monitor.json", CacheFile: "/opt/custom npd/snapshot",
			Timeout: 250 * time.Millisecond, CacheTTL: 100 * time.Millisecond, MaxOutput: 64,
			InvokeInterval: 2 * time.Second, PluginTimeout: 500 * time.Millisecond, Concurrency: 4,
		},
	}
	for _, form := range []string{"equals", "separate"} {
		t.Run(form, func(t *testing.T) {
			var args []string
			for _, flag := range flags {
				if form == "equals" || flag[0] == "generate-config" {
					args = append(args, "--"+flag[0]+"="+flag[1])
				} else {
					args = append(args, "--"+flag[0], flag[1])
				}
			}
			if got := parseConfig(t, args); *got != want {
				t.Fatalf("config = %+v, want %+v", *got, want)
			}
		})
	}
}

func TestContextToConfigModes(t *testing.T) {
	t.Run("generation needs no category", func(t *testing.T) {
		got := parseConfig(t, []string{"--endpoint=/run/health.sock", "--generate-config"})
		if !got.GenerateConfig || got.Category != "" {
			t.Fatalf("unexpected generation mode: %+v", *got)
		}
	})
	t.Run("disabled files and unused monitor options", func(t *testing.T) {
		got := parseConfig(t, []string{
			"--endpoint=/run/health.sock", "--category=dynamic", "--generate-config=false",
			"--config-file=", "--cache-file=", "--cache-ttl=0s",
			"--invoke-interval=0s", "--plugin-timeout=0s", "--concurrency=0",
		})
		if got.GenerateConfig || got.Options.File != "" || got.Options.CacheFile != "" || got.Options.CacheTTL != 0 {
			t.Fatalf("explicit empty values were not preserved: %+v", *got)
		}
	})
}

func configCatalog() *xdmgrpc.GetCategoriesResponse {
	return &xdmgrpc.GetCategoriesResponse{Categories: []*xdmgrpc.CategoryInfo{
		{Category: "dynamic", Condition: "DeviceProblem", OkReason: "DeviceHealthy", OkMessage: "device healthy", BadReason: "DeviceUnhealthy"},
	}}
}

func TestRunGenerateConfig(t *testing.T) {
	var calls atomic.Int32
	endpoint := startHealthServer(t, &healthServer{
		fetch: func(context.Context) (*xdmgrpc.GetAllHealthResponse, error) {
			t.Error("configuration generation must not query health")
			return nil, nil
		},
		categories: func(context.Context) (*xdmgrpc.GetCategoriesResponse, error) {
			calls.Add(1)
			return configCatalog(), nil
		},
	})
	t.Setenv("AMPERF_GRPC_PATH", endpoint)
	path := filepath.Join(t.TempDir(), npdconfig.FileName)
	for i := 0; i < 2; i++ {
		var out, errOut bytes.Buffer
		code := run(context.Background(), []string{"--generate-config", "--config-file", path}, &out, &errOut)
		if code != 0 || out.Len() != 0 || errOut.Len() != 0 {
			t.Fatalf("code=%d, stdout=%q, stderr=%q", code, out.String(), errOut.String())
		}
	}
	if calls.Load() != 2 {
		t.Fatal("explicit generation must refresh the catalog")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		PluginConfig struct {
			InvokeInterval string `json:"invoke_interval"`
		}
		Rules []struct {
			Path string
			Args []string
		}
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	executable, _ := os.Executable()
	if len(doc.Rules) != 1 || doc.Rules[0].Path != executable || !strings.Contains(string(data), "DeviceProblem") {
		t.Fatalf("incorrect generated config: %s", data)
	}
	wantCache := "--cache-file=" + filepath.Join(filepath.Dir(executable), "health.cache")
	args := "\n" + strings.Join(doc.Rules[0].Args, "\n") + "\n"
	for _, want := range []string{wantCache, "--cache-ttl=10s", "--invoke-interval=30s"} {
		if !strings.Contains(args, "\n"+want+"\n") {
			t.Fatalf("generated rule is missing default argument %q: %v", want, doc.Rules[0].Args)
		}
	}
	if doc.PluginConfig.InvokeInterval != "30s" {
		t.Fatalf("NPD invoke interval = %q, want 30s", doc.PluginConfig.InvokeInterval)
	}
	parsed := parseConfig(t, doc.Rules[0].Args)
	if parsed.GenerateConfig || parsed.Category != "dynamic" || parsed.Options.Endpoint != endpoint || parsed.Options.File != path {
		t.Fatalf("generated rule did not round-trip through the CLI: %+v", *parsed)
	}
}

func TestAdapterDefaultFilesShareDirectory(t *testing.T) {
	var healthCalls, categoryCalls atomic.Int32
	endpoint := startHealthServer(t, &healthServer{
		fetch: func(context.Context) (*xdmgrpc.GetAllHealthResponse, error) {
			healthCalls.Add(1)
			return &xdmgrpc.GetAllHealthResponse{Categories: []*xdmgrpc.CategoryHealth{
				{Category: "dynamic", Status: xdmgrpc.HealthStatus_HEALTHY},
			}}, nil
		},
		categories: func(context.Context) (*xdmgrpc.GetCategoriesResponse, error) {
			categoryCalls.Add(1)
			return configCatalog(), nil
		},
	})
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.ReadFile(executable)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	installed := filepath.Join(dir, "rt_npd_adapter")
	if err := os.WriteFile(installed, binary, 0755); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for i := 0; i < 2; i++ {
		cmd := exec.CommandContext(ctx, installed, "-test.run=^TestAdapterProcess$", "--",
			"--endpoint="+endpoint, "--category=dynamic")
		cmd.Env = append(os.Environ(), "RT_TEST_ADAPTER_PROCESS=1")
		if data, err := cmd.CombinedOutput(); err != nil || len(data) != 0 {
			t.Fatalf("installed adapter: %v, output=%s", err, data)
		}
	}
	for _, name := range []string{npdconfig.FileName, npdconfig.FileName + ".lock", "health.cache", "health.cache.lock"} {
		if info, err := os.Stat(filepath.Join(dir, name)); err != nil || !info.Mode().IsRegular() {
			t.Fatalf("missing colocated file %s: %v", name, err)
		}
	}
	if healthCalls.Load() != 1 || categoryCalls.Load() != 1 {
		t.Fatalf("health calls=%d, category calls=%d; want one each", healthCalls.Load(), categoryCalls.Load())
	}
}

func TestRunConfigMissingRetryPreservesHealth(t *testing.T) {
	for _, state := range []xdmgrpc.HealthStatus{xdmgrpc.HealthStatus_HEALTHY, xdmgrpc.HealthStatus_UNHEALTHY, xdmgrpc.HealthStatus_UNKNOWN} {
		t.Run(state.String(), func(t *testing.T) {
			var calls atomic.Int32
			endpoint := startHealthServer(t, &healthServer{
				fetch: func(context.Context) (*xdmgrpc.GetAllHealthResponse, error) {
					return &xdmgrpc.GetAllHealthResponse{Categories: []*xdmgrpc.CategoryHealth{{Category: "dynamic", Status: state, Reason: "health result"}}}, nil
				},
				categories: func(context.Context) (*xdmgrpc.GetCategoriesResponse, error) {
					if calls.Add(1) == 1 {
						return nil, status.Error(codes.Unavailable, "catalog unavailable")
					}
					return configCatalog(), nil
				},
			})
			path := filepath.Join(t.TempDir(), npdconfig.FileName)
			for i := 0; i < 3; i++ {
				var out, errOut bytes.Buffer
				code := run(context.Background(), []string{"--endpoint", endpoint, "--category", "dynamic", "--config-file", path, "--cache-file="}, &out, &errOut)
				wantMessage := "health result"
				if state == xdmgrpc.HealthStatus_HEALTHY {
					wantMessage = ""
				}
				if code != int(state)-1 || out.String() != wantMessage || (errOut.Len() != 0) != (i == 0) {
					t.Fatalf("attempt=%d, code=%d, stdout=%q, stderr=%q", i, code, out.String(), errOut.String())
				}
				if i == 0 {
					if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("failed generation published file: %v", err)
					}
				}
			}
			if calls.Load() != 2 {
				t.Fatalf("existing configuration must skip catalog RPC: calls=%d", calls.Load())
			}
		})
	}
}

func TestRunConfigGenerationUsesRemainingDeadline(t *testing.T) {
	endpoint := startHealthServer(t, &healthServer{
		fetch: func(context.Context) (*xdmgrpc.GetAllHealthResponse, error) {
			return &xdmgrpc.GetAllHealthResponse{Categories: []*xdmgrpc.CategoryHealth{{Category: "dynamic", Status: xdmgrpc.HealthStatus_UNHEALTHY, Reason: "failure"}}}, nil
		},
		categories: func(ctx context.Context) (*xdmgrpc.GetCategoriesResponse, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	})
	path := filepath.Join(t.TempDir(), npdconfig.FileName)
	var out, errOut bytes.Buffer
	start := time.Now()
	code := run(context.Background(), []string{"--endpoint", endpoint, "--category", "dynamic", "--config-file", path, "--cache-file=", "--timeout=100ms"}, &out, &errOut)
	if code != 1 || out.String() != "failure" || errOut.Len() == 0 || time.Since(start) > time.Second {
		t.Fatalf("code=%d, stdout=%q, stderr=%q", code, out.String(), errOut.String())
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("timed out generation published file: %v", err)
	}
}

func TestRunGenerateConfigFailures(t *testing.T) {
	endpoint := startHealthServer(t, &healthServer{categories: func(context.Context) (*xdmgrpc.GetCategoriesResponse, error) {
		return nil, status.Error(codes.Unavailable, "catalog unavailable")
	}})
	path := filepath.Join(t.TempDir(), npdconfig.FileName)
	if err := os.WriteFile(path, []byte("existing configuration"), 0644); err != nil {
		t.Fatal(err)
	}
	for _, extra := range [][]string{
		nil, {"--config-file="}, {"--plugin-timeout=3s"}, {"--invoke-interval=5s"},
		{"--concurrency=0"}, {"--cache-ttl=30s"}, {"--config-file=relative.json"},
	} {
		args := append([]string{"--generate-config", "--endpoint", endpoint, "--config-file", path}, extra...)
		var out, errOut bytes.Buffer
		if code := run(context.Background(), args, &out, &errOut); code != 3 || out.Len() != 0 || errOut.Len() == 0 {
			t.Fatalf("args=%v, code=%d, stdout=%q, stderr=%q", args, code, out.String(), errOut.String())
		}
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "existing configuration" {
		t.Fatal("failed generation overwrote the existing configuration")
	}
}

func TestConcurrentProcessesGenerateMissingConfig(t *testing.T) {
	var calls atomic.Int32
	endpoint := startHealthServer(t, &healthServer{
		fetch: func(context.Context) (*xdmgrpc.GetAllHealthResponse, error) {
			return &xdmgrpc.GetAllHealthResponse{Categories: []*xdmgrpc.CategoryHealth{{Category: "dynamic", Status: xdmgrpc.HealthStatus_HEALTHY}}}, nil
		},
		categories: func(context.Context) (*xdmgrpc.GetCategoriesResponse, error) {
			calls.Add(1)
			time.Sleep(20 * time.Millisecond)
			return configCatalog(), nil
		},
	})
	path := filepath.Join(t.TempDir(), npdconfig.FileName)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAdapterProcess$", "--", "--endpoint", endpoint, "--category=dynamic", "--config-file", path, "--cache-file=")
			cmd.Env = append(os.Environ(), "RT_TEST_ADAPTER_PROCESS=1")
			if data, err := cmd.CombinedOutput(); err != nil || len(data) != 0 {
				t.Errorf("child: %v, output=%s", err, data)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("catalog calls=%d, want 1", calls.Load())
	}
}
