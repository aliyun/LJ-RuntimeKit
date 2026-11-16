package config

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xdmgrpc "github.com/lj-runtimekit/exporter/pkg/grpc/xdm"
	"google.golang.org/protobuf/proto"
)

func testOptions(t *testing.T) Options {
	t.Helper()
	dir := t.TempDir()
	return Options{
		Executable: filepath.Join(dir, "rt_npd_adapter"), File: filepath.Join(dir, FileName),
		Endpoint: "/run/health.sock", Timeout: 3 * time.Second,
		CacheFile: filepath.Join(dir, "health.cache"), CacheTTL: time.Second, MaxOutput: 160,
		InvokeInterval: 10 * time.Second, PluginTimeout: 5 * time.Second, Concurrency: 3,
	}
}

func testCatalog() *xdmgrpc.GetCategoriesResponse {
	return &xdmgrpc.GetCategoriesResponse{Categories: []*xdmgrpc.CategoryInfo{
		{Category: "z", Condition: "ZProblem", OkReason: "ZHealthy", OkMessage: "z healthy", BadReason: "ZUnhealthy"},
		{Category: "a", Condition: "AProblem", OkReason: "AHealthy", OkMessage: "a healthy", BadReason: "AUnhealthy"},
	}}
}

func TestRenderMappingAndDeterminism(t *testing.T) {
	if FileName != "rt-custom-plugin-monitor.json" {
		t.Fatalf("unexpected configuration filename: %s", FileName)
	}
	o := testOptions(t)
	catalog := testCatalog()
	before := proto.Clone(catalog)
	data, err := Render(catalog, o)
	if err != nil {
		t.Fatal(err)
	}
	var got monitor
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.Plugin != "custom" || got.Source != "rt-custom-plugin-monitor" || !got.MetricsReporting ||
		got.PluginConfig != (pluginConfig{InvokeInterval: "10s", Timeout: "5s", MaxOutput: 160, Concurrency: 3}) {
		t.Fatalf("unexpected monitor metadata: %+v", got)
	}
	if len(got.Conditions) != 2 || len(got.Rules) != 2 ||
		got.Conditions[0] != (condition{Type: "AProblem", Reason: "AHealthy", Message: "a healthy"}) ||
		got.Rules[0].Type != "permanent" || got.Rules[0].Condition != "AProblem" || got.Rules[0].Reason != "AUnhealthy" || got.Rules[0].Path != o.Executable {
		t.Fatalf("incorrect category mapping: %+v", got)
	}
	wantArgs := []string{
		"--endpoint=" + o.Endpoint, "--category=a", "--config-file=" + o.File, "--cache-file=" + o.CacheFile,
		"--timeout=3s", "--cache-ttl=1s", "--max-output-length=160", "--invoke-interval=10s", "--plugin-timeout=5s", "--concurrency=3",
	}
	if !reflect.DeepEqual(got.Rules[0].Args, wantArgs) {
		t.Fatalf("args=%v, want %v", got.Rules[0].Args, wantArgs)
	}
	if !proto.Equal(catalog, before) {
		t.Fatal("Render modified the caller's catalog")
	}
	catalog.Categories[0], catalog.Categories[1] = catalog.Categories[1], catalog.Categories[0]
	again, err := Render(catalog, o)
	if err != nil || !bytes.Equal(data, again) {
		t.Fatalf("render is not deterministic: %v", err)
	}
	catalog.Categories[0].Category = "--new=\"quoted\"; $(ignored)\n€"
	catalog.Categories[0].OkMessage = "healthy\n\"device\""
	data, err = Render(catalog, o)
	if err != nil || json.Unmarshal(data, &got) != nil {
		t.Fatalf("escaped metadata did not render: %v", err)
	}
	if got.Rules[0].Args[1] != "--category="+catalog.Categories[0].Category || got.Conditions[0].Message != catalog.Categories[0].OkMessage {
		t.Fatal("metadata must remain a single literal argument")
	}
}

func TestRenderRejectsInvalidCatalog(t *testing.T) {
	for _, name := range []string{"nil", "empty", "nil entry", "category", "condition", "ok reason", "ok message", "bad reason", "duplicate category", "duplicate condition"} {
		t.Run(name, func(t *testing.T) {
			catalog := testCatalog()
			switch name {
			case "nil":
				catalog = nil
			case "empty":
				catalog.Categories = nil
			case "nil entry":
				catalog.Categories[0] = nil
			case "category":
				catalog.Categories[0].Category = " "
			case "condition":
				catalog.Categories[0].Condition = ""
			case "ok reason":
				catalog.Categories[0].OkReason = ""
			case "ok message":
				catalog.Categories[0].OkMessage = ""
			case "bad reason":
				catalog.Categories[0].BadReason = ""
			case "duplicate category":
				catalog.Categories[0].Category = catalog.Categories[1].Category
			case "duplicate condition":
				catalog.Categories[0].Condition = catalog.Categories[1].Condition
			}
			if data, err := Render(catalog, testOptions(t)); err == nil || data != nil {
				t.Fatalf("invalid catalog produced data: %s, %v", data, err)
			}
		})
	}
}

func TestOptionsValidation(t *testing.T) {
	for _, change := range []func(*Options){
		func(o *Options) { o.Executable = "relative" }, func(o *Options) { o.File = "" },
		func(o *Options) { o.Endpoint = "unix:///run/health.sock" }, func(o *Options) { o.CacheFile = "relative" },
		func(o *Options) { o.File = o.Executable }, func(o *Options) { o.File = o.CacheFile },
		func(o *Options) { o.File = o.CacheFile + ".lock" }, func(o *Options) { o.Timeout = 0 },
		func(o *Options) { o.CacheFile = o.Endpoint },
		func(o *Options) { o.CacheFile = "/run/./health.sock" },
		func(o *Options) { o.CacheFile = "/run/nested/../health.sock" },
		func(o *Options) { o.Endpoint = o.CacheFile + ".lock" },
		func(o *Options) { o.File = o.Endpoint },
		func(o *Options) { o.Endpoint = o.File + ".lock" },
		func(o *Options) { o.Endpoint = o.Executable },
		func(o *Options) { o.CacheFile += "\x00" },
		func(o *Options) { o.PluginTimeout = o.Timeout }, func(o *Options) { o.InvokeInterval = o.PluginTimeout },
		func(o *Options) { o.CacheTTL = -1 }, func(o *Options) { o.CacheTTL = o.InvokeInterval },
		func(o *Options) { o.MaxOutput = 0 }, func(o *Options) { o.Concurrency = 0 },
	} {
		o := testOptions(t)
		change(&o)
		if err := o.Validate(); err == nil {
			t.Fatalf("invalid options accepted: %+v", o)
		}
	}
	o := testOptions(t)
	o.CacheFile = ""
	o.CacheTTL = time.Hour
	if err := o.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestValidatePathsWithoutConfig(t *testing.T) {
	o := testOptions(t)
	o.File = ""
	o.InvokeInterval, o.PluginTimeout, o.Concurrency = 0, 0, 0
	if err := o.ValidatePaths(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*Options){
		func(o *Options) { o.CacheFile = o.Endpoint },
		func(o *Options) { o.Endpoint = o.CacheFile + ".lock" },
		func(o *Options) { o.CacheFile = o.Executable },
	} {
		conflict := o
		change(&conflict)
		if err := conflict.ValidatePaths(); err == nil {
			t.Fatalf("conflicting health-only paths accepted: %+v", conflict)
		}
	}
	o.CacheFile = ""
	if err := o.ValidatePaths(); err != nil {
		t.Fatal(err)
	}
}

func TestGenerateConcurrentAndRefresh(t *testing.T) {
	o := testOptions(t)
	var calls atomic.Int32
	g := Generator{Options: o, Fetch: func(context.Context) (*xdmgrpc.GetCategoriesResponse, error) {
		calls.Add(1)
		return testCatalog(), nil
	}}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := g.Generate(context.Background(), false); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("fetch calls=%d, want 1", calls.Load())
	}
	data, err := os.ReadFile(o.File)
	if err != nil || !json.Valid(data) {
		t.Fatalf("configuration=%s, err=%v", data, err)
	}
	info, err := os.Stat(o.File)
	if err != nil || info.Mode().Perm() != 0644 {
		t.Fatalf("incorrect config permissions: %v, %v", info, err)
	}
	if err := g.Generate(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatal("explicit generation must refresh existing files")
	}
	for _, fetch := range []func(context.Context) (*xdmgrpc.GetCategoriesResponse, error){
		func(context.Context) (*xdmgrpc.GetCategoriesResponse, error) { return nil, errors.New("unavailable") },
		func(context.Context) (*xdmgrpc.GetCategoriesResponse, error) { return nil, nil },
	} {
		g.Fetch = fetch
		if err := g.Generate(context.Background(), true); err == nil {
			t.Fatal("generation must fail")
		}
		after, err := os.ReadFile(o.File)
		if err != nil || !bytes.Equal(data, after) {
			t.Fatal("failed refresh modified the existing file")
		}
	}
	leftovers, err := filepath.Glob(filepath.Join(filepath.Dir(o.File), ".npd-config-*.tmp"))
	if err != nil || len(leftovers) != 0 {
		t.Fatalf("temporary files remain: %v, %v", leftovers, err)
	}
}

func TestGenerateDeadlineAndFilesystemErrors(t *testing.T) {
	o := testOptions(t)
	g := Generator{Options: o, Fetch: func(context.Context) (*xdmgrpc.GetCategoriesResponse, error) {
		t.Fatal("fetch must not run")
		return nil, nil
	}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := g.Generate(ctx, false); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	lock, err := lockFile(context.Background(), o.File+".lock")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := g.Generate(ctx, false); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v", err)
	}
	lock.Close()
	if err := os.Mkdir(o.File, 0755); err != nil {
		t.Fatal(err)
	}
	if err := g.Generate(context.Background(), false); err == nil {
		t.Fatal("directory must be rejected")
	}
	o.File = filepath.Join(t.TempDir(), FileName)
	if err := os.Symlink(g.Options.File, o.File); err != nil {
		t.Fatal(err)
	}
	g.Options = o
	if err := g.Generate(context.Background(), true); err == nil {
		t.Fatal("symlink must be rejected")
	}
}

func TestGenerateCancellationAfterFetch(t *testing.T) {
	o := testOptions(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g := Generator{Options: o, Fetch: func(context.Context) (*xdmgrpc.GetCategoriesResponse, error) {
		cancel()
		return testCatalog(), nil
	}}
	if err := g.Generate(ctx, false); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	if _, err := os.Stat(o.File); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("file published after cancellation: %v", err)
	}
}

func TestGenerateAtomicRefresh(t *testing.T) {
	o := testOptions(t)
	g := Generator{Options: o, Fetch: func(context.Context) (*xdmgrpc.GetCategoriesResponse, error) { return testCatalog(), nil }}
	if err := g.Generate(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			data, err := os.ReadFile(o.File)
			if err != nil || !json.Valid(data) {
				t.Errorf("reader observed partial configuration: %v", err)
				return
			}
		}
	}()
	for i := 0; i < 20; i++ {
		if err := g.Generate(context.Background(), true); err != nil {
			t.Error(err)
			break
		}
	}
	close(done)
	wg.Wait()
}
