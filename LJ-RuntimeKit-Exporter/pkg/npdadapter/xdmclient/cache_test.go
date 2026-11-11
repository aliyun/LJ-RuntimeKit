package xdmclient

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xdmgrpc "github.com/lj-runtimekit/exporter/pkg/grpc/xdm"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

func testSnapshot() *xdmgrpc.GetAllHealthResponse {
	return &xdmgrpc.GetAllHealthResponse{
		NodeName: "node-a", SampledAtMs: 1000, TtlMs: 500,
		Categories: []*xdmgrpc.CategoryHealth{
			{Category: "dynamic", Status: xdmgrpc.HealthStatus_UNHEALTHY, Reason: "device failure"},
		},
	}
}

func TestCacheSingleFlight(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "health.cache")
	var calls atomic.Int32
	fetch := func(context.Context) (*xdmgrpc.GetAllHealthResponse, error) {
		calls.Add(1)
		time.Sleep(20 * time.Millisecond)
		return testSnapshot(), nil
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			c := &CachedFetcher{Path: path, Key: "endpoint", TTL: time.Minute, Fetch: fetch}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			got, err := c.Get(ctx)
			if err != nil || !proto.Equal(got, testSnapshot()) {
				t.Errorf("Get = (%v, %v)", got, err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("fetch count = %d, want 1", calls.Load())
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("unexpected cache permissions: %v, %v", info, err)
	}
}

func TestCacheMissesAndDisabled(t *testing.T) {
	for _, scenario := range []string{"missing", "corrupt", "expired", "future", "wrong key", "wrong version", "old schema", "missing payload", "invalid protobuf", "ttl zero", "empty path"} {
		t.Run(scenario, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "health.cache")
			calls := 0
			c := &CachedFetcher{Path: path, Key: "endpoint", TTL: time.Minute, Fetch: func(context.Context) (*xdmgrpc.GetAllHealthResponse, error) {
				calls++
				return testSnapshot(), nil
			}}
			entry := cacheEntry{Version: cacheVersion, Key: c.Key, WrittenAt: time.Now().UnixNano(), Payload: []byte{}}
			switch scenario {
			case "expired":
				entry.WrittenAt = time.Now().Add(-2 * time.Minute).UnixNano()
			case "future":
				entry.WrittenAt = time.Now().Add(time.Hour).UnixNano()
			case "wrong key":
				entry.Key = "other endpoint"
			case "wrong version":
				entry.Version = cacheVersion + 1
			case "old schema":
				entry.Version = 1
			case "missing payload":
				entry.Payload = nil
			case "invalid protobuf":
				entry.Payload = []byte{0xff}
			case "ttl zero":
				c.TTL = 0
			case "empty path":
				c.Path = ""
			}
			data, err := json.Marshal(entry)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "corrupt" {
				data = []byte("{partial")
			}
			if scenario != "missing" {
				if err := os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			got, err := c.Get(context.Background())
			if err != nil || !proto.Equal(got, testSnapshot()) || calls != 1 {
				t.Fatalf("Get = (%v, %v), calls = %d", got, err, calls)
			}
		})
	}
}

func TestCacheHitDoesNotLockOrFetch(t *testing.T) {
	c := &CachedFetcher{Path: filepath.Join(t.TempDir(), "health.cache"), Key: "endpoint", TTL: time.Minute}
	// Cache valid empty snapshots to avoid refetching for each missing category.
	if err := c.persist(&xdmgrpc.GetAllHealthResponse{}); err != nil {
		t.Fatal(err)
	}
	lock, err := c.lock(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if got, err := c.Get(ctx); err != nil || got == nil || len(got.Categories) != 0 {
		t.Fatalf("cache hit failed: %v, %v", got, err)
	}
}

func TestCacheFailureDegrades(t *testing.T) {
	for _, scenario := range []string{"parent is file", "lock is directory", "cache is directory"} {
		t.Run(scenario, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "cache")
			switch scenario {
			case "parent is file":
				if err := os.WriteFile(path, nil, 0600); err != nil {
					t.Fatal(err)
				}
				path = filepath.Join(path, "health.cache")
			case "lock is directory":
				if err := os.Mkdir(path+".lock", 0700); err != nil {
					t.Fatal(err)
				}
			case "cache is directory":
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			}
			logs, calls := 0, 0
			c := &CachedFetcher{Path: path, TTL: time.Minute, Logf: func(string, ...any) { logs++ }, Fetch: func(context.Context) (*xdmgrpc.GetAllHealthResponse, error) {
				calls++
				return testSnapshot(), nil
			}}
			got, err := c.Get(context.Background())
			if err != nil || !proto.Equal(got, testSnapshot()) || calls != 1 || logs != 1 {
				t.Fatalf("cache fallback failed: %v, %v, calls=%d, logs=%d", got, err, calls, logs)
			}
		})
	}
}

func TestCacheSpecialFiles(t *testing.T) {
	for _, kind := range []string{"socket", "symlink", "directory", "fifo"} {
		for _, suffix := range []string{"", ".lock"} {
			t.Run(kind+suffix, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "cache")
				target := path + suffix
				switch kind {
				case "socket":
					listener, err := net.Listen("unix", target)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { listener.Close() })
				case "symlink":
					source := &CachedFetcher{Path: path + ".source", Key: "endpoint"}
					if err := source.persist(testSnapshot()); err != nil {
						t.Fatal(err)
					}
					if err := os.Symlink(source.Path, target); err != nil {
						t.Fatal(err)
					}
				case "directory":
					if err := os.Mkdir(target, 0700); err != nil {
						t.Fatal(err)
					}
				case "fifo":
					if err := unix.Mkfifo(target, 0600); err != nil {
						t.Fatal(err)
					}
				}
				before, err := os.Lstat(target)
				if err != nil {
					t.Fatal(err)
				}
				calls, logs := 0, 0
				c := &CachedFetcher{
					Path: path, Key: "endpoint", TTL: time.Minute,
					Logf: func(string, ...any) { logs++ },
					Fetch: func(context.Context) (*xdmgrpc.GetAllHealthResponse, error) {
						calls++
						return testSnapshot(), nil
					},
				}
				if suffix == "" {
					if got := c.read(); got != nil {
						t.Fatal("non-regular cache must not be read")
					}
					if err := c.persist(testSnapshot()); err == nil {
						t.Fatal("non-regular cache must not be replaced")
					}
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				for i := 0; i < 2; i++ {
					got, err := c.Get(ctx)
					if err != nil || !proto.Equal(got, testSnapshot()) {
						t.Fatalf("direct query failed: %v, %v", got, err)
					}
				}
				if calls != 2 || logs != 2 {
					t.Fatalf("calls=%d, logs=%d; want two direct queries and diagnostics", calls, logs)
				}
				after, err := os.Lstat(target)
				if err != nil || !os.SameFile(before, after) || before.Mode() != after.Mode() {
					t.Fatalf("special file was modified: %v, %v", after, err)
				}
				if kind == "socket" {
					conn, err := net.DialTimeout("unix", target, time.Second)
					if err != nil {
						t.Fatalf("socket is no longer reachable: %v", err)
					}
					conn.Close()
				}
				unused := path + ".lock"
				if suffix != "" {
					unused = path
				}
				if _, err := os.Lstat(unused); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("unexpected cache artifact %s: %v", unused, err)
				}
				leftovers, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".health-*.tmp"))
				if err != nil || len(leftovers) != 0 {
					t.Fatalf("temporary files remain: %v, %v", leftovers, err)
				}
			})
		}
	}
}

func TestCacheSocketAppearsDuringFetch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache")
	logs := 0
	c := &CachedFetcher{
		Path: path, TTL: time.Minute, Logf: func(string, ...any) { logs++ },
		Fetch: func(context.Context) (*xdmgrpc.GetAllHealthResponse, error) {
			listener, err := net.Listen("unix", path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { listener.Close() })
			return testSnapshot(), nil
		},
	}
	got, err := c.Get(context.Background())
	if err != nil || !proto.Equal(got, testSnapshot()) || logs != 1 {
		t.Fatalf("fetch result changed: %v, %v, logs=%d", got, err, logs)
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSocket == 0 {
		t.Fatalf("socket created during fetch was overwritten: %v, %v", info, err)
	}
	conn, err := net.DialTimeout("unix", path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
}

func TestCacheFetchErrorsAreNotCached(t *testing.T) {
	c := &CachedFetcher{Path: filepath.Join(t.TempDir(), "health.cache"), TTL: time.Minute}
	backendErr := errors.New("backend unavailable")
	c.Fetch = func(context.Context) (*xdmgrpc.GetAllHealthResponse, error) { return nil, backendErr }
	if _, err := c.Get(context.Background()); !errors.Is(err, backendErr) {
		t.Fatalf("fetch error was not propagated: %v", err)
	}
	if _, err := os.Stat(c.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed responses must not be cached: %v", err)
	}
	c.Fetch = func(context.Context) (*xdmgrpc.GetAllHealthResponse, error) { return nil, nil }
	if _, err := c.Get(context.Background()); err == nil {
		t.Fatal("a nil response must be rejected")
	}
	c.Fetch = nil
	if _, err := c.Get(context.Background()); err == nil {
		t.Fatal("a nil fetch function must be rejected")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Get(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation was not respected: %v", err)
	}
}

func TestCacheModTimeDoesNotControlTTL(t *testing.T) {
	c := &CachedFetcher{Path: filepath.Join(t.TempDir(), "health.cache"), TTL: time.Minute}
	if err := c.persist(testSnapshot()); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(c.Path, old, old); err != nil {
		t.Fatal(err)
	}
	c.Fetch = func(context.Context) (*xdmgrpc.GetAllHealthResponse, error) {
		t.Fatal("valid cache must not trigger a query based on file modification time")
		return nil, nil
	}
	if got, err := c.Get(context.Background()); err != nil || !proto.Equal(got, testSnapshot()) {
		t.Fatalf("cache hit failed: %v, %v", got, err)
	}
}

func TestCacheRefreshFailurePreservesExpiredFile(t *testing.T) {
	for _, mode := range []string{"rpc error", "rpc timeout", "lock timeout", "lock unavailable"} {
		t.Run(mode, func(t *testing.T) {
			c := &CachedFetcher{Path: filepath.Join(t.TempDir(), "health.cache"), TTL: time.Nanosecond}
			if err := c.persist(testSnapshot()); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(c.Path)
			if err != nil {
				t.Fatal(err)
			}
			beforeInfo, err := os.Stat(c.Path)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "lock unavailable":
				if err := os.Mkdir(c.Path+".lock", 0700); err != nil {
					t.Fatal(err)
				}
			case "lock timeout":
				lock, err := c.lock(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				defer lock.Close()
			}
			backendErr := errors.New("backend unavailable")
			wantErr := error(backendErr)
			if mode == "rpc timeout" || mode == "lock timeout" {
				wantErr = context.DeadlineExceeded
			}
			calls := 0
			c.Fetch = func(ctx context.Context) (*xdmgrpc.GetAllHealthResponse, error) {
				calls++
				if mode == "rpc timeout" {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return nil, backendErr
			}
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			got, err := c.Get(ctx)
			if got != nil || !errors.Is(err, wantErr) {
				t.Fatalf("Get=%v, %v; want no snapshot and cause=%v", got, err, wantErr)
			}
			wantCalls := 1
			if mode == "lock timeout" {
				wantCalls = 0
			}
			if calls != wantCalls {
				t.Fatalf("fetch calls=%d, want %d", calls, wantCalls)
			}
			after, err := os.ReadFile(c.Path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("failed refresh changed cached data: %v", err)
			}
			info, err := os.Stat(c.Path)
			if err != nil || !os.SameFile(beforeInfo, info) || !info.ModTime().Equal(beforeInfo.ModTime()) {
				t.Fatalf("failed refresh replaced or updated the file: %v, %v", info, err)
			}
		})
	}
}

func TestCacheExpiredRefreshSuccess(t *testing.T) {
	for _, lockUnavailable := range []bool{false, true} {
		t.Run(fmt.Sprint(lockUnavailable), func(t *testing.T) {
			c := &CachedFetcher{Path: filepath.Join(t.TempDir(), "health.cache"), TTL: time.Nanosecond}
			if err := c.persist(testSnapshot()); err != nil {
				t.Fatal(err)
			}
			if lockUnavailable {
				if err := os.Mkdir(c.Path+".lock", 0700); err != nil {
					t.Fatal(err)
				}
			}
			want := testSnapshot()
			want.NodeName = "refreshed-node"
			calls := 0
			c.Fetch = func(context.Context) (*xdmgrpc.GetAllHealthResponse, error) {
				calls++
				return want, nil
			}
			got, err := c.Get(context.Background())
			if err != nil || !proto.Equal(got, want) || calls != 1 {
				t.Fatalf("successful refresh was discarded: %v, %v, calls=%d", got, err, calls)
			}
			c.TTL = time.Minute
			if lockUnavailable {
				want = testSnapshot()
			}
			if got := c.read(); !proto.Equal(got, want) {
				t.Fatalf("unexpected cache publication: got %v, want %v", got, want)
			}
		})
	}
}

func TestCacheAtomicReaders(t *testing.T) {
	c := &CachedFetcher{Path: filepath.Join(t.TempDir(), "health.cache"), TTL: time.Minute}
	if err := c.persist(testSnapshot()); err != nil {
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
				if got := c.read(); !proto.Equal(got, testSnapshot()) {
					t.Error("read an invalid snapshot during atomic replacement")
					return
				}
			}
		}
	}()
	for i := 0; i < 30; i++ {
		if err := c.persist(testSnapshot()); err != nil {
			t.Error(err)
			break
		}
	}
	close(done)
	wg.Wait()
}

// The child process tests lock release after SIGKILL; the adapter does not spawn children.
func TestCacheLockHolderProcess(t *testing.T) {
	path := os.Getenv("XDM_TEST_LOCK_PATH")
	if path == "" {
		return
	}
	c := &CachedFetcher{Path: path}
	lock, err := c.lock(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	fmt.Println("locked")
	time.Sleep(time.Hour)
}

func TestCacheLockTimeoutAndKillRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "health.cache")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCacheLockHolderProcess$")
	cmd.Env = append(os.Environ(), "XDM_TEST_LOCK_PATH="+path)
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	if line, err := bufio.NewReader(stdout).ReadString('\n'); err != nil || line != "locked\n" {
		t.Fatalf("child process did not acquire the lock: %q, %v", line, err)
	}
	calls := 0
	c := &CachedFetcher{Path: path, TTL: time.Minute, Fetch: func(context.Context) (*xdmgrpc.GetAllHealthResponse, error) {
		calls++
		return testSnapshot(), nil
	}}
	waitCtx, stop := context.WithTimeout(ctx, 50*time.Millisecond)
	defer stop()
	start := time.Now()
	if _, err := c.Get(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lock wait exceeded its deadline: %v", err)
	}
	if time.Since(start) > time.Second || calls != 0 {
		t.Fatal("fetch must not run after the lock wait times out")
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	if got, err := c.Get(ctx); err != nil || !proto.Equal(got, testSnapshot()) || calls != 1 {
		t.Fatalf("failed to recover after SIGKILL: %v, %v, calls=%d", got, err, calls)
	}
}
