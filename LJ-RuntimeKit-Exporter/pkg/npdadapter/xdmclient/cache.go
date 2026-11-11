// Package xdmclient provides a cross-process cache for health snapshots.
package xdmclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	xdmgrpc "github.com/lj-runtimekit/exporter/pkg/grpc/xdm"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

const (
	maxCacheBytes = 8 << 20
	// Version 2 uses snapshot-level timestamps and rejects the previous layout.
	cacheVersion = 2
)

// FetchFunc retrieves a full health snapshot.
type FetchFunc func(context.Context) (*xdmgrpc.GetAllHealthResponse, error)

// CachedFetcher shares a file cache between processes on the same node.
// Only the adapter user should have write access to the directory containing Path.
// Key should be the normalized endpoint. All categories must share Path, Key, and TTL.
type CachedFetcher struct {
	Path  string
	Key   string
	TTL   time.Duration
	Fetch FetchFunc
	Logf  func(string, ...any)
}

type cacheEntry struct {
	Version   int    `json:"version"`
	Key       string `json:"key"`
	WrittenAt int64  `json:"written_at_ns"`
	Payload   []byte `json:"payload"`
}

// Get checks the cache without locking, then serializes fetches with a lock file.
// A disabled or unavailable cache falls back to a direct query.
func (c *CachedFetcher) Get(ctx context.Context) (*xdmgrpc.GetAllHealthResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.Path == "" || c.TTL <= 0 {
		return c.fetch(ctx)
	}
	if err := checkRegularCacheFile(c.Path); err != nil {
		c.log("cache path unavailable; querying health service directly: %v", err)
		return c.fetch(ctx)
	}
	if snapshot := c.read(); snapshot != nil {
		return snapshot, nil
	}
	lock, err := c.lock(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		c.log("cache lock unavailable; querying health service directly: %v", err)
		return c.fetch(ctx)
	}
	// Never replace or remove the lock file: processes must lock the same inode.
	defer lock.Close()
	if snapshot := c.read(); snapshot != nil {
		return snapshot, nil
	}
	snapshot, err := c.fetch(ctx)
	if err != nil {
		return nil, err
	}
	if err := c.persist(snapshot); err != nil {
		c.log("cache write failed; using the fetched snapshot: %v", err)
	}
	return snapshot, nil
}

func (c *CachedFetcher) fetch(ctx context.Context) (*xdmgrpc.GetAllHealthResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.Fetch == nil {
		return nil, errors.New("health snapshot fetch function is not configured")
	}
	snapshot, err := c.Fetch(ctx)
	if err == nil && snapshot == nil {
		err = errors.New("health service returned a nil response")
	}
	return snapshot, err
}

func (c *CachedFetcher) read() *xdmgrpc.GetAllHealthResponse {
	file, err := openRegularCacheFile(c.Path, os.O_RDONLY)
	if err != nil {
		return nil
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxCacheBytes+1))
	if err != nil || len(data) > maxCacheBytes {
		return nil
	}
	var entry cacheEntry
	if json.Unmarshal(data, &entry) != nil || entry.Version != cacheVersion || entry.Key != c.Key || entry.WrittenAt <= 0 || entry.Payload == nil {
		return nil
	}
	age := time.Since(time.Unix(0, entry.WrittenAt))
	if age < 0 || age >= c.TTL {
		return nil
	}
	snapshot := &xdmgrpc.GetAllHealthResponse{}
	if proto.Unmarshal(entry.Payload, snapshot) != nil {
		return nil
	}
	return snapshot
}

func (c *CachedFetcher) lock(ctx context.Context) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(c.Path), 0700); err != nil {
		return nil, err
	}
	file, err := openRegularCacheFile(c.Path+".lock", os.O_CREATE|os.O_RDWR)
	if err != nil {
		return nil, err
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			file.Close()
			return nil, err
		}
		// Closing an fd does not reliably cancel a blocking flock. Retry without blocking.
		err = unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return file, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) && !errors.Is(err, unix.EINTR) {
			file.Close()
			return nil, err
		}
		select {
		case <-ctx.Done():
			file.Close()
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (c *CachedFetcher) persist(snapshot *xdmgrpc.GetAllHealthResponse) error {
	payload, err := proto.Marshal(snapshot)
	if err != nil {
		return err
	}
	if payload == nil {
		payload = []byte{}
	}
	data, err := json.Marshal(cacheEntry{Version: cacheVersion, Key: c.Key, WrittenAt: time.Now().UnixNano(), Payload: payload})
	if err != nil {
		return err
	}
	if len(data) > maxCacheBytes {
		return fmt.Errorf("snapshot cache exceeds %d bytes", maxCacheBytes)
	}
	file, err := os.CreateTemp(filepath.Dir(c.Path), ".health-*.tmp")
	if err != nil {
		return err
	}
	// Keep the temporary file in the target directory to ensure an atomic rename.
	defer os.Remove(file.Name())
	defer file.Close()
	if _, err := file.Write(data); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := checkRegularCacheFile(c.Path); err != nil {
		return err
	}
	return os.Rename(file.Name(), c.Path)
}

func checkRegularCacheFile(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("cache path is not a regular file: %s", path)
	}
	return nil
}

func openRegularCacheFile(path string, flag int) (*os.File, error) {
	if err := checkRegularCacheFile(path); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, flag|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0600)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err == nil && !info.Mode().IsRegular() {
		err = fmt.Errorf("cache path is not a regular file: %s", path)
	}
	if err != nil {
		file.Close()
		return nil, err
	}
	return file, nil
}

func (c *CachedFetcher) log(format string, args ...any) {
	if c.Logf != nil {
		c.Logf(format, args...)
	}
}
