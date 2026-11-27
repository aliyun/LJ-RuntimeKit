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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeTempFile writes a file under the temp dir and returns its path.
func writeTempFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0644); err != nil {
		t.Fatalf("write %s: %v", p, err)
	}
	return p
}

// readAllWithDeadline reads the reader to EOF within the deadline, so the test
// does not hang on a blocked reader.
func readAllWithDeadline(t *testing.T, r io.Reader, d time.Duration) []byte {
	t.Helper()
	type res struct {
		b   []byte
		err error
	}
	ch := make(chan res, 1)
	go func() {
		b, err := io.ReadAll(r)
		ch <- res{b, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("read to EOF: %v", r.err)
		}
		return r.b
	case <-time.After(d):
		t.Fatalf("read did not reach EOF within %v", d)
		return nil
	}
}

// readOneWithDeadline waits for one Read from the reader (not requiring EOF),
// avoiding a blocking hang. growingFileReader returns EOF only after done
// closes, so io.ReadAll cannot be used to test the intermediate state of
// "waiting for the new file to grow past offset".
func readOneWithDeadline(r io.Reader, buf []byte, d time.Duration) (int, error) {
	type res struct {
		n   int
		err error
	}
	ch := make(chan res, 1)
	go func() {
		n, err := r.Read(buf)
		ch <- res{n, err}
	}()
	select {
	case r := <-ch:
		return r.n, r.err
	case <-time.After(d):
		return 0, fmt.Errorf("read did not return within %v", d)
	}
}

// TestGrowingFileReaderFollowsRetryFileReplacement reproduces the full download
// retry sequence: attempt 0 creates file A with partial data (the main
// goroutine has opened it and consumed the prefix); after a transient failure
// downloadWithRetry os.Removes it and retries, and attempt 1 writes the same
// content fully to new file B. Before the fix the reader held onto old inode A
// and finally read EOF after done -> truncated tar; after the fix the reader
// detects the inode swap, switches to B and continues from the current offset,
// so the extractor gets the complete byte stream.
func TestGrowingFileReaderFollowsRetryFileReplacement(t *testing.T) {
	dir := t.TempDir()
	path := writeTempFile(t, dir, "part-00.tar.tmp", "abcdefgh") // A: attempt 0 partial data

	done := make(chan struct{})
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	r := &growingFileReader{path: path, f: f, done: done, ctx: context.Background()}
	defer r.Close()

	// Before attempt 0 fails, the main goroutine has consumed the first 4 bytes.
	buf := make([]byte, 4)
	if _, err := io.ReadFull(r, buf); err != nil {
		t.Fatalf("read first 4 bytes: %v", err)
	}
	if string(buf) != "abcd" {
		t.Fatalf("unexpected prefix %q", buf)
	}

	// Retry: Remove the old file (the old inode is still held by the reader's FD),
	// create a new file B with the same complete content.
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove %s: %v", path, err)
	}
	writeTempFile(t, dir, "part-00.tar.tmp", "abcdefghijklmnop") // B: complete 16 bytes
	close(done)

	// After done the reader must hand out the rest of the complete stream, not a
	// truncation: "efgh" (leftover of the old inode) + "ijklmnop" (continuation on
	// the new inode).
	rest := readAllWithDeadline(t, r, 5*time.Second)
	if string(rest) != "efghijklmnop" {
		t.Fatalf("stream was truncated or corrupted, got %q, want %q", rest, "efghijklmnop")
	}
}

// TestGrowingFileReaderWaitsForReplacementToCatchUp covers the case where the
// new file is temporarily shorter than the consumed offset: after switching to
// B the reader must keep waiting for B to grow past offset before continuing,
// instead of returning EOF early.
func TestGrowingFileReaderWaitsForReplacementToCatchUp(t *testing.T) {
	dir := t.TempDir()
	path := writeTempFile(t, dir, "part-00.tar.tmp", "abcdefghij") // A: 10 bytes

	done := make(chan struct{})
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	r := &growingFileReader{path: path, f: f, done: done, ctx: context.Background()}
	defer r.Close()

	// Read all 10 bytes of A (offset=10); only further reads enter EOF waiting.
	buf := make([]byte, 10)
	if _, err := io.ReadFull(r, buf); err != nil {
		t.Fatalf("read head: %v", err)
	}

	// Retry: after Remove, the new B temporarily has only 8 bytes (shorter than offset=10), completed asynchronously later.
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove %s: %v", path, err)
	}
	writeTempFile(t, dir, "part-00.tar.tmp", "abcdefgh")
	go func() {
		time.Sleep(50 * time.Millisecond)
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return
		}
		defer f.Close()
		_, _ = f.WriteString("ijklmnop")
	}()

	// The reader should wait for B to grow past offset, continuing with "klmnop" instead of returning EOF early.
	buf = make([]byte, 64)
	n, err := readOneWithDeadline(r, buf, 5*time.Second)
	if err != nil {
		t.Fatalf("replacement catch-up read: %v", err)
	}
	if got := string(buf[:n]); got != "klmnop" {
		t.Fatalf("replacement catch-up read wrong, got %q, want %q", got, "klmnop")
	}
}

// TestGrowingFileReaderRefreshNoSwapWhenInodeUnchanged locks the gate of
// refresh: when path and the FD point to the same inode, the FD must not be
// swapped (the normal "file still growing" case).
func TestGrowingFileReaderRefreshNoSwapWhenInodeUnchanged(t *testing.T) {
	dir := t.TempDir()
	path := writeTempFile(t, dir, "part-00.tar.tmp", "abc")
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	r := &growingFileReader{path: path, f: f}

	if r.refresh() {
		t.Fatal("refresh swapped file although inode is unchanged")
	}
	// A second call is also a no-op (idempotent).
	if r.refresh() {
		t.Fatal("refresh swapped file on second call")
	}
}

// TestGrowingFileReaderRefreshNoSwapWhenPathMissing locks the window between
// Remove and recreate: when path does not exist, refresh must not swap the FD
// (the old inode has no new data; wait for the next poll round).
func TestGrowingFileReaderRefreshNoSwapWhenPathMissing(t *testing.T) {
	dir := t.TempDir()
	path := writeTempFile(t, dir, "part-00.tar.tmp", "abc")
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	r := &growingFileReader{path: path, f: f}

	if err := os.Remove(path); err != nil {
		t.Fatalf("remove %s: %v", path, err)
	}
	if r.refresh() {
		t.Fatal("refresh swapped file although path was removed")
	}
}
