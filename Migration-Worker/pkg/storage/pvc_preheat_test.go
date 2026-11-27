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
	"archive/tar"
	"context"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	migutil "github.com/container-migration/worker/pkg/util"
)

// TestNewLocalFSWithPath smoke-tests the construction helper shared by the
// combined storages: given a creatable dir it must return *LocalFS with the
// correct Path (the degradation logic of primary/fallback construction depends
// on it).
func TestNewLocalFSWithPath(t *testing.T) {
	dir := t.TempDir()
	lf, err := newLocalFSWithPath(nil, dir)
	if err != nil {
		t.Fatalf("newLocalFSWithPath: %v", err)
	}
	if lf == nil || lf.Path != dir {
		t.Fatalf("unexpected LocalFS %+v, want Path=%s", lf, dir)
	}
}

// fakeDownloadInfoStorage is a minimal fake providing fixed GetDownloadInfo
// return values; other methods are never called in tests.
type fakeDownloadInfoStorage struct {
	info *RestoreInfo
	err  error
}

func (f *fakeDownloadInfoStorage) Name() string { return "fake" }
func (f *fakeDownloadInfoStorage) Upload(io.Reader, string) (int64, error) {
	panic("not used")
}
func (f *fakeDownloadInfoStorage) Restore(context.Context, string, string) (*RestoreInfo, error) {
	panic("not used")
}
func (f *fakeDownloadInfoStorage) CleanUpload(string) error { panic("not used") }
func (f *fakeDownloadInfoStorage) GetUploadInfo() (*UploadInfo, error) {
	panic("not used")
}
func (f *fakeDownloadInfoStorage) GetDownloadInfo() (*RestoreInfo, error) {
	return f.info, f.err
}

// TestMergeDownloadInfo locks the merge semantics of progress counters:
// FileSize/FileCount of primary and fallback add up. During fallback primary's
// counter is frozen and only fallback grows; the sum lets the upper progress
// ticker reflect the real advance (instead of a frozen bytesTransferred).
func TestMergeDownloadInfo(t *testing.T) {
	primary := &fakeDownloadInfoStorage{info: &RestoreInfo{FileCount: 3, FileSize: 100}}
	fallback := &fakeDownloadInfoStorage{info: &RestoreInfo{FileCount: 7, FileSize: 250}}

	got, err := mergeDownloadInfo(primary, fallback)
	if err != nil {
		t.Fatalf("mergeDownloadInfo: %v", err)
	}
	if got.FileCount != 10 || got.FileSize != 350 {
		t.Fatalf("merged info = %+v, want FileCount=10 FileSize=350", got)
	}
}

// setEFCWatchParams shrinks the EFC progress monitor time window for unit tests, returning the restore function.
func setEFCWatchParams(t *testing.T, interval, timeout time.Duration) func() {
	t.Helper()
	oldInterval, oldTimeout := efcProgressCheckInterval, efcNoProgressTimeout
	efcProgressCheckInterval, efcNoProgressTimeout = interval, timeout
	return func() {
		efcProgressCheckInterval, efcNoProgressTimeout = oldInterval, oldTimeout
	}
}

// newTestLocalFS constructs a test LocalFS (with its own counter).
func newTestLocalFS(path string) *LocalFS {
	return &LocalFS{Path: path, UploadReader: migutil.NewCountingReader(), DownloadReader: migutil.NewCountingReader()}
}

// writeTarStream writes an uncompressed tar stream containing a single file to f.
func writeTarStream(t *testing.T, f *os.File, fileName, content string) {
	t.Helper()
	tw := tar.NewWriter(f)
	if err := tw.WriteHeader(&tar.Header{Name: fileName, Mode: 0600, Size: int64(len(content))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
}

// writeTarStreamBestEffort is the same as writeTarStream but ignores write
// errors: in the FIFO scenario the read side (primary) may already be cancelled
// and closed, so the write side gets EPIPE - writing data only unblocks the
// read side for it to recover; whether it is fully read does not affect the
// assertions (neither restore path may touch dst).
func writeTarStreamBestEffort(f *os.File, fileName, content string) {
	tw := tar.NewWriter(f)
	_ = tw.WriteHeader(&tar.Header{Name: fileName, Mode: 0600, Size: int64(len(content))})
	_, _ = tw.Write([]byte(content))
	_ = tw.Close()
}

// writeTestPart writes a part-00.tar shard containing a single file under srcDir.
func writeTestPart(t *testing.T, srcDir, fileName, content string) {
	t.Helper()
	if err := os.MkdirAll(srcDir, 0755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(filepath.Join(srcDir, "part-00.tar"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	writeTarStream(t, f, fileName, content)
}

// waitFor polls until cond is true or times out. Async-cleanup assertions use
// polling instead of a fixed sleep to avoid timing sensitivity under heavy
// load.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

// TestRestoreIntoTempAbandonedSkipsCommit locks the commit-abandon semantics:
// when abandoned returns true, dstDir must not be touched (existing data kept),
// only our own tmp is cleaned and an error returned.
func TestRestoreIntoTempAbandonedSkipsCommit(t *testing.T) {
	dst := filepath.Join(t.TempDir(), "dst")
	if err := os.MkdirAll(dst, 0755); err != nil {
		t.Fatal(err)
	}
	keep := filepath.Join(dst, "keep.txt")
	if err := os.WriteFile(keep, []byte("keep"), 0644); err != nil {
		t.Fatal(err)
	}

	var abandoned atomic.Bool
	abandoned.Store(true)
	err := restoreIntoTemp(dst, "", func() bool { return abandoned.Load() }, func(tmpDir string) error {
		return os.WriteFile(filepath.Join(tmpDir, "new.txt"), []byte("new"), 0644)
	})
	if err == nil {
		t.Fatal("restoreIntoTemp with abandoned=true should return error")
	}
	if b, rerr := os.ReadFile(keep); rerr != nil || string(b) != "keep" {
		t.Fatalf("dst was modified despite abandon: %v", rerr)
	}
	if _, serr := os.Stat(dst + "-tmp"); !os.IsNotExist(serr) {
		t.Fatalf("tmp dir not cleaned after abandon: %v", serr)
	}
}

// TestLocalFSRestoreAbandonCheckSkipsCommit directly locks the propagation of
// abandonCheck on the commit path: after a fully successful extract, seeing
// itself abandoned before deleting the old dst and renaming, it may only clean
// its own tmp and return an error; existing dst data must stay untouched.
func TestLocalFSRestoreAbandonCheckSkipsCommit(t *testing.T) {
	src := filepath.Join(t.TempDir(), "src")
	writeTestPart(t, src, "primary.txt", "primary-data")
	dst := filepath.Join(t.TempDir(), "dst")
	if err := os.MkdirAll(dst, 0755); err != nil {
		t.Fatal(err)
	}
	keep := filepath.Join(dst, "keep.txt")
	if err := os.WriteFile(keep, []byte("keep"), 0644); err != nil {
		t.Fatal(err)
	}

	lf := newTestLocalFS(src)
	// Parameterized version: abandonCheck is passed per call (the isolation method
	// of combined reads), no longer instance-field injection.
	if _, err := lf.restoreWith(context.Background(), "", dst, "", func() bool { return true }); err == nil {
		t.Fatal("expected abandoned error from LocalFS.Restore")
	}
	if b, rerr := os.ReadFile(keep); rerr != nil || string(b) != "keep" {
		t.Fatalf("dst modified despite abandon: %v", rerr)
	}
	if _, serr := os.Stat(dst + "-tmp"); !os.IsNotExist(serr) {
		t.Fatal("tmp not cleaned after abandon")
	}
}

// TestRestoreFromEFCWithFallbackHungPrimaryKeepsFallbackResult locks the hang
// scenario: primary is stuck in a FIFO read (simulating FUSE hang); after the
// zero-growth timeout fallback takes over and commits; primary's read then
// returns EOF and goes through error cleanup, which may only clean its own tmp
// and must not touch what fallback already committed to dst.
func TestRestoreFromEFCWithFallbackHungPrimaryKeepsFallbackResult(t *testing.T) {
	restore := setEFCWatchParams(t, 20*time.Millisecond, 60*time.Millisecond)
	defer restore()

	root := t.TempDir()
	efcSrc := filepath.Join(root, "efc-src")
	pvcSrc := filepath.Join(root, "pvc-src")
	dst := filepath.Join(root, "dst")
	if err := os.MkdirAll(efcSrc, 0755); err != nil {
		t.Fatal(err)
	}
	// primary source: part-00.tar is a FIFO; open blocks forever to simulate a hung FUSE read.
	fifo := filepath.Join(efcSrc, "part-00.tar")
	if err := syscall.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	writeTestPart(t, pvcSrc, "fallback.txt", "fallback-data")

	primary := newTestLocalFS(efcSrc)
	fallback := newTestLocalFS(pvcSrc)

	done := make(chan error, 1)
	go func() {
		_, err := restoreFromEFCWithFallback(context.Background(), primary, "", fallback, "", dst)
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("restoreFromEFCWithFallback: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("fallback did not complete in time")
	}

	// Unblock primary's open: the read side then reads EOF, and the failed extract goes through error cleanup.
	w, err := os.OpenFile(fifo, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()
	// Wait for primary to finish error cleanup (cleaning its own dst-tmp, not touching dst).
	waitFor(t, 2*time.Second, func() bool {
		_, serr := os.Stat(dst + "-tmp")
		return os.IsNotExist(serr)
	})

	if b, err := os.ReadFile(filepath.Join(dst, "fallback.txt")); err != nil || string(b) != "fallback-data" {
		t.Fatalf("fallback result at dst was clobbered by primary cleanup: %v", err)
	}
}

// TestRestoreFromEFCWithFallbackPrimaryRecoveryAbandonsCommit locks the
// primary-recovery scenario: after fallback has committed and primary has been
// abandoned, primary's read recovers and the extract succeeds - before commit,
// abandonCheck must make it abandon the commit (cleaning its own tmp), never
// renaming primary's data onto dst or deleting fallback's result.
func TestRestoreFromEFCWithFallbackPrimaryRecoveryAbandonsCommit(t *testing.T) {
	restore := setEFCWatchParams(t, 20*time.Millisecond, 60*time.Millisecond)
	defer restore()

	root := t.TempDir()
	efcSrc := filepath.Join(root, "efc-src")
	pvcSrc := filepath.Join(root, "pvc-src")
	dst := filepath.Join(root, "dst")
	if err := os.MkdirAll(efcSrc, 0755); err != nil {
		t.Fatal(err)
	}
	fifo := filepath.Join(efcSrc, "part-00.tar")
	if err := syscall.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	writeTestPart(t, pvcSrc, "fallback.txt", "fallback-data")

	primary := newTestLocalFS(efcSrc)
	fallback := newTestLocalFS(pvcSrc)

	done := make(chan error, 1)
	go func() {
		_, err := restoreFromEFCWithFallback(context.Background(), primary, "", fallback, "", dst)
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("restoreFromEFCWithFallback: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("fallback did not complete in time")
	}

	// fallback has committed and primary has been abandoned. Write a complete tar
	// into the FIFO: primary's read recovers, and then either the extract is
	// cancelled or it succeeds but is stopped by abandonCheck before commit - both
	// paths may only clean their own tmp and must not touch dst. The write side
	// runs in a goroutine and ignores write errors: primary may already be
	// cancelled and closed the read side, so the write side gets EPIPE.
	go func() {
		w, err := os.OpenFile(fifo, os.O_WRONLY, 0)
		if err != nil {
			return
		}
		defer w.Close()
		writeTarStreamBestEffort(w, "primary.txt", "primary-data")
	}()
	waitFor(t, 2*time.Second, func() bool {
		_, serr := os.Stat(dst + "-tmp")
		return os.IsNotExist(serr)
	})

	if _, err := os.Stat(filepath.Join(dst, "primary.txt")); !os.IsNotExist(err) {
		t.Fatal("primary result committed to dst despite abandon")
	}
	if b, err := os.ReadFile(filepath.Join(dst, "fallback.txt")); err != nil || string(b) != "fallback-data" {
		t.Fatalf("fallback result clobbered by recovered primary: %v", err)
	}
}

// TestRestoreFromEFCWithFallbackContextCancelAborts locks the task-level
// cancellation semantics: ctx cancellation returns the cancellation error
// directly without starting fallback (otherwise the cancelled ctx makes
// fallback fail at the first UnpackPart checkpoint, wasting a "falling back"
// log line).
func TestRestoreFromEFCWithFallbackContextCancelAborts(t *testing.T) {
	root := t.TempDir()
	efcSrc := filepath.Join(root, "efc-src")
	pvcSrc := filepath.Join(root, "pvc-src")
	dst := filepath.Join(root, "dst")
	if err := os.MkdirAll(efcSrc, 0755); err != nil {
		t.Fatal(err)
	}
	fifo := filepath.Join(efcSrc, "part-00.tar")
	if err := syscall.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	writeTestPart(t, pvcSrc, "fallback.txt", "fallback-data")

	primary := newTestLocalFS(efcSrc)
	fallback := newTestLocalFS(pvcSrc)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := restoreFromEFCWithFallback(ctx, primary, "", fallback, "", dst)
		done <- err
	}()

	// Cancel the task after primary is stuck in FIFO open (restoreIntoTemp has created the tmp sentinel).
	waitFor(t, 5*time.Second, func() bool {
		_, err := os.Stat(dst + "-tmp")
		return err == nil
	})
	cancel()

	select {
	case err := <-done:
		if err == nil || err != context.Canceled {
			t.Fatalf("want context.Canceled, got: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("restore did not return after context cancel")
	}

	// Fallback must not have run: dst has no fallback data.
	if _, err := os.Stat(filepath.Join(dst, "fallback.txt")); !os.IsNotExist(err) {
		t.Fatal("fallback ran despite canceled task context")
	}

	// Unblock primary's FIFO open so its goroutine finishes error cleanup and exits.
	if w, err := os.OpenFile(fifo, os.O_WRONLY, 0); err == nil {
		w.Close()
	}
}

// TestMergeDownloadInfoFallbackZero locks the behavior in the normal EFC
// phase: with the fallback counter always 0, the sum is primary's real
// progress, no bogus inflation.
func TestMergeDownloadInfoFallbackZero(t *testing.T) {
	primary := &fakeDownloadInfoStorage{info: &RestoreInfo{FileSize: 1234}}
	fallback := &fakeDownloadInfoStorage{info: &RestoreInfo{}}

	got, err := mergeDownloadInfo(primary, fallback)
	if err != nil {
		t.Fatalf("mergeDownloadInfo: %v", err)
	}
	if got.FileSize != 1234 {
		t.Fatalf("merged FileSize = %d, want 1234", got.FileSize)
	}
}
