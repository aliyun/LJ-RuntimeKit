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
	"strings"
	"testing"
	"time"

	oss_sdk "github.com/container-migration/worker/pkg/storage/oss-sdk"
	migutil "github.com/container-migration/worker/pkg/util"
)

// TestIsOSSRemoteURL locks the http-source mode gate: only virtual-hosted
// *.aliyuncs.com counts as an OSS source. A PVC preheat source's remoteURL is
// http://{Service ClusterIP}:{port}/{pvcPath} with an IP host, which must be
// judged non-OSS - this branch is exactly what makes NewDragonfly nil the
// OssClient.
func TestIsOSSRemoteURL(t *testing.T) {
	tests := []struct {
		name      string
		remoteURL string
		want      bool
	}{
		{
			name:      "oss virtual-hosted style",
			remoteURL: "https://bucket.oss-cn-hangzhou.aliyuncs.com/base/part-00.tar",
			want:      true,
		},
		{
			name:      "pvc preheat source uses service cluster ip",
			remoteURL: "http://10.96.0.10:50090/pod-uid/123/c0/checkpoint",
			want:      false,
		},
		{
			name:      "pvc preheat source path with escaped spaces",
			remoteURL: "http://10.96.0.10:50090/my%20dir/checkpoint",
			want:      false,
		},
		{
			name:      "plain http host",
			remoteURL: "http://preheat-source.example.com/checkpoint",
			want:      false,
		},
		{
			name:      "unparsable",
			remoteURL: "://bad",
			want:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isOSSRemoteURL(tt.remoteURL); got != tt.want {
				t.Fatalf("isOSSRemoteURL(%q) = %v, want %v", tt.remoteURL, got, tt.want)
			}
		})
	}
}

// newHTTPSourceDragonfly reproduces the Dragonfly under http-source mode (PVC
// preheat): OssClient is nil. This is what NewDragonfly really produces when a
// storageType=dragonfly job carries a PVC-source PreheatJob - construction
// succeeds without error, so the upload methods must hold their own nil guards.
//
// BasePath must be non-empty: before the fix, Upload first called
// filepath.Rel("/", Join(BasePath, tar)); with an empty BasePath that call
// errors out immediately and never reaches the OssClient dereference, making
// the negative control meaningless.
func newHTTPSourceDragonfly() *Dragonfly {
	return &Dragonfly{
		BasePath:       "/base",
		httpSourceURL:  "http://10.96.0.10:50090/pod-uid/123/c0/checkpoint",
		UploadReader:   migutil.NewCountingReader(),
		DownloadReader: migutil.NewCountingReader(),
		// OssClient deliberately left nil
	}
}

// catchPanic runs fn and returns any panic it throws as a value; nil means no panic.
func catchPanic(fn func()) (p interface{}) {
	defer func() { p = recover() }()
	fn()
	return nil
}

// TestDragonflyUploadMethodsDoNotPanicOnNilOSSClient is the direct regression for this nil deref.
//
// Deliberately capture with recover without asserting a specific error value,
// so this test **compiles against the pre-fix code too**: before the fix both
// methods throw "invalid memory address or nil pointer dereference"; after the
// fix they do not - the negative control is provable (reverting dragonfly.go
// and rerunning this test fails).
func TestDragonflyUploadMethodsDoNotPanicOnNilOSSClient(t *testing.T) {
	d := newHTTPSourceDragonfly()

	// GetUploadInfo is the first explosion point: before the fix its first line was
	// d.OssClient.GetPouchReadInfo(), whose first line c.mux.Lock() dereferences
	// nil.
	if p := catchPanic(func() { _, _ = d.GetUploadInfo() }); p != nil {
		t.Fatalf("GetUploadInfo panicked on nil OssClient: %v", p)
	}
	// Before the fix, Upload unconditionally called d.OssClient.UploadWithReader
	// after filepath.Rel; its first step resetUploadState()'s c.mu.Lock()
	// dereferences nil.
	if p := catchPanic(func() { _, _ = d.Upload(strings.NewReader("x"), "part-00.tar") }); p != nil {
		t.Fatalf("Upload panicked on nil OssClient: %v", p)
	}
}

// TestDragonflyUploadGuardsNilOSSClient locks the error contract of Upload's guard.
//
// Before the fix, Upload unconditionally executed
// d.OssClient.UploadWithReader(...), whose first step resetUploadState() calls
// c.mu.Lock() - a nil receiver panics directly. The panic happens on the
// progress/upload path of checkpointItem.go, where progress reporting is a
// detached goroutine that no recover can catch, taking down the whole worker
// process.
//
// Deliberately assert only an error **message substring** instead of referencing
// the sentinel errHTTPSourceNoUpload: referencing the symbol makes this file
// fail to compile against the pre-fix code, so the negative control (revert
// dragonfly.go and rerun) cannot be done. The message itself is the contract -
// it is written into the CR status via checkpointItem.go and is the user's only
// self-diagnosis entry.
func TestDragonflyUploadGuardsNilOSSClient(t *testing.T) {
	d := newHTTPSourceDragonfly()

	size, err := d.Upload(strings.NewReader("data"), "part-00.tar")
	if err == nil {
		t.Fatal("Upload with nil OssClient returned no error, want a download-only rejection")
	}
	if size != 0 {
		t.Fatalf("Upload size = %d, want 0", size)
	}
	// The error message must point out the root cause (an http-source PreheatJob cannot upload) and the two ways out, otherwise users cannot self-diagnose.
	for _, want := range []string{"http-source PreheatJob", "download-only", "OSS-source PreheatJob", "storageType=pvc"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Upload error %q does not mention %q", err.Error(), want)
		}
	}
}

// TestDragonflyGetUploadInfoToleratesNilOSSClient locks the contract of
// GetUploadInfo: with a nil OssClient, leave ReadDuration zero and do **not**
// return an error.
//
// It is a pure progress/metric query; the caller polls every second, and
// between it and Upload sits partStream.Next() (the first part may block for
// minutes); returning an error would flood the same warning for minutes. The
// config error fails fast at Upload on the first part and is written into the
// CR status.
//
// Before the fix this unconditionally called d.OssClient.GetPouchReadInfo(),
// whose first line c.mux.Lock() is a nil deref panic, and it was the first
// explosion point (called before Upload).
func TestDragonflyGetUploadInfoToleratesNilOSSClient(t *testing.T) {
	d := newHTTPSourceDragonfly()

	info, err := d.GetUploadInfo()
	if err != nil {
		t.Fatalf("GetUploadInfo with nil OssClient returned error: %v", err)
	}
	if info == nil {
		t.Fatal("GetUploadInfo returned nil UploadInfo")
	}
	if info.ReadDuration != 0 {
		t.Fatalf("ReadDuration = %v, want 0 (no OSS upload in http-source mode)", info.ReadDuration)
	}
	if info.UploadedSize != 0 {
		t.Fatalf("UploadedSize = %d, want 0", info.UploadedSize)
	}
}

// TestDragonflyGetUploadInfoKeepsReadDurationWithOSSClient prevents the guard
// from being written as "always return 0": in non-http-source mode OssClient is
// non-nil and ReadDuration must pass through as usual.
func TestDragonflyGetUploadInfoKeepsReadDurationWithOSSClient(t *testing.T) {
	d := newHTTPSourceDragonfly()
	d.httpSourceURL = ""
	// GetPouchReadInfo only reads ReadDuration (never touches the inner *oss.Client),
	// so passing nil is fine and makes no network call.
	ossClient := oss_sdk.NewClient(nil, 1024, 1, 1)
	ossClient.ReadDuration = 1500 * time.Millisecond
	d.OssClient = ossClient

	info, err := d.GetUploadInfo()
	if err != nil {
		t.Fatalf("GetUploadInfo returned error: %v", err)
	}
	if info.ReadDuration != 1500*time.Millisecond {
		t.Fatalf("ReadDuration = %v, want %v", info.ReadDuration, 1500*time.Millisecond)
	}
}

// setFallbackWatchParams shrinks the monitor time window for unit tests, returning the restore function.
func setFallbackWatchParams(t *testing.T, interval time.Duration, checks int) func() {
	t.Helper()
	oldInterval, oldChecks := fallbackProgressCheckInterval, fallbackStallChecks
	fallbackProgressCheckInterval, fallbackStallChecks = interval, checks
	return func() {
		fallbackProgressCheckInterval, fallbackStallChecks = oldInterval, oldChecks
	}
}

// TestWatchFallbackProgressCancelsOnStall locks the hang judgment:
// fallbackStallChecks consecutive zero-growth checks must trigger cancel
// (otherwise a silent network hang blocks restore forever in fallback direct
// read).
func TestWatchFallbackProgressCancelsOnStall(t *testing.T) {
	restore := setFallbackWatchParams(t, 10*time.Millisecond, 3)
	defer restore()

	counter := migutil.NewCountingReader()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		watchFallbackProgress(counter, cancel, stop)
		close(done)
	}()

	select {
	case <-ctx.Done():
		// After 3 zero-growth windows, cancel is called as expected.
	case <-time.After(2 * time.Second):
		t.Fatal("stall was not detected: cancel never called")
	}
	close(stop)
	<-done
}

// TestWatchFallbackProgressResetsOnProgress locks "slow reads are never
// killed": progress within each window resets the timer, so a long read is not
// judged dead after a fixed duration.
func TestWatchFallbackProgressResetsOnProgress(t *testing.T) {
	restore := setFallbackWatchParams(t, 10*time.Millisecond, 3)
	defer restore()

	counter := migutil.NewCountingReader()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		watchFallbackProgress(counter, cancel, stop)
		close(done)
	}()

	// 5 consecutive windows, one byte fed per window, cancel never fires.
	for i := 0; i < 5; i++ {
		time.Sleep(fallbackProgressCheckInterval)
		w := counter.Wrap(strings.NewReader("x"))
		buf := make([]byte, 1)
		if _, err := w.Read(buf); err != nil {
			t.Fatalf("read: %v", err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("canceled despite progress in every window")
		default:
		}
	}
	close(stop)
	<-done
}
