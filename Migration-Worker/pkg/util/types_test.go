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

package util

import (
	"bytes"
	"io"
	"sync"
	"testing"
)

// TestCountingReader_SetReader covers the single-stream path (upload): attach a
// reader, and after reading Size() equals the bytes read.
func TestCountingReader_SetReader(t *testing.T) {
	const payload = "hello counting reader"
	c := NewCountingReader()
	c.SetReader(bytes.NewReader([]byte(payload)))

	got, err := io.ReadAll(c)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != payload {
		t.Fatalf("payload = %q, want %q", got, payload)
	}
	if c.Size() != int64(len(payload)) {
		t.Fatalf("Size() = %d, want %d", c.Size(), len(payload))
	}
}

// TestCountingReader_NilReaderIsEOF locks the old behavior: with no reader
// attached, Read returns EOF directly, no panic. The optional Counter branch in
// archive.go depends on this.
func TestCountingReader_NilReaderIsEOF(t *testing.T) {
	c := NewCountingReader()
	n, err := c.Read(make([]byte, 8))
	if n != 0 || err != io.EOF {
		t.Fatalf("Read on nil reader = (%d, %v), want (0, EOF)", n, err)
	}
}

// TestCountingReader_WrapConcurrent is the core of this fix: sharded restore
// has N goroutines each wrapping one shard stream and reading concurrently; all
// bytes must atomically accumulate onto the same Size(). The old
// RateLimitedReader's `totalSize += n` non-atomic accumulation loses counts
// under this test (go test -race reports the data race directly).
func TestCountingReader_WrapConcurrent(t *testing.T) {
	const (
		routines     = 8
		bytesPerPart = 1 << 16 // 64 KiB, enough Reads to force interleaving
	)

	c := NewCountingReader()
	var wg sync.WaitGroup
	for i := 0; i < routines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := c.Wrap(bytes.NewReader(make([]byte, bytesPerPart)))
			// Small buffer, forcing multiple Reads per worker to maximize interleaving of concurrent accumulation.
			if _, err := io.CopyBuffer(io.Discard, r, make([]byte, 512)); err != nil {
				t.Errorf("copy: %v", err)
			}
		}()
	}
	wg.Wait()

	want := int64(routines * bytesPerPart)
	if c.Size() != want {
		t.Fatalf("Size() = %d, want %d (bytes lost under concurrency)", c.Size(), want)
	}
}
