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
	"io"
	"sync/atomic"
)

var UseHostBinary string

// CountingReader accumulates the bytes flowing through it, used to report checkpoint upload/download progress.
//
// Two usage patterns:
//   - SetReader, then use CountingReader itself as an io.Reader - single-stream
//     case (upload).
//   - Wrap - concurrent case: during sharded restore N workers read at once;
//     each takes its own wrapper, and all bytes accumulate onto the same
//     CountingReader.
//
// Counting is always atomic: N goroutines accumulate at once, while the
// progress-reporting ticker reads it from yet another goroutine.
type CountingReader struct {
	reader    io.Reader
	totalSize int64
}

func NewCountingReader() *CountingReader {
	return &CountingReader{}
}

func (r *CountingReader) Read(p []byte) (n int, err error) {
	if r.reader == nil {
		return 0, io.EOF
	}
	n, err = r.reader.Read(p)
	atomic.AddInt64(&r.totalSize, int64(n))
	return
}

func (r *CountingReader) Size() int64 {
	return atomic.LoadInt64(&r.totalSize)
}

// SetReader attaches a single data stream to the counter. Only for the case
// where exactly one stream exists at a time; for concurrent multi-stream use
// Wrap.
func (r *CountingReader) SetReader(reader io.Reader) {
	r.reader = reader
}

// Wrap returns an io.Reader that reads inner and records the bytes onto r.
//
// The difference from SetReader is that it can be called multiple times and
// used concurrently: each return value holds its own inner, only the counter is
// shared. The N workers of sharded restore each Wrap one shard stream, summing
// up the total transfer.
func (r *CountingReader) Wrap(inner io.Reader) io.Reader {
	return &countingWrapper{parent: r, inner: inner}
}

type countingWrapper struct {
	parent *CountingReader
	inner  io.Reader
}

func (w *countingWrapper) Read(p []byte) (int, error) {
	n, err := w.inner.Read(p)
	atomic.AddInt64(&w.parent.totalSize, int64(n))
	return n, err
}
