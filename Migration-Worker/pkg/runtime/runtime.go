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

package runtime

import (
	"context"
	"io"
)

// NodeName is the Kubernetes node name the worker is running on.
// It is populated from the KUBE_NODE_NAME env at startup and used by
// metrics / logs.
var NodeName string

// CheckpointPartStream iterates the tar parts of one checkpoint.
//
// A checkpoint is split into N independent tars so the destination node can
// download and extract them in parallel during restore (download dominates the
// restore latency, and the speedup is roughly totalBytes/largestPartBytes).
// Every part is a complete archive carrying the full directory skeleton, so the
// parts have no ordering dependency on each other.
type CheckpointPartStream interface {
	// Next advances to the next part and returns its index together with a
	// reader over that part's tar bytes. The reader must be fully consumed
	// before calling Next again. io.EOF means there are no more parts.
	Next() (partIndex int, r io.Reader, err error)
	// Close aborts the underlying RPC and releases the connection.
	Close() error
}

// ContainerRuntime is the node-local checkpoint engine. The only implementation
// is XDM: the worker never talks to a container runtime directly, it delegates
// the whole checkpoint to the node's X-Device-Manager and just moves bytes.
//
// Restore is no longer driven by the worker: the new architecture has
// xpu-container-runtime intercept runc-create on the destination node. The one
// exception is the XPU device state, which the controller requests through the
// worker (RestoreXPUOnNode) once the restored container is Running.
type ContainerRuntime interface {
	Name() string
	Init() error
	// CheckpointToStream asks the engine to checkpoint a container and returns an
	// iterator over the resulting tar parts (each part holds a slice of
	// rootfs/ + memory/ + xpu/ plus the full directory skeleton).
	CheckpointToStream(ctx context.Context, containerID string, opts map[string]string) (CheckpointPartStream, error)
	// RestoreXPU asks the engine to restore the XPU device state of a container
	// that is already running after `runc restore`. Synchronous; a CPU-only
	// container (no xpu/ under checkpointDir) succeeds without side effects.
	RestoreXPU(ctx context.Context, containerID, checkpointDir string) error
}
