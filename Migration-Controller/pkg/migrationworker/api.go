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

package migrationworker

import (
	"context"
	"io"
)

// Client requests the worker to execute CRD main-path operations, wrapping the gRPC client with the ability to close the connection.
type Client interface {
	// CheckpointToStorage checkpoints the container to the specified storage.
	CheckpointToStorage(ctx context.Context, req *CheckpointToStorageRequest) (*BaseResponse, error)
	// QueryCheckpointToStorageStatus queries the state/result of a CheckpointToStorage task.
	QueryCheckpointToStorageStatus(ctx context.Context, req *QueryCheckpointToStorageStatusRequest) (*QueryCheckpointToStorageStatusResponse, error)
	// RestoreFromStorage pulls and extracts data from storage to the target node.
	RestoreFromStorage(ctx context.Context, req *RestoreFromStorageRequest) (*BaseResponse, error)
	// QueryRestoreFromStorageStatus queries the state/result of a RestoreFromStorage task.
	QueryRestoreFromStorageStatus(ctx context.Context, req *QueryRestoreFromStorageStatusRequest) (*QueryRestoreFromStorageStatusResponse, error)
	// RestoreXPUOnNode synchronously restores the XPU device state of containers
	// on the target node (the worker forwards to the local XDM to run cr-tool).
	// Precondition: the restore target container is Running.
	RestoreXPUOnNode(ctx context.Context, req *RestoreXPUOnNodeRequest) error
	// CleanTask cleans up the runtime context of one migration task.
	CleanTask(ctx context.Context, req *CleanTaskRequest) error

	io.Closer
}
