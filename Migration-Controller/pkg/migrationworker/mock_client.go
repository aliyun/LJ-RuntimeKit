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
	"sync"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// MockClient is for tests only.
type MockClient struct {
	k8sClient client.Client
	options   *MockOptions
	callLog   map[string][]interface{}
	mu        sync.Mutex
}

// MockOptions configures the behavior of MockClient
type MockOptions struct {
	// EnableCallLog enables logging of method calls
	EnableCallLog bool
	// UpdateK8sStatus controls whether to update K8s CRD status
	UpdateK8sStatus bool
}

// NewMockClient creates a MockClient instance
func NewMockClient(k8sClient client.Client) (Client, error) {
	return NewMockClientWithOptions(k8sClient, &MockOptions{
		UpdateK8sStatus: true,
	})
}

// NewMockClientWithOptions creates a MockClient instance with custom options
func NewMockClientWithOptions(k8sClient client.Client, opts *MockOptions) (Client, error) {
	if opts == nil {
		opts = &MockOptions{
			UpdateK8sStatus: true,
		}
	}
	c := &MockClient{
		k8sClient: k8sClient,
		options:   opts,
		callLog:   make(map[string][]interface{}),
	}
	return c, nil
}

// logCall records a method call if logging is enabled
func (c *MockClient) logCall(method string, args ...interface{}) {
	if c.options.EnableCallLog {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.callLog[method] = append(c.callLog[method], args)
	}
}

// GetCallLog returns the call log for testing assertions
func (c *MockClient) GetCallLog(method string) []interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.callLog[method]
}

// ClearCallLog clears the call log
func (c *MockClient) ClearCallLog() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.callLog = make(map[string][]interface{})
}

func (c *MockClient) CheckpointToStorage(ctx context.Context, req *CheckpointToStorageRequest) (*BaseResponse, error) {
	c.logCall("CheckpointToStorage", req)
	return &BaseResponse{}, nil
}

func (c *MockClient) QueryCheckpointToStorageStatus(ctx context.Context, req *QueryCheckpointToStorageStatusRequest) (*QueryCheckpointToStorageStatusResponse, error) {
	c.logCall("QueryCheckpointToStorageStatus", req)
	return &QueryCheckpointToStorageStatusResponse{
		RequestID: req.RequestID,
		Status:    0,
		Message:   "ok",
		Data:      map[string]*CheckpointAttributes{},
	}, nil
}

func (c *MockClient) CleanTask(ctx context.Context, req *CleanTaskRequest) error {
	c.logCall("CleanTask", req)
	return nil
}

func (c *MockClient) RestoreFromStorage(ctx context.Context, req *RestoreFromStorageRequest) (*BaseResponse, error) {
	c.logCall("RestoreFromStorage", req)
	return &BaseResponse{
		RequestID: req.RequestID,
		Status:    0,
		Message:   "ok",
	}, nil
}

func (c *MockClient) QueryRestoreFromStorageStatus(ctx context.Context, req *QueryRestoreFromStorageStatusRequest) (*QueryRestoreFromStorageStatusResponse, error) {
	c.logCall("QueryRestoreFromStorageStatus", req)
	return &QueryRestoreFromStorageStatusResponse{
		RequestID: req.RequestID,
		Status:    0,
		Message:   "ok",
		Data:      map[string]*CheckpointAttributes{},
	}, nil
}

func (c *MockClient) RestoreXPUOnNode(ctx context.Context, req *RestoreXPUOnNodeRequest) error {
	c.logCall("RestoreXPUOnNode", req)
	return nil
}

func (c *MockClient) Close() error {
	c.logCall("Close")
	return nil
}
