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
	"fmt"
	"time"

	"github.com/container-migration/controller/pkg/metrics"
	"github.com/container-migration/controller/pkg/status"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// DefaultClient wraps ContainerStateMigrationServiceClient with the ability to close the connection.
type DefaultClient struct {
	conn          *grpc.ClientConn
	serviceClient ContainerStateMigrationServiceClient
}

// NewDefaultClient creates a DefaultClient instance.
//
// rpcTimeout is the fallback deadline for all unary calls (from
// --rpc-timeout). <=0 means no fallback, reverting to the old behavior of
// "entirely decided by the caller's ctx".
func NewDefaultClient(host string, port int, rpcTimeout time.Duration) (Client, error) {
	address := fmt.Sprintf("%s:%d", host, port)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if rpcTimeout > 0 {
		opts = append(opts, grpc.WithUnaryInterceptor(deadlineInterceptor(rpcTimeout)))
	}
	conn, err := grpc.NewClient(address, opts...)
	if err != nil {
		return nil, err
	}
	serviceClient := NewContainerStateMigrationServiceClient(conn)
	c := &DefaultClient{conn, serviceClient}
	return c, nil
}

// noRPCTimeoutKey marks a call that must bypass the fallback timeout even
// though its ctx carries no deadline. Such calls are bounded by the remote
// side instead of the client: RestoreXPUOnNode runs synchronously inside the
// worker, whose duration is bounded by XDM's cr-tool timeout (2h).
type noRPCTimeoutKey struct{}

// WithNoRPCTimeout returns a copy of ctx that the client interceptor lets
// through without attaching the fallback rpc-timeout. The call then runs until
// the remote returns (or ctx itself is cancelled).
func WithNoRPCTimeout(ctx context.Context) context.Context {
	return context.WithValue(ctx, noRPCTimeoutKey{}, struct{}{})
}

// deadlineInterceptor adds a unified fallback timeout to unary calls without
// their own deadline.
//
// The consequence of not adding one: reconcile uses a bare ctx and
// controller-runtime adds no timeout to it by default, so a worker that is
// TCP-reachable but no longer responding (stuck process, D-state I/O) would
// block the RPC forever, occupying the workqueue's reconcile goroutine. Note
// this differs from "the worker pod was deleted": after process exit the
// kernel replies RST and gRPC immediately returns Unavailable, which never
// hangs.
//
// This interceptor covers all 6 methods of the service (no streaming
// interface), but lets through any call that either already has a deadline or
// is marked with WithNoRPCTimeout - the criterion is the ctx, not a
// method-name blacklist. RestoreXPUOnNode is the marked one: its duration is
// bounded by the XDM-side cr-tool timeout (2h), and a client-side fallback
// would cut the call while cr-tool is still running inside XDM; the following
// DeadlineExceeded would then be judged transient by isTransientError and
// trigger a resend - running cr-tool a second time on the same process.
// So if new long-running interfaces are added later, the callers must bring
// their own deadline or the no-timeout mark.
func deadlineInterceptor(timeout time.Duration) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if _, ok := ctx.Deadline(); ok {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		if _, ok := ctx.Value(noRPCTimeoutKey{}).(struct{}); ok {
			return invoker(ctx, method, req, reply, cc, opts...)
		}

		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// CheckpointToStorage checkpoints the container to the specified storage.
func (c *DefaultClient) CheckpointToStorage(ctx context.Context, req *CheckpointToStorageRequest) (response *BaseResponse, err error) {
	startTime := time.Now()
	defer func() {
		errCode := "0"
		if err == nil {
			errCode = "1"
		}
		metrics.RecordRPCCallLatency("CheckpointToStorage", errCode, time.Since(startTime).Seconds())
	}()

	return c.serviceClient.CheckpointToStorage(ctx, req)
}

func (c *DefaultClient) QueryCheckpointToStorageStatus(ctx context.Context, req *QueryCheckpointToStorageStatusRequest) (response *QueryCheckpointToStorageStatusResponse, err error) {
	startTime := time.Now()
	defer func() {
		errCode := "0"
		if err == nil {
			errCode = "1"
		}
		metrics.RecordRPCCallLatency("QueryCheckpointToStorageStatus", errCode, time.Since(startTime).Seconds())
	}()

	return c.serviceClient.QueryCheckpointToStorageStatus(ctx, req)
}

// RestoreFromStorage pulls and extracts data from storage to the target node.
func (c *DefaultClient) RestoreFromStorage(ctx context.Context, req *RestoreFromStorageRequest) (response *BaseResponse, err error) {
	startTime := time.Now()
	defer func() {
		errCode := "0"
		if err == nil {
			errCode = "1"
		}
		metrics.RecordRPCCallLatency("RestoreFromStorage", errCode, time.Since(startTime).Seconds())
	}()

	return c.serviceClient.RestoreFromStorage(ctx, req)
}

func (c *DefaultClient) QueryRestoreFromStorageStatus(ctx context.Context, req *QueryRestoreFromStorageStatusRequest) (response *QueryRestoreFromStorageStatusResponse, err error) {
	startTime := time.Now()
	defer func() {
		errCode := "0"
		if err == nil {
			errCode = "1"
		}
		metrics.RecordRPCCallLatency("QueryRestoreFromStorageStatus", errCode, time.Since(startTime).Seconds())
	}()

	return c.serviceClient.QueryRestoreFromStorageStatus(ctx, req)
}

// RestoreXPUOnNode synchronously restores the XPU device state of containers
// on the target node (the worker forwards to the local XDM to run cr-tool,
// synchronous throughout). Only called after the target container is
// Running.
func (c *DefaultClient) RestoreXPUOnNode(ctx context.Context, req *RestoreXPUOnNodeRequest) (err error) {
	startTime := time.Now()
	defer func() {
		errCode := "0"
		if err == nil {
			errCode = "1"
		}
		metrics.RecordRPCCallLatency("RestoreXPUOnNode", errCode, time.Since(startTime).Seconds())
	}()

	response, err := c.serviceClient.RestoreXPUOnNode(ctx, req)
	if err != nil {
		return err
	}
	if response.Status != status.StatusOK {
		return fmt.Errorf("restore xpu failed: %s", response.Message)
	}
	return nil
}

func (c *DefaultClient) CleanTask(ctx context.Context, req *CleanTaskRequest) (err error) {
	startTime := time.Now()
	defer func() {
		errCode := "0"
		if err == nil {
			errCode = "1"
		}
		metrics.RecordRPCCallLatency("CleanTask", errCode, time.Since(startTime).Seconds())
	}()

	response, err := c.serviceClient.CleanTask(ctx, req)
	if err != nil {
		return err
	}
	if response.Status != status.StatusOK {
		return fmt.Errorf("clean task failed: %s", response.Message)
	}
	return nil
}

func (c *DefaultClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
