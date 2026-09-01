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

package gpu

import (
	"context"
	"errors"
	"io"
	"testing"

	xpugrpc "github.com/lj-runtimekit/device-plugin/internal/grpc"
	"github.com/lj-runtimekit/device-plugin/internal/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// MockXpuDeviceManagerServiceClient 是对 gRPC 客户端接口的模拟实现
type MockXpuDeviceManagerServiceClient struct {
	mock.Mock
}

func (m *MockXpuDeviceManagerServiceClient) Allocate(ctx context.Context, in *xpugrpc.AllocateRequest, opts ...grpc.CallOption) (*xpugrpc.AllocateResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MockXpuDeviceManagerServiceClient) ListSupportedXPUType(ctx context.Context, in *xpugrpc.XPUTypeRequest, opts ...grpc.CallOption) (*xpugrpc.XPUTypeResponse, error) {
	args := m.Called(ctx, in)
	resp, _ := args.Get(0).(*xpugrpc.XPUTypeResponse)
	return resp, args.Error(1)
}

func (m *MockXpuDeviceManagerServiceClient) ListAndWatch(ctx context.Context, in *xpugrpc.ListAndWatchRequest, opts ...grpc.CallOption) (xpugrpc.XpuDeviceManagerService_ListAndWatchClient, error) {
	args := m.Called(ctx, in)
	stream, _ := args.Get(0).(xpugrpc.XpuDeviceManagerService_ListAndWatchClient)
	return stream, args.Error(1)
}

func (m *MockXpuDeviceManagerServiceClient) ListInitialEnv(ctx context.Context, in *xpugrpc.ListInitialEnvRequest, opts ...grpc.CallOption) (*xpugrpc.ListInitialEnvResponse, error) {
	args := m.Called(ctx, in)
	resp, _ := args.Get(0).(*xpugrpc.ListInitialEnvResponse)
	return resp, args.Error(1)
}

func (m *MockXpuDeviceManagerServiceClient) Release(ctx context.Context, in *xpugrpc.ReleaseRequest, opts ...grpc.CallOption) (*xpugrpc.ReleaseResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MockXpuDeviceManagerServiceClient) ExporterGetConfig(ctx context.Context, in *xpugrpc.ExporterRequest, opts ...grpc.CallOption) (*xpugrpc.ExporterConfigResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MockXpuDeviceManagerServiceClient) ExporterGetStatus(ctx context.Context, in *xpugrpc.ExporterRequest, opts ...grpc.CallOption) (*xpugrpc.ExporterStatusResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MockXpuDeviceManagerServiceClient) ExporterGetDataList(ctx context.Context, in *xpugrpc.ExporterDataRequest, opts ...grpc.CallOption) (*xpugrpc.ExporterDataResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MockXpuDeviceManagerServiceClient) ExporterDataStream(ctx context.Context, in *xpugrpc.ExporterDataRequest, opts ...grpc.CallOption) (xpugrpc.XpuDeviceManagerService_ExporterDataStreamClient, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MockXpuDeviceManagerServiceClient) ListGpuInfo(ctx context.Context, in *xpugrpc.ExporterRequest, opts ...grpc.CallOption) (*xpugrpc.ListGpuInfoResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MockXpuDeviceManagerServiceClient) Checkpoint(ctx context.Context, in *xpugrpc.CheckpointRequest, opts ...grpc.CallOption) (xpugrpc.XpuDeviceManagerService_CheckpointClient, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MockXpuDeviceManagerServiceClient) RestoreCPU(ctx context.Context, in *xpugrpc.RestoreCPURequest, opts ...grpc.CallOption) (*xpugrpc.RestoreCPUResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MockXpuDeviceManagerServiceClient) RestoreXPU(ctx context.Context, in *xpugrpc.RestoreXPURequest, opts ...grpc.CallOption) (*xpugrpc.RestoreXPUResponse, error) {
	//TODO implement me
	panic("implement me")
}

// MockStream 模拟 ListAndWatch 的流式响应
type MockStream struct {
	mock.Mock
	recvCallCount int
	recvFuncs     []func() (*xpugrpc.ListAndWatchResponse, error)
}

func (m *MockStream) Recv() (*xpugrpc.ListAndWatchResponse, error) {
	if m.recvCallCount < len(m.recvFuncs) {
		f := m.recvFuncs[m.recvCallCount]
		m.recvCallCount++
		return f()
	}
	// Default: return EOF if no more funcs
	return nil, io.EOF
}

func (m *MockStream) CloseSend() error {
	panic("implement me")
}

func (m *MockStream) Header() (metadata.MD, error) {
	panic("implement me")
}

func (m *MockStream) Trailer() metadata.MD {
	panic("implement me")
}

func (m *MockStream) Context() context.Context {
	panic("implement me")
}

func (m *MockStream) SendMsg(interface{}) error {
	panic("implement me")
}

func (m *MockStream) RecvMsg(interface{}) error {
	panic("implement me")
}

// --- Unit Tests ---

func TestGetDeviceType_Success(t *testing.T) {
	client := new(MockXpuDeviceManagerServiceClient)
	req := &xpugrpc.XPUTypeRequest{}
	resp := &xpugrpc.XPUTypeResponse{
		Status:        StatusOK,
		SupportedType: []xpugrpc.XPUType{xpugrpc.XPUType_GPU},
	}
	client.On("ListSupportedXPUType", mock.Anything, req).Return(resp, nil)

	result, err := GetDeviceType(client)
	assert.NoError(t, err)
	assert.Equal(t, utils.DeviceTypeGPU, result)
}

func TestGetDeviceType_GRPCError(t *testing.T) {
	client := new(MockXpuDeviceManagerServiceClient)
	req := &xpugrpc.XPUTypeRequest{}
	client.On("ListSupportedXPUType", mock.Anything, req).Return((*xpugrpc.XPUTypeResponse)(nil), errors.New("network error"))

	_, err := GetDeviceType(client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to list supported XPU types")
}

func TestGetDeviceType_NilResponse(t *testing.T) {
	client := new(MockXpuDeviceManagerServiceClient)
	req := &xpugrpc.XPUTypeRequest{}
	client.On("ListSupportedXPUType", mock.Anything, req).Return((*xpugrpc.XPUTypeResponse)(nil), nil)

	_, err := GetDeviceType(client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "received nil response")
}

func TestGetDeviceType_StatusNotOK(t *testing.T) {
	client := new(MockXpuDeviceManagerServiceClient)
	req := &xpugrpc.XPUTypeRequest{}
	resp := &xpugrpc.XPUTypeResponse{
		Status:        StatusInternalError,
		SupportedType: []xpugrpc.XPUType{},
	}
	client.On("ListSupportedXPUType", mock.Anything, req).Return(resp, nil)

	_, err := GetDeviceType(client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "non-OK status")
}

func TestGetDeviceType_EmptyTypes(t *testing.T) {
	client := new(MockXpuDeviceManagerServiceClient)
	req := &xpugrpc.XPUTypeRequest{}
	resp := &xpugrpc.XPUTypeResponse{
		Status:        StatusOK,
		SupportedType: []xpugrpc.XPUType{},
	}
	client.On("ListSupportedXPUType", mock.Anything, req).Return(resp, nil)

	_, err := GetDeviceType(client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "XPU type array is empty")
}

func TestGetDeviceType_UnsupportedType(t *testing.T) {
	client := new(MockXpuDeviceManagerServiceClient)
	req := &xpugrpc.XPUTypeRequest{}
	resp := &xpugrpc.XPUTypeResponse{
		Status:        StatusOK,
		SupportedType: []xpugrpc.XPUType{999}, // Unsupported type
	}
	client.On("ListSupportedXPUType", mock.Anything, req).Return(resp, nil)

	_, err := GetDeviceType(client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported device type")
}

func TestGrpcListAndWatch_Success(t *testing.T) {
	client := new(MockXpuDeviceManagerServiceClient)
	stream := &MockStream{
		recvFuncs: []func() (*xpugrpc.ListAndWatchResponse, error){
			func() (*xpugrpc.ListAndWatchResponse, error) {
				// GrpcListAndWatch keys the cache by the device minor read from
				// the extender resources, a device without it is skipped
				return &xpugrpc.ListAndWatchResponse{Devices: []*xpugrpc.Device{
					{Id: "dev1", ExtenderResources: map[string]string{utils.DeviceMinor: "0"}},
					{Id: "dev2", ExtenderResources: map[string]string{utils.DeviceMinor: "1"}},
				}}, nil
			},
		},
	}
	cache := utils.NewDeviceMeta()

	client.On("ListAndWatch", mock.Anything, mock.Anything).Return(stream, nil)

	GrpcListAndWatch(client, cache)

	assert.Len(t, cache.Cache, 2)
	assert.Equal(t, "dev1", cache.Cache[0].Id)
	assert.Equal(t, "dev2", cache.Cache[1].Id)
}

func TestGrpcListAndWatch_StreamEOF(t *testing.T) {
	client := new(MockXpuDeviceManagerServiceClient)
	stream := &MockStream{
		recvFuncs: []func() (*xpugrpc.ListAndWatchResponse, error){
			func() (*xpugrpc.ListAndWatchResponse, error) {
				return nil, io.EOF
			},
		},
	}
	cache := utils.NewDeviceMeta()

	client.On("ListAndWatch", mock.Anything, mock.Anything).Return(stream, nil)

	GrpcListAndWatch(client, cache)

	assert.Empty(t, cache.Cache)
}

func TestGrpcListAndWatch_RecvError(t *testing.T) {
	client := new(MockXpuDeviceManagerServiceClient)
	stream := &MockStream{
		recvFuncs: []func() (*xpugrpc.ListAndWatchResponse, error){
			func() (*xpugrpc.ListAndWatchResponse, error) {
				return nil, errors.New("some error")
			},
			func() (*xpugrpc.ListAndWatchResponse, error) {
				return nil, io.EOF
			},
		},
	}
	cache := utils.NewDeviceMeta()

	client.On("ListAndWatch", mock.Anything, mock.Anything).Return(stream, nil)

	GrpcListAndWatch(client, cache)

	assert.Empty(t, cache.Cache)
}

func TestGrpcListAndWatch_NilResponse(t *testing.T) {
	client := new(MockXpuDeviceManagerServiceClient)
	stream := &MockStream{
		recvFuncs: []func() (*xpugrpc.ListAndWatchResponse, error){
			func() (*xpugrpc.ListAndWatchResponse, error) {
				return nil, nil
			},
			func() (*xpugrpc.ListAndWatchResponse, error) {
				return nil, io.EOF
			},
		},
	}
	cache := utils.NewDeviceMeta()

	client.On("ListAndWatch", mock.Anything, mock.Anything).Return(stream, nil)

	GrpcListAndWatch(client, cache)

	assert.Empty(t, cache.Cache)
}

func TestGrpcListAndWatch_EmptyDevices(t *testing.T) {
	client := new(MockXpuDeviceManagerServiceClient)
	stream := &MockStream{
		recvFuncs: []func() (*xpugrpc.ListAndWatchResponse, error){
			func() (*xpugrpc.ListAndWatchResponse, error) {
				return &xpugrpc.ListAndWatchResponse{Devices: []*xpugrpc.Device{}}, nil
			},
			func() (*xpugrpc.ListAndWatchResponse, error) {
				return nil, io.EOF
			},
		},
	}
	cache := utils.NewDeviceMeta()

	client.On("ListAndWatch", mock.Anything, mock.Anything).Return(stream, nil)

	GrpcListAndWatch(client, cache)

	assert.Empty(t, cache.Cache)
}
