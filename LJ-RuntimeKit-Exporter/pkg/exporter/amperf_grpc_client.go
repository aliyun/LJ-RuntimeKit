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

package exporter

import (
	"context"
	"time"

	"github.com/lj-runtimekit/exporter/pkg/common"
	xdmgrpc "github.com/lj-runtimekit/exporter/pkg/grpc/xdm"

	"google.golang.org/grpc"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

const (
	StatusOK = 0
)

// NewXdmGrpcClient creates a new gRPC client for communicating with the XDM service.
func NewXdmGrpcClient(config *Config, info *HostInfo, exportType xdmgrpc.ExporterType) (*XdmGrpcClient, error) {
	uds := protocol + config.XdmGrpcPath
	conn, err := grpc.Dial(uds, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return &XdmGrpcClient{
		client:     xdmgrpc.NewXpuDeviceManagerServiceClient(conn),
		conn:       conn,
		config:     config,
		hostInfo:   info,
		exportType: exportType,
	}, nil
}

func (c *XdmGrpcClient) GetConfig() (*MetricConfig, GrpcErrCode) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	req := &xdmgrpc.ExporterRequest{
		ExporterType: c.exportType,
	}

	config, err := c.client.ExporterGetConfig(ctx, req)
	if err != nil {
		if rpcErr, ok := grpcstatus.FromError(err); ok {
			if rpcErr.Code() == grpccodes.Unavailable {
				return nil, GrpcErrCodeSrvUnavailable
			} else if rpcErr.Code() == grpccodes.Canceled ||
				rpcErr.Code() == grpccodes.DeadlineExceeded {
				return nil, GrpcErrCodeCallTimeout
			} else {
				return nil, GrpcErrCodeUnExpected
			}
		}

		return nil, GrpcErrCodeUnExpected
	}

	if config.Status != StatusOK {
		return nil, GrpcErrCodeSrcInternal
	}

	return &MetricConfig{
		module:      int32(config.ExporterType),
		interval:    config.Interval,
		mode:        int32(config.RunningMode),
		SuptFieldID: config.SuptFieldID,
	}, GrpcRetSuccess
}

func (c *XdmGrpcClient) GetStatus() (*xdmgrpc.ExporterStatusResponse, GrpcErrCode) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	req := &xdmgrpc.ExporterRequest{
		ExporterType: c.exportType,
	}
	status, err := c.client.ExporterGetStatus(ctx, req)
	if err != nil {
		if rpcErr, ok := grpcstatus.FromError(err); ok {
			if rpcErr.Code() == grpccodes.Unavailable {
				return nil, GrpcErrCodeSrvUnavailable
			} else if rpcErr.Code() == grpccodes.Canceled ||
				rpcErr.Code() == grpccodes.DeadlineExceeded {
				return nil, GrpcErrCodeCallTimeout
			} else {
				return nil, GrpcErrCodeUnExpected
			}
		}

		return nil, GrpcErrCodeUnExpected
	}

	if status.Status != StatusOK {
		return nil, GrpcErrCodeSrcInternal
	}

	return status, GrpcRetSuccess
}

func (c *XdmGrpcClient) GetGpuInfos() (*xdmgrpc.ListGpuInfoResponse, GrpcErrCode) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	req := &xdmgrpc.ExporterRequest{
		ExporterType: c.exportType,
	}

	infos, err := c.client.ListGpuInfo(ctx, req)
	if err != nil {
		if rpcErr, ok := grpcstatus.FromError(err); ok {
			switch rpcErr.Code() {
			case grpccodes.Unavailable:
				return nil, GrpcErrCodeSrvUnavailable
			case grpccodes.Canceled, grpccodes.DeadlineExceeded:
				return nil, GrpcErrCodeCallTimeout
			case grpccodes.Unimplemented:
				return nil, GrpcErrCodeUnimplemented
			default:
				return nil, GrpcErrCodeUnExpected
			}
		}
		return nil, GrpcErrCodeUnExpected
	}

	if infos.Status != StatusOK {
		return nil, GrpcErrCodeSrcInternal
	}

	return infos, GrpcRetSuccess
}

func (c *XdmGrpcClient) GetMetrics() ([]common.MetricItem, GrpcErrCode) {
	req := &xdmgrpc.ExporterDataRequest{
		ExporterType: c.exportType,
		FieldID:      c.fieldIds,
		Source:       xdmgrpc.ExporterDataSource_CacheData,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	metrics, err := c.client.ExporterGetDataList(ctx, req)
	if err != nil {
		if rpcErr, ok := grpcstatus.FromError(err); ok {
			if rpcErr.Code() == grpccodes.Unavailable {
				return []common.MetricItem{}, GrpcErrCodeSrvUnavailable
			} else if rpcErr.Code() == grpccodes.Canceled ||
				rpcErr.Code() == grpccodes.DeadlineExceeded {
				return []common.MetricItem{}, GrpcErrCodeCallTimeout
			} else {
				return []common.MetricItem{}, GrpcErrCodeUnExpected
			}
		}

		return []common.MetricItem{}, GrpcErrCodeUnExpected
	}

	if metrics.Status != StatusOK {
		return []common.MetricItem{}, GrpcErrCodeSrcInternal
	}

	return transform(c.counterMaps, metrics, c.hostInfo), GrpcRetSuccess
}

func (c *XdmGrpcClient) GetMetricsStream(counters []int32, counterMaps map[int32][]common.Counter) (*MetricStream, func(), GrpcErrCode) {
	req := &xdmgrpc.ExporterDataRequest{
		Source:       xdmgrpc.ExporterDataSource_CacheData,
		FieldID:      counters,
		ExporterType: c.exportType,
	}

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := c.client.ExporterDataStream(ctx, req)
	if err != nil {
		if rpcErr, ok := grpcstatus.FromError(err); ok {
			if rpcErr.Code() == grpccodes.Unavailable {
				cancel()
				return nil, func() {}, GrpcErrCodeSrvUnavailable
			} else if rpcErr.Code() == grpccodes.Canceled ||
				rpcErr.Code() == grpccodes.DeadlineExceeded {
				cancel()
				return nil, func() {}, GrpcErrCodeCallTimeout
			} else {
				cancel()
				return nil, func() {}, GrpcErrCodeUnExpected
			}
		}
		cancel()
		return nil, func() {}, GrpcErrCodeUnExpected
	}

	return &MetricStream{
			stream:      stream,
			counterMaps: counterMaps,
			hostInfo:    c.hostInfo,
		}, func() {
			cancel()
		}, GrpcRetSuccess
}

func (c *XdmGrpcClient) RegisterCounters(counters []int32, counterMaps map[int32][]common.Counter) {
	c.fieldIds = counters
	c.counterMaps = counterMaps
}
