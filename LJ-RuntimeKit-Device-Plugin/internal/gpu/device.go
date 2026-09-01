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
	"fmt"
	"io"
	"strconv"
	"time"

	xpugrpc "github.com/lj-runtimekit/device-plugin/internal/grpc"
	"github.com/lj-runtimekit/device-plugin/internal/utils"

	"k8s.io/klog/v2"
)

func GetDeviceType(client xpugrpc.XpuDeviceManagerServiceClient) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	req := &xpugrpc.XPUTypeRequest{}

	resp, err := client.ListSupportedXPUType(ctx, req)
	if err != nil {
		return "", fmt.Errorf("failed to list supported XPU types: %w", err)
	}

	if resp == nil {
		return "", fmt.Errorf("received nil response from XPU type service")
	}

	if resp.Status != StatusOK {
		return "", fmt.Errorf("non-OK status from XPU type service: %v", resp.Status)
	}
	if len(resp.SupportedType) <= 0 {
		return "", fmt.Errorf("XPU type array is empty")
	}

	var xpuType string
	switch resp.SupportedType[0] {
	case xpugrpc.XPUType_GPU:
		xpuType = utils.DeviceTypeGPU
	case xpugrpc.XPUType_PPU:
		xpuType = utils.DeviceTypePPU
	case xpugrpc.XPUType_AMD:
		xpuType = utils.DeviceTypeAMD
	default:
		return "", fmt.Errorf("unsupported device type")
	}
	return xpuType, nil
}

func GrpcListAndWatch(client xpugrpc.XpuDeviceManagerServiceClient, deviceCache *utils.DeviceMeta) {
	klog.Infof("starting xdm listAndWatch gRPC")
	req := &xpugrpc.ListAndWatchRequest{}

	stream, err := client.ListAndWatch(context.Background(), req)
	if err != nil {
		klog.Errorf("failed to establish ListAndWatch stream: %v", err)
	}
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			klog.Errorf("gRPC stream closed by server")
			break
		}
		if err != nil {
			klog.Errorf("ListAndWatch stream error: %v", err)
			continue
		}

		if resp == nil {
			klog.Errorf("Received nil response from stream")
			continue
		}

		if len(resp.Devices) == 0 {
			klog.Errorf("No devices in response")
			continue
		}
		for _, d := range resp.Devices {
			minorStr := d.ExtenderResources[utils.DeviceMinor]
			minor, err := strconv.Atoi(minorStr)
			if err != nil {
				klog.Errorf("failed to get device %s minor: %v", d.Id, err)
			} else {
				deviceCache.Put(minor, d)
				// print the raw abstract topology json as reported by xdm so it
				// can be verified against the protocol definition
				if topo, ok := d.ExtenderResources[utils.TopologyKey]; ok && topo != "" {
					klog.Infof("listAndWatch device %s (minor %d) reported topology: %s", d.Id, minor, topo)
				}
			}
		}

		klog.V(4).Infof("listAndWatch received device data %v", deviceCache.Cache)
		break
	}

	klog.Info("listAndWatch client exiting after receiving data")
}
