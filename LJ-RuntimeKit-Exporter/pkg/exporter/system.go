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
	"os"

	"github.com/lj-runtimekit/exporter/pkg/common"

	"github.com/sirupsen/logrus"
)

// GetHostInfo retrieves host-level information from the environment.
func GetHostInfo() HostInfo {
	var err error
	var host HostInfo
	host.HostName, _ = os.Hostname()
	host.NodeName = os.Getenv("NODE_NAME")
	host.NodeIP = os.Getenv("HOST_IP")

	host.HostSN, err = common.GetHostSerialNumber()
	if err != nil {
		logrus.Warningf("failed to get host sn:%v", err)
	}
	return host
}

func getSystemInfoByGrpc(client *XdmGrpcClient) (*SystemGpuInfo, GrpcErrCode) {
	if client == nil {
		return nil, GrpcErrCodeUnExpected
	}
	sysInfo := SystemGpuInfo{}
	gpuInfos, ret := client.GetGpuInfos()
	switch ret {
	case GrpcRetSuccess:
		sysInfo.GpuCount = len(gpuInfos.Gpu)
		sysInfo.Driver = gpuInfos.DriverVersion
		for _, gpu := range gpuInfos.Gpu {
			sysInfo.Gpus = append(sysInfo.Gpus, GpuInfo{
				GPU:    int(gpu.GpuID),
				UUID:   gpu.Uuid,
				Model:  gpu.ModelName,
				Serial: gpu.SerialNumber,
			})
		}
		return &sysInfo, ret
	default:
		return nil, ret
	}
}

// checkGpuInfoChange reports whether the GPU info has changed between old and new states.
func checkGpuInfoChange(oldInfo *SystemGpuInfo, newInfo *SystemGpuInfo) bool {
	if oldInfo == nil || newInfo == nil {
		return false
	}
	if oldInfo.GpuCount != newInfo.GpuCount {
		return true
	}

	oGpuMap := make(map[int]GpuInfo)
	nGpuMap := make(map[int]GpuInfo)

	for i := 0; i < oldInfo.GpuCount; i++ {
		oGpuMap[oldInfo.Gpus[i].GPU] = oldInfo.Gpus[i]
	}
	for i := 0; i < newInfo.GpuCount; i++ {
		nGpuMap[newInfo.Gpus[i].GPU] = newInfo.Gpus[i]
	}
	for gpuId, oGpu := range oGpuMap {
		nGpu, ok := nGpuMap[gpuId]
		if !ok {
			return true
		}
		if oGpu != nGpu {
			return true
		}
	}
	return false
}
