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
	"github.com/lj-runtimekit/exporter/pkg/common"
	xdmgrpc "github.com/lj-runtimekit/exporter/pkg/grpc/xdm"

	"google.golang.org/grpc"
)

// Resource names are externalized to a ConfigMap (see loader.go). They are
// declared as var so LoadProtocol can override them at startup; the compiled-in
// values remain the fallback defaults.
var (
	nvidiaResourceName      = "nvidia.com/gpu"
	gpuResourceName         = "aliyun.com/gpu"
	nvidiaMigResourcePrefix = "nvidia.com/mig-"
)

const (
	NanValue = "N/A"
)

// GrpcErrCode represents gRPC error status codes.
type GrpcErrCode int32

const (
	GrpcRetSuccess            GrpcErrCode = 0x00000000
	GrpcErrCodeSrvUnavailable GrpcErrCode = 0x00000001
	GrpcErrCodeCallTimeout    GrpcErrCode = 0x00000002
	GrpcErrCodeUnExpected     GrpcErrCode = 0x00000003
	GrpcErrCodeUnimplemented  GrpcErrCode = 0x00000004
	GrpcErrCodeSrcInternal    GrpcErrCode = 0x00000005
)

func grpcErrString(ret GrpcErrCode) string {
	switch ret {
	case GrpcRetSuccess:
		return "success"
	case GrpcErrCodeSrvUnavailable:
		return "server unavailable"
	case GrpcErrCodeCallTimeout:
		return "timeout"
	case GrpcErrCodeUnExpected:
		return "unexpected"
	case GrpcErrCodeUnimplemented:
		return "api unimplemented"
	default:
		return "unknown"
	}
}

// PodAttrType identifies pod attribute fields.
type PodAttrType int

const (
	PodAttrName PodAttrType = iota + 1
	PodAttrNamespace
	PodAttrContainer
	PodAttrPhase
	PodAttrContainerID
	PodAttrMax
)

// DataAcqMode constants define data acquisition modes.
const (
	protocol = "unix:"

	DataAcqPull = "data-pull"
	DataAcqPush = "data-push"

	OutputFmtProm     = "prom"
	OutputFmtKubergpu = "kubergpu"

	OutputTypeHttpPull = "http-pull"
	OutputTypeHttpPush = "http-push"

	PodCollectTypeNone        = "none"
	PodCollectTypeKubeletUds  = "kubelet-uds"
	PodCollectTypeKubeletHttp = "kubelet-http"
)

// DeviceOptions specifies which GPUs and GPU instances to monitor.
type DeviceOptions struct {
	Flex             bool  // If true, then monitor all GPUs if MIG mode is disabled or all GPU instances if MIG is enabled.
	GpuRange         []int // The indices of each GPU to monitor, or -1 to monitor all
	GpuInstanceRange []int // The indices of each GPU instance to monitor, or -1 to monitor all
}

// Config holds all configuration parameters for the exporter.
type Config struct {
	CollectorsFile  string
	Address         string
	CollectInterval int

	ExporterDataAcqMode      string
	ExporterKubeletPort      uint
	ExporterPodCollectorType string
	ExporterOutputFormat     string
	ExporterOutputType       string

	XdmGrpcPath             string
	GpuChangeDetectInterval int
}

// XdmGrpcClient manages the gRPC connection to the XDM service.
type XdmGrpcClient struct {
	client      xdmgrpc.XpuDeviceManagerServiceClient
	conn        *grpc.ClientConn
	config      *Config
	hostInfo    *HostInfo
	counterMaps map[int32][]common.Counter
	fieldIds    []int32
	exportType  xdmgrpc.ExporterType
}

// GpuInfo holds metadata for a single GPU device.
type GpuInfo struct {
	GPU    int    `json:"index"`
	UUID   string `json:"uuid"`
	Model  string `json:"model"`
	Serial string `json:"serial"`
}

// HostInfo holds metadata about the host node.
type HostInfo struct {
	HostName string `json:"hostName,omitempty"`
	NodeName string `json:"nodeName,omitempty"`
	HostSN   string `json:"hostSN,omitempty"`
	NodeIP   string `json:"nodeIP,omitempty"`
}

// SystemGpuInfo holds the GPU driver version and per-device information.
type SystemGpuInfo struct {
	Driver   string    `json:"driver"`
	GpuCount int       `json:"gpuCount"`
	Gpus     []GpuInfo `json:"gpus"`
}

// SystemInfo aggregates host and GPU system information.
type SystemInfo struct {
	Host HostInfo      `json:"host"`
	Gpu  SystemGpuInfo `json:"gpu"`
}

// GetSystemInfo creates a new SystemInfo with host information populated.
func GetSystemInfo() *SystemInfo {
	sysInfo := &SystemInfo{
		Host: GetHostInfo(),
		Gpu:  SystemGpuInfo{},
	}

	return sysInfo
}

// PodMapper enriches metrics with Kubernetes pod information.
type PodMapper interface {
	Process(metrics []common.MetricItem) ([]common.MetricItem, error)
}

// PodInfoInterface defines the interface for retrieving GPU pod information.
type PodInfoInterface interface {
	GetGpuPodInfo() ([]PodInfo, error)
	Name() string
}

// PodInfo holds metadata about a pod running on a GPU device.
type PodInfo struct {
	Full          bool
	Name          string
	Namespace     string
	Phase         string
	ContainerName string
	ContainerId   string
	DeviceId      string
}

func (p PodInfo) GetVal(attr PodAttrType) string {
	switch attr {
	case PodAttrName:
		return p.Name
	case PodAttrNamespace:
		return p.Namespace
	case PodAttrContainer:
		return p.ContainerName
	case PodAttrPhase:
		return p.Phase
	case PodAttrContainerID:
		return p.ContainerId
	default:
		return "N/A"
	}
}

// MetricStream wraps a gRPC stream for receiving metric data.
type MetricStream struct {
	stream      xdmgrpc.XpuDeviceManagerService_ExporterDataStreamClient
	hostInfo    *HostInfo
	counterMaps map[int32][]common.Counter
}

// MetricConfig holds the configuration for metric collection.
type MetricConfig struct {
	module      int32
	interval    int32
	mode        int32
	SuptFieldID []int32
}

func (m *MetricStream) Read() ([]common.MetricItem, error) {
	list, err := m.stream.Recv()
	if err != nil {
		return nil, err
	}

	return transform(m.counterMaps, list, m.hostInfo), nil
}

func transform(counterMaps map[int32][]common.Counter, list *xdmgrpc.ExporterDataResponse, hostInfo *HostInfo) []common.MetricItem {
	metrics := make([]common.MetricItem, 0)
	for _, item := range list.DataList {
		if counters, ok := counterMaps[item.FieldID]; ok {
			for i, _ := range counters {
				migUUid := ""
				if item.Labels != nil && item.Labels["mig_uuid"] != "" {
					migUUid = item.Labels["mig_uuid"]
				}

				metric := common.MetricItem{
					Counter:       &counterMaps[item.FieldID][i],
					Egpu:          item.EGpu,
					Full:          item.Full,
					Time:          item.Time,
					Value:         item.Value,
					ID:            item.Id,
					UUID:          item.Uuid,
					ModelName:     item.Model,
					MigProfile:    item.MigProf,
					GPUInstanceID: item.GiID,
					ComputeId:     item.CiID,
					Message:       item.ExtraMsg,
					Cid:           item.CId,
					MigUUID:       migUUid,
					Pid:           item.Pid,
					Pname:         item.PName,
					Cgroup:        item.Cgroup,
					RcvLabels:     item.Labels,
					HostName:      hostInfo.HostName,
					NodeName:      hostInfo.NodeName,
					HostSN:        hostInfo.HostSN,
					NodeIP:        hostInfo.NodeIP,
					//RundPodName:      hostInfo.RundPodName,
					//RundPodNameSpace: hostInfo.RundPodNameSpace,
				}
				metrics = append(metrics, metric)
			}
		}
	}

	return metrics
}
