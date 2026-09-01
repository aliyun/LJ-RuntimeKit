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

package utils

import pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"

type MemoryUnit string

type MigDeviceType string

const (
	TypeMigCi MigDeviceType = "ci"
	TypeMigGi MigDeviceType = "gi"
)

// DefaultResourcePossibility is a compile-time numeric constant and must stay
// const (used in unsigned integer comparisons).
const DefaultResourcePossibility = 100

// WholeCardCore is the gpu-core amount equal to one whole card; such requests
// are flagged by the scheduler with the whole card assigned flag.
const WholeCardCore = 100

// Resource names and related constants. Protocol-facing names are vars so
// LoadProtocol can override them from the ConfigMap.
var (
	ResourceMemory      = "aliyun.com/gpu-memory"
	ResourceCore        = "aliyun.com/gpu-core"
	ResourceSlice       = "aliyun.com/gpu-slice"
	ResourceGPU         = "aliyun.com/gpu"
	ResourceNvidia      = "nvidia.com/gpu"
	ResourcePossibility = "aliyun.com/possibility"
	ResourceGPUCount    = "aliyun.com/gpu-count"

	EnvNvGPU       = "NVIDIA_VISIBLE_DEVICES"
	EnvXpuGPU      = "XPU_VISIBLE_DEVICES"
	EnvDeviceIndex = "ALIYUN_COM_GPU_MEM_IDX"

	// EnvHostIP carries the host ip injected through the downward api
	// (status.hostIP). The checkpoint GC uses it to reach the kubelet
	// readonly port 10255 which is bound on the host network.
	EnvHostIP = "HOST_IP"
	// EnvKubeletPodsEndpoint overrides the whole kubelet readonly pods
	// endpoint url used by the checkpoint GC.
	EnvKubeletPodsEndpoint = "KUBELET_PODS_ENDPOINT"

	AnnotationNormalGpuAssignedFlag   = "ALIYUN_COM_GPU_ASSIGNED"
	AnnotationMemoryAssignedFlag      = "ALIYUN_COM_GPU_MEM_ASSIGNED"
	AnnotationCoreAssignedFlag        = "ALIYUN_COM_GPU_CORE_ASSIGNED"
	AnnotationSliceAssignedFlag       = "ALIYUN_COM_GPU_SLICE_ASSIGNED"
	AnnotationPossibilityAssignedFlag = "ALIYUN_COM_GPU_POSSIBILITY_ASSIGNED"

	AnnotationResourceAssumeTime = "ALIYUN_COM_GPU_MEM_ASSUME_TIME"
	AnnotationDeviceIndex        = "ALIYUN_COM_GPU_IDX_LIST"

	// AnnotationAllocateID carries the allocate id generated in the Allocate
	// stage; it correlates the allocation with the dp checkpoint record.
	AnnotationAllocateID = "xdm.amp.com.id"

	// AnnotationKeyAllocateID is the annotation key used to carry the unique
	// allocate id to xdm inside the gRPC AllocateRequest annotations
	AnnotationKeyAllocateID = "AllocateID"

	LabelsFrontend = "vgpu.frontend"
	LabelsReplicas = "vgpu.replicas"
	LabelsBluffing = "vgpu.bluffing"

	AnnotationFrontendIp   = "ALIYUN_COM_FRONTEND_IP"
	AnnotationFrontendPort = "ALIYUN_COM_FRONTEND_PORT"

	GiBPrefix = MemoryUnit("GiB")
	MiBPrefix = MemoryUnit("128MiB")

	AnnotationMigEnableKey     = "aliyun.com/mig-enable"
	AnnotationMigDeviceCIMask  = "aliyun.com/mig-mask"
	AnnotationMigDeviceCiUUID  = "aliyun.com/mig-ci-uuid"
	AnnotationMigDeviceCiID    = "aliyun.com/mig-ci-id"
	AnnotationMigDeviceGiID    = "aliyun.com/mig-gi-id"
	AnnotationMigTopologyStart = "aliyun.com/mig-topology-start"
	AnnotationMigTopologySize  = "aliyun.com/mig-topology-size"
	AnnotationMigDeviceType    = "aliyun.com/mig-type"

	AllocateMigMask  = "MigMask"
	AllocateMigStart = "MigStart"
	AllocateMigSize  = "MigSize"
	AllocateMigType  = "MigType"
	// AllocatePcieIndex is the device extender key carrying the pcie switch
	// indexes assigned by the scheduler; xdm consumes it to mount the matching
	// RDMA devices into the container
	AllocatePcieIndex = "pcie_index"

	MigEnableKey     = "MigEnable"
	MigEnableValue   = "true"
	MigSliceKey      = "MigSlice"
	MigSlice4        = "4"
	MigSlice7        = "7"
	DeviceMinor      = "DeviceMinor"
	DeviceType       = "DeviceType"
	MigType          = "Mig"
	AnnotationDevice = "aliyun.com/device"

	DriverVersion = "aliyun.com/driver-version"

	// TopologyKey is the extender resource key under which xdm reports per
	// device topology levels.
	TopologyKey = "topology"
)

// RDMA 拓扑相关常量
var (
	AnnotationRdmaTopo         = "aliyun.com/gpu-rdma-topo"
	AnnotationRdmaAllocated    = "ALIYUN_COM_RDMA_DEVICES"
	AnnotationRdmaAssignedFlag = "ALIYUN_COM_RDMA_ASSIGNED"
	NodeLabelLeafSwitchID      = "aliyun.com/leaf-switch-id"
	NodeLabelNetworkPoD        = "aliyun.com/network-pod"
	NodeLabelHyperNodeID       = "aliyun.com/hypernode-id"

	// AnnotationPcieSwitchIndex holds the comma separated pcie switch indexes
	// the scheduler picked for the pod, e.g. "0,1"
	AnnotationPcieSwitchIndex = "ALIYUN_COM_PCIESWITCH_INDEX"

	ResourceRDMA = "aliyun.com/rdma"
)

// RDMA socket path / token count stay compile-time constants.
const (
	RDMASocket     = pluginapi.DevicePluginPath + "aliyun-xpu-rdma.sock"
	RDMATokenCount = 100
)

// Device plugin socket paths
const (
	MemorySocket      = pluginapi.DevicePluginPath + "aliyun-xpu-memory.sock"
	CoreSocket        = pluginapi.DevicePluginPath + "aliyun-xpu-core.sock"
	SliceSocket       = pluginapi.DevicePluginPath + "aliyun-xpu-slice.sock"
	NvidiaSocket      = pluginapi.DevicePluginPath + "nvidia-raw1.sock"
	GPUSocket         = pluginapi.DevicePluginPath + "aliyun-xpu-gpu.sock"
	PossibilitySocket = pluginapi.DevicePluginPath + "aliyun-xpu-possibility.sock"

	XDMGrpcSocket = "/run/amp/eKitC/xdm/sockets/dp2mgr.sock"

	// TODO: temporary for testing, will be removed later
	CountSocket = pluginapi.DevicePluginPath + "nvidia-raw0.sock"
)

// Device types
const (
	DeviceTypeGPU    = "GPU"
	DeviceTypePPU    = "PPU"
	DeviceTypeAMD    = "AMD"
	DeviceTypeMemory = "MEMORY"
	DeviceTypeCore   = "CORE"
	DeviceTypeSlice  = "SLICE"
)

func GetResourceType(deviceType string) string {
	switch deviceType {
	case DeviceTypeCore:
		return ResourceCore
	case DeviceTypeMemory:
		return ResourceMemory
	case DeviceTypeSlice:
		return ResourceSlice
	case DeviceTypeGPU:
		return ResourceGPU
	case DeviceTypePPU:
		return ResourceGPU
	case DeviceTypeAMD:
		return ResourceGPU
	default:
		return ""
	}
}
