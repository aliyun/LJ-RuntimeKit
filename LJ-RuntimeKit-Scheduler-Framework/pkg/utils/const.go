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

import (
	"errors"
)

type MigDeviceType string

const (
	TypeMigCi MigDeviceType = "ci"
	TypeMigGi MigDeviceType = "gi"
)

var (
	ResourceMemory = "aliyun.com/gpu-memory"
	ResourceCore   = "aliyun.com/gpu-core"
	ResourceSlice  = "aliyun.com/gpu-slice"
	ResourceGPU    = "aliyun.com/gpu"
	ResourceNvidia = "nvidia.com/gpu"
	ResourceCount  = "aliyun.com/gpu-count"

	ResourceDriverVersion = "aliyun.com/driver-version"
	MajorDriverVersion525 = 525

	AnnoDeviceIndex         = "ALIYUN_COM_GPU_IDX_LIST"    // Device IDs patched by scheduler, indicating which GPU cards the pod uses.
	AnnoCoreResourceByPod   = "ALIYUN_COM_GPU_COMPUTE_POD" // Requested GPU compute per pod.
	AnnoMemoryResourceByPod = "ALIYUN_COM_GPU_MEM_POD"     // Requested GPU memory per pod.

	AnnotationSliceAssignedFlag = "ALIYUN_COM_GPU_SLICE_ASSIGNED"
	AnnoMemoryAssignedFlag      = "ALIYUN_COM_GPU_MEM_ASSIGNED"    // Whether GPU memory has been handled by the device plugin.
	AnnoCoreAssignedFlag        = "ALIYUN_COM_GPU_CORE_ASSIGNED"   // Whether GPU compute has been handled by the device plugin.
	AnnoNormalGpuAssignedFlag   = "ALIYUN_COM_GPU_ASSIGNED"        // Whether the whole GPU card has been handled by the device plugin.
	AnnoResourceAssumeTime      = "ALIYUN_COM_GPU_MEM_ASSUME_TIME" // Timestamp when scheduling completed.

	LabelNodeScheduleStrategy = "node.gpu.placement" // Node scheduling strategy label.
	ScheduleStrategySpread    = "spread"             // Spread scheduling strategy.
	ScheduleStrategyBinpack   = "binpack"            // Binpack scheduling strategy.

	LabelsReplicas = "replicas"

	// Gang scheduling node topology labels.
	LabelNodeLeafSwitchId = "aliyun.com/leaf-switch-id" // Leaf Switch domain.
	LabelNodeHyperNodeId  = "aliyun.com/hypernode-id"   // HyperNode domain.
	LabelNodeNetworkPod   = "aliyun.com/network-pod"    // network-pod domain.

	// JobSet-injected labels used to identify a gang group (one Job per replica).
	LabelJobSetName          = "jobset.sigs.k8s.io/jobset-name"        // JobSet name.
	LabelJobSetReplicatedJob = "jobset.sigs.k8s.io/replicatedjob-name" // ReplicatedJob name.
	LabelJobSetJobIndex      = "jobset.sigs.k8s.io/job-index"          // Job replica index.

	// Number of pods in the Job; the gang is released when all arrive.
	LabelRuntimeKitMinMember = "aliyun.com/min-member"

	AnnoMigDevMask         = "aliyun.com/mig-mask"           // MIG resource mask.
	AnnoMigTopoStart       = "aliyun.com/mig-topology-start" // MIG resource topology start position.
	AnnoMigTopoSize        = "aliyun.com/mig-topology-size"  // MIG resource topology size.
	AnnoMigDeviceEnableKey = "aliyun.com/device-enable"      // Whether the pod requests MIG resources.
	AnnoMigDeviceType      = "aliyun.com/mig-type"           // MIG resource type of the pod (ci/gi).
	AnnoMigMutilDevice     = "aliyun.com/mig-mutil-device"
	AnnotationDevice       = "aliyun.com/device"

	MigProfileSize1 = 1 // MIG profile with 1 compute instance.
	MigProfileSize2 = 2 // MIG profile with 2 compute instances.
	MigProfileSize3 = 3 // MIG profile with 3 compute instances.
	MigProfileSize4 = 4 // MIG profile with 4 compute instances.
	MigProfileSize7 = 7 // MIG profile with 7 compute instances.

	DeviceHealth = "Healthy"
)

// RDMA topology constants.
var (
	AnnotationRdmaTopo = "aliyun.com/gpu-rdma-topo"
	ResourceRDMA       = "aliyun.com/rdma"
	// Selected PCIe switch indexes (comma-separated, e.g. "0,1") passed to xdm.
	AnnotationPcieSwitchIndex = "ALIYUN_COM_PCIESWITCH_INDEX"
)

var NotFoundErr = errors.New("cannot found the value from the key")
