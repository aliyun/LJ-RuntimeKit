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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetResourceType(t *testing.T) {
	tests := []struct {
		name       string
		deviceType string
		expected   string
	}{
		{
			name:       "core device type",
			deviceType: DeviceTypeCore,
			expected:   ResourceCore,
		},
		{
			name:       "memory device type",
			deviceType: DeviceTypeMemory,
			expected:   ResourceMemory,
		},
		{
			name:       "slice device type",
			deviceType: DeviceTypeSlice,
			expected:   ResourceSlice,
		},
		{
			name:       "GPU device type",
			deviceType: DeviceTypeGPU,
			expected:   ResourceGPU,
		},
		{
			name:       "PPU device type",
			deviceType: DeviceTypePPU,
			expected:   ResourceGPU,
		},
		{
			name:       "AMD device type",
			deviceType: DeviceTypeAMD,
			expected:   ResourceGPU,
		},
		{
			name:       "unknown device type",
			deviceType: "UNKNOWN",
			expected:   "",
		},
		{
			name:       "empty device type",
			deviceType: "",
			expected:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetResourceType(tt.deviceType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMemoryUnitConstants(t *testing.T) {
	assert.Equal(t, MemoryUnit("GiB"), GiBPrefix)
	assert.Equal(t, MemoryUnit("128MiB"), MiBPrefix)
}

func TestMigDeviceTypeConstants(t *testing.T) {
	assert.Equal(t, MigDeviceType("ci"), TypeMigCi)
	assert.Equal(t, MigDeviceType("gi"), TypeMigGi)
}

func TestResourceNameConstants(t *testing.T) {
	assert.Equal(t, "aliyun.com/gpu-memory", ResourceMemory)
	assert.Equal(t, "aliyun.com/gpu-core", ResourceCore)
	assert.Equal(t, "aliyun.com/gpu-slice", ResourceSlice)
	assert.Equal(t, "aliyun.com/gpu", ResourceGPU)
	assert.Equal(t, "nvidia.com/gpu", ResourceNvidia)
	assert.Equal(t, "aliyun.com/possibility", ResourcePossibility)
	assert.Equal(t, "aliyun.com/gpu-count", ResourceGPUCount)
}

func TestEnvironmentVariableConstants(t *testing.T) {
	assert.Equal(t, "NVIDIA_VISIBLE_DEVICES", EnvNvGPU)
	assert.Equal(t, "ALIYUN_COM_GPU_MEM_IDX", EnvDeviceIndex)
}

func TestAnnotationConstants(t *testing.T) {
	assert.Equal(t, "ALIYUN_COM_GPU_ASSIGNED", AnnotationNormalGpuAssignedFlag)
	assert.Equal(t, "ALIYUN_COM_GPU_MEM_ASSIGNED", AnnotationMemoryAssignedFlag)
	assert.Equal(t, "ALIYUN_COM_GPU_CORE_ASSIGNED", AnnotationCoreAssignedFlag)
	assert.Equal(t, "ALIYUN_COM_GPU_SLICE_ASSIGNED", AnnotationSliceAssignedFlag)
	assert.Equal(t, "ALIYUN_COM_GPU_POSSIBILITY_ASSIGNED", AnnotationPossibilityAssignedFlag)
	assert.Equal(t, "ALIYUN_COM_GPU_MEM_ASSUME_TIME", AnnotationResourceAssumeTime)
	assert.Equal(t, "ALIYUN_COM_GPU_IDX_LIST", AnnotationDeviceIndex)
}

func TestLabelConstants(t *testing.T) {
	assert.Equal(t, "vgpu.frontend", LabelsFrontend)
	assert.Equal(t, "vgpu.replicas", LabelsReplicas)
	assert.Equal(t, "vgpu.bluffing", LabelsBluffing)
}

func TestFrontendAnnotationConstants(t *testing.T) {
	assert.Equal(t, "ALIYUN_COM_FRONTEND_IP", AnnotationFrontendIp)
	assert.Equal(t, "ALIYUN_COM_FRONTEND_PORT", AnnotationFrontendPort)
}

func TestMigAnnotationConstants(t *testing.T) {
	assert.Equal(t, "aliyun.com/mig-enable", AnnotationMigEnableKey)
	assert.Equal(t, "aliyun.com/mig-mask", AnnotationMigDeviceCIMask)
	assert.Equal(t, "aliyun.com/mig-ci-uuid", AnnotationMigDeviceCiUUID)
	assert.Equal(t, "aliyun.com/mig-ci-id", AnnotationMigDeviceCiID)
	assert.Equal(t, "aliyun.com/mig-gi-id", AnnotationMigDeviceGiID)
	assert.Equal(t, "aliyun.com/mig-topology-start", AnnotationMigTopologyStart)
	assert.Equal(t, "aliyun.com/mig-topology-size", AnnotationMigTopologySize)
	assert.Equal(t, "aliyun.com/mig-type", AnnotationMigDeviceType)
}

func TestMigConstants(t *testing.T) {
	assert.Equal(t, int(100), DefaultResourcePossibility)
	assert.Equal(t, "MigMask", AllocateMigMask)
	assert.Equal(t, "MigStart", AllocateMigStart)
	assert.Equal(t, "MigSize", AllocateMigSize)
	assert.Equal(t, "MigType", AllocateMigType)
	assert.Equal(t, "MigEnable", MigEnableKey)
	assert.Equal(t, "true", MigEnableValue)
	assert.Equal(t, "MigSlice", MigSliceKey)
	assert.Equal(t, "4", MigSlice4)
	assert.Equal(t, "7", MigSlice7)
	assert.Equal(t, "DeviceMinor", DeviceMinor)
	assert.Equal(t, "DeviceType", DeviceType)
	assert.Equal(t, "Mig", MigType)
	assert.Equal(t, "aliyun.com/device", AnnotationDevice)
	assert.Equal(t, "aliyun.com/driver-version", DriverVersion)
}
