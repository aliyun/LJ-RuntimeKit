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
	"encoding/json"
	"testing"

	"github.com/lj-runtimekit/device-plugin/internal/plugin"
	"github.com/lj-runtimekit/device-plugin/internal/utils"

	"github.com/stretchr/testify/assert"
)

func TestDeviceStruct_MarshalJSON(t *testing.T) {
	device := Device{
		DeviceBase: plugin.DeviceBase{
			UUID:  "test-uuid-123",
			Minor: 0,
		},
		Health:     "Healthy",
		Type:       "GPU",
		MigEnable:  true,
		Slice:      4,
		Core:       100,
		Memory:     8192,
		Extensions: map[string]string{"driver": "v1.0"},
	}

	data, err := json.Marshal(device)
	assert.NoError(t, err)
	assert.Contains(t, string(data), `"uuid":"test-uuid-123"`)
	assert.Contains(t, string(data), `"health":"Healthy"`)
	assert.Contains(t, string(data), `"type":"GPU"`)
	assert.Contains(t, string(data), `"migEnable":true`)
}

func TestGpuDeviceStruct(t *testing.T) {
	gpuDevices := GpuDevice{
		Count: 2,
		Devices: map[int]*Device{
			0: {
				DeviceBase: plugin.DeviceBase{UUID: "uuid-0", Minor: 0},
				Health:     "Healthy",
				Memory:     4096,
			},
			1: {
				DeviceBase: plugin.DeviceBase{UUID: "uuid-1", Minor: 1},
				Health:     "Healthy",
				Memory:     4096,
			},
		},
	}

	assert.Equal(t, 2, gpuDevices.Count)
	assert.Len(t, gpuDevices.Devices, 2)
	assert.Equal(t, "uuid-0", gpuDevices.Devices[0].UUID)
	assert.Equal(t, "uuid-1", gpuDevices.Devices[1].UUID)
}

func TestGpuDevice_MarshalJSON(t *testing.T) {
	gpuDevices := GpuDevice{
		Count: 1,
		Devices: map[int]*Device{
			0: {
				DeviceBase: plugin.DeviceBase{UUID: "test-uuid", Minor: 0},
				Health:     "Healthy",
				Core:       100,
				Memory:     8192,
			},
		},
	}

	data, err := json.Marshal(gpuDevices)
	assert.NoError(t, err)

	var decoded GpuDevice
	err = json.Unmarshal(data, &decoded)
	assert.NoError(t, err)
	assert.Equal(t, 1, decoded.Count)
	assert.Equal(t, "test-uuid", decoded.Devices[0].UUID)
}

func TestGetPluginTypes_ForGPU(t *testing.T) {
	types := getPluginTypes(utils.DeviceTypeGPU)

	// Should always include these
	assert.Contains(t, types, utils.ResourceCore)
	assert.Contains(t, types, utils.ResourceMemory)
	assert.Contains(t, types, utils.ResourceGPU)
	assert.Contains(t, types, utils.ResourceSlice)
	assert.Contains(t, types, utils.ResourceNvidia)
	// possibility and gpu count are deliberately not advertised, even though
	// getResourceSocketAndFlag still knows how to serve them
	assert.NotContains(t, types, utils.ResourcePossibility)
	assert.NotContains(t, types, utils.ResourceGPUCount)
}

func TestGetPluginTypes_ForPPU(t *testing.T) {
	types := getPluginTypes(utils.DeviceTypePPU)

	assert.Contains(t, types, utils.ResourceCore)
	assert.Contains(t, types, utils.ResourceMemory)
	assert.Contains(t, types, utils.ResourceGPU)
	// PPU should NOT have slice or nvidia
	assert.NotContains(t, types, utils.ResourceSlice)
	assert.NotContains(t, types, utils.ResourceNvidia)
	assert.NotContains(t, types, utils.ResourcePossibility)
	assert.NotContains(t, types, utils.ResourceGPUCount)
}

func TestGetPluginTypes_ForAMD(t *testing.T) {
	types := getPluginTypes(utils.DeviceTypeAMD)

	assert.Contains(t, types, utils.ResourceCore)
	assert.Contains(t, types, utils.ResourceMemory)
	assert.Contains(t, types, utils.ResourceGPU)
	assert.NotContains(t, types, utils.ResourceSlice)
	assert.NotContains(t, types, utils.ResourceNvidia)
	assert.NotContains(t, types, utils.ResourcePossibility)
	assert.NotContains(t, types, utils.ResourceGPUCount)
}

func TestGetResourceSocketAndFlag(t *testing.T) {
	tests := []struct {
		name         string
		resource     string
		expectedSock string
		expectedFlag string
	}{
		{
			name:         "core resource",
			resource:     utils.ResourceCore,
			expectedSock: utils.CoreSocket,
			expectedFlag: utils.AnnotationCoreAssignedFlag,
		},
		{
			name:         "memory resource",
			resource:     utils.ResourceMemory,
			expectedSock: utils.MemorySocket,
			expectedFlag: utils.AnnotationMemoryAssignedFlag,
		},
		{
			name:         "possibility resource",
			resource:     utils.ResourcePossibility,
			expectedSock: utils.PossibilitySocket,
			expectedFlag: utils.AnnotationPossibilityAssignedFlag,
		},
		{
			name:         "slice resource",
			resource:     utils.ResourceSlice,
			expectedSock: utils.SliceSocket,
			expectedFlag: utils.AnnotationSliceAssignedFlag,
		},
		{
			name:         "gpu count resource",
			resource:     utils.ResourceGPUCount,
			expectedSock: utils.CountSocket,
			expectedFlag: utils.AnnotationNormalGpuAssignedFlag,
		},
		{
			name:         "gpu resource",
			resource:     utils.ResourceGPU,
			expectedSock: utils.GPUSocket,
			expectedFlag: utils.AnnotationNormalGpuAssignedFlag,
		},
		{
			name:         "nvidia resource",
			resource:     utils.ResourceNvidia,
			expectedSock: utils.NvidiaSocket,
			expectedFlag: utils.AnnotationNormalGpuAssignedFlag,
		},
		{
			name:         "unknown resource",
			resource:     "unknown",
			expectedSock: "",
			expectedFlag: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sock, flag := getResourceSocketAndFlag(tt.resource)
			assert.Equal(t, tt.expectedSock, sock)
			assert.Equal(t, tt.expectedFlag, flag)
		})
	}
}

func TestStopPlugins_EmptyList(t *testing.T) {
	err := stopPlugins([]plugin.PluginInterface{})
	assert.NoError(t, err)
}
