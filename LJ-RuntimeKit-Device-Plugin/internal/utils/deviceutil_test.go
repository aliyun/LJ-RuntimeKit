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

	xpugrpc "github.com/lj-runtimekit/device-plugin/internal/grpc"

	"github.com/stretchr/testify/assert"
)

// TestSetAndGetMetric tests SetMetric and GetMetric functions
func TestSetAndGetMetric(t *testing.T) {
	// 保存原始状态
	originalMetric := GetMetric()
	defer SetMetric(originalMetric) // 测试结束后恢复

	// 测试设置GiBPrefix
	SetMetric(GiBPrefix)
	assert.Equal(t, GiBPrefix, GetMetric(), "Should set metric to GiBPrefix")

	// 测试设置其他值
	SetMetric(MemoryUnit("custom"))
	assert.Equal(t, MemoryUnit("custom"), GetMetric(), "Should set metric to custom value")
}

// TestSetGPUMemoryWithGiBPrefix tests SetGPUMemory when metric is GiBPrefix
func TestSetGPUMemoryWithGiBPrefix(t *testing.T) {
	// 保存原始状态
	originalMetric := GetMetric()
	originalGpuMemory := GetGPUMemory()
	defer func() {
		SetMetric(originalMetric)
		gpuMemory = originalGpuMemory
	}()

	// 设置metric为GiBPrefix
	SetMetric(GiBPrefix)

	// 测试内存转换 (除以1024)
	SetGPUMemory(2048)
	assert.Equal(t, uint(2), GetGPUMemory(), "Should convert 2048 bytes to 2 GiB")

	SetGPUMemory(1024)
	assert.Equal(t, uint(1), GetGPUMemory(), "Should convert 1024 bytes to 1 GiB")
}

// TestSetGPUMemoryWithOtherMetric tests SetGPUMemory when metric is not GiBPrefix
func TestSetGPUMemoryWithOtherMetric(t *testing.T) {
	// 保存原始状态
	originalMetric := GetMetric()
	originalGpuMemory := GetGPUMemory()
	defer func() {
		SetMetric(originalMetric)
		gpuMemory = originalGpuMemory
	}()

	// 设置metric为非GiBPrefix值
	SetMetric(MemoryUnit("128MiB")) // 使用 128MiB 作为非GiBPrefix值

	// 测试内存转换 (除以128)
	SetGPUMemory(256)
	assert.Equal(t, uint(2), GetGPUMemory(), "Should convert 256 bytes to 2 units (256/128)")

	SetGPUMemory(128)
	assert.Equal(t, uint(1), GetGPUMemory(), "Should convert 128 bytes to 1 unit (128/128)")
}

// TestConvertGPUMemory tests ConvertGPUMemory function
func TestConvertGPUMemory(t *testing.T) {
	// 保存原始状态
	originalMetric := GetMetric()
	defer SetMetric(originalMetric)

	SetMetric(GiBPrefix)
	result := ConvertGPUMemory(4096)
	assert.Equal(t, uint(4), result, "Should convert 4096 MiB to 4 GiB")

	SetMetric(MiBPrefix)
	result = ConvertGPUMemory(4096)
	assert.Equal(t, uint(32), result, "Should convert 4096 to 32 units of 128MiB")
}

// TestExtractEGPURealDeviceID tests ExtractEGPURealDeviceID function
func TestExtractEGPURealDeviceID(t *testing.T) {
	// 正常情况测试
	result := ExtractEGPURealDeviceID("prefix-realid123-_-suffix")
	assert.Equal(t, "realid123", result, "Should extract real device ID from fake ID")

	// 边界情况测试
	result = ExtractEGPURealDeviceID("prefix-id-_-suffix")
	assert.Equal(t, "id", result, "Should extract short ID")
}

// TestGenerateFakeIDs tests all GenerateFake*ID functions
func TestGenerateFakeIDs(t *testing.T) {
	realID := "device123"
	counter := uint(5)

	// 测试生成假内存ID
	expectedMemID := "MEM-device123-_-" + "5"
	actualMemID := GenerateFakeMemoryID(realID, counter)
	assert.Equal(t, expectedMemID, actualMemID, "Should generate correct fake memory ID")

	// 测试生成假核心ID
	expectedCoreID := "COMP-device123-_-" + "5"
	actualCoreID := GenerateFakeCoreID(realID, counter)
	assert.Equal(t, expectedCoreID, actualCoreID, "Should generate correct fake core ID")

	// 测试生成假可能性ID
	expectedPossID := "POSSI-device123-_-" + "5"
	actualPossID := GenerateFakePossibilityID(realID, counter)
	assert.Equal(t, expectedPossID, actualPossID, "Should generate correct fake possibility ID")
}

// TestGetGPUCore tests GetGPUCore function
func TestGetGPUCore(t *testing.T) {
	assert.Equal(t, uint(100), GetGPUCore(), "Should always return 100")
}

// TestNewDeviceMeta tests NewDeviceMeta function
func TestNewDeviceMeta(t *testing.T) {
	dm := NewDeviceMeta()
	assert.NotNil(t, dm, "Should create DeviceMeta instance")
	assert.NotNil(t, dm.Cache, "Should initialize cache map")
	assert.Empty(t, dm.Cache, "Should create empty cache")
}

// TestDeviceMetaOperations tests DeviceMeta Put/Get/Delete operations
func TestDeviceMetaOperations(t *testing.T) {
	dm := NewDeviceMeta()

	// 创建测试设备
	device := &xpugrpc.Device{
		Id: "test-device",
		// 其他字段可以根据需要添加
	}

	id := 1

	// 测试Put操作
	dm.Put(id, device)

	// 测试Get操作 - 存在的设备
	retrievedDevice, ok := dm.Get(id)
	assert.True(t, ok, "Should find the device")
	assert.Equal(t, device, retrievedDevice, "Should retrieve the same device")

	// 测试Get操作 - 不存在的设备
	nonExistentDevice, ok := dm.Get(999)
	assert.False(t, ok, "Should not find non-existent device")
	assert.Nil(t, nonExistentDevice, "Should return nil for non-existent device")

	// 测试Delete操作
	dm.Delete(id)
	deletedDevice, ok := dm.Get(id)
	assert.False(t, ok, "Should not find deleted device")
	assert.Nil(t, deletedDevice, "Should return nil for deleted device")
}

// TestDeviceMetaConcurrency tests concurrent access to DeviceMeta
func TestDeviceMetaConcurrency(t *testing.T) {
	dm := NewDeviceMeta()

	// 并发写入测试
	for i := 0; i < 10; i++ {
		go func(idx int) {
			device := &xpugrpc.Device{Id: "device" + string(rune(idx))}
			dm.Put(idx, device)
		}(i)
	}

	// 并发读取测试
	for i := 0; i < 10; i++ {
		go func(idx int) {
			dm.Get(idx)
		}(i)
	}

	// 注意：这个测试主要验证不会出现竞态条件，
	// 实际的结果验证可能需要使用channel或其他同步机制
}

// TestExtractMigRealDeviceID tests ExtractMigRealDeviceID function
func TestExtractMigRealDeviceID(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "standard mig id",
			input:    "prefix-realid123-_-suffix",
			expected: "realid123",
		},
		{
			name:     "short id",
			input:    "prefix-id-_-suffix",
			expected: "id",
		},
		{
			name:     "numeric id",
			input:    "MEM-123-_-0",
			expected: "123",
		},
		{
			name:     "complex id",
			input:    "MIG-gpu-uuid-0-_-1_2",
			expected: "gpu-uuid-0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractMigRealDeviceID(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestGenerateFakeMigID tests GenerateFakeMigID function
func TestGenerateFakeMigID(t *testing.T) {
	realID := "device123"
	ciNum := uint(2)
	counter := uint(5)

	expected := "MIG-device123-_-" + "2" + "_" + "5"
	actual := GenerateFakeMigID(realID, ciNum, counter)
	assert.Equal(t, expected, actual)
}

// TestExtractEGPURealDeviceID_EdgeCases tests edge cases for ExtractEGPURealDeviceID
func TestExtractEGPURealDeviceID_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "standard format",
			input:    "GPU-abc123-_-extra",
			expected: "abc123",
		},
		{
			name:     "with dashes in id",
			input:    "MEM-gpu-uuid-0-_-suffix",
			expected: "gpu-uuid-0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractEGPURealDeviceID(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
