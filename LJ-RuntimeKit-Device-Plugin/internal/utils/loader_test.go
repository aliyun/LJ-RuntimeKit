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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

// snapshot captures every protocol var that loadProtocolFrom may override and
// registers a t.Cleanup to restore them, preventing cross-test pollution of the
// package-level state.
func snapshot(t *testing.T) {
	t.Helper()
	orig := map[*string]string{
		&ResourceGPUCount:                orig1(&ResourceGPUCount),
		&ResourceMemory:                  orig1(&ResourceMemory),
		&ResourceCore:                    orig1(&ResourceCore),
		&ResourceSlice:                   orig1(&ResourceSlice),
		&ResourceRDMA:                    orig1(&ResourceRDMA),
		&ResourceGPU:                     orig1(&ResourceGPU),
		&ResourceNvidia:                  orig1(&ResourceNvidia),
		&AnnotationDeviceIndex:           orig1(&AnnotationDeviceIndex),
		&AnnotationNormalGpuAssignedFlag: orig1(&AnnotationNormalGpuAssignedFlag),
		&AnnotationMemoryAssignedFlag:    orig1(&AnnotationMemoryAssignedFlag),
		&AnnotationCoreAssignedFlag:      orig1(&AnnotationCoreAssignedFlag),
		&AnnotationSliceAssignedFlag:     orig1(&AnnotationSliceAssignedFlag),
		&AnnotationResourceAssumeTime:    orig1(&AnnotationResourceAssumeTime),
		&AnnotationPcieSwitchIndex:       orig1(&AnnotationPcieSwitchIndex),
		&AnnotationMigDeviceCIMask:       orig1(&AnnotationMigDeviceCIMask),
		&AnnotationMigTopologyStart:      orig1(&AnnotationMigTopologyStart),
		&AnnotationMigTopologySize:       orig1(&AnnotationMigTopologySize),
		&AnnotationMigDeviceType:         orig1(&AnnotationMigDeviceType),
		&AnnotationMigDeviceCiUUID:       orig1(&AnnotationMigDeviceCiUUID),
		&AnnotationRdmaTopo:              orig1(&AnnotationRdmaTopo),
		&AnnotationDevice:                orig1(&AnnotationDevice),
		&DriverVersion:                   orig1(&DriverVersion),
		&NodeLabelLeafSwitchID:           orig1(&NodeLabelLeafSwitchID),
		&NodeLabelNetworkPoD:             orig1(&NodeLabelNetworkPoD),
		&NodeLabelHyperNodeID:            orig1(&NodeLabelHyperNodeID),
	}
	t.Cleanup(func() {
		for p, v := range orig {
			*p = v
		}
	})
}

func orig1(p *string) string { return *p }

// writeConfig writes content to a temp protocol.yaml and returns its path.
func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "protocol.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return path
}

// TestLoadProtocol_FileMissing: file does not exist -> all vars keep defaults,
// no panic.
func TestLoadProtocol_FileMissing(t *testing.T) {
	snapshot(t)
	loadProtocolFrom(filepath.Join(t.TempDir(), "does-not-exist.yaml"))

	assert.Equal(t, "aliyun.com/rdma", ResourceRDMA)
	assert.Equal(t, "aliyun.com/gpu-memory", ResourceMemory)
	assert.Equal(t, "ALIYUN_COM_PCIESWITCH_INDEX", AnnotationPcieSwitchIndex)
}

// TestLoadProtocol_InvalidYAML: malformed YAML -> all vars fall back to
// defaults, no panic.
func TestLoadProtocol_InvalidYAML(t *testing.T) {
	snapshot(t)
	path := writeConfig(t, "resources.rdma: \"x\"\n\t: : bad: [unterminated")
	loadProtocolFrom(path)

	assert.Equal(t, "aliyun.com/rdma", ResourceRDMA)
	assert.Equal(t, "aliyun.com/gpu-memory", ResourceMemory)
}

// TestLoadProtocol_MissingKey: only some keys present -> present keys override,
// absent keys keep defaults.
func TestLoadProtocol_MissingKey(t *testing.T) {
	snapshot(t)
	path := writeConfig(t, "resources.rdma: \"custom.com/rdma\"\n")
	loadProtocolFrom(path)

	assert.Equal(t, "custom.com/rdma", ResourceRDMA)
	// Not in the file -> unchanged.
	assert.Equal(t, "aliyun.com/gpu-memory", ResourceMemory)
	assert.Equal(t, "ALIYUN_COM_PCIESWITCH_INDEX", AnnotationPcieSwitchIndex)
}

// TestLoadProtocol_EmptyValue: empty string value is treated as invalid ->
// keep default.
func TestLoadProtocol_EmptyValue(t *testing.T) {
	snapshot(t)
	path := writeConfig(t, "resources.rdma: \"\"\nresources.memory: \"custom.com/mem\"\n")
	loadProtocolFrom(path)

	// Empty value ignored.
	assert.Equal(t, "aliyun.com/rdma", ResourceRDMA)
	// Non-empty value applied.
	assert.Equal(t, "custom.com/mem", ResourceMemory)
}

// TestLoadProtocol_AllKeys: every key present with a valid value -> every var
// overridden.
func TestLoadProtocol_AllKeys(t *testing.T) {
	snapshot(t)
	content := `resources.gpu-count: "t/gpu-count"
resources.memory: "t/memory"
resources.core: "t/core"
resources.slice: "t/slice"
resources.rdma: "t/rdma"
resources.gpu: "t/gpu"
resources.nvidia: "t/nvidia"
annotations.gpu-idx-list: "T_GPU_IDX_LIST"
annotations.gpu-assigned: "T_GPU_ASSIGNED"
annotations.gpu-mem-assigned: "T_GPU_MEM_ASSIGNED"
annotations.gpu-core-assigned: "T_GPU_CORE_ASSIGNED"
annotations.gpu-slice-assigned: "T_GPU_SLICE_ASSIGNED"
annotations.gpu-mem-assume-time: "T_GPU_MEM_ASSUME_TIME"
annotations.pcie-switch-index: "T_PCIESWITCH_INDEX"
annotations.mig-mask: "t/mig-mask"
annotations.mig-topology-start: "t/mig-topology-start"
annotations.mig-topology-size: "t/mig-topology-size"
annotations.mig-type: "t/mig-type"
annotations.mig-ci-uuid: "t/mig-ci-uuid"
node.annotations.rdma-topo: "t/rdma-topo"
node.annotations.device: "t/device"
node.annotations.driver-version: "t/driver-version"
node.labels.leaf-switch-id: "t/leaf-switch-id"
node.labels.network-pod: "t/network-pod"
node.labels.hypernode-id: "t/hypernode-id"
`
	loadProtocolFrom(writeConfig(t, content))

	assert.Equal(t, "t/gpu-count", ResourceGPUCount)
	assert.Equal(t, "t/memory", ResourceMemory)
	assert.Equal(t, "t/core", ResourceCore)
	assert.Equal(t, "t/slice", ResourceSlice)
	assert.Equal(t, "t/rdma", ResourceRDMA)
	assert.Equal(t, "t/gpu", ResourceGPU)
	assert.Equal(t, "t/nvidia", ResourceNvidia)
	assert.Equal(t, "T_GPU_IDX_LIST", AnnotationDeviceIndex)
	assert.Equal(t, "T_GPU_ASSIGNED", AnnotationNormalGpuAssignedFlag)
	assert.Equal(t, "T_GPU_MEM_ASSIGNED", AnnotationMemoryAssignedFlag)
	assert.Equal(t, "T_GPU_CORE_ASSIGNED", AnnotationCoreAssignedFlag)
	assert.Equal(t, "T_GPU_SLICE_ASSIGNED", AnnotationSliceAssignedFlag)
	assert.Equal(t, "T_GPU_MEM_ASSUME_TIME", AnnotationResourceAssumeTime)
	assert.Equal(t, "T_PCIESWITCH_INDEX", AnnotationPcieSwitchIndex)
	assert.Equal(t, "t/mig-mask", AnnotationMigDeviceCIMask)
	assert.Equal(t, "t/mig-topology-start", AnnotationMigTopologyStart)
	assert.Equal(t, "t/mig-topology-size", AnnotationMigTopologySize)
	assert.Equal(t, "t/mig-type", AnnotationMigDeviceType)
	assert.Equal(t, "t/mig-ci-uuid", AnnotationMigDeviceCiUUID)
	assert.Equal(t, "t/rdma-topo", AnnotationRdmaTopo)
	assert.Equal(t, "t/device", AnnotationDevice)
	assert.Equal(t, "t/driver-version", DriverVersion)
	assert.Equal(t, "t/leaf-switch-id", NodeLabelLeafSwitchID)
	assert.Equal(t, "t/network-pod", NodeLabelNetworkPoD)
	assert.Equal(t, "t/hypernode-id", NodeLabelHyperNodeID)
}
