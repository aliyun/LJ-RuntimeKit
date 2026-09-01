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
)

// snapshotProtocolVars saves package-level vars that LoadProtocol may overwrite
// and returns a restore function for t.Cleanup.
func snapshotProtocolVars() func() {
	saved := map[*string]string{
		&ResourceCount:               ResourceCount,
		&ResourceMemory:              ResourceMemory,
		&ResourceCore:                ResourceCore,
		&ResourceSlice:               ResourceSlice,
		&ResourceRDMA:                ResourceRDMA,
		&ResourceGPU:                 ResourceGPU,
		&ResourceNvidia:              ResourceNvidia,
		&AnnoDeviceIndex:             AnnoDeviceIndex,
		&AnnoNormalGpuAssignedFlag:   AnnoNormalGpuAssignedFlag,
		&AnnoMemoryAssignedFlag:      AnnoMemoryAssignedFlag,
		&AnnoCoreAssignedFlag:        AnnoCoreAssignedFlag,
		&AnnotationSliceAssignedFlag: AnnotationSliceAssignedFlag,
		&AnnoResourceAssumeTime:      AnnoResourceAssumeTime,
		&AnnotationPcieSwitchIndex:   AnnotationPcieSwitchIndex,
		&AnnoMigDevMask:              AnnoMigDevMask,
		&AnnoMigTopoStart:            AnnoMigTopoStart,
		&AnnoMigTopoSize:             AnnoMigTopoSize,
		&AnnoMigDeviceType:           AnnoMigDeviceType,
		&AnnotationRdmaTopo:          AnnotationRdmaTopo,
		&AnnotationDevice:            AnnotationDevice,
		&ResourceDriverVersion:       ResourceDriverVersion,
		&LabelNodeLeafSwitchId:       LabelNodeLeafSwitchId,
		&LabelNodeNetworkPod:         LabelNodeNetworkPod,
		&LabelNodeHyperNodeId:        LabelNodeHyperNodeId,
		&LabelNodeScheduleStrategy:   LabelNodeScheduleStrategy,
		&ScheduleStrategySpread:      ScheduleStrategySpread,
		&ScheduleStrategyBinpack:     ScheduleStrategyBinpack,
		&LabelRuntimeKitMinMember:    LabelRuntimeKitMinMember,
		&LabelJobSetName:             LabelJobSetName,
		&LabelJobSetReplicatedJob:    LabelJobSetReplicatedJob,
		&LabelJobSetJobIndex:         LabelJobSetJobIndex,
	}
	return func() {
		for ptr, v := range saved {
			*ptr = v
		}
	}
}

// setProtocolPath overrides protocolConfigPathVar for the test and restores it on cleanup.
func setProtocolPath(t *testing.T, path string) {
	orig := protocolConfigPathVar
	protocolConfigPathVar = path
	t.Cleanup(func() { protocolConfigPathVar = orig })
}

// TestLoadProtocol_FileMissing: missing file keeps defaults.
func TestLoadProtocol_FileMissing(t *testing.T) {
	t.Cleanup(snapshotProtocolVars())
	setProtocolPath(t, filepath.Join(t.TempDir(), "does-not-exist.yaml"))

	LoadProtocol()

	if ResourceRDMA != "aliyun.com/rdma" {
		t.Errorf("ResourceRDMA should keep default, got %q", ResourceRDMA)
	}
	if AnnotationPcieSwitchIndex != "ALIYUN_COM_PCIESWITCH_INDEX" {
		t.Errorf("AnnotationPcieSwitchIndex should keep default, got %q", AnnotationPcieSwitchIndex)
	}
}

// TestLoadProtocol_InvalidYAML: invalid YAML keeps defaults.
func TestLoadProtocol_InvalidYAML(t *testing.T) {
	t.Cleanup(snapshotProtocolVars())
	p := filepath.Join(t.TempDir(), "protocol.yaml")
	if err := os.WriteFile(p, []byte("this: : : not valid yaml\n\t- bad"), 0o644); err != nil {
		t.Fatal(err)
	}
	setProtocolPath(t, p)

	LoadProtocol()

	if ResourceRDMA != "aliyun.com/rdma" {
		t.Errorf("ResourceRDMA should keep default on parse failure, got %q", ResourceRDMA)
	}
}

// TestLoadProtocol_PartialKeys: missing keys keep defaults, present keys are overridden.
func TestLoadProtocol_PartialKeys(t *testing.T) {
	t.Cleanup(snapshotProtocolVars())
	p := filepath.Join(t.TempDir(), "protocol.yaml")
	content := "resources.rdma: \"custom.com/rdma\"\n" +
		"strategies.spread: \"my-spread\"\n"
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	setProtocolPath(t, p)

	LoadProtocol()

	if ResourceRDMA != "custom.com/rdma" {
		t.Errorf("ResourceRDMA should be overridden, got %q", ResourceRDMA)
	}
	if ScheduleStrategySpread != "my-spread" {
		t.Errorf("ScheduleStrategySpread should be overridden, got %q", ScheduleStrategySpread)
	}
	if ResourceMemory != "aliyun.com/gpu-memory" {
		t.Errorf("ResourceMemory should keep default, got %q", ResourceMemory)
	}
}

// TestLoadProtocol_EmptyValue: empty values are ignored, defaults kept.
func TestLoadProtocol_EmptyValue(t *testing.T) {
	t.Cleanup(snapshotProtocolVars())
	p := filepath.Join(t.TempDir(), "protocol.yaml")
	content := "resources.rdma: \"\"\n"
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	setProtocolPath(t, p)

	LoadProtocol()

	if ResourceRDMA != "aliyun.com/rdma" {
		t.Errorf("empty value should keep default, got %q", ResourceRDMA)
	}
}

// TestLoadProtocol_AllKeys: all keys are overridden when present.
func TestLoadProtocol_AllKeys(t *testing.T) {
	t.Cleanup(snapshotProtocolVars())
	p := filepath.Join(t.TempDir(), "protocol.yaml")
	content := "" +
		"resources.gpu-count: \"c/gpu-count\"\n" +
		"resources.memory: \"c/gpu-memory\"\n" +
		"resources.core: \"c/gpu-core\"\n" +
		"resources.slice: \"c/gpu-slice\"\n" +
		"resources.rdma: \"c/rdma\"\n" +
		"resources.gpu: \"c/gpu\"\n" +
		"resources.nvidia: \"c/nvidia\"\n" +
		"annotations.gpu-idx-list: \"c_idx\"\n" +
		"annotations.gpu-assigned: \"c_assigned\"\n" +
		"annotations.gpu-mem-assigned: \"c_mem_assigned\"\n" +
		"annotations.gpu-core-assigned: \"c_core_assigned\"\n" +
		"annotations.gpu-slice-assigned: \"c_slice_assigned\"\n" +
		"annotations.gpu-mem-assume-time: \"c_assume_time\"\n" +
		"annotations.pcie-switch-index: \"c_pcie\"\n" +
		"annotations.mig-mask: \"c/mig-mask\"\n" +
		"annotations.mig-topology-start: \"c/mig-start\"\n" +
		"annotations.mig-topology-size: \"c/mig-size\"\n" +
		"annotations.mig-type: \"c/mig-type\"\n" +
		"node.annotations.rdma-topo: \"c/rdma-topo\"\n" +
		"node.annotations.device: \"c/device\"\n" +
		"node.annotations.driver-version: \"c/driver-version\"\n" +
		"node.labels.leaf-switch-id: \"c/leaf\"\n" +
		"node.labels.network-pod: \"c/netpod\"\n" +
		"node.labels.hypernode-id: \"c/hypernode\"\n" +
		"strategies.node-placement-label: \"c.placement\"\n" +
		"strategies.spread: \"c-spread\"\n" +
		"strategies.binpack: \"c-binpack\"\n" +
		"labels.min-member: \"c/min-member\"\n" +
		"labels.jobset-name: \"c/jobset-name\"\n" +
		"labels.jobset-replicated: \"c/jobset-replicated\"\n" +
		"labels.jobset-job-index: \"c/jobset-job-index\"\n"
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	setProtocolPath(t, p)

	LoadProtocol()

	checks := map[string]string{
		"ResourceCount":               ResourceCount,
		"ResourceMemory":              ResourceMemory,
		"ResourceCore":                ResourceCore,
		"ResourceSlice":               ResourceSlice,
		"ResourceRDMA":                ResourceRDMA,
		"ResourceGPU":                 ResourceGPU,
		"ResourceNvidia":              ResourceNvidia,
		"AnnoDeviceIndex":             AnnoDeviceIndex,
		"AnnoNormalGpuAssignedFlag":   AnnoNormalGpuAssignedFlag,
		"AnnoMemoryAssignedFlag":      AnnoMemoryAssignedFlag,
		"AnnoCoreAssignedFlag":        AnnoCoreAssignedFlag,
		"AnnotationSliceAssignedFlag": AnnotationSliceAssignedFlag,
		"AnnoResourceAssumeTime":      AnnoResourceAssumeTime,
		"AnnotationPcieSwitchIndex":   AnnotationPcieSwitchIndex,
		"AnnoMigDevMask":              AnnoMigDevMask,
		"AnnoMigTopoStart":            AnnoMigTopoStart,
		"AnnoMigTopoSize":             AnnoMigTopoSize,
		"AnnoMigDeviceType":           AnnoMigDeviceType,
		"AnnotationRdmaTopo":          AnnotationRdmaTopo,
		"AnnotationDevice":            AnnotationDevice,
		"ResourceDriverVersion":       ResourceDriverVersion,
		"LabelNodeLeafSwitchId":       LabelNodeLeafSwitchId,
		"LabelNodeNetworkPod":         LabelNodeNetworkPod,
		"LabelNodeHyperNodeId":        LabelNodeHyperNodeId,
		"LabelNodeScheduleStrategy":   LabelNodeScheduleStrategy,
		"ScheduleStrategySpread":      ScheduleStrategySpread,
		"ScheduleStrategyBinpack":     ScheduleStrategyBinpack,
		"LabelRuntimeKitMinMember":    LabelRuntimeKitMinMember,
		"LabelJobSetName":             LabelJobSetName,
		"LabelJobSetReplicatedJob":    LabelJobSetReplicatedJob,
		"LabelJobSetJobIndex":         LabelJobSetJobIndex,
	}
	// All ConfigMap values start with "c", confirming no default leaked through.
	for name, got := range checks {
		if got == "" || got[0] != 'c' {
			t.Errorf("%s should be overridden by CM value, got %q", name, got)
		}
	}
}
