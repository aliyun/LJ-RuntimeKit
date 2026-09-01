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

	"k8s.io/klog/v2"
	"sigs.k8s.io/yaml"
)

// protocolConfigPath is the container path where the protocol ConfigMap is
// mounted. SF reuses the existing scheduler-config ConfigMap mount (the whole
// CM is mounted at /etc/kubernetes/), so the appended protocol.yaml data key
// shows up at this path automatically without changing the Deployment.
const protocolConfigPath = "/etc/kubernetes/protocol.yaml"

// protocolConfigPathVar defaults to protocolConfigPath; it is extracted into a
// var only so unit tests can override the read path.
var protocolConfigPathVar = protocolConfigPath

// LoadProtocol reads the cross-component protocol constants from the mounted
// ConfigMap file and overrides the package-level var values. It is idempotent
// and safe to call repeatedly. On any failure (missing file, parse error,
// missing key, empty value) it keeps the compiled-in defaults and only logs a
// warning, never exits, so the process still starts when the ConfigMap is absent.
func LoadProtocol() {
	data, err := os.ReadFile(protocolConfigPathVar)
	if err != nil {
		klog.Warningf("protocol config not loaded (%v), using compiled-in defaults", err)
		return
	}
	var cfg map[string]string
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		klog.Warningf("protocol config parse failed (%v), using compiled-in defaults", err)
		return
	}

	// Resource names.
	apply(cfg, "resources.gpu-count", &ResourceCount)
	apply(cfg, "resources.memory", &ResourceMemory)
	apply(cfg, "resources.core", &ResourceCore)
	apply(cfg, "resources.slice", &ResourceSlice)
	apply(cfg, "resources.rdma", &ResourceRDMA)
	apply(cfg, "resources.gpu", &ResourceGPU)
	apply(cfg, "resources.nvidia", &ResourceNvidia)

	// Pod annotations (SF is the writer).
	apply(cfg, "annotations.gpu-idx-list", &AnnoDeviceIndex)
	apply(cfg, "annotations.gpu-assigned", &AnnoNormalGpuAssignedFlag)
	apply(cfg, "annotations.gpu-mem-assigned", &AnnoMemoryAssignedFlag)
	apply(cfg, "annotations.gpu-core-assigned", &AnnoCoreAssignedFlag)
	apply(cfg, "annotations.gpu-slice-assigned", &AnnotationSliceAssignedFlag)
	apply(cfg, "annotations.gpu-mem-assume-time", &AnnoResourceAssumeTime)
	apply(cfg, "annotations.pcie-switch-index", &AnnotationPcieSwitchIndex)
	apply(cfg, "annotations.mig-mask", &AnnoMigDevMask)
	apply(cfg, "annotations.mig-topology-start", &AnnoMigTopoStart)
	apply(cfg, "annotations.mig-topology-size", &AnnoMigTopoSize)
	apply(cfg, "annotations.mig-type", &AnnoMigDeviceType)

	// Node annotations (SF is the reader).
	apply(cfg, "node.annotations.rdma-topo", &AnnotationRdmaTopo)
	apply(cfg, "node.annotations.device", &AnnotationDevice)
	apply(cfg, "node.annotations.driver-version", &ResourceDriverVersion)

	// Node labels.
	apply(cfg, "node.labels.leaf-switch-id", &LabelNodeLeafSwitchId)
	apply(cfg, "node.labels.network-pod", &LabelNodeNetworkPod)
	apply(cfg, "node.labels.hypernode-id", &LabelNodeHyperNodeId)

	// Scheduling strategy values.
	apply(cfg, "strategies.node-placement-label", &LabelNodeScheduleStrategy)
	apply(cfg, "strategies.spread", &ScheduleStrategySpread)
	apply(cfg, "strategies.binpack", &ScheduleStrategyBinpack)

	// Gang scheduling group identifier labels.
	apply(cfg, "labels.min-member", &LabelRuntimeKitMinMember)
	apply(cfg, "labels.jobset-name", &LabelJobSetName)
	apply(cfg, "labels.jobset-replicated", &LabelJobSetReplicatedJob)
	apply(cfg, "labels.jobset-job-index", &LabelJobSetJobIndex)
}

// apply overrides *target with cfg[key] when the key exists and its value is
// non-empty. Missing keys and empty values keep the compiled-in default.
func apply(cfg map[string]string, key string, target *string) {
	if v, ok := cfg[key]; ok && v != "" {
		*target = v
	}
}
