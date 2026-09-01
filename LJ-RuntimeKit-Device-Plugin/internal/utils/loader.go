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

// protocolConfigPath is the mounted protocol ConfigMap file. When absent,
// compiled-in defaults are used.
const protocolConfigPath = "/etc/aliyun-gpu-protocol/protocol.yaml"

// LoadProtocol reads protocol constants from the mounted ConfigMap and
// overrides package-level vars. Failures keep the compiled-in defaults.
func LoadProtocol() {
	loadProtocolFrom(protocolConfigPath)
}

// loadProtocolFrom is the testable core of LoadProtocol: it loads the protocol
// overrides from the given path. Extracted to allow unit tests to point at a
// temporary file.
func loadProtocolFrom(path string) {
	data, err := os.ReadFile(path)
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
	apply(cfg, "resources.gpu-count", &ResourceGPUCount)
	apply(cfg, "resources.memory", &ResourceMemory)
	apply(cfg, "resources.core", &ResourceCore)
	apply(cfg, "resources.slice", &ResourceSlice)
	apply(cfg, "resources.rdma", &ResourceRDMA)
	apply(cfg, "resources.gpu", &ResourceGPU)
	apply(cfg, "resources.nvidia", &ResourceNvidia)

	// Pod annotations.
	apply(cfg, "annotations.gpu-idx-list", &AnnotationDeviceIndex)
	apply(cfg, "annotations.gpu-assigned", &AnnotationNormalGpuAssignedFlag)
	apply(cfg, "annotations.gpu-mem-assigned", &AnnotationMemoryAssignedFlag)
	apply(cfg, "annotations.gpu-core-assigned", &AnnotationCoreAssignedFlag)
	apply(cfg, "annotations.gpu-slice-assigned", &AnnotationSliceAssignedFlag)
	apply(cfg, "annotations.gpu-mem-assume-time", &AnnotationResourceAssumeTime)
	apply(cfg, "annotations.pcie-switch-index", &AnnotationPcieSwitchIndex)
	apply(cfg, "annotations.mig-mask", &AnnotationMigDeviceCIMask)
	apply(cfg, "annotations.mig-topology-start", &AnnotationMigTopologyStart)
	apply(cfg, "annotations.mig-topology-size", &AnnotationMigTopologySize)
	apply(cfg, "annotations.mig-type", &AnnotationMigDeviceType)
	apply(cfg, "annotations.mig-ci-uuid", &AnnotationMigDeviceCiUUID)

	// Node annotations / labels.
	apply(cfg, "node.annotations.rdma-topo", &AnnotationRdmaTopo)
	apply(cfg, "node.annotations.device", &AnnotationDevice)
	apply(cfg, "node.annotations.driver-version", &DriverVersion)
	apply(cfg, "node.labels.leaf-switch-id", &NodeLabelLeafSwitchID)
	apply(cfg, "node.labels.network-pod", &NodeLabelNetworkPoD)
	apply(cfg, "node.labels.hypernode-id", &NodeLabelHyperNodeID)
}

// apply overrides *target with cfg[key] when the key exists and its value is
// non-empty. Missing keys and empty values keep the compiled-in default.
func apply(cfg map[string]string, key string, target *string) {
	if v, ok := cfg[key]; ok && v != "" {
		*target = v
	}
}
