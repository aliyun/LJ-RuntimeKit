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
	"os"

	"github.com/sirupsen/logrus"
	"sigs.k8s.io/yaml"
)

// protocolConfigPath is the in-container path where the protocol ConfigMap is
// mounted (subPath: protocol.yaml). It is provided by an optional ConfigMap
// volume; when absent the compiled-in defaults are used.
const protocolConfigPath = "/etc/aliyun-gpu-protocol/protocol.yaml"

// LoadProtocol reads the cross-component protocol constants from the ConfigMap
// volume file and overrides the corresponding package-level vars. It is
// idempotent and safe to call more than once.
//
// Any failure (file missing, YAML parse error, missing key, empty value) falls
// back to the compiled-in defaults and only emits a warning; it never exits.
func LoadProtocol() {
	loadProtocolFromFile(protocolConfigPath)
}

// loadProtocolFromFile is the testable core of LoadProtocol: it reads the given
// path and overrides the package-level vars. Kept unexported so tests can point
// it at temporary files without touching the fixed mount path.
func loadProtocolFromFile(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		logrus.Warningf("protocol config not loaded (%v), using compiled-in defaults", err)
		return
	}

	var cfg map[string]string
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		logrus.Warningf("protocol config parse failed (%v), using compiled-in defaults", err)
		return
	}

	// APIServer channel resource names (pod_kubelet_apiserver.go).
	apply(cfg, "resources.memory", &EGPU_MEMORY_RESOURCE_STR)
	apply(cfg, "resources.core", &EGPU_COMPUTE_RESOURCE_STR)
	apply(cfg, "resources.slice", &EGPU_MIG_RESOURCE_STR)
	apply(cfg, "resources.gpu", &EGPU_GPU_RESOURCE_STR)
	apply(cfg, "resources.nvidia", &EGPU_GPU_NV_RESOURCE_STR)

	// PodResources channel resource names (types.go).
	apply(cfg, "resources.podresources.nvidia", &nvidiaResourceName)
	apply(cfg, "resources.podresources.gpu", &gpuResourceName)
	apply(cfg, "resources.podresources.mig-prefix", &nvidiaMigResourcePrefix)

	// Pod annotation keys (pod_kubelet_apiserver.go).
	apply(cfg, "annotations.gpu-idx-list", &EGPU_SCHEDULER_ANNOTATION_GPU_ID_STR)
	apply(cfg, "annotations.gpu-mem-idx-legacy", &EGPU_SCHEDULER_ANNOTATION_GPU_ID_STR_LEGACY)
	apply(cfg, "annotations.mig-ci-uuid", &KUBERGPU_SCHEDULER_ANNOTATION_MIG_UUID)
}

// apply overrides *target with cfg[key] only when the key exists and its value
// is a non-empty string; otherwise the existing (default) value is kept.
func apply(cfg map[string]string, key string, target *string) {
	if v, ok := cfg[key]; ok && v != "" {
		*target = v
	}
}
