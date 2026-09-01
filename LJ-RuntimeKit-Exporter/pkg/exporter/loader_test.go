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
	"path/filepath"
	"testing"
)

// snapshotProtocolVars captures the current values of every var that
// LoadProtocol may override, and registers a t.Cleanup to restore them so tests
// never pollute one another.
func snapshotProtocolVars(t *testing.T) {
	t.Helper()
	orig := struct {
		mem, core, slice, gpu, nv                 string
		prNvidia, prGpu, prMigPrefix               string
		annIdxList, annMemIdxLegacy, annMigCiUUID   string
	}{
		mem:             EGPU_MEMORY_RESOURCE_STR,
		core:            EGPU_COMPUTE_RESOURCE_STR,
		slice:           EGPU_MIG_RESOURCE_STR,
		gpu:             EGPU_GPU_RESOURCE_STR,
		nv:              EGPU_GPU_NV_RESOURCE_STR,
		prNvidia:        nvidiaResourceName,
		prGpu:           gpuResourceName,
		prMigPrefix:     nvidiaMigResourcePrefix,
		annIdxList:      EGPU_SCHEDULER_ANNOTATION_GPU_ID_STR,
		annMemIdxLegacy: EGPU_SCHEDULER_ANNOTATION_GPU_ID_STR_LEGACY,
		annMigCiUUID:    KUBERGPU_SCHEDULER_ANNOTATION_MIG_UUID,
	}
	t.Cleanup(func() {
		EGPU_MEMORY_RESOURCE_STR = orig.mem
		EGPU_COMPUTE_RESOURCE_STR = orig.core
		EGPU_MIG_RESOURCE_STR = orig.slice
		EGPU_GPU_RESOURCE_STR = orig.gpu
		EGPU_GPU_NV_RESOURCE_STR = orig.nv
		nvidiaResourceName = orig.prNvidia
		gpuResourceName = orig.prGpu
		nvidiaMigResourcePrefix = orig.prMigPrefix
		EGPU_SCHEDULER_ANNOTATION_GPU_ID_STR = orig.annIdxList
		EGPU_SCHEDULER_ANNOTATION_GPU_ID_STR_LEGACY = orig.annMemIdxLegacy
		KUBERGPU_SCHEDULER_ANNOTATION_MIG_UUID = orig.annMigCiUUID
	})
}

// writeTempConfig writes content to a temp file and returns its path.
func writeTempConfig(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "protocol.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("failed to write temp config: %v", err)
	}
	return path
}

// Scenario 1: file missing -> all vars keep their defaults, no panic.
func TestLoadProtocol_FileMissing(t *testing.T) {
	snapshotProtocolVars(t)

	loadProtocolFromFile(filepath.Join(t.TempDir(), "does-not-exist.yaml"))

	if EGPU_MEMORY_RESOURCE_STR != "aliyun.com/gpu-memory" {
		t.Errorf("expected default memory resource, got %q", EGPU_MEMORY_RESOURCE_STR)
	}
	if nvidiaResourceName != "nvidia.com/gpu" {
		t.Errorf("expected default nvidia podresource, got %q", nvidiaResourceName)
	}
	if KUBERGPU_SCHEDULER_ANNOTATION_MIG_UUID != "aliyun.com/mig-ci-uuid" {
		t.Errorf("expected default mig-ci-uuid annotation, got %q", KUBERGPU_SCHEDULER_ANNOTATION_MIG_UUID)
	}
}

// Scenario 2: malformed YAML -> full fallback, all vars keep defaults.
func TestLoadProtocol_MalformedYAML(t *testing.T) {
	snapshotProtocolVars(t)

	path := writeTempConfig(t, "resources.memory: \"x\"\n  : : bad-yaml: [unclosed")
	loadProtocolFromFile(path)

	if EGPU_MEMORY_RESOURCE_STR != "aliyun.com/gpu-memory" {
		t.Errorf("malformed YAML should keep default, got %q", EGPU_MEMORY_RESOURCE_STR)
	}
	if EGPU_COMPUTE_RESOURCE_STR != "aliyun.com/gpu-core" {
		t.Errorf("malformed YAML should keep default core, got %q", EGPU_COMPUTE_RESOURCE_STR)
	}
}

// Scenario 3: some keys present, others missing -> present keys override,
// missing keys keep defaults.
func TestLoadProtocol_PartialKeys(t *testing.T) {
	snapshotProtocolVars(t)

	path := writeTempConfig(t, "resources.memory: \"custom.com/mem\"\nannotations.mig-ci-uuid: \"custom.com/mig-uuid\"\n")
	loadProtocolFromFile(path)

	if EGPU_MEMORY_RESOURCE_STR != "custom.com/mem" {
		t.Errorf("present key should override, got %q", EGPU_MEMORY_RESOURCE_STR)
	}
	if KUBERGPU_SCHEDULER_ANNOTATION_MIG_UUID != "custom.com/mig-uuid" {
		t.Errorf("present annotation key should override, got %q", KUBERGPU_SCHEDULER_ANNOTATION_MIG_UUID)
	}
	// Missing keys keep defaults.
	if EGPU_COMPUTE_RESOURCE_STR != "aliyun.com/gpu-core" {
		t.Errorf("missing key should keep default, got %q", EGPU_COMPUTE_RESOURCE_STR)
	}
	if nvidiaResourceName != "nvidia.com/gpu" {
		t.Errorf("missing podresource key should keep default, got %q", nvidiaResourceName)
	}
}

// Scenario 4: empty-string values -> treated as invalid, keep defaults.
func TestLoadProtocol_EmptyValues(t *testing.T) {
	snapshotProtocolVars(t)

	path := writeTempConfig(t, "resources.memory: \"\"\nresources.core: \"\"\nresources.podresources.nvidia: \"\"\n")
	loadProtocolFromFile(path)

	if EGPU_MEMORY_RESOURCE_STR != "aliyun.com/gpu-memory" {
		t.Errorf("empty value should keep default memory, got %q", EGPU_MEMORY_RESOURCE_STR)
	}
	if EGPU_COMPUTE_RESOURCE_STR != "aliyun.com/gpu-core" {
		t.Errorf("empty value should keep default core, got %q", EGPU_COMPUTE_RESOURCE_STR)
	}
	if nvidiaResourceName != "nvidia.com/gpu" {
		t.Errorf("empty value should keep default nvidia podresource, got %q", nvidiaResourceName)
	}
}

// Scenario 5: all keys present and valid -> every var is overridden.
func TestLoadProtocol_AllKeys(t *testing.T) {
	snapshotProtocolVars(t)

	content := `resources.memory: "t.com/mem"
resources.core: "t.com/core"
resources.slice: "t.com/slice"
resources.gpu: "t.com/gpu"
resources.nvidia: "t.com/nvidia"
resources.podresources.nvidia: "t.com/pr-nvidia"
resources.podresources.gpu: "t.com/pr-gpu"
resources.podresources.mig-prefix: "t.com/mig-"
annotations.gpu-idx-list: "T_GPU_IDX_LIST"
annotations.gpu-mem-idx-legacy: "T_GPU_MEM_IDX"
annotations.mig-ci-uuid: "t.com/mig-ci-uuid"
`
	path := writeTempConfig(t, content)
	loadProtocolFromFile(path)

	checks := map[string]struct {
		got  string
		want string
	}{
		"resources.memory":              {EGPU_MEMORY_RESOURCE_STR, "t.com/mem"},
		"resources.core":                {EGPU_COMPUTE_RESOURCE_STR, "t.com/core"},
		"resources.slice":               {EGPU_MIG_RESOURCE_STR, "t.com/slice"},
		"resources.gpu":                 {EGPU_GPU_RESOURCE_STR, "t.com/gpu"},
		"resources.nvidia":              {EGPU_GPU_NV_RESOURCE_STR, "t.com/nvidia"},
		"resources.podresources.nvidia": {nvidiaResourceName, "t.com/pr-nvidia"},
		"resources.podresources.gpu":    {gpuResourceName, "t.com/pr-gpu"},
		"resources.podresources.mig-prefix": {nvidiaMigResourcePrefix, "t.com/mig-"},
		"annotations.gpu-idx-list":      {EGPU_SCHEDULER_ANNOTATION_GPU_ID_STR, "T_GPU_IDX_LIST"},
		"annotations.gpu-mem-idx-legacy": {EGPU_SCHEDULER_ANNOTATION_GPU_ID_STR_LEGACY, "T_GPU_MEM_IDX"},
		"annotations.mig-ci-uuid":       {KUBERGPU_SCHEDULER_ANNOTATION_MIG_UUID, "t.com/mig-ci-uuid"},
	}
	for key, c := range checks {
		if c.got != c.want {
			t.Errorf("key %q: expected %q, got %q", key, c.want, c.got)
		}
	}
}
