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

package util

import (
	"testing"

	"github.com/container-migration/controller/pkg/labels"
)

const workerTemplateWithLabels = `metadata:
  labels:
    component: lj-runtimekit-migration-worker
    migration.io/worker-config-hash: user-supplied-should-be-overridden
  annotations:
    team: infra
spec:
  containers:
  - name: migration-worker
    image: registry.example.com/worker:v1
`

// TestBuildMigrationWorker_StampsConfigHash verifies the hash label / source
// annotation are written after copying the template: it must both stamp
// successfully and override the same-name hash key in the user template, while
// keeping the template's other labels.
func TestBuildMigrationWorker_StampsConfigHash(t *testing.T) {
	pod, err := BuildMigrationWorker("node-a", "kube-system", workerTemplateWithLabels,
		"deadbeefcafe0000", "kube-system/cm@42")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := pod.Labels[labels.LabelWorkerConfigHash]; got != "deadbeefcafe0000" {
		t.Errorf("config hash label = %q, want controller value (user value must be overridden)", got)
	}
	if got := pod.Labels["component"]; got != "lj-runtimekit-migration-worker" {
		t.Errorf("template label 'component' lost, got %q", got)
	}
	if got := pod.Annotations[labels.AnnotationWorkerConfigSource]; got != "kube-system/cm@42" {
		t.Errorf("config source annotation = %q, want kube-system/cm@42", got)
	}
	if got := pod.Annotations["team"]; got != "infra" {
		t.Errorf("template annotation 'team' lost, got %q", got)
	}
	if pod.Name != GenerateMigrationWorkerName("node-a") {
		t.Errorf("pod name = %q, want %q", pod.Name, GenerateMigrationWorkerName("node-a"))
	}
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil {
		t.Error("node affinity must be injected to pin the worker to its node")
	}
}

// TestBuildMigrationWorker_EmptyHashLeavesNoLabel: no key injected when
// hash/source are empty, for SyncConfigMap's template validity check scenario
// (empty passed in).
func TestBuildMigrationWorker_EmptyHashLeavesNoLabel(t *testing.T) {
	pod, err := BuildMigrationWorker("node-a", "kube-system",
		"spec:\n  containers:\n  - name: w\n    image: x\n", "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := pod.Labels[labels.LabelWorkerConfigHash]; ok {
		t.Error("empty hash must not add the config-hash label")
	}
}

func TestBuildMigrationWorker_RejectsEmptyInput(t *testing.T) {
	if _, err := BuildMigrationWorker("", "kube-system", "spec: {}", "h", "s"); err == nil {
		t.Error("expected error for empty node name")
	}
	if _, err := BuildMigrationWorker("node-a", "kube-system", "", "h", "s"); err == nil {
		t.Error("expected error for empty template")
	}
}
