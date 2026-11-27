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
	"fmt"

	"github.com/container-migration/controller/api/v1beta1"
	"github.com/container-migration/controller/pkg/labels"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

// BuildMigrationWorker creates a worker pod instance from the template YAML.
//
// configHash / configSource are written only after copying the template's
// labels/annotations, ensuring same-name keys written by the user template do
// not override the control plane's fingerprint markers.
func BuildMigrationWorker(nodeName string, namespace string, templateYaml string, configHash, configSource string) (*corev1.Pod, error) {
	if nodeName == "" || templateYaml == "" {
		return nil, fmt.Errorf("node name or template yaml is empty")
	}
	podTemplateSpec := &corev1.PodTemplateSpec{}
	err := yaml.Unmarshal([]byte(templateYaml), podTemplateSpec)
	if err != nil {
		return nil, err
	}
	podLabels := podTemplateSpec.Labels
	if podLabels == nil {
		podLabels = map[string]string{}
	}
	podAnnotations := podTemplateSpec.Annotations
	if podAnnotations == nil {
		podAnnotations = map[string]string{}
	}
	// Written after the template's labels/annotations, avoiding being overridden by the user template.
	if configHash != "" {
		podLabels[labels.LabelWorkerConfigHash] = configHash
	}
	if configSource != "" {
		podAnnotations[labels.AnnotationWorkerConfigSource] = configSource
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        GenerateMigrationWorkerName(nodeName),
			Namespace:   namespace,
			Labels:      podLabels,
			Annotations: podAnnotations,
		},
		Spec: podTemplateSpec.Spec,
	}
	pod.Spec.Affinity = &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{
						MatchFields: []corev1.NodeSelectorRequirement{
							{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{nodeName}},
						},
					},
				},
			},
		},
	}
	return pod, nil
}

// GenerateMigrationWorkerName generates a worker pod name for the given node.
func GenerateMigrationWorkerName(nodeName string) string {
	return fmt.Sprintf("lj-runtimekit-migration-worker-%s", nodeName)
}

// GenerateTaskMigrationWorkerName generates a task-scoped worker pod name for the
// given node and job: lj-runtimekit-migration-worker-<nodeName>-<first 8 chars of jobUID>.
// Task-level workers coexist for multiple tasks on the same node, so the name
// must carry the task identity.
func GenerateTaskMigrationWorkerName(nodeName, jobUID string) string {
	suffix := jobUID
	if len(suffix) > 8 {
		suffix = suffix[:8]
	}
	return fmt.Sprintf("lj-runtimekit-migration-worker-%s-%s", nodeName, suffix)
}

// AssertJobPhaseIn checks whether jobPhase matches any value in phaseList.
func AssertJobPhaseIn(jobPhase v1beta1.JobPhase, phaseList ...v1beta1.JobPhase) bool {
	for _, phase := range phaseList {
		if jobPhase == phase {
			return true
		}
	}
	return false
}
