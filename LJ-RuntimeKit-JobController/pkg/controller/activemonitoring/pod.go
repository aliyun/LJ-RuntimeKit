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

package activemonitoring

import (
	"fmt"
	"strconv"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/rand"

	gpuv1alpha1 "github.com/lj-runtimekit/job-controller/pkg/apis/gpu/v1alpha1"
)

// buildTestPod materialises the detection Pod for a single Task.
func buildTestPod(cr *gpuv1alpha1.ActiveMonitoring, task gpuv1alpha1.TaskStatus, settings *detectionSettings) (*unstructured.Unstructured, error) {
	grace := defaultTerminationGracePeriodSeconds

	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%s-%s", podNamePrefix, task.ID, randSuffix(randSuffixLen)),
			Namespace: cr.Namespace,
			Labels: map[string]string{
				LabelActiveMonitoring:        "true",
				LabelActiveMonitoringCR:      cr.Name,
				LabelActiveMonitoringTask:    task.ID,
				LabelActiveMonitoringRound:   roundKey(task.CurrentCycle, task.CompletedRounds),
				LabelActiveMonitoringAttempt: strconv.Itoa(task.Attempts + 1),
			},
			Annotations: map[string]string{
				AnnoActiveMonitoringTargetMinor: strconv.Itoa(task.GPUMinor),
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         gpuv1alpha1.SchemeGroupVersion.String(),
				Kind:               Kind,
				Name:               cr.Name,
				UID:                cr.UID,
				Controller:         boolPtr(true),
				BlockOwnerDeletion: boolPtr(false),
			}},
		},
		Spec: v1.PodSpec{
			SchedulerName:                 DefaultSchedulerName,
			PriorityClassName:             ActiveMonitoringPriorityClassName,
			RestartPolicy:                 v1.RestartPolicyNever,
			TerminationGracePeriodSeconds: int64Ptr(grace),
			RuntimeClassName:              strPtr(LJRuntimeKitRuntimeClassName),
			Affinity:                      &v1.Affinity{NodeAffinity: hostnameAffinity(task.NodeName)},
			Containers: []v1.Container{{
				Name:  ActiveMonitoringContainerName,
				Image: settings.image,
				Env: []v1.EnvVar{{
					Name:  EnvXPUVisibleDevices,
					Value: strconv.Itoa(task.GPUMinor),
				}},
				Resources: v1.ResourceRequirements{Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("2"),
					v1.ResourceMemory: resource.MustParse("24Gi"),
				}, Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("2"),
					v1.ResourceMemory: resource.MustParse("16Gi"),
				}},
				VolumeMounts: []v1.VolumeMount{{
					Name:      runtimeKitSocketVolumeName,
					MountPath: settings.socketPath,
				}},
				TerminationMessagePath:   "/dev/termination-log",
				TerminationMessagePolicy: v1.TerminationMessageFallbackToLogsOnError,
			}},
			Volumes: []v1.Volume{{
				Name: runtimeKitSocketVolumeName,
				VolumeSource: v1.VolumeSource{
					HostPath: &v1.HostPathVolumeSource{
						Path: settings.socketPath,
						Type: hostPathTypePtr(v1.HostPathSocket),
					},
				},
			}},
		},
	}

	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(pod)
	if err != nil {
		return nil, err
	}
	return &unstructured.Unstructured{Object: obj}, nil
}

// hostnameAffinity pins a Pod to a single node via the kubernetes.io/hostname label.
func hostnameAffinity(nodeName string) *v1.NodeAffinity {
	return &v1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
			NodeSelectorTerms: []v1.NodeSelectorTerm{{
				MatchExpressions: []v1.NodeSelectorRequirement{{
					Key:      HostnameLabelKey,
					Operator: v1.NodeSelectorOpIn,
					Values:   []string{nodeName},
				}},
			}},
		},
	}
}

// randSuffix returns a random alphanumeric suffix of length n for unique Pod names.
func randSuffix(n int) string {
	return rand.String(n)
}

func boolPtr(b bool) *bool { return &b }

func strPtr(s string) *string { return &s }

func int64Ptr(i int64) *int64 { return &i }

func hostPathTypePtr(t v1.HostPathType) *v1.HostPathType { return &t }
