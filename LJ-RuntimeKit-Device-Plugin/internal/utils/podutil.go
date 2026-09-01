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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
)

func GetPodResource(pod *v1.Pod, resourceName string) uint {
	var total uint
	containers := pod.Spec.Containers
	for _, container := range containers {
		if val, ok := container.Resources.Limits[v1.ResourceName(resourceName)]; ok {
			total += uint(val.Value())
		}
	}
	return total
}

// IsWholeCardPod reports whether the total gpu-core limit reaches
// WholeCardCore; invalid amounts above it still count.
func IsWholeCardPod(pod *v1.Pod) bool {
	core := GetPodResource(pod, ResourceCore)
	if core >= WholeCardCore {
		if core%WholeCardCore != 0 {
			klog.Warningf("pod %s/%s requests invalid gpu-core amount %d, treated as whole card", pod.Namespace, pod.Name, core)
		}
		return true
	}
	return false
}

// ScheduledByLjScheduler reports whether the pod went through the lj-runtime
// scheduler, told by the device index annotation.
func ScheduledByLjScheduler(pod *v1.Pod) bool {
	_, found := pod.Annotations[AnnotationDeviceIndex]
	return found
}

func GetScheduledDeviceIds(pod *v1.Pod) ([]int, error) {
	ids := make([]int, 0)
	if len(pod.ObjectMeta.Annotations) > 0 {
		value, found := pod.ObjectMeta.Annotations[AnnotationDeviceIndex]
		if found {
			infos := strings.Split(value, ",")
			for _, info := range infos {
				info = strings.TrimSpace(info)
				id, err := strconv.Atoi(info)
				if err != nil || id < 0 {
					return nil, fmt.Errorf("failed to parse GPU IDs")
				}
				ids = append(ids, id)
			}
		} else {
			return ids, nil
		}
	}
	return ids, nil
}

// GetPcieSwitchIndex returns the scheduler assigned pcie switch indexes as a
// normalized comma separated string. Empty means no assignment.
func GetPcieSwitchIndex(pod *v1.Pod) string {
	if len(pod.Annotations) == 0 {
		return ""
	}
	value, found := pod.Annotations[AnnotationPcieSwitchIndex]
	if !found {
		return ""
	}

	indexes := make([]string, 0)
	for _, item := range strings.Split(value, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		// keep the raw string but reject non numeric entries, xdm expects
		// plain switch indexes and a malformed value must not be forwarded
		if _, err := strconv.Atoi(item); err != nil {
			klog.Errorf("pod %s/%s has invalid pcie switch index %q in annotation %s, skipped",
				pod.Namespace, pod.Name, item, AnnotationPcieSwitchIndex)
			continue
		}
		indexes = append(indexes, item)
	}
	return strings.Join(indexes, ",")
}

func getReplicasFromLabels(pod *v1.Pod) int {
	defaultReplicas := 1
	if pod.Labels == nil {
		return defaultReplicas
	}
	replicas, ok := pod.Labels[LabelsReplicas]
	if !ok {
		return defaultReplicas
	}
	rep, err := strconv.Atoi(replicas)
	if err != nil {
		klog.Errorf("error: parse %s failed due to %v", replicas, err)
		return defaultReplicas
	}
	if rep <= 0 {
		return defaultReplicas
	}
	return rep
}

func GetFrontendFromLabels(pod *v1.Pod) (bool, error) {
	if pod.Labels == nil {
		return false, fmt.Errorf("pod %s has no labels", pod.Name)
	}
	frontend, ok := pod.Labels[LabelsFrontend]
	if !ok {
		return false, fmt.Errorf("pod %s label %s does not exist", pod.Name, LabelsFrontend)
	}

	switch strings.ToLower(frontend) {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf("invalid value for label %q: %s", LabelsFrontend, frontend)
	}
}

func CheckBluffingPod(pod *v1.Pod) (bool, error) {
	if pod.Labels == nil {
		return false, fmt.Errorf("pod %s has no labels", pod.Name)
	}
	bluffing, ok := pod.Labels[LabelsBluffing]
	if !ok {
		return false, fmt.Errorf("pod %s label %s does not exist", pod.Name, LabelsBluffing)
	}
	return bluffing == "true", nil
}

func GetBluffingMemory(pod *v1.Pod) (string, error) {
	if len(pod.Spec.Containers) == 0 {
		return "", fmt.Errorf("no containers found in pod spec")
	}
	for _, container := range pod.Spec.Containers {
		if limit, ok := container.Resources.Limits[v1.ResourceMemory]; ok {
			innerJSON := map[string]interface{}{
				"bluffing": true,
				"memory":   limit.Value(),
			}
			innerJSONBytes, err := json.Marshal(innerJSON)
			if err != nil {
				return "", fmt.Errorf("failed to marshal inner JSON: %v", err)
			}
			return string(innerJSONBytes), nil
		}
	}
	return "", fmt.Errorf("no memory limit set for any container in the pod")
}

func RetryOnConflictOrTooManyRequests(fn func() error) error {
	return retry.OnError(retry.DefaultBackoff, func(err error) bool {
		return errors.IsConflict(err) || errors.IsTooManyRequests(err)
	}, fn)
}

func GetFrontendAnnotations(frontend bool, pod *v1.Pod) (string, error) {
	var ip string
	var ipFound bool
	if ip, ipFound = pod.Annotations[AnnotationFrontendIp]; !ipFound {
		return "", fmt.Errorf("frontend IP not found in pod %s(%s) annotations", pod.Name, pod.Namespace)
	}
	var portStr string
	var portFound bool
	if portStr, portFound = pod.Annotations[AnnotationFrontendPort]; !portFound {
		return "", fmt.Errorf("frontend port not found in pod %s(%s) annotations", pod.Name, pod.Namespace)
	}

	innerJSON := map[string]interface{}{
		"frontend": frontend,
		"srcIp":    ip,
		"port":     portStr,
	}

	innerJSONBytes, err := json.Marshal(innerJSON)
	if err != nil {
		return "", fmt.Errorf("failed to marshal inner JSON: %v", err)
	}
	return string(innerJSONBytes), nil
}

func GetMigMaskFromAnnotations(pod *v1.Pod) (uint8, error) {
	mask, found := pod.Annotations[AnnotationMigDeviceCIMask]
	if !found {
		klog.V(4).Infof("pod %s(%s) has no MIG mask annotation", pod.Name, pod.Namespace)
		return 0, fmt.Errorf("pod has no MIG mask annotation")
	}

	val, err := strconv.Atoi(mask)
	if err != nil {
		klog.Errorf("pod %s(%s) failed to parse %s: %v", pod.Name, pod.Namespace, mask, err)
		return 0, err
	}
	return uint8(val), nil
}

func GetMigStartAndSizeFromAnnotations(pod *v1.Pod) (int, int, error) {
	startStr, found := pod.Annotations[AnnotationMigTopologyStart]
	if !found {
		klog.V(4).Infof("pod %s(%s) has no mig-topology-start annotation", pod.Name, pod.Namespace)
		return 0, 0, fmt.Errorf("pod has no mig-topology-start annotation")
	}
	sizeStr, found := pod.Annotations[AnnotationMigTopologySize]
	if !found {
		klog.V(4).Infof("pod %s(%s) has no mig-topology-size annotation", pod.Name, pod.Namespace)
		return 0, 0, fmt.Errorf("pod has no mig-topology-size annotation")
	}
	klog.V(4).Infof("got device topology annotation %s %s from pod %s/%s", startStr, sizeStr, pod.Namespace, pod.Name)

	start, err := strconv.Atoi(startStr)
	if err != nil {
		klog.Errorf("failed to parse %s to int: %v", startStr, err)
		return 0, 0, err
	}

	size, err := strconv.Atoi(sizeStr)
	if err != nil {
		klog.Errorf("failed to parse %s to int: %v", sizeStr, err)
		return 0, 0, err
	}

	return start, size, nil
}

func GetMigTypeFromAnnotation(pod *v1.Pod) MigDeviceType {
	if len(pod.Annotations) > 0 {
		migType, _ := pod.Annotations[AnnotationMigDeviceType]
		return MigDeviceType(migType)
	}
	return MigDeviceType("")
}

func MigType2Mode(enum MigDeviceType) int {
	switch enum {
	case TypeMigCi:
		return 0
	default:
		return 1
	}
}
