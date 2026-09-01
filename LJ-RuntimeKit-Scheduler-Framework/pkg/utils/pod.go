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
	"fmt"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

// GetMaskFromAnno reads the MIG mask annotation from a pod and returns a map of device index to mask value.
func GetMaskFromAnno(pod *v1.Pod, ids []int) (map[int]int, error) {
	maskMap := make(map[int]int)

	if pod.Annotations == nil {
		return maskMap, NotFoundErr
	}

	if _, ok := pod.Annotations[AnnoMigMutilDevice]; !ok {
		if len(ids) == 0 {
			return maskMap, fmt.Errorf("ids length is 0")
		}

		mask, _ := GetAnnotationsFromPodInt(pod, AnnoMigDevMask)

		maskMap[ids[0]] = mask

		return maskMap, nil
	}

	masksStr, _ := GetAnnotationsFromPodStr(pod, AnnoMigDevMask)
	masksStrSlice := strings.Split(masksStr, ",")

	for _, maskStr := range masksStrSlice {
		maskSlice := strings.Split(maskStr, "-")
		if len(maskSlice) != 2 {
			klog.Errorf("pod %s/%s maskStr %s is not valid", pod.Namespace, pod.Name, maskStr)
		}

		maskId, _ := strconv.Atoi(maskSlice[0])
		maskInt, _ := strconv.Atoi(maskSlice[1])

		maskMap[maskId] = maskInt
	}

	return maskMap, nil
}

// GetResourceReq returns the requested resource quantity for a pod, checking both container limits
// and pod annotations.
func GetResourceReq(pod *v1.Pod, resourceName string) int {
	value := 0
	value = GetResourceReqFromPod(pod, resourceName)
	if value == 0 {
		value = GetResourceReqFromAnno(pod, resourceName)
	}

	return value
}

// GetResourceReqFromAnno returns the requested resource quantity from pod annotations.
func GetResourceReqFromAnno(pod *v1.Pod, resourceName string) int {
	value := 0
	if pod.Annotations == nil {
		return value
	}
	if val, ok := pod.Annotations[resourceName]; ok {
		ret, err := strconv.Atoi(val)
		if err != nil {
			return value
		}
		return ret
	}

	return value
}

// GetResourceReqFromPod returns the total requested resource quantity from all container limits.
func GetResourceReqFromPod(pod *v1.Pod, resourceName string) int {
	value := 0
	for _, c := range pod.Spec.Containers {
		if val, ok := c.Resources.Limits[v1.ResourceName(resourceName)]; ok {
			value += int(val.Value())
		}
	}
	return value
}

// GetAnnotationsFromPodListInt parses a comma-separated annotation value into an int slice.
func GetAnnotationsFromPodListInt(pod *v1.Pod, annoKey string) ([]int, error) {
	value, err := GetAnnotationsFromPodStr(pod, annoKey)
	if err != nil {
		return nil, err
	}
	ids := make([]int, 0)
	infos := strings.Split(value, ",")
	if len(infos) == 0 {
		return nil, fmt.Errorf("value is empty")
	}
	for _, info := range infos {
		info = strings.TrimSpace(info)
		id, err := strconv.Atoi(info)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// GetAnnotationsFromPodStr returns the string value of a pod annotation.
func GetAnnotationsFromPodStr(pod *v1.Pod, annoKey string) (string, error) {
	if pod.Annotations == nil {
		return "", NotFoundErr
	}
	val, ok := pod.Annotations[annoKey]
	if !ok {
		return "", NotFoundErr
	}
	return val, nil
}

// GetAnnotationsFromPodInt returns the integer value of a pod annotation.
func GetAnnotationsFromPodInt(pod *v1.Pod, annoKey string) (int, error) {
	if pod.Annotations == nil {
		return 0, NotFoundErr
	}
	val, ok := pod.Annotations[annoKey]
	if !ok {
		return 0, NotFoundErr
	}
	ret, err := strconv.Atoi(val)
	if err != nil {
		return 0, err
	}
	return ret, nil
}

// GetLabelsFromPodInt returns the integer value of a pod label, or 0 if absent.
func GetLabelsFromPodInt(pod *v1.Pod, labelKey string) int {
	if pod.Labels == nil {
		return 0
	}
	val, ok := pod.Labels[labelKey]
	if !ok {
		return 0
	}
	ret, err := strconv.Atoi(val)
	if err != nil {
		return 0
	}
	return ret
}

// GetLabelsFromPodStr returns the string value of a pod label, or empty string if absent.
func GetLabelsFromPodStr(pod *v1.Pod, labelKey string) string {
	if pod.Labels == nil {
		return ""
	}
	val, ok := pod.Labels[labelKey]
	if !ok {
		return ""
	}
	return val
}

// NotRunning returns true if the pod is not running (not yet scheduled or completed/failed).
func NotRunning(pod *v1.Pod) bool {
	if len(pod.Spec.NodeName) == 0 {
		return true
	}
	if pod.Status.Phase == v1.PodSucceeded || pod.Status.Phase == v1.PodFailed {
		return true
	}
	return false
}

// IsStandardPodTask returns true if the pod requests GPU core or memory resources.
func IsStandardPodTask(pod *v1.Pod) bool {
	return GetResourceReqFromPod(pod, ResourceCore) > 0 || GetResourceReqFromPod(pod, ResourceMemory) > 0
}

// IsOldNormalPodTask returns true if the pod requests a whole GPU card (vcore >= 100 and divisible by 100).
func IsOldNormalPodTask(pod *v1.Pod) bool {
	vcore := GetResourceReqFromPod(pod, ResourceCore)
	return vcore >= 100 && vcore%100 == 0
}

// IsPublicMigPodTask returns true if the pod has MIG device enable annotation.
func IsPublicMigPodTask(pod *v1.Pod) bool {
	_, ok := pod.Annotations[AnnoMigDeviceEnableKey]
	return ok
}

// IsNormalPodTask returns true if the pod requests a whole GPU card (gpu/nvidia/GPU count).
func IsNormalPodTask(pod *v1.Pod) bool {
	return GetResourceReqFromPod(pod, ResourceGPU) > 0 ||
		(GetResourceReqFromPod(pod, ResourceNvidia) > 0 && !IsPublicMigPodTask(pod)) ||
		IsOldNormalPodTask(pod) ||
		GetResourceReqFromPod(pod, ResourceCount) > 0
}

// ResponseForTask returns true if the scheduler plugin is responsible for this pod's GPU scheduling.
func ResponseForTask(pod *v1.Pod) bool {
	return IsStandardPodTask(pod) || IsNormalPodTask(pod) || IsMigPodTask(pod) || IsPublicMigPodTask(pod)
}

// IsLocalPendingReady returns true if the pod is pending or not ready locally on the node.
// Used by the device plugin to determine if a pod's resources should be considered pending.
func IsLocalPendingReady(pod *v1.Pod) bool {
	if pod.Status.Phase != v1.PodRunning {
		return true
	}
	ready := true
	for _, con := range pod.Status.Conditions {
		if con.Type == v1.PodReady {
			if con.Status == v1.ConditionTrue {
				ready = false
				break
			}
		}
	}
	return ready
}

// IsMigPodTask returns true if the pod requests GPU slice (MIG) resources.
func IsMigPodTask(pod *v1.Pod) bool {
	return GetResourceReqFromPod(pod, ResourceSlice) > 0
}

// IsCiTask returns true if the pod is a MIG CI (Compute Instance) task (requesting both slice and memory).
func IsCiTask(pod *v1.Pod) bool {
	return GetResourceReqFromPod(pod, ResourceMemory) > 0 && IsMigPodTask(pod)
}

// IsGiTask returns true if the pod is a MIG GI (GPU Instance) task (requesting slice only).
func IsGiTask(pod *v1.Pod) bool {
	return GetResourceReqFromPod(pod, ResourceMemory) <= 0 && IsMigPodTask(pod)
}
