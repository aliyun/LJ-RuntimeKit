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

package mig

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/lj-runtimekit/scheduler/pkg/utils"

	v1 "k8s.io/api/core/v1"
)

// patchPodAssigned builds a JSON patch to annotate a MIG pod with assigned device info.
func patchPodAssigned(pod *v1.Pod, migType utils.MigDeviceType, mask uint8, start int, size int, devId int) ([]byte, error) {
	annotations := make(map[string]string)
	annotations[utils.AnnoMigDeviceEnableKey] = "true"
	annotations[utils.AnnoMigDeviceType] = string(migType)
	annotations[utils.AnnoResourceAssumeTime] = fmt.Sprintf("%d", time.Now().UnixNano())
	annotations[utils.AnnotationSliceAssignedFlag] = "false"
	annotations[utils.AnnoMigDevMask] = fmt.Sprintf("%d", mask)
	annotations[utils.AnnoMigTopoStart] = fmt.Sprintf("%d", start)
	annotations[utils.AnnoMigTopoSize] = fmt.Sprintf("%d", size)
	annotations[utils.AnnoDeviceIndex] = fmt.Sprintf("%d", devId)

	gpuSlice := utils.GetResourceReqFromPod(pod, utils.ResourceSlice)
	annotations[utils.ResourceSlice] = fmt.Sprintf("%d", gpuSlice)
	if migType == utils.TypeMigCi {
		vmemory := utils.GetResourceReqFromPod(pod, utils.ResourceMemory)
		annotations[utils.AnnoMemoryResourceByPod] = fmt.Sprintf("%d", vmemory)
		annotations[utils.AnnoMemoryAssignedFlag] = "false"
	}
	patchAnnotations := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": annotations,
		},
	}
	return json.Marshal(patchAnnotations)
}
