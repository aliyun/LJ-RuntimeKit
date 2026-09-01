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

package standard

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/lj-runtimekit/scheduler/pkg/utils"
)

// assignedFlag indicates which resource types have been assigned to a pod during Reserve.
type assignedFlag int

const (
	normalAssigned assignedFlag = 0
	vcoreAssigned  assignedFlag = 1
	vmemAssigned   assignedFlag = 2
)

var patchAnnotationRes2AssignedFlag = map[assignedFlag]string{
	normalAssigned: utils.AnnoNormalGpuAssignedFlag,
	vcoreAssigned:  utils.AnnoCoreAssignedFlag,
	vmemAssigned:   utils.AnnoMemoryAssignedFlag,
}

// patchPodAssigned builds a JSON patch to annotate a pod with assigned device info.
func patchPodAssigned(idList []int, patchMap map[string]string, flags ...assignedFlag) ([]byte, error) {
	ids := make([]string, 0)
	for _, id := range idList {
		ids = append(ids, fmt.Sprintf("%d", id))
	}
	now := time.Now()
	annotations := make(map[string]string, 0)
	annotations[utils.AnnoDeviceIndex] = fmt.Sprintf("%s", strings.Join(ids, ","))
	annotations[utils.AnnoResourceAssumeTime] = fmt.Sprintf("%d", now.UnixNano())
	for _, flag := range flags {
		anno, ok := patchAnnotationRes2AssignedFlag[flag]
		if !ok {
			return nil, fmt.Errorf("flag %d is not registered", flag)
		}
		annotations[anno] = "false"
	}
	for key, val := range patchMap {
		annotations[key] = val
	}
	patchAnnotations := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": annotations,
		},
	}
	return json.Marshal(patchAnnotations)
}
