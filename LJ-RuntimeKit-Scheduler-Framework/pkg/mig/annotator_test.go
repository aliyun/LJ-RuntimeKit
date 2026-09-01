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
	"testing"

	"github.com/lj-runtimekit/scheduler/pkg/utils"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPatchPodAssigned_GI(t *testing.T) {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mig-gi-pod",
			Namespace: "default",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(utils.ResourceSlice): resource.MustParse("1"),
						},
					},
				},
			},
		},
	}

	result, err := patchPodAssigned(pod, utils.TypeMigGi, 0b111, 0, 3, 1)
	assert.NoError(t, err)
	assert.NotNil(t, result)

	var patch map[string]interface{}
	err = json.Unmarshal(result, &patch)
	assert.NoError(t, err)

	metadata := patch["metadata"].(map[string]interface{})
	annotations := metadata["annotations"].(map[string]interface{})

	assert.Equal(t, "true", annotations[utils.AnnoMigDeviceEnableKey])
	assert.Equal(t, "gi", annotations[utils.AnnoMigDeviceType])
	assert.Equal(t, "7", annotations[utils.AnnoMigDevMask])
	assert.Equal(t, "0", annotations[utils.AnnoMigTopoStart])
	assert.Equal(t, "3", annotations[utils.AnnoMigTopoSize])
	assert.Equal(t, "1", annotations[utils.AnnoDeviceIndex])
	assert.Equal(t, "1", annotations[utils.ResourceSlice])
	assert.Equal(t, "false", annotations[utils.AnnotationSliceAssignedFlag])

	// GI mode should NOT have memory assigned flag
	_, hasMemAssigned := annotations[utils.AnnoMemoryAssignedFlag]
	assert.False(t, hasMemAssigned, "GI mode should not set memory assigned flag")

	// Verify assume time is set
	_, hasTime := annotations[utils.AnnoResourceAssumeTime]
	assert.True(t, hasTime, "should have assume time")
}

func TestPatchPodAssigned_CI(t *testing.T) {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mig-ci-pod",
			Namespace: "default",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(utils.ResourceSlice):  resource.MustParse("1"),
							v1.ResourceName(utils.ResourceMemory): resource.MustParse("2048"),
							v1.ResourceName(utils.ResourceCore):   resource.MustParse("25"),
						},
					},
				},
			},
		},
	}

	result, err := patchPodAssigned(pod, utils.TypeMigCi, 0b001, 2, 1, 0)
	assert.NoError(t, err)
	assert.NotNil(t, result)

	var patch map[string]interface{}
	err = json.Unmarshal(result, &patch)
	assert.NoError(t, err)

	metadata := patch["metadata"].(map[string]interface{})
	annotations := metadata["annotations"].(map[string]interface{})

	assert.Equal(t, "true", annotations[utils.AnnoMigDeviceEnableKey])
	assert.Equal(t, "ci", annotations[utils.AnnoMigDeviceType])
	assert.Equal(t, "1", annotations[utils.AnnoMigDevMask])
	assert.Equal(t, "2", annotations[utils.AnnoMigTopoStart])
	assert.Equal(t, "1", annotations[utils.AnnoMigTopoSize])
	assert.Equal(t, "0", annotations[utils.AnnoDeviceIndex])
	assert.Equal(t, "1", annotations[utils.ResourceSlice])
	assert.Equal(t, "false", annotations[utils.AnnotationSliceAssignedFlag])

	// CI mode SHOULD have memory assigned flag and memory value
	assert.Equal(t, "false", annotations[utils.AnnoMemoryAssignedFlag])
	assert.Equal(t, "2048", annotations[utils.AnnoMemoryResourceByPod])
}
