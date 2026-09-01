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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetResourceReqFromPod(t *testing.T) {
	tests := []struct {
		name         string
		pod          *v1.Pod
		resourceName string
		expected     int
	}{
		{
			name: "single container with resource limit",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName(ResourceMemory): resource.MustParse("2Gi"),
								},
							},
						},
					},
				},
			},
			resourceName: ResourceMemory,
			expected:     2147483648, // 2 * 1024^3
		},
		{
			name: "multiple containers",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName(ResourceCore): resource.MustParse("2"),
								},
							},
						},
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName(ResourceCore): resource.MustParse("3"),
								},
							},
						},
					},
				},
			},
			resourceName: ResourceCore,
			expected:     5,
		},
		{
			name: "no limits defined",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{}},
				},
			},
			resourceName: ResourceNvidia,
			expected:     0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetResourceReqFromPod(tt.pod, tt.resourceName)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetAnnotationsFromPodStr(t *testing.T) {
	podWithAnno := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				"device/index": "0,1",
			},
		},
	}
	podWithoutAnno := &v1.Pod{}
	podNilAnno := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Annotations: nil}}

	tests := []struct {
		name    string
		pod     *v1.Pod
		key     string
		wantVal string
		wantErr bool
	}{
		{"found", podWithAnno, "device/index", "0,1", false},
		{"not found", podWithAnno, "missing/key", "", true},
		{"nil annotations", podNilAnno, "any", "", true},
		{"empty annotations", podWithoutAnno, "any", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := GetAnnotationsFromPodStr(tt.pod, tt.key)
			if tt.wantErr {
				assert.Error(t, err)
				assert.True(t, errors.Is(err, NotFoundErr))
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.wantVal, val)
			}
		})
	}
}

func TestGetAnnotationsFromPodInt(t *testing.T) {
	podWithAnno := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				"replicas": "3",
			},
		},
	}
	podInvalid := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				"replicas": "abc",
			},
		},
	}
	podNil := &v1.Pod{}

	tests := []struct {
		name    string
		pod     *v1.Pod
		key     string
		wantVal int
		wantErr bool
	}{
		{"valid int", podWithAnno, "replicas", 3, false},
		{"invalid int", podInvalid, "replicas", 0, true},
		{"missing key", podWithAnno, "missing", 0, true},
		{"nil annotations", podNil, "any", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := GetAnnotationsFromPodInt(tt.pod, tt.key)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.wantVal, val)
			}
		})
	}
}

func TestGetAnnotationsFromPodListInt(t *testing.T) {
	podValid := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				"device/index": "0,1, 2",
			},
		},
	}
	podEmpty := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				"device/index": "",
			},
		},
	}
	podInvalid := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				"device/index": "0,abc",
			},
		},
	}
	podNil := &v1.Pod{}

	tests := []struct {
		name    string
		pod     *v1.Pod
		key     string
		want    []int
		wantErr bool
	}{
		{"valid list", podValid, "device/index", []int{0, 1, 2}, false},
		{"empty value", podEmpty, "device/index", nil, true},
		{"invalid number", podInvalid, "device/index", nil, true},
		{"missing key", podValid, "missing", nil, true},
		{"nil annotations", podNil, "any", nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := GetAnnotationsFromPodListInt(tt.pod, tt.key)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, val)
			}
		})
	}
}

func TestGetLabelsFromPodInt(t *testing.T) {
	podWithLabel := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				"priority": "5",
			},
		},
	}
	podInvalid := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				"priority": "high",
			},
		},
	}
	podNil := &v1.Pod{}

	assert.Equal(t, 5, GetLabelsFromPodInt(podWithLabel, "priority"))
	assert.Equal(t, 0, GetLabelsFromPodInt(podInvalid, "priority"))
	assert.Equal(t, 0, GetLabelsFromPodInt(podNil, "any"))
	assert.Equal(t, 0, GetLabelsFromPodInt(&v1.Pod{}, "missing"))
}

func TestGetLabelsFromPodStr(t *testing.T) {
	podWithLabel := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				"app": "scheduler",
			},
		},
	}
	podNil := &v1.Pod{}

	assert.Equal(t, "scheduler", GetLabelsFromPodStr(podWithLabel, "app"))
	assert.Equal(t, "", GetLabelsFromPodStr(podWithLabel, "missing"))
	assert.Equal(t, "", GetLabelsFromPodStr(podNil, "any"))
	assert.Equal(t, "", GetLabelsFromPodStr(&v1.Pod{}, "any"))
}

func TestNotRunning(t *testing.T) {
	tests := []struct {
		name     string
		pod      *v1.Pod
		expected bool
	}{
		{
			name:     "not scheduled",
			pod:      &v1.Pod{Spec: v1.PodSpec{NodeName: ""}},
			expected: true,
		},
		{
			name: "succeeded",
			pod: &v1.Pod{
				Spec:   v1.PodSpec{NodeName: "node1"},
				Status: v1.PodStatus{Phase: v1.PodSucceeded},
			},
			expected: true,
		},
		{
			name: "failed",
			pod: &v1.Pod{
				Spec:   v1.PodSpec{NodeName: "node1"},
				Status: v1.PodStatus{Phase: v1.PodFailed},
			},
			expected: true,
		},
		{
			name: "running",
			pod: &v1.Pod{
				Spec:   v1.PodSpec{NodeName: "node1"},
				Status: v1.PodStatus{Phase: v1.PodRunning},
			},
			expected: false,
		},
		{
			name: "pending but scheduled",
			pod: &v1.Pod{
				Spec:   v1.PodSpec{NodeName: "node1"},
				Status: v1.PodStatus{Phase: v1.PodPending},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, NotRunning(tt.pod))
		})
	}
}

func TestIsStandardPodTask(t *testing.T) {
	// Assume ResourceCore = "cpu.virtual" and ResourceMemory = "memory.virtual"
	// These are just placeholder strings; actual values depend on your constants

	podWithVCore := &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(ResourceCore): resource.MustParse("2"),
						},
					},
				},
			},
		},
	}
	podWithVMem := &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(ResourceMemory): resource.MustParse("1Gi"),
						},
					},
				},
			},
		},
	}
	podNone := &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{{}},
		},
	}

	assert.True(t, IsStandardPodTask(podWithVCore))
	assert.True(t, IsStandardPodTask(podWithVMem))
	assert.False(t, IsStandardPodTask(podNone))
}

func TestIsOldNormalPodTask(t *testing.T) {
	pod100 := &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(ResourceCore): resource.MustParse("100"),
						},
					},
				},
			},
		},
	}
	pod200 := &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(ResourceCore): resource.MustParse("200"),
						},
					},
				},
			},
		},
	}
	pod150 := &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(ResourceCore): resource.MustParse("150"),
						},
					},
				},
			},
		},
	}
	podNone := &v1.Pod{}

	assert.True(t, IsOldNormalPodTask(pod100))
	assert.True(t, IsOldNormalPodTask(pod200))
	assert.False(t, IsOldNormalPodTask(pod150))
	assert.False(t, IsOldNormalPodTask(podNone))
}

func TestIsNormalPodTask(t *testing.T) {
	gpuPod := &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(ResourceNvidia): resource.MustParse("1"),
						},
					},
				},
			},
		},
	}
	amdPod := &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(ResourceGPU): resource.MustParse("1"),
						},
					},
				},
			},
		},
	}
	ppuPod := &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(ResourceGPU): resource.MustParse("1"),
						},
					},
				},
			},
		},
	}
	oldNormalPod := &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(ResourceCore): resource.MustParse("100"),
						},
					},
				},
			},
		},
	}
	nonePod := &v1.Pod{}

	assert.True(t, IsNormalPodTask(gpuPod))
	assert.True(t, IsNormalPodTask(amdPod))
	assert.True(t, IsNormalPodTask(ppuPod))
	assert.True(t, IsNormalPodTask(oldNormalPod))
	assert.False(t, IsNormalPodTask(nonePod))
}

func TestResponseForTask(t *testing.T) {
	standardPod := &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(ResourceMemory): resource.MustParse("1Gi"),
						},
					},
				},
			},
		},
	}
	normalPod := &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(ResourceNvidia): resource.MustParse("1"),
						},
					},
				},
			},
		},
	}
	neitherPod := &v1.Pod{}

	assert.True(t, ResponseForTask(standardPod))
	assert.True(t, ResponseForTask(normalPod))
	assert.False(t, ResponseForTask(neitherPod))
}

func TestIsLocalPendingReady(t *testing.T) {
	readyPod := &v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
			Conditions: []v1.PodCondition{
				{Type: v1.PodReady, Status: v1.ConditionTrue},
			},
		},
	}
	notReadyPod := &v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
			Conditions: []v1.PodCondition{
				{Type: v1.PodReady, Status: v1.ConditionFalse},
			},
		},
	}
	noConditionPod := &v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
		},
	}
	pendingPod := &v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	assert.False(t, IsLocalPendingReady(readyPod))      // ready → not pending-ready
	assert.True(t, IsLocalPendingReady(notReadyPod))    // running but not ready → local pending ready
	assert.True(t, IsLocalPendingReady(noConditionPod)) // no ready condition → treated as not ready
	assert.True(t, IsLocalPendingReady(pendingPod))     // not running → local pending ready
}
