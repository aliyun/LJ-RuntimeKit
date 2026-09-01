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
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetPodResource(t *testing.T) {
	pod := &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName("cpu.virtual"): resource.MustParse("2"),
						},
					},
				},
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName("cpu.virtual"): resource.MustParse("3"),
						},
					},
				},
			},
		},
	}

	result := GetPodResource(pod, "cpu.virtual")
	assert.Equal(t, uint(5), result)

	// Test unknown resource
	result = GetPodResource(pod, "unknown")
	assert.Equal(t, uint(0), result)
}

// TestIsWholeCardPod verifies whole card recognition purely by the total
// gpu-core limit reaching WholeCardCore.
func TestIsWholeCardPod(t *testing.T) {
	podWithCore := func(core string) *v1.Pod {
		return &v1.Pod{
			Spec: v1.PodSpec{Containers: []v1.Container{{
				Resources: v1.ResourceRequirements{Limits: v1.ResourceList{
					v1.ResourceName(ResourceCore): resource.MustParse(core),
				}},
			}}},
		}
	}
	tests := []struct {
		name     string
		core     string
		expected bool
	}{
		{name: "core reaching one whole card", core: "100", expected: true},
		{name: "invalid amount above the threshold still counts", core: "150", expected: true},
		{name: "two whole cards", core: "200", expected: true},
		{name: "shared card amount", core: "50", expected: false},
		{name: "no core limit at all", core: "", expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &v1.Pod{}
			if tt.core != "" {
				pod = podWithCore(tt.core)
			}
			assert.Equal(t, tt.expected, IsWholeCardPod(pod))
		})
	}
}

// TestScheduledByLjScheduler verifies the device index annotation tells an
// lj-runtime scheduled pod apart from a community one.
func TestScheduledByLjScheduler(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		expected    bool
	}{
		{
			name:        "scheduled by lj-runtime",
			annotations: map[string]string{AnnotationDeviceIndex: "0,1"},
			expected:    true,
		},
		{
			name:        "annotation present but empty still means scheduled",
			annotations: map[string]string{AnnotationDeviceIndex: ""},
			expected:    true,
		},
		{
			name:        "no annotations at all",
			annotations: nil,
			expected:    false,
		},
		{
			name:        "only unrelated annotations",
			annotations: map[string]string{AnnotationMemoryAssignedFlag: "false"},
			expected:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Annotations: tt.annotations}}
			assert.Equal(t, tt.expected, ScheduledByLjScheduler(pod))
		})
	}
}

func TestGetScheduledDeviceIds(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		expected    []int
		expectError bool
	}{
		{
			name:        "valid ids",
			annotations: map[string]string{AnnotationDeviceIndex: "0,1, 2"},
			expected:    []int{0, 1, 2},
			expectError: false,
		},
		{
			name:        "empty annotation",
			annotations: map[string]string{AnnotationDeviceIndex: ""},
			expected:    nil,
			expectError: true,
		},
		{
			name:        "no annotation",
			annotations: map[string]string{},
			expected:    []int{},
			expectError: false,
		},
		{
			name:        "invalid id",
			annotations: map[string]string{AnnotationDeviceIndex: "0,abc"},
			expected:    nil,
			expectError: true,
		},
		{
			name:        "negative id",
			annotations: map[string]string{AnnotationDeviceIndex: "0,-1"},
			expected:    nil,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: tt.annotations,
				},
			}
			ids, err := GetScheduledDeviceIds(pod)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, ids)
			}
		})
	}
}

func TestGetReplicasFromLabels(t *testing.T) {
	tests := []struct {
		name     string
		labels   map[string]string
		expected int
	}{
		{
			name:     "valid replicas",
			labels:   map[string]string{LabelsReplicas: "3"},
			expected: 3,
		},
		{
			name:     "no labels",
			labels:   nil,
			expected: 1,
		},
		{
			name:     "missing label",
			labels:   map[string]string{"other": "value"},
			expected: 1,
		},
		{
			name:     "invalid number",
			labels:   map[string]string{LabelsReplicas: "abc"},
			expected: 1,
		},
		{
			name:     "zero replicas",
			labels:   map[string]string{LabelsReplicas: "0"},
			expected: 1,
		},
		{
			name:     "negative replicas",
			labels:   map[string]string{LabelsReplicas: "-1"},
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: tt.labels,
				},
			}
			result := getReplicasFromLabels(pod)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetFrontendFromLabels(t *testing.T) {
	tests := []struct {
		name        string
		labels      map[string]string
		expected    bool
		expectError bool
	}{
		{
			name:        "true",
			labels:      map[string]string{LabelsFrontend: "true"},
			expected:    true,
			expectError: false,
		},
		{
			name:        "True (case insensitive)",
			labels:      map[string]string{LabelsFrontend: "True"},
			expected:    true,
			expectError: false,
		},
		{
			name:        "false",
			labels:      map[string]string{LabelsFrontend: "false"},
			expected:    false,
			expectError: false,
		},
		{
			name:        "no labels",
			labels:      nil,
			expected:    false,
			expectError: true,
		},
		{
			name:        "missing label",
			labels:      map[string]string{"other": "value"},
			expected:    false,
			expectError: true,
		},
		{
			name:        "invalid value",
			labels:      map[string]string{LabelsFrontend: "maybe"},
			expected:    false,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "test-pod",
					Labels: tt.labels,
				},
			}
			result, err := GetFrontendFromLabels(pod)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestCheckBluffingPod(t *testing.T) {
	tests := []struct {
		name        string
		labels      map[string]string
		expected    bool
		expectError bool
	}{
		{
			name:        "bluffing true",
			labels:      map[string]string{LabelsBluffing: "true"},
			expected:    true,
			expectError: false,
		},
		{
			name:        "bluffing false",
			labels:      map[string]string{LabelsBluffing: "false"},
			expected:    false,
			expectError: false,
		},
		{
			name:        "no labels",
			labels:      nil,
			expected:    false,
			expectError: true,
		},
		{
			name:        "missing label",
			labels:      map[string]string{"other": "value"},
			expected:    false,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "test-pod",
					Labels: tt.labels,
				},
			}
			result, err := CheckBluffingPod(pod)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestGetBluffingMemory(t *testing.T) {
	tests := []struct {
		name        string
		pod         *v1.Pod
		expectError bool
	}{
		{
			name: "with memory limit",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceMemory: resource.MustParse("2Gi"),
								},
							},
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "no containers",
			pod: &v1.Pod{
				Spec: v1.PodSpec{},
			},
			expectError: true,
		},
		{
			name: "no memory limit",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{}},
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := GetBluffingMemory(tt.pod)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Contains(t, result, `"bluffing":true`)
				assert.Contains(t, result, `"memory":2147483648`) // 2Gi in bytes
			}
		})
	}
}

func TestRetryOnConflictOrTooManyRequests(t *testing.T) {
	attempts := 0
	errConflict := errors.NewConflict(v1.Resource("pod"), "test-pod", nil)
	errTooMany := errors.NewTooManyRequests("too many requests", 10)
	errOther := errors.NewServiceUnavailable("other error")

	// Test retry on conflict
	fn := func() error {
		attempts++
		if attempts < 3 {
			return errConflict
		}
		return nil
	}

	err := RetryOnConflictOrTooManyRequests(fn)
	assert.NoError(t, err)
	assert.Equal(t, 3, attempts)

	// Test retry on too many requests
	attempts = 0
	fn2 := func() error {
		attempts++
		if attempts < 2 {
			return errTooMany
		}
		return nil
	}

	err = RetryOnConflictOrTooManyRequests(fn2)
	assert.NoError(t, err)
	assert.Equal(t, 2, attempts)

	// Test no retry on other error
	attempts = 0
	fn3 := func() error {
		attempts++
		return errOther
	}

	err = RetryOnConflictOrTooManyRequests(fn3)
	assert.Error(t, err)
	assert.Equal(t, 1, attempts)
}

func TestGetFrontendAnnotations(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		expectError bool
	}{
		{
			name: "valid annotations",
			annotations: map[string]string{
				AnnotationFrontendIp:   "192.168.1.100",
				AnnotationFrontendPort: "8080",
			},
			expectError: false,
		},
		{
			name: "missing ip",
			annotations: map[string]string{
				AnnotationFrontendPort: "8080",
			},
			expectError: true,
		},
		{
			name: "missing port",
			annotations: map[string]string{
				AnnotationFrontendIp: "192.168.1.100",
			},
			expectError: true,
		},
		{
			name:        "missing both",
			annotations: map[string]string{},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "test-pod",
					Namespace:   "default",
					Annotations: tt.annotations,
				},
			}
			result, err := GetFrontendAnnotations(true, pod)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Contains(t, result, `"frontend":true`)
				assert.Contains(t, result, `"srcIp":"192.168.1.100"`)
				assert.Contains(t, result, `"port":"8080"`)
			}
		})
	}
}

func TestGetMigMaskFromAnnotations(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		expected    uint8
		expectError bool
	}{
		{
			name:        "valid mask",
			annotations: map[string]string{AnnotationMigDeviceCIMask: "255"},
			expected:    255,
			expectError: false,
		},
		{
			name:        "zero mask",
			annotations: map[string]string{AnnotationMigDeviceCIMask: "0"},
			expected:    0,
			expectError: false,
		},
		{
			name:        "invalid mask",
			annotations: map[string]string{AnnotationMigDeviceCIMask: "abc"},
			expected:    0,
			expectError: true,
		},
		{
			name:        "missing annotation",
			annotations: map[string]string{},
			expected:    0,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "test-pod",
					Namespace:   "default",
					Annotations: tt.annotations,
				},
			}
			result, err := GetMigMaskFromAnnotations(pod)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestGetMigStartAndSizeFromAnnotations(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		expectStart int
		expectSize  int
		expectError bool
	}{
		{
			name: "valid annotations",
			annotations: map[string]string{
				AnnotationMigTopologyStart: "0",
				AnnotationMigTopologySize:  "4",
			},
			expectStart: 0,
			expectSize:  4,
			expectError: false,
		},
		{
			name: "large values",
			annotations: map[string]string{
				AnnotationMigTopologyStart: "100",
				AnnotationMigTopologySize:  "50",
			},
			expectStart: 100,
			expectSize:  50,
			expectError: false,
		},
		{
			name: "missing start",
			annotations: map[string]string{
				AnnotationMigTopologySize: "4",
			},
			expectError: true,
		},
		{
			name: "missing size",
			annotations: map[string]string{
				AnnotationMigTopologyStart: "0",
			},
			expectError: true,
		},
		{
			name: "invalid start",
			annotations: map[string]string{
				AnnotationMigTopologyStart: "abc",
				AnnotationMigTopologySize:  "4",
			},
			expectError: true,
		},
		{
			name: "invalid size",
			annotations: map[string]string{
				AnnotationMigTopologyStart: "0",
				AnnotationMigTopologySize:  "xyz",
			},
			expectError: true,
		},
		{
			name:        "missing both",
			annotations: map[string]string{},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "test-pod",
					Namespace:   "default",
					Annotations: tt.annotations,
				},
			}
			start, size, err := GetMigStartAndSizeFromAnnotations(pod)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectStart, start)
				assert.Equal(t, tt.expectSize, size)
			}
		})
	}
}

func TestGetMigTypeFromAnnotation(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		expected    MigDeviceType
	}{
		{
			name:        "ci type",
			annotations: map[string]string{AnnotationMigDeviceType: "ci"},
			expected:    TypeMigCi,
		},
		{
			name:        "gi type",
			annotations: map[string]string{AnnotationMigDeviceType: "gi"},
			expected:    TypeMigGi,
		},
		{
			name:        "empty annotation",
			annotations: map[string]string{},
			expected:    MigDeviceType(""),
		},
		{
			name:        "missing annotation",
			annotations: map[string]string{"other": "value"},
			expected:    MigDeviceType(""),
		},
		{
			name:        "nil annotations",
			annotations: nil,
			expected:    MigDeviceType(""),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "test-pod",
					Namespace:   "default",
					Annotations: tt.annotations,
				},
			}
			result := GetMigTypeFromAnnotation(pod)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMigType2Mode(t *testing.T) {
	tests := []struct {
		name     string
		migType  MigDeviceType
		expected int
	}{
		{
			name:     "ci type returns 0",
			migType:  TypeMigCi,
			expected: 0,
		},
		{
			name:     "gi type returns 1",
			migType:  TypeMigGi,
			expected: 1,
		},
		{
			name:     "empty type returns 1",
			migType:  MigDeviceType(""),
			expected: 1,
		},
		{
			name:     "unknown type returns 1",
			migType:  MigDeviceType("unknown"),
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MigType2Mode(tt.migType)
			assert.Equal(t, tt.expected, result)
		})
	}
}
