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
	"strconv"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/stretchr/testify/assert"
)

func TestSortPodByAssumeTime_EmptyList(t *testing.T) {
	pods := []*v1.Pod{}
	sorted := SortPodByAssumeTime(pods)
	assert.Empty(t, sorted)
}

func TestSortPodByAssumeTime_SinglePod(t *testing.T) {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "single-pod",
			Annotations: map[string]string{
				AnnotationResourceAssumeTime: "1234567890",
			},
		},
	}

	pods := []*v1.Pod{pod}
	sorted := SortPodByAssumeTime(pods)

	assert.Len(t, sorted, 1)
	assert.Equal(t, "single-pod", sorted[0].Name)
}

func TestSortPodByAssumeTime_MultiplePods(t *testing.T) {
	now := time.Now().UnixNano()

	pod1 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-1",
			Annotations: map[string]string{
				AnnotationResourceAssumeTime: strconv.FormatInt(now, 10),
			},
		},
	}
	pod2 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-2",
			Annotations: map[string]string{
				AnnotationResourceAssumeTime: strconv.FormatInt(now+1000, 10),
			},
		},
	}
	pod3 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-3",
			// No annotation - should be treated as earliest (0)
		},
	}

	// Shuffle the order
	pods := []*v1.Pod{pod2, pod1, pod3}
	sorted := SortPodByAssumeTime(pods)

	// Expected order: pod3 (0), pod1 (now), pod2 (now+1000)
	assert.Len(t, sorted, 3)
	assert.Equal(t, "pod-3", sorted[0].Name)
	assert.Equal(t, "pod-1", sorted[1].Name)
	assert.Equal(t, "pod-2", sorted[2].Name)
}

func TestSortPodByAssumeTime_InvalidAssumeTime(t *testing.T) {
	pod1 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-invalid",
			Annotations: map[string]string{
				AnnotationResourceAssumeTime: "invalid-timestamp",
			},
		},
	}
	pod2 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-valid",
			Annotations: map[string]string{
				AnnotationResourceAssumeTime: "1234567890",
			},
		},
	}

	pods := []*v1.Pod{pod1, pod2}
	sorted := SortPodByAssumeTime(pods)

	// Both should be present, invalid timestamp treated as 0
	assert.Len(t, sorted, 2)
}

func TestSortPodByAssumeTime_SameAssumeTime(t *testing.T) {
	pod1 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-a",
			Annotations: map[string]string{
				AnnotationResourceAssumeTime: "1234567890",
			},
		},
	}
	pod2 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-b",
			Annotations: map[string]string{
				AnnotationResourceAssumeTime: "1234567890",
			},
		},
	}

	pods := []*v1.Pod{pod1, pod2}
	sorted := SortPodByAssumeTime(pods)

	// Both have same time, order may vary but both should be present
	assert.Len(t, sorted, 2)
}

func TestSetNodeName(t *testing.T) {
	// Save original
	original := nodeName
	defer func() { nodeName = original }()

	SetNodeName("test-node-123")
	assert.Equal(t, "test-node-123", nodeName)
}

func TestOrderedPodByAssumeTime_Swap(t *testing.T) {
	pod1 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-1",
			Annotations: map[string]string{
				AnnotationResourceAssumeTime: "100",
			},
		},
	}
	pod2 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-2",
			Annotations: map[string]string{
				AnnotationResourceAssumeTime: "200",
			},
		},
	}

	pods := orderedPodByAssumeTime{pod1, pod2}
	assert.Equal(t, "pod-1", pods[0].Name)
	assert.Equal(t, "pod-2", pods[1].Name)

	// Test Swap
	pods.Swap(0, 1)
	assert.Equal(t, "pod-2", pods[0].Name)
	assert.Equal(t, "pod-1", pods[1].Name)
}

func TestOrderedPodByAssumeTime_Len(t *testing.T) {
	pods := orderedPodByAssumeTime{
		{ObjectMeta: metav1.ObjectMeta{Name: "pod-1"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "pod-2"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "pod-3"}},
	}

	assert.Equal(t, 3, pods.Len())
}

func TestOrderedPodByAssumeTime_Less(t *testing.T) {
	pod1 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				AnnotationResourceAssumeTime: "100",
			},
		},
	}
	pod2 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				AnnotationResourceAssumeTime: "200",
			},
		},
	}

	pods := orderedPodByAssumeTime{pod1, pod2}
	assert.True(t, pods.Less(0, 1))
	assert.False(t, pods.Less(1, 0))
}
