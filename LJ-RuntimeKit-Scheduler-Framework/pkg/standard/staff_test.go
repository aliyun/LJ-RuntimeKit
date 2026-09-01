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
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStaffGPU_GetResourceRatio(t *testing.T) {
	tests := []struct {
		name      string
		staff     staffGPU
		wantRatio int64
	}{
		{
			name: "normal case with both resources",
			staff: staffGPU{
				devId:     0,
				total:     &gpuResources{memory: 100, compute: 100},
				available: &gpuResources{memory: 50, compute: 50},
			},
			wantRatio: 50, // (50*50/100) + (50*50/100) = 25 + 25 = 50
		},
		{
			name: "only memory matters when compute is zero",
			staff: staffGPU{
				devId:     0,
				total:     &gpuResources{memory: 200, compute: 0},
				available: &gpuResources{memory: 100, compute: 0},
			},
			wantRatio: 50, // 100 * 100 / 200 = 50
		},
		{
			name: "full utilization",
			staff: staffGPU{
				devId:     0,
				total:     &gpuResources{memory: 100, compute: 100},
				available: &gpuResources{memory: 100, compute: 100},
			},
			wantRatio: 100,
		},
		{
			name: "zero total memory (edge case - avoid panic)",
			staff: staffGPU{
				devId:     0,
				total:     &gpuResources{memory: 0, compute: 100},
				available: &gpuResources{memory: 0, compute: 50},
			},
			wantRatio: 0, // In practice, this should not happen, but test behavior
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.staff.getResourceRatio()
			assert.Equal(t, tt.wantRatio, got)
		})
	}
}

func TestResourceList_Sort(t *testing.T) {
	list := resourceList{
		{devId: 0, total: &gpuResources{100, 100}, available: &gpuResources{80, 80}}, // ratio = 80
		{devId: 1, total: &gpuResources{100, 100}, available: &gpuResources{20, 20}}, // ratio = 20
		{devId: 2, total: &gpuResources{100, 100}, available: &gpuResources{50, 50}}, // ratio = 50
	}

	// Sort in ascending order of resource ratio (most free first)
	sort.Sort(list)

	expectedOrder := []int{1, 2, 0} // ratios: 20, 50, 80
	for i, expectedID := range expectedOrder {
		assert.Equal(t, expectedID, list[i].devId)
	}
}

func TestReverseResourceList_Sort(t *testing.T) {
	list := reverseResourceList{
		{devId: 0, total: &gpuResources{100, 100}, available: &gpuResources{80, 80}}, // ratio = 80
		{devId: 1, total: &gpuResources{100, 100}, available: &gpuResources{20, 20}}, // ratio = 20
		{devId: 2, total: &gpuResources{100, 100}, available: &gpuResources{50, 50}}, // ratio = 50
	}

	// Sort in descending order of resource ratio (least free first)
	sort.Sort(list)

	expectedOrder := []int{0, 2, 1} // ratios: 80, 50, 20
	for i, expectedID := range expectedOrder {
		assert.Equal(t, expectedID, list[i].devId)
	}
}
