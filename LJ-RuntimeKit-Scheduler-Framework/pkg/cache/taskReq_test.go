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

package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStandardTaskReq_Clone(t *testing.T) {
	original := StandardTaskReq{
		Vmem:            1024,
		Vcore:           4,
		Vslice:          2,
		VReplicas:       3,
		NormalGpuReqCnt: 1,
		AoaReplicas: map[string]int{
			"node-1": 2,
			"node-2": 1,
		},
	}

	clonedStateData := original.Clone()
	cloned, ok := clonedStateData.(*StandardTaskReq)
	assert.True(t, ok, "Clone() should return *StandardTaskReq")
	assert.NotNil(t, cloned)

	// Verify primitive fields
	assert.Equal(t, original.Vmem, cloned.Vmem)
	assert.Equal(t, original.Vcore, cloned.Vcore)
	assert.Equal(t, original.Vslice, cloned.Vslice)
	assert.Equal(t, original.VReplicas, cloned.VReplicas)
	assert.Equal(t, original.NormalGpuReqCnt, cloned.NormalGpuReqCnt)

	// Verify AoaReplicas map is deeply copied
	assert.Equal(t, original.AoaReplicas, cloned.AoaReplicas)

	// Ensure it's a deep copy: modifying clone should not affect original
	cloned.AoaReplicas["node-3"] = 5
	assert.NotContains(t, original.AoaReplicas, "node-3")
	assert.Contains(t, cloned.AoaReplicas, "node-3")

	// Edge case: empty AoaReplicas
	emptyReq := StandardTaskReq{
		AoaReplicas: nil,
	}
	emptyCloned := emptyReq.Clone().(*StandardTaskReq)
	assert.Empty(t, emptyCloned.AoaReplicas)
}
