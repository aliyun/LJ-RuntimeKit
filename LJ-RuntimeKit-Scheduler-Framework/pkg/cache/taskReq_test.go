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
	"k8s.io/kubernetes/pkg/scheduler/framework"
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
	cloned, ok := clonedStateData.(StandardTaskReq)
	assert.True(t, ok, "Clone() must return the value type StandardTaskReq, matching what PreFilter writes and GetPreFilterStateData asserts on")

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
	emptyCloned := emptyReq.Clone().(StandardTaskReq)
	assert.Empty(t, emptyCloned.AoaReplicas)
}

// TestStandardTaskReq_CloneSurvivesCycleStateClone guards the preemption path.
// DefaultPreemption clones the CycleState once per candidate node and every entry
// goes through StateData.Clone() before Filter is re-run; GetPreFilterStateData
// must still read the request back out of that cloned state. When Clone() returned
// a pointer this failed with "can not convert to standardTaskReq", aborting all
// preemption so business Pods could never evict ActiveMonitoring detection Pods.
func TestStandardTaskReq_CloneSurvivesCycleStateClone(t *testing.T) {
	// Mirrors a whole-card business Pod request as parsed by PreFilter.
	req := StandardTaskReq{VReplicas: 1, NormalGpuReqCnt: 1}

	state := framework.NewCycleState()
	state.Write(VStandardTaskReqStateKey, req)
	state.Write(ClusterDevicesStateKey, make(AvailableClusterDevices))
	state.Write(ClusterMigDeviceStateKey, make(ClusterNodeMigDevices))

	// Exactly what preemption.go does before SelectVictimsOnNode.
	stateCopy := state.Clone()

	clusterDevices, migDevices, clonedReq, err := GetPreFilterStateData(stateCopy)
	// Bail out instead of dereferencing a nil request, so a regression surfaces the
	// real conversion error rather than a nil-pointer panic.
	if !assert.NoError(t, err, "GetPreFilterStateData must succeed on a cloned CycleState") {
		return
	}
	if !assert.NotNil(t, clonedReq) {
		return
	}
	assert.Equal(t, req.VReplicas, clonedReq.VReplicas)
	assert.Equal(t, req.NormalGpuReqCnt, clonedReq.NormalGpuReqCnt)
	assert.NotNil(t, clusterDevices)
	assert.NotNil(t, migDevices)

	// The un-cloned state must keep working as well.
	_, _, origReq, err := GetPreFilterStateData(state)
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, req.NormalGpuReqCnt, origReq.NormalGpuReqCnt)
}
