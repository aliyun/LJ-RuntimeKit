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
	"context"
	"encoding/json"
	"fmt"
	"testing"

	ampcache "github.com/lj-runtimekit/scheduler/pkg/cache"
	"github.com/lj-runtimekit/scheduler/pkg/utils"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// ===========================================================================
// Test: Score method RDMA tiered scoring end-to-end
// ===========================================================================

// TestScore_RdmaTieredScoring verifies that the Score method returns correct
// tiered scores (100/75/25/0) for a Pod requesting RDMA when the node has
// rdma-topo annotation.
func TestScore_RdmaTieredScoring(t *testing.T) {
	const nodeName = "node-score-rdma"

	topo := buildRdmaTopo()
	topoJSON, err := json.Marshal(topo)
	if err != nil {
		t.Fatalf("failed to marshal rdmaTopo: %v", err)
	}

	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName,
			Annotations: map[string]string{
				utils.AnnotationRdmaTopo: string(topoJSON),
			},
		},
		Status: v1.NodeStatus{
			Allocatable: v1.ResourceList{
				v1.ResourceName(utils.ResourceCount):  resource.MustParse("8"),
				v1.ResourceName(utils.ResourceNvidia): resource.MustParse("8"),
				v1.ResourceName(utils.ResourceMemory): resource.MustParse("8192"),
				v1.ResourceName(utils.ResourceCore):   resource.MustParse("800"),
			},
		},
	}

	// Pod requesting GPU + RDMA
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "rdma-score-pod",
			Namespace: "default",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name: "main",
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(utils.ResourceNvidia): resource.MustParse("2"),
							v1.ResourceName(utils.ResourceRDMA):   resource.MustParse("1"),
						},
					},
				},
			},
		},
	}

	tests := []struct {
		name        string
		devices     map[int]*ampcache.DeviceInfo
		numCards    int
		expectScore int64
	}{
		{
			name:        "PCIe level (100): 2 idle in same switch",
			devices:     makeDevices([]int{0, 1}, []int{2, 3, 4, 5, 6, 7}),
			numCards:    2,
			expectScore: 100,
		},
		{
			name:        "NUMA level (75): need 3 across switches in same NUMA",
			devices:     makeDevices([]int{0, 1, 2, 3}, []int{4, 5, 6, 7}),
			numCards:    3,
			expectScore: 75,
		},
		{
			name:        "Cross-socket level (25): need 5 across sockets",
			devices:     allIdle(),
			numCards:    5,
			expectScore: 25,
		},
		{
			name:        "Unsatisfied (0): need 4 but only 2 idle",
			devices:     makeDevices([]int{0, 1}, []int{2, 3, 4, 5, 6, 7}),
			numCards:    4,
			expectScore: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Update pod resource to match numCards
			pod := pod.DeepCopy()
			pod.Spec.Containers[0].Resources.Limits[v1.ResourceName(utils.ResourceNvidia)] = resource.MustParse(
				fmt.Sprintf("%d", tt.numCards))

			nodeInfo := framework.NewNodeInfo()
			nodeInfo.SetNode(node)
			nodeInfos := map[string]*framework.NodeInfo{nodeName: nodeInfo}

			handle := newFakeHandle(nodeInfos, pod)
			plugin := &NormalStandardPlugin{fh: handle}

			state := framework.NewCycleState()
			clusterDevices := make(ampcache.AvailableClusterDevices)
			clusterDevices[nodeName] = tt.devices
			state.Write(ampcache.ClusterDevicesStateKey, clusterDevices)
			state.Write(ampcache.VStandardTaskReqStateKey, ampcache.StandardTaskReq{
				NormalGpuReqCnt: tt.numCards,
				VReplicas:       1,
			})
			state.Write(ampcache.ClusterMigDeviceStateKey, make(ampcache.ClusterNodeMigDevices))

			score, status := plugin.Score(context.Background(), state, pod, nodeName)
			assert.Nil(t, status.AsError(), "Score should not return error, got: %v", status)
			assert.Equal(t, tt.expectScore, score)
		})
	}
}
