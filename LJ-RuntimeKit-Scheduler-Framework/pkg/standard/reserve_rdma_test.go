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
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	fakeclientset "k8s.io/client-go/kubernetes/fake"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// ===========================================================================
// Fake framework.Handle (minimal implementation for Reserve tests)
// ===========================================================================

// fakeHandle implements framework.Handle for testing.
// Only ClientSet() and SnapshotSharedLister() are implemented;
// other methods will panic if called (they are not exercised in Reserve).
type fakeHandle struct {
	framework.Handle // embedded nil interface satisfies the type; overridden below
	client     clientset.Interface
	lister     framework.SharedLister
}

func (f *fakeHandle) ClientSet() clientset.Interface        { return f.client }
func (f *fakeHandle) SnapshotSharedLister() framework.SharedLister { return f.lister }

// fakeSharedLister implements framework.SharedLister.
type fakeSharedLister struct {
	nodeInfoLister framework.NodeInfoLister
}

func (f *fakeSharedLister) NodeInfos() framework.NodeInfoLister           { return f.nodeInfoLister }

// fakeNodeInfoLister implements framework.NodeInfoLister.
type fakeNodeInfoLister struct {
	nodes map[string]*framework.NodeInfo
}

func (f *fakeNodeInfoLister) List() ([]*framework.NodeInfo, error) {
	result := make([]*framework.NodeInfo, 0, len(f.nodes))
	for _, ni := range f.nodes {
		result = append(result, ni)
	}
	return result, nil
}
func (f *fakeNodeInfoLister) HavePodsWithAffinityList() ([]*framework.NodeInfo, error) { return nil, nil }
func (f *fakeNodeInfoLister) HavePodsWithRequiredAntiAffinityList() ([]*framework.NodeInfo, error) {
	return nil, nil
}
func (f *fakeNodeInfoLister) Get(nodeName string) (*framework.NodeInfo, error) {
	ni, ok := f.nodes[nodeName]
	if !ok {
		return nil, fmt.Errorf("node %q not found", nodeName)
	}
	return ni, nil
}

// newFakeHandle builds a minimal fakeHandle for Reserve tests.
func newFakeHandle(nodeInfos map[string]*framework.NodeInfo, objects ...runtime.Object) *fakeHandle {
	return &fakeHandle{
		client: fakeclientset.NewSimpleClientset(objects...),
		lister: &fakeSharedLister{
			nodeInfoLister: &fakeNodeInfoLister{nodes: nodeInfos},
		},
	}
}

// ===========================================================================
// Test: Reserve RDMA fallback when topology group is unsatisfied
// ===========================================================================

// TestReserve_RdmaFallbackWhenTopoGroupUnsatisfied verifies the end-to-end Reserve path
// for the RDMA degradation scenario:
//
// Scenario:
//   - Pod requests 4 whole GPUs + aliyun.com/rdma
//   - Node has rdma-topo annotation with a topology tree covering only GPUs 0-3
//   - Only GPUs 0 and 1 (within the topo tree) are idle → tryRdmaTopoReserve fails
//     (needs 4, only 2 idle within topo)
//   - GPUs 4,5 also exist in availableDevices and are idle (not in topo tree)
//   - Fallback selects from ALL idle GPUs (0,1,4,5) → exactly 4, all assigned
//   - The RDMA fallback derives pcie switch indexes for the assigned GPUs;
//     only in-tree GPUs 0,1 map to switch 0
//
// Expected:
//   - Reserve returns Success
//   - Pod has AnnotationPcieSwitchIndex == "0"
func TestReserve_RdmaFallbackWhenTopoGroupUnsatisfied(t *testing.T) {
	const nodeName = "node-rdma-fallback"
	const normalGpuReqCnt = 4

	// --- Build a partial rdmaTopo that covers only GPUs 0-3 ---
	// This means tryRdmaTopoReserve only considers GPUs 0-3 for topo group selection.
	// GPUs 4-7 are outside the topo tree, so the fallback derives switch indexes only
	// from the in-tree assigned GPUs (0,1 → switch 0).
	rdmaTopo := &RdmaTopoAnnotation{
		CPUSockets: []RdmaCPUSocket{
			{
				Index: 0,
				NUMANodes: []RdmaNUMANode{
					{
						Index: 0,
						PCIeSwitches: []RdmaPCIeSwitch{
							{
								Index: 0,
								GPUs:  []int{0, 1},
							},
							{
								Index: 1,
								GPUs:  []int{2, 3},
							},
						},
					},
				},
			},
		},
	}
	rdmaTopoJSON, err := json.Marshal(rdmaTopo)
	if err != nil {
		t.Fatalf("failed to marshal rdmaTopo: %v", err)
	}

	// --- Build Node ---
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName,
			Annotations: map[string]string{
				utils.AnnotationRdmaTopo: string(rdmaTopoJSON),
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

	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(node)
	nodeInfos := map[string]*framework.NodeInfo{nodeName: nodeInfo}

	// --- Build availableDevices ---
	// GPUs 0, 1: idle (within topo tree, switch 0) → only 2 idle in topo → tryRdmaTopoReserve fails for 4
	// GPUs 2, 3: busy (within topo tree)
	// GPUs 4, 5: idle (NOT in topo tree, but in availableDevices)
	// GPUs 6, 7: busy
	// Total idle for fallback: 0, 1, 4, 5 = 4 == normalGpuReqCnt → all assigned deterministically
	devices := makeDevices([]int{0, 1, 4, 5}, []int{2, 3, 6, 7})

	// --- Build Pod requesting GPU + RDMA ---
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "rdma-fallback-pod",
			Namespace: "default",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name: "main",
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(utils.ResourceNvidia): resource.MustParse("4"),
							v1.ResourceName(utils.ResourceRDMA):   resource.MustParse("1"),
						},
					},
				},
			},
		},
	}

	// --- Setup CycleState ---
	state := framework.NewCycleState()
	clusterDevices := make(ampcache.AvailableClusterDevices)
	clusterDevices[nodeName] = devices
	state.Write(ampcache.ClusterDevicesStateKey, clusterDevices)
	state.Write(ampcache.VStandardTaskReqStateKey, ampcache.StandardTaskReq{
		NormalGpuReqCnt: normalGpuReqCnt,
		VReplicas:       1,
	})
	state.Write(ampcache.ClusterMigDeviceStateKey, make(ampcache.ClusterNodeMigDevices))

	// --- Setup fake Handle ---
	handle := newFakeHandle(nodeInfos, pod)

	// --- Create plugin and call Reserve ---
	plugin := &NormalStandardPlugin{fh: handle}
	status := plugin.Reserve(context.Background(), state, pod, nodeName)

	// --- Assertions ---
	assert.True(t, status.IsSuccess(), "Reserve should succeed, got: %v", status.Message())

	// Verify RDMA annotations were written
	// The fake clientset's Patch will update the object in its store.
	// Retrieve the patched pod from the fake clientset.
	patchedPod, err := handle.client.CoreV1().Pods("default").Get(
		context.Background(), "rdma-fallback-pod", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get patched pod: %v", err)
	}

	// Check AnnotationPcieSwitchIndex exists and equals the switch index derived
	// from the assigned GPUs. Fallback assigns all 4 idle GPUs (0,1,4,5); only
	// in-tree GPUs 0,1 belong to switch 0, so the derived value is "0".
	pcieSwitchIndex, ok := patchedPod.Annotations[utils.AnnotationPcieSwitchIndex]
	assert.True(t, ok, "AnnotationPcieSwitchIndex annotation should be present")
	assert.Equal(t, "0", pcieSwitchIndex)

	t.Logf("RDMA fallback succeeded: pcie switch index %q", pcieSwitchIndex)
}
