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
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// newTestNode creates a v1.Node with allocatable GPU/count/memory/core resources
// and an optional topology annotation.
func newTestNode(name string, gpuCount int64, vmemPerGPU, vcorePerGPU int64, topoAnno string) *v1.Node {
	totalMem := resource.NewQuantity(gpuCount*vmemPerGPU, resource.DecimalSI)
	totalCore := resource.NewQuantity(gpuCount*vcorePerGPU, resource.DecimalSI)
	totalGpu := resource.NewQuantity(gpuCount, resource.DecimalSI)

	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Status: v1.NodeStatus{
			Allocatable: v1.ResourceList{
				v1.ResourceName(utils.ResourceCount):  *totalGpu,
				v1.ResourceName(utils.ResourceNvidia): *totalGpu,
				v1.ResourceName(utils.ResourceMemory): *totalMem,
				v1.ResourceName(utils.ResourceCore):   *totalCore,
			},
		},
	}
	if topoAnno != "" {
		node.Annotations = map[string]string{
			utils.AnnotationDevice: topoAnno,
		}
	}
	return node
}

// newTestNodeInfo wraps a node into a framework.NodeInfo.
func newTestNodeInfo(name string, gpuCount int64, vmemPerGPU, vcorePerGPU int64, topoAnno string) *framework.NodeInfo {
	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(newTestNode(name, gpuCount, vmemPerGPU, vcorePerGPU, topoAnno))
	return nodeInfo
}

// buildAvailableDevices calls ampcache.BuildCache and wraps it as AvailableDevices.
func buildAvailableDevices(name string, gpuCount int64, vmemPerGPU, vcorePerGPU int64, topoAnno string) map[int]*ampcache.DeviceInfo {
	return ampcache.BuildCache(newTestNodeInfo(name, gpuCount, vmemPerGPU, vcorePerGPU, topoAnno))
}

// topoAnnotation builds the JSON annotation for GpuDevice with P2P topology links.
// groups is a list where each element is a list of GPU minors that share a topology group.
// E.g. [[0,1],[2,3]] creates two 2-card groups.
func topoAnnotation(gpuCount int, groups [][]int, migIdx ...int) string {
	gd := ampcache.GpuDevice{
		Count:   gpuCount,
		Devices: make(map[int]*ampcache.Device),
	}

	migSet := make(map[int]bool)
	for _, idx := range migIdx {
		migSet[idx] = true
	}

	for i := 0; i < gpuCount; i++ {
		dev := &ampcache.Device{
			Minor:     uint(i),
			Uuid:      fmt.Sprintf("gpu-%d", i),
			Health:    "Healthy",
			Type:      "gpu",
			MigEnable: migSet[i],
		}
		// Add topology links for devices that belong to a group.
		for _, group := range groups {
			for _, minor := range group {
				if i == minor {
					peerStr := ""
					for j, m := range group {
						if j > 0 {
							peerStr += ","
						}
						peerStr += fmt.Sprintf("%d", m)
					}
					dev.Topology = append(dev.Topology, ampcache.DeviceP2PLink{
						PeerMinor: peerStr,
						Type:      "P2P",
					})
				}
			}
		}
		gd.Devices[i] = dev
	}

	b, _ := json.Marshal(gd)
	return string(b)
}

// emptyMigDevices returns a GpuDevice with no MIG devices.
func emptyMigDevices() *ampcache.GpuDevice {
	return &ampcache.GpuDevice{
		Devices: make(map[int]*ampcache.Device),
	}
}

// ===========================================================================
// Tests: New / Name
// ===========================================================================

func TestNew(t *testing.T) {
	plugin, err := New(&runtime.Unknown{}, nil)
	assert.NoError(t, err)
	assert.NotNil(t, plugin)
	assert.Equal(t, Name, plugin.Name())
}

func TestName(t *testing.T) {
	p := &NormalStandardPlugin{}
	assert.Equal(t, Name, p.Name())
}

// ===========================================================================
// Tests: findIdleTopologyGroups
// ===========================================================================

func TestFindIdleTopologyGroups_NoTopology(t *testing.T) {
	// Devices without topology info produce no groups.
	devices := buildAvailableDevices("node-1", 4, 1024, 200, "")
	assert.Empty(t, findIdleTopologyGroups(devices, emptyMigDevices(), 2))
}

func TestFindIdleTopologyGroups_TwoCard_AllIdle(t *testing.T) {
	anno := topoAnnotation(2, [][]int{{0, 1}})
	devices := buildAvailableDevices("node-1", 2, 1024, 200, anno)
	groups := findIdleTopologyGroups(devices, emptyMigDevices(), 2)
	assert.Len(t, groups, 1)
	assert.ElementsMatch(t, []int{0, 1}, groups[0])
}

func TestFindIdleTopologyGroups_TwoCard_OneUsed(t *testing.T) {
	anno := topoAnnotation(2, [][]int{{0, 1}})
	devices := buildAvailableDevices("node-1", 2, 1024, 200, anno)
	// Mark device 1 as used.
	devices[1].TrunkDevice()
	assert.Empty(t, findIdleTopologyGroups(devices, emptyMigDevices(), 2))
}

func TestFindIdleTopologyGroups_FourCard_TwoGroups(t *testing.T) {
	anno := topoAnnotation(4, [][]int{{0, 1}, {2, 3}})
	devices := buildAvailableDevices("node-1", 4, 1024, 200, anno)
	groups := findIdleTopologyGroups(devices, emptyMigDevices(), 2)
	assert.Len(t, groups, 2)
}

func TestFindIdleTopologyGroups_MigDeviceExcluded(t *testing.T) {
	// GPU 0 is MIG, so the {0,1} group should be excluded.
	anno := topoAnnotation(4, [][]int{{0, 1}, {2, 3}}, 0)
	devices := buildAvailableDevices("node-1", 4, 1024, 200, anno)

	migDevs := &ampcache.GpuDevice{
		Devices: map[int]*ampcache.Device{
			0: {Minor: 0, MigEnable: true},
		},
	}

	groups := findIdleTopologyGroups(devices, migDevs, 2)
	assert.Len(t, groups, 1) // only {2,3} remains
	assert.ElementsMatch(t, []int{2, 3}, groups[0])
}

func TestFindIdleTopologyGroups_FourCard_Topo(t *testing.T) {
	anno := topoAnnotation(8, [][]int{{0, 1, 2, 3}, {4, 5, 6, 7}})
	devices := buildAvailableDevices("node-1", 8, 1024, 200, anno)
	groups := findIdleTopologyGroups(devices, emptyMigDevices(), 4)
	assert.Len(t, groups, 2)
}

func TestFindIdleTopologyGroups_Dedup(t *testing.T) {
	// When two devices in the same group each report the same topology,
	// the group should be counted only once.
	anno := topoAnnotation(2, [][]int{{0, 1}})
	devices := buildAvailableDevices("node-1", 2, 1024, 200, anno)
	groups := findIdleTopologyGroups(devices, emptyMigDevices(), 2)
	assert.Len(t, groups, 1)
}

// ===========================================================================
// Tests: findResourceTopologyGroups
// ===========================================================================

func TestFindResourceTopologyGroups_EnoughResource(t *testing.T) {
	anno := topoAnnotation(2, [][]int{{0, 1}})
	devices := buildAvailableDevices("node-1", 2, 1024, 200, anno)
	// Request half of each device.
	groups := findResourceTopologyGroups(devices, emptyMigDevices(), 2, 512, 100)
	assert.Len(t, groups, 1)
	assert.ElementsMatch(t, []int{0, 1}, groups[0])
}

func TestFindResourceTopologyGroups_InsufficientMemory(t *testing.T) {
	anno := topoAnnotation(2, [][]int{{0, 1}})
	devices := buildAvailableDevices("node-1", 2, 1024, 200, anno)
	// Request more memory than each device has.
	groups := findResourceTopologyGroups(devices, emptyMigDevices(), 2, 2048, 100)
	assert.Empty(t, groups)
}

func TestFindResourceTopologyGroups_InsufficientCore(t *testing.T) {
	anno := topoAnnotation(2, [][]int{{0, 1}})
	devices := buildAvailableDevices("node-1", 2, 1024, 200, anno)
	// Request more core than each device has.
	groups := findResourceTopologyGroups(devices, emptyMigDevices(), 2, 512, 500)
	assert.Empty(t, groups)
}

func TestFindResourceTopologyGroups_MigExcluded(t *testing.T) {
	anno := topoAnnotation(2, [][]int{{0, 1}}, 1)
	devices := buildAvailableDevices("node-1", 2, 1024, 200, anno)
	migDevs := &ampcache.GpuDevice{
		Devices: map[int]*ampcache.Device{
			1: {Minor: 1, MigEnable: true},
		},
	}
	groups := findResourceTopologyGroups(devices, migDevs, 2, 512, 100)
	assert.Empty(t, groups)
}

func TestFindResourceTopologyGroups_PartiallyUsed(t *testing.T) {
	anno := topoAnnotation(3, [][]int{{0, 1, 2}})
	devices := buildAvailableDevices("node-1", 3, 1024, 200, anno)
	// Use half of device 1.
	devices[1].UsedResource(512, 100, 0)
	// Request still fits (device 1 has 512 mem, 100 core left).
	groups := findResourceTopologyGroups(devices, emptyMigDevices(), 3, 512, 100)
	assert.Len(t, groups, 1)
}

// ===========================================================================
// Tests: tryTopoReserve
// ===========================================================================

func TestTryTopoReserve_Success(t *testing.T) {
	anno := topoAnnotation(4, [][]int{{0, 1}, {2, 3}})
	devices := buildAvailableDevices("node-1", 4, 1024, 200, anno)
	selected, ok := tryTopoReserve(devices, emptyMigDevices(), 2, utils.ScheduleStrategyBinpack)
	assert.True(t, ok)
	assert.Len(t, selected, 2)
}

func TestTryTopoReserve_NoGroups(t *testing.T) {
	devices := buildAvailableDevices("node-1", 4, 1024, 200, "")
	selected, ok := tryTopoReserve(devices, emptyMigDevices(), 2, utils.ScheduleStrategyBinpack)
	assert.False(t, ok)
	assert.Nil(t, selected)
}

// ===========================================================================
// Tests: tryVirtualTopoReserve
// ===========================================================================

func TestTryVirtualTopoReserve_Binpack(t *testing.T) {
	// Two groups: {0,1} with more idle resources, {2,3} with less.
	// Binpack should pick the group with LOWEST ratio (most packed).
	anno := topoAnnotation(4, [][]int{{0, 1}, {2, 3}})
	devices := buildAvailableDevices("node-1", 4, 1024, 200, anno)
	// Group {2,3}: use 800 mem on device 2, 800 mem on device 3 → lower idle ratio.
	devices[2].UsedResource(800, 0, 0)
	devices[3].UsedResource(800, 0, 0)

	selected, ok := tryVirtualTopoReserve(devices, emptyMigDevices(), 2, 100, 100, utils.ScheduleStrategyBinpack)
	assert.True(t, ok)
	// Binpack picks the group with lower idle ratio → {2,3}
	assert.ElementsMatch(t, []int{2, 3}, selected)
}

func TestTryVirtualTopoReserve_Spread(t *testing.T) {
	anno := topoAnnotation(4, [][]int{{0, 1}, {2, 3}})
	devices := buildAvailableDevices("node-1", 4, 1024, 200, anno)
	// Group {2,3}: use 800 mem on each → higher utilization.
	devices[2].UsedResource(800, 0, 0)
	devices[3].UsedResource(800, 0, 0)

	selected, ok := tryVirtualTopoReserve(devices, emptyMigDevices(), 2, 100, 100, utils.ScheduleStrategySpread)
	assert.True(t, ok)
	// Spread picks the group with higher idle ratio → {0,1}
	assert.ElementsMatch(t, []int{0, 1}, selected)
}

func TestTryVirtualTopoReserve_NoGroups(t *testing.T) {
	devices := buildAvailableDevices("node-1", 4, 1024, 200, "")
	selected, ok := tryVirtualTopoReserve(devices, emptyMigDevices(), 2, 100, 100, utils.ScheduleStrategyBinpack)
	assert.False(t, ok)
	assert.Nil(t, selected)
}

// ===========================================================================
// Tests: Filter
// ===========================================================================

func TestFilter_WholeCard_EnoughIdle(t *testing.T) {
	state := framework.NewCycleState()
	req := ampcache.StandardTaskReq{NormalGpuReqCnt: 2, VReplicas: 1}
	devices := make(ampcache.AvailableClusterDevices)
	devices["node-1"] = buildAvailableDevices("node-1", 4, 1024, 200, "")
	state.Write(ampcache.ClusterDevicesStateKey, devices)
	state.Write(ampcache.VStandardTaskReqStateKey, req)
	state.Write(ampcache.ClusterMigDeviceStateKey, make(ampcache.ClusterNodeMigDevices))

	nodeInfo := newTestNodeInfo("node-1", 4, 1024, 200, "")
	plugin := &NormalStandardPlugin{}

	status := plugin.Filter(context.Background(), state, &v1.Pod{}, nodeInfo)
	assert.True(t, status.IsSuccess())
}

func TestFilter_WholeCard_InsufficientIdle(t *testing.T) {
	state := framework.NewCycleState()
	req := ampcache.StandardTaskReq{NormalGpuReqCnt: 3, VReplicas: 1}
	devices := make(ampcache.AvailableClusterDevices)
	devices["node-1"] = buildAvailableDevices("node-1", 4, 1024, 200, "")
	// Mark 2 devices as fully used → only 2 left idle.
	devices["node-1"][0].TrunkDevice()
	devices["node-1"][1].TrunkDevice()
	state.Write(ampcache.ClusterDevicesStateKey, devices)
	state.Write(ampcache.VStandardTaskReqStateKey, req)
	state.Write(ampcache.ClusterMigDeviceStateKey, make(ampcache.ClusterNodeMigDevices))

	nodeInfo := newTestNodeInfo("node-1", 4, 1024, 200, "")
	plugin := &NormalStandardPlugin{}

	status := plugin.Filter(context.Background(), state, &v1.Pod{}, nodeInfo)
	assert.Equal(t, framework.Unschedulable, status.Code())
}

func TestFilter_WholeCard_MigExcluded(t *testing.T) {
	state := framework.NewCycleState()
	req := ampcache.StandardTaskReq{NormalGpuReqCnt: 1, VReplicas: 1}
	devices := make(ampcache.AvailableClusterDevices)
	devices["node-1"] = buildAvailableDevices("node-1", 2, 1024, 200, "")

	migDevices := ampcache.ClusterNodeMigDevices{
		"node-1": &ampcache.GpuDevice{
			Devices: map[int]*ampcache.Device{
				0: {Minor: 0, MigEnable: true},
			},
		},
	}
	state.Write(ampcache.ClusterDevicesStateKey, devices)
	state.Write(ampcache.VStandardTaskReqStateKey, req)
	state.Write(ampcache.ClusterMigDeviceStateKey, migDevices)

	nodeInfo := newTestNodeInfo("node-1", 2, 1024, 200, "")
	plugin := &NormalStandardPlugin{}

	status := plugin.Filter(context.Background(), state, &v1.Pod{}, nodeInfo)
	assert.True(t, status.IsSuccess()) // device 1 is idle and non-MIG
}

func TestFilter_VirtualCard_EnoughReplicas(t *testing.T) {
	state := framework.NewCycleState()
	req := ampcache.StandardTaskReq{Vmem: 512, Vcore: 100, VReplicas: 2}
	devices := make(ampcache.AvailableClusterDevices)
	devices["node-1"] = buildAvailableDevices("node-1", 4, 1024, 200, "")
	state.Write(ampcache.ClusterDevicesStateKey, devices)
	state.Write(ampcache.VStandardTaskReqStateKey, req)
	state.Write(ampcache.ClusterMigDeviceStateKey, make(ampcache.ClusterNodeMigDevices))

	nodeInfo := newTestNodeInfo("node-1", 4, 1024, 200, "")
	plugin := &NormalStandardPlugin{}

	status := plugin.Filter(context.Background(), state, &v1.Pod{}, nodeInfo)
	assert.True(t, status.IsSuccess())
}

func TestFilter_VirtualCard_InsufficientReplicas(t *testing.T) {
	state := framework.NewCycleState()
	req := ampcache.StandardTaskReq{Vmem: 2048, Vcore: 100, VReplicas: 2}
	devices := make(ampcache.AvailableClusterDevices)
	devices["node-1"] = buildAvailableDevices("node-1", 4, 1024, 200, "")
	state.Write(ampcache.ClusterDevicesStateKey, devices)
	state.Write(ampcache.VStandardTaskReqStateKey, req)
	state.Write(ampcache.ClusterMigDeviceStateKey, make(ampcache.ClusterNodeMigDevices))

	nodeInfo := newTestNodeInfo("node-1", 4, 1024, 200, "")
	plugin := &NormalStandardPlugin{}

	status := plugin.Filter(context.Background(), state, &v1.Pod{}, nodeInfo)
	assert.Equal(t, framework.Unschedulable, status.Code())
}

func TestFilter_ZeroRequest_CPUPod(t *testing.T) {
	state := framework.NewCycleState()
	req := ampcache.StandardTaskReq{Vmem: 0, Vcore: 0, Vslice: 0, NormalGpuReqCnt: 0, VReplicas: 1}
	devices := make(ampcache.AvailableClusterDevices)
	devices["node-1"] = buildAvailableDevices("node-1", 1, 1024, 200, "")
	state.Write(ampcache.ClusterDevicesStateKey, devices)
	state.Write(ampcache.VStandardTaskReqStateKey, req)
	state.Write(ampcache.ClusterMigDeviceStateKey, make(ampcache.ClusterNodeMigDevices))

	nodeInfo := newTestNodeInfo("node-1", 1, 1024, 200, "")
	plugin := &NormalStandardPlugin{}

	status := plugin.Filter(context.Background(), state, &v1.Pod{}, nodeInfo)
	assert.True(t, status.IsSuccess())
}

func TestFilter_MigSlice_PassesThrough(t *testing.T) {
	state := framework.NewCycleState()
	req := ampcache.StandardTaskReq{Vslice: 1, Vmem: 100, Vcore: 100, VReplicas: 1}
	devices := make(ampcache.AvailableClusterDevices)
	devices["node-1"] = buildAvailableDevices("node-1", 4, 1024, 200, "")
	state.Write(ampcache.ClusterDevicesStateKey, devices)
	state.Write(ampcache.VStandardTaskReqStateKey, req)
	state.Write(ampcache.ClusterMigDeviceStateKey, make(ampcache.ClusterNodeMigDevices))

	nodeInfo := newTestNodeInfo("node-1", 4, 1024, 200, "")
	plugin := &NormalStandardPlugin{}

	status := plugin.Filter(context.Background(), state, &v1.Pod{}, nodeInfo)
	assert.True(t, status.IsSuccess())
}

func TestFilter_NodeNotFound_InCluster(t *testing.T) {
	state := framework.NewCycleState()
	req := ampcache.StandardTaskReq{NormalGpuReqCnt: 1, VReplicas: 1}
	devices := make(ampcache.AvailableClusterDevices)
	devices["node-1"] = buildAvailableDevices("node-1", 2, 1024, 200, "")
	state.Write(ampcache.ClusterDevicesStateKey, devices)
	state.Write(ampcache.VStandardTaskReqStateKey, req)
	state.Write(ampcache.ClusterMigDeviceStateKey, make(ampcache.ClusterNodeMigDevices))

	// Node name not in cluster devices map.
	nodeInfo := newTestNodeInfo("node-2", 2, 1024, 200, "")
	plugin := &NormalStandardPlugin{}

	status := plugin.Filter(context.Background(), state, &v1.Pod{}, nodeInfo)
	assert.Equal(t, framework.Error, status.Code())
}

// ===========================================================================
// Tests: Score
// ===========================================================================

func TestScore_WholeCard_TwoGPU_HasTopology(t *testing.T) {
	anno := topoAnnotation(4, [][]int{{0, 1}, {2, 3}})
	state := framework.NewCycleState()
	req := ampcache.StandardTaskReq{NormalGpuReqCnt: 2, VReplicas: 1}
	devices := make(ampcache.AvailableClusterDevices)
	devices["node-1"] = buildAvailableDevices("node-1", 4, 1024, 200, anno)
	state.Write(ampcache.ClusterDevicesStateKey, devices)
	state.Write(ampcache.VStandardTaskReqStateKey, req)
	state.Write(ampcache.ClusterMigDeviceStateKey, make(ampcache.ClusterNodeMigDevices))

	plugin := &NormalStandardPlugin{}
	score, status := plugin.Score(context.Background(), state, &v1.Pod{}, "node-1")
	assert.True(t, status.IsSuccess())
	assert.Equal(t, framework.MaxNodeScore, score)
}

func TestScore_WholeCard_TwoGPU_NoTopology(t *testing.T) {
	state := framework.NewCycleState()
	req := ampcache.StandardTaskReq{NormalGpuReqCnt: 2, VReplicas: 1}
	devices := make(ampcache.AvailableClusterDevices)
	devices["node-1"] = buildAvailableDevices("node-1", 4, 1024, 200, "")
	state.Write(ampcache.ClusterDevicesStateKey, devices)
	state.Write(ampcache.VStandardTaskReqStateKey, req)
	state.Write(ampcache.ClusterMigDeviceStateKey, make(ampcache.ClusterNodeMigDevices))

	plugin := &NormalStandardPlugin{}
	score, status := plugin.Score(context.Background(), state, &v1.Pod{}, "node-1")
	assert.True(t, status.IsSuccess())
	assert.Equal(t, int64(0), score)
}

func TestScore_WholeCard_NonTopoRequest(t *testing.T) {
	// Requests with 1 or 3 cards don't benefit from topology scoring.
	anno := topoAnnotation(4, [][]int{{0, 1}, {2, 3}})
	state := framework.NewCycleState()
	req := ampcache.StandardTaskReq{NormalGpuReqCnt: 3, VReplicas: 1}
	devices := make(ampcache.AvailableClusterDevices)
	devices["node-1"] = buildAvailableDevices("node-1", 4, 1024, 200, anno)
	state.Write(ampcache.ClusterDevicesStateKey, devices)
	state.Write(ampcache.VStandardTaskReqStateKey, req)
	state.Write(ampcache.ClusterMigDeviceStateKey, make(ampcache.ClusterNodeMigDevices))

	plugin := &NormalStandardPlugin{}
	score, status := plugin.Score(context.Background(), state, &v1.Pod{}, "node-1")
	assert.True(t, status.IsSuccess())
	assert.Equal(t, int64(0), score)
}

func TestScore_VirtualCard_TwoReplicas_HasTopology(t *testing.T) {
	anno := topoAnnotation(4, [][]int{{0, 1}, {2, 3}})
	state := framework.NewCycleState()
	req := ampcache.StandardTaskReq{Vmem: 512, Vcore: 100, VReplicas: 2}
	devices := make(ampcache.AvailableClusterDevices)
	devices["node-1"] = buildAvailableDevices("node-1", 4, 1024, 200, anno)
	state.Write(ampcache.ClusterDevicesStateKey, devices)
	state.Write(ampcache.VStandardTaskReqStateKey, req)
	state.Write(ampcache.ClusterMigDeviceStateKey, make(ampcache.ClusterNodeMigDevices))

	plugin := &NormalStandardPlugin{}
	score, status := plugin.Score(context.Background(), state, &v1.Pod{}, "node-1")
	assert.True(t, status.IsSuccess())
	assert.Equal(t, framework.MaxNodeScore, score)
}

func TestScore_VirtualCard_TwoReplicas_NoTopology(t *testing.T) {
	state := framework.NewCycleState()
	req := ampcache.StandardTaskReq{Vmem: 512, Vcore: 100, VReplicas: 2}
	devices := make(ampcache.AvailableClusterDevices)
	devices["node-1"] = buildAvailableDevices("node-1", 4, 1024, 200, "")
	state.Write(ampcache.ClusterDevicesStateKey, devices)
	state.Write(ampcache.VStandardTaskReqStateKey, req)
	state.Write(ampcache.ClusterMigDeviceStateKey, make(ampcache.ClusterNodeMigDevices))

	plugin := &NormalStandardPlugin{}
	score, status := plugin.Score(context.Background(), state, &v1.Pod{}, "node-1")
	assert.True(t, status.IsSuccess())
	assert.Equal(t, int64(0), score)
}

func TestScore_VirtualCard_NonTopoReplicas(t *testing.T) {
	// Non 2/4/8 replicas: no topology scoring.
	anno := topoAnnotation(4, [][]int{{0, 1}, {2, 3}})
	state := framework.NewCycleState()
	req := ampcache.StandardTaskReq{Vmem: 512, Vcore: 100, VReplicas: 3}
	devices := make(ampcache.AvailableClusterDevices)
	devices["node-1"] = buildAvailableDevices("node-1", 4, 1024, 200, anno)
	state.Write(ampcache.ClusterDevicesStateKey, devices)
	state.Write(ampcache.VStandardTaskReqStateKey, req)
	state.Write(ampcache.ClusterMigDeviceStateKey, make(ampcache.ClusterNodeMigDevices))

	plugin := &NormalStandardPlugin{}
	score, status := plugin.Score(context.Background(), state, &v1.Pod{}, "node-1")
	assert.True(t, status.IsSuccess())
	assert.Equal(t, int64(0), score)
}

func TestScore_NodeNotFound(t *testing.T) {
	state := framework.NewCycleState()
	req := ampcache.StandardTaskReq{NormalGpuReqCnt: 2, VReplicas: 1}
	devices := make(ampcache.AvailableClusterDevices)
	devices["node-1"] = buildAvailableDevices("node-1", 4, 1024, 200, "")
	state.Write(ampcache.ClusterDevicesStateKey, devices)
	state.Write(ampcache.VStandardTaskReqStateKey, req)
	state.Write(ampcache.ClusterMigDeviceStateKey, make(ampcache.ClusterNodeMigDevices))

	plugin := &NormalStandardPlugin{}
	score, status := plugin.Score(context.Background(), state, &v1.Pod{}, "node-unknown")
	assert.True(t, status.IsSuccess())
	assert.Equal(t, int64(0), score)
}

func TestScore_FourCard_WholeGPU(t *testing.T) {
	anno := topoAnnotation(8, [][]int{{0, 1, 2, 3}, {4, 5, 6, 7}})
	state := framework.NewCycleState()
	req := ampcache.StandardTaskReq{NormalGpuReqCnt: 4, VReplicas: 1}
	devices := make(ampcache.AvailableClusterDevices)
	devices["node-1"] = buildAvailableDevices("node-1", 8, 1024, 200, anno)
	state.Write(ampcache.ClusterDevicesStateKey, devices)
	state.Write(ampcache.VStandardTaskReqStateKey, req)
	state.Write(ampcache.ClusterMigDeviceStateKey, make(ampcache.ClusterNodeMigDevices))

	plugin := &NormalStandardPlugin{}
	score, status := plugin.Score(context.Background(), state, &v1.Pod{}, "node-1")
	assert.True(t, status.IsSuccess())
	assert.Equal(t, framework.MaxNodeScore, score)
}

// ===========================================================================
// Tests: PreFilter — NormalGpuReqCnt cascade derivation
// ===========================================================================

// newTestPodWithResources builds a pod whose single container requests the given
// resources. GetResourceReqFromPod reads from container Limits, so both Limits and
// Requests are populated to mirror a realistic pod spec.
func newTestPodWithResources(requests map[string]int64) *v1.Pod {
	rl := v1.ResourceList{}
	for name, qty := range requests {
		rl[v1.ResourceName(name)] = *resource.NewQuantity(qty, resource.DecimalSI)
	}
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name: "c0",
					Resources: v1.ResourceRequirements{
						Limits:   rl,
						Requests: rl,
					},
				},
			},
		},
	}
}

// TestPreFilter_NormalGpuReqCnt exercises the full PreFilter path that derives
// NormalGpuReqCnt from a pod's resource requests and writes it to the CycleState.
// It covers the cascade: ResourceGPU -> ResourceNvidia -> ResourceCount, the vcore
// override (vcore/100 when vcore >= 100), and the invalid vcore error branch.
func TestPreFilter_NormalGpuReqCnt(t *testing.T) {
	tests := []struct {
		name     string
		requests map[string]int64
		wantCnt  int
		wantErr  bool
	}{
		{
			name:     "only ResourceGPU",
			requests: map[string]int64{utils.ResourceGPU: 2},
			wantCnt:  2,
		},
		{
			name:     "only ResourceNvidia",
			requests: map[string]int64{utils.ResourceNvidia: 3},
			wantCnt:  3,
		},
		{
			name:     "only ResourceCount",
			requests: map[string]int64{utils.ResourceCount: 4},
			wantCnt:  4,
		},
		{
			name:     "vcore overrides when no gpu resource",
			requests: map[string]int64{utils.ResourceCore: 200},
			wantCnt:  2,
		},
		{
			name:     "vcore overrides gpu value",
			requests: map[string]int64{utils.ResourceGPU: 1, utils.ResourceCore: 300},
			wantCnt:  3,
		},
		{
			name:     "invalid vcore not multiple of 100",
			requests: map[string]int64{utils.ResourceCore: 150},
			wantErr:  true,
		},
		{
			name:     "no gpu related resource",
			requests: map[string]int64{},
			wantCnt:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := newTestPodWithResources(tt.requests)
			nodeInfos := map[string]*framework.NodeInfo{
				"node-1": newTestNodeInfo("node-1", 8, 1024, 200, ""),
			}
			plugin := &NormalStandardPlugin{fh: newFakeHandle(nodeInfos)}
			state := framework.NewCycleState()

			status := plugin.PreFilter(context.Background(), state, pod)

			if tt.wantErr {
				assert.NotNil(t, status)
				assert.Equal(t, framework.Error, status.Code())
				return
			}

			assert.True(t, status.IsSuccess())
			_, _, req, err := ampcache.GetPreFilterStateData(state)
			assert.NoError(t, err)
			assert.Equal(t, tt.wantCnt, req.NormalGpuReqCnt)
		})
	}
}
