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
	"strconv"
	"strings"
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

// slicesPerGPU is the slice capacity exported per card by the fake nodes below.
const slicesPerGPU = int64(7)

// newTestNode creates a v1.Node with allocatable GPU/count/memory/core/slice
// resources and an optional topology annotation.
func newTestNode(name string, gpuCount int64, vmemPerGPU, vcorePerGPU int64, topoAnno string) *v1.Node {
	totalMem := resource.NewQuantity(gpuCount*vmemPerGPU, resource.DecimalSI)
	totalCore := resource.NewQuantity(gpuCount*vcorePerGPU, resource.DecimalSI)
	totalSlice := resource.NewQuantity(gpuCount*slicesPerGPU, resource.DecimalSI)
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
				v1.ResourceName(utils.ResourceSlice):  *totalSlice,
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

// newTestNodeInfoWithPods wraps a node and running pods into a framework.NodeInfo.
func newTestNodeInfoWithPods(node *v1.Node, pods ...*v1.Pod) *framework.NodeInfo {
	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(node)
	for _, pod := range pods {
		nodeInfo.AddPod(pod)
	}
	return nodeInfo
}

// newRunningPod creates a running pod assigned to the given device ids.
// A pod with slice > 0 is an MPS task (no MIG label), otherwise it is a
// time-sharing task driven by gpu-core and gpu-memory.
func newRunningPod(name string, deviceIdx []int, core, mem, slice int64) *v1.Pod {
	ids := make([]string, len(deviceIdx))
	for i, idx := range deviceIdx {
		ids[i] = strconv.Itoa(idx)
	}
	limits := v1.ResourceList{
		v1.ResourceName(utils.ResourceMemory): *resource.NewQuantity(mem, resource.DecimalSI),
	}
	if slice > 0 {
		limits[v1.ResourceName(utils.ResourceSlice)] = *resource.NewQuantity(slice, resource.DecimalSI)
	} else {
		limits[v1.ResourceName(utils.ResourceCore)] = *resource.NewQuantity(core, resource.DecimalSI)
	}
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   "default",
			Annotations: map[string]string{utils.AnnoDeviceIndex: strings.Join(ids, ",")},
		},
		Spec: v1.PodSpec{
			NodeName: "node-1",
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: limits,
					},
				},
			},
		},
		Status: v1.PodStatus{Phase: v1.PodRunning},
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
// Tests: preferNonTestCards (card-level preference, design §5.5)
// ===========================================================================

// TestPreferNonTestCards verifies cards without a pinned detection Pod are ordered first
// while the candidate set is preserved.
func TestPreferNonTestCards(t *testing.T) {
	// Node with 3 whole cards; a running detection Pod pinned to minor 1.
	nodeInfo := newTestNodeInfo("node-1", 3, 1024, 200, "")
	testPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "rt-diag-x",
			Namespace: "default",
			Labels:    map[string]string{utils.LabelActiveMonitoring: "true"},
			Annotations: map[string]string{
				utils.AnnoActiveMonitoringTargetMinor: "1",
			},
		},
		Spec: v1.PodSpec{
			NodeName: "node-1",
			// Detection Pods request no GPU resource (免资源占卡); the target minor alone
			// drives the hasTestPod flag that preferNonTestCards reads.
			Containers: []v1.Container{{}},
		},
		Status: v1.PodStatus{Phase: v1.PodRunning},
	}
	nodeInfo.AddPod(testPod)

	// The pinned minor 1 is flagged; it still reads as idle because detection Pods use no resource.
	devices := ampcache.BuildCache(nodeInfo)
	assert.True(t, devices[1].HasTestPod())
	assert.False(t, devices[0].HasTestPod())

	got := preferNonTestCards([]int{0, 1, 2}, devices)
	assert.Equal(t, []int{0, 2, 1}, got, "non-test cards first, test card last")
	assert.ElementsMatch(t, []int{0, 1, 2}, got, "no candidate may be dropped")
}

// TestPreferNonTestCards_EdgeCases: with no detection Pods the order is unchanged; empty in → empty out.
func TestPreferNonTestCards_EdgeCases(t *testing.T) {
	devices := buildAvailableDevices("node-1", 3, 1024, 200, "")
	assert.Equal(t, []int{2, 0, 1}, preferNonTestCards([]int{2, 0, 1}, devices), "no test pods → order preserved")
	assert.Empty(t, preferNonTestCards(nil, devices))
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
	groups := findResourceTopologyGroups(devices, emptyMigDevices(), 2, 512, 100, utils.TaskModeUnknown)
	assert.Len(t, groups, 1)
	assert.ElementsMatch(t, []int{0, 1}, groups[0])
}

func TestFindResourceTopologyGroups_InsufficientMemory(t *testing.T) {
	anno := topoAnnotation(2, [][]int{{0, 1}})
	devices := buildAvailableDevices("node-1", 2, 1024, 200, anno)
	// Request more memory than each device has.
	groups := findResourceTopologyGroups(devices, emptyMigDevices(), 2, 2048, 100, utils.TaskModeUnknown)
	assert.Empty(t, groups)
}

func TestFindResourceTopologyGroups_InsufficientCore(t *testing.T) {
	anno := topoAnnotation(2, [][]int{{0, 1}})
	devices := buildAvailableDevices("node-1", 2, 1024, 200, anno)
	// Request more core than each device has.
	groups := findResourceTopologyGroups(devices, emptyMigDevices(), 2, 512, 500, utils.TaskModeUnknown)
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
	groups := findResourceTopologyGroups(devices, migDevs, 2, 512, 100, utils.TaskModeUnknown)
	assert.Empty(t, groups)
}

func TestFindResourceTopologyGroups_PartiallyUsed(t *testing.T) {
	anno := topoAnnotation(3, [][]int{{0, 1, 2}})
	devices := buildAvailableDevices("node-1", 3, 1024, 200, anno)
	// Use half of device 1.
	devices[1].UsedResource(512, 100, 0, 0)
	// Request still fits (device 1 has 512 mem, 100 core left).
	groups := findResourceTopologyGroups(devices, emptyMigDevices(), 3, 512, 100, utils.TaskModeUnknown)
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
	devices[2].UsedResource(800, 0, 0, 0)
	devices[3].UsedResource(800, 0, 0, 0)

	selected, ok := tryVirtualTopoReserve(devices, emptyMigDevices(), 2, 100, 100, utils.ScheduleStrategyBinpack, utils.TaskModeUnknown)
	assert.True(t, ok)
	// Binpack picks the group with lower idle ratio → {2,3}
	assert.ElementsMatch(t, []int{2, 3}, selected)
}

func TestTryVirtualTopoReserve_Spread(t *testing.T) {
	anno := topoAnnotation(4, [][]int{{0, 1}, {2, 3}})
	devices := buildAvailableDevices("node-1", 4, 1024, 200, anno)
	// Group {2,3}: use 800 mem on each → higher utilization.
	devices[2].UsedResource(800, 0, 0, 0)
	devices[3].UsedResource(800, 0, 0, 0)

	selected, ok := tryVirtualTopoReserve(devices, emptyMigDevices(), 2, 100, 100, utils.ScheduleStrategySpread, utils.TaskModeUnknown)
	assert.True(t, ok)
	// Spread picks the group with higher idle ratio → {0,1}
	assert.ElementsMatch(t, []int{0, 1}, selected)
}

func TestTryVirtualTopoReserve_NoGroups(t *testing.T) {
	devices := buildAvailableDevices("node-1", 4, 1024, 200, "")
	selected, ok := tryVirtualTopoReserve(devices, emptyMigDevices(), 2, 100, 100, utils.ScheduleStrategyBinpack, utils.TaskModeUnknown)
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

// TestFilter_WholeCard_BlockedByMPSTask verifies that a device running an MPS task is
// rejected for a whole-card request even though the whole-card path does not compare
// task modes: the MPS accounting already makes the device non-idle.
func TestFilter_WholeCard_BlockedByMPSTask(t *testing.T) {
	nodeName := "node-1"
	node := newTestNode(nodeName, 1, 1024, 200, "")

	// The only device is running an MPS task (slice only, no core).
	mpsPod := newRunningPod("mps-pod", []int{0}, 0, 0, 1)
	nodeInfo := newTestNodeInfoWithPods(node, mpsPod)
	devices := ampcache.BuildCache(nodeInfo)
	assert.False(t, devices[0].IdleDevice(), "device running an MPS task must not be idle")

	state := framework.NewCycleState()
	clusterDevices := make(ampcache.AvailableClusterDevices)
	clusterDevices[nodeName] = devices
	state.Write(ampcache.ClusterDevicesStateKey, clusterDevices)
	state.Write(ampcache.ClusterMigDeviceStateKey, make(ampcache.ClusterNodeMigDevices))
	state.Write(ampcache.VStandardTaskReqStateKey, ampcache.StandardTaskReq{
		Vmem:            1024,
		Vcore:           100,
		VReplicas:       1,
		NormalGpuReqCnt: 1,
		TaskMode:        utils.TaskModeNormal,
	})

	plugin := &NormalStandardPlugin{}
	status := plugin.Filter(context.Background(), state, &v1.Pod{}, nodeInfo)
	assert.Equal(t, framework.Unschedulable, status.Code(), "whole-card pod should not take a device running an MPS task")
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
	req := ampcache.StandardTaskReq{Vslice: 1, Vmem: 100, VReplicas: 1, TaskMode: utils.TaskModeNormal}
	devices := make(ampcache.AvailableClusterDevices)
	devices["node-1"] = buildAvailableDevices("node-1", 4, 1024, 200, "")
	state.Write(ampcache.ClusterDevicesStateKey, devices)
	state.Write(ampcache.VStandardTaskReqStateKey, req)
	state.Write(ampcache.ClusterMigDeviceStateKey, make(ampcache.ClusterNodeMigDevices))

	nodeInfo := newTestNodeInfo("node-1", 4, 1024, 200, "")
	plugin := &NormalStandardPlugin{}

	// MIG pods carry the rt_mig_enable label and are handed over to the Mig plugin.
	pod := newRequestingPod(map[string]string{utils.LabelsMigTask: utils.MigLabelValue}, 0, 100, 1)
	status := plugin.Filter(context.Background(), state, pod, nodeInfo)
	assert.True(t, status.IsSuccess())
	assert.Contains(t, status.Message(), "mig pod")
}

// TestFilter_NodeReservedForMig verifies that a node labeled rt_mig_enable=true
// only accepts MIG tasks, rejecting whole-card, time-sharing and MPS pods.
func TestFilter_NodeReservedForMig(t *testing.T) {
	migNode := newTestNode("node-1", 4, 1024, 200, "")
	migNode.Labels = map[string]string{utils.LabelsMigTask: utils.MigLabelValue}
	nodeInfo := newTestNodeInfoWithPods(migNode)
	devices := ampcache.AvailableClusterDevices{"node-1": ampcache.BuildCache(nodeInfo)}

	tests := []struct {
		name     string
		pod      *v1.Pod
		req      ampcache.StandardTaskReq
		expected framework.Code
	}{
		{
			name:     "whole card task is rejected",
			pod:      newTestPodWithResources(map[string]int64{utils.ResourceNvidia: 1}),
			req:      ampcache.StandardTaskReq{NormalGpuReqCnt: 1, VReplicas: 1},
			expected: framework.Unschedulable,
		},
		{
			name:     "time-sharing task is rejected",
			pod:      newRequestingPod(nil, 50, 512, 0),
			req:      ampcache.StandardTaskReq{Vcore: 50, Vmem: 512, VReplicas: 1, TaskMode: utils.TaskModeNormal},
			expected: framework.Unschedulable,
		},
		{
			name:     "MPS task is rejected",
			pod:      newRequestingPod(nil, 0, 512, 1),
			req:      ampcache.StandardTaskReq{Vmem: 512, Vslice: 1, VReplicas: 1, TaskMode: utils.TaskModeMPS},
			expected: framework.Unschedulable,
		},
		{
			name:     "MIG task is handed over to the Mig plugin",
			pod:      newRequestingPod(map[string]string{utils.LabelsMigTask: utils.MigLabelValue}, 0, 512, 1),
			req:      ampcache.StandardTaskReq{Vmem: 512, Vslice: 1, VReplicas: 1, TaskMode: utils.TaskModeNormal},
			expected: framework.Success,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := framework.NewCycleState()
			state.Write(ampcache.ClusterDevicesStateKey, devices)
			state.Write(ampcache.VStandardTaskReqStateKey, tt.req)
			state.Write(ampcache.ClusterMigDeviceStateKey, make(ampcache.ClusterNodeMigDevices))

			plugin := &NormalStandardPlugin{}
			status := plugin.Filter(context.Background(), state, tt.pod, nodeInfo)
			assert.Equal(t, tt.expected, status.Code(), status.Message())
		})
	}
}

// migSliceAnnotation builds a device annotation where every card reports MIG support
// with the given number of MIG slices, which is independent of the Allocatable pool.
func migSliceAnnotation(gpuCount int, slicePerCard uint) string {
	gd := ampcache.GpuDevice{Count: gpuCount, Devices: make(map[int]*ampcache.Device)}
	for i := 0; i < gpuCount; i++ {
		gd.Devices[i] = &ampcache.Device{
			Minor:     uint(i),
			Uuid:      fmt.Sprintf("gpu-%d", i),
			Health:    utils.DeviceHealth,
			Type:      "gpu",
			MigEnable: true,
			Slice:     slicePerCard,
		}
	}
	b, _ := json.Marshal(gd)
	return string(b)
}

// TestFilter_MPS_UsesAllocatableSlice verifies that MPS capacity comes from the node
// Allocatable gpu-slice pool and is not limited by the MIG slices in the annotation.
func TestFilter_MPS_UsesAllocatableSlice(t *testing.T) {
	// 4 cards, Allocatable exports 7 slices per card while the annotation reports 1 MIG slice per card.
	node := newTestNode("node-1", 4, 1024, 200, migSliceAnnotation(4, 1))
	nodeInfo := newTestNodeInfoWithPods(node)
	devices := ampcache.AvailableClusterDevices{"node-1": ampcache.BuildCache(nodeInfo)}

	tests := []struct {
		name     string
		slice    int
		expected framework.Code
	}{
		{name: "request above the annotation MIG slices but within Allocatable", slice: 6, expected: framework.Success},
		{name: "request above the Allocatable slices", slice: 8, expected: framework.Unschedulable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := framework.NewCycleState()
			state.Write(ampcache.ClusterDevicesStateKey, devices)
			state.Write(ampcache.VStandardTaskReqStateKey, ampcache.StandardTaskReq{Vslice: tt.slice, VReplicas: 1, TaskMode: utils.TaskModeMPS})
			state.Write(ampcache.ClusterMigDeviceStateKey, make(ampcache.ClusterNodeMigDevices))

			plugin := &NormalStandardPlugin{}
			status := plugin.Filter(context.Background(), state, newRequestingPod(nil, 0, 0, int64(tt.slice)), nodeInfo)
			assert.Equal(t, tt.expected, status.Code(), status.Message())
		})
	}
}

// TestReserve_NodeReservedForMig verifies that Reserve refuses non-MIG pods on a
// node reserved for MIG tasks.
func TestReserve_NodeReservedForMig(t *testing.T) {
	nodeName := "node-1"
	node := newTestNode(nodeName, 4, 1024, 200, "")
	node.Labels = map[string]string{utils.LabelsMigTask: utils.MigLabelValue}
	pendingPod := newRequestingPod(nil, 50, 512, 0)

	handle := newFakeHandle(map[string]*framework.NodeInfo{nodeName: newTestNodeInfoWithPods(node)}, pendingPod)
	plugin := &NormalStandardPlugin{fh: handle}

	state := framework.NewCycleState()
	_, status := plugin.PreFilter(context.Background(), state, pendingPod)
	assert.True(t, status.IsSuccess())

	status = plugin.Reserve(context.Background(), state, pendingPod, nodeName)
	assert.Equal(t, framework.Error, status.Code(), "time-sharing pod must not be reserved on a MIG reserved node")
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

func TestScore_MPS_PrefersNodeRunningMPS(t *testing.T) {
	defaultReq := ampcache.StandardTaskReq{
		Vmem:      512,
		Vslice:    1,
		VReplicas: 1,
		TaskMode:  utils.TaskModeMPS,
	}

	tests := []struct {
		name      string
		pods      []*v1.Pod
		req       *ampcache.StandardTaskReq
		wantScore int64
	}{
		{
			name:      "node without MPS task scores zero",
			pods:      nil,
			wantScore: 0,
		},
		{
			name:      "node with an MPS task and enough idle resources scores max",
			pods:      []*v1.Pod{newRunningPod("mps-pod", []int{0}, 0, 512, 1)},
			wantScore: framework.MaxNodeScore,
		},
		{
			name:      "node whose MPS card is full scores zero",
			pods:      []*v1.Pod{newRunningPod("mps-full-pod", []int{0}, 0, 512, slicesPerGPU)},
			wantScore: 0,
		},
		{
			name: "node with an MPS task but fewer idle slices than requested scores zero",
			// The card keeps a single idle slice while the pod asks for two.
			pods:      []*v1.Pod{newRunningPod("mps-slice-pod", []int{0}, 0, 512, slicesPerGPU-1)},
			req:       &ampcache.StandardTaskReq{Vmem: 512, Vslice: 2, VReplicas: 1, TaskMode: utils.TaskModeMPS},
			wantScore: 0,
		},
		{
			name:      "node with an MPS task but less idle memory than requested scores zero",
			pods:      []*v1.Pod{newRunningPod("mps-mem-pod", []int{0}, 0, 700, 1)},
			wantScore: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := newTestNode("node-1", 2, 1024, 200, "")
			nodeInfo := newTestNodeInfoWithPods(node, tt.pods...)
			devices := make(ampcache.AvailableClusterDevices)
			devices["node-1"] = ampcache.BuildCache(nodeInfo)

			req := defaultReq
			if tt.req != nil {
				req = *tt.req
			}

			state := framework.NewCycleState()
			state.Write(ampcache.ClusterDevicesStateKey, devices)
			state.Write(ampcache.VStandardTaskReqStateKey, req)
			state.Write(ampcache.ClusterMigDeviceStateKey, make(ampcache.ClusterNodeMigDevices))

			plugin := &NormalStandardPlugin{}
			score, status := plugin.Score(context.Background(), state, &v1.Pod{}, "node-1")
			assert.True(t, status.IsSuccess())
			assert.Equal(t, tt.wantScore, score)
		})
	}
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

			_, status := plugin.PreFilter(context.Background(), state, pod)

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

// ===========================================================================
// Tests: MPS / normal virtual scheduling
// ===========================================================================

func TestFilter_MPS_MutualExclusion(t *testing.T) {
	nodeName := "node-1"
	node := newTestNode(nodeName, 2, 1024, 200, "")

	// Device 0 is running a normal virtual task.
	normalPod := newRunningPod("normal-pod", []int{0}, 50, 512, 0)
	nodeInfo := newTestNodeInfoWithPods(node, normalPod)
	devices := ampcache.BuildCache(nodeInfo)

	state := framework.NewCycleState()
	clusterDevices := make(ampcache.AvailableClusterDevices)
	clusterDevices[nodeName] = devices
	state.Write(ampcache.ClusterDevicesStateKey, clusterDevices)
	state.Write(ampcache.ClusterMigDeviceStateKey, make(ampcache.ClusterNodeMigDevices))

	plugin := &NormalStandardPlugin{}

	// MPS pod cannot use device 0 (normal), and device 1 is idle → fits.
	mpsReq := ampcache.StandardTaskReq{Vmem: 512, Vslice: 1, VReplicas: 1, TaskMode: utils.TaskModeMPS}
	state.Write(ampcache.VStandardTaskReqStateKey, mpsReq)
	mpsStatus := plugin.Filter(context.Background(), state, &v1.Pod{}, nodeInfo)
	assert.True(t, mpsStatus.IsSuccess(), "MPS pod should fit on idle device 1")

	// MPS pod with 2 replicas: only device 1 is compatible → insufficient.
	mpsReq2 := ampcache.StandardTaskReq{Vmem: 512, Vslice: 1, VReplicas: 2, TaskMode: utils.TaskModeMPS}
	state.Write(ampcache.VStandardTaskReqStateKey, mpsReq2)
	mpsStatus2 := plugin.Filter(context.Background(), state, &v1.Pod{}, nodeInfo)
	assert.Equal(t, framework.Unschedulable, mpsStatus2.Code(), "MPS pod should not share GPU with normal virtual task")

	// Normal virtual pod can use device 0 (same type) and device 1 (idle).
	normalReq := ampcache.StandardTaskReq{Vmem: 512, Vcore: 50, VReplicas: 2, TaskMode: utils.TaskModeNormal}
	state.Write(ampcache.VStandardTaskReqStateKey, normalReq)
	normalStatus := plugin.Filter(context.Background(), state, &v1.Pod{}, nodeInfo)
	assert.True(t, normalStatus.IsSuccess(), "normal virtual pod should fit on device 0 and device 1")
}

func TestFilter_MPS_DeviceWithMPSBlockedForNormal(t *testing.T) {
	nodeName := "node-1"
	node := newTestNode(nodeName, 1, 1024, 200, "")

	// Device 0 is running an MPS task.
	mpsPod := newRunningPod("mps-pod", []int{0}, 0, 512, 1)
	nodeInfo := newTestNodeInfoWithPods(node, mpsPod)
	devices := ampcache.BuildCache(nodeInfo)

	state := framework.NewCycleState()
	clusterDevices := make(ampcache.AvailableClusterDevices)
	clusterDevices[nodeName] = devices
	state.Write(ampcache.ClusterDevicesStateKey, clusterDevices)
	state.Write(ampcache.ClusterMigDeviceStateKey, make(ampcache.ClusterNodeMigDevices))

	plugin := &NormalStandardPlugin{}

	// Normal virtual pod cannot use the only device because it has an MPS task.
	normalReq := ampcache.StandardTaskReq{Vmem: 512, Vcore: 50, VReplicas: 1, TaskMode: utils.TaskModeNormal}
	state.Write(ampcache.VStandardTaskReqStateKey, normalReq)
	status := plugin.Filter(context.Background(), state, &v1.Pod{}, nodeInfo)
	assert.Equal(t, framework.Unschedulable, status.Code(), "normal virtual pod should not share GPU with MPS task")
}

func TestReserve_MPS_RespectsMutualExclusion(t *testing.T) {
	nodeName := "node-1"
	node := newTestNode(nodeName, 4, 1024, 200, "")

	// Device 0 is running an MPS task (uses 50 core, 512 mem).
	mpsPod := newRunningPod("mps-pod", []int{0}, 0, 512, 1)
	// Device 1 is running a normal virtual task.
	normalPod := newRunningPod("normal-pod", []int{1}, 50, 512, 0)

	nodeInfo := newTestNodeInfoWithPods(node, mpsPod, normalPod)
	devices := ampcache.BuildCache(nodeInfo)

	state := framework.NewCycleState()
	clusterDevices := make(ampcache.AvailableClusterDevices)
	clusterDevices[nodeName] = devices
	state.Write(ampcache.ClusterDevicesStateKey, clusterDevices)
	state.Write(ampcache.ClusterMigDeviceStateKey, make(ampcache.ClusterNodeMigDevices))
	state.Write(ampcache.VStandardTaskReqStateKey, ampcache.StandardTaskReq{
		Vmem:      512,
		Vslice:    1,
		VReplicas: 1,
		TaskMode:  utils.TaskModeMPS,
	})

	newPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "new-mps-pod", Namespace: "default"},
	}
	handle := newFakeHandle(map[string]*framework.NodeInfo{nodeName: nodeInfo}, newPod)
	plugin := &NormalStandardPlugin{fh: handle}

	status := plugin.Reserve(context.Background(), state, newPod, nodeName)
	assert.True(t, status.IsSuccess(), "Reserve should succeed")

	patchedPod, err := handle.client.CoreV1().Pods("default").Get(context.Background(), "new-mps-pod", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Equal(t, "0", patchedPod.Annotations[utils.AnnoDeviceIndex], "MPS pod should only use MPS-compatible device")
}

func TestReserve_Normal_RespectsMutualExclusion(t *testing.T) {
	nodeName := "node-1"
	node := newTestNode(nodeName, 4, 1024, 200, "")

	// Device 0 is running a normal virtual task.
	normalPod := newRunningPod("normal-pod", []int{0}, 50, 512, 0)
	// Device 1 is running an MPS task.
	mpsPod := newRunningPod("mps-pod", []int{1}, 0, 512, 1)

	nodeInfo := newTestNodeInfoWithPods(node, normalPod, mpsPod)
	devices := ampcache.BuildCache(nodeInfo)

	state := framework.NewCycleState()
	clusterDevices := make(ampcache.AvailableClusterDevices)
	clusterDevices[nodeName] = devices
	state.Write(ampcache.ClusterDevicesStateKey, clusterDevices)
	state.Write(ampcache.ClusterMigDeviceStateKey, make(ampcache.ClusterNodeMigDevices))
	state.Write(ampcache.VStandardTaskReqStateKey, ampcache.StandardTaskReq{
		Vmem:      512,
		Vcore:     50,
		VReplicas: 1,
		TaskMode:  utils.TaskModeNormal,
	})

	newPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "new-normal-pod", Namespace: "default"},
	}
	handle := newFakeHandle(map[string]*framework.NodeInfo{nodeName: nodeInfo}, newPod)
	plugin := &NormalStandardPlugin{fh: handle}

	status := plugin.Reserve(context.Background(), state, newPod, nodeName)
	assert.True(t, status.IsSuccess(), "Reserve should succeed")

	patchedPod, err := handle.client.CoreV1().Pods("default").Get(context.Background(), "new-normal-pod", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Equal(t, "0", patchedPod.Annotations[utils.AnnoDeviceIndex], "normal virtual pod should only use normal-compatible device")
}

func TestReserve_MPS_FollowsBinpackStrategy(t *testing.T) {
	nodeName := "node-1"
	node := newTestNode(nodeName, 4, 1024, 200, "")

	// Device 0 has an MPS task holding 1 slice and 300 memory.
	mpsPod0 := newRunningPod("mps-pod-0", []int{0}, 0, 300, 1)
	// Device 1 has an MPS task holding 5 slices and 700 memory.
	mpsPod1 := newRunningPod("mps-pod-1", []int{1}, 0, 700, 5)

	nodeInfo := newTestNodeInfoWithPods(node, mpsPod0, mpsPod1)
	devices := ampcache.BuildCache(nodeInfo)

	state := framework.NewCycleState()
	clusterDevices := make(ampcache.AvailableClusterDevices)
	clusterDevices[nodeName] = devices
	state.Write(ampcache.ClusterDevicesStateKey, clusterDevices)
	state.Write(ampcache.ClusterMigDeviceStateKey, make(ampcache.ClusterNodeMigDevices))
	state.Write(ampcache.VStandardTaskReqStateKey, ampcache.StandardTaskReq{
		Vmem:      100,
		Vslice:    1,
		VReplicas: 1,
		TaskMode:  utils.TaskModeMPS,
	})

	newPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "new-mps-pod", Namespace: "default"},
	}
	handle := newFakeHandle(map[string]*framework.NodeInfo{nodeName: nodeInfo}, newPod)
	plugin := &NormalStandardPlugin{fh: handle}

	status := plugin.Reserve(context.Background(), state, newPod, nodeName)
	assert.True(t, status.IsSuccess(), "Reserve should succeed")

	patchedPod, err := handle.client.CoreV1().Pods("default").Get(context.Background(), "new-mps-pod", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Equal(t, "1", patchedPod.Annotations[utils.AnnoDeviceIndex], "MPS pod should follow binpack and select the more utilized device")
}

// ===========================================================================
// Tests: MPS / MIG coexistence and PreFilter validation
// ===========================================================================

func TestFilter_MPS_SkipsMIGDevice(t *testing.T) {
	nodeName := "node-1"
	node := newTestNode(nodeName, 2, 1024, 200, "")

	// Device 0 is a MIG device (from the scheduler's MIG cache perspective).
	migDevices := ampcache.ClusterNodeMigDevices{
		nodeName: &ampcache.GpuDevice{
			Devices: map[int]*ampcache.Device{
				0: {Minor: 0, MigEnable: true},
			},
		},
	}

	nodeInfo := newTestNodeInfoWithPods(node)
	devices := ampcache.BuildCache(nodeInfo)

	state := framework.NewCycleState()
	clusterDevices := make(ampcache.AvailableClusterDevices)
	clusterDevices[nodeName] = devices
	state.Write(ampcache.ClusterDevicesStateKey, clusterDevices)
	state.Write(ampcache.ClusterMigDeviceStateKey, migDevices)

	plugin := &NormalStandardPlugin{}

	// MPS pod can only use device 1 because device 0 is MIG.
	mpsReq := ampcache.StandardTaskReq{Vmem: 512, Vslice: 1, VReplicas: 1, TaskMode: utils.TaskModeMPS}
	state.Write(ampcache.VStandardTaskReqStateKey, mpsReq)
	status := plugin.Filter(context.Background(), state, &v1.Pod{}, nodeInfo)
	assert.True(t, status.IsSuccess(), "MPS pod should fit on non-MIG device 1")
}

func TestFilter_MPS_AllDevicesMIGOrBlocked(t *testing.T) {
	nodeName := "node-1"
	node := newTestNode(nodeName, 2, 1024, 200, "")

	// Device 0 is MIG; device 1 is running a normal virtual task.
	normalPod := newRunningPod("normal-pod", []int{1}, 50, 512, 0)

	migDevices := ampcache.ClusterNodeMigDevices{
		nodeName: &ampcache.GpuDevice{
			Devices: map[int]*ampcache.Device{
				0: {Minor: 0, MigEnable: true},
			},
		},
	}

	nodeInfo := newTestNodeInfoWithPods(node, normalPod)
	devices := ampcache.BuildCache(nodeInfo)

	state := framework.NewCycleState()
	clusterDevices := make(ampcache.AvailableClusterDevices)
	clusterDevices[nodeName] = devices
	state.Write(ampcache.ClusterDevicesStateKey, clusterDevices)
	state.Write(ampcache.ClusterMigDeviceStateKey, migDevices)

	plugin := &NormalStandardPlugin{}

	// MPS pod cannot use device 0 (MIG) or device 1 (normal task).
	mpsReq := ampcache.StandardTaskReq{Vmem: 512, Vslice: 1, VReplicas: 1, TaskMode: utils.TaskModeMPS}
	state.Write(ampcache.VStandardTaskReqStateKey, mpsReq)
	status := plugin.Filter(context.Background(), state, &v1.Pod{}, nodeInfo)
	assert.Equal(t, framework.Unschedulable, status.Code(), "MPS pod should have no available device")
}

func TestPreFilter_MPS_MultiReplicaDowngradedToSingleCard(t *testing.T) {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mps-multi-replica",
			Namespace: "default",
			Labels: map[string]string{
				utils.LabelsReplicas: "2",
			},
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

	nodeInfos := map[string]*framework.NodeInfo{
		"node-1": newTestNodeInfo("node-1", 4, 1024, 200, ""),
	}
	plugin := &NormalStandardPlugin{fh: newFakeHandle(nodeInfos)}
	state := framework.NewCycleState()
	_, status := plugin.PreFilter(context.Background(), state, pod)
	assert.True(t, status.IsSuccess(), "MPS task with replicas > 1 should be downgraded instead of rejected")

	_, _, req, err := ampcache.GetPreFilterStateData(state)
	assert.NoError(t, err)
	assert.Equal(t, utils.TaskModeMPS, req.TaskMode)
	assert.Equal(t, 1, req.VReplicas, "MPS task replicas should be downgraded to 1")
}

func TestReserve_MPS_MultiReplicaAssignsSingleCard(t *testing.T) {
	nodeName := "node-1"
	node := newTestNode(nodeName, 4, 1024, 200, "")
	nodeInfo := newTestNodeInfoWithPods(node)

	// The pending MPS pod carries replicas=2 but must end up on exactly one card.
	pendingPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mps-multi-replica",
			Namespace: "default",
			Labels:    map[string]string{utils.LabelsReplicas: "2"},
		},
		Spec: v1.PodSpec{
			NodeName: nodeName,
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(utils.ResourceSlice):  resource.MustParse("1"),
							v1.ResourceName(utils.ResourceMemory): resource.MustParse("512"),
						},
					},
				},
			},
		},
	}

	handle := newFakeHandle(map[string]*framework.NodeInfo{nodeName: nodeInfo}, pendingPod)
	plugin := &NormalStandardPlugin{fh: handle}

	state := framework.NewCycleState()
	_, status := plugin.PreFilter(context.Background(), state, pendingPod)
	assert.True(t, status.IsSuccess())

	status = plugin.Reserve(context.Background(), state, pendingPod, nodeName)
	assert.True(t, status.IsSuccess(), "Reserve should succeed")

	patchedPod, err := handle.client.CoreV1().Pods("default").Get(context.Background(), "mps-multi-replica", metav1.GetOptions{})
	assert.NoError(t, err)
	idxList, err := utils.GetAnnotationsFromPodListInt(patchedPod, utils.AnnoDeviceIndex)
	assert.NoError(t, err)
	assert.Len(t, idxList, 1, "MPS pod should be assigned exactly one GPU card")
	assert.Equal(t, "1", patchedPod.Annotations[utils.AnnoCoreResourceByPod], "assigned slices should be annotated")
	assert.Equal(t, "false", patchedPod.Annotations[utils.AnnotationSliceAssignedFlag])
	assert.Equal(t, "false", patchedPod.Annotations[utils.AnnoMemoryAssignedFlag])
	// An MPS task is sized by slices, so core is never flagged as assigned.
	_, hasCoreAssigned := patchedPod.Annotations[utils.AnnoCoreAssignedFlag]
	assert.False(t, hasCoreAssigned, "MPS task must not mark gpu-core as assigned")
}

// TestReserve_TimeSharingWithSliceAnnotatesAssignedOnly verifies that a pod requesting
// both gpu-slice and 0 < gpu-core < 100 is scheduled as a time-sharing task: only the
// resources really assigned by the scheduler (gpu-core and gpu-memory) are flagged.
func TestReserve_TimeSharingWithSliceAnnotatesAssignedOnly(t *testing.T) {
	nodeName := "node-1"
	node := newTestNode(nodeName, 4, 1024, 200, "")
	nodeInfo := newTestNodeInfoWithPods(node)

	pendingPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "mixed-pod", Namespace: "default"},
		Spec: v1.PodSpec{
			NodeName: nodeName,
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(utils.ResourceCore):   resource.MustParse("50"),
							v1.ResourceName(utils.ResourceSlice):  resource.MustParse("1"),
							v1.ResourceName(utils.ResourceMemory): resource.MustParse("512"),
						},
					},
				},
			},
		},
	}

	handle := newFakeHandle(map[string]*framework.NodeInfo{nodeName: nodeInfo}, pendingPod)
	plugin := &NormalStandardPlugin{fh: handle}

	state := framework.NewCycleState()
	_, status := plugin.PreFilter(context.Background(), state, pendingPod)
	assert.True(t, status.IsSuccess())

	_, _, req, err := ampcache.GetPreFilterStateData(state)
	assert.NoError(t, err)
	assert.Equal(t, utils.TaskModeNormal, req.TaskMode, "gpu-core<100 wins over gpu-slice")

	status = plugin.Reserve(context.Background(), state, pendingPod, nodeName)
	assert.True(t, status.IsSuccess(), "Reserve should succeed")

	patchedPod, err := handle.client.CoreV1().Pods("default").Get(context.Background(), "mixed-pod", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Equal(t, "50", patchedPod.Annotations[utils.AnnoCoreResourceByPod])
	assert.Equal(t, "512", patchedPod.Annotations[utils.AnnoMemoryResourceByPod])
	assert.Equal(t, "false", patchedPod.Annotations[utils.AnnoCoreAssignedFlag])
	assert.Equal(t, "false", patchedPod.Annotations[utils.AnnoMemoryAssignedFlag])
	// A time-sharing task does not consume slices, so neither the slice value nor the
	// slice assigned flag may be written even when the pod asks for gpu-slice.
	_, hasSliceValue := patchedPod.Annotations[utils.ResourceSlice]
	assert.False(t, hasSliceValue, "time-sharing task must not be annotated with slices")
	_, hasSliceAssigned := patchedPod.Annotations[utils.AnnotationSliceAssignedFlag]
	assert.False(t, hasSliceAssigned, "time-sharing task must not mark slices as assigned")
}

func TestPreFilter_PodTaskMode(t *testing.T) {
	tests := []struct {
		name     string
		pod      *v1.Pod
		expected utils.TaskMode
	}{
		{
			name:     "gpu-slice without MIG label is MPS",
			pod:      newRequestingPod(nil, 0, 0, 2),
			expected: utils.TaskModeMPS,
		},
		{
			name:     "gpu-slice with MIG label is a normal (MIG) task",
			pod:      newRequestingPod(map[string]string{utils.LabelsMigTask: utils.MigLabelValue}, 0, 0, 2),
			expected: utils.TaskModeNormal,
		},
		{
			name:     "gpu-core<100 is a time-sharing task",
			pod:      newRequestingPod(nil, 50, 0, 0),
			expected: utils.TaskModeNormal,
		},
		{
			name:     "gpu-slice with gpu-core<100 is a time-sharing task",
			pod:      newRequestingPod(nil, 50, 0, 1),
			expected: utils.TaskModeNormal,
		},
		{
			name:     "gpu-memory only is a time-sharing task",
			pod:      newRequestingPod(nil, 0, 512, 0),
			expected: utils.TaskModeNormal,
		},
	}

	nodeInfos := map[string]*framework.NodeInfo{
		"node-1": newTestNodeInfo("node-1", 4, 1024, 200, ""),
	}
	plugin := &NormalStandardPlugin{fh: newFakeHandle(nodeInfos)}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := framework.NewCycleState()
			_, status := plugin.PreFilter(context.Background(), state, tt.pod)
			assert.True(t, status.IsSuccess())

			_, _, req, err := ampcache.GetPreFilterStateData(state)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, req.TaskMode)
		})
	}
}

// newRequestingPod builds a pod requesting the given gpu-core, gpu-memory and gpu-slice.
func newRequestingPod(labels map[string]string, core, mem, slice int64) *v1.Pod {
	limits := v1.ResourceList{}
	if core > 0 {
		limits[v1.ResourceName(utils.ResourceCore)] = *resource.NewQuantity(core, resource.DecimalSI)
	}
	if mem > 0 {
		limits[v1.ResourceName(utils.ResourceMemory)] = *resource.NewQuantity(mem, resource.DecimalSI)
	}
	if slice > 0 {
		limits[v1.ResourceName(utils.ResourceSlice)] = *resource.NewQuantity(slice, resource.DecimalSI)
	}
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "requesting-pod",
			Namespace: "default",
			Labels:    labels,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: limits,
					},
				},
			},
		},
	}
}
