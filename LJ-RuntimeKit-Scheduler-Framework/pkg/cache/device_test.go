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
	"encoding/json"
	"fmt"
	"testing"

	"github.com/lj-runtimekit/scheduler/pkg/utils"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

func TestNewDeviceInfo(t *testing.T) {
	dev := NewDeviceInfo(1, 1024, 4)
	assert.Equal(t, 1, dev.idx)
	assert.Equal(t, int64(1024), dev.totalVMemory)
	assert.Equal(t, int64(4), dev.totalVCore)
	assert.Equal(t, int64(0), dev.usedVMemory)
	assert.Equal(t, int64(0), dev.usedVCore)
	assert.NotNil(t, dev.scheduledBEPods)
}

func TestDeviceInfo_TrunkDevice(t *testing.T) {
	dev := NewDeviceInfo(0, 2048, 8)
	dev.TrunkDevice()
	assert.Equal(t, dev.totalVMemory, dev.usedVMemory)
	assert.Equal(t, dev.totalVCore, dev.usedVCore)
}

func TestDeviceInfo_UsedResource(t *testing.T) {
	dev := NewDeviceInfo(0, 4096, 16)
	dev.UsedResource(1024, 4, 0, 0)
	assert.Equal(t, int64(1024), dev.usedVMemory)
	assert.Equal(t, int64(4), dev.usedVCore)

	// Slices are tracked separately so that MPS tasks can share a device.
	dev.SetTotalSlice(7)
	dev.UsedResource(0, 0, 2, 0)
	assert.Equal(t, int64(2), dev.GetUsedSlice())
	assert.Equal(t, int64(5), dev.GetIdleSlice())
}

func TestDeviceInfo_IdleDevice(t *testing.T) {
	dev := NewDeviceInfo(0, 1024, 4)
	assert.True(t, dev.IdleDevice())

	dev.UsedResource(100, 1, 0, 0)
	assert.False(t, dev.IdleDevice())

	// A device holding only slices is still in use.
	sliceDev := NewDeviceInfo(0, 1024, 4)
	sliceDev.SetTotalSlice(7)
	sliceDev.UsedResource(0, 0, 1, 0)
	assert.False(t, sliceDev.IdleDevice())

	// Edge case: zero total resources
	devZero := &DeviceInfo{
		totalVMemory: 0,
		totalVCore:   0,
	}
	assert.False(t, devZero.IdleDevice())
}

func TestDeviceInfo_GetTotalAndIdleResource(t *testing.T) {
	dev := NewDeviceInfo(0, 2048, 8)
	dev.UsedResource(512, 2, 0, 0)

	totalMem, totalCore := dev.GetTotalResource()
	idleMem, idleCore := dev.GetIdleResource()

	assert.Equal(t, int64(2048), totalMem)
	assert.Equal(t, int64(8), totalCore)
	assert.Equal(t, int64(1536), idleMem)
	assert.Equal(t, int64(6), idleCore)

	// Overuse should not result in negative idle
	devOver := NewDeviceInfo(0, 100, 2)
	devOver.UsedResource(200, 5, 0, 0)
	idleMemOver, idleCoreOver := devOver.GetIdleResource()
	assert.Equal(t, int64(0), idleMemOver)
	assert.Equal(t, int64(0), idleCoreOver)
}

func TestDeviceInfo_CanRunTask(t *testing.T) {
	tests := []struct {
		name     string
		setup    func() *DeviceInfo
		mode     utils.TaskMode
		vmem     int64
		vcore    int64
		vslice   int64
		expected bool
	}{
		{
			name: "mps fits on idle device",
			setup: func() *DeviceInfo {
				dev := NewDeviceInfo(0, 1024, 100)
				dev.SetTotalSlice(7)
				return dev
			},
			mode: utils.TaskModeMPS, vmem: 512, vslice: 2, expected: true,
		},
		{
			name: "mps rejected when slices are insufficient",
			setup: func() *DeviceInfo {
				dev := NewDeviceInfo(0, 1024, 100)
				dev.SetTotalSlice(7)
				dev.UsedResource(0, 0, 6, 0)
				return dev
			},
			mode: utils.TaskModeMPS, vmem: 512, vslice: 2, expected: false,
		},
		{
			name: "mps rejected when memory is insufficient",
			setup: func() *DeviceInfo {
				dev := NewDeviceInfo(0, 1024, 100)
				dev.SetTotalSlice(7)
				dev.UsedResource(800, 0, 0, 0)
				return dev
			},
			mode: utils.TaskModeMPS, vmem: 512, vslice: 1, expected: false,
		},
		{
			name: "mps rejected on device running time-sharing tasks",
			setup: func() *DeviceInfo {
				dev := NewDeviceInfo(0, 1024, 100)
				dev.SetTotalSlice(7)
				dev.SetTaskMode(utils.TaskModeNormal)
				dev.UsedResource(100, 10, 0, 0)
				return dev
			},
			mode: utils.TaskModeMPS, vmem: 100, vslice: 1, expected: false,
		},
		{
			name: "time-sharing task fits on idle device",
			setup: func() *DeviceInfo {
				return NewDeviceInfo(0, 1024, 100)
			},
			mode: utils.TaskModeNormal, vmem: 512, vcore: 50, expected: true,
		},
		{
			name: "time-sharing task rejected on device running mps tasks",
			setup: func() *DeviceInfo {
				dev := NewDeviceInfo(0, 1024, 100)
				dev.SetTotalSlice(7)
				dev.SetTaskMode(utils.TaskModeMPS)
				dev.UsedResource(0, 0, 1, 0)
				return dev
			},
			mode: utils.TaskModeNormal, vmem: 512, vcore: 50, expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dev := tt.setup()
			assert.Equal(t, tt.expected, dev.CanRunTask(tt.mode, tt.vmem, tt.vcore, tt.vslice))
		})
	}
}

func TestDeviceInfo_DeepCopy(t *testing.T) {
	original := NewDeviceInfo(2, 1024, 4)
	original.SetTotalSlice(7)
	original.UsedResource(256, 1, 2, 0)
	original.hasTestPod = true

	copyDev := original.DeepCopy()
	assert.Equal(t, original.idx, copyDev.idx)
	assert.Equal(t, original.totalVMemory, copyDev.totalVMemory)
	assert.Equal(t, original.usedVMemory, copyDev.usedVMemory)
	assert.Equal(t, original.totalVCore, copyDev.totalVCore)
	assert.Equal(t, original.usedVCore, copyDev.usedVCore)
	assert.Equal(t, original.totalVSlice, copyDev.totalVSlice)
	assert.Equal(t, original.usedVSlice, copyDev.usedVSlice)
	assert.True(t, copyDev.HasTestPod(), "DeepCopy must preserve hasTestPod (Clone relies on it)")

	// Modify copy and ensure original is unchanged
	copyDev.usedVMemory = 512
	assert.NotEqual(t, original.usedVMemory, copyDev.usedVMemory)
}

func TestAvailableDevices_Clone(t *testing.T) {
	devices := make(AvailableDevices)
	devices[0] = NewDeviceInfo(0, 1024, 4)
	devices[0].UsedResource(256, 1, 0, 0)

	cloned := devices.Clone()
	assert.Equal(t, len(devices), len(cloned))
	assert.Equal(t, devices[0].usedVMemory, cloned[0].usedVMemory)
	assert.Equal(t, devices[0].idx, cloned[0].idx)

	// Ensure deep copy: modify clone shouldn't affect original
	cloned[0].usedVMemory = 512
	assert.NotEqual(t, devices[0].usedVMemory, cloned[0].usedVMemory)
}

func TestAvailableClusterDevices_Clone(t *testing.T) {
	cluster := make(AvailableClusterDevices)
	cluster["node-1"] = make(AvailableDevices)
	cluster["node-1"][0] = NewDeviceInfo(0, 2048, 8)
	cluster["node-1"][0].UsedResource(1024, 4, 0, 0)

	cloned := cluster.Clone().(AvailableClusterDevices)
	assert.Equal(t, len(cluster), len(cloned))
	assert.Equal(t, cluster["node-1"][0].usedVMemory, cloned["node-1"][0].usedVMemory)

	// Deep copy check
	cloned["node-1"][0].usedVMemory = 0
	assert.NotEqual(t, cluster["node-1"][0].usedVMemory, cloned["node-1"][0].usedVMemory)
}

func TestBuildCache(t *testing.T) {
	nodeName := "test-node"
	gpuCount := int64(2)
	totalMem := int64(4096)
	totalCore := int64(8)

	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: nodeName},
		Status: v1.NodeStatus{
			Allocatable: v1.ResourceList{
				v1.ResourceName(utils.ResourceNvidia): *resource.NewQuantity(gpuCount, resource.DecimalSI),
				v1.ResourceName(utils.ResourceMemory): *resource.NewQuantity(totalMem, resource.DecimalSI),
				v1.ResourceName(utils.ResourceCore):   *resource.NewQuantity(totalCore, resource.DecimalSI),
			},
		},
	}

	podUID := types.UID("pod-uid")
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
			UID:       podUID,
			Annotations: map[string]string{
				utils.AnnoDeviceIndex: "0",
			},
		},
		Spec: v1.PodSpec{
			NodeName: nodeName,
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(utils.ResourceMemory): *resource.NewQuantity(512, resource.DecimalSI),
							v1.ResourceName(utils.ResourceCore):   *resource.NewQuantity(2, resource.DecimalSI),
						},
					},
				},
			},
		},
		Status: v1.PodStatus{Phase: v1.PodRunning},
	}

	// Mock utils functions if needed via test setup or use real ones if deterministic
	// Here we assume utils.ResponseForTask returns true for this pod
	// and utils.IsNormalPodTask returns false (so it's a BE task)

	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(node)
	nodeInfo.AddPod(pod)

	devices := BuildCache(nodeInfo)

	assert.Len(t, devices, 2)
	assert.Contains(t, devices, 0)
	assert.Contains(t, devices, 1)

	dev0 := devices[0]
	dev1 := devices[1]

	fmt.Printf("0. %v\n", *dev0)
	fmt.Printf("1. %v\n", *dev1)
	assert.Equal(t, int64(512), dev0.usedVMemory)
	assert.Equal(t, int64(2), dev0.usedVCore)
	assert.Equal(t, totalMem/gpuCount, dev0.totalVMemory)
	assert.Equal(t, totalCore/gpuCount, dev0.totalVCore)

	dev1 = devices[1]
	assert.Equal(t, int64(0), dev1.usedVMemory)
	assert.Equal(t, int64(0), dev1.usedVCore)
}

// TestBuildCacheMarksTestPodCard verifies the card-level preference marker (design §5.5):
// a running detection Pod flags its target minor (rt-diag-target-minor) with hasTestPod while
// contributing no resource usage, so the pinned card still reads as idle. Standard uses the
// flag to prefer fully-idle cards; ActiveMonitoring Filter uses it to reject a card another
// detection Pod already occupies.
func TestBuildCacheMarksTestPodCard(t *testing.T) {
	nodeName := "test-node"
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: nodeName},
		Status: v1.NodeStatus{
			Allocatable: v1.ResourceList{
				v1.ResourceName(utils.ResourceGPU):    *resource.NewQuantity(2, resource.DecimalSI),
				v1.ResourceName(utils.ResourceMemory): *resource.NewQuantity(4096, resource.DecimalSI),
				v1.ResourceName(utils.ResourceCore):   *resource.NewQuantity(8, resource.DecimalSI),
			},
		},
	}
	// A running detection Pod pinned to minor 0 via the target-minor annotation. Detection
	// Pods request no GPU resource (免资源占卡), only CPU/memory for the container itself.
	testPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "rt-diag-x",
			Namespace: "default",
			UID:       types.UID("test-uid"),
			Labels:    map[string]string{utils.LabelActiveMonitoring: "true"},
			Annotations: map[string]string{
				utils.AnnoActiveMonitoringTargetMinor: "0",
			},
		},
		Spec: v1.PodSpec{
			NodeName: nodeName,
			Containers: []v1.Container{{
				Resources: v1.ResourceRequirements{Limits: v1.ResourceList{
					v1.ResourceCPU:    *resource.NewQuantity(2, resource.DecimalSI),
					v1.ResourceMemory: resource.MustParse("24Gi"),
				}},
			}},
		},
		Status: v1.PodStatus{Phase: v1.PodRunning},
	}
	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(node)
	nodeInfo.AddPod(testPod)

	view := BuildCache(nodeInfo)
	assert.True(t, view[0].HasTestPod(), "the target minor must be flagged for card-level preference")
	assert.True(t, view[0].IdleDevice(), "a detection Pod contributes no resource usage, so the card stays idle")
	assert.False(t, view[1].HasTestPod(), "an untouched minor must not be flagged")
}

func TestGetPreFilterStateData(t *testing.T) {
	state := framework.NewCycleState()

	// Prepare mock data
	mockReq := StandardTaskReq{
		Vmem:  1024,
		Vcore: 4,
	}
	mockDevices := make(AvailableClusterDevices)
	mockDevices["node-1"] = make(AvailableDevices)
	mockDevices["node-1"][0] = NewDeviceInfo(0, 2048, 8)

	mockMigDevice := make(ClusterNodeMigDevices)
	mockMigDevice["node-1"] = &GpuDevice{
		Count:   1,
		Devices: make(map[int]*Device),
	}

	state.Write(VStandardTaskReqStateKey, mockReq)
	state.Write(ClusterDevicesStateKey, mockDevices)
	state.Write(ClusterMigDeviceStateKey, mockMigDevice)

	devices, _, req, err := GetPreFilterStateData(state)
	assert.NoError(t, err)
	assert.Equal(t, mockReq.Vmem, req.Vmem)
	assert.Equal(t, mockReq.Vcore, req.Vcore)
	assert.Equal(t, mockDevices["node-1"][0].totalVMemory, devices["node-1"][0].totalVMemory)
}

func TestBuildCache_MPSAndNormal(t *testing.T) {
	nodeName := "test-node"
	gpuCount := int64(4)
	totalMem := int64(4096)
	totalCore := int64(800)
	totalSlice := int64(28)

	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: nodeName},
		Status: v1.NodeStatus{
			Allocatable: v1.ResourceList{
				v1.ResourceName(utils.ResourceNvidia): *resource.NewQuantity(gpuCount, resource.DecimalSI),
				v1.ResourceName(utils.ResourceMemory): *resource.NewQuantity(totalMem, resource.DecimalSI),
				v1.ResourceName(utils.ResourceCore):   *resource.NewQuantity(totalCore, resource.DecimalSI),
				v1.ResourceName(utils.ResourceSlice):  *resource.NewQuantity(totalSlice, resource.DecimalSI),
			},
		},
	}

	// MPS task: requests gpu-slice plus optional memory, without the MIG label.
	mpsPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mps-pod",
			Namespace: "default",
			UID:       types.UID("mps-pod-uid"),
			Annotations: map[string]string{
				utils.AnnoDeviceIndex: "0",
			},
		},
		Spec: v1.PodSpec{
			NodeName: nodeName,
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(utils.ResourceMemory): *resource.NewQuantity(512, resource.DecimalSI),
							v1.ResourceName(utils.ResourceSlice):  *resource.NewQuantity(2, resource.DecimalSI),
						},
					},
				},
			},
		},
		Status: v1.PodStatus{Phase: v1.PodRunning},
	}

	// Time-sharing task: requests gpu-core and gpu-memory only.
	normalPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "normal-pod",
			Namespace: "default",
			UID:       types.UID("normal-pod-uid"),
			Annotations: map[string]string{
				utils.AnnoDeviceIndex: "1",
			},
		},
		Spec: v1.PodSpec{
			NodeName: nodeName,
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(utils.ResourceMemory): *resource.NewQuantity(512, resource.DecimalSI),
							v1.ResourceName(utils.ResourceCore):   *resource.NewQuantity(50, resource.DecimalSI),
						},
					},
				},
			},
		},
		Status: v1.PodStatus{Phase: v1.PodRunning},
	}

	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(node)
	nodeInfo.AddPod(mpsPod)
	nodeInfo.AddPod(normalPod)

	devices := BuildCache(nodeInfo)

	// Each device gets 28/4 = 7 slices.
	assert.Equal(t, totalSlice/gpuCount, devices[0].GetTotalSlice())

	assert.True(t, devices[0].GetTaskMode() == utils.TaskModeMPS, "device 0 should be marked as having MPS task")
	assert.False(t, devices[0].GetTaskMode() == utils.TaskModeNormal, "device 0 should not be marked as having normal task")
	assert.Equal(t, int64(2), devices[0].GetUsedSlice(), "device 0 should track used slices")
	assert.Equal(t, int64(512), devices[0].usedVMemory, "device 0 should track used memory")

	assert.True(t, devices[1].GetTaskMode() == utils.TaskModeNormal, "device 1 should be marked as having normal task")
	assert.False(t, devices[1].GetTaskMode() == utils.TaskModeMPS, "device 1 should not be marked as having MPS task")
	assert.Equal(t, int64(0), devices[1].GetUsedSlice(), "device 1 should not consume slices")

	assert.False(t, devices[2].GetTaskMode() == utils.TaskModeMPS, "device 2 should be idle")
	assert.False(t, devices[2].GetTaskMode() == utils.TaskModeNormal, "device 2 should be idle")
	assert.True(t, devices[2].IdleDevice(), "device 2 should be idle")
}

// TestBuildCache_MpsSliceOnlyPod covers MPS pods that request only gpu-slice,
// without any gpu-memory: they must be tracked by slices and mark the device as MPS.
func TestBuildCache_MpsSliceOnlyPod(t *testing.T) {
	nodeName := "test-node"
	gpuCount := int64(2)
	totalSlice := int64(14)

	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: nodeName},
		Status: v1.NodeStatus{
			Allocatable: v1.ResourceList{
				v1.ResourceName(utils.ResourceNvidia): *resource.NewQuantity(gpuCount, resource.DecimalSI),
				v1.ResourceName(utils.ResourceSlice):  *resource.NewQuantity(totalSlice, resource.DecimalSI),
				v1.ResourceName(utils.ResourceMemory): *resource.NewQuantity(gpuCount*1024, resource.DecimalSI),
				v1.ResourceName(utils.ResourceCore):   *resource.NewQuantity(gpuCount*200, resource.DecimalSI),
			},
		},
	}

	mpsPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "mps-slice-only-pod",
			Namespace:   "default",
			UID:         types.UID("mps-slice-only-pod-uid"),
			Annotations: map[string]string{utils.AnnoDeviceIndex: "0"},
		},
		Spec: v1.PodSpec{
			NodeName: nodeName,
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(utils.ResourceSlice): *resource.NewQuantity(3, resource.DecimalSI),
						},
					},
				},
			},
		},
		Status: v1.PodStatus{Phase: v1.PodRunning},
	}

	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(node)
	nodeInfo.AddPod(mpsPod)

	devices := BuildCache(nodeInfo)

	assert.Equal(t, totalSlice/gpuCount, devices[0].GetTotalSlice())
	assert.Equal(t, int64(3), devices[0].GetUsedSlice())
	assert.Equal(t, totalSlice/gpuCount-3, devices[0].GetIdleSlice())
	assert.Equal(t, int64(0), devices[0].usedVMemory)
	assert.Equal(t, utils.TaskModeMPS, devices[0].GetTaskMode())
	// MPS tasks only account memory and slice, so the core accounting stays untouched.
	idleMemory, idleCore := devices[0].GetIdleResource()
	assert.Equal(t, int64(1024), idleMemory)
	assert.Equal(t, int64(200), idleCore, "MPS task must not consume core")
	assert.False(t, devices[0].IdleDevice(), "device running an MPS task is not idle")
	assert.True(t, devices[1].IdleDevice(), "device 1 has no task and is idle")
}

func TestBuildCache_WholeCardAndMIGMarkedAsNormal(t *testing.T) {
	nodeName := "test-node"
	gpuCount := int64(4)
	totalMem := int64(4096)
	totalCore := int64(800)

	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: nodeName},
		Status: v1.NodeStatus{
			Allocatable: v1.ResourceList{
				v1.ResourceName(utils.ResourceNvidia): *resource.NewQuantity(gpuCount, resource.DecimalSI),
				v1.ResourceName(utils.ResourceMemory): *resource.NewQuantity(totalMem, resource.DecimalSI),
				v1.ResourceName(utils.ResourceCore):   *resource.NewQuantity(totalCore, resource.DecimalSI),
				v1.ResourceName(utils.ResourceSlice):  *resource.NewQuantity(gpuCount*7, resource.DecimalSI),
			},
		},
	}

	// Device 0: whole-card task (requests nvidia.com/gpu).
	wholeCardPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "whole-card-pod",
			Namespace: "default",
			UID:       types.UID("whole-card-pod-uid"),
			Annotations: map[string]string{
				utils.AnnoDeviceIndex: "0",
			},
		},
		Spec: v1.PodSpec{
			NodeName: nodeName,
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(utils.ResourceNvidia): *resource.NewQuantity(1, resource.DecimalSI),
						},
					},
				},
			},
		},
		Status: v1.PodStatus{Phase: v1.PodRunning},
	}

	// Device 1: MIG task (requests gpu-slice with the MIG label).
	migPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mig-pod",
			Namespace: "default",
			UID:       types.UID("mig-pod-uid"),
			Labels:    map[string]string{utils.LabelsMigTask: utils.MigLabelValue},
			Annotations: map[string]string{
				utils.AnnoDeviceIndex: "1",
			},
		},
		Spec: v1.PodSpec{
			NodeName: nodeName,
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(utils.ResourceSlice): *resource.NewQuantity(1, resource.DecimalSI),
						},
					},
				},
			},
		},
		Status: v1.PodStatus{Phase: v1.PodRunning},
	}

	// Device 2: MPS task (requests gpu-slice without the MIG label).
	mpsPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mps-pod",
			Namespace: "default",
			UID:       types.UID("mps-pod-uid"),
			Annotations: map[string]string{
				utils.AnnoDeviceIndex: "2",
			},
		},
		Spec: v1.PodSpec{
			NodeName: nodeName,
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(utils.ResourceSlice): *resource.NewQuantity(1, resource.DecimalSI),
						},
					},
				},
			},
		},
		Status: v1.PodStatus{Phase: v1.PodRunning},
	}

	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(node)
	nodeInfo.AddPod(wholeCardPod)
	nodeInfo.AddPod(migPod)
	nodeInfo.AddPod(mpsPod)

	devices := BuildCache(nodeInfo)

	assert.True(t, devices[0].GetTaskMode() == utils.TaskModeNormal, "device 0 with whole-card task should be marked as normal")
	assert.True(t, devices[1].GetTaskMode() == utils.TaskModeNormal, "device 1 with MIG slice task should be marked as normal")
	assert.True(t, devices[2].GetTaskMode() == utils.TaskModeMPS, "device 2 with MPS slice task should be marked as MPS")
	assert.True(t, devices[3].GetTaskMode() == utils.TaskModeUnknown, "device 3 should be idle")
}

// TestBuildCache_NodeMigLabelReservesDevices verifies that a node labeled
// rt_mig_enable=true only accepts MIG tasks: every device rejects whole-card,
// time-sharing and MPS requests.
func TestBuildCache_NodeMigLabelReservesDevices(t *testing.T) {
	gpuCount := int64(2)
	totalMem := int64(4096)
	totalCore := int64(200)

	newNode := func(name string, labels map[string]string) *v1.Node {
		return &v1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
			Status: v1.NodeStatus{
				Allocatable: v1.ResourceList{
					v1.ResourceName(utils.ResourceNvidia): *resource.NewQuantity(gpuCount, resource.DecimalSI),
					v1.ResourceName(utils.ResourceMemory): *resource.NewQuantity(totalMem, resource.DecimalSI),
					v1.ResourceName(utils.ResourceCore):   *resource.NewQuantity(totalCore, resource.DecimalSI),
					v1.ResourceName(utils.ResourceSlice):  *resource.NewQuantity(gpuCount*7, resource.DecimalSI),
				},
			},
		}
	}

	migNode := newNode("mig-node", map[string]string{utils.LabelsMigTask: utils.MigLabelValue})
	migNodeInfo := framework.NewNodeInfo()
	migNodeInfo.SetNode(migNode)
	migDevices := BuildCache(migNodeInfo)

	assert.Len(t, migDevices, 2)
	for idx, dev := range migDevices {
		assert.True(t, dev.NodeMigOnly(), "device %d of the MIG reserved node should be marked mig only", idx)
		// MIG pods are handled by the Mig plugin, so every task mode evaluated here is rejected.
		assert.False(t, dev.CompatibleWith(utils.TaskModeNormal), "device %d must reject whole-card/time-sharing tasks", idx)
		assert.False(t, dev.CompatibleWith(utils.TaskModeMPS), "device %d must reject MPS tasks", idx)
		assert.False(t, dev.CanRunTask(utils.TaskModeNormal, 512, 2, 0), "device %d must reject time-sharing requests", idx)
		assert.False(t, dev.CanRunTask(utils.TaskModeMPS, 0, 0, 1), "device %d must reject MPS requests", idx)
		// The copy keeps the node reservation flag.
		assert.True(t, dev.DeepCopy().NodeMigOnly(), "device %d copy should keep the mig only flag", idx)
	}

	// A node without the label keeps accepting every task mode.
	normalNode := newNode("normal-node", nil)
	normalNodeInfo := framework.NewNodeInfo()
	normalNodeInfo.SetNode(normalNode)
	normalDevices := BuildCache(normalNodeInfo)

	assert.Len(t, normalDevices, 2)
	for idx, dev := range normalDevices {
		assert.False(t, dev.NodeMigOnly(), "device %d of the normal node should not be mig only", idx)
		assert.True(t, dev.CanRunTask(utils.TaskModeNormal, 512, 2, 0), "device %d should accept time-sharing requests", idx)
		assert.True(t, dev.CanRunTask(utils.TaskModeMPS, 0, 0, 1), "device %d should accept MPS requests", idx)
	}
}

// TestBuildCache_MigPodSliceAccountedByLimits verifies that outside a node reserved for
// MIG tasks, the slice request of a MIG pod is accounted by its limits, while its MIG
// usage stays visible through the annotation driven mask.
func TestBuildCache_MigPodSliceAccountedByLimits(t *testing.T) {
	nodeName := "test-node"
	gpuCount := int64(2)
	allocatableSlice := int64(14) // 7 per card for MPS
	annotationSlice := uint(4)    // 4 MIG profiles per card, reported by the annotation

	gd := GpuDevice{
		Count: int(gpuCount),
		Devices: map[int]*Device{
			0: {Minor: 0, Uuid: "gpu-0", Health: utils.DeviceHealth, Type: "gpu", MigEnable: true, Slice: annotationSlice},
			1: {Minor: 1, Uuid: "gpu-1", Health: utils.DeviceHealth, Type: "gpu", MigEnable: true, Slice: annotationSlice},
		},
	}
	anno, err := json.Marshal(gd)
	assert.NoError(t, err)

	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        nodeName,
			Annotations: map[string]string{utils.AnnotationDevice: string(anno)},
		},
		Status: v1.NodeStatus{
			Allocatable: v1.ResourceList{
				v1.ResourceName(utils.ResourceNvidia): *resource.NewQuantity(gpuCount, resource.DecimalSI),
				v1.ResourceName(utils.ResourceMemory): *resource.NewQuantity(gpuCount*1024, resource.DecimalSI),
				v1.ResourceName(utils.ResourceCore):   *resource.NewQuantity(gpuCount*200, resource.DecimalSI),
				v1.ResourceName(utils.ResourceSlice):  *resource.NewQuantity(allocatableSlice, resource.DecimalSI),
			},
		},
	}

	// Device 0: MPS pod consumes the Allocatable slice pool.
	mpsPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "mps-pod",
			Namespace:   "default",
			UID:         types.UID("mps-pod-uid"),
			Annotations: map[string]string{utils.AnnoDeviceIndex: "0"},
		},
		Spec: v1.PodSpec{
			NodeName: nodeName,
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(utils.ResourceSlice): *resource.NewQuantity(2, resource.DecimalSI),
						},
					},
				},
			},
		},
		Status: v1.PodStatus{Phase: v1.PodRunning},
	}

	// Device 1: MIG pod is accounted by its mask, not by the Allocatable slice pool.
	migPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mig-pod",
			Namespace: "default",
			UID:       types.UID("mig-pod-uid"),
			Labels:    map[string]string{utils.LabelsMigTask: utils.MigLabelValue},
			Annotations: map[string]string{
				utils.AnnoDeviceIndex: "1",
				utils.AnnoMigDevMask:  "1",
			},
		},
		Spec: v1.PodSpec{
			NodeName: nodeName,
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(utils.ResourceSlice): *resource.NewQuantity(3, resource.DecimalSI),
						},
					},
				},
			},
		},
		Status: v1.PodStatus{Phase: v1.PodRunning},
	}

	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(node)
	nodeInfo.AddPod(mpsPod)
	nodeInfo.AddPod(migPod)

	devices := BuildCache(nodeInfo)

	// The MPS pool comes from the node Allocatable: 14 / 2 = 7 slices per card.
	assert.Equal(t, int64(7), devices[0].GetTotalSlice())
	assert.Equal(t, int64(2), devices[0].GetUsedSlice())
	assert.Equal(t, int64(5), devices[0].GetIdleSlice())

	// The slices written in the limits of the MIG pod are accounted as well.
	assert.Equal(t, int64(7), devices[1].GetTotalSlice())
	assert.Equal(t, int64(3), devices[1].GetUsedSlice(), "the slices in the pod limits must be accounted")
	assert.Equal(t, int64(4), devices[1].GetIdleSlice())
	// MIG usage is visible through the annotation driven mask.
	assert.NotZero(t, devices[1].GetUsedMask(), "MIG pod usage must be tracked by the mask")
	assert.Equal(t, utils.TaskModeNormal, devices[1].GetTaskMode())
}

// TestBuildCache_TimeSharingPodConsumesSliceInLimits verifies that a time-sharing task
// requesting both core and slice is accounted by its limits, because kubelet deducts
// every resource written in the limits whatever the task type is.
func TestBuildCache_TimeSharingPodConsumesSliceInLimits(t *testing.T) {
	nodeName := "test-node"
	gpuCount := int64(2)
	allocatableSlice := int64(14) // 7 per card for MPS

	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: nodeName},
		Status: v1.NodeStatus{
			Allocatable: v1.ResourceList{
				v1.ResourceName(utils.ResourceNvidia): *resource.NewQuantity(gpuCount, resource.DecimalSI),
				v1.ResourceName(utils.ResourceMemory): *resource.NewQuantity(gpuCount*1024, resource.DecimalSI),
				v1.ResourceName(utils.ResourceCore):   *resource.NewQuantity(gpuCount*200, resource.DecimalSI),
				v1.ResourceName(utils.ResourceSlice):  *resource.NewQuantity(allocatableSlice, resource.DecimalSI),
			},
		},
	}

	// A time-sharing task: 0 < core < 100, requesting slice as well.
	tsPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "ts-pod",
			Namespace:   "default",
			UID:         types.UID("ts-pod-uid"),
			Annotations: map[string]string{utils.AnnoDeviceIndex: "0"},
		},
		Spec: v1.PodSpec{
			NodeName: nodeName,
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(utils.ResourceCore):   *resource.NewQuantity(50, resource.DecimalSI),
							v1.ResourceName(utils.ResourceMemory): *resource.NewQuantity(512, resource.DecimalSI),
							v1.ResourceName(utils.ResourceSlice):  *resource.NewQuantity(3, resource.DecimalSI),
						},
					},
				},
			},
		},
		Status: v1.PodStatus{Phase: v1.PodRunning},
	}

	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(node)
	nodeInfo.AddPod(tsPod)

	devices := BuildCache(nodeInfo)

	// Memory and core are consumed.
	idleMemory, idleCore := devices[0].GetIdleResource()
	assert.Equal(t, int64(512), idleMemory)
	assert.Equal(t, int64(150), idleCore)
	assert.Equal(t, utils.TaskModeNormal, devices[0].GetTaskMode())

	// The slice request of a time-sharing task is accounted against the slice pool.
	assert.Equal(t, int64(7), devices[0].GetTotalSlice())
	assert.Equal(t, int64(3), devices[0].GetUsedSlice(), "the slices in the pod limits must be accounted")
	assert.Equal(t, int64(4), devices[0].GetIdleSlice())
	// The device is still exclusive to its task mode, so MPS cannot share the card.
	assert.False(t, devices[0].CanRunTask(utils.TaskModeMPS, 0, 0, 1))
	// The idle card keeps the full slice capacity available for MPS.
	assert.True(t, devices[1].CanRunTask(utils.TaskModeMPS, 0, 0, 7))
}
