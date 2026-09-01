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
	dev.UsedResource(1024, 4, 0)
	assert.Equal(t, int64(1024), dev.usedVMemory)
	assert.Equal(t, int64(4), dev.usedVCore)
}

func TestDeviceInfo_IdleDevice(t *testing.T) {
	dev := NewDeviceInfo(0, 1024, 4)
	assert.True(t, dev.IdleDevice())

	dev.UsedResource(100, 1, 0)
	assert.False(t, dev.IdleDevice())

	// Edge case: zero total resources
	devZero := &DeviceInfo{
		totalVMemory: 0,
		totalVCore:   0,
	}
	assert.False(t, devZero.IdleDevice())
}

func TestDeviceInfo_GetTotalAndIdleResource(t *testing.T) {
	dev := NewDeviceInfo(0, 2048, 8)
	dev.UsedResource(512, 2, 0)

	totalMem, totalCore := dev.GetTotalResource()
	idleMem, idleCore := dev.GetIdleResource()

	assert.Equal(t, int64(2048), totalMem)
	assert.Equal(t, int64(8), totalCore)
	assert.Equal(t, int64(1536), idleMem)
	assert.Equal(t, int64(6), idleCore)

	// Overuse should not result in negative idle
	devOver := NewDeviceInfo(0, 100, 2)
	devOver.UsedResource(200, 5, 0)
	idleMemOver, idleCoreOver := devOver.GetIdleResource()
	assert.Equal(t, int64(0), idleMemOver)
	assert.Equal(t, int64(0), idleCoreOver)
}

func TestDeviceInfo_DeepCopy(t *testing.T) {
	original := NewDeviceInfo(2, 1024, 4)
	original.UsedResource(256, 1, 0)

	copyDev := original.DeepCopy()
	assert.Equal(t, original.idx, copyDev.idx)
	assert.Equal(t, original.totalVMemory, copyDev.totalVMemory)
	assert.Equal(t, original.usedVMemory, copyDev.usedVMemory)
	assert.Equal(t, original.totalVCore, copyDev.totalVCore)
	assert.Equal(t, original.usedVCore, copyDev.usedVCore)

	// Modify copy and ensure original is unchanged
	copyDev.usedVMemory = 512
	assert.NotEqual(t, original.usedVMemory, copyDev.usedVMemory)
}

func TestAvailableDevices_Clone(t *testing.T) {
	devices := make(AvailableDevices)
	devices[0] = NewDeviceInfo(0, 1024, 4)
	devices[0].UsedResource(256, 1, 0)

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
	cluster["node-1"][0].UsedResource(1024, 4, 0)

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
