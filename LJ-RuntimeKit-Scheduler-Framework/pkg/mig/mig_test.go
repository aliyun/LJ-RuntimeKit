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

package mig

import (
	"context"
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

func newTestNode(name string, gpuCount int64, vmemPerGPU, vcorePerGPU int64) *v1.Node {
	totalMem := resource.NewQuantity(gpuCount*vmemPerGPU, resource.DecimalSI)
	totalCore := resource.NewQuantity(gpuCount*vcorePerGPU, resource.DecimalSI)
	totalGpu := resource.NewQuantity(gpuCount, resource.DecimalSI)

	return &v1.Node{
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
}

func newTestNodeInfo(name string, gpuCount int64, vmemPerGPU, vcorePerGPU int64) *framework.NodeInfo {
	ni := framework.NewNodeInfo()
	ni.SetNode(newTestNode(name, gpuCount, vmemPerGPU, vcorePerGPU))
	return ni
}

func newMigCycleState(nodeName string, devices ampcache.AvailableDevices, migDevices ampcache.ClusterNodeMigDevices, req ampcache.StandardTaskReq) *framework.CycleState {
	state := framework.NewCycleState()
	clusterDevices := make(ampcache.AvailableClusterDevices)
	clusterDevices[nodeName] = devices
	state.Write(ampcache.ClusterDevicesStateKey, clusterDevices)
	state.Write(ampcache.ClusterMigDeviceStateKey, migDevices)
	state.Write(ampcache.VStandardTaskReqStateKey, req)
	return state
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
	p := &MigPlugin{}
	assert.Equal(t, "Mig", p.Name())
}

// ===========================================================================
// Tests: Filter
// ===========================================================================

func TestFilter_PodNotMigTask_Success(t *testing.T) {
	// Pod without MIG resources should pass immediately.
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "normal-pod",
			Namespace: "default",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("1"),
						},
					},
				},
			},
		},
	}

	nodeInfo := newTestNodeInfo("node-1", 4, 16384, 100)
	devices := ampcache.BuildCache(nodeInfo)
	migDevices := make(ampcache.ClusterNodeMigDevices)
	migDevices["node-1"] = &ampcache.GpuDevice{Devices: make(map[int]*ampcache.Device)}
	state := newMigCycleState("node-1", devices, migDevices, ampcache.StandardTaskReq{})

	plugin := &MigPlugin{}
	status := plugin.Filter(context.Background(), state, pod, nodeInfo)
	assert.True(t, status.IsSuccess())
	assert.Contains(t, status.Message(), "not mig task")
}

func TestFilter_MigPodTotalCard_Success(t *testing.T) {
	// Pod requests MIG slice but also has NormalGpuReqCnt > 0 → treated as total card task.
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "total-card-pod",
			Namespace: "default",
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

	nodeInfo := newTestNodeInfo("node-1", 4, 16384, 100)
	devices := ampcache.BuildCache(nodeInfo)
	migDevices := make(ampcache.ClusterNodeMigDevices)
	migDevices["node-1"] = &ampcache.GpuDevice{Devices: make(map[int]*ampcache.Device)}
	req := ampcache.StandardTaskReq{NormalGpuReqCnt: 1}
	state := newMigCycleState("node-1", devices, migDevices, req)

	plugin := &MigPlugin{}
	status := plugin.Filter(context.Background(), state, pod, nodeInfo)
	assert.True(t, status.IsSuccess())
	assert.Contains(t, status.Message(), "total card task")
}

func TestFilter_GiPod_NoMigDevice_Unschedulable(t *testing.T) {
	// GI pod (slice only, no memory) with no MIG device on node → Unschedulable.
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gi-pod",
			Namespace: "default",
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

	nodeInfo := newTestNodeInfo("node-1", 4, 16384, 100)
	devices := ampcache.BuildCache(nodeInfo)
	// No MIG devices on node
	migDevices := make(ampcache.ClusterNodeMigDevices)
	migDevices["node-1"] = &ampcache.GpuDevice{Devices: make(map[int]*ampcache.Device)}
	req := ampcache.StandardTaskReq{NormalGpuReqCnt: 0, Vslice: 1}
	state := newMigCycleState("node-1", devices, migDevices, req)

	plugin := &MigPlugin{}
	status := plugin.Filter(context.Background(), state, pod, nodeInfo)
	// GI mode: no MIG devices found → assume fails → Unschedulable
	assert.Equal(t, framework.Unschedulable, status.Code())
}

func TestFilter_GiPod_WithMigDevice_Success(t *testing.T) {
	// GI pod with available MIG device → Success.
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gi-pod",
			Namespace: "default",
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

	nodeInfo := newTestNodeInfo("node-1", 4, 16384, 100)
	devices := ampcache.BuildCache(nodeInfo)
	// Add a MIG device on the node with 7 slices
	migDevices := make(ampcache.ClusterNodeMigDevices)
	migDevices["node-1"] = &ampcache.GpuDevice{
		Devices: map[int]*ampcache.Device{
			0: {
				Minor:      0,
				Health:     utils.DeviceHealth,
				MigEnable:  true,
				Slice:      7,
				Extensions: map[string]string{},
			},
		},
	}
	req := ampcache.StandardTaskReq{NormalGpuReqCnt: 0, Vslice: 1}
	state := newMigCycleState("node-1", devices, migDevices, req)

	plugin := &MigPlugin{}
	status := plugin.Filter(context.Background(), state, pod, nodeInfo)
	assert.True(t, status.IsSuccess())
}

func TestFilter_CiPod_WithMigDevice_Success(t *testing.T) {
	// CI pod (requests both slice and memory) with available MIG device → Success.
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ci-pod",
			Namespace: "default",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceName(utils.ResourceSlice):  resource.MustParse("1"),
							v1.ResourceName(utils.ResourceMemory): resource.MustParse("2048"),
						},
					},
				},
			},
		},
	}

	nodeInfo := newTestNodeInfo("node-1", 4, 16384, 100)
	devices := ampcache.BuildCache(nodeInfo)
	// Add a MIG device on the node with 7 slices
	migDevices := make(ampcache.ClusterNodeMigDevices)
	migDevices["node-1"] = &ampcache.GpuDevice{
		Devices: map[int]*ampcache.Device{
			0: {
				Minor:      0,
				Health:     utils.DeviceHealth,
				MigEnable:  true,
				Slice:      7,
				Extensions: map[string]string{},
			},
		},
	}
	req := ampcache.StandardTaskReq{NormalGpuReqCnt: 0, Vslice: 1}
	state := newMigCycleState("node-1", devices, migDevices, req)

	plugin := &MigPlugin{}
	status := plugin.Filter(context.Background(), state, pod, nodeInfo)
	assert.True(t, status.IsSuccess())
}

// ===========================================================================
// Tests: Reserve
// ===========================================================================

// NOTE: Reserve depends on framework.Handle.ClientSet() to patch pod annotations.
// Full testing requires a fake kubernetes clientset or integration test environment.
// The following tests only cover the early-exit paths.

func TestReserve_PodNotMigTask_Success(t *testing.T) {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "normal-pod",
			Namespace: "default",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("1"),
						},
					},
				},
			},
		},
	}

	state := framework.NewCycleState()
	plugin := &MigPlugin{}
	status := plugin.Reserve(context.Background(), state, pod, "node-1")
	assert.True(t, status.IsSuccess())
	assert.Contains(t, status.Message(), "not mig task")
}

func TestReserve_TotalCardTask_Success(t *testing.T) {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "total-card-pod",
			Namespace: "default",
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

	nodeInfo := newTestNodeInfo("node-1", 4, 16384, 100)
	devices := ampcache.BuildCache(nodeInfo)
	migDevices := make(ampcache.ClusterNodeMigDevices)
	migDevices["node-1"] = &ampcache.GpuDevice{Devices: make(map[int]*ampcache.Device)}
	req := ampcache.StandardTaskReq{NormalGpuReqCnt: 1}
	state := newMigCycleState("node-1", devices, migDevices, req)

	plugin := &MigPlugin{}
	status := plugin.Reserve(context.Background(), state, pod, "node-1")
	assert.True(t, status.IsSuccess())
	assert.Contains(t, status.Message(), "total card task")
}

// ===========================================================================
// Tests: Unreserve
// ===========================================================================

func TestUnreserve_NoOp(t *testing.T) {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
		},
	}
	state := framework.NewCycleState()
	plugin := &MigPlugin{}
	// Unreserve is a no-op, just verify it doesn't panic.
	plugin.Unreserve(context.Background(), state, pod, "node-1")
}
