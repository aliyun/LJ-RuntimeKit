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

package migDevice

import (
	"testing"

	ampcache "github.com/lj-runtimekit/scheduler/pkg/cache"
	"github.com/lj-runtimekit/scheduler/pkg/utils"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func newCiTestNodeInfo(name string, gpuCount int64, vmemPerGPU, vcorePerGPU int64) *framework.NodeInfo {
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
	ni := framework.NewNodeInfo()
	ni.SetNode(node)
	return ni
}

// ===========================================================================
// Tests: NewCiPlugin / Name
// ===========================================================================

func TestNewCiPlugin(t *testing.T) {
	migDevices := &ampcache.GpuDevice{
		Devices: map[int]*ampcache.Device{
			0: {
				Minor:      0,
				Health:     utils.DeviceHealth,
				MigEnable:  true,
				Slice:      7,
				Extensions: map[string]string{utils.ResourceDriverVersion: "525"},
			},
			1: {
				Minor:      1,
				Health:     utils.DeviceHealth,
				MigEnable:  true,
				Slice:      4,
				Extensions: map[string]string{},
			},
		},
	}

	plugin := NewCiPlugin(migDevices)
	assert.NotNil(t, plugin)
	assert.Equal(t, utils.TypeMigCi, plugin.Name())
}

func TestNewCiPlugin_SkipsUnhealthy(t *testing.T) {
	migDevices := &ampcache.GpuDevice{
		Devices: map[int]*ampcache.Device{
			0: {
				Minor:      0,
				Health:     "Unhealthy",
				MigEnable:  true,
				Slice:      7,
				Extensions: map[string]string{},
			},
			1: {
				Minor:      1,
				Health:     utils.DeviceHealth,
				MigEnable:  false, // MIG not enabled
				Slice:      7,
				Extensions: map[string]string{},
			},
		},
	}

	plugin := NewCiPlugin(migDevices)
	assert.NotNil(t, plugin)
	// The underlying migDeviceCi should have empty details since both devices are filtered out
	ci := plugin.(*migDeviceCi)
	assert.Empty(t, ci.details)
}

func TestNewCiPlugin_Empty(t *testing.T) {
	migDevices := &ampcache.GpuDevice{
		Devices: make(map[int]*ampcache.Device),
	}

	plugin := NewCiPlugin(migDevices)
	assert.NotNil(t, plugin)
	assert.Equal(t, utils.TypeMigCi, plugin.Name())
}

func TestCiPlugin_Name(t *testing.T) {
	plugin := NewCiPlugin(&ampcache.GpuDevice{Devices: make(map[int]*ampcache.Device)})
	assert.Equal(t, utils.TypeMigCi, plugin.Name())
}

// ===========================================================================
// Tests: AssumePod
// ===========================================================================

func TestCiAssumePod_UnsupportedSliceSize(t *testing.T) {
	migDevices := &ampcache.GpuDevice{
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
	plugin := NewCiPlugin(migDevices)
	nodeInfo := newCiTestNodeInfo("node-1", 4, 16384, 100)
	nodeDevices := ampcache.BuildCache(nodeInfo)

	// Pod with unsupported slice size (5)
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
							v1.ResourceName(utils.ResourceSlice):  resource.MustParse("5"),
							v1.ResourceName(utils.ResourceMemory): resource.MustParse("1024"),
						},
					},
				},
			},
		},
	}

	result := plugin.AssumePod(nodeDevices, pod, nodeInfo)
	assert.False(t, result, "unsupported slice size should return false")
}

func TestCiAssumePod_NoMatchingDevice(t *testing.T) {
	// No MIG devices in details → should return false.
	migDevices := &ampcache.GpuDevice{
		Devices: make(map[int]*ampcache.Device),
	}
	plugin := NewCiPlugin(migDevices)
	nodeInfo := newCiTestNodeInfo("node-1", 4, 16384, 100)
	nodeDevices := ampcache.BuildCache(nodeInfo)

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
							v1.ResourceName(utils.ResourceMemory): resource.MustParse("1024"),
						},
					},
				},
			},
		},
	}

	result := plugin.AssumePod(nodeDevices, pod, nodeInfo)
	assert.False(t, result, "no matching device should return false")
}

func TestCiAssumePod_Success(t *testing.T) {
	migDevices := &ampcache.GpuDevice{
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
	plugin := NewCiPlugin(migDevices)
	nodeInfo := newCiTestNodeInfo("node-1", 4, 16384, 100)
	nodeDevices := ampcache.BuildCache(nodeInfo)

	// Pod requests 1 slice and 1024 memory (well within capacity)
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
							v1.ResourceName(utils.ResourceMemory): resource.MustParse("1024"),
						},
					},
				},
			},
		},
	}

	result := plugin.AssumePod(nodeDevices, pod, nodeInfo)
	assert.True(t, result, "should succeed with available MIG device")
}

func TestCiAssumePod_InsufficientMemory(t *testing.T) {
	migDevices := &ampcache.GpuDevice{
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
	plugin := NewCiPlugin(migDevices)
	// Node with very low memory per GPU (1 unit)
	nodeInfo := newCiTestNodeInfo("node-1", 4, 1, 100)
	nodeDevices := ampcache.BuildCache(nodeInfo)

	// Pod requests more memory than available per device
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
							v1.ResourceName(utils.ResourceMemory): resource.MustParse("9999"),
						},
					},
				},
			},
		},
	}

	result := plugin.AssumePod(nodeDevices, pod, nodeInfo)
	assert.False(t, result, "insufficient memory should return false")
}

func TestCiAssumePod_MultipleSliceSizes(t *testing.T) {
	tests := []struct {
		name      string
		sliceReq  string
		sliceCap  uint
		expectOk  bool
	}{
		{"slice_1_on_7ci", "1", 7, true},
		{"slice_2_on_7ci", "2", 7, true},
		{"slice_3_on_7ci", "3", 7, true},
		{"slice_4_on_7ci", "4", 7, true},
		{"slice_7_on_7ci", "7", 7, true},
		{"slice_1_on_4ci", "1", 4, true},
		{"slice_4_on_4ci", "4", 4, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			migDevices := &ampcache.GpuDevice{
				Devices: map[int]*ampcache.Device{
					0: {
						Minor:      0,
						Health:     utils.DeviceHealth,
						MigEnable:  true,
						Slice:      tt.sliceCap,
						Extensions: map[string]string{},
					},
				},
			}
			plugin := NewCiPlugin(migDevices)
			nodeInfo := newCiTestNodeInfo("node-1", 4, 16384, 100)
			nodeDevices := ampcache.BuildCache(nodeInfo)

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
									v1.ResourceName(utils.ResourceSlice):  resource.MustParse(tt.sliceReq),
									v1.ResourceName(utils.ResourceMemory): resource.MustParse("1024"),
								},
							},
						},
					},
				},
			}

			result := plugin.AssumePod(nodeDevices, pod, nodeInfo)
			assert.Equal(t, tt.expectOk, result)
		})
	}
}
