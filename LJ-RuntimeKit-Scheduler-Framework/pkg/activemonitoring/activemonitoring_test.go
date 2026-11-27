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

package activemonitoring

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

// newState builds a CycleState carrying a device cache for node-1 where minor 0
// is idle and minor 1 is fully occupied (whole card).
func newState(t *testing.T) *framework.CycleState {
	t.Helper()
	idle := ampcache.NewDeviceInfo(0, 2048, 8)
	busy := ampcache.NewDeviceInfo(1, 2048, 8)
	busy.TrunkDevice()
	devs := ampcache.AvailableDevices{0: idle, 1: busy}

	state := framework.NewCycleState()
	state.Write(ampcache.ClusterDevicesStateKey, ampcache.AvailableClusterDevices{"node-1": devs})
	state.Write(ampcache.ClusterMigDeviceStateKey, ampcache.ClusterNodeMigDevices{
		"node-1": {Devices: make(map[int]*ampcache.Device)},
	})
	state.Write(ampcache.VStandardTaskReqStateKey, ampcache.StandardTaskReq{})
	return state
}

func newNodeInfo(name string) *framework.NodeInfo {
	ni := framework.NewNodeInfo()
	ni.SetNode(&v1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}})
	return ni
}

func testPodObj(minor string, isTest bool) *v1.Pod {
	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default"}}
	if isTest {
		pod.Labels = map[string]string{utils.LabelActiveMonitoring: "true"}
	}
	if minor != "" {
		pod.Annotations = map[string]string{utils.AnnoActiveMonitoringTargetMinor: minor}
	}
	return pod
}

func TestNewAndName(t *testing.T) {
	plugin, err := New(&runtime.Unknown{}, nil)
	assert.NoError(t, err)
	assert.NotNil(t, plugin)
	assert.Equal(t, Name, plugin.Name())
	assert.Equal(t, "ActiveMonitoring", Name)
}

func TestFilter_BusinessPodPassthrough(t *testing.T) {
	p := &Plugin{}
	status := p.Filter(context.Background(), newState(t), testPodObj("", false), newNodeInfo("node-1"))
	assert.True(t, status.IsSuccess())
	assert.Contains(t, status.Message(), "not a activemonitoring pod")
}

func TestFilter_TargetMinorIdle_Success(t *testing.T) {
	p := &Plugin{}
	status := p.Filter(context.Background(), newState(t), testPodObj("0", true), newNodeInfo("node-1"))
	assert.True(t, status.IsSuccess())
}

func TestFilter_TargetMinorBusy_Unschedulable(t *testing.T) {
	p := &Plugin{}
	status := p.Filter(context.Background(), newState(t), testPodObj("1", true), newNodeInfo("node-1"))
	assert.True(t, status.IsUnschedulable())
	assert.Contains(t, status.Message(), "busy")
}

func TestFilter_MinorNotFound_Unschedulable(t *testing.T) {
	p := &Plugin{}
	status := p.Filter(context.Background(), newState(t), testPodObj("7", true), newNodeInfo("node-1"))
	assert.True(t, status.IsUnschedulable())
	assert.Contains(t, status.Message(), "not found")
}

func TestFilter_MissingAnnotation_Unschedulable(t *testing.T) {
	p := &Plugin{}
	status := p.Filter(context.Background(), newState(t), testPodObj("", true), newNodeInfo("node-1"))
	assert.True(t, status.IsUnschedulable())
	assert.Contains(t, status.Message(), "missing target minor annotation")
}

func TestFilter_NodeNotInCache_Unschedulable(t *testing.T) {
	p := &Plugin{}
	status := p.Filter(context.Background(), newState(t), testPodObj("0", true), newNodeInfo("other-node"))
	assert.True(t, status.IsUnschedulable())
	assert.Contains(t, status.Message(), "not in cache")
}

// TestFilter_TargetMinorHasTestPod_Unschedulable verifies a detection Pod is rejected when
// its target minor is already pinned by another detection Pod (hasTestPod), even though the
// card reads as idle because detection Pods consume no GPU resource.
func TestFilter_TargetMinorHasTestPod_Unschedulable(t *testing.T) {
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
		Status: v1.NodeStatus{
			Allocatable: v1.ResourceList{
				v1.ResourceName(utils.ResourceGPU):    *resource.NewQuantity(2, resource.DecimalSI),
				v1.ResourceName(utils.ResourceMemory): *resource.NewQuantity(4096, resource.DecimalSI),
				v1.ResourceName(utils.ResourceCore):   *resource.NewQuantity(8, resource.DecimalSI),
			},
		},
	}
	// An existing running detection Pod pinned to minor 0 flags it via hasTestPod.
	existing := testPodObj("0", true)
	existing.Spec.NodeName = "node-1"
	existing.Status.Phase = v1.PodRunning
	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(node)
	nodeInfo.AddPod(existing)

	state := framework.NewCycleState()
	state.Write(ampcache.ClusterDevicesStateKey, ampcache.AvailableClusterDevices{"node-1": ampcache.BuildCache(nodeInfo)})
	state.Write(ampcache.ClusterMigDeviceStateKey, ampcache.ClusterNodeMigDevices{
		"node-1": {Devices: make(map[int]*ampcache.Device)},
	})
	state.Write(ampcache.VStandardTaskReqStateKey, ampcache.StandardTaskReq{})

	// A second detection Pod targeting the same minor 0 must be rejected.
	p := &Plugin{}
	status := p.Filter(context.Background(), state, testPodObj("0", true), newNodeInfo("node-1"))
	assert.True(t, status.IsUnschedulable())
	assert.Contains(t, status.Message(), "another detection pod")
}

func TestScoreForTestPodCount(t *testing.T) {
	assert.Equal(t, int64(framework.MaxNodeScore), scoreForTestPodCount(0))
	assert.Equal(t, int64(framework.MaxNodeScore)-penaltyPerTestPod, scoreForTestPodCount(1))
	// Floored at 0 once penalties exceed MaxNodeScore.
	assert.Equal(t, int64(0), scoreForTestPodCount(1000))
}
