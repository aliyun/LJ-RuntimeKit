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
	"math"

	"github.com/lj-runtimekit/scheduler/pkg/utils"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// AvailableDevices maps device index to DeviceInfo on a single node.
type AvailableDevices map[int]*DeviceInfo

// AvailableClusterDevices maps node name to AvailableDevices across the cluster.
type AvailableClusterDevices map[string]AvailableDevices

func (c AvailableClusterDevices) Clone() framework.StateData {
	target := make(AvailableClusterDevices)
	for node, devices := range c {
		target[node] = devices.Clone()
	}
	return target
}

func (d AvailableDevices) Clone() AvailableDevices {
	target := make(AvailableDevices)
	for key, a := range d {
		target[key] = a.DeepCopy()
	}
	return target
}

// DeviceInfo holds the scheduling state for a single GPU device.
type DeviceInfo struct {
	idx             int
	scheduledBEPods map[types.UID]*v1.Pod
	usedMask        Mask
	usedVMemory     int64
	usedVCore       int64
	totalVMemory    int64
	totalVCore      int64
	topologyInfo    []DeviceP2PLink
}

// BuildCache builds the available device cache for a node from its current pod assignments.
func BuildCache(nodeInfo *framework.NodeInfo) map[int]*DeviceInfo {
	availableDevices := make(map[int]*DeviceInfo)
	vTotalMemory := nodeInfo.Allocatable.ScalarResources[v1.ResourceName(utils.ResourceMemory)]
	vTotalCore := nodeInfo.Allocatable.ScalarResources[v1.ResourceName(utils.ResourceCore)]
	gpuCount := nodeInfo.Allocatable.ScalarResources[v1.ResourceName(utils.ResourceGPU)]
	if gpuCount == 0 {
		gpuCount = nodeInfo.Allocatable.ScalarResources[v1.ResourceName(utils.ResourceNvidia)]
		if gpuCount == 0 {
			gpuCount = nodeInfo.Allocatable.ScalarResources[v1.ResourceName(utils.ResourceCount)]
		}
	}

	klog.V(5).Infof("node %s has %d gpus, %d vmem, %d vcore", nodeInfo.Node().Name, gpuCount, vTotalMemory, vTotalCore)

	// Parse device topology info from node annotation.
	var gpuDevice *GpuDevice
	node := nodeInfo.Node()
	if node != nil && node.Annotations != nil {
		if annoJSON, ok := node.Annotations[utils.AnnotationDevice]; ok && len(annoJSON) > 0 {
			parsed := new(GpuDevice)
			if err := json.Unmarshal([]byte(annoJSON), parsed); err != nil {
				klog.V(6).Infof("node %s failed to unmarshal device annotation: %v", node.Name, err)
			} else {
				gpuDevice = parsed
			}
		}
	}

	for i := 0; i < int(gpuCount); i++ {
		dev := NewDeviceInfo(i, vTotalMemory/gpuCount, vTotalCore/gpuCount)
		// Extract topology info from annotation.
		if gpuDevice != nil && gpuDevice.Devices != nil {
			if d, ok := gpuDevice.Devices[i]; ok && len(d.Topology) > 0 {
				dev.topologyInfo = make([]DeviceP2PLink, len(d.Topology))
				copy(dev.topologyInfo, d.Topology)
			}
		}
		availableDevices[i] = dev
	}
	for _, podInfo := range nodeInfo.Pods {
		if utils.NotRunning(podInfo.Pod) {
			continue
		}
		if !utils.ResponseForTask(podInfo.Pod) {
			continue
		}
		ids, err := utils.GetAnnotationsFromPodListInt(podInfo.Pod, utils.AnnoDeviceIndex)
		if err != nil {
			klog.Errorf("pod %s/%s get device ids from annotations failed due to %v", podInfo.Pod.Namespace, podInfo.Pod.Name, err)
			continue
		}

		consumeCore := utils.GetResourceReqFromPod(podInfo.Pod, utils.ResourceCore)
		consumeMemory := utils.GetResourceReqFromPod(podInfo.Pod, utils.ResourceMemory)

		// Community MIG may bind a pod to multiple cards, adapt accordingly.
		maskMap, _ := utils.GetMaskFromAnno(podInfo.Pod, ids)

		klog.V(6).Infof("pod %s/%s running in dev %v req %d, %d, %v", podInfo.Pod.Namespace, podInfo.Pod.Name, ids, consumeMemory, consumeCore, maskMap)
		for _, id := range ids {
			dev, ok := availableDevices[id]
			if !ok {
				klog.Errorf("cannot found dev %d in available cache", id)
				continue
			}
			if utils.IsNormalPodTask(podInfo.Pod) {
				dev.TrunkDevice()
				continue
			}

			dev.UsedResource(int64(consumeMemory), int64(consumeCore), uint8(maskMap[id]))
		}
	}

	return availableDevices
}

// NewDeviceInfo creates a new DeviceInfo with the given index and total resources.
func NewDeviceInfo(idx int, vmem, vcore int64) *DeviceInfo {
	dev := new(DeviceInfo)
	dev.scheduledBEPods = make(map[types.UID]*v1.Pod)
	dev.idx = idx
	dev.totalVMemory = vmem
	dev.totalVCore = vcore
	return dev
}

// TrunkDevice marks the device as fully used (whole-card assignment).
func (d *DeviceInfo) TrunkDevice() {
	d.usedVMemory = d.totalVMemory
	d.usedVCore = d.totalVCore
	return
}

// UsedResource adds the given resource usage to the device.
func (d *DeviceInfo) UsedResource(vmem, vcore int64, mask uint8) {
	d.usedVCore += vcore
	d.usedVMemory += vmem
	d.usedMask = d.usedMask.AddMask(mask)
	return
}

// IdleDevice returns true if the device has no resources in use.
func (d *DeviceInfo) IdleDevice() bool {
	return d.usedVMemory == 0 && d.usedVCore == 0 && (d.totalVCore != 0 || d.totalVMemory != 0)
}

// GetTotalResource returns the total memory and core of this device.
func (d *DeviceInfo) GetTotalResource() (int64, int64) {
	return d.totalVMemory, d.totalVCore
}

// GetIdleResource returns the available (idle) memory and core of this device.
func (d *DeviceInfo) GetIdleResource() (int64, int64) {
	return int64(math.Max(float64(d.totalVMemory-d.usedVMemory), float64(0))),
		int64(math.Max(float64(d.totalVCore-d.usedVCore), float64(0)))
}

// GetUsedMask returns the current MIG mask for this device.
func (d *DeviceInfo) GetUsedMask() Mask {
	return d.usedMask
}

// GetTopologyInfo returns the P2P topology links for this device.
func (d *DeviceInfo) GetTopologyInfo() []DeviceP2PLink {
	return d.topologyInfo
}

// DeepCopy returns a deep copy of the DeviceInfo.
func (d *DeviceInfo) DeepCopy() *DeviceInfo {
	targetDevice := new(DeviceInfo)
	targetDevice.idx = d.idx
	targetDevice.usedMask = d.usedMask
	targetDevice.usedVCore = d.usedVCore
	targetDevice.usedVMemory = d.usedVMemory
	targetDevice.totalVCore = d.totalVCore
	targetDevice.totalVMemory = d.totalVMemory
	if d.topologyInfo != nil {
		targetDevice.topologyInfo = make([]DeviceP2PLink, len(d.topologyInfo))
		copy(targetDevice.topologyInfo, d.topologyInfo)
	}
	return targetDevice
}

// GetPreFilterStateData reads the PreFilter state data from the CycleState.
func GetPreFilterStateData(state *framework.CycleState) (AvailableClusterDevices, ClusterNodeMigDevices, *StandardTaskReq, error) {
	reqData, err := state.Read(VStandardTaskReqStateKey)
	if err != nil {
		return nil, nil, nil, err
	}
	clusterData, err := state.Read(ClusterDevicesStateKey)
	if err != nil {
		return nil, nil, nil, err
	}
	clusterMigData, err := state.Read(ClusterMigDeviceStateKey)
	if err != nil {
		return nil, nil, nil, err
	}

	standardReq, ok := reqData.(StandardTaskReq)
	if !ok {
		return nil, nil, nil, fmt.Errorf("%v can not convert to standardTaskReq", reqData)
	}
	clusterDevices, ok := clusterData.(AvailableClusterDevices)
	if !ok {
		return nil, nil, nil, fmt.Errorf("%v can not convert type AvailableClusterDevices", clusterData)
	}
	clusterMigDevices, ok := clusterMigData.(ClusterNodeMigDevices)
	if !ok {
		return nil, nil, nil, fmt.Errorf("%v can not convert type ClusterMigDevices", clusterMigData)
	}

	return clusterDevices, clusterMigDevices, &standardReq, nil
}
