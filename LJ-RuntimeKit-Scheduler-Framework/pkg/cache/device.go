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
	usedVSlice      int64
	totalVMemory    int64
	totalVCore      int64
	totalVSlice     int64
	topologyInfo    []DeviceP2PLink
	taskMode        utils.TaskMode // scheduling task mode based on running pods
	// nodeMigOnly is true when the node carries the label rt_mig_enable=true.
	// Such a node is reserved for MIG tasks, which are scheduled by the Mig plugin,
	// so the device rejects every task mode evaluated here.
	nodeMigOnly bool
	hasTestPod  bool
}

// BuildCache builds the available device cache for a node from its current pod assignments.
func BuildCache(nodeInfo *framework.NodeInfo) map[int]*DeviceInfo {
	availableDevices := make(map[int]*DeviceInfo)
	vTotalMemory := nodeInfo.Allocatable.ScalarResources[v1.ResourceName(utils.ResourceMemory)]
	vTotalCore := nodeInfo.Allocatable.ScalarResources[v1.ResourceName(utils.ResourceCore)]
	vTotalSlice := nodeInfo.Allocatable.ScalarResources[v1.ResourceName(utils.ResourceSlice)]
	gpuCount := nodeInfo.Allocatable.ScalarResources[v1.ResourceName(utils.ResourceGPU)]
	if gpuCount == 0 {
		gpuCount = nodeInfo.Allocatable.ScalarResources[v1.ResourceName(utils.ResourceNvidia)]
		if gpuCount == 0 {
			gpuCount = nodeInfo.Allocatable.ScalarResources[v1.ResourceName(utils.ResourceCount)]
		}
	}

	klog.V(5).Infof("node %s has %d gpus, %d vmem, %d vcore, %d vslice", nodeInfo.Node().Name, gpuCount, vTotalMemory, vTotalCore, vTotalSlice)

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

	var slicePerGPU int64
	// A node labeled rt_mig_enable=true is reserved for MIG tasks: every device is
	// marked as MIG only, so whole-card, time-sharing and MPS pods are filtered out.
	migOnly := utils.NodeSupportsMig(node)
	if migOnly {
		klog.V(5).Infof("node %s carries %s=%s, only MIG tasks are allowed on its devices",
			node.Name, utils.LabelsMigTask, utils.MigLabelValue)
	} else {
		// Slices exported by the node Allocatable are split evenly across its cards.
		// This per-card capacity is used by MPS tasks only; MIG tasks are sized by the
		// slice capacity reported in the node device annotation.
		if gpuCount > 0 {
			slicePerGPU = vTotalSlice / gpuCount
		}
	}

	for i := 0; i < int(gpuCount); i++ {
		dev := NewDeviceInfo(i, vTotalMemory/gpuCount, vTotalCore/gpuCount)
		if !migOnly {
			dev.SetTotalSlice(slicePerGPU)
		}

		dev.SetNodeMigOnly(migOnly)
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
		if utils.IsActiveMonitoringPod(podInfo.Pod) {
			if minor, mErr := utils.GetActiveMonitoringTargetMinor(podInfo.Pod); mErr == nil {
				if dev, ok := availableDevices[minor]; ok {
					dev.hasTestPod = true
				}
			}
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
		consumeSlice := utils.GetResourceReqFromPod(podInfo.Pod, utils.ResourceSlice)

		// Community MIG may bind a pod to multiple cards, adapt accordingly.
		maskMap, _ := utils.GetMaskFromAnno(podInfo.Pod, ids)

		klog.V(6).Infof("pod %s/%s running in dev %v req %d, %d, %d, %v", podInfo.Pod.Namespace, podInfo.Pod.Name, ids, consumeMemory, consumeCore, consumeSlice, maskMap)
		for _, id := range ids {
			dev, ok := availableDevices[id]
			if !ok {
				klog.Errorf("cannot found dev %d in available cache", id)
				continue
			}
			if utils.IsNormalPodTask(podInfo.Pod) {
				dev.TrunkDevice()
				dev.SetTaskMode(utils.TaskModeNormal)
				continue
			}

			dev.SetTaskMode(utils.GetPodTaskMode(podInfo.Pod))

			if migOnly {
				dev.UsedResource(int64(consumeMemory), int64(consumeCore), 0, uint8(maskMap[id]))
			} else {
				dev.UsedResource(int64(consumeMemory), int64(consumeCore), int64(consumeSlice), uint8(maskMap[id]))
			}

		}
	}

	return availableDevices
}

// NewDeviceInfo creates a new DeviceInfo with the given index and total resources.
// The slice capacity is set separately through SetTotalSlice.
func NewDeviceInfo(idx int, vmem, vcore int64) *DeviceInfo {
	dev := new(DeviceInfo)
	dev.scheduledBEPods = make(map[types.UID]*v1.Pod)
	dev.idx = idx
	dev.totalVMemory = vmem
	dev.totalVCore = vcore
	return dev
}

// SetTotalSlice sets the per-card MPS slice capacity, taken from the node Allocatable.
func (d *DeviceInfo) SetTotalSlice(vslice int64) {
	d.totalVSlice = vslice
}

// TrunkDevice marks the device as fully used (whole-card assignment).
func (d *DeviceInfo) TrunkDevice() {
	d.usedVMemory = d.totalVMemory
	d.usedVCore = d.totalVCore
	d.usedVSlice = d.totalVSlice
	return
}

// UsedResource adds the given resource usage to the device.
func (d *DeviceInfo) UsedResource(vmem, vcore, vslice int64, mask uint8) {
	d.usedVCore += vcore
	d.usedVMemory += vmem
	d.usedVSlice += vslice
	d.usedMask = d.usedMask.AddMask(mask)
	return
}

// IdleDevice returns true if the device has no resources in use.
func (d *DeviceInfo) IdleDevice() bool {
	return d.usedVMemory == 0 && d.usedVCore == 0 && d.usedVSlice == 0 &&
		(d.totalVCore != 0 || d.totalVMemory != 0)
}

// SetTaskMode sets the device task mode if it is currently unknown.
// The first scheduled virtual task determines the device mode.
// If a different mode is detected later, a warning is logged to help diagnose
// inconsistent pod assignments.
func (d *DeviceInfo) SetTaskMode(mode utils.TaskMode) {
	if d.taskMode == utils.TaskModeUnknown {
		d.taskMode = mode
		return
	}
	if d.taskMode != mode {
		klog.Warningf("device %d task mode conflict: existing %v, new %v", d.idx, d.taskMode, mode)
	}
}

// GetTaskMode returns the current scheduling task mode of the device.
func (d *DeviceInfo) GetTaskMode() utils.TaskMode {
	return d.taskMode
}

// SetNodeMigOnly marks whether the owning node is reserved for MIG tasks
// (node label rt_mig_enable=true).
func (d *DeviceInfo) SetNodeMigOnly(migOnly bool) {
	d.nodeMigOnly = migOnly
}

// NodeMigOnly returns true if the owning node is reserved for MIG tasks.
func (d *DeviceInfo) NodeMigOnly() bool {
	return d.nodeMigOnly
}

// CompatibleWith reports whether the device can run a pod with the given task mode.
// MPS tasks cannot share a GPU with normal tasks (whole-card, virtual, or MIG).
// A device on a MIG reserved node rejects every mode, because MIG pods are
// scheduled by the Mig plugin and never reach this check.
func (d *DeviceInfo) CompatibleWith(mode utils.TaskMode) bool {
	if d.nodeMigOnly {
		return false
	}
	if d.taskMode == utils.TaskModeUnknown {
		return true
	}
	return d.taskMode == mode
}

// HasTestPod returns true if a ActiveMonitoring Pod uses the device (a card-level preference hint).
func (d *DeviceInfo) HasTestPod() bool {
	return d.hasTestPod
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

// GetTotalSlice returns the total slice capacity of this device.
func (d *DeviceInfo) GetTotalSlice() int64 {
	return d.totalVSlice
}

// GetUsedSlice returns the number of slices already in use on this device.
// Only MPS tasks consume these slices: MIG tasks are tracked by the annotation
// driven mask, and time-sharing tasks are validated by memory and core only.
func (d *DeviceInfo) GetUsedSlice() int64 {
	return d.usedVSlice
}

// GetIdleSlice returns the available (idle) slices of this device.
func (d *DeviceInfo) GetIdleSlice() int64 {
	return int64(math.Max(float64(d.totalVSlice-d.usedVSlice), float64(0)))
}

// CanRunTask reports whether the device can host a pod requesting vmem, vcore and
// vslice in the given task mode. MPS tasks are limited by the slices reported by the
// node Allocatable plus optional memory, while time-sharing and whole-card tasks are
// limited by memory and core. MIG tasks are sized from the node annotation by the Mig
// plugin and verified with the device mask.
// A device already running the other task mode is always rejected.
func (d *DeviceInfo) CanRunTask(mode utils.TaskMode, vmem, vcore, vslice int64) bool {
	if !d.CompatibleWith(mode) {
		return false
	}

	idleVMemory, idleVCore := d.GetIdleResource()
	totalVMemory, totalVCore := d.GetTotalResource()
	if mode == utils.TaskModeMPS {
		if vslice > 0 && (d.GetIdleSlice() < vslice || d.totalVSlice < vslice) {
			return false
		}
		return idleVMemory >= vmem && totalVMemory >= vmem
	}

	return idleVMemory >= vmem && idleVCore >= vcore &&
		totalVMemory >= vmem && totalVCore >= vcore
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
	targetDevice.usedVSlice = d.usedVSlice
	targetDevice.totalVCore = d.totalVCore
	targetDevice.totalVMemory = d.totalVMemory
	targetDevice.totalVSlice = d.totalVSlice
	targetDevice.taskMode = d.taskMode
	targetDevice.nodeMigOnly = d.nodeMigOnly
	targetDevice.hasTestPod = d.hasTestPod
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
