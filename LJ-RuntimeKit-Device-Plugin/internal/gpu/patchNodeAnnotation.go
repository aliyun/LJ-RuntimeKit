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

package gpu

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/lj-runtimekit/device-plugin/internal/plugin"
	"github.com/lj-runtimekit/device-plugin/internal/utils"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

type DeviceP2PLink struct {
	PeerMinor string `json:"peerMinor"`
	Type      string `json:"type"`
}

type Device struct {
	plugin.DeviceBase
	Health     string            `json:"health"`
	Type       string            `json:"type"`
	MigEnable  bool              `json:"migEnable"`
	Slice      uint              `json:"slice"`
	Core       uint              `json:"core"`
	Memory     uint              `json:"memory"`
	Extensions map[string]string `json:"extensions"`
	Topology   []DeviceP2PLink   `json:"topology,omitempty"`
}

type GpuDevice struct {
	Count   int             `json:"count"`
	Devices map[int]*Device `json:"devices"`
}

func (dm *DeviceManager) addNodeAnnotation(devices *GpuDevice) error {

	deviceJson, err := json.Marshal(devices)
	if err != nil {
		return err
	}

	// 本轮没有产出的拓扑字段必须先置为 null：json merge patch 里省略键表示
	// 保留而不是删除，只有 null 才会把节点上的旧值去掉。置空在前、覆盖在后，
	// 于是拓扑或 label 被部分撤销时（例如只剩 LeafSwitch）旧字段也会被清掉。
	annotations := map[string]interface{}{
		utils.AnnotationDevice:   string(deviceJson),
		utils.AnnotationRdmaTopo: nil,
	}
	labels := map[string]interface{}{
		utils.NodeLabelLeafSwitchID: nil,
		utils.NodeLabelNetworkPoD:   nil,
		utils.NodeLabelHyperNodeID:  nil,
	}

	// 拓扑 annotation 与 label 的数据均来自 xdm 的 ListAndWatch 上报
	if dm.rdmaTopoAnnotation != nil {
		rdmaTopoJSON, err := json.Marshal(dm.rdmaTopoAnnotation)
		if err != nil {
			klog.Errorf("marshal rdma topo annotation failed: %v", err)
		} else {
			annotations[utils.AnnotationRdmaTopo] = string(rdmaTopoJSON)
		}
	}

	// 节点级拓扑标识（超节点 / Leaf Switch / PoD 域）以 Node Label 发布
	if dm.topoLabels != nil {
		if dm.topoLabels.LeafSwitchID != "" {
			labels[utils.NodeLabelLeafSwitchID] = dm.topoLabels.LeafSwitchID
		}
		if dm.topoLabels.PointOfDelivery != "" {
			labels[utils.NodeLabelNetworkPoD] = dm.topoLabels.PointOfDelivery
		}
		if dm.topoLabels.HyperNodeID != "" {
			labels[utils.NodeLabelHyperNodeID] = dm.topoLabels.HyperNodeID
		}
	}

	metadata := map[string]interface{}{
		"annotations": annotations,
		"labels":      labels,
	}

	return dm.patchNodeMetadata(metadata)
}

// patchNodeMetadata merges metadata into the node of this DaemonSet. The patch
// is a json merge patch, so a null value deletes the key it belongs to.
func (dm *DeviceManager) patchNodeMetadata(metadata map[string]interface{}) error {
	patchBytes, err := json.Marshal(map[string]interface{}{"metadata": metadata})
	if err != nil {
		return fmt.Errorf("marshal patch data failed: %w", err)
	}

	if err := utils.RetryOnConflictOrTooManyRequests(func() error {
		_, err := dm.k8sClient.CoreV1().Nodes().Patch(context.Background(), nodeName, types.MergePatchType, patchBytes, metav1.PatchOptions{})
		return err
	}); err != nil {
		return fmt.Errorf("failed to patch node %s: %v", nodeName, err)
	}

	return nil
}

// addDeviceAnnotation publishes the device annotation only.
func (dm *DeviceManager) addDeviceAnnotation(devices *GpuDevice) error {
	deviceJSON, err := json.Marshal(devices)
	if err != nil {
		return err
	}

	return dm.patchNodeMetadata(map[string]interface{}{
		"annotations": map[string]interface{}{
			utils.AnnotationDevice: string(deviceJSON),
		},
	})
}

// patchDeviceAnnotation republishes the device annotation after a health change.
// A health report says nothing about the topology, so the topology annotation
// and the node labels, which the topology worker owns, are left untouched: a
// transient translate failure must not wipe the published topology through a
// health event.
func (dm *DeviceManager) patchDeviceAnnotation() error {
	dm.annotationMu.Lock()
	defer dm.annotationMu.Unlock()

	devices, _ := dm.buildDeviceAnnotation()

	return dm.addDeviceAnnotation(devices)
}

// buildDeviceAnnotation turns the device cache into the device annotation view.
// It leaves the node topology target alone: callers that need it go through
// buildNodeAnnotation.
func (dm *DeviceManager) buildDeviceAnnotation() (*GpuDevice, map[int]string) {
	dm.deviceMeta.RLock()

	gpuDevices := GpuDevice{
		Devices: make(map[int]*Device),
	}
	// per device abstract topology reported by xdm, keyed by device minor
	perDeviceTopology := make(map[int]string)

	// NVML failures are merged here: the xdm snapshot keeps reporting the device
	// as healthy, so a health report written into the snapshot would not survive
	// the next refresh
	unhealthyDevices := dm.unhealthySnapshot()

	var majorDriverVersion string
	for _, d := range dm.deviceMeta.Cache {
		device := Device{
			Health:     d.Health,
			Core:       uint(d.TotalCore),
			Memory:     utils.ConvertGPUMemory(uint(d.TotalMemory)),
			Extensions: make(map[string]string),
		}

		device.UUID = d.Id

		minorStr, ok := d.ExtenderResources[utils.DeviceMinor]
		if ok {
			minor, err := strconv.Atoi(minorStr)
			if err != nil {
				klog.Errorf("failed to get device %s minor: %v", d.Id, err)
			} else {
				device.Minor = uint(minor)
				if unhealthyDevices[minor] {
					device.Health = pluginapi.Unhealthy
				}
			}
		}

		deviceType, ok := d.ExtenderResources[utils.DeviceType]
		if ok {
			device.Type = deviceType
		}

		migEnable, ok := d.ExtenderResources[utils.MigEnableKey]
		if ok && migEnable == utils.MigEnableValue {
			device.MigEnable = true
			sliceStr, ok := d.ExtenderResources[utils.MigSliceKey]
			if ok {
				slice, err := strconv.Atoi(sliceStr)
				if err != nil {
					klog.Errorf("failed to get device %s slice: %v", d.Id, err)
				} else {
					device.Slice = uint(slice)
				}
			}
		}

		if len(majorDriverVersion) == 0 {
			driverVersion, ok := d.ExtenderResources[utils.DriverVersion]
			if ok {
				majorDriverVersion = driverVersion
			}
		}

		device.Extensions[utils.DriverVersion] = majorDriverVersion

		if abstractTopo, ok := d.ExtenderResources[utils.TopologyKey]; ok && abstractTopo != "" {
			perDeviceTopology[int(device.Minor)] = abstractTopo
		}

		gpuDevices.Devices[int(device.Minor)] = &device
	}
	dm.deviceMeta.RUnlock()

	gpuDevices.Count = len(gpuDevices.Devices)

	dm.annotationDevices = &gpuDevices

	return &gpuDevices, perDeviceTopology
}

// buildNodeAnnotation reads the device cache and recomputes the device
// annotation plus the node topology target. The bool reports whether the
// translated snapshot really carries an intra node pcie switch tree, which is
// what the rdma resource needs: a snapshot with only interconnect levels or node
// identifiers has no pcie switch to mount rdma devices behind, so it must keep
// the resource withdrawn. A translate error returns no usable topology either,
// so a broken snapshot withdraws instead of republishing the previous one.
func (dm *DeviceManager) buildNodeAnnotation() (*GpuDevice, bool, error) {
	devices, perDeviceTopology := dm.buildDeviceAnnotation()

	// Translate the abstract topology xdm reports per device into node
	// annotations and labels.
	nodeTopo, err := TranslateTopology(perDeviceTopology)
	if err != nil {
		// the snapshot cannot be translated: drop the cached topology so the
		// node fields are cleaned up instead of republishing the stale one
		dm.rdmaTopoAnnotation = nil
		dm.topoLabels = nil

		return devices, false, fmt.Errorf("translate xdm topology failed: %w", err)
	}

	dm.rdmaTopoAnnotation = nodeTopo.RdmaTopo
	dm.topoLabels = nodeTopo.Labels

	// icnlink groups ride along in the aliyun.com/device annotation
	for minor, links := range nodeTopo.P2PLinks {
		if device, ok := devices.Devices[minor]; ok {
			device.Topology = links
		}
	}

	return devices, nodeTopo.RdmaTopo != nil, nil
}

// patchNodeAnnotation publishes the device cache, the topology annotation and the
// node labels in one shot. It is the startup path when TopoWatch is disabled; the
// health check publishes the device annotation alone, see patchDeviceAnnotation.
func (dm *DeviceManager) patchNodeAnnotation() error {
	dm.annotationMu.Lock()
	defer dm.annotationMu.Unlock()

	// a translate error still yields a device annotation with the topology
	// fields cleared, so the node converges on the withdrawn state
	devices, _, buildErr := dm.buildNodeAnnotation()
	if buildErr != nil {
		klog.Errorf("build node annotation: %v", buildErr)
	}

	if err := dm.addNodeAnnotation(devices); err != nil {
		return fmt.Errorf("failed to add node annotation: %w", err)
	}

	return nil
}

// nodeSignature fingerprints the node fields this component owns, so a
// snapshot that changes nothing does not cause another patch.
func (dm *DeviceManager) nodeSignature(devices *GpuDevice) string {
	deviceJSON, err := json.Marshal(devices)
	if err != nil {
		klog.Errorf("marshal device annotation for signature failed: %v", err)
		return ""
	}

	topoJSON, _ := json.Marshal(dm.rdmaTopoAnnotation)
	labelsJSON, _ := json.Marshal(dm.topoLabels)

	return string(deviceJSON) + "|" + string(topoJSON) + "|" + string(labelsJSON)
}
