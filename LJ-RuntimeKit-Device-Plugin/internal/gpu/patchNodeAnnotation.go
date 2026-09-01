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

	annotations := map[string]interface{}{
		utils.AnnotationDevice: string(deviceJson),
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

	patchData := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": annotations,
		},
	}

	// 节点级拓扑标识（超节点 / Leaf Switch / PoD 域）以 Node Label 发布
	if !dm.topoLabels.IsEmpty() {
		labels := map[string]interface{}{}
		if dm.topoLabels.LeafSwitchID != "" {
			labels[utils.NodeLabelLeafSwitchID] = dm.topoLabels.LeafSwitchID
		}
		if dm.topoLabels.PointOfDelivery != "" {
			labels[utils.NodeLabelNetworkPoD] = dm.topoLabels.PointOfDelivery
		}
		if dm.topoLabels.HyperNodeID != "" {
			labels[utils.NodeLabelHyperNodeID] = dm.topoLabels.HyperNodeID
		}
		if len(labels) > 0 {
			patchData["metadata"].(map[string]interface{})["labels"] = labels
		}
	}

	patchBytes, err := json.Marshal(patchData)
	if err != nil {
		return fmt.Errorf("marshal patch data failed: %w", err)
	}

	if err := utils.RetryOnConflictOrTooManyRequests(func() error {
		var applyErr error
		_, applyErr = dm.k8sClient.CoreV1().Nodes().Patch(
			context.Background(),
			nodeName,
			types.StrategicMergePatchType,
			patchBytes,
			metav1.PatchOptions{},
		)
		return applyErr
	}); err != nil {
		return fmt.Errorf("failed to patch node %s: %v", nodeName, err)
	}

	return nil
}

func (dm *DeviceManager) patchNodeAnnotation() error {
	dm.deviceMeta.RLock()

	gpuDevices := GpuDevice{
		Devices: make(map[int]*Device),
	}
	// per device abstract topology reported by xdm, keyed by device minor
	perDeviceTopology := make(map[int]string)

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

	// Translate the abstract topology xdm reports per device into node
	// annotations and labels.
	nodeTopo, err := TranslateTopology(perDeviceTopology)
	if err != nil {
		klog.Errorf("translate xdm topology failed, node topo annotation and labels are skipped: %v", err)
	} else {
		dm.rdmaTopoAnnotation = nodeTopo.RdmaTopo
		dm.topoLabels = nodeTopo.Labels
		// icnlink groups ride along in the aliyun.com/device annotation
		for minor, links := range nodeTopo.P2PLinks {
			if device, ok := gpuDevices.Devices[minor]; ok {
				device.Topology = links
			}
		}
	}

	dm.annotationDevices = &gpuDevices

	if err := dm.addNodeAnnotation(&gpuDevices); err != nil {
		return fmt.Errorf("failed to add node annotation: %w", err)
	}

	return nil
}
