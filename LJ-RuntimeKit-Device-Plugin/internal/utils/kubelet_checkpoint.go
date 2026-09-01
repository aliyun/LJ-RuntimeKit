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

package utils

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"k8s.io/klog/v2"
)

// KubeletCheckpointPath is kubelet's internal device manager checkpoint.
// Declared as var so tests can override it.
var KubeletCheckpointPath = "/var/lib/kubelet/device-plugins/kubelet_internal_checkpoint"

// KubeletPodDeviceEntry is the unified view of one kubelet checkpoint entry,
// covering flat (k8s 1.13 ~ 1.19) and NUMA keyed (k8s >= 1.20) DeviceIDs.
// PodName/PodNamespace are resolved separately by PodUID.
type KubeletPodDeviceEntry struct {
	PodUID        string   `json:"PodUID"`
	PodNamespace  string   `json:"PodNamespace,omitempty"`
	PodName       string   `json:"PodName,omitempty"`
	ContainerName string   `json:"ContainerName"`
	ResourceName  string   `json:"ResourceName"`
	DeviceIDs     []string `json:"DeviceIDs"`
}

// kubeletPodDeviceEntryFlat mirrors checkpoint.PodDevicesEntry of kubelet
// 1.13 ~ 1.19, where DeviceIDs is a flat list.
type kubeletPodDeviceEntryFlat struct {
	PodUID        string   `json:"PodUID"`
	ContainerName string   `json:"ContainerName"`
	ResourceName  string   `json:"ResourceName"`
	DeviceIDs     []string `json:"DeviceIDs"`
}

// kubeletPodDeviceEntryNUMA mirrors checkpoint.PodDevicesEntry of kubelet
// >= 1.20, where DeviceIDs is keyed by NUMA node id.
type kubeletPodDeviceEntryNUMA struct {
	PodUID        string             `json:"PodUID"`
	ContainerName string             `json:"ContainerName"`
	ResourceName  string             `json:"ResourceName"`
	DeviceIDs     map[int64][]string `json:"DeviceIDs"`
}

type kubeletCheckpointRawData struct {
	Data struct {
		PodDeviceEntries []json.RawMessage `json:"PodDeviceEntries"`
	} `json:"Data"`
	Checksum uint64 `json:"Checksum"`
}

// FindPodInKubeletCheckpoint returns the checkpoint entry owning the given
// device ids. The last matching entry wins; both DeviceIDs layouts are
// supported.
func FindPodInKubeletCheckpoint(resourceName string, deviceIDs []string) (*KubeletPodDeviceEntry, error) {
	content, err := os.ReadFile(KubeletCheckpointPath)
	if err != nil {
		return nil, fmt.Errorf("read kubelet checkpoint file failed: %w", err)
	}

	var data kubeletCheckpointRawData
	if err := json.Unmarshal(content, &data); err != nil {
		return nil, fmt.Errorf("unmarshal kubelet checkpoint file failed: %w", err)
	}

	var matched *KubeletPodDeviceEntry
	for _, raw := range data.Data.PodDeviceEntries {
		entry, ok, err := parseKubeletCheckpointEntry(raw, resourceName)
		if err != nil {
			klog.Warningf("parse kubelet checkpoint entry %s failed: %v", string(raw), err)
			continue
		}
		if !ok {
			continue
		}
		if !containsAll(entry.DeviceIDs, deviceIDs) {
			continue
		}
		matched = entry
	}
	if matched == nil {
		return nil, fmt.Errorf("no kubelet checkpoint entry matched resource %s with device ids %v", resourceName, deviceIDs)
	}
	return matched, nil
}

// parseKubeletCheckpointEntry parses one raw entry and flattens its device
// ids. ok is false for entries of another resource. Array DeviceIDs are flat,
// object DeviceIDs are NUMA keyed.
func parseKubeletCheckpointEntry(raw json.RawMessage, resourceName string) (*KubeletPodDeviceEntry, bool, error) {
	var probe struct {
		ResourceName string          `json:"ResourceName"`
		DeviceIDs    json.RawMessage `json:"DeviceIDs"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil, false, fmt.Errorf("probe kubelet checkpoint entry failed: %w", err)
	}
	// an empty resourceName matches entries of any resource
	if resourceName != "" && probe.ResourceName != resourceName {
		return nil, false, nil
	}

	trimmed := strings.TrimSpace(string(probe.DeviceIDs))
	if strings.HasPrefix(trimmed, "{") {
		// NUMA layout (k8s >= 1.20): devices keyed by NUMA node id
		var numa kubeletPodDeviceEntryNUMA
		if err := json.Unmarshal(raw, &numa); err != nil {
			return nil, false, fmt.Errorf("unmarshal NUMA kubelet checkpoint entry failed: %w", err)
		}
		ids := make([]string, 0)
		for _, numaIDs := range numa.DeviceIDs {
			ids = append(ids, numaIDs...)
		}
		return &KubeletPodDeviceEntry{
			PodUID:        numa.PodUID,
			ContainerName: numa.ContainerName,
			ResourceName:  numa.ResourceName,
			DeviceIDs:     ids,
		}, true, nil
	}

	// flat layout (k8s 1.13 ~ 1.19)
	var flat kubeletPodDeviceEntryFlat
	if err := json.Unmarshal(raw, &flat); err != nil {
		return nil, false, fmt.Errorf("unmarshal flat kubelet checkpoint entry failed: %w", err)
	}
	return &KubeletPodDeviceEntry{
		PodUID:        flat.PodUID,
		ContainerName: flat.ContainerName,
		ResourceName:  flat.ResourceName,
		DeviceIDs:     flat.DeviceIDs,
	}, true, nil
}

// KubeletCheckpointState is a parsed view of kubelet's checkpoint file:
// the pod uids kubelet still holds device allocations for, and the device
// ids currently allocated, keyed by resource name.
type KubeletCheckpointState struct {
	PodUIDs         map[string]struct{}
	AssignedDevices map[string]map[string]struct{}
}

// LoadKubeletCheckpointState reads kubelet's checkpoint and returns the pod
// uids and allocated device ids it currently holds. Both DeviceIDs layouts
// are supported.
func LoadKubeletCheckpointState() (*KubeletCheckpointState, error) {
	content, err := os.ReadFile(KubeletCheckpointPath)
	if err != nil {
		return nil, fmt.Errorf("read kubelet checkpoint file failed: %w", err)
	}

	var data kubeletCheckpointRawData
	if err := json.Unmarshal(content, &data); err != nil {
		return nil, fmt.Errorf("unmarshal kubelet checkpoint file failed: %w", err)
	}

	state := &KubeletCheckpointState{
		PodUIDs:         make(map[string]struct{}),
		AssignedDevices: make(map[string]map[string]struct{}),
	}
	for _, raw := range data.Data.PodDeviceEntries {
		entry, ok, err := parseKubeletCheckpointEntry(raw, "")
		if err != nil {
			klog.Warningf("parse kubelet checkpoint entry %s failed: %v", string(raw), err)
			continue
		}
		if !ok {
			continue
		}
		if entry.PodUID != "" {
			state.PodUIDs[entry.PodUID] = struct{}{}
		}
		deviceSet, exists := state.AssignedDevices[entry.ResourceName]
		if !exists {
			deviceSet = make(map[string]struct{})
			state.AssignedDevices[entry.ResourceName] = deviceSet
		}
		for _, deviceID := range entry.DeviceIDs {
			deviceSet[deviceID] = struct{}{}
		}
	}
	return state, nil
}
