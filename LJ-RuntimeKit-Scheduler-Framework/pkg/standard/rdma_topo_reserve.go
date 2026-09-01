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

package standard

import (
	"encoding/json"
	"sort"

	ampcache "github.com/lj-runtimekit/scheduler/pkg/cache"
	"github.com/lj-runtimekit/scheduler/pkg/utils"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

// RDMA topology structures aligned with the device-plugin protocol.
// The node annotation only reports Socket→NUMA→PCIeSwitch→GPUs; the scheduler
// selects PCIe switch indexes and passes them to xdm via pod annotation.

type RdmaCPUSocket struct {
	Index     int            `json:"index"`
	NUMANodes []RdmaNUMANode `json:"numaNodes"`
}

type RdmaNUMANode struct {
	Index        int              `json:"index"`
	PCIeSwitches []RdmaPCIeSwitch `json:"pcieSwitches"`
}

type RdmaPCIeSwitch struct {
	Index int   `json:"index"`
	GPUs  []int `json:"gpus"`
}

// RdmaTopoAnnotation holds the node annotation RDMA topology data.
type RdmaTopoAnnotation struct {
	CPUSockets []RdmaCPUSocket `json:"cpuSockets"`
}

// topoGroup is a set of GPUs that satisfy a topology constraint and their PCIe switches.
type topoGroup struct {
	gpus              []int
	pcieSwitchIndexes []int
}



// getRdmaTopoFromNode parses the RDMA topology annotation from a node.
func getRdmaTopoFromNode(node *v1.Node) *RdmaTopoAnnotation {
	if node == nil || node.Annotations == nil {
		return nil
	}
	annoJSON, ok := node.Annotations[utils.AnnotationRdmaTopo]
	if !ok || len(annoJSON) == 0 {
		return nil
	}
	var topo RdmaTopoAnnotation
	if err := json.Unmarshal([]byte(annoJSON), &topo); err != nil {
		klog.Errorf("failed to unmarshal rdma topo annotation: %v", err)
		return nil
	}
	return &topo
}

// tryRdmaTopoReserve selects GPUs for PPU 1.5 by degrading topology level:
// same PCIe Switch → same NUMA → same Socket → cross Socket.
func tryRdmaTopoReserve(
	availableDevices map[int]*ampcache.DeviceInfo,
	rdmaTopo *RdmaTopoAnnotation,
	numCards int,
) (selectedGPUs []int, pcieSwitchIndexes []int, ok bool) {

	// Level 1: same PCIe Switch.
	groups := findIdleGPUsByPCIeSwitch(availableDevices, rdmaTopo, numCards)
	if len(groups) > 0 {
		return groups[0].gpus, groups[0].pcieSwitchIndexes, true
	}
	// Level 2: same NUMA.
	groups = findIdleGPUsByNUMA(availableDevices, rdmaTopo, numCards)
	if len(groups) > 0 {
		return groups[0].gpus, groups[0].pcieSwitchIndexes, true
	}
	// Level 3: same Socket.
	groups = findIdleGPUsBySocket(availableDevices, rdmaTopo, numCards)
	if len(groups) > 0 {
		return groups[0].gpus, groups[0].pcieSwitchIndexes, true
	}
	// Level 4: cross Socket.
	groups = findAllIdleGPUs(availableDevices, rdmaTopo, numCards)
	if len(groups) > 0 {
		return groups[0].gpus, groups[0].pcieSwitchIndexes, true
	}

	return nil, nil, false
}



// findIdleGPUsByPCIeSwitch finds N idle GPUs within a single PCIe Switch.
func findIdleGPUsByPCIeSwitch(
	availableDevices map[int]*ampcache.DeviceInfo,
	rdmaTopo *RdmaTopoAnnotation,
	numCards int,
) []topoGroup {
	var groups []topoGroup

	for _, socket := range rdmaTopo.CPUSockets {
		for _, numa := range socket.NUMANodes {
			for _, pcieSwitch := range numa.PCIeSwitches {
				idleGPUs := getIdleGPUsFromList(pcieSwitch.GPUs, availableDevices)
				if len(idleGPUs) < numCards {
					continue
				}
				sort.Ints(idleGPUs)
				selected := idleGPUs[:numCards]
				groups = append(groups, topoGroup{
					gpus:              selected,
					pcieSwitchIndexes: []int{pcieSwitch.Index},
				})
			}
		}
	}
	return groups
}

// findIdleGPUsByNUMA finds N idle GPUs within a single NUMA node.
func findIdleGPUsByNUMA(
	availableDevices map[int]*ampcache.DeviceInfo,
	rdmaTopo *RdmaTopoAnnotation,
	numCards int,
) []topoGroup {
	var groups []topoGroup

	for _, socket := range rdmaTopo.CPUSockets {
		for _, numa := range socket.NUMANodes {
			var numaGPUs []int
			for _, pcieSwitch := range numa.PCIeSwitches {
				numaGPUs = append(numaGPUs, pcieSwitch.GPUs...)
			}
			idleGPUs := getIdleGPUsFromList(numaGPUs, availableDevices)
			if len(idleGPUs) < numCards {
				continue
			}
			sort.Ints(idleGPUs)
			selected := idleGPUs[:numCards]
			groups = append(groups, topoGroup{
				gpus:              selected,
				pcieSwitchIndexes: deriveSwitchIndexesForGPUs(rdmaTopo, selected),
			})
		}
	}
	return groups
}

// findIdleGPUsBySocket finds N idle GPUs within a single CPU socket.
func findIdleGPUsBySocket(
	availableDevices map[int]*ampcache.DeviceInfo,
	rdmaTopo *RdmaTopoAnnotation,
	numCards int,
) []topoGroup {
	var groups []topoGroup

	for _, socket := range rdmaTopo.CPUSockets {
		var socketGPUs []int
		for _, numa := range socket.NUMANodes {
			for _, pcieSwitch := range numa.PCIeSwitches {
				socketGPUs = append(socketGPUs, pcieSwitch.GPUs...)
			}
		}
		idleGPUs := getIdleGPUsFromList(socketGPUs, availableDevices)
		if len(idleGPUs) < numCards {
			continue
		}
		sort.Ints(idleGPUs)
		selected := idleGPUs[:numCards]
		groups = append(groups, topoGroup{
			gpus:              selected,
			pcieSwitchIndexes: deriveSwitchIndexesForGPUs(rdmaTopo, selected),
		})
	}
	return groups
}

// findAllIdleGPUs finds N idle GPUs across all sockets.
func findAllIdleGPUs(
	availableDevices map[int]*ampcache.DeviceInfo,
	rdmaTopo *RdmaTopoAnnotation,
	numCards int,
) []topoGroup {
	var allGPUs []int
	for _, socket := range rdmaTopo.CPUSockets {
		for _, numa := range socket.NUMANodes {
			for _, pcieSwitch := range numa.PCIeSwitches {
				allGPUs = append(allGPUs, pcieSwitch.GPUs...)
			}
		}
	}
	idleGPUs := getIdleGPUsFromList(allGPUs, availableDevices)
	if len(idleGPUs) < numCards {
		return nil
	}
	sort.Ints(idleGPUs)
	selected := idleGPUs[:numCards]
	return []topoGroup{{
		gpus:              selected,
		pcieSwitchIndexes: deriveSwitchIndexesForGPUs(rdmaTopo, selected),
	}}
}



// getIdleGPUsFromList filters idle GPUs from the given minor list.
func getIdleGPUsFromList(gpuMinors []int, availableDevices map[int]*ampcache.DeviceInfo) []int {
	var idle []int
	for _, minor := range gpuMinors {
		dev, ok := availableDevices[minor]
		if !ok || !dev.IdleDevice() {
			continue
		}
		idle = append(idle, minor)
	}
	return idle
}

// deriveSwitchIndexesForGPUs returns the unique PCIe switch indexes for the given GPUs.
func deriveSwitchIndexesForGPUs(rdmaTopo *RdmaTopoAnnotation, gpus []int) []int {
	gpuSet := make(map[int]bool, len(gpus))
	for _, g := range gpus {
		gpuSet[g] = true
	}
	seen := make(map[int]bool)
	for _, socket := range rdmaTopo.CPUSockets {
		for _, numa := range socket.NUMANodes {
			for _, sw := range numa.PCIeSwitches {
				for _, gpu := range sw.GPUs {
					if gpuSet[gpu] {
						seen[sw.Index] = true
					}
				}
			}
		}
	}
	result := make([]int, 0, len(seen))
	for idx := range seen {
		result = append(result, idx)
	}
	sort.Ints(result)
	return result
}

// scoreRdmaTopoLevel scores a node by the tightest RDMA topology level available.
func scoreRdmaTopoLevel(
	availableDevices map[int]*ampcache.DeviceInfo,
	rdmaTopo *RdmaTopoAnnotation,
	numCards int,
) int64 {
	if len(findIdleGPUsByPCIeSwitch(availableDevices, rdmaTopo, numCards)) > 0 {
		return 100 // same PCIe Switch
	}
	if len(findIdleGPUsByNUMA(availableDevices, rdmaTopo, numCards)) > 0 {
		return 75 // same NUMA
	}
	if len(findIdleGPUsBySocket(availableDevices, rdmaTopo, numCards)) > 0 {
		return 50 // same Socket
	}
	if len(findAllIdleGPUs(availableDevices, rdmaTopo, numCards)) > 0 {
		return 25 // cross Socket
	}
	return 0 // insufficient GPUs
}
