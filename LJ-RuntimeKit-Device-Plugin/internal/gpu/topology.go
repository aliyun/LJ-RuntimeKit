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
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/lj-runtimekit/device-plugin/internal/utils"

	"k8s.io/klog/v2"
)

// Level names reported by xdm in the abstract topology protocol. Translation
// keys off the name, never the level number, because numbering is machine
// dependent (e.g. 810E shifts pcieSwitches to level 4-6).
const (
	topoNamePcieSwitches    = "pcieSwitches"
	topoNameNumaNodes       = "numaNodes"
	topoNameCPUSockets      = "cpuSockets"
	topoNameHyperNode       = "hyperNode"
	topoNameLeafSwitch      = "leafSwitch"
	topoNamePointOfDelivery = "pointOfDelivery"

	// topoNameIcnLinkPrefix marks the intra node interconnect levels, named
	// icnLink2 / icnLink4 / icnLink8 on a PPU 810E.
	topoNameIcnLinkPrefix = "icnLink"
	// icnLinkType is the p2p link type published inside the aliyun.com/device
	// annotation, kept at the value xdm used before the abstract topology
	// protocol replaced its dedicated TopologyInfo report.
	icnLinkType = "icnlink"
)

// Topology is the per device abstract topology xdm reports through
// Device.extenderResources["topology"]. It mirrors the xdm side definition in
// pkg/util/topology.go.
type Topology struct {
	Levels []TopologyLevel `json:"levels"`
}

// TopologyLevel is one abstract topology level. 1-9 are intra node, 11-19
// rack, 21-29 network. Peers are only reported for intra node levels.
type TopologyLevel struct {
	Level int
	Name  string
	Peers []int
	// TopoX is the level identifier under the dynamic "topo<level>" key:
	// []int for intra node levels, string for rack/network levels, nil when
	// the level has no identifier (e.g. 810E icnLink).
	TopoX interface{}
}

// topoKey returns the dynamic field name of this level, topo<level>.
func (tl TopologyLevel) topoKey() string {
	return fmt.Sprintf("topo%d", tl.Level)
}

// isIntraNode reports whether this level belongs to the intra node segment,
// which is what decides the type of the topo<level> value.
func (tl TopologyLevel) isIntraNode() bool {
	return tl.Level >= 1 && tl.Level <= 9
}

// UnmarshalJSON mirrors the xdm side serialization: the fixed level / name /
// peers fields plus the dynamic topo<level> field, whose type is decided by the
// level segment rather than guessed from the json shape.
func (tl *TopologyLevel) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	if v, ok := raw["level"]; ok {
		if err := json.Unmarshal(v, &tl.Level); err != nil {
			return err
		}
	}
	if v, ok := raw["name"]; ok {
		if err := json.Unmarshal(v, &tl.Name); err != nil {
			return err
		}
	}
	if v, ok := raw["peers"]; ok {
		if err := json.Unmarshal(v, &tl.Peers); err != nil {
			return err
		}
	}

	v, ok := raw[tl.topoKey()]
	if !ok {
		tl.TopoX = nil
		return nil
	}
	if tl.isIntraNode() {
		var ints []int
		if err := json.Unmarshal(v, &ints); err == nil {
			tl.TopoX = ints
		}
		return nil
	}
	var s string
	if err := json.Unmarshal(v, &s); err == nil {
		tl.TopoX = s
	}
	return nil
}

// topoIndex returns the identifier of an intra node level. xdm always reports a
// single element for the pcie switch / numa / socket levels, so the first entry
// is the index of this device within that level.
func (tl TopologyLevel) topoIndex() (int, bool) {
	indexes, ok := tl.TopoX.([]int)
	if !ok || len(indexes) == 0 {
		return 0, false
	}
	return indexes[0], true
}

// topoIdentifier returns the identifier of a rack or network level.
func (tl TopologyLevel) topoIdentifier() string {
	id, _ := tl.TopoX.(string)
	return id
}

// findLevel returns the level carrying the given semantic name.
func (t *Topology) findLevel(name string) (TopologyLevel, bool) {
	for _, level := range t.Levels {
		if level.Name == name {
			return level, true
		}
	}
	return TopologyLevel{}, false
}

// PCIeSwitch is a pcie switch and the gpu minors sitting behind it.
type PCIeSwitch struct {
	Index int   `json:"index"`
	GPUs  []int `json:"gpus"`
}

// NUMANode is a numa node and the pcie switches below it.
type NUMANode struct {
	Index        int          `json:"index"`
	PCIeSwitches []PCIeSwitch `json:"pcieSwitches"`
}

// CPUSocket is a cpu socket and the numa nodes below it.
type CPUSocket struct {
	Index     int        `json:"index"`
	NUMANodes []NUMANode `json:"numaNodes"`
}

// RdmaTopoAnnotation is the value of the aliyun.com/gpu-rdma-topo node
// annotation: the socket / numa / pcie switch tree used for placement.
// xdm resolves RDMA device details from the pcie switch index.
type RdmaTopoAnnotation struct {
	CPUSockets []CPUSocket `json:"cpuSockets"`
}

// NodeTopoLabels holds the node scoped topology identifiers that are published
// as node labels rather than annotations.
type NodeTopoLabels struct {
	HyperNodeID     string
	LeafSwitchID    string
	PointOfDelivery string
}

// IsEmpty reports whether no node scoped topology identifier was reported.
func (l *NodeTopoLabels) IsEmpty() bool {
	return l == nil || (l.HyperNodeID == "" && l.LeafSwitchID == "" && l.PointOfDelivery == "")
}

// NodeTopology is the node wide view translated from the per device topology
// xdm reports through the ListAndWatch stream.
type NodeTopology struct {
	// RdmaTopo is the socket / numa / pcie switch tree published as the
	// aliyun.com/gpu-rdma-topo annotation. It is nil when xdm reported no intra
	// node pcie topology.
	RdmaTopo *RdmaTopoAnnotation
	// Labels carries the node scoped identifiers published as node labels.
	Labels *NodeTopoLabels
	// P2PLinks holds the interconnect groups of every device keyed by minor,
	// published inside the aliyun.com/device annotation.
	P2PLinks map[int][]DeviceP2PLink
}

// numaKey and switchKey identify a numa node / pcie switch within its parent.
// The indexes are keyed by their parent instead of globally, so that machines
// numbering numa nodes or switches per socket are handled correctly too.
type numaKey struct {
	socket int
	numa   int
}

type switchKey struct {
	socket int
	numa   int
	sw     int
}

// TranslateTopology aggregates per device topology into the node wide view:
// pcie tree for the rdma topo annotation, node identifiers for labels, and
// interconnect groups for the device annotation.
func TranslateTopology(perDeviceTopology map[int]string) (*NodeTopology, error) {
	result := &NodeTopology{
		Labels:   &NodeTopoLabels{},
		P2PLinks: make(map[int][]DeviceP2PLink),
	}
	labels := result.Labels

	var socketOrder []int
	numaOrder := make(map[int][]int)
	switchOrder := make(map[numaKey][]int)
	switchGPUs := make(map[switchKey][]int)

	// iterate the minors in a stable order so that the generated annotation is
	// byte identical across restarts and does not cause pointless node patches
	minors := make([]int, 0, len(perDeviceTopology))
	for minor := range perDeviceTopology {
		minors = append(minors, minor)
	}
	sort.Ints(minors)

	for _, minor := range minors {
		var topo Topology
		if err := json.Unmarshal([]byte(perDeviceTopology[minor]), &topo); err != nil {
			return nil, fmt.Errorf("unmarshal topology of device minor %d failed: %w", minor, err)
		}

		// the interconnect levels are a per device property, unlike everything
		// else translated here
		if links := icnLinkP2PLinks(&topo); len(links) > 0 {
			result.P2PLinks[minor] = links
		}

		// node scoped identifiers are reported identically by every device,
		// the first non empty value wins
		if labels.HyperNodeID == "" {
			if level, ok := topo.findLevel(topoNameHyperNode); ok {
				labels.HyperNodeID = level.topoIdentifier()
			}
		}
		if labels.LeafSwitchID == "" {
			if level, ok := topo.findLevel(topoNameLeafSwitch); ok {
				labels.LeafSwitchID = level.topoIdentifier()
			}
		}
		if labels.PointOfDelivery == "" {
			if level, ok := topo.findLevel(topoNamePointOfDelivery); ok {
				labels.PointOfDelivery = level.topoIdentifier()
			}
		}

		// the tree needs all three intra node levels to place a switch; a
		// machine reporting only icnLink levels contributes labels only
		socketLevel, hasSocket := topo.findLevel(topoNameCPUSockets)
		numaLevel, hasNuma := topo.findLevel(topoNameNumaNodes)
		switchLevel, hasSwitch := topo.findLevel(topoNamePcieSwitches)
		if !hasSocket || !hasNuma || !hasSwitch {
			continue
		}

		socketIdx, err := parseTopoIndex(socketLevel, minor, topoNameCPUSockets)
		if err != nil {
			klog.Errorf("%v", err)
			continue
		}
		numaIdx, err := parseTopoIndex(numaLevel, minor, topoNameNumaNodes)
		if err != nil {
			klog.Errorf("%v", err)
			continue
		}
		swIdx, err := parseTopoIndex(switchLevel, minor, topoNamePcieSwitches)
		if err != nil {
			klog.Errorf("%v", err)
			continue
		}

		nKey := numaKey{socket: socketIdx, numa: numaIdx}
		sKey := switchKey{socket: socketIdx, numa: numaIdx, sw: swIdx}

		if _, seen := numaOrder[socketIdx]; !seen {
			socketOrder = append(socketOrder, socketIdx)
		}
		if !containsInt(numaOrder[socketIdx], numaIdx) {
			numaOrder[socketIdx] = append(numaOrder[socketIdx], numaIdx)
		}
		if !containsInt(switchOrder[nKey], swIdx) {
			switchOrder[nKey] = append(switchOrder[nKey], swIdx)
		}
		// every device behind a switch reports the same peers, so recording it
		// once per switch is enough; fall back to the reporting device itself
		// when the level carries no peers
		if _, ok := switchGPUs[sKey]; !ok {
			gpus := switchLevel.Peers
			if len(gpus) == 0 {
				gpus = []int{minor}
			}
			switchGPUs[sKey] = dedupSortedInts(gpus)
		}
	}

	if len(socketOrder) == 0 {
		klog.Infof("xdm reported no pcie switch topology, skip %s annotation", utils.AnnotationRdmaTopo)
		return result, nil
	}

	sort.Ints(socketOrder)
	annotation := &RdmaTopoAnnotation{CPUSockets: make([]CPUSocket, 0, len(socketOrder))}
	for _, socketIdx := range socketOrder {
		numaIdxs := numaOrder[socketIdx]
		sort.Ints(numaIdxs)

		socket := CPUSocket{Index: socketIdx, NUMANodes: make([]NUMANode, 0, len(numaIdxs))}
		for _, numaIdx := range numaIdxs {
			nKey := numaKey{socket: socketIdx, numa: numaIdx}
			swIdxs := switchOrder[nKey]
			sort.Ints(swIdxs)

			numa := NUMANode{Index: numaIdx, PCIeSwitches: make([]PCIeSwitch, 0, len(swIdxs))}
			for _, swIdx := range swIdxs {
				numa.PCIeSwitches = append(numa.PCIeSwitches, PCIeSwitch{
					Index: swIdx,
					GPUs:  switchGPUs[switchKey{socket: socketIdx, numa: numaIdx, sw: swIdx}],
				})
			}
			socket.NUMANodes = append(socket.NUMANodes, numa)
		}
		annotation.CPUSockets = append(annotation.CPUSockets, socket)
	}

	result.RdmaTopo = annotation
	return result, nil
}

// icnLinkP2PLinks turns a device's interconnect levels into the p2p link
// list in the aliyun.com/device annotation. The peer list always contains
// the device itself so consumers can match on peer count.
func icnLinkP2PLinks(topo *Topology) []DeviceP2PLink {
	icnLevels := make([]TopologyLevel, 0, len(topo.Levels))
	for _, level := range topo.Levels {
		if strings.HasPrefix(level.Name, topoNameIcnLinkPrefix) && len(level.Peers) > 0 {
			icnLevels = append(icnLevels, level)
		}
	}
	// tightest group first, matching the order xdm emitted the plans in
	sort.Slice(icnLevels, func(i, j int) bool { return icnLevels[i].Level < icnLevels[j].Level })

	links := make([]DeviceP2PLink, 0, len(icnLevels))
	for _, level := range icnLevels {
		peers := dedupSortedInts(level.Peers)
		parts := make([]string, 0, len(peers))
		for _, peer := range peers {
			parts = append(parts, strconv.Itoa(peer))
		}
		links = append(links, DeviceP2PLink{
			PeerMinor: strings.Join(parts, ","),
			Type:      icnLinkType,
		})
	}
	return links
}

// HasPcieSwitchTopology reports whether xdm advertised a pcie switch level.
// The rdma token plugin is started only on such nodes.
func HasPcieSwitchTopology(deviceMeta *utils.DeviceMeta) bool {
	if deviceMeta == nil {
		return false
	}
	deviceMeta.RLock()
	defer deviceMeta.RUnlock()

	for _, d := range deviceMeta.Cache {
		if d == nil {
			continue
		}
		topoJSON, ok := d.ExtenderResources[utils.TopologyKey]
		if !ok || topoJSON == "" {
			continue
		}
		var topo Topology
		if err := json.Unmarshal([]byte(topoJSON), &topo); err != nil {
			continue
		}
		if _, found := topo.findLevel(topoNamePcieSwitches); found {
			return true
		}
	}
	return false
}

func parseTopoIndex(level TopologyLevel, minor int, name string) (int, error) {
	idx, ok := level.topoIndex()
	if !ok {
		return 0, fmt.Errorf("device minor %d level %s carries no valid %s identifier", minor, name, level.topoKey())
	}
	return idx, nil
}

func containsInt(items []int, target int) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}

func dedupSortedInts(items []int) []int {
	result := make([]int, 0, len(items))
	seen := make(map[int]struct{}, len(items))
	for _, item := range items {
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		result = append(result, item)
	}
	sort.Ints(result)
	return result
}
