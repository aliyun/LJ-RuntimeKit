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
	"testing"

	xpugrpc "github.com/lj-runtimekit/device-plugin/internal/grpc"
	"github.com/lj-runtimekit/device-plugin/internal/utils"

	"github.com/stretchr/testify/assert"
)

// marshalTopology serializes a Topology the way xdm does, with fixed fields
// plus the dynamic topo<level> key. Kept in tests because production only
// deserializes.
func marshalTopology(topo Topology) string {
	levels := make([]map[string]interface{}, 0, len(topo.Levels))
	for _, level := range topo.Levels {
		m := map[string]interface{}{"level": level.Level, "name": level.Name}
		if len(level.Peers) > 0 {
			m["peers"] = level.Peers
		}
		if level.TopoX != nil {
			m[level.topoKey()] = level.TopoX
		}
		levels = append(levels, m)
	}
	b, err := json.Marshal(map[string]interface{}{"levels": levels})
	if err != nil {
		panic(err)
	}
	return string(b)
}

func unmarshalLevel(raw string, level *TopologyLevel) error {
	return json.Unmarshal([]byte(raw), level)
}

// ppu890PTopology builds the topology xdm reports for a 890P style node, where
// the intra node levels start at 1: 4 pcie switches of 2 gpus, 2 numa nodes and
// 2 sockets of 4 gpus each.
func ppu890PTopology(minor int) string {
	switchIdx := minor / 2
	numaIdx := minor / 4
	socketIdx := minor / 4

	switchPeers := []int{switchIdx * 2, switchIdx*2 + 1}
	groupPeers := []int{numaIdx * 4, numaIdx*4 + 1, numaIdx*4 + 2, numaIdx*4 + 3}

	topo := Topology{Levels: []TopologyLevel{
		{Level: 1, Name: topoNamePcieSwitches, Peers: switchPeers, TopoX: []int{switchIdx}},
		{Level: 2, Name: topoNameNumaNodes, Peers: groupPeers, TopoX: []int{numaIdx}},
		{Level: 3, Name: topoNameCPUSockets, Peers: groupPeers, TopoX: []int{socketIdx}},
		{Level: 11, Name: topoNameHyperNode, TopoX: "cluster-uuid-7"},
		{Level: 21, Name: topoNameLeafSwitch, TopoX: "asw-001"},
		{Level: 22, Name: topoNamePointOfDelivery, TopoX: "pod-g13-p1"},
	}}
	return marshalTopology(topo)
}

func TestTranslateTopology_890P(t *testing.T) {
	perDevice := make(map[int]string)
	for minor := 0; minor < 8; minor++ {
		perDevice[minor] = ppu890PTopology(minor)
	}

	nodeTopo, err := TranslateTopology(perDevice)
	assert.NoError(t, err)
	assert.NotNil(t, nodeTopo.RdmaTopo)
	annotation, labels := nodeTopo.RdmaTopo, nodeTopo.Labels

	// a 890P reports no interconnect levels
	assert.Empty(t, nodeTopo.P2PLinks)

	// 2 sockets, each with 1 numa holding 2 switches of 2 gpus
	assert.Len(t, annotation.CPUSockets, 2)
	assert.Equal(t, 0, annotation.CPUSockets[0].Index)
	assert.Equal(t, 1, annotation.CPUSockets[1].Index)

	socket0 := annotation.CPUSockets[0]
	assert.Len(t, socket0.NUMANodes, 1)
	assert.Equal(t, 0, socket0.NUMANodes[0].Index)
	assert.Len(t, socket0.NUMANodes[0].PCIeSwitches, 2)
	assert.Equal(t, 0, socket0.NUMANodes[0].PCIeSwitches[0].Index)
	assert.Equal(t, []int{0, 1}, socket0.NUMANodes[0].PCIeSwitches[0].GPUs)
	assert.Equal(t, 1, socket0.NUMANodes[0].PCIeSwitches[1].Index)
	assert.Equal(t, []int{2, 3}, socket0.NUMANodes[0].PCIeSwitches[1].GPUs)

	socket1 := annotation.CPUSockets[1]
	assert.Len(t, socket1.NUMANodes, 1)
	assert.Equal(t, 1, socket1.NUMANodes[0].Index)
	assert.Equal(t, []int{4, 5}, socket1.NUMANodes[0].PCIeSwitches[0].GPUs)
	assert.Equal(t, []int{6, 7}, socket1.NUMANodes[0].PCIeSwitches[1].GPUs)

	assert.Equal(t, "cluster-uuid-7", labels.HyperNodeID)
	assert.Equal(t, "asw-001", labels.LeafSwitchID)
	assert.Equal(t, "pod-g13-p1", labels.PointOfDelivery)
	assert.False(t, labels.IsEmpty())
}

// TestTranslateTopology_810EShiftedLevels verifies translation keys off the
// level name, not the level number (810E reports pcie tree at levels 4-6).
func TestTranslateTopology_810EShiftedLevels(t *testing.T) {
	topo := Topology{Levels: []TopologyLevel{
		{Level: 1, Name: "icnLink2", Peers: []int{0, 3}},
		{Level: 2, Name: "icnLink4", Peers: []int{0, 1, 2, 3}},
		{Level: 3, Name: "icnLink8", Peers: []int{0, 1, 2, 3, 4, 5, 6, 7}},
		{Level: 4, Name: topoNamePcieSwitches, Peers: []int{0, 1, 2, 3}, TopoX: []int{4}},
		{Level: 5, Name: topoNameNumaNodes, Peers: []int{0, 1, 2, 3}, TopoX: []int{1}},
		{Level: 6, Name: topoNameCPUSockets, Peers: []int{0, 1, 2, 3}, TopoX: []int{0}},
		{Level: 21, Name: topoNameLeafSwitch, TopoX: "ASW-VM-SQA-G10-P2-S25"},
	}}

	nodeTopo, err := TranslateTopology(map[int]string{0: marshalTopology(topo)})
	assert.NoError(t, err)
	assert.NotNil(t, nodeTopo.RdmaTopo)
	annotation, labels := nodeTopo.RdmaTopo, nodeTopo.Labels

	assert.Len(t, annotation.CPUSockets, 1)
	assert.Equal(t, 0, annotation.CPUSockets[0].Index)
	assert.Equal(t, 1, annotation.CPUSockets[0].NUMANodes[0].Index)
	assert.Equal(t, 4, annotation.CPUSockets[0].NUMANodes[0].PCIeSwitches[0].Index)
	assert.Equal(t, []int{0, 1, 2, 3}, annotation.CPUSockets[0].NUMANodes[0].PCIeSwitches[0].GPUs)

	assert.Equal(t, "ASW-VM-SQA-G10-P2-S25", labels.LeafSwitchID)
	assert.Empty(t, labels.HyperNodeID)
}

// TestTranslateTopology_IcnLinkOnly covers a node whose xdm has no pcie_topo
// source: only the icnLink levels are reported, so no gpu-rdma-topo annotation
// can be built, but the interconnect groups still reach aliyun.com/device.
func TestTranslateTopology_IcnLinkOnly(t *testing.T) {
	topo := Topology{Levels: []TopologyLevel{
		{Level: 1, Name: "icnLink2", Peers: []int{0, 3}},
		{Level: 2, Name: "icnLink4", Peers: []int{0, 1, 2, 3}},
	}}

	nodeTopo, err := TranslateTopology(map[int]string{0: marshalTopology(topo)})
	assert.NoError(t, err)
	assert.Nil(t, nodeTopo.RdmaTopo)
	assert.True(t, nodeTopo.Labels.IsEmpty())
	assert.Equal(t, []DeviceP2PLink{
		{PeerMinor: "0,3", Type: "icnlink"},
		{PeerMinor: "0,1,2,3", Type: "icnlink"},
	}, nodeTopo.P2PLinks[0])
}

// TestTranslateTopology_IcnLinkP2PLinks verifies interconnect levels become
// p2p links in aliyun.com/device, tightest group first, peers including self.
func TestTranslateTopology_IcnLinkP2PLinks(t *testing.T) {
	topo := Topology{Levels: []TopologyLevel{
		// deliberately out of order to prove the levels get sorted
		{Level: 3, Name: "icnLink8", Peers: []int{0, 1, 2, 3, 4, 5, 6, 7}},
		{Level: 1, Name: "icnLink2", Peers: []int{3, 0}},
		{Level: 2, Name: "icnLink4", Peers: []int{0, 1, 2, 3}},
		// the pcie tree levels must not leak into the p2p link list
		{Level: 4, Name: topoNamePcieSwitches, Peers: []int{0, 1}, TopoX: []int{0}},
	}}

	nodeTopo, err := TranslateTopology(map[int]string{0: marshalTopology(topo)})
	assert.NoError(t, err)
	assert.Equal(t, []DeviceP2PLink{
		{PeerMinor: "0,3", Type: "icnlink"},
		{PeerMinor: "0,1,2,3", Type: "icnlink"},
		{PeerMinor: "0,1,2,3,4,5,6,7", Type: "icnlink"},
	}, nodeTopo.P2PLinks[0])
}

func TestTranslateTopology_Empty(t *testing.T) {
	nodeTopo, err := TranslateTopology(map[int]string{})
	assert.NoError(t, err)
	assert.Nil(t, nodeTopo.RdmaTopo)
	assert.True(t, nodeTopo.Labels.IsEmpty())
	assert.Empty(t, nodeTopo.P2PLinks)
}

func TestTranslateTopology_InvalidJSON(t *testing.T) {
	_, err := TranslateTopology(map[int]string{0: "{not json"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "device minor 0")
}

// TestTopologyLevelUnmarshal_TopoValueTypes covers the typing rule xdm applies:
// the topoN value of an intra node level (1-9) is an int array, while a rack or
// network level (11-29) carries a plain string.
func TestTopologyLevelUnmarshal_TopoValueTypes(t *testing.T) {
	t.Run("intra node level is an int array", func(t *testing.T) {
		var level TopologyLevel
		assert.NoError(t, unmarshalLevel(`{"level":1,"name":"pcieSwitches","peers":[0,1],"topo1":[3]}`, &level))
		idx, ok := level.topoIndex()
		assert.True(t, ok)
		assert.Equal(t, 3, idx)
		assert.Equal(t, []int{0, 1}, level.Peers)
	})

	t.Run("network level is a string", func(t *testing.T) {
		var level TopologyLevel
		assert.NoError(t, unmarshalLevel(`{"level":21,"name":"leafSwitch","topo21":"asw-001"}`, &level))
		assert.Equal(t, "asw-001", level.topoIdentifier())
		assert.Empty(t, level.Peers)
	})

	t.Run("missing topo field", func(t *testing.T) {
		var level TopologyLevel
		assert.NoError(t, unmarshalLevel(`{"level":1,"name":"icnLink2","peers":[0,3]}`, &level))
		assert.Nil(t, level.TopoX)
		_, ok := level.topoIndex()
		assert.False(t, ok)
	})

	t.Run("multi valued intra node level takes the first index", func(t *testing.T) {
		var level TopologyLevel
		assert.NoError(t, unmarshalLevel(`{"level":1,"name":"pcieSwitches","topo1":[2,5]}`, &level))
		idx, ok := level.topoIndex()
		assert.True(t, ok)
		assert.Equal(t, 2, idx)
	})
}

// TestTranslateTopology_XdmWireFormat feeds the raw json xdm emits (written
// by hand) so a wire contract change breaks the test instead of silently
// mistranslating in production.
func TestTranslateTopology_XdmWireFormat(t *testing.T) {
	perDevice := map[int]string{
		0: `{"levels":[{"level":1,"name":"pcieSwitches","peers":[0,1],"topo1":[0]},{"level":2,"name":"numaNodes","peers":[0,1,2,3],"topo2":[0]},{"level":3,"name":"cpuSockets","peers":[0,1,2,3],"topo3":[0]},{"level":11,"name":"hyperNode","topo11":"9e1a2b3c-0000-0000-0000-000000000001-5"},{"level":21,"name":"leafSwitch","topo21":"ASW-001"},{"level":22,"name":"pointOfDelivery","topo22":"POD-G13-P1"}]}`,
		1: `{"levels":[{"level":1,"name":"pcieSwitches","peers":[0,1],"topo1":[0]},{"level":2,"name":"numaNodes","peers":[0,1,2,3],"topo2":[0]},{"level":3,"name":"cpuSockets","peers":[0,1,2,3],"topo3":[0]},{"level":11,"name":"hyperNode","topo11":"9e1a2b3c-0000-0000-0000-000000000001-5"},{"level":21,"name":"leafSwitch","topo21":"ASW-001"},{"level":22,"name":"pointOfDelivery","topo22":"POD-G13-P1"}]}`,
		2: `{"levels":[{"level":1,"name":"pcieSwitches","peers":[2,3],"topo1":[1]},{"level":2,"name":"numaNodes","peers":[0,1,2,3],"topo2":[0]},{"level":3,"name":"cpuSockets","peers":[0,1,2,3],"topo3":[0]},{"level":11,"name":"hyperNode","topo11":"9e1a2b3c-0000-0000-0000-000000000001-5"},{"level":21,"name":"leafSwitch","topo21":"ASW-001"},{"level":22,"name":"pointOfDelivery","topo22":"POD-G13-P1"}]}`,
		3: `{"levels":[{"level":1,"name":"pcieSwitches","peers":[2,3],"topo1":[1]},{"level":2,"name":"numaNodes","peers":[0,1,2,3],"topo2":[0]},{"level":3,"name":"cpuSockets","peers":[0,1,2,3],"topo3":[0]},{"level":11,"name":"hyperNode","topo11":"9e1a2b3c-0000-0000-0000-000000000001-5"},{"level":21,"name":"leafSwitch","topo21":"ASW-001"},{"level":22,"name":"pointOfDelivery","topo22":"POD-G13-P1"}]}`,
	}

	nodeTopo, err := TranslateTopology(perDevice)
	assert.NoError(t, err)
	assert.NotNil(t, nodeTopo.RdmaTopo)

	assert.Equal(t, []CPUSocket{{
		Index: 0,
		NUMANodes: []NUMANode{{
			Index: 0,
			PCIeSwitches: []PCIeSwitch{
				{Index: 0, GPUs: []int{0, 1}},
				{Index: 1, GPUs: []int{2, 3}},
			},
		}},
	}}, nodeTopo.RdmaTopo.CPUSockets)

	assert.Equal(t, "9e1a2b3c-0000-0000-0000-000000000001-5", nodeTopo.Labels.HyperNodeID)
	assert.Equal(t, "ASW-001", nodeTopo.Labels.LeafSwitchID)
	assert.Equal(t, "POD-G13-P1", nodeTopo.Labels.PointOfDelivery)
	assert.Empty(t, nodeTopo.P2PLinks)
}

// TestTranslateTopology_XdmWireFormat810E feeds the exact json xdm emits for a
// 810E, where the icnLink levels occupy 1-3 without any topoN field and the
// pcie tree is shifted to 4-6.
func TestTranslateTopology_XdmWireFormat810E(t *testing.T) {
	perDevice := map[int]string{
		0: `{"levels":[{"level":1,"name":"icnLink2","peers":[0,3]},{"level":2,"name":"icnLink4","peers":[0,1,2,3]},{"level":3,"name":"icnLink8","peers":[0,1,2,3,4,5,6,7]},{"level":4,"name":"pcieSwitches","peers":[0,1,2,3],"topo4":[4]},{"level":5,"name":"numaNodes","peers":[0,1,2,3],"topo5":[1]},{"level":6,"name":"cpuSockets","peers":[0,1,2,3],"topo6":[0]}]}`,
	}

	nodeTopo, err := TranslateTopology(perDevice)
	assert.NoError(t, err)
	assert.NotNil(t, nodeTopo.RdmaTopo)

	// keyed by name, so the shifted level numbers still land correctly
	assert.Equal(t, []CPUSocket{{
		Index: 0,
		NUMANodes: []NUMANode{{
			Index:        1,
			PCIeSwitches: []PCIeSwitch{{Index: 4, GPUs: []int{0, 1, 2, 3}}},
		}},
	}}, nodeTopo.RdmaTopo.CPUSockets)

	assert.Equal(t, []DeviceP2PLink{
		{PeerMinor: "0,3", Type: "icnlink"},
		{PeerMinor: "0,1,2,3", Type: "icnlink"},
		{PeerMinor: "0,1,2,3,4,5,6,7", Type: "icnlink"},
	}, nodeTopo.P2PLinks[0])
	assert.True(t, nodeTopo.Labels.IsEmpty())
}

func TestHasPcieSwitchTopology(t *testing.T) {
	withSwitch := marshalTopology(Topology{Levels: []TopologyLevel{
		{Level: 1, Name: topoNamePcieSwitches, Peers: []int{0, 1}, TopoX: []int{0}},
	}})
	icnLinkOnly := marshalTopology(Topology{Levels: []TopologyLevel{
		{Level: 1, Name: "icnLink2", Peers: []int{0, 3}},
	}})

	cases := []struct {
		name     string
		topology string
		expected bool
	}{
		{"pcie switch level reported", withSwitch, true},
		{"icnlink only", icnLinkOnly, false},
		{"no topology key", "", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			meta := utils.NewDeviceMeta()
			extender := map[string]string{utils.DeviceMinor: "0"}
			if tc.topology != "" {
				extender[utils.TopologyKey] = tc.topology
			}
			meta.Put(0, &xpugrpc.Device{Id: "dev0", ExtenderResources: extender})

			assert.Equal(t, tc.expected, HasPcieSwitchTopology(meta))
		})
	}

	assert.False(t, HasPcieSwitchTopology(nil))
}
