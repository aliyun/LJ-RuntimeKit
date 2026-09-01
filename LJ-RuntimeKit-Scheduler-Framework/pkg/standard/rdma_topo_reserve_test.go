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
	"testing"

	ampcache "github.com/lj-runtimekit/scheduler/pkg/cache"
	"github.com/lj-runtimekit/scheduler/pkg/utils"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ===========================================================================
// Helpers
// ===========================================================================

// buildRdmaTopo creates a typical 8-GPU topology:
// 2 Sockets × 1 NUMA × 2 PCIeSwitch × 2 GPUs = 8 GPUs
// Socket0: NUMA0 → Switch0[GPU 0,1], Switch1[GPU 2,3]
// Socket1: NUMA1 → Switch2[GPU 4,5], Switch3[GPU 6,7]
func buildRdmaTopo() *RdmaTopoAnnotation {
	return &RdmaTopoAnnotation{
		CPUSockets: []RdmaCPUSocket{
			{
				Index: 0,
				NUMANodes: []RdmaNUMANode{
					{
						Index: 0,
						PCIeSwitches: []RdmaPCIeSwitch{
							{
								Index: 0,
								GPUs:  []int{0, 1},
							},
							{
								Index: 1,
								GPUs:  []int{2, 3},
							},
						},
					},
				},
			},
			{
				Index: 1,
				NUMANodes: []RdmaNUMANode{
					{
						Index: 1,
						PCIeSwitches: []RdmaPCIeSwitch{
							{
								Index: 2,
								GPUs:  []int{4, 5},
							},
							{
								Index: 3,
								GPUs:  []int{6, 7},
							},
						},
					},
				},
			},
		},
	}
}

// makeDevices creates a map of DeviceInfo where the given idleMinors are idle (have resources)
// and busyMinors are fully used.
func makeDevices(idleMinors, busyMinors []int) map[int]*ampcache.DeviceInfo {
	devices := make(map[int]*ampcache.DeviceInfo)
	for _, m := range idleMinors {
		devices[m] = ampcache.NewDeviceInfo(m, 1024, 100)
	}
	for _, m := range busyMinors {
		dev := ampcache.NewDeviceInfo(m, 1024, 100)
		dev.TrunkDevice()
		devices[m] = dev
	}
	return devices
}

// allIdle returns all 8 GPUs idle.
func allIdle() map[int]*ampcache.DeviceInfo {
	return makeDevices([]int{0, 1, 2, 3, 4, 5, 6, 7}, nil)
}

// ===========================================================================
// Tests: getRdmaTopoFromNode
// ===========================================================================

func TestRdmaGetTopoFromNode(t *testing.T) {
	topo := buildRdmaTopo()
	topoJSON, _ := json.Marshal(topo)

	tests := []struct {
		name   string
		node   *v1.Node
		expect bool // true means non-nil result
	}{
		{
			name:   "node is nil",
			node:   nil,
			expect: false,
		},
		{
			name: "annotation not present",
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node-1",
					Annotations: map[string]string{},
				},
			},
			expect: false,
		},
		{
			name: "valid annotation JSON",
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-1",
					Annotations: map[string]string{
						utils.AnnotationRdmaTopo: string(topoJSON),
					},
				},
			},
			expect: true,
		},
		{
			name: "invalid annotation JSON",
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-1",
					Annotations: map[string]string{
						utils.AnnotationRdmaTopo: "not-valid-json{{{",
					},
				},
			},
			expect: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getRdmaTopoFromNode(tt.node)
			if tt.expect {
				assert.NotNil(t, result)
				assert.Equal(t, len(topo.CPUSockets), len(result.CPUSockets))
			} else {
				assert.Nil(t, result)
			}
		})
	}
}

// ===========================================================================
// Tests: scoreRdmaTopoLevel
// ===========================================================================

func TestScoreRdmaTopoLevel_AllLevels(t *testing.T) {
	// buildRdmaTopo topology (used for levels 100, 75, 25, 0):
	// Socket0: NUMA0 → Switch0[GPU 0,1], Switch1[GPU 2,3]
	// Socket1: NUMA1 → Switch2[GPU 4,5], Switch3[GPU 6,7]
	topo := buildRdmaTopo()

	// For Socket level (50) we need a topology where Socket has multiple NUMAs,
	// so NUMA level fails but Socket level succeeds.
	// Socket0: NUMA0 → Switch0[GPU 0,1], NUMA1 → Switch1[GPU 2,3]
	// Socket1: NUMA2 → Switch2[GPU 4,5], NUMA3 → Switch3[GPU 6,7]
	multiNumaTopo := &RdmaTopoAnnotation{
		CPUSockets: []RdmaCPUSocket{
			{
				Index: 0,
				NUMANodes: []RdmaNUMANode{
					{
						Index: 0,
						PCIeSwitches: []RdmaPCIeSwitch{
							{Index: 0, GPUs: []int{0, 1}},
						},
					},
					{
						Index: 1,
						PCIeSwitches: []RdmaPCIeSwitch{
							{Index: 1, GPUs: []int{2, 3}},
						},
					},
				},
			},
			{
				Index: 1,
				NUMANodes: []RdmaNUMANode{
					{
						Index: 2,
						PCIeSwitches: []RdmaPCIeSwitch{
							{Index: 2, GPUs: []int{4, 5}},
						},
					},
					{
						Index: 3,
						PCIeSwitches: []RdmaPCIeSwitch{
							{Index: 3, GPUs: []int{6, 7}},
						},
					},
				},
			},
		},
	}

	t.Run("PCIe Switch level (100)", func(t *testing.T) {
		// Switch0 has GPU 0,1 both idle → satisfies numCards=2 at PCIe level
		devices := makeDevices([]int{0, 1}, []int{2, 3, 4, 5, 6, 7})
		score := scoreRdmaTopoLevel(devices, topo, 2)
		assert.Equal(t, int64(100), score)
	})

	t.Run("NUMA level (75)", func(t *testing.T) {
		// Need 3: no single switch has 3 (max 2), but NUMA0 has 4 idle across 2 switches
		devices := makeDevices([]int{0, 1, 2, 3}, []int{4, 5, 6, 7})
		score := scoreRdmaTopoLevel(devices, topo, 3)
		assert.Equal(t, int64(75), score)
	})

	t.Run("Socket level (50)", func(t *testing.T) {
		// multiNumaTopo: each NUMA has only 2 GPUs (1 switch with 2 GPUs)
		// Need 3: PCIe max=2, NUMA max=2, but Socket0 has NUMA0+NUMA1 = 4 idle
		devices := makeDevices([]int{0, 1, 2, 3, 4, 5, 6, 7}, nil)
		score := scoreRdmaTopoLevel(devices, multiNumaTopo, 3)
		assert.Equal(t, int64(50), score)
	})

	t.Run("Cross-socket level (25)", func(t *testing.T) {
		// Need 5: each socket has 4 GPUs max, so need cross-socket
		devices := allIdle()
		score := scoreRdmaTopoLevel(devices, topo, 5)
		assert.Equal(t, int64(25), score)
	})

	t.Run("Unsatisfied (0)", func(t *testing.T) {
		// Need 4 but only 3 idle total
		devices := makeDevices([]int{0, 1, 2}, []int{3, 4, 5, 6, 7})
		score := scoreRdmaTopoLevel(devices, topo, 4)
		assert.Equal(t, int64(0), score)
	})
}

// ===========================================================================
// Tests: tryRdmaTopoReserve
// ===========================================================================

func TestRdmaTryTopoReserve(t *testing.T) {
	topo := buildRdmaTopo()

	tests := []struct {
		name        string
		devices     map[int]*ampcache.DeviceInfo
		numCards    int
		expectOk    bool
		expectGPUs  []int
		checkLen    bool // if true, just check length instead of exact match
		expectRDMA  int  // expected number of RDMA devices
	}{
		{
			name:       "4 cards - PCIeSwitch has enough (switch0+switch1 in socket0/numa0)",
			devices:    allIdle(),
			numCards:   4,
			expectOk:   true,
			checkLen:   true,
			expectRDMA: 2, // NUMA level: GPU 0,1,2,3 → mlx5_0, mlx5_1
		},
		{
			name:       "2 cards - PCIeSwitch level (switch has 2 idle)",
			devices:    allIdle(),
			numCards:   2,
			expectOk:   true,
			expectGPUs: []int{0, 1},
			expectRDMA: 1, // both map to mlx5_0
		},
		{
			name:     "2 cards - PCIeSwitch not enough, fallback to NUMA",
			devices:  makeDevices([]int{0, 2, 4, 5, 6, 7}, []int{1, 3}), // switch0 has only GPU0, switch1 has only GPU2
			numCards: 2,
			expectOk: true,
			checkLen: true,
		},
		{
			name:       "8 cards - cross Socket",
			devices:    allIdle(),
			numCards:   8,
			expectOk:   true,
			expectGPUs: []int{0, 1, 2, 3, 4, 5, 6, 7},
			expectRDMA: 4,
		},
		{
			name:     "request exceeds available",
			devices:  makeDevices([]int{0, 1, 2}, []int{3, 4, 5, 6, 7}),
			numCards: 4,
			expectOk: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gpus, switches, ok := tryRdmaTopoReserve(tt.devices, topo, tt.numCards)
			assert.Equal(t, tt.expectOk, ok)
			if !tt.expectOk {
				return
			}
			if tt.checkLen {
				assert.Len(t, gpus, tt.numCards)
			} else {
				assert.Equal(t, tt.expectGPUs, gpus)
			}
			if tt.expectRDMA > 0 {
				assert.Len(t, switches, tt.expectRDMA)
			}
		})
	}
}

// ===========================================================================
// Tests: findIdleGPUsByPCIeSwitch
// ===========================================================================

func TestRdmaFindIdleGPUsByPCIeSwitch(t *testing.T) {
	topo := buildRdmaTopo()

	tests := []struct {
		name     string
		devices  map[int]*ampcache.DeviceInfo
		numCards int
		wantLen  int // number of groups found
	}{
		{
			name:     "all idle, request 2 - 4 switches each have 2 → 4 groups",
			devices:  allIdle(),
			numCards: 2,
			wantLen:  4,
		},
		{
			name:     "all idle, request 3 - no switch has 3 GPUs",
			devices:  allIdle(),
			numCards: 3,
			wantLen:  0,
		},
		{
			name:     "partial idle - only switch with GPU 4,5 is fully idle",
			devices:  makeDevices([]int{4, 5}, []int{0, 1, 2, 3, 6, 7}),
			numCards: 2,
			wantLen:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			groups := findIdleGPUsByPCIeSwitch(tt.devices, topo, tt.numCards)
			assert.Len(t, groups, tt.wantLen)
		})
	}
}

// ===========================================================================
// Tests: findIdleGPUsByNUMA
// ===========================================================================

func TestRdmaFindIdleGPUsByNUMA(t *testing.T) {
	topo := buildRdmaTopo()

	tests := []struct {
		name     string
		devices  map[int]*ampcache.DeviceInfo
		numCards int
		wantLen  int
	}{
		{
			name:     "all idle, request 4 - each NUMA has 4 GPUs → 2 groups",
			devices:  allIdle(),
			numCards: 4,
			wantLen:  2,
		},
		{
			name:     "all idle, request 5 - no NUMA has 5 GPUs",
			devices:  allIdle(),
			numCards: 5,
			wantLen:  0,
		},
		{
			name:     "only NUMA1 (socket1) has enough",
			devices:  makeDevices([]int{4, 5, 6, 7}, []int{0, 1, 2, 3}),
			numCards: 3,
			wantLen:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			groups := findIdleGPUsByNUMA(tt.devices, topo, tt.numCards)
			assert.Len(t, groups, tt.wantLen)
		})
	}
}

// ===========================================================================
// Tests: findIdleGPUsBySocket
// ===========================================================================

func TestRdmaFindIdleGPUsBySocket(t *testing.T) {
	topo := buildRdmaTopo()

	tests := []struct {
		name     string
		devices  map[int]*ampcache.DeviceInfo
		numCards int
		wantLen  int
	}{
		{
			name:     "all idle, request 4 - each socket has 4 → 2 groups",
			devices:  allIdle(),
			numCards: 4,
			wantLen:  2,
		},
		{
			name:     "all idle, request 5 - no socket has 5",
			devices:  allIdle(),
			numCards: 5,
			wantLen:  0,
		},
		{
			name:     "socket1 has 3 idle, socket0 has 1 idle - request 3",
			devices:  makeDevices([]int{0, 4, 5, 6}, []int{1, 2, 3, 7}),
			numCards: 3,
			wantLen:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			groups := findIdleGPUsBySocket(tt.devices, topo, tt.numCards)
			assert.Len(t, groups, tt.wantLen)
		})
	}
}

// ===========================================================================
// Tests: findAllIdleGPUs
// ===========================================================================

func TestRdmaFindAllIdleGPUs(t *testing.T) {
	topo := buildRdmaTopo()

	tests := []struct {
		name     string
		devices  map[int]*ampcache.DeviceInfo
		numCards int
		wantLen  int
	}{
		{
			name:     "all idle, request 8 - returns 1 group",
			devices:  allIdle(),
			numCards: 8,
			wantLen:  1,
		},
		{
			name:     "only 3 idle, request 4 - not enough",
			devices:  makeDevices([]int{0, 1, 2}, []int{3, 4, 5, 6, 7}),
			numCards: 4,
			wantLen:  0,
		},
		{
			name:     "5 idle across sockets, request 5 - returns 1 group",
			devices:  makeDevices([]int{0, 1, 2, 4, 5}, []int{3, 6, 7}),
			numCards: 5,
			wantLen:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			groups := findAllIdleGPUs(tt.devices, topo, tt.numCards)
			assert.Len(t, groups, tt.wantLen)
		})
	}
}

// ===========================================================================
// Tests: deriveSwitchIndexesForGPUs
// ===========================================================================

func TestDeriveSwitchIndexesForGPUs(t *testing.T) {
	topo := buildRdmaTopo()

	tests := []struct {
		name   string
		gpus   []int
		expect []int
	}{
		{"single GPU in switch 0", []int{0}, []int{0}},
		{"two GPUs same switch", []int{0, 1}, []int{0}},
		{"GPUs across two switches same NUMA", []int{0, 1, 2, 3}, []int{0, 1}},
		{"GPUs across all switches", []int{0, 2, 4, 6}, []int{0, 1, 2, 3}},
		{"empty GPU list", []int{}, nil},
		{"GPU not in topology", []int{99}, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := deriveSwitchIndexesForGPUs(topo, tt.gpus)
			if tt.expect == nil {
				assert.Empty(t, result)
			} else {
				assert.Equal(t, tt.expect, result)
			}
		})
	}
}
