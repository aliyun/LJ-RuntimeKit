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
	"testing"

	"github.com/lj-runtimekit/scheduler/pkg/utils"

	"github.com/stretchr/testify/assert"
)

func TestClusterMigDevices_Clone(t *testing.T) {
	cmd := make(ClusterMigDevices)
	cmd["node-1"] = utils.New()
	cmd["node-1"].Insert(0)
	cmd["node-1"].Insert(1)

	cloned := cmd.Clone().(ClusterMigDevices)
	assert.Equal(t, len(cmd), len(cloned))
	assert.True(t, cloned["node-1"].Has(0))
	assert.True(t, cloned["node-1"].Has(1))

	// Deep copy check
	cloned["node-1"].Insert(2)
	assert.False(t, cmd["node-1"].Has(2))
}

func TestClusterMigDevices_Insert(t *testing.T) {
	cmd := make(ClusterMigDevices)

	// Insert into new node
	cmd.Insert("node-1", 0)
	assert.NotNil(t, cmd["node-1"])
	assert.True(t, cmd["node-1"].Has(0))

	// Insert additional indices
	cmd.Insert("node-1", 1)
	cmd.Insert("node-1", 2)
	assert.True(t, cmd["node-1"].Has(1))
	assert.True(t, cmd["node-1"].Has(2))

	// Insert into different node
	cmd.Insert("node-2", 0)
	assert.True(t, cmd["node-2"].Has(0))
	assert.Equal(t, 2, len(cmd))
}

func TestDevice_DeepCopy(t *testing.T) {
	dev := &Device{
		Minor:      0,
		Uuid:       "gpu-uuid-001",
		Health:     "Healthy",
		Type:       "A100",
		MigEnable:  true,
		Slice:      7,
		Core:       4,
		Memory:     4096,
		Extensions: map[string]string{"driver": "525.60.13"},
	}

	copy := dev.DeepCopy()
	assert.Equal(t, dev.Minor, copy.Minor)
	assert.Equal(t, dev.Uuid, copy.Uuid)
	assert.Equal(t, dev.Health, copy.Health)
	assert.Equal(t, dev.Type, copy.Type)
	assert.Equal(t, dev.MigEnable, copy.MigEnable)
	assert.Equal(t, dev.Slice, copy.Slice)
	assert.Equal(t, dev.Core, copy.Core)
	assert.Equal(t, dev.Memory, copy.Memory)
	assert.Equal(t, dev.Extensions["driver"], copy.Extensions["driver"])

	// Modify copy
	copy.Uuid = "gpu-uuid-002"
	copy.Extensions["driver"] = "535.00.00"
	assert.NotEqual(t, dev.Uuid, copy.Uuid)
	assert.NotEqual(t, dev.Extensions["driver"], copy.Extensions["driver"])
}

func TestGpuDevice_Has(t *testing.T) {
	gd := GpuDevice{
		Count: 2,
		Devices: map[int]*Device{
			0: {Minor: 0, MigEnable: true},
			1: {Minor: 1, MigEnable: false},
		},
	}

	assert.True(t, gd.Has(0))   // exists + mig enabled
	assert.False(t, gd.Has(1))  // exists but mig disabled
	assert.False(t, gd.Has(2))  // doesn't exist
}

func TestGpuDevice_Clone(t *testing.T) {
	gd := GpuDevice{
		Count: 2,
		Devices: map[int]*Device{
			0: {Minor: 0, Uuid: "gpu-0", MigEnable: true, Slice: 7, Extensions: map[string]string{"k": "v"}},
			1: {Minor: 1, Uuid: "gpu-1", MigEnable: false},
		},
	}

	cloned := gd.Clone()
	assert.Equal(t, gd.Count, cloned.Count)
	assert.Equal(t, len(gd.Devices), len(cloned.Devices))
	assert.Equal(t, gd.Devices[0].Uuid, cloned.Devices[0].Uuid)

	// Deep copy check
	cloned.Devices[0].Uuid = "gpu-cloned"
	cloned.Devices[0].Extensions["k"] = "v2"
	assert.NotEqual(t, gd.Devices[0].Uuid, cloned.Devices[0].Uuid)
	assert.NotEqual(t, gd.Devices[0].Extensions["k"], cloned.Devices[0].Extensions["k"])
}

func TestClusterNodeMigDevices_Clone(t *testing.T) {
	cnmd := make(ClusterNodeMigDevices)
	cnmd["node-1"] = &GpuDevice{
		Count: 1,
		Devices: map[int]*Device{
			0: {Minor: 0, MigEnable: true},
		},
	}

	cloned := cnmd.Clone().(ClusterNodeMigDevices)
	assert.Equal(t, len(cnmd), len(cloned))
	assert.Equal(t, cnmd["node-1"].Count, cloned["node-1"].Count)

	// Deep copy check
	cloned["node-1"].Count = 99
	assert.NotEqual(t, cnmd["node-1"].Count, cloned["node-1"].Count)
}
