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
	"github.com/lj-runtimekit/scheduler/pkg/utils"

	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// ClusterMigDevices maps node names to sets of MIG device indices.
type ClusterMigDevices map[string]*utils.Sets

func (c ClusterMigDevices) Clone() framework.StateData {
	target := make(ClusterMigDevices)
	for node, migSets := range c {
		target[node] = migSets.Clone()
	}
	return target
}

// Insert adds a device index to the MIG device set for the given node.
func (c ClusterMigDevices) Insert(name string, idx int) {
	sets, ok := c[name]
	if !ok {
		sets = utils.New()
	}
	sets.Insert(idx)
	c[name] = sets
	return
}

// DeviceP2PLink represents a P2P link between two GPU devices.
type DeviceP2PLink struct {
	PeerMinor string `json:"peerMinor"`
	Type      string `json:"type"`
}

// Device represents a single GPU device (physical or MIG slice).
type Device struct {
	Minor      uint              `json:"minor"`
	Uuid       string            `json:"uuid"`
	Health     string            `json:"health"`
	Type       string            `json:"type"`
	MigEnable  bool              `json:"migEnable"`
	Slice      uint              `json:"slice"`
	Core       uint              `json:"core"`
	Memory     uint              `json:"memory"`
	Extensions map[string]string `json:"extensions"`
	Topology   []DeviceP2PLink   `json:"topology,omitempty"`
}

// DeepCopy returns a deep copy of the Device.
func (d Device) DeepCopy() *Device {
	target := new(Device)
	target.Minor = d.Minor
	target.Uuid = d.Uuid
	target.Health = d.Health
	target.Type = d.Type
	target.MigEnable = d.MigEnable
	target.Slice = d.Slice
	target.Core = d.Core
	target.Memory = d.Memory
	target.Extensions = make(map[string]string)
	for key, v := range d.Extensions {
		target.Extensions[key] = v
	}
	if d.Topology != nil {
		target.Topology = make([]DeviceP2PLink, len(d.Topology))
		copy(target.Topology, d.Topology)
	}
	return target
}

// GpuDevice holds device information for all GPUs on a node, typically parsed from node annotations.
type GpuDevice struct {
	Count   int             `json:"count"`
	Devices map[int]*Device `json:"devices"`
}

// Has returns true if the device at the given index exists and has MIG enabled.
func (gd GpuDevice) Has(id int) bool {
	device, ok := gd.Devices[id]
	if ok && device.MigEnable {
		return true
	}
	return false
}

// Clone returns a deep copy of the GpuDevice.
func (gd GpuDevice) Clone() *GpuDevice {
	target := new(GpuDevice)
	target.Count = gd.Count
	target.Devices = make(map[int]*Device)
	for key, d := range gd.Devices {
		target.Devices[key] = d.DeepCopy()
	}

	return target
}

// ClusterNodeMigDevices maps node names to GpuDevice for MIG scheduling.
type ClusterNodeMigDevices map[string]*GpuDevice

func (cd ClusterNodeMigDevices) Clone() framework.StateData {
	target := make(ClusterNodeMigDevices)
	for node, migDevice := range cd {
		target[node] = migDevice.Clone()
	}
	return target
}
