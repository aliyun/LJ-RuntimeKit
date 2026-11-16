/*
Copyright (c) 2026 Alibaba Cloud.

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

package computedomain

// Device 是 kubelet NodePrepareResources 响应中的设备条目。
// (对应 k8s.io/dynamic-resource-allocation/kubeletplugin.Device)
type Device struct {
	// Requests is the list of requests this device is associated with.
	Requests []string
	// PoolName is the name of the device pool this device belongs to.
	PoolName string
	// DeviceName is the name of the device within the pool.
	DeviceName string
	// CDIDeviceIDs is the list of CDI device IDs to inject into the
	// consuming container.
	// TODO(CDI): 目前留空,由 cdi.go 中的 CDIHandler 填充。
	CDIDeviceIDs []string
}

// PrepareResult is the result of preparing a single ResourceClaim.
type PrepareResult struct {
	Devices []Device
	Err     error
}

// NamespacedObject carries the UID / namespace / name of an object,
// used by NodeUnprepareResources.
type NamespacedObject struct {
	NamespacedName struct {
		Namespace string
		Name      string
	}
	UID string
}

func (n NamespacedObject) String() string {
	return n.NamespacedName.Namespace + "/" + n.NamespacedName.Name + ":" + n.UID
}

// PreparedDeviceList is a list of prepared devices.
type PreparedDeviceList []PreparedDevice

// PreparedDevice is one prepared channel device.
type PreparedDevice struct {
	Channel *PreparedComputeDomainChannel `json:"channel"`
}

// PreparedComputeDomainChannel couples the channel info with the DRA device
// it got prepared for.
type PreparedComputeDomainChannel struct {
	Info   *ComputeDomainChannelInfo `json:"info"`
	Device *Device                   `json:"device"`
}

func (d PreparedDevice) Type() string {
	if d.Channel != nil {
		return ComputeDomainChannelType
	}
	return UnknownDeviceType
}

func (d *PreparedDevice) CanonicalName() string {
	switch d.Type() {
	case ComputeDomainChannelType:
		return d.Channel.Info.CanonicalName()
	}
	panic("unexpected type for PreparedDevice")
}

func (l PreparedDeviceList) GetDevices() []Device {
	var devices []Device
	for _, device := range l {
		if device.Type() == ComputeDomainChannelType && device.Channel.Device != nil {
			devices = append(devices, *device.Channel.Device)
		}
	}
	return devices
}

// GetNonAdminDevices returns a map of device names that were requested
// without admin access in the prepared claim.
func (c *PreparedClaim) GetNonAdminDevices() map[string]struct{} {
	requested := make(map[string]struct{}, len(c.Status.Allocation.Devices.Results))

	if c.Status.Allocation == nil {
		return requested
	}
	for _, r := range c.Status.Allocation.Devices.Results {
		if r.Driver != DriverName {
			continue
		}
		if r.AdminAccess != nil && *r.AdminAccess {
			continue
		}
		requested[r.Device] = struct{}{}
	}
	return requested
}
