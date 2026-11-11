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

import (
	resourceapi "k8s.io/api/resource/v1"
)

type AllocatableDevices map[string]*AllocatableDevice

type AllocatableDevice struct {
	Channel *ComputeDomainChannelInfo
}

func (d AllocatableDevice) Type() string {
	if d.Channel != nil {
		return ComputeDomainChannelType
	}
	return UnknownDeviceType
}

func (d *AllocatableDevice) CanonicalName() string {
	switch d.Type() {
	case ComputeDomainChannelType:
		return d.Channel.CanonicalName()
	}
	panic("unexpected type for AllocatableDevice")
}

func (d *AllocatableDevice) GetDevice() resourceapi.Device {
	switch d.Type() {
	case ComputeDomainChannelType:
		return d.Channel.GetDevice()
	}
	panic("unexpected type for AllocatableDevice")
}
