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

package plugin

type PluginInterface interface {
	Name() string
	Start() error
	Stop() error
	Unhealthy(dev *DeviceBase)
}

// HealthSetter is implemented by plugins whose whole device list can be
// flipped between healthy and unhealthy. The topology sync uses it to stop
// advertising the rdma resource after xdm withdrew the topology and to offer
// it again once the topology is published.
type HealthSetter interface {
	SetHealth(healthy bool)
}
