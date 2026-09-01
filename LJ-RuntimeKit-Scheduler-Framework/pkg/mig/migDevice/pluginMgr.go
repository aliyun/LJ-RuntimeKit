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

package migDevice

import (
	"fmt"

	ampcache "github.com/lj-runtimekit/scheduler/pkg/cache"
	"github.com/lj-runtimekit/scheduler/pkg/utils"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// VirtualMode defines the interface for MIG device allocation strategies.
type VirtualMode interface {
	Name() utils.MigDeviceType
	AssumePod(nodeDevice ampcache.AvailableDevices, pod *v1.Pod, nodeInfo *framework.NodeInfo) bool
	AllocatePod(nodeDevice ampcache.AvailableDevices, pod *v1.Pod) (int, uint8, int, int, bool)
}

// NewFactory is a constructor function for creating VirtualMode instances from GpuDevice info.
type NewFactory func(device *ampcache.GpuDevice) VirtualMode

// VirtualModeFactory holds registered MIG mode plugin constructors.
var VirtualModeFactory map[string]NewFactory

// RegisterMigModePlugin registers a MIG mode plugin constructor.
func RegisterMigModePlugin(name string, fn NewFactory) {
	if VirtualModeFactory == nil {
		VirtualModeFactory = make(map[string]NewFactory)
	}
	_, ok := VirtualModeFactory[name]
	if ok {
		klog.Fatalf("device mode %s is registered duplicated", name)
	}
	VirtualModeFactory[name] = fn
	return
}

// GetMigDeviceModePlugin retrieves a registered MIG mode plugin constructor by name.
func GetMigDeviceModePlugin(name string) (NewFactory, error) {
	factory, ok := VirtualModeFactory[name]
	if !ok {
		return nil, fmt.Errorf("can not find the device mode %s check if it is registered", name)
	}
	return factory, nil
}
