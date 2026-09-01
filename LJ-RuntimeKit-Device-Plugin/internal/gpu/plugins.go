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
	"errors"

	"github.com/lj-runtimekit/device-plugin/internal/feature"
	xpuplugin "github.com/lj-runtimekit/device-plugin/internal/plugin"
	"github.com/lj-runtimekit/device-plugin/internal/utils"

	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

func getPluginTypes(deviceType string) []string {
	var pluginTypes []string

	pluginTypes = append(pluginTypes, utils.ResourceCore)
	pluginTypes = append(pluginTypes, utils.ResourceMemory)
	pluginTypes = append(pluginTypes, utils.GetResourceType(deviceType))

	switch deviceType {
	case utils.DeviceTypeGPU:
		pluginTypes = append(pluginTypes, utils.ResourceSlice)
		pluginTypes = append(pluginTypes, utils.ResourceNvidia)
	default:
	}

	return pluginTypes
}

func getResourceSocketAndFlag(resourceName string) (string, string) {
	switch resourceName {
	case utils.ResourceCore:
		return utils.CoreSocket, utils.AnnotationCoreAssignedFlag
	case utils.ResourceMemory:
		return utils.MemorySocket, utils.AnnotationMemoryAssignedFlag
	case utils.ResourcePossibility:
		return utils.PossibilitySocket, utils.AnnotationPossibilityAssignedFlag
	case utils.ResourceSlice:
		return utils.SliceSocket, utils.AnnotationSliceAssignedFlag
	case utils.ResourceGPUCount:
		return utils.CountSocket, utils.AnnotationNormalGpuAssignedFlag
	case utils.ResourceGPU:
		return utils.GPUSocket, utils.AnnotationNormalGpuAssignedFlag
	case utils.ResourceNvidia:
		return utils.NvidiaSocket, utils.AnnotationNormalGpuAssignedFlag
	default:
		return "", ""
	}
}

func NewPlugins(deviceType string, deviceMeta *utils.DeviceMeta, k8sClient *kubernetes.Clientset) []xpuplugin.PluginInterface {
	pluginTypes := getPluginTypes(deviceType)

	var devicePlugins []xpuplugin.PluginInterface
	for _, pluginType := range pluginTypes {
		klog.Infof("found plugin resourceName %s", pluginType)
		socketPath, assignedFlag := getResourceSocketAndFlag(pluginType)
		if socketPath == "" || assignedFlag == "" {
			continue
		}

		plugin, err := xpuplugin.DevicePluginForResource(pluginType, socketPath, assignedFlag, deviceMeta, k8sClient)
		if err != nil {
			klog.Errorf("DevicePluginForResource %s failed: %v", pluginType, err)
			continue
		}

		if ok := utils.DefaultFeatureGate.Enabled(feature.PluginFeatureConvert(pluginType)); ok {
			devicePlugins = append(devicePlugins, plugin)
		}
	}

	return devicePlugins
}

func startPlugins(deviceType string, deviceMeta *utils.DeviceMeta, k8sClient *kubernetes.Clientset) ([]xpuplugin.PluginInterface, bool, error) {
	plugins := NewPlugins(deviceType, deviceMeta, k8sClient)

	// the rdma token plugin is only meaningful when xdm advertised a pcie
	// switch level, which is what the scheduler picks from and what xdm needs
	// to resolve the rdma devices of an allocation
	if HasPcieSwitchTopology(deviceMeta) {
		klog.Infof("xdm reported pcie switch topology, starting RDMA token plugin")
		rdmaPlugin, err := xpuplugin.DevicePluginForResource(utils.ResourceRDMA, utils.RDMASocket, utils.AnnotationRdmaAssignedFlag, nil, k8sClient)
		if err != nil {
			klog.Errorf("DevicePluginForResource %s error %s", utils.ResourceRDMA, err.Error())
		} else {
			plugins = append(plugins, rdmaPlugin)
		}
	}

	started := 0
	for _, plugin := range plugins {
		// Start the gRPC server for plugin p and connect it with the kubelet.
		if err := plugin.Start(); err != nil {
			klog.Errorf("Failed to start plugin: %v", err)
			return plugins, true, nil
		}

		started++
	}

	if started == 0 {
		klog.Info("No devices found. Waiting indefinitely.")
	}

	return plugins, false, nil
}

func stopPlugins(plugins []xpuplugin.PluginInterface) error {
	klog.Info("Stopping plugins.")
	var errs error
	for _, p := range plugins {
		errs = errors.Join(errs, p.Stop())
	}

	return errs
}
