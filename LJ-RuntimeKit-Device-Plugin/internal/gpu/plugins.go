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
	"fmt"

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

func NewPlugins(deviceType string, deviceMeta *utils.DeviceMeta, k8sClient kubernetes.Interface) []xpuplugin.PluginInterface {
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

// startPlugins starts the per resource plugins. withRdma decides whether the
// rdma token plugin takes part: it is only meaningful when xdm advertised a
// pcie switch level, which is what the scheduler picks from and what xdm needs
// to resolve the rdma devices of an allocation.
func startPlugins(deviceType string, deviceMeta *utils.DeviceMeta, k8sClient kubernetes.Interface, withRdma bool) ([]xpuplugin.PluginInterface, bool, error) {
	plugins := NewPlugins(deviceType, deviceMeta, k8sClient)

	if withRdma {
		klog.Infof("xdm reported pcie switch topology, starting RDMA token plugin")
		rdmaPlugin, err := xpuplugin.DevicePluginForResource(utils.ResourceRDMA, utils.RDMASocket, utils.AnnotationRdmaAssignedFlag, nil, k8sClient)
		if err != nil {
			klog.Errorf("DevicePluginForResource %s error %s", utils.ResourceRDMA, err.Error())
		} else {
			plugins = append(plugins, rdmaPlugin)
		}
	}

	started, ok := startPluginSet(plugins)
	if !ok {
		return plugins, true, nil
	}

	if started == 0 {
		klog.Info("No devices found. Waiting indefinitely.")
	}

	return plugins, false, nil
}

// startPluginSet starts the plugins in order and stops the ones that already
// started when one of them fails, so a failed round leaves nothing running. A
// survivor would be registered nowhere on failure, and the next round removes
// the same sockets, which turns it into a server serving an unlinked socket
// with no reference left to stop it.
func startPluginSet(plugins []xpuplugin.PluginInterface) (int, bool) {
	started := 0
	for _, plugin := range plugins {
		// Start the gRPC server for plugin p and connect it with the kubelet.
		if err := plugin.Start(); err != nil {
			klog.Errorf("Failed to start plugin: %v", err)
			stopStartedPlugins(plugins[:started])
			return started, false
		}

		started++
	}

	return started, true
}

// stopStartedPlugins rolls back a partially started set, logging instead of
// propagating so one failing Stop cannot leave the rest running.
func stopStartedPlugins(plugins []xpuplugin.PluginInterface) {
	for _, plugin := range plugins {
		if err := plugin.Stop(); err != nil {
			klog.Errorf("Failed to stop plugin %s during rollback: %v", plugin.Name(), err)
		}
	}
}

func stopPlugins(plugins []xpuplugin.PluginInterface) error {
	klog.Info("Stopping plugins.")
	var errs error
	for _, p := range plugins {
		errs = errors.Join(errs, p.Stop())
	}

	return errs
}

// setPlugins installs the plugin set of this run and remembers the rdma one.
func (dm *DeviceManager) setPlugins(plugins []xpuplugin.PluginInterface) {
	dm.pluginsMu.Lock()
	defer dm.pluginsMu.Unlock()

	dm.plugins = plugins
	dm.rdmaPlugin = nil
	for _, p := range plugins {
		if p.Name() == utils.ResourceRDMA {
			dm.rdmaPlugin = p
		}
	}
}

// stopAllPlugins stops every plugin of this run and forgets them, the next run
// rebuilds the set from the topology known at that moment.
func (dm *DeviceManager) stopAllPlugins() error {
	dm.pluginsMu.Lock()
	plugins := dm.plugins
	dm.plugins = make([]xpuplugin.PluginInterface, 0)
	dm.rdmaPlugin = nil
	dm.pluginsMu.Unlock()

	return stopPlugins(plugins)
}

// beginRebuild marks the plugin set as being torn down or rebuilt, see the
// rebuilding field of DeviceManager.
func (dm *DeviceManager) beginRebuild() {
	dm.pluginsMu.Lock()
	defer dm.pluginsMu.Unlock()

	dm.rebuilding = true
}

// endRebuild releases the freeze once the new plugin set is installed.
func (dm *DeviceManager) endRebuild() {
	dm.pluginsMu.Lock()
	defer dm.pluginsMu.Unlock()

	dm.rebuilding = false
}

// snapshotPlugins returns the plugins of the current run. Callers must not
// iterate dm.plugins: the restart loop replaces the slice under pluginsMu.
func (dm *DeviceManager) snapshotPlugins() []xpuplugin.PluginInterface {
	dm.pluginsMu.Lock()
	defer dm.pluginsMu.Unlock()

	plugins := make([]xpuplugin.PluginInterface, len(dm.plugins))
	copy(plugins, dm.plugins)

	return plugins
}

// ensureRDMAStarted starts the rdma plugin when it is not running, which is the
// case when the node booted without a pcie switch topology and xdm published
// the topology later.
func (dm *DeviceManager) ensureRDMAStarted() error {
	dm.pluginsMu.Lock()
	defer dm.pluginsMu.Unlock()

	// the set is being replaced or torn down: starting a plugin now would create
	// one that the run that owns the set never sees and never stops. The caller
	// retries, see the topology sync retry interval.
	if dm.rebuilding {
		return errors.New("plugin set is being rebuilt")
	}

	if dm.rdmaPlugin != nil {
		return nil
	}

	rdmaPlugin, err := xpuplugin.DevicePluginForResource(utils.ResourceRDMA, utils.RDMASocket, utils.AnnotationRdmaAssignedFlag, nil, dm.k8sClient)
	if err != nil {
		return fmt.Errorf("create rdma device plugin: %w", err)
	}

	if err := rdmaPlugin.Start(); err != nil {
		return fmt.Errorf("start rdma device plugin: %w", err)
	}

	klog.Infof("rdma device plugin started after the topology was published")
	dm.rdmaPlugin = rdmaPlugin
	dm.plugins = append(dm.plugins, rdmaPlugin)

	return nil
}

// setRDMAHealth advertises or withdraws the rdma resource without unregistering
// the plugin: kubelet drops unhealthy devices from the allocatable amount and
// restores them once they are healthy again.
func (dm *DeviceManager) setRDMAHealth(healthy bool) {
	dm.pluginsMu.Lock()
	plugin := dm.rdmaPlugin
	dm.pluginsMu.Unlock()

	if plugin == nil {
		return
	}

	setter, ok := plugin.(xpuplugin.HealthSetter)
	if !ok {
		return
	}

	setter.SetHealth(healthy)
}
