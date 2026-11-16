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

import (
	"sync"
	"testing"

	"github.com/lj-runtimekit/device-plugin/internal/utils"

	"github.com/stretchr/testify/assert"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

// SetHealth must flip the whole list both ways and leave at most one pending
// wake up for ListAndWatch.
func TestSetHealthFlipsEveryDevice(t *testing.T) {
	dp := &XpuDevicePlugin{
		ResourceName: utils.ResourceRDMA,
		Devs: map[string]*pluginapi.Device{
			"rdma-token-0": {ID: "rdma-token-0", Health: pluginapi.Healthy},
			"rdma-token-1": {ID: "rdma-token-1", Health: pluginapi.Healthy},
		},
		stop:   make(chan struct{}),
		health: make(chan struct{}, 1),
	}

	dp.SetHealth(false)

	for _, dev := range dp.Devs {
		assert.Equal(t, pluginapi.Unhealthy, dev.Health)
	}
	assert.Len(t, dp.health, 1)

	// a second call must not queue another wake up
	dp.SetHealth(false)
	assert.Len(t, dp.health, 1)

	dp.SetHealth(true)

	for _, dev := range dp.Devs {
		assert.Equal(t, pluginapi.Healthy, dev.Health)
	}
}

// A stopped plugin may still be reached by the topology sync, it must neither
// block nor panic on the closed stop channel.
func TestSetHealthAfterStop(t *testing.T) {
	dp := &XpuDevicePlugin{
		ResourceName: utils.ResourceRDMA,
		Devs: map[string]*pluginapi.Device{
			"rdma-token-0": {ID: "rdma-token-0", Health: pluginapi.Healthy},
		},
		stop:   make(chan struct{}),
		health: make(chan struct{}, 1),
	}
	close(dp.stop)

	dp.SetHealth(false)

	assert.Equal(t, pluginapi.Unhealthy, dp.Devs["rdma-token-0"].Health)
	assert.Len(t, dp.health, 0)
}

// The caller owns the returned devices: mutating them must not reach the state
// that SetHealth and ListAndWatch share.
func TestGetPluginDevicesReturnsACopy(t *testing.T) {
	dp := &XpuDevicePlugin{
		ResourceName: utils.ResourceRDMA,
		Devs: map[string]*pluginapi.Device{
			"rdma-token-0": {ID: "rdma-token-0", Health: pluginapi.Healthy},
		},
	}

	devices := dp.GetPluginDevices()
	if !assert.Len(t, devices, 1) {
		return
	}

	devices[0].Health = pluginapi.Unhealthy
	devices[0].ID = "rewritten"

	assert.Equal(t, pluginapi.Healthy, dp.Devs["rdma-token-0"].Health)
	assert.Equal(t, "rdma-token-0", dp.Devs["rdma-token-0"].ID)
}

// ListAndWatch serialises the list outside of devsMu while the topology sync
// rewrites the health of the same devices, so the snapshot must not share them.
func TestGetPluginDevicesIsConcurrentSafe(t *testing.T) {
	dp := &XpuDevicePlugin{
		ResourceName: utils.ResourceRDMA,
		Devs: map[string]*pluginapi.Device{
			"rdma-token-0": {ID: "rdma-token-0", Health: pluginapi.Healthy},
			"rdma-token-1": {ID: "rdma-token-1", Health: pluginapi.Healthy},
		},
		stop:   make(chan struct{}),
		health: make(chan struct{}, 1),
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			dp.SetHealth(i%2 == 0)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			for _, device := range dp.GetPluginDevices() {
				_ = device.Health
			}
		}
	}()

	wg.Wait()
}
