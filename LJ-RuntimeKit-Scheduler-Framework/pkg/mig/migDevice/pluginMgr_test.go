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
	"testing"

	ampcache "github.com/lj-runtimekit/scheduler/pkg/cache"

	"github.com/stretchr/testify/assert"
)

func TestRegisterMigModePlugin(t *testing.T) {
	// Save and restore original state
	orig := VirtualModeFactory
	defer func() { VirtualModeFactory = orig }()

	VirtualModeFactory = nil

	// Register a new plugin
	RegisterMigModePlugin("test-mode", func(device *ampcache.GpuDevice) VirtualMode { return nil })

	assert.NotNil(t, VirtualModeFactory)
	assert.Contains(t, VirtualModeFactory, "test-mode")

	// Register another plugin
	RegisterMigModePlugin("another-mode", func(device *ampcache.GpuDevice) VirtualMode { return nil })
	assert.Contains(t, VirtualModeFactory, "another-mode")
	assert.Len(t, VirtualModeFactory, 2)
}

func TestGetMigDeviceModePlugin(t *testing.T) {
	// Save and restore original state
	orig := VirtualModeFactory
	defer func() { VirtualModeFactory = orig }()

	VirtualModeFactory = nil
	RegisterMigModePlugin("get-test", func(device *ampcache.GpuDevice) VirtualMode { return nil })

	factory, err := GetMigDeviceModePlugin("get-test")
	assert.NoError(t, err)
	assert.NotNil(t, factory)

	// Get non-existent plugin
	_, err = GetMigDeviceModePlugin("non-existent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "can not find the device mode")
}
