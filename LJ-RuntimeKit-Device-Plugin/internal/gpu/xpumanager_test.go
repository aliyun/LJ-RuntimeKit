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
	"testing"
	"time"

	"github.com/lj-runtimekit/device-plugin/internal/plugin"

	"github.com/stretchr/testify/assert"
)

// TestDeviceManagerStopRearmsChannel verifies Stop can run once per plugin
// restart: it closes the channel of the current run and arms an open one for
// the next run instead of closing the same channel twice.
func TestDeviceManagerStopRearmsChannel(t *testing.T) {
	dm := &DeviceManager{stopChan: make(chan struct{})}

	for round := 1; round <= 3; round++ {
		previous := dm.stopChan
		assert.NotPanics(t, dm.Stop, "Stop round %d", round)

		select {
		case <-previous:
		default:
			t.Fatalf("round %d: the stop channel of the previous run is not closed", round)
		}
		select {
		case <-dm.stopChan:
			t.Fatalf("round %d: the stop channel armed for the next run is already closed", round)
		default:
		}
	}
}

// TestWatchUnhealthyFollowsItsOwnRun verifies a restart only stops the health
// check of the previous run: the one started after the restart keeps running
// until its own run is stopped.
func TestWatchUnhealthyFollowsItsOwnRun(t *testing.T) {
	dm := &DeviceManager{
		stopChan:          make(chan struct{}),
		annotationDevices: &GpuDevice{Devices: make(map[int]*Device)},
	}
	unhealthy := make(chan *plugin.DeviceBase)

	watch := func() <-chan struct{} {
		exited := make(chan struct{})
		stop := dm.stopChan
		go func() {
			dm.watchUnhealthy(stop, unhealthy)
			close(exited)
		}()
		return exited
	}

	firstRun := watch()
	dm.Stop() // plugin restart
	select {
	case <-firstRun:
	case <-time.After(time.Second):
		t.Fatal("the health check of the stopped run did not exit")
	}

	secondRun := watch()
	select {
	case <-secondRun:
		t.Fatal("the health check started after the restart exited right away")
	case <-time.After(100 * time.Millisecond):
	}

	dm.Stop() // exit
	select {
	case <-secondRun:
	case <-time.After(time.Second):
		t.Fatal("the health check of the second run did not exit on its own stop")
	}
}
