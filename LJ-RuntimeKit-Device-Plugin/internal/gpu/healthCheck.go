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
	"context"
	"fmt"
	"time"

	"github.com/lj-runtimekit/device-plugin/internal/plugin"

	"github.com/NVIDIA/go-nvml/pkg/nvml"
	"k8s.io/klog/v2"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

func (dm *DeviceManager) checkHealth(ctx context.Context, unhealthy chan<- *plugin.DeviceBase) error {
	applicationErrorXids := []uint64{
		13, // Graphics Engine Exception
		31, // GPU memory page fault
		43, // GPU stopped processing
		45, // Preemptive cleanup, due to previous errors
		68, // Video processor exception
		//109, // Context Switch Timeout Error
	}

	skippedXids := make(map[uint64]bool)
	for _, id := range applicationErrorXids {
		skippedXids[id] = true
	}

	eventSet, ret := nvml.EventSetCreate()
	if ret != nvml.SUCCESS {
		return fmt.Errorf("failed to create event set: %v", ret)
	}
	defer func() {
		_ = eventSet.Free()
	}()

	deviceCache := make([]*plugin.DeviceBase, 0, len(dm.annotationDevices.Devices))
	for _, device := range dm.annotationDevices.Devices {
		deviceCache = append(deviceCache, &plugin.DeviceBase{
			UUID:  device.UUID,
			Minor: device.Minor,
		})
	}

	const maxRetries = 5
	retryInterval := 3 * time.Second

	eventMask := uint64(nvml.EventTypeXidCriticalError | nvml.EventTypeDoubleBitEccError | nvml.EventTypeSingleBitEccError)
	for _, device := range deviceCache {
		var gpuHandle nvml.Device
		var ret nvml.Return
		for retry := 0; retry < maxRetries; retry++ {
			gpuHandle, ret = nvml.DeviceGetHandleByIndex(int(device.Minor))
			if ret == nvml.SUCCESS {
				break
			}
			klog.Infof("unable to get device handle from minor %d UUID %s, ret %v; retry %d/%d",
				device.Minor, device.UUID, ret, retry+1, maxRetries)
			time.Sleep(retryInterval)
		}
		if ret != nvml.SUCCESS {
			klog.Warningf("unable to get device handle from minor %d UUID %s after %d retries, ret %v; marking it as unhealthy",
				device.Minor, device.UUID, maxRetries, ret)
			unhealthy <- device
			continue
		}

		supportedEvents, ret := gpuHandle.GetSupportedEventTypes()
		if ret != nvml.SUCCESS {
			klog.Infof("unable to determine the supported events for %v: %v; marking it as unhealthy", device.UUID, ret)
			unhealthy <- device
			continue
		}

		ret = gpuHandle.RegisterEvents(eventMask&supportedEvents, eventSet)
		if ret == nvml.ERROR_NOT_SUPPORTED {
			klog.Warningf("Device %v is too old to support healthchecking.", device.UUID)
		} else if ret != nvml.SUCCESS {
			klog.Infof("Marking device %v as unhealthy: %v", device.UUID, ret)
			unhealthy <- device
		}
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		e, ret := eventSet.Wait(5000)
		if ret == nvml.ERROR_TIMEOUT {
			continue
		}
		if ret != nvml.SUCCESS {
			klog.Infof("Error waiting for event: %v; Marking all devices as unhealthy", ret)
			for _, device := range deviceCache {
				unhealthy <- device
			}
			continue
		}

		if e.EventType != nvml.EventTypeXidCriticalError {
			klog.Infof("Skipping non-nvmlEventTypeXidCriticalError event: %+v", e)
			continue
		}

		if _, ok := skippedXids[e.EventData]; ok {
			klog.Infof("Skipping event %+v", e)
			continue
		}

		klog.Infof("Processing event %+v", e)
		eventUUID, ret := e.Device.GetUUID()
		if ret != nvml.SUCCESS {
			// If we cannot reliably determine the device UUID, we mark all devices as unhealthy.
			klog.Infof("Failed to determine uuid for event %v: %v; Marking all devices as unhealthy.", e, ret)
			for _, device := range deviceCache {
				unhealthy <- device
			}
			continue
		}

		for _, device := range deviceCache {
			if device.UUID == eventUUID {
				unhealthy <- device
			}
		}
	}
}

func (dm *DeviceManager) DeviceHealthCheck() {
	ctx, cancel := context.WithCancel(context.Background())
	unhealthy := make(chan *plugin.DeviceBase)

	go func() {
		err := dm.checkHealth(ctx, unhealthy)
		if err != nil {
			klog.Errorf("Failed to start health check: %v; continuing with health checks disabled", err)
		}
	}()

	for {
		select {
		case <-dm.stopChan:
			cancel()
			return
		case device := <-unhealthy:
			if gpuDevice, ok := dm.annotationDevices.Devices[int(device.Minor)]; ok {
				gpuDevice.Health = pluginapi.Unhealthy
			}

			klog.Infof("device %d %s become unhealthy", device.Minor, device.UUID)
			if err := dm.addNodeAnnotation(dm.annotationDevices); err != nil {
				klog.Errorf("failed to add node annotation: %v", err)
			}

			for _, plugin := range dm.plugins {
				plugin.Unhealthy(device)
			}
		}
	}
}
