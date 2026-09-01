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

package utils

import (
	"fmt"
	"strings"
	"sync"

	xpugrpc "github.com/lj-runtimekit/device-plugin/internal/grpc"

	"k8s.io/klog/v2"
)

var (
	gpuMemory uint
	metric    MemoryUnit
)

func SetMetric(m MemoryUnit) {
	metric = m
}

func GetMetric() MemoryUnit {
	return metric
}

func SetGPUMemory(raw uint) {
	v := raw
	if metric == GiBPrefix {
		v = raw / 1024
	} else {
		v = raw / 128
	}
	gpuMemory = v
	klog.Infof("set gpu memory: %d", gpuMemory)
}

func ConvertGPUMemory(raw uint) uint {
	v := raw
	if metric == GiBPrefix {
		v = raw / 1024
	} else {
		v = raw / 128
	}

	klog.Infof("convert gpu memory: %d", v)
	return v
}

func ExtractMigRealDeviceID(fakeDeviceID string) string {
	fakeID := strings.Split(fakeDeviceID, "-_-")[0]
	return strings.SplitN(fakeID, "-", 2)[1]
}

// fakeIDPrefixes are the prefixes of the fake device ids reported to kubelet
// in ListAndWatch, see GenerateFakeCoreID / GenerateFakeMemoryID /
// GenerateFakeMigID / GenerateFakePossibilityID.
var fakeIDPrefixes = []string{"COMP-", "MEM-", "MIG-", "POSSI-"}

// extractRealDeviceID recovers the xdm device id embedded in a kubelet
// assigned device id. Raw ids are returned unchanged.
func extractRealDeviceID(deviceID string) string {
	for _, prefix := range fakeIDPrefixes {
		if !strings.HasPrefix(deviceID, prefix) {
			continue
		}
		rest := deviceID[len(prefix):]
		// strip the fake counter suffix: mig ids carry "-_-<ciNum>_<counter>",
		// other fake ids carry "-_<counter>"
		if idx := strings.Index(rest, "-_-"); idx >= 0 {
			return rest[:idx]
		}
		if idx := strings.LastIndex(rest, "-_"); idx >= 0 {
			return rest[:idx]
		}
		return rest
	}
	return deviceID
}

// GetDeviceMinorFromID converts a kubelet assigned device id into a minor
// by reverse-looking it up in the ListAndWatch cache.
func GetDeviceMinorFromID(deviceMeta *DeviceMeta, deviceID string) (int, error) {
	// the default branch of initialize reports dev.Id as-is, which is the
	// real gpu uuid; resolve it directly first
	if minor, ok := deviceMeta.GetMinorByDeviceID(deviceID); ok {
		return minor, nil
	}
	// fake ids (core/memory/mig/possibility) embed dev.Id between a known
	// prefix and a counter suffix; strip them and reverse-lookup again
	realID := extractRealDeviceID(deviceID)
	minor, ok := deviceMeta.GetMinorByDeviceID(realID)
	if !ok {
		return 0, fmt.Errorf("device id %s (real id %s) not found in reported devices", deviceID, realID)
	}
	return minor, nil
}

// GetDeviceMinorsFromIDs converts a batch of kubelet assigned device ids
// into deduplicated device minors, keeping first-seen order.
func GetDeviceMinorsFromIDs(deviceMeta *DeviceMeta, deviceIDs []string) ([]int, error) {
	minors := make([]int, 0, len(deviceIDs))
	seen := make(map[int]struct{}, len(deviceIDs))
	for _, deviceID := range deviceIDs {
		minor, err := GetDeviceMinorFromID(deviceMeta, deviceID)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[minor]; ok {
			continue
		}
		seen[minor] = struct{}{}
		minors = append(minors, minor)
	}
	return minors, nil
}

func ExtractEGPURealDeviceID(fakeDeviceID string) string {
	fakeID := strings.Split(fakeDeviceID, "-_-")[0]
	return strings.SplitN(fakeID, "-", 2)[1]
}

func GetGPUMemory() uint {
	return gpuMemory
}

func GenerateFakeMemoryID(realID string, fakeCounter uint) string {
	return fmt.Sprintf("%s-%s-_-%d", "MEM", realID, fakeCounter)
}

func GenerateFakeCoreID(realID string, fakeCounter uint) string {
	return fmt.Sprintf("%s-%s-_-%d", "COMP", realID, fakeCounter)
}

func GenerateFakeMigID(realID string, ciNum, fakeCounter uint) string {
	return fmt.Sprintf("%s-%s-_-%d_%d", "MIG", realID, ciNum, fakeCounter)
}

func GenerateFakePossibilityID(realID string, fakeCounter uint) string {
	return fmt.Sprintf("%s-%s-_-%d", "POSSI", realID, fakeCounter)
}

func GetGPUCore() uint {
	return uint(100)
}

type DeviceMeta struct {
	sync.RWMutex
	Cache map[int]*xpugrpc.Device
}

func NewDeviceMeta() *DeviceMeta {
	nda := new(DeviceMeta)
	nda.Cache = make(map[int]*xpugrpc.Device)
	return nda
}
func (d *DeviceMeta) Get(id int) (*xpugrpc.Device, bool) {
	d.RLock()
	defer d.RUnlock()
	res, ok := d.Cache[id]
	if !ok {
		return nil, false
	}
	return res, true
}

// GetMinorByDeviceID reverse-resolves the device minor from the xdm device
// id (dev.Id) reported in ListAndWatch.
func (d *DeviceMeta) GetMinorByDeviceID(deviceID string) (int, bool) {
	d.RLock()
	defer d.RUnlock()
	for minor, dev := range d.Cache {
		if dev != nil && dev.Id == deviceID {
			return minor, true
		}
	}
	return 0, false
}

func (d *DeviceMeta) Put(id int, res *xpugrpc.Device) {
	d.Lock()
	defer d.Unlock()
	d.Cache[id] = res
	return
}

func (d *DeviceMeta) Delete(id int) {
	d.Lock()
	defer d.Unlock()
	delete(d.Cache, id)
	return
}
