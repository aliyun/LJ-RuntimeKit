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
	"math"
	"strconv"
	"strings"

	ampcache "github.com/lj-runtimekit/scheduler/pkg/cache"
	"github.com/lj-runtimekit/scheduler/pkg/utils"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// migDeviceCi implements VirtualMode for MIG CI (Compute Instance) mode.
type migDeviceCi struct {
	details            map[uint32]int
	giSets             *utils.Sets
	majorDriverVersion int
}

func (m *migDeviceCi) Name() utils.MigDeviceType {
	return utils.TypeMigCi
}

func (m *migDeviceCi) AssumePod(nodeDevice ampcache.AvailableDevices, pod *v1.Pod, nodeInfo *framework.NodeInfo) bool {
	vmemory := utils.GetResourceReqFromPod(pod, utils.ResourceMemory)
	gpuSlice := utils.GetResourceReqFromPod(pod, utils.ResourceSlice)
	switch gpuSlice {
	case utils.MigProfileSize1:
	case utils.MigProfileSize2:
	case utils.MigProfileSize3:
	case utils.MigProfileSize4:
	case utils.MigProfileSize7:
	default:
		klog.V(5).Infof("Mig process ci mode assume pod %s/%s ci task req slice %d not support", pod.Namespace, pod.Name, gpuSlice)
		return false
	}

	klog.V(5).Infof("Mig process ci mode assume pod %s/%s ci task req vmemory %d, slice %d on node %s", pod.Namespace, pod.Name, vmemory, gpuSlice, nodeInfo.Node().Name)

	migGiSets := utils.New()
	for _, podInfo := range nodeInfo.Pods {
		if utils.NotRunning(podInfo.Pod) || !utils.ResponseForTask(podInfo.Pod) {
			continue
		}
		ids, err := utils.GetAnnotationsFromPodListInt(podInfo.Pod, utils.AnnoDeviceIndex)
		if err != nil {
			klog.Errorf("pod %s/%s get ids from annotations failed due to %v", podInfo.Pod.Namespace, podInfo.Pod.Name, err)
			continue
		}
		migType, ok := podInfo.Pod.Annotations[utils.AnnoMigDeviceType]
		if ok && migType == string(utils.TypeMigGi) {
			migGiSets.Insert(ids...)
		}
	}
	m.giSets = migGiSets
	klog.V(6).Infof("migGiSets current key %v", migGiSets)

	found := false
	for idx, sliceCount := range m.details {
		if sliceCount == 0 {
			continue
		}

		dev, ok := nodeDevice[int(idx)]
		if !ok {
			continue
		}
		if migGiSets.Has(int(idx)) {
			continue
		}
		idleVMemory, _ := dev.GetIdleResource()
		if int(idleVMemory) < vmemory {
			continue
		}
		if dev.GetUsedMask().CheckMigTopoMatch(gpuSlice, sliceCount) {
			found = true
			break
		}

	}
	return found
}

func (m *migDeviceCi) AllocatePod(nodeDevice ampcache.AvailableDevices, pod *v1.Pod) (devId int, mask uint8, start int, size int, found bool) {
	klog.V(5).Infof("start mig process ci mode allocate for pod %s/%s", pod.Namespace, pod.Name)

	vmemory := utils.GetResourceReqFromPod(pod, utils.ResourceMemory)
	gpuSlice := utils.GetResourceReqFromPod(pod, utils.ResourceSlice)
	switch gpuSlice {
	case utils.MigProfileSize1:
	case utils.MigProfileSize2:
	case utils.MigProfileSize3:
	case utils.MigProfileSize4:
	case utils.MigProfileSize7:
	default:
		klog.V(5).Infof("Mig process ci mode allocate pod %s/%s ci task req slice %d not support", pod.Namespace, pod.Name, gpuSlice)
		return
	}

	minLen := math.MaxInt32
	selectId := -1
	selectCiCnt := 7
	assignedMask := ampcache.Mask(0)

	for idx, sliceCount := range m.details {
		if sliceCount == 0 {
			continue
		}

		dev, ok := nodeDevice[int(idx)]
		if !ok {
			continue
		}
		if m.giSets.Has(int(idx)) {
			continue
		}
		idleVMemory, _ := dev.GetIdleResource()
		if !dev.GetUsedMask().CheckMigTopoMatch(gpuSlice, sliceCount) || vmemory > int(idleVMemory) {
			continue
		}
		found = true
		ll := dev.GetUsedMask().GetLongestMatchedTopo(sliceCount)
		if ll < minLen {
			minLen = ll
			selectId = int(idx)
			selectCiCnt = sliceCount
			assignedMask = dev.GetUsedMask()
		}
	}

	if !found {
		klog.Errorf("insufficient gpu-mem/gpu-slice for pod %s/%s", pod.Namespace, pod.Name)
		return
	}

	srcStr := fmt.Sprintf("%07b", assignedMask)
	srcStr = srcStr[7-selectCiCnt:]
	targetStr := strings.Repeat("0", gpuSlice)

	klog.V(6).Infof("srcMask %s, reqCis: %s", srcStr, targetStr)

	pos := strings.Index(srcStr, targetStr)
	if pos == -1 {
		klog.Errorf("unexpected error, srcMask %s vs targetStr %s", srcStr, targetStr)
		found = false
		return
	}

	mask = ampcache.BuildMask(selectCiCnt, pos, gpuSlice)
	devId = selectId
	size = gpuSlice
	start = pos
	return
}

// NewCiPlugin creates a new CI mode plugin from MIG device info.
func NewCiPlugin(migDevices *ampcache.GpuDevice) VirtualMode {
	details := make(map[uint32]int)
	driverVersion := 0
	for _, device := range migDevices.Devices {
		if !device.MigEnable || device.Health != utils.DeviceHealth {
			continue
		}

		details[uint32(device.Minor)] = int(device.Slice)

		if driverVersion == 0 {
			versionStr, ok := device.Extensions[utils.ResourceDriverVersion]
			if ok {
				version, err := strconv.Atoi(versionStr)
				if err != nil {
					klog.Errorf("convert device %d version %s to int failed with error: %v", device.Minor, versionStr, err)
				} else {
					driverVersion = version
				}
			}
		}
	}
	return &migDeviceCi{
		details:            details,
		giSets:             new(utils.Sets),
		majorDriverVersion: driverVersion,
	}
}

func init() {
	RegisterMigModePlugin(string(utils.TypeMigCi), NewCiPlugin)
}
