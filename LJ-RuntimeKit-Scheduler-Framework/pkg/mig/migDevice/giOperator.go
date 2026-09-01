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
	"math"
	"strconv"

	ampcache "github.com/lj-runtimekit/scheduler/pkg/cache"
	"github.com/lj-runtimekit/scheduler/pkg/utils"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// migDeviceGi implements VirtualMode for MIG GI (GPU Instance) mode.
type migDeviceGi struct {
	details            map[uint32]int
	ciSets             *utils.Sets
	pods               []*framework.PodInfo
	majorDriverVersion int
}

func (m *migDeviceGi) Name() utils.MigDeviceType {
	return utils.TypeMigGi
}

func (m *migDeviceGi) AssumePod(nodeDevice ampcache.AvailableDevices, pod *v1.Pod, nodeInfo *framework.NodeInfo) bool {
	gpuSlice := utils.GetResourceReqFromPod(pod, utils.ResourceSlice)
	switch gpuSlice {
	case utils.MigProfileSize1:
	case utils.MigProfileSize2:
	case utils.MigProfileSize3:
	case utils.MigProfileSize4:
	case utils.MigProfileSize7:
	default:
		klog.V(5).Infof("Mig process gi mode assume pod %s/%s ci task req slice %d not support", pod.Namespace, pod.Name, gpuSlice)
		return false
	}

	klog.V(5).Infof("Mig process gi mode assume pod %s/%s gi task req slice %d on node %s", pod.Namespace, pod.Name, gpuSlice, nodeInfo.Node().Name)

	m.pods = nodeInfo.Pods
	migCiSets := utils.New()
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
		if ok && migType == string(utils.TypeMigCi) {
			migCiSets.Insert(ids...)
		}
	}

	m.ciSets = migCiSets
	klog.V(5).Infof("migCiSets current key %v", migCiSets)

	found := false
	for idx, sliceCount := range m.details {
		if sliceCount == 0 {
			continue
		}

		dev, ok := nodeDevice[int(idx)]
		if !ok {
			continue
		}
		if m.ciSets.Has(int(idx)) {
			continue
		}
		var pods = make([]*framework.PodInfo, 0)
		for _, podInfo := range nodeInfo.Pods {
			if utils.NotRunning(podInfo.Pod) || !utils.ResponseForTask(podInfo.Pod) {
				continue
			}

			ids, err := utils.GetAnnotationsFromPodListInt(podInfo.Pod, utils.AnnoDeviceIndex)
			if err != nil {
				klog.Errorf("pod %s/%s get ids from annotations failed due to %v", podInfo.Pod.Namespace, podInfo.Pod.Name, err)
				continue
			}

			for _, id := range ids {
				if id == int(idx) {
					pods = append(pods, podInfo)
				}
			}
		}

		for _, podInCache := range pods {
			klog.V(6).Infof("pod %s/%s current idx %d", podInCache.Pod.Namespace, podInCache.Pod.Name, idx)
		}

		if dev.GetUsedMask().CheckMigGiTopoMatch(gpuSlice, sliceCount, m.majorDriverVersion, pods) {
			klog.V(5).Infof("assume pod %s/%s success", pod.Namespace, pod.Name)
			found = true
			break
		}
	}
	return found
}

func (m *migDeviceGi) AllocatePod(nodeDevice ampcache.AvailableDevices, pod *v1.Pod) (devId int, mask uint8, start int, size int, found bool) {
	klog.V(5).Infof("start mig process gi allocate for pod %s/%s", pod.Namespace, pod.Name)
	gpuSlice := utils.GetResourceReqFromPod(pod, utils.ResourceSlice)
	switch gpuSlice {
	case utils.MigProfileSize1:
	case utils.MigProfileSize2:
	case utils.MigProfileSize3:
	case utils.MigProfileSize4:
	case utils.MigProfileSize7:
	default:
		klog.V(5).Infof("Mig process gi mode allocate pod %s/%s ci task req slice %d not support", pod.Namespace, pod.Name, gpuSlice)
		return
	}

	var (
		selectCiType = 4
		selectIdx    = -1
		selectDevId  = -1
		topoStart    = -1
		topoSize     = -1
		longestTopo  = math.MaxInt32
	)

	for idx, sliceCount := range m.details {
		if sliceCount == 0 {
			continue
		}

		dev, ok := nodeDevice[int(idx)]
		if !ok {
			continue
		}
		if m.ciSets.Has(int(idx)) {
			continue
		}

		var pods = make([]*framework.PodInfo, 0)
		for _, podInfo := range m.pods {
			if utils.NotRunning(podInfo.Pod) || !utils.ResponseForTask(podInfo.Pod) {
				continue
			}

			ids, err := utils.GetAnnotationsFromPodListInt(podInfo.Pod, utils.AnnoDeviceIndex)
			if err != nil {
				klog.Errorf("pod %s/%s get ids from annotations failed due to %v", podInfo.Pod.Namespace, podInfo.Pod.Name, err)
				continue
			}

			for _, id := range ids {
				if id == int(idx) {
					pods = append(pods, podInfo)
				}
			}
		}

		for _, pod := range pods {
			klog.V(6).Infof("pod %s/%s current idx %d", pod.Pod.Namespace, pod.Pod.Name, idx)
		}

		usedMask := dev.GetUsedMask()
		start, size := usedMask.FindLastIdxCiReq(gpuSlice, sliceCount, m.majorDriverVersion, pods)
		if start == -1 {
			continue
		}

		ll := usedMask.GetLongestMatchedTopo(sliceCount)
		klog.V(5).Infof("allocateMigGiTask for pod %s/%s in dev idx %d used mask %v "+
			"slice %d find start %d, size %d, empty topo %d", pod.Namespace, pod.Name, idx, usedMask, sliceCount, start, size, ll)
		found = true
		if ll < longestTopo {
			selectIdx = start
			selectDevId = int(idx)
			selectCiType = sliceCount
			topoStart = start
			topoSize = size
			longestTopo = ll
		} else {
			if ll == longestTopo && start > selectIdx {
				selectIdx = start
				selectDevId = int(idx)
				selectCiType = sliceCount
				topoStart = start
				topoSize = size
			}
		}
	}

	if !found {
		return
	}
	mask = ampcache.BuildMask(selectCiType, topoStart, topoSize)
	devId = selectDevId
	start = topoStart
	size = gpuSlice
	return
}

// NewGiPlugin creates a new GI mode plugin from MIG device info.
func NewGiPlugin(migDevices *ampcache.GpuDevice) VirtualMode {
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
	return &migDeviceGi{
		details:            details,
		ciSets:             new(utils.Sets),
		majorDriverVersion: driverVersion,
	}
}

func init() {
	RegisterMigModePlugin(string(utils.TypeMigGi), NewGiPlugin)
}
