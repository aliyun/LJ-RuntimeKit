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

package standard

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	ampcache "github.com/lj-runtimekit/scheduler/pkg/cache"
	"github.com/lj-runtimekit/scheduler/pkg/utils"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

const (
	Name = "Standard"
)

var _ framework.PreFilterPlugin = &NormalStandardPlugin{}
var _ framework.FilterPlugin = &NormalStandardPlugin{}
var _ framework.ScorePlugin = &NormalStandardPlugin{}
var _ framework.ReservePlugin = &NormalStandardPlugin{}
var _ framework.PreBindPlugin = &NormalStandardPlugin{}
var _ framework.PostBindPlugin = &NormalStandardPlugin{}

type NormalStandardPlugin struct {
	fh framework.Handle
}

func (n *NormalStandardPlugin) Name() string {
	return Name
}

func New(obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	klog.V(6).Infof("begin to new standard plugin")

	n := &NormalStandardPlugin{
		fh: handle,
	}
	return n, nil
}

func (n *NormalStandardPlugin) PreFilter(ctx context.Context, state *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	nodeInfos, err := n.fh.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		klog.Errorf("pod %s/%s in preFilter stage list nodeInfos failed due to %v", pod.Namespace, pod.Name, err)
		return nil, framework.AsStatus(err)
	}
	var clusterDevices = make(ampcache.AvailableClusterDevices)
	for _, nodeInfo := range nodeInfos {
		node := nodeInfo.Node()
		if node == nil {
			continue
		}
		clusterDevices[node.Name] = ampcache.BuildCache(nodeInfo)
	}
	state.Write(ampcache.ClusterDevicesStateKey, clusterDevices)

	vmemory := utils.GetResourceReqFromPod(pod, utils.ResourceMemory)
	vcore := utils.GetResourceReqFromPod(pod, utils.ResourceCore)
	vslice := utils.GetResourceReqFromPod(pod, utils.ResourceSlice)

	vReplicas := utils.GetLabelsFromPodInt(pod, utils.LabelsReplicas)
	if vReplicas == 0 {
		vReplicas = 1
	}

	normalGpuReqCnt := utils.GetResourceReqFromPod(pod, utils.ResourceGPU)
	if normalGpuReqCnt <= 0 {
		normalGpuReqCnt = utils.GetResourceReqFromPod(pod, utils.ResourceNvidia)
		if normalGpuReqCnt <= 0 {
			normalGpuReqCnt = utils.GetResourceReqFromPod(pod, utils.ResourceCount)
		}
	}

	if vcore > 100 && vcore%100 != 0 {
		return nil, framework.NewStatus(framework.Error, fmt.Sprintf("invalid argument vcore %d", vcore))
	}

	if vcore >= 100 {
		normalGpuReqCnt = vcore / 100
	}

	klog.V(4).Infof("pod %s/%s req vmem %d, vcore %d, vslice %d, normalGpu %d replicas %d", pod.Namespace, pod.Name, vmemory, vcore, vslice, normalGpuReqCnt, vReplicas)

	state.Write(ampcache.VStandardTaskReqStateKey, ampcache.StandardTaskReq{
		Vmem:            vmemory,
		Vcore:           vcore,
		Vslice:          vslice,
		VReplicas:       vReplicas,
		NormalGpuReqCnt: normalGpuReqCnt,
	})

	clusterNodeMigDevices := make(ampcache.ClusterNodeMigDevices)
	for _, nodeInfo := range nodeInfos {
		node := nodeInfo.Node()
		if node == nil {
			continue
		}

		migDevice := ampcache.GpuDevice{
			Count:   0,
			Devices: make(map[int]*ampcache.Device),
		}

		migDeviceAnnotationJson, ok := node.Annotations[utils.AnnotationDevice]
		if !ok {
			continue
		}

		if err := json.Unmarshal([]byte(migDeviceAnnotationJson), &migDevice); err != nil {
			klog.V(6).Infof("node %s mig device json unmarshal %s failed with error: %s", node.Name, migDeviceAnnotationJson, err.Error())
			continue
		}

		clusterNodeMigDevices[node.Name] = &migDevice
	}

	state.Write(ampcache.ClusterMigDeviceStateKey, clusterNodeMigDevices)

	return nil, framework.NewStatus(framework.Success, "")
}

func (n *NormalStandardPlugin) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

func (n *NormalStandardPlugin) Filter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	node := nodeInfo.Node()
	if node == nil {
		return framework.NewStatus(framework.Error, "node not found")
	}
	clusterDevices, clusterMigDevices, standardReq, err := ampcache.GetPreFilterStateData(state)
	if err != nil {
		klog.Errorf("pod %s/%s in filter stage getPreFilterStateData failed due to %v", pod.Namespace, pod.Name, err)
		return framework.AsStatus(err)
	}

	klog.V(5).Infof("pod %s/%s request %v", pod.Namespace, pod.Name, standardReq)

	migDevices, ok := clusterMigDevices[node.Name]
	if !ok {
		migDevices = &ampcache.GpuDevice{
			Devices: make(map[int]*ampcache.Device),
		}
	}

	availableDevices, ok := clusterDevices[node.Name]
	if !ok {
		return framework.NewStatus(framework.Error, fmt.Sprintf("can not found node %s in normal cluster cache", node.Name))
	}

	if klog.V(6).Enabled() {
		for idx, device := range availableDevices {
			imem, ivcore := device.GetIdleResource()
			klog.Infof("Filter stage: pod %s/%s available Dev %d %d in node %s, dev %d", pod.Namespace, pod.Name, imem, ivcore, node.Name, idx)
		}
	}

	klog.V(5).Infof("check if pod %s/%s can be scheduled in node %s", pod.Namespace, pod.Name, node.Name)

	found := false
	switch {
	case standardReq.NormalGpuReqCnt > 0:
		gpus := 0
		for idx, dev := range availableDevices {
			if migDevices.Has(idx) {
				continue
			}

			if dev.IdleDevice() {
				gpus++
			}
		}
		found = gpus >= standardReq.NormalGpuReqCnt
	default:

		vslice := standardReq.Vslice
		if vslice > 0 {
			klog.V(5).Infof("pod %s/%s is requesting Mig resources, which will enter the Mig filter process", pod.Namespace, pod.Name)
			return framework.NewStatus(framework.Success, "standard reserve process pod %s is a mig pod", pod.Name)
		}

		if standardReq.Vcore == 0 && standardReq.Vmem == 0 {
			klog.V(5).Infof("pod %s/%s req core %d memory %d slice %d normalGpuReqCnt %d all is 0", pod.Namespace, pod.Name, standardReq.Vcore, standardReq.Vmem, standardReq.Vslice, standardReq.NormalGpuReqCnt)
			return framework.NewStatus(framework.Success, "standard reserve process pod %s/%s is cpu pod", pod.Namespace, pod.Name)
		}

		devsReplicas := 0
		for idx, dev := range availableDevices {
			if migDevices.Has(idx) {
				continue
			}

			idleVMemory, idleVCore := dev.GetIdleResource()
			totalMem, totalCore := dev.GetTotalResource()
			realTotalMem, realTotalCore := int(totalMem), int(totalCore)
			if idleVMemory >= int64(standardReq.Vmem) &&
				idleVCore >= int64(standardReq.Vcore) &&
				realTotalMem >= standardReq.Vmem &&
				realTotalCore >= standardReq.Vcore {
				devsReplicas++
			}
		}
		found = devsReplicas >= standardReq.VReplicas
	}

	if found {
		klog.V(4).Infof("pod %s/%s can be scheduled in node %s", pod.Namespace, pod.Name, node.Name)
		return framework.NewStatus(framework.Success, "")
	}
	klog.V(2).Infof("pod %s/%s can not be scheduled in node %s due to insufficient VCore or VMemory", pod.Namespace, pod.Name, node.Name)
	return framework.NewStatus(framework.Unschedulable, "insufficient VCore or VMemory")
}

func (n *NormalStandardPlugin) Reserve(ctx context.Context, state *framework.CycleState, p *v1.Pod, nodeName string) *framework.Status {
	nodeInfo, err := n.fh.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil {
		return framework.AsStatus(err)
	}
	if nodeInfo.Node() == nil {
		return framework.NewStatus(framework.Error, "node not found")
	}
	clusterDevices, migClusterDevices, standardReq, err := ampcache.GetPreFilterStateData(state)
	if err != nil {
		klog.Errorf("pod %s/%s in Reserve stage getPreFilterStateData failed due to %v", p.Namespace, p.Name, err)
		return framework.AsStatus(err)
	}
	availableDevices, ok := clusterDevices[nodeName]
	if !ok {
		return framework.NewStatus(framework.Error, fmt.Sprintf("can not found the %s in normalCluster cache", nodeName))
	}

	migDevices, ok := migClusterDevices[nodeName]
	if !ok {
		migDevices = &ampcache.GpuDevice{
			Devices: make(map[int]*ampcache.Device),
		}
	}

	if klog.V(6).Enabled() {
		for idx, device := range availableDevices {
			imem, ivcore := device.GetIdleResource()
			klog.Infof("Reserve stage: pod %s/%s available Dev %d %d in node %s, dev %d", p.Namespace, p.Name, imem, ivcore, nodeName, idx)
		}
	}

	strategy, _ := utils.GetLabelsFromNodeStr(nodeInfo.Node(), utils.LabelNodeScheduleStrategy)
	defaultStrategy := utils.ScheduleStrategyBinpack
	switch strategy {
	case utils.ScheduleStrategyBinpack, utils.ScheduleStrategySpread:
		defaultStrategy = strategy
	default:
	}

	klog.V(5).Infof("using strategy %s to reserve pod %s/%s", defaultStrategy, p.Namespace, p.Name)
	klog.V(5).Infof("pod %s/%s in reserve stage found standard Task Req %+v", p.Namespace, p.Name, *standardReq)

	vmemory := standardReq.Vmem
	vcore := standardReq.Vcore
	normalGpuReqCnt := standardReq.NormalGpuReqCnt
	vReplicas := int(math.Max(float64(1), float64(standardReq.VReplicas)))

	klog.V(4).Infof("pod %s/%s in reserve stage get pods args %d, %d, %d, %d", p.Namespace, p.Name, vmemory, vcore, normalGpuReqCnt, vReplicas)
	candidateDeviceIdList := make([]int, 0)
	found := false
	normalGpuTask := false
	var pcieSwitchIndexStr string
	switch {
	case normalGpuReqCnt > 0:
		normalGpuTask = true
		// Skip RDMA logic when the node lacks RDMA topology or the pod does not request it.
		rdmaTopo := getRdmaTopoFromNode(nodeInfo.Node())
		podRequestsRDMA := podHasRDMARequest(p)

		if rdmaTopo != nil && podRequestsRDMA {
			// PPU 1.5 RDMA topology path (1-8 whole cards, no MIG).
			if topoSelected, switches, ok := tryRdmaTopoReserve(availableDevices, rdmaTopo, normalGpuReqCnt); ok {
				candidateDeviceIdList = topoSelected
				found = true
				pcieSwitchIndexStr = joinIntSlice(switches)
				klog.V(4).Infof("pod %s/%s rdma topo reserve selected GPUs %v, pcie switches %s", p.Namespace, p.Name, candidateDeviceIdList, pcieSwitchIndexStr)
			}
		} else if normalGpuReqCnt == 2 || normalGpuReqCnt == 4 || normalGpuReqCnt == 8 {
			// PPU 810E topology path (2/4/8 whole cards only).
			if topoSelected, ok := tryTopoReserve(availableDevices, migDevices, normalGpuReqCnt, defaultStrategy); ok {
				candidateDeviceIdList = topoSelected
				found = true
				klog.V(4).Infof("pod %s/%s topo reserve selected devices %v", p.Namespace, p.Name, candidateDeviceIdList)
				// Derive PCIe switches for PPU810e RDMA when requested.
				if podRequestsRDMA && rdmaTopo != nil {
					switches := deriveSwitchIndexesForGPUs(rdmaTopo, topoSelected)
					if len(switches) > 0 {
						pcieSwitchIndexStr = joinIntSlice(switches)
						klog.V(4).Infof("pod %s/%s PPU810e RDMA: selected pcie switches %s for GPUs %v", p.Namespace, p.Name, pcieSwitchIndexStr, topoSelected)
					}
				}
			}
		}
		// Fall back to original logic if no topology group satisfies the request.
		if !found {
			for idx, dev := range availableDevices {
				if migDevices.Has(idx) {
					continue
				}
				if dev.IdleDevice() {
					candidateDeviceIdList = append(candidateDeviceIdList, idx)
				}
			}
		}
		// Fallback: collect RDMA switches for the GPUs that will actually be assigned.
		if podRequestsRDMA && rdmaTopo != nil && pcieSwitchIndexStr == "" && len(candidateDeviceIdList) >= normalGpuReqCnt {
			assignedGpus := candidateDeviceIdList[:normalGpuReqCnt]
			switches := deriveSwitchIndexesForGPUs(rdmaTopo, assignedGpus)
			if len(switches) > 0 {
				pcieSwitchIndexStr = joinIntSlice(switches)
				klog.V(4).Infof("pod %s/%s RDMA fallback: topology group unsatisfied, selected pcie switches %s for GPUs %v", p.Namespace, p.Name, pcieSwitchIndexStr, assignedGpus)
			}
		}
	default:
		if standardReq.Vslice > 0 {
			klog.V(5).Infof("pod %s/%s is requesting Mig resources, which will enter the Mig Reserve process", p.Namespace, p.Name)
			return framework.NewStatus(framework.Success, "standard reserve process pod %s is a mig pod", p.Name)
		}

		if vmemory == 0 && vcore == 0 {
			klog.V(5).Infof("pod %s/%s req core %d memory %d slice %d normalGpuReqCnt %d all is 0", p.Namespace, p.Name, vcore, vmemory, standardReq.Vslice, normalGpuReqCnt)
			return framework.NewStatus(framework.Success, "standard reserve process pod %s/%s is cpu pod", p.Namespace, p.Name)
		}

		// For 2/4/8 virtual card requests, prefer devices in the same topology group.
		// When multiple groups exist, pick the best according to defaultStrategy.
		if vReplicas == 2 || vReplicas == 4 || vReplicas == 8 {
			if topoSelected, ok := tryVirtualTopoReserve(availableDevices, migDevices, vReplicas, int64(vmemory), int64(vcore), defaultStrategy); ok {
				candidateDeviceIdList = topoSelected
				found = true
				klog.V(4).Infof("pod %s/%s (virtual card) topo reserve selected devices %v", p.Namespace, p.Name, candidateDeviceIdList)
			}
		}
		// Fall back to original logic if no topology group satisfies the request.
		if !found {
			staffGpuList := make([]staffGPU, 0)
			for idx, dev := range availableDevices {
				if migDevices.Has(idx) {
					continue
				}
				totalVMemory, totalVCore := dev.GetTotalResource()
				idleVMemory, idleVCore := dev.GetIdleResource()
				if idleVMemory < int64(vmemory) || idleVCore < int64(vcore) {
					continue
				}
				staff := staffGPU{
					devId: idx,
					available: &gpuResources{
						memory:  idleVMemory,
						compute: idleVCore,
					},
					total: &gpuResources{
						memory:  totalVMemory,
						compute: totalVCore,
					},
				}
				staffGpuList = append(staffGpuList, staff)
			}
			if defaultStrategy == utils.ScheduleStrategyBinpack {
				sort.Sort(resourceList(staffGpuList))
			} else {
				sort.Sort(reverseResourceList(staffGpuList))
			}
			for _, st := range staffGpuList {
				candidateDeviceIdList = append(candidateDeviceIdList, st.devId)
			}
			klog.V(4).Infof("pod %s/%s want %d replicas and get %v", p.Namespace, p.Name, vReplicas, staffGpuList)
		}
	}

	if normalGpuTask {
		found = len(candidateDeviceIdList) >= normalGpuReqCnt
	} else {
		found = len(candidateDeviceIdList) >= vReplicas
	}

	if !found {
		return framework.NewStatus(framework.Unschedulable, "cannot found the candidate dev")
	}

	patchMap := make(map[string]string)
	if pcieSwitchIndexStr != "" {
		patchMap[utils.AnnotationPcieSwitchIndex] = pcieSwitchIndexStr
	}
	flags := make([]assignedFlag, 0)
	if !normalGpuTask {
		if vcore != 0 {
			patchMap[utils.AnnoCoreResourceByPod] = fmt.Sprintf("%d", vcore)
			flags = append(flags, vcoreAssigned)
		}
		if vmemory != 0 {
			patchMap[utils.AnnoMemoryResourceByPod] = fmt.Sprintf("%d", vmemory)
			flags = append(flags, vmemAssigned)
		}
		candidateDeviceIdList = candidateDeviceIdList[:vReplicas]
	} else {
		flags = append(flags, normalAssigned)
		candidateDeviceIdList = candidateDeviceIdList[:normalGpuReqCnt]
	}
	klog.V(5).Infof("found the candidate ids list %v patchMap %v flagList %v for pod %s/%s", candidateDeviceIdList, patchMap, flags, p.Namespace, p.Name)

	patchedAnnotationBytes, err := patchPodAssigned(candidateDeviceIdList, patchMap, flags...)
	if err != nil {
		return framework.NewStatus(framework.Error, fmt.Sprintf("build patch byte failed due to %v", err))
	}

	var newPod *v1.Pod
	kubeClient := n.fh.ClientSet()
	if err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		var err error
		newPod, err = kubeClient.CoreV1().Pods(p.Namespace).Patch(context.TODO(), p.Name, types.StrategicMergePatchType, patchedAnnotationBytes, metav1.PatchOptions{})
		return err
	}); err != nil {
		klog.Errorf("patch pod %s/%s failed due to %v", p.Namespace, p.Name, err)
		return framework.NewStatus(framework.Error, fmt.Sprintf("patch pod %s/%s failed due to %v", p.Namespace, p.Name, err))
	}
	p.Annotations = newPod.Annotations
	klog.V(5).Infof("pod %s/%s patch annotations %v", p.Namespace, p.Name, p.Annotations)
	return framework.NewStatus(framework.Success, "")
}

// filterTopologyGroups finds all topology groups where all devices are non-MIG and idle.
// Each group has exactly numCards devices.
func findIdleTopologyGroups(availableDevices map[int]*ampcache.DeviceInfo, migDevices *ampcache.GpuDevice, numCards int) [][]int {
	groupSet := make(map[string][]int)

	for _, dev := range availableDevices {
		for _, link := range dev.GetTopologyInfo() {
			parts := strings.Split(link.PeerMinor, ",")
			if len(parts) != numCards {
				continue
			}

			minors := make([]int, 0, numCards)
			valid := true
			for _, p := range parts {
				m, err := strconv.Atoi(strings.TrimSpace(p))
				if err != nil {
					valid = false
					break
				}
				minors = append(minors, m)
			}
			if !valid {
				continue
			}

			// Dedup by sorted key.
			sort.Ints(minors)
			key := fmt.Sprintf("%v", minors)
			if _, exists := groupSet[key]; exists {
				continue
			}

			// Verify that all devices in the group are non-MIG and idle.
			allIdle := true
			for _, m := range minors {
				if migDevices.Has(m) {
					allIdle = false
					break
				}
				d, ok := availableDevices[m]
				if !ok || !d.IdleDevice() {
					allIdle = false
					break
				}
			}

			if allIdle {
				groupSet[key] = minors
			}
		}
	}

	result := make([][]int, 0, len(groupSet))
	for _, group := range groupSet {
		result = append(result, group)
	}
	return result
}

// findResourceTopologyGroups finds topology groups where all devices have enough idle resources
// to satisfy the virtual card resource request.
func findResourceTopologyGroups(availableDevices map[int]*ampcache.DeviceInfo, migDevices *ampcache.GpuDevice, numCards int, reqVmem, reqVcore int64) [][]int {
	groupSet := make(map[string][]int)

	for _, dev := range availableDevices {
		for _, link := range dev.GetTopologyInfo() {
			parts := strings.Split(link.PeerMinor, ",")
			if len(parts) != numCards {
				continue
			}

			minors := make([]int, 0, numCards)
			valid := true
			for _, p := range parts {
				m, err := strconv.Atoi(strings.TrimSpace(p))
				if err != nil {
					valid = false
					break
				}
				minors = append(minors, m)
			}
			if !valid {
				continue
			}

			sort.Ints(minors)
			key := fmt.Sprintf("%v", minors)
			if _, exists := groupSet[key]; exists {
				continue
			}

			// Verify that devices are non-MIG and have sufficient resources.
			allValid := true
			for _, m := range minors {
				if migDevices.Has(m) {
					allValid = false
					break
				}
				d, ok := availableDevices[m]
				if !ok {
					allValid = false
					break
				}
				idleMem, idleCore := d.GetIdleResource()
				if idleCore < reqVcore || idleMem < reqVmem {
					allValid = false
					break
				}
			}

			if allValid {
				groupSet[key] = minors
			}
		}
	}

	result := make([][]int, 0, len(groupSet))
	for _, group := range groupSet {
		result = append(result, group)
	}
	return result
}

// tryVirtualTopoReserve attempts to reserve a topology group for a virtual card request.
// BINPACK: selects the group with the lowest resource usage. SPREAD: selects the highest.
func tryVirtualTopoReserve(availableDevices map[int]*ampcache.DeviceInfo, migDevices *ampcache.GpuDevice, numCards int, reqVmem, reqVcore int64, defaultStrategy string) ([]int, bool) {
	groups := findResourceTopologyGroups(availableDevices, migDevices, numCards, reqVmem, reqVcore)
	if len(groups) == 0 {
		return nil, false
	}

	type scored struct {
		minors []int
		score  int64
	}

	scoredGroups := make([]scored, 0, len(groups))
	for _, group := range groups {
		var totalScore int64
		for _, minor := range group {
			if dev, ok := availableDevices[minor]; ok {
				idleMem, idleCore := dev.GetIdleResource()
				totalMem, totalCore := dev.GetTotalResource()
				st := staffGPU{
					devId: minor,
					available: &gpuResources{
						memory:  idleMem,
						compute: idleCore,
					},
					total: &gpuResources{
						memory:  totalMem,
						compute: totalCore,
					},
				}
				totalScore += st.getResourceRatio()
			}
		}
		scoredGroups = append(scoredGroups, scored{minors: group, score: totalScore})
	}

	if defaultStrategy == utils.ScheduleStrategyBinpack {
		sort.Slice(scoredGroups, func(i, j int) bool {
			return scoredGroups[i].score < scoredGroups[j].score
		})
	} else {
		sort.Slice(scoredGroups, func(i, j int) bool {
			return scoredGroups[i].score > scoredGroups[j].score
		})
	}

	return scoredGroups[0].minors, true
}

// tryTopoReserve attempts to reserve a topology group for a whole-card request.
// BINPACK strategy is planned for future implementation.
func tryTopoReserve(availableDevices map[int]*ampcache.DeviceInfo, migDevices *ampcache.GpuDevice, numCards int, defaultStrategy string) ([]int, bool) {
	groups := findIdleTopologyGroups(availableDevices, migDevices, numCards)
	if len(groups) == 0 {
		return nil, false
	}

	return groups[0], true
}

func (n *NormalStandardPlugin) Unreserve(ctx context.Context, state *framework.CycleState, p *v1.Pod, nodeName string) {
}

func (n *NormalStandardPlugin) PreBind(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) *framework.Status {
	return framework.NewStatus(framework.Success, "")
}

func (n *NormalStandardPlugin) PostBind(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) {
}

// Score evaluates whether a node meets GPU topology scheduling requirements.
// For 2/4/8 whole-card or virtual-card requests, returns MaxNodeScore if the node
// has eligible topology groups; otherwise returns 0.
func (n *NormalStandardPlugin) Score(ctx context.Context, state *framework.CycleState, p *v1.Pod, nodeName string) (int64, *framework.Status) {
	clusterDevices, clusterMigDevices, standardReq, err := ampcache.GetPreFilterStateData(state)
	if err != nil {
		klog.Errorf("Score stage pod %s/%s getPreFilterStateData failed due to %v", p.Namespace, p.Name, err)
		return 0, framework.AsStatus(err)
	}

	availableDevices, ok := clusterDevices[nodeName]
	if !ok {
		klog.Errorf("Score stage pod %s/%s get node %s failed", p.Namespace, p.Name, nodeName)
		return 0, nil
	}

	migDevices, ok := clusterMigDevices[nodeName]
	if !ok {
		migDevices = &ampcache.GpuDevice{
			Devices: make(map[int]*ampcache.Device),
		}
	}

	// Whole-card request: prefer nodes with idle topology groups.
	if standardReq.NormalGpuReqCnt > 0 {
		var rdmaTopo *RdmaTopoAnnotation
		if n.fh != nil {
			scoreNodeInfo, nodeErr := n.fh.SnapshotSharedLister().NodeInfos().Get(nodeName)
			if nodeErr == nil && scoreNodeInfo.Node() != nil {
				rdmaTopo = getRdmaTopoFromNode(scoreNodeInfo.Node())
			}
		}
		podRequestsRDMA := podHasRDMARequest(p)

		if rdmaTopo != nil && podRequestsRDMA {
			// PPU 1.5: prefer closer RDMA topology levels.
			score := scoreRdmaTopoLevel(availableDevices, rdmaTopo, standardReq.NormalGpuReqCnt)
			if score > 0 {
				klog.V(4).Infof("Score stage pod %s/%s score: node %s rdma topo score %d for %d whole-card GPUs",
					p.Namespace, p.Name, nodeName, score, standardReq.NormalGpuReqCnt)
				return score, nil
			}
		} else if standardReq.NormalGpuReqCnt == 2 || standardReq.NormalGpuReqCnt == 4 || standardReq.NormalGpuReqCnt == 8 {
			// PPU 810E topology path (2/4/8 whole cards only).
			topoGroups := findIdleTopologyGroups(availableDevices, migDevices, standardReq.NormalGpuReqCnt)
			if len(topoGroups) > 0 {
				klog.V(4).Infof("Score stage pod %s/%s score: node %s has %d idle topo groups for %d whole-card GPUs, returning MaxNodeScore",
					p.Namespace, p.Name, nodeName, len(topoGroups), standardReq.NormalGpuReqCnt)
				return framework.MaxNodeScore, nil
			}
		}
		klog.V(4).Infof("Score stage pod %s/%s score: node %s has no idle topo groups for %d whole-card GPUs",
			p.Namespace, p.Name, nodeName, standardReq.NormalGpuReqCnt)
		return 0, nil
	}

	// Check for virtual-card 2/4/8 GPU requests: verify topology groups with sufficient resources.
	if standardReq.VReplicas == 2 || standardReq.VReplicas == 4 || standardReq.VReplicas == 8 {
		topoGroups := findResourceTopologyGroups(availableDevices, migDevices, standardReq.VReplicas, int64(standardReq.Vmem), int64(standardReq.Vcore))
		if len(topoGroups) > 0 {
			klog.V(4).Infof("Score stage pod %s/%s score: node %s has %d resource topo groups for %d virtual GPUs, returning MaxNodeScore",
				p.Namespace, p.Name, nodeName, len(topoGroups), standardReq.VReplicas)
			return framework.MaxNodeScore, nil
		}
	}

	klog.V(4).Infof("Score stage pod %s/%s score: node %s has no resource topo groups for %d virtual GPUs",
		p.Namespace, p.Name, nodeName, standardReq.VReplicas)

	return 0, nil
}

// ScoreExtensions returns nil, indicating the Score result does not need normalization.
func (n *NormalStandardPlugin) ScoreExtensions() framework.ScoreExtensions {
	return nil
}

// podHasRDMARequest reports whether the pod requests RDMA resources.
func podHasRDMARequest(pod *v1.Pod) bool {
	for _, c := range pod.Spec.Containers {
		if _, ok := c.Resources.Limits[v1.ResourceName(utils.ResourceRDMA)]; ok {
			return true
		}
		if _, ok := c.Resources.Requests[v1.ResourceName(utils.ResourceRDMA)]; ok {
			return true
		}
	}
	return false
}

// joinIntSlice converts ints to a comma-separated string for the PCIe switch annotation.
func joinIntSlice(ints []int) string {
	s := make([]string, len(ints))
	for i, v := range ints {
		s[i] = strconv.Itoa(v)
	}
	return strings.Join(s, ",")
}
