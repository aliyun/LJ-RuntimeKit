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

package gang

import (
	"context"
	"fmt"
	"time"

	"github.com/lj-runtimekit/scheduler/pkg/utils"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

const (
	Name = "Gang"

	// WaitTime is the max time gang members wait for each other in the Permit stage.
	WaitTime = 180 * time.Second
)

// Topology affinity score tiers: same node > same HyperNode > same leaf switch > same network-pod.
const (
	scoreSameNode       int64 = 100
	scoreSameHyperNode  int64 = 75
	scoreSameLeafSwitch int64 = 50
	scoreSameNetworkPod int64 = 25
)

// Capacity score tiers for the first member, using the same priority order as affinity.
const (
	scoreCapSameNode   int64 = 100
	scoreCapHyperNode  int64 = 75
	scoreCapLeafSwitch int64 = 50
	scoreCapNetworkPod int64 = 25
)

var (
	_ framework.QueueSortPlugin = &GangSchedulingPlugin{}
	_ framework.PreFilterPlugin = &GangSchedulingPlugin{}
	_ framework.FilterPlugin    = &GangSchedulingPlugin{}
	_ framework.ScorePlugin     = &GangSchedulingPlugin{}
	_ framework.ReservePlugin   = &GangSchedulingPlugin{}
	_ framework.PermitPlugin    = &GangSchedulingPlugin{}
)

// waitingPodsOps abstracts waiting-pod operations; framework.Handle implements it,
// and the interface exists only for test mocking.
type waitingPodsOps interface {
	IterateOverWaitingPods(callback func(framework.WaitingPod))
	GetWaitingPod(uid types.UID) framework.WaitingPod
}

type GangSchedulingPlugin struct {
	fh        framework.Handle
	wpOps     waitingPodsOps
	pgMgr     *Manager
	podLister corev1listers.PodLister
}

func New(obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	klog.V(6).Infof("begin to new gang plugin")
	return &GangSchedulingPlugin{
		fh:        handle,
		wpOps:     handle,
		pgMgr:     NewManager(),
		podLister: handle.SharedInformerFactory().Core().V1().Pods().Lister(),
	}, nil
}

func (g *GangSchedulingPlugin) Name() string {
	return Name
}

// ------------------ CycleState ------------------

const preFilterStateKey = "PreFilterGangPodGroupState"

type preFilterState struct {
	groupKey string
	boundDomains []NodeDomain // domains of bound members from the informer.
	minMember    int          // total group size.
	deviceReq    int64        // per-member whole-GPU request; 0 skips capacity scoring.
}

func (s *preFilterState) Clone() framework.StateData {
	return s
}

// getPreFilterStateObj returns the state written by PreFilter, or nil for non-gang pods.
func getPreFilterStateObj(state *framework.CycleState) *preFilterState {
	d, err := state.Read(preFilterStateKey)
	if err != nil {
		return nil
	}
	s, ok := d.(*preFilterState)
	if !ok || s == nil {
		return nil
	}
	return s
}

// getPreFilterState returns the group key written by PreFilter, or "" for non-gang pods.
func getPreFilterState(state *framework.CycleState) string {
	if s := getPreFilterStateObj(state); s != nil {
		return s.groupKey
	}
	return ""
}

// ------------------ QueueSort ------------------

// Less orders pods by priority, group creation time, group key, then pod creation time.
func (g *GangSchedulingPlugin) Less(podInfo1, podInfo2 *framework.QueuedPodInfo) bool {
	p1, p2 := podInfo1.Pod, podInfo2.Pod

	prio1, groupTime1, groupKey1 := g.pgMgr.SortKey(p1)
	prio2, groupTime2, groupKey2 := g.pgMgr.SortKey(p2)

	if prio1 != prio2 {
		return prio1 > prio2
	}
	if !groupTime1.Equal(groupTime2) {
		return groupTime1.Before(groupTime2)
	}
	if groupKey1 != groupKey2 {
		return groupKey1 < groupKey2
	}
	// Within the same group, sort by pod creation time.
	return p1.CreationTimestamp.Time.Before(p2.CreationTimestamp.Time)
}

// ------------------ PreFilter ------------------

func (g *GangSchedulingPlugin) PreFilter(ctx context.Context, state *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	key := PodGroupKey(pod)
	if key == "" {
		return nil, framework.NewStatus(framework.Success, "")
	}

	pg := g.pgMgr.GetOrCreateGroup(pod)
	klog.V(4).Infof("gang PreFilter: pod %s/%s belongs to podgroup %s, minMember %d",
		pod.Namespace, pod.Name, pg.Key, pg.MinMember)

	boundDomains := g.boundMemberDomains(pod)

	state.Write(preFilterStateKey, &preFilterState{
		groupKey:     pg.Key,
		boundDomains: boundDomains,
		minMember:    pg.MinMember,
		deviceReq:    deviceRequest(pod),
	})
	return nil, framework.NewStatus(framework.Success, "")
}

// deviceRequest returns the per-member whole-GPU request for capacity scoring.
func deviceRequest(pod *v1.Pod) int64 {
	if r := utils.GetResourceReq(pod, utils.ResourceGPU); r > 0 {
		return int64(r)
	}
	if r := utils.GetResourceReq(pod, utils.ResourceNvidia); r > 0 {
		return int64(r)
	}
	return 0
}

// boundMemberDomains returns domains of bound group members from the informer.
// Used to recover state after scheduler restart.
func (g *GangSchedulingPlugin) boundMemberDomains(pod *v1.Pod) []NodeDomain {
	if g.podLister == nil {
		return nil
	}
	pods, err := g.podLister.Pods(pod.Namespace).List(GroupSelector(pod))
	if err != nil {
		klog.V(4).Infof("gang boundMemberDomains: list members of podgroup %s failed: %v", PodGroupKey(pod), err)
		return nil
	}

	var domains []NodeDomain
	for _, p := range pods {
		if p.Spec.NodeName == "" || p.DeletionTimestamp != nil {
			continue
		}
		nodeInfo, err := g.fh.SnapshotSharedLister().NodeInfos().Get(p.Spec.NodeName)
		if err != nil || nodeInfo.Node() == nil {
			continue
		}
		domains = append(domains, GetNodeDomain(nodeInfo.Node()))
	}
	return domains
}

func (g *GangSchedulingPlugin) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

// ------------------ Filter ------------------

// Filter enforces gang constraints:
// 1) nodes must have a network-pod label;
// 2) after the first member locks the domain, all members stay in the same network-pod;
// 3) the network-pod must have enough capacity for the remaining members.
func (g *GangSchedulingPlugin) Filter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	st := getPreFilterStateObj(state)
	if st == nil || st.groupKey == "" {
		return framework.NewStatus(framework.Success, "")
	}
	key := st.groupKey

	node := nodeInfo.Node()
	if node == nil {
		return framework.NewStatus(framework.Error, "node not found")
	}

	domain := GetNodeDomain(node)
	if domain.NetworkPod == "" {
		return framework.NewStatus(framework.Unschedulable,
			fmt.Sprintf("node %s has no %s label, cannot host podgroup %s", node.Name, utils.LabelNodeNetworkPod, key))
	}

	locked := g.pgMgr.GroupDomain(key)
	if locked == nil && len(st.boundDomains) > 0 {
		// Recover domain lock from bound members after scheduler restart.
		g.pgMgr.TryLockDomain(key, st.boundDomains[0])
		locked = &st.boundDomains[0]
		klog.V(4).Infof("gang Filter: podgroup %s domain lock recovered from bound members, network-pod %s", key, locked.NetworkPod)
	}

	if locked != nil && locked.NetworkPod != domain.NetworkPod {
		return framework.NewStatus(framework.Unschedulable,
			fmt.Sprintf("podgroup %s is locked in network-pod %s, but node %s belongs to %s", key, locked.NetworkPod, node.Name, domain.NetworkPod))
	}

	// Domain capacity check: reject nodes whose network-pod cannot host the remaining members.
	if st.deviceReq > 0 && st.minMember > 1 {
		_, _, _, npFree, reservedCount := g.groupTopologyCapacity(st, domain)
		need := int64(st.minMember - len(st.boundDomains) - reservedCount)
		if need < 1 {
			need = 1
		}
		if npFree < need {
			return framework.NewStatus(framework.Unschedulable,
				fmt.Sprintf("podgroup %s needs %d more members, but network-pod %s can only host %d",
					key, need, domain.NetworkPod, npFree))
		}
	}

	return framework.NewStatus(framework.Success, "")
}

// ------------------ Score ------------------

// Score prefers nodes closer to already reserved or bound members.
func (g *GangSchedulingPlugin) Score(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	st := getPreFilterStateObj(state)
	if st == nil || st.groupKey == "" {
		return 0, framework.NewStatus(framework.Success, "")
	}

	// Include bound members as affinity references after restart.
	domains := g.pgMgr.ReservedDomains(st.groupKey)
	domains = append(domains, st.boundDomains...)
	if len(domains) == 0 {
		// First member: score by domain capacity instead of affinity.
		return g.capacityScore(st, nodeName, pod)
	}

	nodeInfo, err := g.fh.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil || nodeInfo.Node() == nil {
		klog.V(4).Infof("gang Score: pod %s/%s get node %s failed: %v", pod.Namespace, pod.Name, nodeName, err)
		return 0, framework.NewStatus(framework.Success, "")
	}

	score := calcScore(domains, GetNodeDomain(nodeInfo.Node()))
	klog.V(5).Infof("gang Score: pod %s/%s node %s get topo affinity score %d", pod.Namespace, pod.Name, nodeName, score)
	return score, framework.NewStatus(framework.Success, "")
}

// calcScore returns the highest affinity score against reserved/bound members.
func calcScore(reserved []NodeDomain, candidate NodeDomain) int64 {
	var best int64
	for _, d := range reserved {
		if s := relationScore(d, candidate); s > best {
			best = s
		}
	}
	return best
}

// relationScore returns the topology affinity between reserved and candidate nodes.
func relationScore(reserved, candidate NodeDomain) int64 {
	switch {
	case reserved.NodeName != "" && reserved.NodeName == candidate.NodeName:
		return scoreSameNode
	case reserved.HyperNodeId != "" && reserved.HyperNodeId == candidate.HyperNodeId:
		return scoreSameHyperNode
	case reserved.LeafSwitchId != "" && reserved.LeafSwitchId == candidate.LeafSwitchId:
		return scoreSameLeafSwitch
	case reserved.NetworkPod != "" && reserved.NetworkPod == candidate.NetworkPod:
		return scoreSameNetworkPod
	}
	return 0
}

// capacityScore scores the first member by the smallest domain that fits the whole group.
func (g *GangSchedulingPlugin) capacityScore(st *preFilterState, nodeName string, pod *v1.Pod) (int64, *framework.Status) {
	if st.deviceReq <= 0 || st.minMember <= 1 {
		// No device request or single-member group: let other plugins decide.
		return 0, framework.NewStatus(framework.Success, "")
	}

	nodeInfo, err := g.fh.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil || nodeInfo.Node() == nil {
		return 0, framework.NewStatus(framework.Success, "")
	}
	cand := GetNodeDomain(nodeInfo.Node())

	freeByNode, hyperNodeFree, leafSwitchFree, npFree, reservedCount := g.groupTopologyCapacity(st, cand)
	if freeByNode == nil {
		return 0, framework.NewStatus(framework.Success, "")
	}

	// Remaining members to schedule, including this pod.
	need := int64(st.minMember - len(st.boundDomains) - reservedCount)
	if need < 1 {
		need = 1
	}

	// Filter already verified npFree >= need.
	var score int64
	switch {
	case freeByNode[nodeName] >= need:
		score = scoreCapSameNode
	case hyperNodeFree >= need:
		score = scoreCapHyperNode
	case leafSwitchFree >= need:
		score = scoreCapLeafSwitch
	case npFree >= need:
		score = scoreCapNetworkPod
	default:
		score = 0
	}
	klog.V(4).Infof("gang Score(capacity): pod %s/%s node %s nodeFit %d hyperNodeFree %d leafSwitchFree %d npFree %d need %d score %d",
		pod.Namespace, pod.Name, nodeName, freeByNode[nodeName], hyperNodeFree, leafSwitchFree, npFree, need, score)
	return score, framework.NewStatus(framework.Success, "")
}

// groupTopologyCapacity returns per-node free member count and aggregated free capacity
// for the candidate's HyperNode / leaf-switch / network-pod domains.
func (g *GangSchedulingPlugin) groupTopologyCapacity(st *preFilterState, cand NodeDomain) (map[string]int64, int64, int64, int64, int) {
	allNodes, err := g.fh.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		return nil, 0, 0, 0, 0
	}

	// Count free members per node.
	freeByNode := make(map[string]int64, len(allNodes))
	for _, ni := range allNodes {
		n := ni.Node()
		if n == nil {
			continue
		}
		freeByNode[n.Name] = nodeFreeDevice(ni, st.deviceReq)
	}

	// Reserved-but-not-bound members are already assumed in the snapshot; do not subtract again.
	reserved := g.pgMgr.ReservedDomains(st.groupKey)

	// Aggregate capacity by topology level.
	var hyperNodeFree, leafSwitchFree, npFree int64
	for _, ni := range allNodes {
		n := ni.Node()
		if n == nil {
			continue
		}
		free := freeByNode[n.Name]
		if free <= 0 {
			continue
		}
		d := GetNodeDomain(n)
		if cand.HyperNodeId != "" && d.HyperNodeId == cand.HyperNodeId {
			hyperNodeFree += free
		}
		if cand.LeafSwitchId != "" && d.LeafSwitchId == cand.LeafSwitchId {
			leafSwitchFree += free
		}
		if cand.NetworkPod != "" && d.NetworkPod == cand.NetworkPod {
			npFree += free
		}
	}
	return freeByNode, hyperNodeFree, leafSwitchFree, npFree, len(reserved)
}

// nodeFreeDevice estimates how many whole members fit on a node.
func nodeFreeDevice(ni *framework.NodeInfo, perMember int64) int64 {
	node := ni.Node()
	total := utils.GetResourceFromNode(node, utils.ResourceGPU)
	if total <= 0 {
		total = utils.GetResourceFromNode(node, utils.ResourceNvidia)
	}
	if total <= 0 {
		return 0
	}

	var used int64
	if ni.Requested != nil && ni.Requested.ScalarResources != nil {
		used = ni.Requested.ScalarResources[v1.ResourceName(utils.ResourceGPU)]
		if used == 0 {
			used = ni.Requested.ScalarResources[v1.ResourceName(utils.ResourceNvidia)]
		}
	}
	free := total - used
	if free <= 0 {
		return 0
	}

	// Members must fit entirely on one node.
	nodeFit := free / perMember
	physFit := total / perMember
	if nodeFit > physFit {
		nodeFit = physFit
	}
	return nodeFit
}

func (g *GangSchedulingPlugin) ScoreExtensions() framework.ScoreExtensions {
	return nil
}

// ------------------ Reserve / Unreserve ------------------

// Reserve records the member's domain; the first member locks the network-pod domain.
func (g *GangSchedulingPlugin) Reserve(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) *framework.Status {
	key := getPreFilterState(state)
	if key == "" {
		return framework.NewStatus(framework.Success, "")
	}

	nodeInfo, err := g.fh.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil || nodeInfo.Node() == nil {
		return framework.NewStatus(framework.Error, fmt.Sprintf("gang Reserve: node %s not found", nodeName))
	}

	g.pgMgr.AddReserved(key, pod.UID, GetNodeDomain(nodeInfo.Node()))
	klog.V(4).Infof("gang Reserve: pod %s/%s reserved on node %s for podgroup %s", pod.Namespace, pod.Name, nodeName, key)
	return framework.NewStatus(framework.Success, "")
}

// Unreserve cleans up and rejects other waiting members on failure (all-or-nothing).
func (g *GangSchedulingPlugin) Unreserve(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) {
	key := getPreFilterState(state)
	if key == "" {
		return
	}

	g.pgMgr.RemoveReserved(key, pod.UID)

	// Reject other waiting members when one member fails.
	for _, uid := range g.pgMgr.WaitingUIDs(key) {
		if wp := g.wpOps.GetWaitingPod(uid); wp != nil {
			wp.Reject(Name, fmt.Sprintf("podgroup %s member %s/%s failed, all or nothing", key, pod.Namespace, pod.Name))
		}
		// Remove them now; their own Unreserve will clean up later.
		g.pgMgr.RemoveWaiting(key, uid)
	}

	g.pgMgr.CleanupIfEmpty(key)
	klog.V(4).Infof("gang Unreserve: pod %s/%s unreserved from node %s for podgroup %s", pod.Namespace, pod.Name, nodeName, key)
}

// ------------------ Permit ------------------

// Permit waits until all members arrive; the last arrival allows the whole group.
// Bound members are counted so restart does not cause permanent timeout.
func (g *GangSchedulingPlugin) Permit(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (*framework.Status, time.Duration) {
	st := getPreFilterStateObj(state)
	if st == nil || st.groupKey == "" {
		return framework.NewStatus(framework.Success, ""), 0
	}
	key := st.groupKey

	waiting, minMember, found := g.pgMgr.AddWaiting(key, pod.UID)
	if !found {
		return framework.NewStatus(framework.Success, ""), 0
	}

	arrived := waiting + len(st.boundDomains)
	if arrived >= minMember {
		klog.V(4).Infof("gang Permit: podgroup %s arrived %d/%d (waiting %d, bound %d), allowing waiting members",
			key, arrived, minMember, waiting, len(st.boundDomains))
		g.wpOps.IterateOverWaitingPods(func(wp framework.WaitingPod) {
			if PodGroupKey(wp.GetPod()) == key {
				wp.Allow(Name)
			}
		})
		g.pgMgr.CompleteGroup(key)
		return framework.NewStatus(framework.Success, ""), 0
	}

	klog.V(4).Infof("gang Permit: podgroup %s waiting for other members, arrived %d/%d (waiting %d, bound %d)",
		key, arrived, minMember, waiting, len(st.boundDomains))
	return framework.NewStatus(framework.Wait,
		fmt.Sprintf("podgroup %s waiting for other members, arrived %d/%d", key, arrived, minMember)), WaitTime
}
