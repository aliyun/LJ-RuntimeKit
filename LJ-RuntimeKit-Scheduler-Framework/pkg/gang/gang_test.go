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
	"testing"
	"time"

	"github.com/lj-runtimekit/scheduler/pkg/utils"
	"github.com/stretchr/testify/assert"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// ------------------ test helpers ------------------

// makeGangPod builds a gang pod with JobSet-injected labels.
func makeGangPod(name, ns, group, minMember string, prio int32, createTime time.Time) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			UID:       types.UID(ns + "/" + name),
			Labels: map[string]string{
				utils.LabelJobSetName:          "js-" + group,
				utils.LabelJobSetReplicatedJob: group,
				utils.LabelRuntimeKitMinMember: minMember,
			},
			CreationTimestamp: metav1.NewTime(createTime),
		},
		Spec: v1.PodSpec{Priority: &prio},
	}
}

func makePlainPod(name, ns string, prio int32, createTime time.Time) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         ns,
			UID:               types.UID(ns + "/" + name),
			CreationTimestamp: metav1.NewTime(createTime),
		},
		Spec: v1.PodSpec{Priority: &prio},
	}
}

// makeJobSetPod builds a pod created by JobSet with the min-member label.
func makeJobSetPod(name, ns, jobSetName, minMember string, prio int32, createTime time.Time) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			UID:       types.UID(ns + "/" + name),
			Labels: map[string]string{
				utils.LabelJobSetName:          jobSetName,
				utils.LabelJobSetReplicatedJob: "worker",
				utils.LabelRuntimeKitMinMember: minMember,
			},
			CreationTimestamp: metav1.NewTime(createTime),
		},
		Spec: v1.PodSpec{Priority: &prio},
	}
}

func makeNode(name, networkPod, leafSwitchId, hyperNodeId string) *v1.Node {
	labels := map[string]string{}
	if networkPod != "" {
		labels[utils.LabelNodeNetworkPod] = networkPod
	}
	if leafSwitchId != "" {
		labels[utils.LabelNodeLeafSwitchId] = leafSwitchId
	}
	if hyperNodeId != "" {
		labels[utils.LabelNodeHyperNodeId] = hyperNodeId
	}
	return &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}}
}

func makeNodeInfo(name, networkPod, leafSwitchId, hyperNodeId string) *framework.NodeInfo {
	ni := framework.NewNodeInfo()
	ni.SetNode(makeNode(name, networkPod, leafSwitchId, hyperNodeId))
	return ni
}

func gangCycleState(groupKey string) *framework.CycleState {
	state := framework.NewCycleState()
	state.Write(preFilterStateKey, &preFilterState{groupKey: groupKey})
	return state
}

// gangCycleStateWithBound simulates recovery after scheduler restart.
func gangCycleStateWithBound(groupKey string, boundDomains []NodeDomain) *framework.CycleState {
	state := framework.NewCycleState()
	state.Write(preFilterStateKey, &preFilterState{groupKey: groupKey, boundDomains: boundDomains})
	return state
}

// fakeWaitingPod mocks a pod waiting in the Permit stage.
type fakeWaitingPod struct {
	pod      *v1.Pod
	allowed  []string
	rejected []string
}

func (f *fakeWaitingPod) GetPod() *v1.Pod               { return f.pod }
func (f *fakeWaitingPod) GetPendingPlugins() []string   { return nil }
func (f *fakeWaitingPod) Allow(pluginName string)       { f.allowed = append(f.allowed, pluginName) }
func (f *fakeWaitingPod) Reject(pluginName, msg string) { f.rejected = append(f.rejected, pluginName) }

// fakeWaitingPodsOps mocks waitingPodsOps and records allow/reject calls.
type fakeWaitingPodsOps struct {
	waitingPods []*fakeWaitingPod
}

func (h *fakeWaitingPodsOps) IterateOverWaitingPods(callback func(framework.WaitingPod)) {
	for _, wp := range h.waitingPods {
		callback(wp)
	}
}

func (h *fakeWaitingPodsOps) GetWaitingPod(uid types.UID) framework.WaitingPod {
	for _, wp := range h.waitingPods {
		if wp.pod.UID == uid {
			return wp
		}
	}
	return nil
}

// ------------------ Manager tests ------------------

func TestPodGroupKey(t *testing.T) {
	pod := makeGangPod("p1", "default", "grp-a", "2", 0, time.Now())
	assert.Equal(t, "grp-a", GetPodGroupName(pod))
	assert.Equal(t, "default/js-grp-a/grp-a", PodGroupKey(pod))

	plain := makePlainPod("p2", "default", 0, time.Now())
	assert.Equal(t, "", GetPodGroupName(plain))
	assert.Equal(t, "", PodGroupKey(plain))
}

// TestJobSetPodGroup verifies JobSet-injected labels define the group.
func TestJobSetPodGroup(t *testing.T) {
	pod := makeJobSetPod("js-worker-0", "default", "my-training", "4", 0, time.Now())
	assert.True(t, IsJobSetPod(pod))
	assert.Equal(t, "worker", GetPodGroupName(pod))
	assert.Equal(t, "default/my-training/worker", PodGroupKey(pod))

	both := makeJobSetPod("js-worker-1", "default", "my-training", "4", 0, time.Now())
	both.Labels["some-other-label"] = "manual-group"
	assert.Equal(t, "worker", GetPodGroupName(both))

	assert.Equal(t, 4, getMinMember(pod))

	assert.True(t, GroupSelector(pod).Matches(labels.Set{
		utils.LabelJobSetName:          "my-training",
		utils.LabelJobSetReplicatedJob: "worker",
	}))
	assert.False(t, GroupSelector(pod).Matches(labels.Set{
		utils.LabelJobSetName:          "my-training",
		utils.LabelJobSetReplicatedJob: "leader",
	}))
	assert.Equal(t, labels.Nothing(), GroupSelector(makePlainPod("p", "default", 0, time.Now())))
}

// TestJobSetPermitMinMemberLabel verifies the min-member label controls Permit.
func TestJobSetPermitMinMemberLabel(t *testing.T) {
	mgr := NewManager()
	p1 := makeJobSetPod("js-worker-0", "default", "my-training", "2", 0, time.Now())
	p2 := makeJobSetPod("js-worker-1", "default", "my-training", "2", 0, time.Now())
	waitingPod := &fakeWaitingPod{pod: p1}
	g := &GangSchedulingPlugin{pgMgr: mgr, wpOps: &fakeWaitingPodsOps{waitingPods: []*fakeWaitingPod{waitingPod}}}

	key := PodGroupKey(p1)
	pg := mgr.GetOrCreateGroup(p1)
	assert.Equal(t, 2, pg.MinMember)
	mgr.GetOrCreateGroup(p2)

	t.Run("not ready yet keeps waiting", func(t *testing.T) {
		status, wait := g.Permit(context.Background(), gangCycleState(key), p1, "node-1")
		assert.Equal(t, framework.Wait, status.Code())
		assert.Equal(t, WaitTime, wait)
	})

	t.Run("one waiting plus one bound reaches min-member", func(t *testing.T) {
		bound := []NodeDomain{GetNodeDomain(makeNode("node-2", "VM-G13-P1", "", ""))}
		status, wait := g.Permit(context.Background(), gangCycleStateWithBound(key, bound), p2, "node-3")
		assert.True(t, status.IsSuccess())
		assert.Equal(t, time.Duration(0), wait)
		assert.Contains(t, waitingPod.allowed, Name)
		assert.Nil(t, mgr.GetGroup(key))
	})
}

func TestManagerGetOrCreateGroup(t *testing.T) {
	mgr := NewManager()
	t0 := time.Now()

	p1 := makeGangPod("p1", "default", "grp-a", "2", 10, t0.Add(2*time.Second))
	p2 := makeGangPod("p2", "default", "grp-a", "4", 100, t0.Add(1*time.Second))

	pg := mgr.GetOrCreateGroup(p1)
	assert.NotNil(t, pg)
	assert.Equal(t, 2, pg.MinMember)
	assert.Equal(t, int32(10), pg.Priority)

	pg = mgr.GetOrCreateGroup(p2)
	assert.Equal(t, 4, pg.MinMember)
	assert.Equal(t, int32(100), pg.Priority)
	assert.True(t, pg.CreationTime.Equal(t0.Add(1*time.Second)))

	assert.Equal(t, pg, mgr.GetGroup("default/js-grp-a/grp-a"))

	assert.Nil(t, mgr.GetOrCreateGroup(makePlainPod("p3", "default", 0, t0)))
}

func TestManagerReservedDomain(t *testing.T) {
	mgr := NewManager()
	pod := makeGangPod("p1", "default", "grp-a", "2", 0, time.Now())
	key := PodGroupKey(pod)
	mgr.GetOrCreateGroup(pod)

	d1 := GetNodeDomain(makeNode("node-1", "VM-G13-P1", "ASW-1", "32766"))
	mgr.AddReserved(key, pod.UID, d1)

	locked := mgr.GroupDomain(key)
	assert.NotNil(t, locked)
	assert.Equal(t, "VM-G13-P1", locked.NetworkPod)

	domains := mgr.ReservedDomains(key)
	assert.Len(t, domains, 1)

	assert.Equal(t, 0, mgr.RemoveReserved(key, pod.UID))
	assert.Nil(t, mgr.GroupDomain(key))
}

func TestManagerWaitingAndCleanup(t *testing.T) {
	mgr := NewManager()
	pod := makeGangPod("p1", "default", "grp-a", "2", 0, time.Now())
	key := PodGroupKey(pod)
	mgr.GetOrCreateGroup(pod)

	waiting, minMember, found := mgr.AddWaiting(key, pod.UID)
	assert.True(t, found)
	assert.Equal(t, 1, waiting)
	assert.Equal(t, 2, minMember)
	assert.Len(t, mgr.WaitingUIDs(key), 1)

	mgr.CleanupIfEmpty(key)
	assert.NotNil(t, mgr.GetGroup(key))

	mgr.CompleteGroup(key)
	assert.Nil(t, mgr.GetGroup(key))
}

// ------------------ QueueSort tests ------------------

func TestSortKeyAndLess(t *testing.T) {
	mgr := NewManager()
	g := &GangSchedulingPlugin{pgMgr: mgr}
	t0 := time.Now()

	a1 := makeGangPod("a1", "default", "grp-a", "2", 100, t0)
	a2 := makeGangPod("a2", "default", "grp-a", "2", 100, t0.Add(2*time.Second))
	b1 := makeGangPod("b1", "default", "grp-b", "2", 100, t0.Add(1*time.Second))
	high := makePlainPod("high", "default", 200, t0.Add(5*time.Second))

	qpi := func(pod *v1.Pod) *framework.QueuedPodInfo {
		return &framework.QueuedPodInfo{PodInfo: &framework.PodInfo{Pod: pod}}
	}

	assert.True(t, g.Less(qpi(high), qpi(a1)))
	assert.True(t, g.Less(qpi(a1), qpi(b1)))
	assert.True(t, g.Less(qpi(a2), qpi(b1)))
	assert.True(t, g.Less(qpi(a1), qpi(a2)))
	assert.False(t, g.Less(qpi(a2), qpi(a1)))
}

// ------------------ topology score tests ------------------

func TestRelationScore(t *testing.T) {
	reserved := NodeDomain{NodeName: "node-1", HyperNodeId: "32766", LeafSwitchId: "ASW-1", NetworkPod: "VM-G13-P1"}

	tests := []struct {
		name      string
		candidate NodeDomain
		expected  int64
	}{
		{"same node", NodeDomain{NodeName: "node-1", HyperNodeId: "other", LeafSwitchId: "other", NetworkPod: "other"}, scoreSameNode},
		{"same hypernode", NodeDomain{NodeName: "node-2", HyperNodeId: "32766", LeafSwitchId: "other", NetworkPod: "other"}, scoreSameHyperNode},
		{"same leaf switch", NodeDomain{NodeName: "node-2", HyperNodeId: "other", LeafSwitchId: "ASW-1", NetworkPod: "other"}, scoreSameLeafSwitch},
		{"same network-pod", NodeDomain{NodeName: "node-2", HyperNodeId: "other", LeafSwitchId: "other", NetworkPod: "VM-G13-P1"}, scoreSameNetworkPod},
		{"unrelated", NodeDomain{NodeName: "node-2", HyperNodeId: "other", LeafSwitchId: "other", NetworkPod: "other"}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, relationScore(reserved, tt.candidate))
		})
	}

	best := calcScore([]NodeDomain{
		{NodeName: "node-9", NetworkPod: "VM-G13-P1"},
		{NodeName: "node-1", HyperNodeId: "32766"},
	}, NodeDomain{NodeName: "node-1", HyperNodeId: "32766"})
	assert.Equal(t, scoreSameNode, best)
}

// ------------------ Filter tests ------------------

func TestFilter(t *testing.T) {
	mgr := NewManager()
	g := &GangSchedulingPlugin{pgMgr: mgr}
	pod := makeGangPod("p1", "default", "grp-a", "2", 0, time.Now())
	key := PodGroupKey(pod)
	mgr.GetOrCreateGroup(pod)

	t.Run("non-gang pod passes", func(t *testing.T) {
		status := g.Filter(context.Background(), framework.NewCycleState(), pod, makeNodeInfo("node-1", "", "", ""))
		assert.True(t, status.IsSuccess())
	})

	t.Run("node without network-pod label is rejected", func(t *testing.T) {
		status := g.Filter(context.Background(), gangCycleState(key), pod, makeNodeInfo("node-1", "", "", ""))
		assert.Equal(t, framework.Unschedulable, status.Code())
	})

	t.Run("node in same network-pod passes when unlocked", func(t *testing.T) {
		status := g.Filter(context.Background(), gangCycleState(key), pod, makeNodeInfo("node-1", "VM-G13-P1", "ASW-1", "32766"))
		assert.True(t, status.IsSuccess())
	})

	t.Run("node in different network-pod rejected after lock", func(t *testing.T) {
		mgr.AddReserved(key, types.UID("x"), GetNodeDomain(makeNode("node-1", "VM-G13-P1", "ASW-1", "32766")))
		status := g.Filter(context.Background(), gangCycleState(key), pod, makeNodeInfo("node-2", "VM-G13-P2", "ASW-1", "32766"))
		assert.Equal(t, framework.Unschedulable, status.Code())
		status = g.Filter(context.Background(), gangCycleState(key), pod, makeNodeInfo("node-3", "VM-G13-P1", "ASW-2", "99"))
		assert.True(t, status.IsSuccess())
	})

	t.Run("recover domain lock from bound members after restart", func(t *testing.T) {
		mgr2 := NewManager()
		g2 := &GangSchedulingPlugin{pgMgr: mgr2}
		mgr2.GetOrCreateGroup(pod)
		assert.Nil(t, mgr2.GroupDomain(key))

		bound := []NodeDomain{GetNodeDomain(makeNode("node-1", "VM-G13-P1", "ASW-1", "32766"))}
		status := g2.Filter(context.Background(), gangCycleStateWithBound(key, bound), pod, makeNodeInfo("node-2", "VM-G13-P2", "", ""))
		assert.Equal(t, framework.Unschedulable, status.Code())
		locked := mgr2.GroupDomain(key)
		assert.NotNil(t, locked)
		assert.Equal(t, "VM-G13-P1", locked.NetworkPod)
		status = g2.Filter(context.Background(), gangCycleStateWithBound(key, bound), pod, makeNodeInfo("node-3", "VM-G13-P1", "", ""))
		assert.True(t, status.IsSuccess())
	})
}

// ------------------ Permit tests ------------------

func TestPermit(t *testing.T) {
	mgr := NewManager()
	waitingPod := &fakeWaitingPod{pod: makeGangPod("p1", "default", "grp-a", "2", 0, time.Now())}
	g := &GangSchedulingPlugin{pgMgr: mgr, wpOps: &fakeWaitingPodsOps{waitingPods: []*fakeWaitingPod{waitingPod}}}

	key := PodGroupKey(waitingPod.pod)
	mgr.GetOrCreateGroup(waitingPod.pod)
	p2 := makeGangPod("p2", "default", "grp-a", "2", 0, time.Now())
	mgr.GetOrCreateGroup(p2)

	t.Run("non-gang pod passes", func(t *testing.T) {
		status, wait := g.Permit(context.Background(), framework.NewCycleState(), waitingPod.pod, "node-1")
		assert.True(t, status.IsSuccess())
		assert.Equal(t, time.Duration(0), wait)
	})

	t.Run("incomplete group waits", func(t *testing.T) {
		status, wait := g.Permit(context.Background(), gangCycleState(key), waitingPod.pod, "node-1")
		assert.Equal(t, framework.Wait, status.Code())
		assert.Equal(t, WaitTime, wait)
	})

	t.Run("complete group allows all waiting members", func(t *testing.T) {
		status, wait := g.Permit(context.Background(), gangCycleState(key), p2, "node-2")
		assert.True(t, status.IsSuccess())
		assert.Equal(t, time.Duration(0), wait)
		assert.Contains(t, waitingPod.allowed, Name)
		assert.Nil(t, mgr.GetGroup(key))
	})
}

// TestPermitRecoverAfterRestart verifies bound members count toward min-member.
func TestPermitRecoverAfterRestart(t *testing.T) {
	mgr := NewManager()
	p1 := makeGangPod("p1", "default", "grp-a", "4", 0, time.Now())
	p2 := makeGangPod("p2", "default", "grp-a", "4", 0, time.Now())
	waitingPod := &fakeWaitingPod{pod: p1}
	g := &GangSchedulingPlugin{pgMgr: mgr, wpOps: &fakeWaitingPodsOps{waitingPods: []*fakeWaitingPod{waitingPod}}}

	key := PodGroupKey(p1)
	mgr.GetOrCreateGroup(p1)
	mgr.GetOrCreateGroup(p2)

	bound := []NodeDomain{
		GetNodeDomain(makeNode("node-1", "VM-G13-P1", "", "")),
		GetNodeDomain(makeNode("node-2", "VM-G13-P1", "", "")),
	}

	t.Run("one waiting plus two bound still incomplete", func(t *testing.T) {
		status, wait := g.Permit(context.Background(), gangCycleStateWithBound(key, bound), p1, "node-3")
		assert.Equal(t, framework.Wait, status.Code())
		assert.Equal(t, WaitTime, wait)
	})

	t.Run("one waiting plus two bound is three of four", func(t *testing.T) {
		mgr2 := NewManager()
		g2 := &GangSchedulingPlugin{pgMgr: mgr2}
		mgr2.GetOrCreateGroup(p1)
		status, _ := g2.Permit(context.Background(), gangCycleStateWithBound(key, bound), p1, "node-3")
		assert.Equal(t, framework.Wait, status.Code())
	})

	t.Run("two waiting plus two bound complete", func(t *testing.T) {
		status, wait := g.Permit(context.Background(), gangCycleStateWithBound(key, bound), p2, "node-4")
		assert.True(t, status.IsSuccess())
		assert.Equal(t, time.Duration(0), wait)
		assert.Contains(t, waitingPod.allowed, Name)
		assert.Nil(t, mgr.GetGroup(key))
	})
}

// ------------------ Unreserve tests ------------------

func TestUnreserveRejectsWaitingMembers(t *testing.T) {
	mgr := NewManager()
	failedPod := makeGangPod("p1", "default", "grp-a", "2", 0, time.Now())
	waitingPod := &fakeWaitingPod{pod: makeGangPod("p2", "default", "grp-a", "2", 0, time.Now())}
	g := &GangSchedulingPlugin{pgMgr: mgr, wpOps: &fakeWaitingPodsOps{waitingPods: []*fakeWaitingPod{waitingPod}}}

	key := PodGroupKey(failedPod)
	mgr.GetOrCreateGroup(failedPod)
	mgr.GetOrCreateGroup(waitingPod.pod)
	mgr.AddReserved(key, failedPod.UID, GetNodeDomain(makeNode("node-1", "VM-G13-P1", "", "")))
	mgr.AddWaiting(key, waitingPod.pod.UID)

	g.Unreserve(context.Background(), gangCycleState(key), failedPod, "node-1")

	assert.Contains(t, waitingPod.rejected, Name)
	assert.Nil(t, mgr.GetGroup(key))
}
