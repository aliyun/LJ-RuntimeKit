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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lj-runtimekit/scheduler/pkg/utils"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
)

// NodeDomain records a node's network topology domain.
type NodeDomain struct {
	NodeName      string
	HyperNodeId   string
	LeafSwitchId  string
	NetworkPod    string
}

// GetNodeDomain extracts the topology domain from node labels.
func GetNodeDomain(node *v1.Node) NodeDomain {
	d := NodeDomain{NodeName: node.Name}
	if node.Labels != nil {
		d.HyperNodeId = node.Labels[utils.LabelNodeHyperNodeId]
		d.LeafSwitchId = node.Labels[utils.LabelNodeLeafSwitchId]
		d.NetworkPod = node.Labels[utils.LabelNodeNetworkPod]
	}
	return d
}

// PodGroup is the runtime state of a gang pod group.
type PodGroup struct {
	Key          string                   // namespace/jobset/group unique key.
	MinMember    int                      // gang size for all-or-nothing scheduling.
	Priority     int32                    // highest member priority for queue sorting.
	CreationTime time.Time                // earliest member creation time for queue sorting.
	Domain       *NodeDomain              // locked network-pod domain from the first reserved member.
	Reserved     map[types.UID]NodeDomain // reserved members and their node domains.
	Waiting      map[types.UID]struct{}   // members waiting in the Permit stage.
}

// Manager stores all PodGroup runtime states; methods are concurrency-safe.
type Manager struct {
	sync.RWMutex
	groups map[string]*PodGroup
}

func NewManager() *Manager {
	return &Manager{groups: make(map[string]*PodGroup)}
}

// GetPodGroupName returns the gang group name for a pod, or "" if none.
// One JobSet Job forms one group; replicas are split by job-index.
func GetPodGroupName(pod *v1.Pod) string {
	if pod.Labels == nil {
		return ""
	}
	if IsJobSetPod(pod) {
		name := pod.Labels[utils.LabelJobSetReplicatedJob]
		if name == "" {
			name = pod.Labels[utils.LabelJobSetName]
		}
		// Split multiple replicas of the same ReplicatedJob into separate groups.
		if idx := pod.Labels[utils.LabelJobSetJobIndex]; idx != "" {
			return name + "/" + idx
		}
		return name
	}
	return ""
}

// IsJobSetPod reports whether the pod was created by JobSet.
func IsJobSetPod(pod *v1.Pod) bool {
	return pod.Labels != nil && pod.Labels[utils.LabelJobSetName] != ""
}

// GroupSelector returns a label selector matching all members of pod's gang group.
func GroupSelector(pod *v1.Pod) labels.Selector {
	if pod.Labels == nil {
		return labels.Nothing()
	}
	if IsJobSetPod(pod) {
		set := labels.Set{utils.LabelJobSetName: pod.Labels[utils.LabelJobSetName]}
		if v := pod.Labels[utils.LabelJobSetReplicatedJob]; v != "" {
			set[utils.LabelJobSetReplicatedJob] = v
		}
		if v := pod.Labels[utils.LabelJobSetJobIndex]; v != "" {
			set[utils.LabelJobSetJobIndex] = v
		}
		return labels.SelectorFromSet(set)
	}
	return labels.Nothing()
}

// PodGroupKey returns the unique key for a gang pod group, or "" for non-gang pods.
func PodGroupKey(pod *v1.Pod) string {
	name := GetPodGroupName(pod)
	if name == "" {
		return ""
	}
	return pod.Namespace + "/" + pod.Labels[utils.LabelJobSetName] + "/" + name
}

// SplitGroupKey splits a group key into namespace and name.
func SplitGroupKey(key string) (namespace, name string) {
	parts := strings.SplitN(key, "/", 2)
	if len(parts) != 2 {
		return "", key
	}
	return parts[0], parts[1]
}

// getMinMember parses the gang size from the JobSet min-member label; defaults to 1.
func getMinMember(pod *v1.Pod) int {
	if pod.Labels == nil {
		return 1
	}
	val, ok := pod.Labels[utils.LabelRuntimeKitMinMember]
	if !ok {
		return 1
	}
	ret, err := strconv.Atoi(val)
	if err != nil || ret <= 0 {
		return 1
	}
	return ret
}

func podPriority(pod *v1.Pod) int32 {
	if pod.Spec.Priority != nil {
		return *pod.Spec.Priority
	}
	return 0
}

// getOrCreateLocked returns or creates a PodGroup; caller must hold the lock.
func (m *Manager) getOrCreateLocked(pod *v1.Pod) *PodGroup {
	key := PodGroupKey(pod)
	if key == "" {
		return nil
	}
	pg, ok := m.groups[key]
	if !ok {
		pg = &PodGroup{
			Key:          key,
			MinMember:    getMinMember(pod),
			Priority:     podPriority(pod),
			CreationTime: pod.CreationTimestamp.Time,
			Reserved:     make(map[types.UID]NodeDomain),
			Waiting:      make(map[types.UID]struct{}),
		}
		m.groups[key] = pg
	}
	// Aggregate earliest creation time, highest priority, and largest min-member.
	if pod.CreationTimestamp.Time.Before(pg.CreationTime) {
		pg.CreationTime = pod.CreationTimestamp.Time
	}
	if prio := podPriority(pod); prio > pg.Priority {
		pg.Priority = prio
	}
	if mm := getMinMember(pod); mm > pg.MinMember {
		pg.MinMember = mm
	}
	return pg
}

// GetOrCreateGroup returns or creates the PodGroup for a pod.
func (m *Manager) GetOrCreateGroup(pod *v1.Pod) *PodGroup {
	m.Lock()
	defer m.Unlock()
	return m.getOrCreateLocked(pod)
}

// SortKey returns the queue sort key (priority, group creation time, group key).
func (m *Manager) SortKey(pod *v1.Pod) (priority int32, groupTime time.Time, groupKey string) {
	key := PodGroupKey(pod)
	if key == "" {
		return podPriority(pod), pod.CreationTimestamp.Time, string(pod.UID)
	}
	m.Lock()
	defer m.Unlock()
	pg := m.getOrCreateLocked(pod)
	return pg.Priority, pg.CreationTime, pg.Key
}

// GetGroup returns the PodGroup by key, or nil if absent.
func (m *Manager) GetGroup(key string) *PodGroup {
	m.RLock()
	defer m.RUnlock()
	return m.groups[key]
}

// AddReserved records a reserved member and locks the group's network-pod domain.
func (m *Manager) AddReserved(key string, uid types.UID, domain NodeDomain) {
	m.Lock()
	defer m.Unlock()
	pg, ok := m.groups[key]
	if !ok {
		return
	}
	pg.Reserved[uid] = domain
	if pg.Domain == nil {
		d := domain
		pg.Domain = &d
	}
}

// RemoveReserved removes a member's reservation and waiting state; returns remaining reserved count.
func (m *Manager) RemoveReserved(key string, uid types.UID) int {
	m.Lock()
	defer m.Unlock()
	pg, ok := m.groups[key]
	if !ok {
		return 0
	}
	delete(pg.Reserved, uid)
	delete(pg.Waiting, uid)
	if len(pg.Reserved) == 0 {
		pg.Domain = nil
	}
	return len(pg.Reserved)
}

// ReservedDomains returns the node domains of all reserved members.
func (m *Manager) ReservedDomains(key string) []NodeDomain {
	m.RLock()
	defer m.RUnlock()
	pg, ok := m.groups[key]
	if !ok {
		return nil
	}
	domains := make([]NodeDomain, 0, len(pg.Reserved))
	for _, d := range pg.Reserved {
		domains = append(domains, d)
	}
	return domains
}

// GroupDomain returns the locked network-pod domain, or nil if not locked.
func (m *Manager) GroupDomain(key string) *NodeDomain {
	m.RLock()
	defer m.RUnlock()
	pg, ok := m.groups[key]
	if !ok {
		return nil
	}
	return pg.Domain
}

// TryLockDomain locks the group's network-pod domain if not already locked.
func (m *Manager) TryLockDomain(key string, domain NodeDomain) {
	m.Lock()
	defer m.Unlock()
	pg, ok := m.groups[key]
	if !ok || pg.Domain != nil {
		return
	}
	d := domain
	pg.Domain = &d
}

// AddWaiting records a member waiting in the Permit stage.
func (m *Manager) AddWaiting(key string, uid types.UID) (waiting int, minMember int, found bool) {
	m.Lock()
	defer m.Unlock()
	pg, ok := m.groups[key]
	if !ok {
		return 0, 0, false
	}
	pg.Waiting[uid] = struct{}{}
	return len(pg.Waiting), pg.MinMember, true
}

// WaitingUIDs returns the UIDs of members waiting in the Permit stage.
func (m *Manager) WaitingUIDs(key string) []types.UID {
	m.RLock()
	defer m.RUnlock()
	pg, ok := m.groups[key]
	if !ok {
		return nil
	}
	uids := make([]types.UID, 0, len(pg.Waiting))
	for uid := range pg.Waiting {
		uids = append(uids, uid)
	}
	return uids
}

// RemoveWaiting removes a member from the waiting set.
func (m *Manager) RemoveWaiting(key string, uid types.UID) {
	m.Lock()
	defer m.Unlock()
	if pg, ok := m.groups[key]; ok {
		delete(pg.Waiting, uid)
	}
}

// CompleteGroup deletes the group state after all members arrive.
func (m *Manager) CompleteGroup(key string) {
	m.Lock()
	defer m.Unlock()
	delete(m.groups, key)
}

// CleanupIfEmpty deletes the group state when it has no reserved or waiting members.
func (m *Manager) CleanupIfEmpty(key string) {
	m.Lock()
	defer m.Unlock()
	pg, ok := m.groups[key]
	if !ok {
		return
	}
	if len(pg.Reserved) == 0 && len(pg.Waiting) == 0 {
		delete(m.groups, key)
	}
}
