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
	"testing"
	"time"

	"github.com/lj-runtimekit/scheduler/pkg/utils"
	"github.com/stretchr/testify/assert"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// ------------------ TestManagerGetNodeDomain ------------------

func TestManagerGetNodeDomain(t *testing.T) {
	tests := []struct {
		name     string
		node     *v1.Node
		expected NodeDomain
	}{
		{
			name: "node with all three labels returns full NodeDomain",
			node: makeNode("node-1", "VM-G13-P1", "ASW-1", "32766"),
			expected: NodeDomain{
				NodeName:     "node-1",
				HyperNodeId:  "32766",
				LeafSwitchId: "ASW-1",
				NetworkPod:   "VM-G13-P1",
			},
		},
		{
			name: "node with only leaf-switch and network-pod labels",
			node: makeNode("node-2", "VM-G13-P2", "ASW-2", ""),
			expected: NodeDomain{
				NodeName:     "node-2",
				HyperNodeId:  "",
				LeafSwitchId: "ASW-2",
				NetworkPod:   "VM-G13-P2",
			},
		},
		{
			name: "node without topology labels returns empty domain",
			node: &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-3", Labels: map[string]string{}}},
			expected: NodeDomain{
				NodeName:     "node-3",
				HyperNodeId:  "",
				LeafSwitchId: "",
				NetworkPod:   "",
			},
		},
		{
			name: "node with nil labels returns empty domain",
			node: &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-4"}},
			expected: NodeDomain{
				NodeName:     "node-4",
				HyperNodeId:  "",
				LeafSwitchId: "",
				NetworkPod:   "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetNodeDomain(tt.node)
			assert.Equal(t, tt.expected, got)
		})
	}
}

// ------------------ TestManagerSplitGroupKey ------------------

func TestManagerSplitGroupKey(t *testing.T) {
	tests := []struct {
		name      string
		key       string
		wantNS    string
		wantName  string
	}{
		{
			name:     "normal ns/name split",
			key:      "default/my-group",
			wantNS:   "default",
			wantName: "my-group",
		},
		{
			name:     "no slash returns empty namespace",
			key:      "noSlashHere",
			wantNS:   "",
			wantName: "noSlashHere",
		},
		{
			name:     "multiple slashes split only first",
			key:      "default/js-training/worker/0",
			wantNS:   "default",
			wantName: "js-training/worker/0",
		},
		{
			name:     "empty string",
			key:      "",
			wantNS:   "",
			wantName: "",
		},
		{
			name:     "leading slash",
			key:      "/name",
			wantNS:   "",
			wantName: "name",
		},
		{
			name:     "trailing slash",
			key:      "ns/",
			wantNS:   "ns",
			wantName: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns, name := SplitGroupKey(tt.key)
			assert.Equal(t, tt.wantNS, ns)
			assert.Equal(t, tt.wantName, name)
		})
	}
}

// ------------------ TestManagerGroupSelector ------------------

func TestManagerGroupSelector(t *testing.T) {
	tests := []struct {
		name          string
		pod           *v1.Pod
		matchLabels   labels.Set
		shouldMatch   bool
		expectNothing bool
	}{
		{
			name: "full jobset labels match selector",
			pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				utils.LabelJobSetName:          "my-training",
				utils.LabelJobSetReplicatedJob: "worker",
				utils.LabelJobSetJobIndex:      "0",
			}}},
			matchLabels: labels.Set{
				utils.LabelJobSetName:          "my-training",
				utils.LabelJobSetReplicatedJob: "worker",
				utils.LabelJobSetJobIndex:      "0",
			},
			shouldMatch: true,
		},
		{
			name: "full jobset labels mismatch job-index",
			pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				utils.LabelJobSetName:          "my-training",
				utils.LabelJobSetReplicatedJob: "worker",
				utils.LabelJobSetJobIndex:      "0",
			}}},
			matchLabels: labels.Set{
				utils.LabelJobSetName:          "my-training",
				utils.LabelJobSetReplicatedJob: "worker",
				utils.LabelJobSetJobIndex:      "1",
			},
			shouldMatch: false,
		},
		{
			name: "jobset-name and replicatedjob-name without job-index",
			pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				utils.LabelJobSetName:          "my-training",
				utils.LabelJobSetReplicatedJob: "worker",
			}}},
			matchLabels: labels.Set{
				utils.LabelJobSetName:          "my-training",
				utils.LabelJobSetReplicatedJob: "worker",
			},
			shouldMatch: true,
		},
		{
			name:          "pod without labels returns Nothing",
			pod:           &v1.Pod{ObjectMeta: metav1.ObjectMeta{}},
			expectNothing: true,
		},
		{
			name: "pod without jobset-name returns Nothing",
			pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				"some-other": "label",
			}}},
			expectNothing: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sel := GroupSelector(tt.pod)
			if tt.expectNothing {
				assert.Equal(t, labels.Nothing(), sel)
			} else {
				assert.Equal(t, tt.shouldMatch, sel.Matches(tt.matchLabels))
			}
		})
	}
}

// ------------------ TestManagerTryLockDomain ------------------

func TestManagerTryLockDomain(t *testing.T) {
	mgr := NewManager()
	pod := makeGangPod("p1", "default", "grp-lock", "2", 0, time.Now())
	key := PodGroupKey(pod)
	mgr.GetOrCreateGroup(pod)

	domain1 := NodeDomain{NodeName: "node-1", HyperNodeId: "32766", LeafSwitchId: "ASW-1", NetworkPod: "VM-G13-P1"}
	domain2 := NodeDomain{NodeName: "node-2", HyperNodeId: "99", LeafSwitchId: "ASW-2", NetworkPod: "VM-G13-P2"}

	t.Run("first lock succeeds", func(t *testing.T) {
		assert.Nil(t, mgr.GroupDomain(key))
		mgr.TryLockDomain(key, domain1)
		locked := mgr.GroupDomain(key)
		assert.NotNil(t, locked)
		assert.Equal(t, "VM-G13-P1", locked.NetworkPod)
		assert.Equal(t, "ASW-1", locked.LeafSwitchId)
		assert.Equal(t, "32766", locked.HyperNodeId)
	})

	t.Run("TryLock does not overwrite existing lock", func(t *testing.T) {
		mgr.TryLockDomain(key, domain2)
		locked := mgr.GroupDomain(key)
		assert.NotNil(t, locked)
		assert.Equal(t, "VM-G13-P1", locked.NetworkPod)
		assert.Equal(t, "ASW-1", locked.LeafSwitchId)
	})

	t.Run("nonexistent key does not panic", func(t *testing.T) {
		assert.NotPanics(t, func() {
			mgr.TryLockDomain("nonexistent/key", domain1)
		})
	})
}

// ------------------ TestManagerIsJobSetPod ------------------

func TestManagerIsJobSetPod(t *testing.T) {
	tests := []struct {
		name   string
		pod    *v1.Pod
		expect bool
	}{
		{
			name: "jobset-name label present",
			pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				utils.LabelJobSetName: "my-training",
			}}},
			expect: true,
		},
		{
			name: "jobset-name with other labels",
			pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				utils.LabelJobSetName:          "my-training",
				utils.LabelJobSetReplicatedJob: "worker",
				utils.LabelRuntimeKitMinMember: "4",
			}}},
			expect: true,
		},
		{
			name: "no jobset-name label",
			pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				"app": "nginx",
			}}},
			expect: false,
		},
		{
			name:   "nil labels",
			pod:    &v1.Pod{ObjectMeta: metav1.ObjectMeta{}},
			expect: false,
		},
		{
			name: "empty jobset-name value",
			pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				utils.LabelJobSetName: "",
			}}},
			expect: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expect, IsJobSetPod(tt.pod))
		})
	}
}
