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

package activemonitoring

import (
	"encoding/json"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	gpuv1alpha1 "github.com/lj-runtimekit/job-controller/pkg/apis/gpu/v1alpha1"
)

func TestEnumerateMinors(t *testing.T) {
	tests := []struct {
		name string
		node *v1.Node
		want []int
	}{
		{
			name: "from device annotation",
			node: &v1.Node{
				ObjectMeta: nodeMeta("n1", map[string]string{AnnoNodeDevices: `{"count":3,"devices":{"2":{},"0":{},"1":{}}}`}),
				Status:     v1.NodeStatus{Capacity: gpuCapacity(8)},
			},
			want: []int{0, 1, 2},
		},
		{
			name: "annotation malformed falls back to capacity",
			node: &v1.Node{
				ObjectMeta: nodeMeta("n1", map[string]string{AnnoNodeDevices: `not-json`}),
				Status:     v1.NodeStatus{Capacity: gpuCapacity(2)},
			},
			want: []int{0, 1},
		},
		{
			name: "no annotation uses capacity",
			node: &v1.Node{
				ObjectMeta: nodeMeta("n1", nil),
				Status:     v1.NodeStatus{Capacity: gpuCapacity(4)},
			},
			want: []int{0, 1, 2, 3},
		},
		{
			name: "zero capacity yields nothing",
			node: &v1.Node{
				ObjectMeta: nodeMeta("n1", nil),
				Status:     v1.NodeStatus{Capacity: gpuCapacity(0)},
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := enumerateMinors(tt.node, defaultGPUResourceName)
			if !intSliceEqual(got, tt.want) {
				t.Fatalf("enumerateMinors = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNodeMatchesNodeAffinity(t *testing.T) {
	node := &v1.Node{ObjectMeta: nodeMeta("gpu-node-01", map[string]string{
		HostnameLabelKey: "gpu-node-01",
		"zone":           "a",
	})}

	hostnameIn := &v1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
		NodeSelectorTerms: []v1.NodeSelectorTerm{{MatchExpressions: []v1.NodeSelectorRequirement{{
			Key: HostnameLabelKey, Operator: v1.NodeSelectorOpIn, Values: []string{"gpu-node-01", "other"},
		}}}},
	}}
	hostnameNotIn := &v1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
		NodeSelectorTerms: []v1.NodeSelectorTerm{{MatchExpressions: []v1.NodeSelectorRequirement{{
			Key: HostnameLabelKey, Operator: v1.NodeSelectorOpIn, Values: []string{"other"},
		}}}},
	}}
	fieldSelector := &v1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
		NodeSelectorTerms: []v1.NodeSelectorTerm{{MatchFields: []v1.NodeSelectorRequirement{{
			Key: "metadata.name", Operator: v1.NodeSelectorOpIn, Values: []string{"gpu-node-01"},
		}}}},
	}}

	if !nodeMatchesNodeAffinity(node, nil) {
		t.Error("nil affinity should match")
	}
	if !nodeMatchesNodeAffinity(node, hostnameIn) {
		t.Error("hostname In should match")
	}
	if nodeMatchesNodeAffinity(node, hostnameNotIn) {
		t.Error("hostname In(other) should not match")
	}
	if !nodeMatchesNodeAffinity(node, fieldSelector) {
		t.Error("metadata.name field selector should match")
	}
}

func TestAllLjRuntimeKitEnabledSerialization(t *testing.T) {
	for _, tt := range []struct {
		name string
		raw  string
		want bool
	}{
		{name: "enabled", raw: `{"spec":{"allLjRuntimeKitEnabled":true}}`, want: true},
		{name: "disabled", raw: `{"spec":{"allLjRuntimeKitEnabled":false}}`},
		{name: "omitted", raw: `{"spec":{}}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var cr gpuv1alpha1.ActiveMonitoring
			if err := json.Unmarshal([]byte(tt.raw), &cr); err != nil {
				t.Fatal(err)
			}
			if cr.Spec.AllLjRuntimeKitEnabled != tt.want {
				t.Fatalf("decoded allLjRuntimeKitEnabled = %v, want %v", cr.Spec.AllLjRuntimeKitEnabled, tt.want)
			}
			u, err := fromActiveMonitoring(cr.DeepCopy())
			if err != nil {
				t.Fatal(err)
			}
			got, found, err := unstructured.NestedBool(u.Object, "spec", "allLjRuntimeKitEnabled")
			if err != nil || got != tt.want || found != tt.want {
				t.Fatalf("encoded allLjRuntimeKitEnabled = %v, found=%v, err=%v; want %v with zero value omitted", got, found, err, tt.want)
			}
			roundTrip, err := toActiveMonitoring(u)
			if err != nil {
				t.Fatal(err)
			}
			if roundTrip.Spec.AllLjRuntimeKitEnabled != tt.want {
				t.Fatalf("round-trip allLjRuntimeKitEnabled = %v, want %v", roundTrip.Spec.AllLjRuntimeKitEnabled, tt.want)
			}
		})
	}
}

func TestBuildQueueAllLjRuntimeKitEnabled(t *testing.T) {
	n1 := gpuNode("gpu-node-01", 2)
	n1.Labels[NodeEnabledLabelKey] = NodeEnabledLabelValue
	n2 := gpuNode("gpu-node-02", 1)
	n2.Labels[NodeEnabledLabelKey] = NodeEnabledLabelValue
	disabled := gpuNode("gpu-node-03", 4) // GPU but no runtimekit-enable label
	noGPU := gpuNode("cpu-node", 0)
	c, cancel := newTestController(t, n1, n2, disabled, noGPU)
	defer cancel()

	cr := testCR("default", "t1", gpuv1alpha1.ActiveMonitoringSpec{
		AllLjRuntimeKitEnabled: true,
		Affinity: &v1.Affinity{NodeAffinity: &v1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
				NodeSelectorTerms: []v1.NodeSelectorTerm{{MatchExpressions: []v1.NodeSelectorRequirement{{
					Key: HostnameLabelKey, Operator: v1.NodeSelectorOpIn, Values: []string{"does-not-exist"},
				}}}},
			},
		}},
	})
	queue, err := c.buildQueue(cr)
	if err != nil {
		t.Fatalf("buildQueue: %v", err)
	}
	// 2 + 1 = 3 tasks; the unlabelled GPU node and cpu-node are skipped.
	if len(queue) != 3 {
		t.Fatalf("expected 3 tasks, got %d: %+v", len(queue), queue)
	}
	if queue[0].ID != "gpu-node-01-gpu-0" || queue[0].State != gpuv1alpha1.TaskStatePending {
		t.Fatalf("unexpected first task: %+v", queue[0])
	}
	cr.Spec.AllLjRuntimeKitEnabled = false
	queue, err = c.buildQueue(cr)
	if err != nil {
		t.Fatal(err)
	}
	if len(queue) != 0 {
		t.Fatalf("allLjRuntimeKitEnabled=false must respect affinity, got %d tasks", len(queue))
	}
}

func TestBuildQueueAffinityNoMatch(t *testing.T) {
	n1 := gpuNode("gpu-node-01", 2)
	n1.Labels[NodeEnabledLabelKey] = NodeEnabledLabelValue
	c, cancel := newTestController(t, n1)
	defer cancel()

	cr := testCR("default", "t1", gpuv1alpha1.ActiveMonitoringSpec{
		Affinity: &v1.Affinity{NodeAffinity: &v1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
				NodeSelectorTerms: []v1.NodeSelectorTerm{{MatchExpressions: []v1.NodeSelectorRequirement{{
					Key: HostnameLabelKey, Operator: v1.NodeSelectorOpIn, Values: []string{"does-not-exist"},
				}}}},
			},
		}},
	})
	queue, err := c.buildQueue(cr)
	if err != nil {
		t.Fatalf("buildQueue: %v", err)
	}
	if len(queue) != 0 {
		t.Fatalf("expected empty queue for no affinity match, got %d", len(queue))
	}
}

func TestBuildQueueAffinityMatch(t *testing.T) {
	n1 := gpuNode("gpu-node-01", 2)
	n1.Labels[NodeEnabledLabelKey] = NodeEnabledLabelValue
	n2 := gpuNode("gpu-node-02", 2)
	n2.Labels[NodeEnabledLabelKey] = NodeEnabledLabelValue
	c, cancel := newTestController(t, n1, n2)
	defer cancel()

	cr := testCR("default", "t1", gpuv1alpha1.ActiveMonitoringSpec{
		Affinity: &v1.Affinity{NodeAffinity: &v1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
				NodeSelectorTerms: []v1.NodeSelectorTerm{{MatchExpressions: []v1.NodeSelectorRequirement{{
					Key: HostnameLabelKey, Operator: v1.NodeSelectorOpIn, Values: []string{"gpu-node-02"},
				}}}},
			},
		}},
	})
	queue, err := c.buildQueue(cr)
	if err != nil {
		t.Fatalf("buildQueue: %v", err)
	}
	if len(queue) != 2 {
		t.Fatalf("expected 2 tasks (only gpu-node-02), got %d", len(queue))
	}
	for _, task := range queue {
		if task.NodeName != "gpu-node-02" {
			t.Fatalf("unexpected node in queue: %+v", task)
		}
	}
}
