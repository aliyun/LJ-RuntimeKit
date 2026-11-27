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
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"
)

const amTestNode = "am-node"

// amPod builds a Pod; when isDiag is true it carries the rt-diag label, and a non-empty
// minor sets the target-minor annotation.
func amPod(name, node, minor string, isDiag bool) *v1.Pod {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec:       v1.PodSpec{NodeName: node},
	}
	if isDiag {
		pod.Labels = map[string]string{LabelActiveMonitoring: "true"}
	}
	if minor != "" {
		pod.Annotations = map[string]string{AnnoActiveMonitoringTargetMinor: minor}
	}
	return pod
}

func TestIsActiveMonitoringPod(t *testing.T) {
	assert.True(t, IsActiveMonitoringPod(amPod("d", amTestNode, "0", true)))
	assert.False(t, IsActiveMonitoringPod(amPod("b", amTestNode, "", false)))
	assert.False(t, IsActiveMonitoringPod(nil))
}

func TestGetActiveMonitoringTargetMinor(t *testing.T) {
	minor, err := GetActiveMonitoringTargetMinor(amPod("d", amTestNode, "3", true))
	assert.NoError(t, err)
	assert.Equal(t, 3, minor)

	// Surrounding whitespace is tolerated.
	minor, err = GetActiveMonitoringTargetMinor(amPod("d", amTestNode, " 5 ", true))
	assert.NoError(t, err)
	assert.Equal(t, 5, minor)

	// Missing annotation.
	_, err = GetActiveMonitoringTargetMinor(amPod("d", amTestNode, "", true))
	assert.Error(t, err)

	// Non-numeric annotation.
	_, err = GetActiveMonitoringTargetMinor(amPod("d", amTestNode, "abc", true))
	assert.Error(t, err)

	// Nil pod.
	_, err = GetActiveMonitoringTargetMinor(nil)
	assert.Error(t, err)
}

func TestListActiveMonitoringPodsOnNode(t *testing.T) {
	SetNodeName(amTestNode)
	// A detection Pod on this node, a detection Pod on another node, and a business Pod on
	// this node. Whatever selectors the (fake) apiserver honours, the in-code filter must
	// narrow the result to the single detection Pod pinned to this node.
	client := k8sfake.NewSimpleClientset(
		amPod("diag-here", amTestNode, "1", true),
		amPod("diag-elsewhere", "other-node", "2", true),
		amPod("biz-here", amTestNode, "", false),
	)
	pods, err := ListActiveMonitoringPodsOnNode(client)
	assert.NoError(t, err)
	assert.Len(t, pods, 1)
	assert.Equal(t, "diag-here", pods[0].Name)
}

func TestMatchActiveMonitoringByMinor(t *testing.T) {
	pods := []*v1.Pod{
		amPod("d0", amTestNode, "0", true),
		amPod("d1", amTestNode, "1", true),
		amPod("d2", amTestNode, "2", true),
		amPod("dbad", amTestNode, "x", true), // unparsable minor → skipped
	}
	matched := MatchActiveMonitoringByMinor(pods, []int{1, 2, 7})
	names := make([]string, 0, len(matched))
	for _, p := range matched {
		names = append(names, p.Name)
	}
	assert.ElementsMatch(t, []string{"d1", "d2"}, names)

	// No overlap → empty.
	assert.Empty(t, MatchActiveMonitoringByMinor(pods, []int{9}))
}
