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
	"fmt"
	"sort"
	"strconv"

	v1 "k8s.io/api/core/v1"

	gpuv1alpha1 "github.com/lj-runtimekit/job-controller/pkg/apis/gpu/v1alpha1"
)

// buildQueue enumerates the target nodes and their GPU minors, producing one Pending
// TaskStatus per (node, minor). TaskID is "{nodeName}-gpu-{minor}".
func (c *Controller) buildQueue(cr *gpuv1alpha1.ActiveMonitoring) ([]gpuv1alpha1.TaskStatus, error) {
	nodes := c.matchTargetNodes(cr)
	var queue []gpuv1alpha1.TaskStatus
	for _, node := range nodes {
		for _, m := range enumerateMinors(node, defaultGPUResourceName) {
			queue = append(queue, gpuv1alpha1.TaskStatus{
				ID:              fmt.Sprintf("%s-gpu-%d", node.Name, m),
				NodeName:        node.Name,
				GPUMinor:        m,
				CurrentCycle:    1,
				State:           gpuv1alpha1.TaskStatePending,
				Attempts:        0,
				CompletedRounds: 0,
			})
		}
	}
	return queue, nil
}

func (c *Controller) matchTargetNodes(cr *gpuv1alpha1.ActiveMonitoring) []*v1.Node {
	nodes := c.listNodes()
	matched := make([]*v1.Node, 0, len(nodes))
	for _, node := range nodes {
		if !nodeHasGPU(node, defaultGPUResourceName) {
			continue
		}
		if cr.Spec.AllLjRuntimeKitEnabled || cr.Spec.Affinity == nil || cr.Spec.Affinity.NodeAffinity == nil {
			matched = append(matched, node)
			continue
		}
		if nodeMatchesNodeAffinity(node, cr.Spec.Affinity.NodeAffinity) {
			matched = append(matched, node)
		}
	}
	sort.Slice(matched, func(i, j int) bool { return matched[i].Name < matched[j].Name })
	return matched
}

// nodeHasGPU reports whether the node advertises a positive capacity for resName.
func nodeHasGPU(node *v1.Node, resName string) bool {
	q, ok := node.Status.Capacity[v1.ResourceName(resName)]
	return ok && q.Value() > 0
}

// enumerateMinors returns the ascending GPU minor numbers on a node. It prefers
// the aliyun.com/device annotation (Devices map key == minor) and falls back to
// 0..capacity[resName]-1 when the annotation is absent or unparseable.
func enumerateMinors(node *v1.Node, resName string) []int {
	if minors := minorsFromDeviceAnnotation(node); len(minors) > 0 {
		return minors
	}
	if q, ok := node.Status.Capacity[v1.ResourceName(resName)]; ok {
		n := int(q.Value())
		if n <= 0 {
			return nil
		}
		minors := make([]int, 0, n)
		for i := 0; i < n; i++ {
			minors = append(minors, i)
		}
		return minors
	}
	return nil
}

// minorsFromDeviceAnnotation parses the aliyun.com/device node annotation and
// returns the sorted set of Devices map keys (each key is a GPU minor).
func minorsFromDeviceAnnotation(node *v1.Node) []int {
	if node.Annotations == nil {
		return nil
	}
	raw, ok := node.Annotations[AnnoNodeDevices]
	if !ok || raw == "" {
		return nil
	}
	var parsed struct {
		Devices map[int]json.RawMessage `json:"devices"`
	}
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return nil
	}
	if len(parsed.Devices) == 0 {
		return nil
	}
	minors := make([]int, 0, len(parsed.Devices))
	for m := range parsed.Devices {
		minors = append(minors, m)
	}
	sort.Ints(minors)
	return minors
}

// This is a self-contained reimplementation because the repo deliberately avoids
// depending on k8s.io/component-helpers (not in go.mod).
func nodeMatchesNodeAffinity(node *v1.Node, na *v1.NodeAffinity) bool {
	if na == nil || na.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return true
	}
	terms := na.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	for _, term := range terms {
		if matchNodeSelectorTerm(node, term) {
			return true
		}
	}
	return false
}

func matchNodeSelectorTerm(node *v1.Node, term v1.NodeSelectorTerm) bool {
	if len(term.MatchExpressions) == 0 && len(term.MatchFields) == 0 {
		return false
	}
	for _, expr := range term.MatchExpressions {
		if !matchNodeSelectorRequirement(node.Labels, expr) {
			return false
		}
	}
	for _, field := range term.MatchFields {
		if !matchNodeFieldSelector(node, field) {
			return false
		}
	}
	return true
}

// matchNodeSelectorRequirement evaluates a single label selector requirement
// supporting the full NodeSelectorOperator set (In/NotIn/Exists/DoesNotExist/Gt/Lt).
func matchNodeSelectorRequirement(labels map[string]string, req v1.NodeSelectorRequirement) bool {
	val, exists := labels[req.Key]
	switch req.Operator {
	case v1.NodeSelectorOpExists:
		return exists
	case v1.NodeSelectorOpDoesNotExist:
		return !exists
	case v1.NodeSelectorOpIn:
		if !exists {
			return false
		}
		for _, v := range req.Values {
			if v == val {
				return true
			}
		}
		return false
	case v1.NodeSelectorOpNotIn:
		if !exists {
			return true
		}
		for _, v := range req.Values {
			if v == val {
				return false
			}
		}
		return true
	case v1.NodeSelectorOpGt, v1.NodeSelectorOpLt:
		if !exists || len(req.Values) != 1 {
			return false
		}
		labelVal, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return false
		}
		reqVal, err := strconv.ParseFloat(req.Values[0], 64)
		if err != nil {
			return false
		}
		if req.Operator == v1.NodeSelectorOpGt {
			return labelVal > reqVal
		}
		return labelVal < reqVal
	default:
		return false
	}
}

// matchNodeFieldSelector supports the metadata.name field selector (the common
// node-pinning case); unsupported field keys cannot be verified and thus do not
// match.
func matchNodeFieldSelector(node *v1.Node, req v1.NodeSelectorRequirement) bool {
	switch req.Key {
	case "metadata.name":
		return matchNodeSelectorRequirement(map[string]string{req.Key: node.Name}, req)
	default:
		return false
	}
}
