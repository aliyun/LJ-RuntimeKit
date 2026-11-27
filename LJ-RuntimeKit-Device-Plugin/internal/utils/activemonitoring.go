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
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

// IsActiveMonitoringPod reports whether the pod is an ActiveMonitoring detection Pod.
func IsActiveMonitoringPod(pod *v1.Pod) bool {
	return pod != nil && pod.Labels[LabelActiveMonitoring] == "true"
}

// GetActiveMonitoringTargetMinor reads the rt-diag-target-minor annotation and returns the
// GPU minor an active monitoring Pod is pinned to.
func GetActiveMonitoringTargetMinor(pod *v1.Pod) (int, error) {
	if pod == nil {
		return 0, fmt.Errorf("nil pod")
	}
	raw, ok := pod.Annotations[AnnoActiveMonitoringTargetMinor]
	if !ok {
		return 0, fmt.Errorf("pod %s/%s missing annotation %s", pod.Namespace, pod.Name, AnnoActiveMonitoringTargetMinor)
	}
	minor, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, fmt.Errorf("pod %s/%s annotation %s=%q is not a valid minor: %w",
			pod.Namespace, pod.Name, AnnoActiveMonitoringTargetMinor, raw, err)
	}
	return minor, nil
}

// ListActiveMonitoringPodsOnNode returns the active monitoring Pods pinned to the current node.
func ListActiveMonitoringPodsOnNode(k8sClient kubernetes.Interface) ([]*v1.Pod, error) {
	fieldSelector := fields.SelectorFromSet(fields.Set{"spec.nodeName": nodeName}).String()
	labelSelector := labels.SelectorFromSet(labels.Set{LabelActiveMonitoring: "true"}).String()

	var podList *v1.PodList
	var err error
	for i := 0; i < retries; i++ {
		podList, err = k8sClient.CoreV1().Pods(v1.NamespaceAll).List(context.Background(), metav1.ListOptions{
			FieldSelector: fieldSelector,
			LabelSelector: labelSelector,
		})
		if err == nil {
			break
		}
		klog.Warningf("list activemonitoring pods on node %s failed (attempt %d): %v", nodeName, i+1, err)
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to list activemonitoring pods on node %s: %w", nodeName, err)
	}

	pods := make([]*v1.Pod, 0, len(podList.Items))
	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.Spec.NodeName != nodeName || !IsActiveMonitoringPod(pod) {
			continue
		}
		pods = append(pods, pod.DeepCopy())
	}
	return pods, nil
}

// MatchActiveMonitoringByMinor returns the active monitoring Pods in the given minor set.
func MatchActiveMonitoringByMinor(pods []*v1.Pod, minors []int) []*v1.Pod {
	want := make(map[int]struct{}, len(minors))
	for _, m := range minors {
		want[m] = struct{}{}
	}
	matched := make([]*v1.Pod, 0, len(pods))
	for _, pod := range pods {
		minor, err := GetActiveMonitoringTargetMinor(pod)
		if err != nil {
			klog.V(5).Infof("skip activemonitoring pod without a valid target minor: %v", err)
			continue
		}
		if _, ok := want[minor]; ok {
			matched = append(matched, pod)
		}
	}
	return matched
}
