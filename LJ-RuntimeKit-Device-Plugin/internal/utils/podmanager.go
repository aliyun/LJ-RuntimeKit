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
	"sort"
	"strconv"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

const retries = 3

var (
	nodeName string
)

func SetNodeName(node string) {
	nodeName = node
}

func SortPodByAssumeTime(pods []*v1.Pod) []*v1.Pod {
	newPodList := make(orderedPodByAssumeTime, 0, len(pods))
	for _, v := range pods {
		newPodList = append(newPodList, v)
	}
	sort.Sort(newPodList)
	return newPodList
}

type orderedPodByAssumeTime []*v1.Pod

func (o orderedPodByAssumeTime) Len() int { return len(o) }
func (o orderedPodByAssumeTime) Less(i, j int) bool {
	return getPodAssumeTime(o[i]) <= getPodAssumeTime(o[j])
}
func (o orderedPodByAssumeTime) Swap(i, j int) { o[i], o[j] = o[j], o[i] }

func getPodAssumeTime(pod *v1.Pod) (assumeTime uint64) {
	if assumeTimeStr, ok := pod.ObjectMeta.Annotations[AnnotationResourceAssumeTime]; ok {
		u64, err := strconv.ParseUint(assumeTimeStr, 10, 64)
		if err != nil {
			klog.Warningf("Failed to parse assume Timestamp %s due to %v", assumeTimeStr, err)
		} else {
			assumeTime = u64
		}
	}
	return assumeTime
}

func GetPendingPod(k8sClient *kubernetes.Clientset) ([]*v1.Pod, error) {
	podList, err := getPendingPodList(k8sClient)
	if err != nil {
		return nil, err
	}
	pods := make([]*v1.Pod, 0)
	podMap := make(map[types.UID]struct{})
	for _, pod := range podList.Items {
		klog.V(4).Infof("pod %s/%s nodeName %s currentNode %s", pod.Namespace, pod.Name, pod.Spec.NodeName, nodeName)

		if len(pod.Spec.NodeName) <= 0 {
			klog.V(4).Infof("pod %s/%s nodeName is empty, skipping", pod.Namespace, pod.Name)
			continue
		}
		if pod.Spec.NodeName != nodeName {
			klog.V(4).Infof("pod %s/%s nodeName %s does not match current node %s, skipping", pod.Namespace, pod.Name, pod.Spec.NodeName, nodeName)
			continue
		}
		_, ok := podMap[pod.UID]
		if !ok {
			pods = append(pods, pod.DeepCopy())
			podMap[pod.UID] = struct{}{}
		}
	}

	for _, pod := range pods {
		klog.V(4).Infof("pending pod %s/%s", pod.Namespace, pod.Name)
	}

	return pods, nil
}

// GetPodByUID returns the pod with the given uid on the current node. The
// full identity is resolved through the apiserver because the kubelet
// checkpoint only carries the uid.
func GetPodByUID(k8sClient kubernetes.Interface, podUID string) (*v1.Pod, error) {
	selector := fields.SelectorFromSet(fields.Set{"spec.nodeName": nodeName})
	podList, err := k8sClient.CoreV1().Pods(v1.NamespaceAll).List(context.Background(), metav1.ListOptions{
		FieldSelector: selector.String(),
		LabelSelector: labels.Everything().String(),
	})
	for i := 0; i < retries && err != nil; i++ {
		time.Sleep(200 * time.Millisecond)
		podList, err = k8sClient.CoreV1().Pods(v1.NamespaceAll).List(context.Background(), metav1.ListOptions{
			FieldSelector: selector.String(),
			LabelSelector: labels.Everything().String(),
		})
	}
	if err != nil {
		return nil, fmt.Errorf("failed to list pods on node %s: %w", nodeName, err)
	}
	for i := range podList.Items {
		if string(podList.Items[i].UID) == podUID {
			return podList.Items[i].DeepCopy(), nil
		}
	}
	return nil, fmt.Errorf("pod with uid %s not found on node %s", podUID, nodeName)
}

func getPendingPodList(client *kubernetes.Clientset) (*v1.PodList, error) {
	podList, err := getPodListsByListAPIServer(client)
	if err != nil {
		return nil, err
	}
	resultPodList := &v1.PodList{}
	for _, metaPod := range podList.Items {
		if metaPod.Status.Phase != v1.PodPending {
			continue
		}
		resultPodList.Items = append(resultPodList.Items, metaPod)
	}

	if len(resultPodList.Items) == 0 {
		return nil, fmt.Errorf("not found pending pod")
	}
	return resultPodList, nil
}

func getPodListsByListAPIServer(k8sClient *kubernetes.Clientset) (*v1.PodList, error) {
	selector := fields.SelectorFromSet(fields.Set{"spec.nodeName": nodeName, "status.phase": "Pending"})
	podList, err := k8sClient.CoreV1().Pods(v1.NamespaceAll).List(context.Background(), metav1.ListOptions{
		FieldSelector: selector.String(),
		LabelSelector: labels.Everything().String(),
	})
	for i := 0; i < retries && err != nil; i++ {
		podList, err = k8sClient.CoreV1().Pods(v1.NamespaceAll).List(context.Background(), metav1.ListOptions{
			FieldSelector: selector.String(),
			LabelSelector: labels.Everything().String(),
		})
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get Pods assigned to node %v", nodeName)
	}

	return podList, nil
}
