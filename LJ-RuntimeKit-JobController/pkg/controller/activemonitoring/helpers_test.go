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
	"context"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	gpuv1alpha1 "github.com/lj-runtimekit/job-controller/pkg/apis/gpu/v1alpha1"
)

// newTestController builds a Controller backed by a fake dynamic client with the
// given objects preloaded, starts its informers and waits for cache sync. The
// returned cancel func stops the informers.
func newTestController(t *testing.T, objs ...runtime.Object) (*Controller, context.CancelFunc) {
	t.Helper()
	scheme := runtime.NewScheme()
	unstructuredObjs := make([]runtime.Object, 0, len(objs))
	for _, obj := range objs {
		unstructuredObjs = append(unstructuredObjs, mustUnstructured(t, obj))
	}
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			activemonitoringGVR: "ActiveMonitoringList",
			podGVR:              "PodList",
			nodeGVR:             "NodeList",
		}, unstructuredObjs...)

	c := NewController(Config{DynamicClient: client})
	ctx, cancel := context.WithCancel(context.Background())
	if err := c.startInformers(ctx); err != nil {
		cancel()
		t.Fatalf("start informers: %v", err)
	}
	return c, cancel
}

func mustUnstructured(t *testing.T, obj runtime.Object) *unstructured.Unstructured {
	t.Helper()
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		t.Fatalf("convert to unstructured: %v", err)
	}
	return &unstructured.Unstructured{Object: m}
}

// gpuNode builds a Ready node advertising gpuCount units of aliyun.com/gpu.
func gpuNode(name string, gpuCount int64) *v1.Node {
	return &v1.Node{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Node"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{HostnameLabelKey: name}},
		Status: v1.NodeStatus{
			Capacity: v1.ResourceList{
				v1.ResourceName(defaultGPUResourceName): *resource.NewQuantity(gpuCount, resource.DecimalSI),
			},
			Conditions: []v1.NodeCondition{{Type: v1.NodeReady, Status: v1.ConditionTrue}},
		},
	}
}

// testPod builds a detection Pod for crName in the given phase, created at t0.
func testPod(ns, name, crName string, phase v1.PodPhase, createdAt time.Time) *v1.Pod {
	return &v1.Pod{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         ns,
			Labels:            map[string]string{LabelActiveMonitoring: "true", LabelActiveMonitoringCR: crName},
			CreationTimestamp: metav1.Time{Time: createdAt},
		},
		Status: v1.PodStatus{Phase: phase},
	}
}

// testCR builds a ActiveMonitoring CR with the given spec.
func testCR(ns, name string, spec gpuv1alpha1.ActiveMonitoringSpec) *gpuv1alpha1.ActiveMonitoring {
	return &gpuv1alpha1.ActiveMonitoring{
		TypeMeta:   metav1.TypeMeta{APIVersion: gpuv1alpha1.SchemeGroupVersion.String(), Kind: Kind},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, UID: types.UID(name + "-uid")},
		Spec:       spec,
	}
}

// nodeMeta builds ObjectMeta for a node with the hostname label and given annotations.
func nodeMeta(name string, annotations map[string]string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: name, Labels: map[string]string{HostnameLabelKey: name}, Annotations: annotations}
}

// gpuCapacity builds a ResourceList advertising n units of the default GPU resource.
func gpuCapacity(n int64) v1.ResourceList {
	return v1.ResourceList{v1.ResourceName(defaultGPUResourceName): *resource.NewQuantity(n, resource.DecimalSI)}
}

func intSliceEqual(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
