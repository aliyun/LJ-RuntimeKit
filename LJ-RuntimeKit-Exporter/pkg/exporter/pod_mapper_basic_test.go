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

package exporter

import (
	"errors"
	"testing"

	"github.com/lj-runtimekit/exporter/pkg/common"
)

type mockPodClient struct {
	pods []PodInfo
	err  error
}

func (m *mockPodClient) GetGpuPodInfo() ([]PodInfo, error) {
	return m.pods, m.err
}

func (m *mockPodClient) Name() string { return "mock" }

type mockFormatter struct {
	labels map[PodAttrType][]string
}

func (m *mockFormatter) Format(int32, interface{}) string { return "" }

func (m *mockFormatter) GetPodLabel(attr PodAttrType) ([]string, bool) {
	l, ok := m.labels[attr]
	return l, ok
}

func (m *mockFormatter) GetLabelByID(LabelID) ([]string, bool) { return nil, false }

func newTestMapper(podClient PodInfoInterface, formatter FormatterInterface) *BasicPodMapper {
	return &BasicPodMapper{
		podClient: podClient,
		formatter: formatter,
		config:    &Config{},
		gpuInfo:   nil,
	}
}

func TestBasicPodMapper_Process_FindsPodByUUID(t *testing.T) {
	podClient := &mockPodClient{
		pods: []PodInfo{
			{
				Full:          true,
				Name:          "test-pod",
				Namespace:     "default",
				Phase:         "Running",
				ContainerName: "test-container",
				ContainerId:   "container-1",
				DeviceId:      "gpu-uuid-1",
			},
		},
	}

	formatter := &mockFormatter{
		labels: map[PodAttrType][]string{
			PodAttrName:      {"pod"},
			PodAttrNamespace: {"namespace"},
			PodAttrContainer: {"container"},
		},
	}

	mapper := newTestMapper(podClient, formatter)

	metrics := []common.MetricItem{
		{
			Counter: &common.Counter{
				MetricName: "gpu_utilization",
				PodMap:     true,
			},
			UUID: "gpu-uuid-1",
			Egpu: false,
			Full: true,
		},
	}

	result, err := mapper.Process(metrics)
	if err != nil {
		t.Fatalf("Process() returned unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("Process() returned %d metrics, want 1", len(result))
	}

	labels := result[0].Labels
	if labels == nil {
		t.Fatal("Process() did not add labels to the metric")
	}

	if labels["pod"] != "test-pod" {
		t.Errorf("labels['pod'] = %q, want 'test-pod'", labels["pod"])
	}
	if labels["namespace"] != "default" {
		t.Errorf("labels['namespace'] = %q, want 'default'", labels["namespace"])
	}
	if labels["container"] != "test-container" {
		t.Errorf("labels['container'] = %q, want 'test-container'", labels["container"])
	}
}

func TestBasicPodMapper_Process_PodNotFound(t *testing.T) {
	podClient := &mockPodClient{
		pods: []PodInfo{},
	}

	formatter := &mockFormatter{
		labels: map[PodAttrType][]string{
			PodAttrName: {"pod"},
		},
	}

	mapper := newTestMapper(podClient, formatter)

	metrics := []common.MetricItem{
		{
			Counter: &common.Counter{
				MetricName: "gpu_utilization",
				PodMap:     true,
			},
			UUID: "gpu-uuid-999",
			Egpu: false,
			Full: true,
		},
	}

	result, err := mapper.Process(metrics)
	if err != nil {
		t.Fatalf("Process() returned unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("Process() returned %d metrics, want 1", len(result))
	}

	// Pod not found, so no labels should be added
	if result[0].Labels != nil && len(result[0].Labels) > 0 {
		t.Errorf("Process() added labels when pod was not found: %v", result[0].Labels)
	}
}

func TestBasicPodMapper_Process_CounterNotPodMap(t *testing.T) {
	podClient := &mockPodClient{
		pods: []PodInfo{
			{
				Full:     true,
				Name:     "test-pod",
				DeviceId: "gpu-uuid-1",
			},
		},
	}

	formatter := &mockFormatter{
		labels: map[PodAttrType][]string{
			PodAttrName: {"pod"},
		},
	}

	mapper := newTestMapper(podClient, formatter)

	metrics := []common.MetricItem{
		{
			Counter: &common.Counter{
				MetricName: "gpu_temperature",
				PodMap:     false,
			},
			UUID: "gpu-uuid-1",
			Egpu: false,
			Full: true,
		},
	}

	result, err := mapper.Process(metrics)
	if err != nil {
		t.Fatalf("Process() returned unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("Process() returned %d metrics, want 1", len(result))
	}

	// Counter.PodMap=false, so Process should skip label mapping
	if result[0].Labels != nil && len(result[0].Labels) > 0 {
		t.Errorf("Process() added labels when Counter.PodMap=false: %v", result[0].Labels)
	}
}

func TestBasicPodMapper_Process_MigUUIDPriority(t *testing.T) {
	podClient := &mockPodClient{
		pods: []PodInfo{
			{
				Full:          false,
				Name:          "mig-pod",
				Namespace:     "mig-ns",
				Phase:         "Running",
				ContainerName: "mig-container",
				ContainerId:   "mig-container-id",
				DeviceId:      "MIG-device-id",
			},
		},
	}

	formatter := &mockFormatter{
		labels: map[PodAttrType][]string{
			PodAttrName:      {"pod"},
			PodAttrNamespace: {"namespace"},
			PodAttrContainer: {"container"},
		},
	}

	mapper := newTestMapper(podClient, formatter)

	// Both MigUUID and UUID are set; MigUUID should take priority
	metrics := []common.MetricItem{
		{
			Counter: &common.Counter{
				MetricName: "gpu_utilization",
				PodMap:     true,
			},
			UUID:    "gpu-uuid-1",
			MigUUID: "MIG-device-id",
			Egpu:    false,
			Full:    false,
		},
	}

	result, err := mapper.Process(metrics)
	if err != nil {
		t.Fatalf("Process() returned unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("Process() returned %d metrics, want 1", len(result))
	}

	labels := result[0].Labels
	if labels == nil {
		t.Fatal("Process() did not add labels to the metric")
	}

	// If MigUUID is used as deviceId, it should find the MIG pod
	if labels["pod"] != "mig-pod" {
		t.Errorf("labels['pod'] = %q, want 'mig-pod' (MigUUID should take priority)", labels["pod"])
	}
	if labels["namespace"] != "mig-ns" {
		t.Errorf("labels['namespace'] = %q, want 'mig-ns'", labels["namespace"])
	}
	if labels["container"] != "mig-container" {
		t.Errorf("labels['container'] = %q, want 'mig-container'", labels["container"])
	}
}

func TestBasicPodMapper_Process_PodClientError(t *testing.T) {
	expectedErr := errors.New("pod client connection failed")
	podClient := &mockPodClient{
		pods: nil,
		err:  expectedErr,
	}

	formatter := &mockFormatter{
		labels: map[PodAttrType][]string{},
	}

	mapper := newTestMapper(podClient, formatter)

	metrics := []common.MetricItem{
		{
			Counter: &common.Counter{
				MetricName: "gpu_utilization",
				PodMap:     true,
			},
			UUID: "gpu-uuid-1",
			Egpu: false,
			Full: true,
		},
	}

	result, err := mapper.Process(metrics)
	if err == nil {
		t.Fatal("Process() did not return expected error")
	}
	if err != expectedErr {
		t.Errorf("Process() error = %v, want %v", err, expectedErr)
	}
	if result != nil {
		t.Errorf("Process() returned non-nil result on error: %v", result)
	}
}

func TestBasicPodMapper_handleNonEGPU_SingleFullPod(t *testing.T) {
	formatter := &mockFormatter{
		labels: map[PodAttrType][]string{
			PodAttrName:      {"pod"},
			PodAttrNamespace: {"namespace"},
			PodAttrContainer: {"container"},
		},
	}

	mapper := newTestMapper(nil, formatter)

	metric := &common.MetricItem{
		Labels: make(map[string]string),
		Egpu:   false,
	}

	pods := []PodInfo{
		{
			Full:          true,
			Name:          "single-pod",
			Namespace:     "default",
			ContainerName: "single-container",
			DeviceId:      "gpu-uuid-1",
		},
	}

	mapper.handleNonEGPU(metric, pods)

	if metric.Labels["pod"] != "single-pod" {
		t.Errorf("labels['pod'] = %q, want 'single-pod'", metric.Labels["pod"])
	}
	if metric.Labels["namespace"] != "default" {
		t.Errorf("labels['namespace'] = %q, want 'default'", metric.Labels["namespace"])
	}
	if metric.Labels["container"] != "single-container" {
		t.Errorf("labels['container'] = %q, want 'single-container'", metric.Labels["container"])
	}
}

func TestBasicPodMapper_handleNonEGPU_MultiplePods(t *testing.T) {
	formatter := &mockFormatter{
		labels: map[PodAttrType][]string{
			PodAttrName: {"pod"},
		},
	}

	mapper := newTestMapper(nil, formatter)

	metric := &common.MetricItem{
		Labels: make(map[string]string),
		Egpu:   false,
	}

	// Multiple pods for the same device — handleNonEGPU should NOT add labels
	// because the condition requires len(pods) == 1
	pods := []PodInfo{
		{
			Full:     true,
			Name:     "pod-one",
			DeviceId: "gpu-uuid-1",
		},
		{
			Full:     true,
			Name:     "pod-two",
			DeviceId: "gpu-uuid-1",
		},
	}

	mapper.handleNonEGPU(metric, pods)

	if len(metric.Labels) != 0 {
		t.Errorf("handleNonEGPU added labels with multiple pods: %v, want no labels", metric.Labels)
	}
}

func TestBasicPodMapper_handleNonEGPU_MIGPod(t *testing.T) {
	formatter := &mockFormatter{
		labels: map[PodAttrType][]string{
			PodAttrName:      {"pod"},
			PodAttrNamespace: {"namespace"},
			PodAttrContainer: {"container"},
		},
	}

	mapper := newTestMapper(nil, formatter)

	metric := &common.MetricItem{
		Labels: make(map[string]string),
		Egpu:   false,
	}

	// Single pod with MIG DeviceId (starts with "MIG")
	pods := []PodInfo{
		{
			Full:          false,
			Name:          "mig-pod",
			Namespace:     "mig-namespace",
			ContainerName: "mig-container",
			DeviceId:      "MIG-abc123",
		},
	}

	mapper.handleNonEGPU(metric, pods)

	if metric.Labels["pod"] != "mig-pod" {
		t.Errorf("labels['pod'] = %q, want 'mig-pod'", metric.Labels["pod"])
	}
	if metric.Labels["namespace"] != "mig-namespace" {
		t.Errorf("labels['namespace'] = %q, want 'mig-namespace'", metric.Labels["namespace"])
	}
	if metric.Labels["container"] != "mig-container" {
		t.Errorf("labels['container'] = %q, want 'mig-container'", metric.Labels["container"])
	}
}
