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
	"fmt"
	"reflect"
	"testing"

	"github.com/lj-runtimekit/exporter/pkg/common"
)

func TestSetLabelIfNA(t *testing.T) {
	// 测试用例 1：value == NanValue，应该添加标签
	labels := make(map[string]string)
	setLabelIfNA(labels, "gpu", NanValue)
	expected := map[string]string{"gpu": NanValue}
	if !reflect.DeepEqual(labels, expected) {
		t.Errorf("Expected %v, got %v", expected, labels)
	}

	// 测试用例 2：value != NanValue（如 "123"），不应改变 labels
	labels2 := map[string]string{"cpu": "10"}
	setLabelIfNA(labels2, "gpu", "123")
	expected2 := map[string]string{"cpu": "10"}
	if !reflect.DeepEqual(labels2, expected2) {
		t.Errorf("Expected %v, got %v", expected2, labels2)
	}

	// 测试用例 3：value == ""（空字符串），不应写入
	labels3 := make(map[string]string)
	setLabelIfNA(labels3, "gpu", "") // 空字符串
	expected3 := map[string]string{} // 应该不变
	if !reflect.DeepEqual(labels3, expected3) {
		t.Errorf("Expected empty map, got %v", labels3)
	}
}

func TestSetLabelIfNotEmpty(t *testing.T) {
	tests := []struct {
		name     string
		initial  map[string]string
		label    string
		value    string
		expected map[string]string
	}{
		{
			name:     "empty map with valid value writes value",
			initial:  make(map[string]string),
			label:    "hostname",
			value:    "test-host",
			expected: map[string]string{"hostname": "test-host"},
		},
		{
			name:     "existing labels preserved and new label added",
			initial:  map[string]string{"cpu": "10"},
			label:    "hostname",
			value:    "test-host",
			expected: map[string]string{"cpu": "10", "hostname": "test-host"},
		},
		{
			name:     "NanValue should NOT be written",
			initial:  map[string]string{"cpu": "10"},
			label:    "hostname",
			value:    NanValue,
			expected: map[string]string{"cpu": "10"},
		},
		{
			name:     "empty string should NOT be written",
			initial:  map[string]string{"cpu": "10"},
			label:    "hostname",
			value:    "",
			expected: map[string]string{"cpu": "10"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setLabelIfNotEmpty(tt.initial, tt.label, tt.value)
			if !reflect.DeepEqual(tt.initial, tt.expected) {
				t.Errorf("Expected %v, got %v", tt.expected, tt.initial)
			}
		})
	}
}

func TestSetLabelIfNonZero(t *testing.T) {
	tests := []struct {
		name     string
		initial  map[string]string
		label    string
		value    int64
		expected map[string]string
	}{
		{
			name:     "non-zero value written as string",
			initial:  make(map[string]string),
			label:    "pid",
			value:    12345,
			expected: map[string]string{"pid": "12345"},
		},
		{
			name:     "zero value NOT written",
			initial:  map[string]string{"cpu": "10"},
			label:    "pid",
			value:    0,
			expected: map[string]string{"cpu": "10"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setLabelIfNonZero(tt.initial, tt.label, tt.value)
			if !reflect.DeepEqual(tt.initial, tt.expected) {
				t.Errorf("Expected %v, got %v", tt.expected, tt.initial)
			}
		})
	}
}

func TestParseFmtConfigJson_EmptyString(t *testing.T) {
	cfg, err := parseFmtConfigJson("")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(cfg.Labels) != 0 {
		t.Errorf("Expected empty labels, got %v", cfg.Labels)
	}
}

func TestParseFmtConfigJson_EmptyObject(t *testing.T) {
	cfg, err := parseFmtConfigJson("{}")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(cfg.Labels) != 0 {
		t.Errorf("Expected empty labels, got %v", cfg.Labels)
	}
}

func TestParseFmtConfigJson_InvalidJSON(t *testing.T) {
	_, err := parseFmtConfigJson("not valid json")
	if err == nil {
		t.Fatal("Expected error for invalid JSON, got nil")
	}
}

func TestParseFmtConfigJson_ValidConfig(t *testing.T) {
	cfg, err := parseFmtConfigJson(`{"labels": {"1": ["hostname"]}}`)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	val, ok := cfg.Labels[1]
	if !ok {
		t.Fatal("Expected label ID 1 to exist")
	}
	if len(val) != 1 || val[0] != "hostname" {
		t.Errorf("Expected ['hostname'], got %v", val)
	}
}

func TestPodAttr2LabelID(t *testing.T) {
	tests := []struct {
		name     string
		attr     PodAttrType
		expected LabelID
	}{
		{
			name:     "PodAttrName maps to LabelIdPodName",
			attr:     PodAttrName,
			expected: LabelIdPodName,
		},
		{
			name:     "PodAttrNamespace maps to LabelIdPodNamespace",
			attr:     PodAttrNamespace,
			expected: LabelIdPodNamespace,
		},
		{
			name:     "PodAttrContainer maps to LabelIdContainerName",
			attr:     PodAttrContainer,
			expected: LabelIdContainerName,
		},
		{
			name:     "PodAttrContainerID maps to LabelIdContainerID",
			attr:     PodAttrContainerID,
			expected: LabelIdContainerID,
		},
		{
			name:     "PodAttrPhase maps to LabelIdPodPhase",
			attr:     PodAttrPhase,
			expected: LabelIdPodPhase,
		},
		{
			name:     "unknown PodAttrType maps to LabelIdInvalid",
			attr:     PodAttrType(999),
			expected: LabelIdInvalid,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := podAttr2LabelID(tt.attr)
			if result != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, result)
			}
		})
	}
}

func TestGrpcErrString(t *testing.T) {
	tests := []struct {
		name     string
		errCode  GrpcErrCode
		expected string
	}{
		{
			name:     "GrpcRetSuccess returns success",
			errCode:  GrpcRetSuccess,
			expected: "success",
		},
		{
			name:     "GrpcErrCodeSrvUnavailable returns server unavailable",
			errCode:  GrpcErrCodeSrvUnavailable,
			expected: "server unavailable",
		},
		{
			name:     "GrpcErrCodeCallTimeout returns timeout",
			errCode:  GrpcErrCodeCallTimeout,
			expected: "timeout",
		},
		{
			name:     "GrpcErrCodeUnExpected returns unexpected",
			errCode:  GrpcErrCodeUnExpected,
			expected: "unexpected",
		},
		{
			name:     "GrpcErrCodeUnimplemented returns api unimplemented",
			errCode:  GrpcErrCodeUnimplemented,
			expected: "api unimplemented",
		},
		{
			name:     "unknown error code returns unknown",
			errCode:  GrpcErrCode(999),
			expected: "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := grpcErrString(tt.errCode)
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestPodInfoGetVal(t *testing.T) {
	pod := PodInfo{
		Name:          "test-pod",
		Namespace:     "default",
		ContainerName: "test-container",
		Phase:         "Running",
		ContainerId:   "container-123",
	}

	tests := []struct {
		name     string
		attr     PodAttrType
		expected string
	}{
		{
			name:     "PodAttrName returns pod.Name",
			attr:     PodAttrName,
			expected: pod.Name,
		},
		{
			name:     "PodAttrNamespace returns pod.Namespace",
			attr:     PodAttrNamespace,
			expected: pod.Namespace,
		},
		{
			name:     "PodAttrContainer returns pod.ContainerName",
			attr:     PodAttrContainer,
			expected: pod.ContainerName,
		},
		{
			name:     "PodAttrPhase returns pod.Phase",
			attr:     PodAttrPhase,
			expected: pod.Phase,
		},
		{
			name:     "PodAttrContainerID returns pod.ContainerId",
			attr:     PodAttrContainerID,
			expected: pod.ContainerId,
		},
		{
			name:     "unknown PodAttrType returns N/A",
			attr:     PodAttrType(999),
			expected: "N/A",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := pod.GetVal(tt.attr)
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestTransformMetric2fmtItem_NilMetric(t *testing.T) {
	_, err := transformMetric2fmtItem(nil, nil, labelMapProm)
	if err == nil {
		t.Fatal("Expected error for nil metric, got nil")
	}
}

func TestTransformMetric2fmtItem_Labels(t *testing.T) {
	metric := &common.MetricItem{
		Counter: &common.Counter{
			MetricName: "test_metric",
		},
		Value:     "42",
		ID:        "0",
		UUID:      "gpu-uuid-123",
		ModelName: "A100",
		HostName:  "test-host",
		RcvLabels: map[string]string{
			"custom_label": "custom_value",
		},
	}

	item, err := transformMetric2fmtItem(metric, nil, labelMapProm)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Verify RcvLabels are copied to item.Labels
	if val, ok := item.Labels["custom_label"]; !ok || val != "custom_value" {
		t.Errorf("Expected RcvLabel 'custom_label' = 'custom_value', got %q (exists: %v)", val, ok)
	}

	// Verify ModelName mapped to "modelName" label (LabelIdGPUName -> defaultLabelGPUName = "modelName")
	if val, ok := item.Labels["modelName"]; !ok || val != "A100" {
		t.Errorf("Expected modelName label = 'A100', got %q (exists: %v)", val, ok)
	}

	// Verify UUID mapped to "UUID" label (LabelIdGPUUUID -> defaultLabelGPUUUID = "UUID")
	if val, ok := item.Labels["UUID"]; !ok || val != "gpu-uuid-123" {
		t.Errorf("Expected UUID label = 'gpu-uuid-123', got %q (exists: %v)", val, ok)
	}
}

func TestTransformMetric2fmtItem_NodeAcceleratorTotal(t *testing.T) {
	metric := &common.MetricItem{
		Counter: &common.Counter{
			MetricName: "node_accelerator_total",
		},
		Value:    "999",
		ID:       "0",
		HostName: "test-host",
	}

	item, err := transformMetric2fmtItem(metric, nil, labelMapProm)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Value should be "1" regardless of input when MetricName == "node_accelerator_total"
	if item.Value != "1" {
		t.Errorf("Expected value '1', got %q", item.Value)
	}
}

// Additional helper test: verify setLabelIfNotEmpty with fmt import usage
func TestSetLabelIfNotEmpty_FormatValue(t *testing.T) {
	labels := make(map[string]string)
	setLabelIfNonZero(labels, "pid", int64(42))
	if labels["pid"] != fmt.Sprintf("%d", 42) {
		t.Errorf("Expected pid = '42', got %q", labels["pid"])
	}
}
