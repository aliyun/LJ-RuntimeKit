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

package common

import (
	"reflect"
	"testing"
)

func TestCounterClone(t *testing.T) {
	// Create a Counter with all fields set
	original := &Counter{
		PodMap:     true,
		PullOnly:   false,
		FieldID:    42,
		RepEvent:   true,
		FieldName:  "test_field",
		MetricName: "test_metric",
		ShortName:  "tm",
		PromType:   "gauge",
		Help:       "test help text",
	}

	// Clone it
	cloned := original.Clone()

	// Verify they are different pointers (not the same object)
	if original == cloned {
		t.Error("Clone() returned the same pointer, expected a new instance")
	}

	// Verify all fields that Clone() actually copies are equal.
	// Note: Counter.Clone() does NOT copy RepEvent, so we verify
	// the copied fields individually rather than using DeepEqual on the whole struct.
	if cloned.PodMap != original.PodMap {
		t.Errorf("cloned.PodMap = %v, want %v", cloned.PodMap, original.PodMap)
	}
	if cloned.PullOnly != original.PullOnly {
		t.Errorf("cloned.PullOnly = %v, want %v", cloned.PullOnly, original.PullOnly)
	}
	if cloned.FieldID != original.FieldID {
		t.Errorf("cloned.FieldID = %d, want %d", cloned.FieldID, original.FieldID)
	}
	if cloned.FieldName != original.FieldName {
		t.Errorf("cloned.FieldName = %q, want %q", cloned.FieldName, original.FieldName)
	}
	if cloned.MetricName != original.MetricName {
		t.Errorf("cloned.MetricName = %q, want %q", cloned.MetricName, original.MetricName)
	}
	if cloned.ShortName != original.ShortName {
		t.Errorf("cloned.ShortName = %q, want %q", cloned.ShortName, original.ShortName)
	}
	if cloned.PromType != original.PromType {
		t.Errorf("cloned.PromType = %q, want %q", cloned.PromType, original.PromType)
	}
	if cloned.Help != original.Help {
		t.Errorf("cloned.Help = %q, want %q", cloned.Help, original.Help)
	}

	// Verify RepEvent is NOT copied (it is zero-valued in the clone)
	if cloned.RepEvent != false {
		t.Errorf("cloned.RepEvent = %v, want false (Clone does not copy RepEvent)", cloned.RepEvent)
	}

	// Create a counter with RepEvent=false to verify DeepEqual works when RepEvent matches
	originalNoRepEvent := &Counter{
		PodMap:     true,
		PullOnly:   false,
		FieldID:    42,
		RepEvent:   false,
		FieldName:  "test_field",
		MetricName: "test_metric",
		ShortName:  "tm",
		PromType:   "gauge",
		Help:       "test help text",
	}
	clonedNoRepEvent := originalNoRepEvent.Clone()
	if !reflect.DeepEqual(originalNoRepEvent, clonedNoRepEvent) {
		t.Errorf("Clone() with RepEvent=false: got %+v, want %+v", clonedNoRepEvent, originalNoRepEvent)
	}

	// Modify the clone and verify original is unchanged
	cloned.PodMap = false
	cloned.FieldID = 99
	cloned.FieldName = "modified_field"
	cloned.MetricName = "modified_metric"
	cloned.ShortName = "mm"
	cloned.PromType = "counter"
	cloned.Help = "modified help"
	cloned.PullOnly = true

	if original.PodMap != true {
		t.Errorf("original.PodMap changed to %v, want true", original.PodMap)
	}
	if original.FieldID != 42 {
		t.Errorf("original.FieldID changed to %d, want 42", original.FieldID)
	}
	if original.FieldName != "test_field" {
		t.Errorf("original.FieldName changed to %q, want %q", original.FieldName, "test_field")
	}
	if original.MetricName != "test_metric" {
		t.Errorf("original.MetricName changed to %q, want %q", original.MetricName, "test_metric")
	}
	if original.ShortName != "tm" {
		t.Errorf("original.ShortName changed to %q, want %q", original.ShortName, "tm")
	}
	if original.PromType != "gauge" {
		t.Errorf("original.PromType changed to %q, want %q", original.PromType, "gauge")
	}
	if original.Help != "test help text" {
		t.Errorf("original.Help changed to %q, want %q", original.Help, "test help text")
	}
	if original.PullOnly != false {
		t.Errorf("original.PullOnly changed to %v, want false", original.PullOnly)
	}
}

func TestMetricItemClone(t *testing.T) {
	// Create a Counter for the MetricItem
	counter := &Counter{
		PodMap:     true,
		PullOnly:   false,
		FieldID:    10,
		RepEvent:   false,
		FieldName:  "cpu_usage",
		MetricName: "node_cpu_usage",
		ShortName:  "ncu",
		PromType:   "gauge",
		Help:       "CPU usage metric",
	}

	// Create a MetricItem with Counter, Labels map, and RcvLabels map
	original := &MetricItem{
		Counter:       counter,
		Egpu:          true,
		Full:          false,
		Pid:           1234,
		Pname:         "test_process",
		Time:          1700000000,
		Value:         "42.5",
		ID:            "metric-id-1",
		UUID:          "uuid-abc-123",
		ModelName:     "Tesla V100",
		MigUUID:       "mig-uuid-1",
		MigProfile:    "mig-profile-1",
		GPUInstanceID: "gpu-inst-1",
		ComputeId:     "compute-1",
		Message:       "test message",
		HostName:      "host-1",
		NodeName:      "node-1",
		HostSN:        "sn-001",
		NodeIP:        "192.168.1.1",
		Cid:           "cid-1",
		Cgroup:        "/sys/fs/cgroup/test",
		Labels: map[string]string{
			"app":     "webserver",
			"env":     "production",
			"version": "v1.0",
		},
		RcvLabels: map[string]string{
			"region": "us-west-1",
			"zone":   "a",
		},
	}

	// Clone it
	cloned := original.Clone()

	// Verify they are different pointers
	if original == cloned {
		t.Error("Clone() returned the same pointer, expected a new instance")
	}

	// Verify deep equality of the overall structure
	if !reflect.DeepEqual(original, cloned) {
		t.Errorf("Clone() not deeply equal to original.\ngot:  %+v\nwant: %+v", cloned, original)
	}

	// Verify Counter is also cloned (different pointer)
	if original.Counter == cloned.Counter {
		t.Error("Clone() did not deep copy Counter, same pointer")
	}
	if !reflect.DeepEqual(original.Counter, cloned.Counter) {
		t.Errorf("Cloned Counter not equal to original.\ngot:  %+v\nwant: %+v", cloned.Counter, original.Counter)
	}

	// Modify clone's Labels map and verify original is unchanged
	cloned.Labels["app"] = "modified"
	cloned.Labels["new_key"] = "new_value"
	delete(cloned.Labels, "env")

	if original.Labels["app"] != "webserver" {
		t.Errorf("original.Labels[\"app\"] changed to %q, want %q", original.Labels["app"], "webserver")
	}
	if _, exists := original.Labels["env"]; !exists {
		t.Error("original.Labels[\"env\"] was deleted, should still exist")
	}
	if _, exists := original.Labels["new_key"]; exists {
		t.Error("original.Labels has unexpected key \"new_key\"")
	}

	// Modify clone's RcvLabels map and verify original is unchanged
	cloned.RcvLabels["region"] = "modified-region"
	cloned.RcvLabels["new_label"] = "new_value"
	delete(cloned.RcvLabels, "zone")

	if original.RcvLabels["region"] != "us-west-1" {
		t.Errorf("original.RcvLabels[\"region\"] changed to %q, want %q", original.RcvLabels["region"], "us-west-1")
	}
	if _, exists := original.RcvLabels["zone"]; !exists {
		t.Error("original.RcvLabels[\"zone\"] was isolated, should still exist")
	}
	if _, exists := original.RcvLabels["new_label"]; exists {
		t.Error("original.RcvLabels has unexpected key \"new_label\"")
	}

	// Modify clone's Counter and verify original is unchanged
	cloned.Counter.FieldID = 999
	cloned.Counter.FieldName = "modified_field"
	cloned.Counter.MetricName = "modified_metric"

	if original.Counter.FieldID != 10 {
		t.Errorf("original.Counter.FieldID changed to %d, want 10", original.Counter.FieldID)
	}
	if original.Counter.FieldName != "cpu_usage" {
		t.Errorf("original.Counter.FieldName changed to %q, want %q", original.Counter.FieldName, "cpu_usage")
	}
	if original.Counter.MetricName != "node_cpu_usage" {
		t.Errorf("original.Counter.MetricName changed to %q, want %q", original.Counter.MetricName, "node_cpu_usage")
	}

	// Modify scalar fields on clone and verify original is unchanged
	cloned.Egpu = false
	cloned.Full = true
	cloned.Pid = 5678
	cloned.Pname = "modified_process"
	cloned.Time = 9999999999
	cloned.Value = "99.9"
	cloned.ID = "modified-id"

	if original.Egpu != true {
		t.Errorf("original.Egpu changed to %v, want true", original.Egpu)
	}
	if original.Full != false {
		t.Errorf("original.Full changed to %v, want false", original.Full)
	}
	if original.Pid != 1234 {
		t.Errorf("original.Pid changed to %d, want 1234", original.Pid)
	}
	if original.Pname != "test_process" {
		t.Errorf("original.Pname changed to %q, want %q", original.Pname, "test_process")
	}
	if original.Time != 1700000000 {
		t.Errorf("original.Time changed to %d, want 1700000000", original.Time)
	}
	if original.Value != "42.5" {
		t.Errorf("original.Value changed to %q, want %q", original.Value, "42.5")
	}
	if original.ID != "metric-id-1" {
		t.Errorf("original.ID changed to %q, want %q", original.ID, "metric-id-1")
	}
}

func TestMetricItemCloneNilReceiver(t *testing.T) {
	var mi *MetricItem
	result := mi.Clone()
	if result != nil {
		t.Errorf("Clone() on nil receiver = %+v, want nil", result)
	}
}
