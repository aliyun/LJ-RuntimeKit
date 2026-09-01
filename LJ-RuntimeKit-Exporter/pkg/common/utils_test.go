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
	"sync"
	"testing"
	"time"
)

func TestWaitWithTimeout_CompletesBeforeTimeout(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond)
	}()

	err := WaitWithTimeout(&wg, 5*time.Second)
	if err != nil {
		t.Errorf("expected nil error, got: %v", err)
	}
}

func TestWaitWithTimeout_NeverCompletes(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	// Never call wg.Done() — this goroutine will never complete

	err := WaitWithTimeout(&wg, 50*time.Millisecond)
	if err == nil {
		t.Error("expected timeout error, got nil")
	}
}

func TestMetricItemGetLabels_NilReceiver(t *testing.T) {
	var m *MetricItem
	labels := m.GetLabels()
	if len(labels) != 0 {
		t.Errorf("expected empty map for nil receiver, got: %v", labels)
	}
}

func TestMetricItemGetLabels_BothRcvLabelsAndLabels(t *testing.T) {
	m := &MetricItem{
		RcvLabels: map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
		},
		Labels: map[string]string{
			"b": "20", // overlaps with RcvLabels — should overwrite
			"d": "40",
		},
	}

	labels := m.GetLabels()

	// Verify RcvLabels keys are present
	if v, ok := labels["a"]; !ok || v != "1" {
		t.Errorf("expected labels[a]=1, got %q (ok=%v)", v, ok)
	}
	if v, ok := labels["c"]; !ok || v != "3" {
		t.Errorf("expected labels[c]=3, got %q (ok=%v)", v, ok)
	}

	// Verify Labels key "b" overwrote RcvLabels["b"]
	if v, ok := labels["b"]; !ok || v != "20" {
		t.Errorf("expected labels[b]=20 (Labels overwrites RcvLabels), got %q (ok=%v)", v, ok)
	}

	// Verify Labels-only key is present
	if v, ok := labels["d"]; !ok || v != "40" {
		t.Errorf("expected labels[d]=40, got %q (ok=%v)", v, ok)
	}

	// Total should be 4 keys
	if len(labels) != 4 {
		t.Errorf("expected 4 labels, got %d", len(labels))
	}
}

func TestMetricItemGetLabelValue_NilReceiver(t *testing.T) {
	var m *MetricItem
	val := m.GetLabelValue("any")
	if val != "" {
		t.Errorf("expected empty string for nil receiver, got: %q", val)
	}
}

func TestMetricItemGetLabelValue_LabelsFirst(t *testing.T) {
	m := &MetricItem{
		Labels: map[string]string{
			"env": "production",
			"foo": "bar",
		},
		RcvLabels: map[string]string{
			"region": "cn-hangzhou",
		},
	}

	// Key exists in Labels
	if v := m.GetLabelValue("env"); v != "production" {
		t.Errorf("expected env=production, got %q", v)
	}

	// Key exists only in RcvLabels
	if v := m.GetLabelValue("region"); v != "cn-hangzhou" {
		t.Errorf("expected region=cn-hangzhou, got %q", v)
	}

	// Key does not exist anywhere
	if v := m.GetLabelValue("missing"); v != "" {
		t.Errorf("expected empty string for missing key, got %q", v)
	}
}

func TestMetricItemGetLabelValue_KeyInBoth_ReturnsLabelsValue(t *testing.T) {
	m := &MetricItem{
		Labels: map[string]string{
			"overlap": "labels-wins",
		},
		RcvLabels: map[string]string{
			"overlap": "rcv-labels-loses",
		},
	}

	v := m.GetLabelValue("overlap")
	if v != "labels-wins" {
		t.Errorf("expected Labels value 'labels-wins' (checked first), got %q", v)
	}
}
