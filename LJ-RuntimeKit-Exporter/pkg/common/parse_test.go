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
	"os"
	"testing"
)

func createTempCSV(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp("", "test_*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	f.WriteString(content)
	f.Close()
	return f.Name()
}

func TestExtractAllCountersNotVerify_DCGM(t *testing.T) {
	content := "DCGM_FI_DEV_GPU_UTIL,true,false,false,gpu_util,utilization,gauge,GPU utilization\n"
	f := createTempCSV(t, content)
	defer os.Remove(f)

	counters, err := ExtractAllCountersNotVerify(f, true)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if len(counters) != 1 {
		t.Fatalf("Expected 1 counter, got %d", len(counters))
	}

	c := counters[0]
	if c.FieldName != "DCGM_FI_DEV_GPU_UTIL" {
		t.Errorf("Expected FieldName 'DCGM_FI_DEV_GPU_UTIL', got '%s'", c.FieldName)
	}
	if c.MetricName != "gpu_util" {
		t.Errorf("Expected MetricName 'gpu_util', got '%s'", c.MetricName)
	}
	if c.ShortName != "utilization" {
		t.Errorf("Expected ShortName 'utilization', got '%s'", c.ShortName)
	}
	if c.PromType != "gauge" {
		t.Errorf("Expected PromType 'gauge', got '%s'", c.PromType)
	}
	if c.Help != "GPU utilization" {
		t.Errorf("Expected Help 'GPU utilization', got '%s'", c.Help)
	}
	if c.PodMap != true {
		t.Errorf("Expected PodMap true, got %v", c.PodMap)
	}
	if c.PullOnly != false {
		t.Errorf("Expected PullOnly false, got %v", c.PullOnly)
	}
	if c.RepEvent != false {
		t.Errorf("Expected RepEvent false, got %v", c.RepEvent)
	}
}

func TestExtractAllCountersNotVerify_AMPF(t *testing.T) {
	content := "AMPF_FI_FROF_FP64_TFPS_USED,true,false,false,fp64,fp64,gauge,FP64 TFLOPS\n"
	f := createTempCSV(t, content)
	defer os.Remove(f)

	counters, err := ExtractAllCountersNotVerify(f, true)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if len(counters) != 1 {
		t.Fatalf("Expected 1 counter, got %d", len(counters))
	}

	c := counters[0]
	if c.FieldName != "AMPF_FI_FROF_FP64_TFPS_USED" {
		t.Errorf("Expected FieldName 'AMPF_FI_FROF_FP64_TFPS_USED', got '%s'", c.FieldName)
	}
	if c.MetricName != "fp64" {
		t.Errorf("Expected MetricName 'fp64', got '%s'", c.MetricName)
	}
	if c.ShortName != "fp64" {
		t.Errorf("Expected ShortName 'fp64', got '%s'", c.ShortName)
	}
	if c.PromType != "gauge" {
		t.Errorf("Expected PromType 'gauge', got '%s'", c.PromType)
	}
	if c.Help != "FP64 TFLOPS" {
		t.Errorf("Expected Help 'FP64 TFLOPS', got '%s'", c.Help)
	}
	if c.FieldID != FieldIdFp64Tflops {
		t.Errorf("Expected FieldID %d, got %d", FieldIdFp64Tflops, c.FieldID)
	}
}

func TestExtractAllCountersNotVerify_MixedDCGMAndAMPF(t *testing.T) {
	content := `DCGM_FI_DEV_GPU_UTIL,true,false,false,gpu_util,utilization,gauge,GPU utilization
AMPF_FI_FROF_FP64_TFPS_USED,false,true,false,fp64,fp64,gauge,FP64 TFLOPS
`
	f := createTempCSV(t, content)
	defer os.Remove(f)

	counters, err := ExtractAllCountersNotVerify(f, true)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if len(counters) != 2 {
		t.Fatalf("Expected 2 counters, got %d", len(counters))
	}

	// First counter should be DCGM
	if counters[0].FieldName != "DCGM_FI_DEV_GPU_UTIL" {
		t.Errorf("Expected first counter FieldName 'DCGM_FI_DEV_GPU_UTIL', got '%s'", counters[0].FieldName)
	}

	// Second counter should be AMPF
	if counters[1].FieldName != "AMPF_FI_FROF_FP64_TFPS_USED" {
		t.Errorf("Expected second counter FieldName 'AMPF_FI_FROF_FP64_TFPS_USED', got '%s'", counters[1].FieldName)
	}
}

func TestExtractAllCountersNotVerify_EmptyFile(t *testing.T) {
	f := createTempCSV(t, "")
	defer os.Remove(f)

	counters, err := ExtractAllCountersNotVerify(f, true)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if counters != nil {
		t.Errorf("Expected nil counters for empty file, got %v", counters)
	}
}

func TestExtractAllCountersNotVerify_MalformedCSV(t *testing.T) {
	// Wrong number of fields (only 5 instead of 8)
	content := "DCGM_FI_DEV_GPU_UTIL,true,false,gpu_util,utilization\n"
	f := createTempCSV(t, content)
	defer os.Remove(f)

	counters, err := ExtractAllCountersNotVerify(f, true)
	if err == nil {
		t.Fatal("Expected error for malformed CSV, got nil")
	}
	if counters != nil {
		t.Errorf("Expected nil counters on error, got %v", counters)
	}
}

func TestExtractAllCountersNotVerify_UnknownFieldPrefix(t *testing.T) {
	content := "UNKNOWN_FI_SOMETHING,true,false,false,metric,shortname,gauge,Help text\n"
	f := createTempCSV(t, content)
	defer os.Remove(f)

	counters, err := ExtractAllCountersNotVerify(f, true)
	if err == nil {
		t.Fatal("Expected error for unknown field prefix, got nil")
	}
	if counters != nil {
		t.Errorf("Expected nil counters on error, got %v", counters)
	}
}

func TestExtractAllCountersNotVerify_InvalidPromMetricType(t *testing.T) {
	content := "DCGM_FI_DEV_GPU_UTIL,true,false,false,gpu_util,utilization,invalid_type,GPU utilization\n"
	f := createTempCSV(t, content)
	defer os.Remove(f)

	counters, err := ExtractAllCountersNotVerify(f, true)
	if err == nil {
		t.Fatal("Expected error for invalid prom metric type, got nil")
	}
	if counters != nil {
		t.Errorf("Expected nil counters on error, got %v", counters)
	}
}

func TestExtractAllCountersNotVerify_DuplicateRemoved(t *testing.T) {
	// Same DCGM field twice; rmDuplicate=true should keep only 1
	content := `DCGM_FI_DEV_GPU_UTIL,true,false,false,gpu_util,utilization,gauge,GPU utilization
DCGM_FI_DEV_GPU_UTIL,true,false,false,gpu_util,utilization,gauge,GPU utilization
`
	f := createTempCSV(t, content)
	defer os.Remove(f)

	counters, err := ExtractAllCountersNotVerify(f, true)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if len(counters) != 1 {
		t.Errorf("Expected 1 counter after duplicate removal, got %d", len(counters))
	}
}

func TestExtractAllCountersNotVerify_DuplicateNotRemoved(t *testing.T) {
	// Same DCGM field twice; rmDuplicate=false should keep both
	content := `DCGM_FI_DEV_GPU_UTIL,true,false,false,gpu_util,utilization,gauge,GPU utilization
DCGM_FI_DEV_GPU_UTIL,true,false,false,gpu_util,utilization,gauge,GPU utilization
`
	f := createTempCSV(t, content)
	defer os.Remove(f)

	counters, err := ExtractAllCountersNotVerify(f, false)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if len(counters) != 2 {
		t.Errorf("Expected 2 counters without duplicate removal, got %d", len(counters))
	}
}
