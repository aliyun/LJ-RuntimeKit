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
	"bytes"
	"os"
	"testing"
)

func TestTraceBegin(t *testing.T) {
	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	TraceBegin("test operation")

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	buf.ReadFrom(r)
	output := buf.String()

	if len(output) == 0 {
		t.Error("TraceBegin should print to stdout")
	}
	// Check that the output contains "BEGIN" and "test operation"
	if !containsStr(output, "BEGIN") || !containsStr(output, "test operation") {
		t.Errorf("TraceBegin output should contain 'BEGIN' and 'test operation', got: %s", output)
	}
}

func TestTraceEnd(t *testing.T) {
	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	TraceEnd("test operation")

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	buf.ReadFrom(r)
	output := buf.String()

	if len(output) == 0 {
		t.Error("TraceEnd should print to stdout")
	}
	if !containsStr(output, "END") || !containsStr(output, "test operation") {
		t.Errorf("TraceEnd output should contain 'END' and 'test operation', got: %s", output)
	}
}

// Helper function (avoiding import of strings package)
func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && searchStr(s, substr)
}

func searchStr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
