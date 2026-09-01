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

package gpu

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStackTrace_ReturnsNonEmptyString(t *testing.T) {
	result := stackTrace(false)
	assert.NotEmpty(t, result)
	assert.Contains(t, result, "goroutine")
}

func TestStackTrace_AllGoroutines(t *testing.T) {
	result := stackTrace(true)
	assert.NotEmpty(t, result)
	// Should contain multiple goroutines when all=true
	assert.Contains(t, result, "goroutine")
}

func TestStackTrace_ContainsCurrentFunction(t *testing.T) {
	result := stackTrace(false)
	// The stack trace should contain the current test function
	assert.Contains(t, result, "TestStackTrace_ContainsCurrentFunction")
}

func TestCoreDump_CreatesFile(t *testing.T) {
	// Create a temporary file path
	tmpFile := "/tmp/test_coredump_" + t.Name() + ".txt"

	// Clean up after test
	defer os.Remove(tmpFile)

	// Call coreDump
	coreDump(tmpFile)

	// Verify file was created
	_, err := os.Stat(tmpFile)
	assert.NoError(t, err)

	// Verify file content
	content, err := os.ReadFile(tmpFile)
	assert.NoError(t, err)
	assert.NotEmpty(t, content)
	assert.True(t, strings.Contains(string(content), "goroutine"))
}

func TestCoreDump_FileContainsStackTrace(t *testing.T) {
	tmpFile := "/tmp/test_coredump_stacktrace_" + t.Name() + ".txt"
	defer os.Remove(tmpFile)

	coreDump(tmpFile)

	content, err := os.ReadFile(tmpFile)
	assert.NoError(t, err)

	contentStr := string(content)
	// Verify it looks like a Go stack trace
	assert.Contains(t, contentStr, "goroutine")
	assert.Contains(t, contentStr, "[running]:")
}
