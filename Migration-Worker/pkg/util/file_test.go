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

package util

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCountFiles(t *testing.T) {
	testDir, err := os.MkdirTemp("", "testdir")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %s", err)
	}
	defer os.RemoveAll(testDir)

	testFiles := []string{
		filepath.Join(testDir, "file1.txt"),
		filepath.Join(testDir, "file2.txt"),
		filepath.Join(testDir, "subdir", "file3.txt"),
	}
	for _, file := range testFiles {
		dir := filepath.Dir(file)
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("Failed to create directory: %s", err)
		}
		if err := os.WriteFile(file, []byte("test"), 0666); err != nil {
			t.Fatalf("Failed to create test file: %s", err)
		}
	}

	count, size, err := GetDirData(testDir)
	assert.NoError(t, err)
	assert.Equal(t, count, int64(5))
	assert.Equal(t, size, int64(12))
}

func TestWaitUntilTimeout_ExecFuncCompletesBeforeTimeout_NoError(t *testing.T) {
	var execFunc ExecFunc = func() error {
		time.Sleep(100 * time.Millisecond) // simulate execution time
		return nil
	}
	var timeoutFunc TimeoutFunc = func() error {
		return errors.New("timeout error")
	}

	err := WaitUntilTimeout(500*time.Millisecond, execFunc, timeoutFunc)
	if err != nil {
		t.Errorf("Expected no error, but got: %v", err)
	}
}

func TestWaitUntilTimeout_ExecFuncCompletesBeforeTimeout_WithError(t *testing.T) {
	var execFunc ExecFunc = func() error {
		time.Sleep(100 * time.Millisecond) // simulate execution time
		return errors.New("execution error")
	}
	var timeoutFunc TimeoutFunc = func() error {
		return errors.New("timeout error")
	}

	err := WaitUntilTimeout(500*time.Millisecond, execFunc, timeoutFunc)
	if err == nil {
		t.Errorf("Expected an error, but got none")
	} else if err.Error() != "execution error" {
		t.Errorf("Expected 'execution error', but got: %v", err)
	}
}

func TestWaitUntilTimeout_ExecFuncTimesOut(t *testing.T) {
	var execFunc ExecFunc = func() error {
		time.Sleep(1 * time.Second) // simulate execution time exceeding the timeout
		return nil
	}
	var timeoutFunc TimeoutFunc = func() error {
		return errors.New("timeout error")
	}

	err := WaitUntilTimeout(500*time.Millisecond, execFunc, timeoutFunc)
	if err == nil {
		t.Errorf("Expected a timeout error, but got none")
	} else if err.Error() != "timeout error" {
		t.Errorf("Expected 'timeout error', but got: %v", err)
	}
}

func TestWaitUntilTimeout_ExecFuncTimesOut_NoTimeoutError(t *testing.T) {
	var execFunc ExecFunc = func() error {
		time.Sleep(1 * time.Second)
		return nil
	}
	var timeoutFunc TimeoutFunc = func() error {
		return nil
	}

	err := WaitUntilTimeout(500*time.Millisecond, execFunc, timeoutFunc)
	if err != nil {
		t.Errorf("Expected no error, but got: %v", err)
	}
}

func TestCalculateMD5(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected string
	}{
		{
			name:     "empty string",
			input:    []byte(""),
			expected: "d41d8cd98f00b204e9800998ecf8427e",
		},
		{
			name:     "hello world",
			input:    []byte("hello world"),
			expected: "5eb63bbbe01eeed093cb22bb8f5acdc3",
		},
		{
			name:     "Hello World (capitalized)",
			input:    []byte("Hello World"),
			expected: "b10a8db164e0754105b7a99be72e3fe5",
		},
		{
			name:     "numbers",
			input:    []byte("12345"),
			expected: "827ccb0eea8a706c4c34a16891f84e7b",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CalculateMD5(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestCheckPathExists(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name       string
		setup      func() string
		wantResult bool
	}{
		{
			name: "path does not exist",
			setup: func() string {
				return filepath.Join(tmpDir, "not-exist")
			},
			wantResult: false,
		},
		{
			name: "path exists and is a directory",
			setup: func() string {
				dirPath := filepath.Join(tmpDir, "exist-dir")
				err := os.Mkdir(dirPath, 0755)
				require.NoError(t, err)
				return dirPath
			},
			wantResult: true,
		},
		{
			name: "path exists but is a file",
			setup: func() string {
				filePath := filepath.Join(tmpDir, "exist-file.txt")
				err := os.WriteFile(filePath, []byte("dummy"), 0644)
				require.NoError(t, err)
				return filePath
			},
			wantResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testPath := tt.setup()

			result := CheckPathExists(testPath)
			require.Equal(t, tt.wantResult, result)
		})
	}
}
