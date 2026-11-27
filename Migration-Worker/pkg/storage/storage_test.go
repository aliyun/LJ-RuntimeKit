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

package storage

import (
	"strings"
	"testing"

	"github.com/container-migration/worker/cmd/migration-worker/options"
	migrationworker "github.com/container-migration/worker/pkg/apis"
)

// TestInferPartCountFromTaskID locks the empty-value semantics of http-source
// shard count inference: an empty TaskID must error - strings.Split("", ",")
// returns [""] (length 1), and without an explicit guard an unfinished preheat
// would be silently masked as one shard.
func TestInferPartCountFromTaskID(t *testing.T) {
	tests := []struct {
		name    string
		taskID  string
		want    int
		wantErr bool
	}{
		{name: "empty", taskID: "", wantErr: true},
		{name: "whitespace only", taskID: "   ", wantErr: true},
		{name: "single part", taskID: "2316", want: 1},
		{name: "multiple parts", taskID: "2316,2317,2318", want: 3},
		{name: "padded taskID", taskID: " 2316,2317 ", want: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := inferPartCountFromTaskID(tt.taskID)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("inferPartCountFromTaskID(%q) = %d, want error", tt.taskID, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("inferPartCountFromTaskID(%q): %v", tt.taskID, err)
			}
			if got != tt.want {
				t.Fatalf("inferPartCountFromTaskID(%q) = %d, want %d", tt.taskID, got, tt.want)
			}
		})
	}
}

func TestEFCStoragePath(t *testing.T) {
	tests := []struct {
		name       string
		bucketName string
		wantPath   string
		wantErr    bool
	}{
		{name: "normal bucket", bucketName: "checkpoint-bucket", wantPath: "/mnt/efc/checkpoint-bucket"},
		{name: "multi-level path", bucketName: "demo/migration-worker", wantPath: "/mnt/efc/demo/migration-worker"},
		{name: "multi-level path with trailing slash", bucketName: "demo/migration-worker/", wantPath: "/mnt/efc/demo/migration-worker"},
		{name: "empty bucket", bucketName: "", wantErr: true},
		{name: "current directory", bucketName: ".", wantErr: true},
		{name: "parent directory", bucketName: "..", wantErr: true},
		{name: "hidden directory", bucketName: ".hidden", wantErr: true},
		{name: "absolute path", bucketName: "/tmp/checkpoint", wantErr: true},
		{name: "backslash", bucketName: `bucket\child`, wantErr: true},
		{name: "traversal path", bucketName: "../checkpoint", wantErr: true},
		{name: "nested traversal", bucketName: "a/../../checkpoint", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := efcStoragePath(tt.bucketName)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("efcStoragePath(%q) returned no error", tt.bucketName)
				}
				return
			}
			if err != nil {
				t.Fatalf("efcStoragePath(%q) returned error: %v", tt.bucketName, err)
			}
			if got != tt.wantPath {
				t.Fatalf("efcStoragePath(%q) = %q, want %q", tt.bucketName, got, tt.wantPath)
			}
		})
	}
}

// TestSanitizeEFCRelPath pins the normalization and escape criteria of
// cluster-side path fields like PV.path / status.efcPath (aligned with
// preheatjob.validRelPath on the controller side): legal values are kept after
// trimming leading/trailing slashes; empty/root normalize to the empty prefix
// (legal, meaning mounted at the bucket root); hidden dir names and arbitrary
// chars of user-made dirs are legal; only ".." traversal and backslashes error.
func TestSanitizeEFCRelPath(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    string
		wantErr bool
	}{
		{name: "absolute multi-level", raw: "/demo/migration-worker", want: "demo/migration-worker"},
		{name: "relative multi-level", raw: "demo/migration-worker", want: "demo/migration-worker"},
		{name: "trailing slash", raw: "/data/", want: "data"},
		{name: "dot segment inside", raw: "/a/./b", want: "a/b"},
		{name: "empty", raw: "", want: ""},
		{name: "root slash", raw: "/", want: ""},
		{name: "current directory", raw: ".", want: ""},
		// Hidden dir names are legal: checkpoint data may be moved to a user-made
		// dir; as long as no traversal relies on it, it must pass (consistent with
		// validRelPath on the controller side).
		{name: "hidden directory", raw: "/.hidden", want: ".hidden"},
		{name: "hidden directory with subpath", raw: "/.hidden/checkpoint", want: ".hidden/checkpoint"},
		{name: "dot inside longer segment", raw: "/a.b/c.d", want: "a.b/c.d"},
		{name: "user dir with spaces and parens", raw: "/my dir/backup (2026)", want: "my dir/backup (2026)"},
		{name: "shell metacharacters", raw: "/a;b$c/`d`|e", want: "a;b$c/`d`|e"},
		{name: "non-ascii segments", raw: "/café/naïve", want: "café/naïve"},
		{name: "parent directory", raw: "..", wantErr: true},
		{name: "absolute escape", raw: "/../../etc", wantErr: true},
		{name: "escape to root", raw: "/demo/../..", wantErr: true},
		{name: "nested escape", raw: "a/../../etc/passwd", wantErr: true},
		{name: "hidden directory cannot be used to escape", raw: "/.hidden/../../etc", wantErr: true},
		{name: "backslash", raw: `a\b`, wantErr: true},
		{name: "windows style traversal", raw: `..\..\etc`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := sanitizeEFCRelPath(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("sanitizeEFCRelPath(%q) = %q, want an error", tt.raw, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("sanitizeEFCRelPath(%q) returned error: %v", tt.raw, err)
			}
			if got != tt.want {
				t.Fatalf("sanitizeEFCRelPath(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}

func TestNewStorageInterfaceEFCValidation(t *testing.T) {
	tests := []struct {
		name        string
		storageInfo *migrationworker.Storage
		wantError   string
	}{
		{name: "nil storage", wantError: "efc storage info not initialized"},
		{name: "nil EFC", storageInfo: &migrationworker.Storage{}, wantError: "efc storage not initialized"},
		{
			name:        "invalid bucket",
			storageInfo: &migrationworker.Storage{Efc: &migrationworker.EFC{BucketName: ".."}},
			wantError:   "invalid efc bucket name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewStorageInterface(nil, "efc", tt.storageInfo)
			if err == nil {
				t.Fatal("NewStorageInterface returned no error")
			}
			if !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("NewStorageInterface error = %q, want it to contain %q", err, tt.wantError)
			}
		})
	}
}

func TestNewStorageInterfaceEFCUsesLocalMountPath(t *testing.T) {
	originalLocalFactory := StorageRegisterFactory["local"]
	StorageRegisterFactory["local"] = func(_ *options.Options, storageInfo *migrationworker.Storage) (StorageInterface, error) {
		return &LocalFS{Path: storageInfo.Local.Path}, nil
	}
	t.Cleanup(func() {
		StorageRegisterFactory["local"] = originalLocalFactory
	})

	storageInfo := &migrationworker.Storage{Efc: &migrationworker.EFC{BucketName: "checkpoint-bucket"}}
	instance, err := NewStorageInterface(nil, "efc", storageInfo)
	if err != nil {
		t.Fatalf("NewStorageInterface returned error: %v", err)
	}
	if storageInfo.Local == nil {
		t.Fatal("NewStorageInterface did not initialize local storage")
	}
	if storageInfo.Local.Path != "/mnt/efc/checkpoint-bucket" {
		t.Fatalf("local storage path = %q, want %q", storageInfo.Local.Path, "/mnt/efc/checkpoint-bucket")
	}
	local, ok := instance.(*LocalFS)
	if !ok {
		t.Fatalf("NewStorageInterface returned %T, want *LocalFS", instance)
	}
	if local.Path != storageInfo.Local.Path {
		t.Fatalf("local instance path = %q, want %q", local.Path, storageInfo.Local.Path)
	}
}
