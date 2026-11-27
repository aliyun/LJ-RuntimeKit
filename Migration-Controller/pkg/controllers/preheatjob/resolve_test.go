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

package preheatjob

import (
	"strings"
	"testing"

	"github.com/container-migration/controller/api/v1beta1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestBuildRemoteURL pins the join result consistent with the worker side.
// The worker download fetches objects by
// https://{bucket}.{ossEndpoint}/{Rel("/", Join(basePath, path))}; one slash
// more or less here would make dfdaemon compute a different task_id and miss
// the preheat cache.
func TestBuildRemoteURL(t *testing.T) {
	const objectPath = "6c1ab104-f99a-4582-bd22-33efde1b51b7/1756276800000000000/demo-gpu-123321/checkpoint.tar"

	tests := []struct {
		name       string
		mountPoint string
		endpoint   string
		want       string
	}{
		{
			name:       "trailing slash in mount point",
			mountPoint: "oss://example-bucket/data/migration-worker/",
			endpoint:   "oss-cn-hangzhou.aliyuncs.com",
			want:       "https://example-bucket.oss-cn-hangzhou.aliyuncs.com/data/migration-worker/" + objectPath,
		},
		{
			name:       "no trailing slash in mount point",
			mountPoint: "oss://example-bucket/data/migration-worker",
			endpoint:   "oss-cn-hangzhou.aliyuncs.com",
			want:       "https://example-bucket.oss-cn-hangzhou.aliyuncs.com/data/migration-worker/" + objectPath,
		},
		{
			name:       "bucket root",
			mountPoint: "oss://my-bucket",
			endpoint:   "oss-cn-hangzhou.aliyuncs.com",
			want:       "https://my-bucket.oss-cn-hangzhou.aliyuncs.com/" + objectPath,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildRemoteURL(tt.mountPoint, tt.endpoint, objectPath)
			if err != nil {
				t.Fatalf("buildRemoteURL: %v", err)
			}
			if got != tt.want {
				t.Errorf("buildRemoteURL = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBuildRemoteURLRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name       string
		mountPoint string
		endpoint   string
	}{
		{name: "empty mount point", mountPoint: "", endpoint: "oss-cn-hangzhou.aliyuncs.com"},
		{name: "empty endpoint", mountPoint: "oss://bucket/path", endpoint: ""},
		{name: "wrong scheme", mountPoint: "https://bucket/path", endpoint: "oss-cn-hangzhou.aliyuncs.com"},
		{name: "no bucket", mountPoint: "oss:///path", endpoint: "oss-cn-hangzhou.aliyuncs.com"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := buildRemoteURL(tt.mountPoint, tt.endpoint, "uid/v/c/checkpoint.tar"); err == nil {
				t.Error("expected an error, got nil")
			}
		})
	}
}

// TestCheckpointObjectPath covers the container selection rule: checkpoints
// dump only a single container, so it can resolve only when there is exactly
// one container with a data path; everything else errors out (set Failed by
// the caller).
func TestCheckpointObjectPath(t *testing.T) {
	cr := func(statuses ...v1beta1.ContainerMigrationStatus) *v1beta1.ContainerStateMigrationJobRequest {
		return &v1beta1.ContainerStateMigrationJobRequest{
			ObjectMeta: metav1.ObjectMeta{Name: "cp", Namespace: "default"},
			Status: v1beta1.ContainerStateMigrationJobRequestStatus{
				CheckpointStatus: &v1beta1.JobCheckpointStatus{
					ContainerMigrationStatuses: statuses,
				},
			},
		}
	}
	withPath := func(name, path string) v1beta1.ContainerMigrationStatus {
		return v1beta1.ContainerMigrationStatus{
			Name:                 name,
			CheckpointAttributes: &v1beta1.CheckpointAttributes{Path: path},
		}
	}

	t.Run("single container", func(t *testing.T) {
		got, err := checkpointObjectPath(cr(withPath("only", "uid/v/only/checkpoint.tar")))
		if err != nil {
			t.Fatalf("checkpointObjectPath: %v", err)
		}
		if got != "uid/v/only/checkpoint.tar" {
			t.Errorf("got %q", got)
		}
	})

	t.Run("multi container fails", func(t *testing.T) {
		_, err := checkpointObjectPath(
			cr(withPath("a", "uid/v/a/checkpoint.tar"), withPath("b", "uid/v/b/checkpoint.tar")))
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
		// The error message must list the container names, otherwise the user cannot tell which containers conflicted.
		if !strings.Contains(err.Error(), "a, b") {
			t.Errorf("error should list the containers, got: %v", err)
		}
	})

	t.Run("checkpoint not finished yet fails", func(t *testing.T) {
		if _, err := checkpointObjectPath(cr(v1beta1.ContainerMigrationStatus{Name: "a"})); err == nil {
			t.Fatal("expected an error, got nil")
		}
	})

	t.Run("no checkpointStatus fails", func(t *testing.T) {
		bare := &v1beta1.ContainerStateMigrationJobRequest{
			ObjectMeta: metav1.ObjectMeta{Name: "cp", Namespace: "default"},
		}
		if _, err := checkpointObjectPath(bare); err == nil {
			t.Fatal("expected an error, got nil")
		}
	})
}

// TestOSSCoordinates confirms only the oss / dragonfly backends can resolve an origin address.
func TestOSSCoordinates(t *testing.T) {
	t.Run("oss backend", func(t *testing.T) {
		cr := &v1beta1.ContainerStateMigrationJobRequest{
			Spec: v1beta1.ContainerStateMigrationJobRequestSpec{
				StorageType: storageTypeOSS,
				Storage: &v1beta1.JobStorage{
					OSS: &v1beta1.JobOSS{
						MountPoint: "oss://bucket/base/",
						Endpoint:   "oss-cn-hangzhou.aliyuncs.com",
						Options:    map[string]string{OptionAccessKeyID: "ak", OptionAccessKeySecret: "sk"},
					},
				},
			},
		}
		mountPoint, endpoint, options, err := ossCoordinates(cr)
		if err != nil {
			t.Fatalf("ossCoordinates: %v", err)
		}
		if mountPoint != "oss://bucket/base/" || endpoint != "oss-cn-hangzhou.aliyuncs.com" ||
			options[OptionAccessKeyID] != "ak" || options[OptionAccessKeySecret] != "sk" {
			t.Errorf("unexpected result: %q %q %v", mountPoint, endpoint, options)
		}
	})

	t.Run("dragonfly backend", func(t *testing.T) {
		cr := &v1beta1.ContainerStateMigrationJobRequest{
			Spec: v1beta1.ContainerStateMigrationJobRequestSpec{
				StorageType: storageTypeDragonfly,
				Storage: &v1beta1.JobStorage{
					Dragonfly: &v1beta1.JobDragonfly{
						OSSMountPoint: "oss://bucket/base/",
						OSSEndpoint:   "oss-cn-hangzhou.aliyuncs.com",
						Options:       map[string]string{OptionAccessKeyID: "ak"},
					},
				},
			},
		}
		mountPoint, endpoint, options, err := ossCoordinates(cr)
		if err != nil {
			t.Fatalf("ossCoordinates: %v", err)
		}
		if mountPoint != "oss://bucket/base/" || endpoint != "oss-cn-hangzhou.aliyuncs.com" ||
			options[OptionAccessKeyID] != "ak" {
			t.Errorf("unexpected result: %q %q %v", mountPoint, endpoint, options)
		}
	})

	t.Run("local backend is rejected", func(t *testing.T) {
		cr := &v1beta1.ContainerStateMigrationJobRequest{
			Spec: v1beta1.ContainerStateMigrationJobRequestSpec{
				StorageType: "local",
				Storage:     &v1beta1.JobStorage{Local: &v1beta1.JobLocal{Path: "/data"}},
			},
		}
		if _, _, _, err := ossCoordinates(cr); err == nil {
			t.Fatal("expected an error, got nil")
		}
	})
}
