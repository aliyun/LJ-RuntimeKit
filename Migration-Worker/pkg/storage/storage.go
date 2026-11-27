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
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/container-migration/worker/cmd/migration-worker/options"
	migrationworker "github.com/container-migration/worker/pkg/apis"
)

type StorageInterface interface {
	Name() string
	Upload(reader io.Reader, dstTarFile string) (int64, error)
	Restore(ctx context.Context, tarFile string, dstDir string) (*RestoreInfo, error)
	CleanUpload(path string) error
	GetUploadInfo() (*UploadInfo, error)
	GetDownloadInfo() (*RestoreInfo, error)
}

type NewStorage func(opts *options.Options, storageInfo *migrationworker.Storage) (StorageInterface, error)

const efcMountRoot = "/mnt/efc"

// pvcMountPoint is the fixed mount point of PVC direct-mount storage: the
// controller mounts the task's PVC at /mnt/pv when creating the task-level
// worker, and the worker side converts storageType=pvc into the local plugin
// and reads/writes this path directly, bypassing remote storage such as
// OSS/EFC.
const pvcMountPoint = "/mnt/pv"

var StorageRegisterFactory map[string]NewStorage

func RegisterStoragePlugin(name string, newFunc NewStorage) {
	if StorageRegisterFactory == nil {
		StorageRegisterFactory = make(map[string]NewStorage, 0)
	}
	if _, ok := StorageRegisterFactory[name]; ok {
		panic("Duplicate Plugins")
	}
	StorageRegisterFactory[name] = newFunc
}

func GetStoragePlugin(name string) (NewStorage, error) {
	newFunc, ok := StorageRegisterFactory[name]
	if !ok {
		return nil, fmt.Errorf("invalid storage plugin name %v", name)
	}
	return newFunc, nil
}

func efcStoragePath(bucketName string) (string, error) {
	if bucketName == "" {
		return "", fmt.Errorf("bucket name must not be empty")
	}
	if filepath.IsAbs(bucketName) {
		return "", fmt.Errorf("bucket name must not be an absolute path")
	}
	if strings.ContainsRune(bucketName, '\\') {
		return "", fmt.Errorf("bucket name must not contain backslashes")
	}
	// Multi-level relative paths allowed (e.g. chanke/migration-worker/, trailing
	// slashes normalized too): checkpoint data carries a subdir prefix inside the
	// bucket, and bucketName must be able to express that prefix. Clean first then
	// validate; path traversal is guarded by the Rel escape check below.
	name := filepath.Clean(bucketName)
	if strings.HasPrefix(name, ".") {
		return "", fmt.Errorf("bucket name must not be a dot directory")
	}

	root := filepath.Clean(efcMountRoot)
	path := filepath.Join(root, name)
	relativePath, err := filepath.Rel(root, path)
	if err != nil {
		return "", fmt.Errorf("resolve bucket path: %w", err)
	}
	if filepath.IsAbs(relativePath) || relativePath == ".." || strings.HasPrefix(relativePath, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("bucket path escapes mount root")
	}
	return path, nil
}

// sanitizeEFCRelPath normalizes a cluster-side path field into a relative path
// safe to append after efcMountRoot: trim leading/trailing slashes +
// filepath.Clean, then confirm the result does not escape the mount root.
//
// The division of labor with efcStoragePath is the opposite empty-value
// semantics: efcStoragePath validates the CR's bucketName (empty is invalid and
// absolute paths and hidden dirs are explicitly rejected); this function
// validates fields like PV.path and PreheatJob status.efcPath that are
// "habitually written as absolute paths, empty means mounted at the bucket
// root", so empty and "." are both normalized to the empty prefix and treated
// as valid, leaving the caller to decide how to use the empty prefix.
//
// The criterion aligns with preheatjob.validRelPath on the controller side
// (both sides validate the same batch of fields: PV.path and pvcPath /
// status.efcPath; inconsistent rules would let one side pass and the other
// degrade on the same value): only reject ".." traversal and backslashes, do
// **not reject hidden dir names** - checkpoint data may be moved to a user-made
// dir, and names like ".hidden/foo" are legitimate. A cleaned relative path
// that really escapes necessarily starts with "../"; filepath.Rel is the final
// guard.
func sanitizeEFCRelPath(raw string) (string, error) {
	trimmed := strings.Trim(raw, "/")
	if strings.ContainsRune(trimmed, '\\') {
		return "", fmt.Errorf("path must not contain backslashes")
	}
	name := filepath.Clean(trimmed)
	if name == "" || name == "." {
		return "", nil
	}
	if name == ".." || strings.HasPrefix(name, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path escapes mount root %s", efcMountRoot)
	}

	root := filepath.Clean(efcMountRoot)
	relativePath, err := filepath.Rel(root, filepath.Join(root, name))
	if err != nil {
		return "", fmt.Errorf("resolve path under %s: %w", efcMountRoot, err)
	}
	if filepath.IsAbs(relativePath) || relativePath == ".." || strings.HasPrefix(relativePath, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path escapes mount root %s", efcMountRoot)
	}
	return name, nil
}

func NewStorageInterface(opts *options.Options, storageType string, storageInfo *migrationworker.Storage) (StorageInterface, error) {
	if storageType == "efc" {
		if storageInfo == nil {
			return nil, fmt.Errorf("efc storage info not initialized")
		}
		if storageInfo.Efc == nil {
			return nil, fmt.Errorf("efc storage not initialized")
		}

		path, err := efcStoragePath(storageInfo.Efc.BucketName)
		if err != nil {
			return nil, fmt.Errorf("invalid efc bucket name %q: %w", storageInfo.Efc.BucketName, err)
		}
		storageType = "local"
		storageInfo.Local = &migrationworker.LocalStorage{Path: path}
	}
	if storageType == "pvc" {
		if storageInfo == nil {
			return nil, fmt.Errorf("pvc storage info not initialized")
		}
		if storageInfo.Pvc == nil {
			return nil, fmt.Errorf("pvc storage not initialized")
		}
		// The preheat backend is decided by the worker flag --pvc-preheat-backend
		// (global, no per-CR choice; invalid values are rejected at main startup):
		//   - dragonfly (default): derive the ClusterIP of the long-running Service
		//     from (pvcNamespace, pvcName, pvcPath) to rebuild the preheat URL, P2P
		//     pull via dfdaemon + per-part direct PVC read fallback, and co-host the
		//     origin (self-registered EndpointSlice);
		//   - efc: the worker derives the EFC relative path from the PVC/PV; EFC first
		//     + PVC fallback.
		switch opts.PvcPreheatBackend {
		case options.PvcPreheatBackendDragonfly:
			return newPVCWithDragonflyPreheat(opts, storageInfo)
		default:
			return newPVCWithDerivedEFC(opts, storageInfo)
		}
	}
	newStorage, err := GetStoragePlugin(storageType)
	if err != nil {
		return nil, fmt.Errorf("failed to get storage plugin: %v", err)
	}
	return newStorage(opts, storageInfo)
}
