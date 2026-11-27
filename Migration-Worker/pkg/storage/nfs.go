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
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/container-migration/worker/cmd/migration-worker/options"
	migrationworker "github.com/container-migration/worker/pkg/apis"
	migutil "github.com/container-migration/worker/pkg/util"
	migarchive "github.com/container-migration/worker/pkg/util/archive"

	"k8s.io/klog/v2"
	mount "k8s.io/mount-utils"
)

type Nfs struct {
	Server         string
	Path           string
	MountPoint     string
	Mounter        mount.Interface
	Options        map[string]string
	UploadReader   *migutil.CountingReader
	DownloadReader *migutil.CountingReader
	StartTime      time.Time
}

func NewNfs(opts *options.Options, storageInfo *migrationworker.Storage) (StorageInterface, error) {
	nfsInstance := &Nfs{}
	nfsInstance.UploadReader = migutil.NewCountingReader()
	nfsInstance.DownloadReader = migutil.NewCountingReader()
	nfsInstance.Options = make(map[string]string)
	if storageInfo.Nfs == nil {
		return nil, fmt.Errorf("NFS storage info is nil")
	}
	nfsInstance.Path = storageInfo.Nfs.Path
	nfsInstance.Server = storageInfo.Nfs.Server
	for k, v := range storageInfo.Nfs.Options {
		nfsInstance.Options[k] = v
	}
	mounter := mount.New("")
	if runtime.GOOS == "linux" {
		mounter = mounter.(mount.MounterForceUnmounter)
	}
	nfsInstance.Mounter = mounter
	tmpDir, err := ioutil.TempDir("", "migration-worker-")
	if err != nil {
		return nil, fmt.Errorf("failed to create tmp dir: %v", err)
	}
	nfsInstance.MountPoint = tmpDir
	return nfsInstance, nil
}

func (n *Nfs) Name() string {
	return "nfs"
}

func (n *Nfs) Upload(reader io.Reader, dstTarFile string) (int64, error) {
	if err := n.mount(); err != nil {
		return 0, err
	}
	defer func() {
		if err := n.umount(); err != nil {
			klog.Errorf("Failed to umount %s: %v", n.MountPoint, err)
		}
	}()
	n.UploadReader.SetReader(reader)
	absTarPath := filepath.Join(n.MountPoint, dstTarFile)
	if err := os.MkdirAll(filepath.Dir(absTarPath), 0755); err != nil {
		return 0, fmt.Errorf("error creating path %s: %v", absTarPath, err)
	}
	if err := migutil.CopyCheckPointFromRemote(n.UploadReader, absTarPath); err != nil {
		return 0, fmt.Errorf("error copying checkpoint from remote: %v", err)
	}
	info, err := os.Stat(absTarPath)
	if err != nil {
		return 0, fmt.Errorf("error stat %s: %v", absTarPath, err)
	}
	return info.Size(), nil
}

// Restore concurrently extracts all shards under checkpointDir on the mounted NFS.
func (n *Nfs) Restore(ctx context.Context, checkpointDir string, dstDir string) (*RestoreInfo, error) {
	n.StartTime = time.Now()
	if err := n.mount(); err != nil {
		return nil, err
	}
	defer func() {
		if err := n.umount(); err != nil {
			klog.Errorf("Failed to umount %s: %v", n.MountPoint, err)
		}
	}()

	srcDir := filepath.Join(n.MountPoint, checkpointDir)
	indices, err := DiscoverLocalParts(srcDir)
	if err != nil {
		return nil, err
	}

	if err := restoreIntoTemp(dstDir, "", nil, func(tmpDir string) error {
		return unpackPartsConcurrently(ctx, indices, tmpDir, openLocalPart(srcDir, n.DownloadReader),
			&migarchive.TarOptions{Compression: migarchive.Uncompressed})
	}); err != nil {
		return nil, fmt.Errorf("error unTar checkpoint: %w", err)
	}

	fileCount, err := restoreInfoOf(dstDir)
	if err != nil {
		return nil, err
	}
	return &RestoreInfo{
		FileCount: fileCount,
		FileSize:  n.DownloadReader.Size(),
	}, nil
}

func (n *Nfs) mount() error {
	mountOptions := []string{}
	for k, v := range n.Options {
		if len(v) == 0 {
			mountOptions = append(mountOptions, k)
		} else {
			mountOptions = append(mountOptions, fmt.Sprintf("%s=%s", k, v))
		}
	}
	execFunc := func() error {
		return n.Mounter.Mount(fmt.Sprintf("%s:%s", n.Server, n.Path), n.MountPoint, "nfs", mountOptions)
	}
	timeoutFunc := func() error { return fmt.Errorf("time out") }
	if err := migutil.WaitUntilTimeout(90*time.Second, execFunc, timeoutFunc); err != nil {
		if os.IsPermission(err) {
			return fmt.Errorf("permission denied")
		}
		if strings.Contains(err.Error(), "invalid argument") {
			return fmt.Errorf("invalid argument")
		}
		return fmt.Errorf("mount %s to %s failed: %v", n.Path, n.MountPoint, err)
	}
	return nil
}

func (n *Nfs) umount() error {
	var err error
	extensiveMountPointCheck := true
	forceUnmounter, ok := n.Mounter.(mount.MounterForceUnmounter)
	if ok {
		err = mount.CleanupMountWithForce(n.MountPoint, forceUnmounter, extensiveMountPointCheck, 30*time.Second)
	} else {
		err = mount.CleanupMountPoint(n.MountPoint, n.Mounter, extensiveMountPointCheck)
	}
	if err != nil {
		return fmt.Errorf("error cleaning up mount point %s: %v", n.MountPoint, err)
	}
	return nil
}

func (n *Nfs) CleanUpload(path string) error {
	if err := n.mount(); err != nil {
		return err
	}
	defer func() {
		if err := n.umount(); err != nil {
			klog.Errorf("Failed to umount %s: %v", n.MountPoint, err)
		}
	}()
	absPath := filepath.Join(n.MountPoint, path)
	if _, err := os.Stat(absPath); os.IsNotExist(err) {
		return nil
	}
	return os.RemoveAll(absPath)
}

func (n *Nfs) GetUploadInfo() (*UploadInfo, error) {
	return &UploadInfo{
		UploadedSize: n.UploadReader.Size(),
	}, nil
}

func (n *Nfs) GetDownloadInfo() (*RestoreInfo, error) {
	return &RestoreInfo{
		FileSize: n.DownloadReader.Size(),
	}, nil
}

func init() {
	RegisterStoragePlugin("nfs", NewNfs)
}
