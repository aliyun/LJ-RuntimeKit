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
	"os"
	"path/filepath"
	"time"

	"github.com/container-migration/worker/cmd/migration-worker/options"
	migrationworker "github.com/container-migration/worker/pkg/apis"
	migutil "github.com/container-migration/worker/pkg/util"
	migarchive "github.com/container-migration/worker/pkg/util/archive"
)

type LocalFS struct {
	Path           string
	UploadReader   *migutil.CountingReader
	DownloadReader *migutil.CountingReader
	StartTime      time.Time
}

func NewLocalFS(opts *options.Options, storageInfo *migrationworker.Storage) (StorageInterface, error) {
	localInstance := &LocalFS{}
	localInstance.UploadReader = migutil.NewCountingReader()
	localInstance.DownloadReader = migutil.NewCountingReader()
	if storageInfo == nil {
		return nil, fmt.Errorf("storage info not initialized")
	}
	if storageInfo.Local == nil {
		return nil, fmt.Errorf("local storage not initialized")
	}
	localInstance.Path = storageInfo.Local.Path
	if err := os.MkdirAll(localInstance.Path, 0755); err != nil {
		return nil, fmt.Errorf("error creating storage path %s: %v", localInstance.Path, err)
	}
	return localInstance, nil
}

func (n *LocalFS) Name() string {
	return "local"
}

func (n *LocalFS) Upload(reader io.Reader, dstTarFile string) (int64, error) {
	n.UploadReader.SetReader(reader)
	absTarPath := filepath.Join(n.Path, dstTarFile)
	if err := os.MkdirAll(filepath.Dir(absTarPath), 0755); err != nil {
		return 0, fmt.Errorf("error creating path %s: %v", absTarPath, err)
	}
	if err := migutil.CopyCheckPointFromRemote(n.UploadReader, absTarPath); err != nil {
		return 0, fmt.Errorf("error copying checkpoint from remote: %v", err)
	}
	info, err := os.Stat(absTarPath)
	if err != nil {
		return 0, fmt.Errorf("error stat file %s: %v", absTarPath, err)
	}
	return info.Size(), nil
}

// Restore concurrently extracts all shards under checkpointDir.
//
// A local path can know which shards exist by reading the dir directly - simpler and more accurate than probing.
func (n *LocalFS) Restore(ctx context.Context, checkpointDir string, dstDir string) (*RestoreInfo, error) {
	return n.restoreWith(ctx, checkpointDir, dstDir, "", nil)
}

// restoreWith is the parameterized version of Restore for combined reads
// (EFC-first + PVC-fallback): a non-empty tmpSuffix separates the temp dir from
// plain calls (<dstDir><tmpSuffix>-tmp); a non-nil abandonCheck is called by
// restoreIntoTemp before deleting the old dst and renaming, and returning true
// makes this path abandon the commit (only clean up its own tmp).
//
// Both are passed per call with no shared instance state - during multi-container
// concurrent Restores each goroutine's injection is isolated, no data race or
// cross interference from instance-field injection.
func (n *LocalFS) restoreWith(ctx context.Context, checkpointDir, dstDir string,
	tmpSuffix string, abandonCheck func() bool) (*RestoreInfo, error) {
	n.StartTime = time.Now()

	srcDir := filepath.Join(n.Path, checkpointDir)
	indices, err := DiscoverLocalParts(srcDir)
	if err != nil {
		return nil, err
	}

	if err := restoreIntoTemp(dstDir, tmpSuffix, abandonCheck, func(tmpDir string) error {
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

// openLocalPart returns a partOpener that opens a shard file directly from the dir, shared by localfs and nfs.
//
// counter sums the bytes read by the N shard workers; it is the only data
// source for the extras bytesTransferred / transfer rate - during the sharding
// refactor this layer once returned bare *os.File, so the counter was never
// wired up and all transfer metrics were constantly 0.
func openLocalPart(srcDir string, counter *migutil.CountingReader) partOpener {
	return func(ctx context.Context, idx int) (io.Reader, func(), error) {
		p := filepath.Join(srcDir, PartName(idx))
		f, err := os.Open(p)
		if err != nil {
			return nil, nil, fmt.Errorf("open checkpoint part %s: %w", p, err)
		}
		return counter.Wrap(f), func() { f.Close() }, nil
	}
}

func (n *LocalFS) CleanUpload(path string) error {
	checkpointDir := filepath.Join(n.Path, path)
	if _, err := os.Stat(checkpointDir); os.IsNotExist(err) {
		return nil
	}
	return os.RemoveAll(checkpointDir)
}

func (n *LocalFS) GetUploadInfo() (*UploadInfo, error) {
	return &UploadInfo{
		UploadedSize: n.UploadReader.Size(),
	}, nil
}

func (n *LocalFS) GetDownloadInfo() (*RestoreInfo, error) {
	return &RestoreInfo{
		FileSize: n.DownloadReader.Size(),
	}, nil
}

func init() {
	RegisterStoragePlugin("local", NewLocalFS)
}
