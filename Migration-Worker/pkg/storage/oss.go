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
	"time"

	"github.com/container-migration/worker/cmd/migration-worker/options"
	migrationworker "github.com/container-migration/worker/pkg/apis"
	"github.com/container-migration/worker/pkg/storage/oss-sdk"
	migutil "github.com/container-migration/worker/pkg/util"
	migarchive "github.com/container-migration/worker/pkg/util/archive"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
)

type Oss struct {
	BucketName     string
	BasePath       string
	Uri            string
	ChunkSize      int64
	Parallelism    int
	OssClient      *oss_sdk.Client
	UploadReader   *migutil.CountingReader
	DownloadReader *migutil.CountingReader
	StartTime      time.Time

	// endpoint / accessKeyID / accessKeySecret are kept only to sign a presigned
	// URL ourselves, probing which shards exist under the checkpoint dir with
	// Range. Probing instead of ListObjects: probing uses the existing
	// oss:GetObject permission, no RAM policy change needed.
	endpoint        string
	accessKeyID     string
	accessKeySecret string
}

func NewOss(opt *options.Options, storageInfo *migrationworker.Storage) (StorageInterface, error) {
	ossInstance := &Oss{
		Parallelism: opt.OssUploadThreadsCount,
		ChunkSize:   opt.OssMultiPartSize * 1024 * 1024,
	}
	ossInstance.UploadReader = migutil.NewCountingReader()
	ossInstance.DownloadReader = migutil.NewCountingReader()
	if storageInfo == nil {
		return nil, fmt.Errorf("storageInfo is nil")
	}
	if storageInfo.Oss == nil {
		return nil, fmt.Errorf("OSS storage info is nil")
	}
	var err error
	ossInstance.BucketName, ossInstance.BasePath, err = extractOSSBucketAndObjectName(storageInfo.Oss.MountPoint)
	if err != nil {
		return nil, err
	}
	endpoint := storageInfo.Oss.Endpoint
	if storageInfo.Oss.Options == nil {
		return nil, fmt.Errorf("OSS options are empty")
	}
	accessKeyId, ok := storageInfo.Oss.Options[OSS_ACCESS_KEY_ID]
	if !ok {
		return nil, fmt.Errorf("OSS access key id is empty")
	}
	accessKeySecret, ok := storageInfo.Oss.Options[OSS_ACCESS_KEY_SECRET]
	if !ok {
		return nil, fmt.Errorf("OSS access key secret is empty")
	}
	ossInstance.Uri = spliceUri(ossInstance.BucketName, endpoint, accessKeyId, accessKeySecret, ossInstance.BasePath)
	ossInstance.endpoint = endpoint
	ossInstance.accessKeyID = accessKeyId
	ossInstance.accessKeySecret = accessKeySecret
	provider := credentials.NewStaticCredentialsProvider(accessKeyId, accessKeySecret)
	cfg := oss.LoadDefaultConfig().
		WithSignatureVersion(oss.SignatureVersionV1).
		WithCredentialsProvider(provider).
		WithEndpoint(endpoint)
	client := oss.NewClient(cfg)
	ossInstance.OssClient = oss_sdk.NewClient(client, ossInstance.ChunkSize, ossInstance.Parallelism, 3)

	return ossInstance, nil
}

func (o *Oss) Name() string {
	return "oss"
}

func (o *Oss) Upload(reader io.Reader, dstTarFile string) (int64, error) {
	o.UploadReader.SetReader(reader)
	realObjectName, err := filepath.Rel("/", filepath.Join(o.BasePath, dstTarFile))
	if err != nil {
		return 0, err
	}

	return o.OssClient.UploadWithReader(o.UploadReader, o.BucketName, realObjectName)
}

// signedPartURL generates the presigned URL for probing the idx-th shard.
func (o *Oss) signedPartURL(checkpointDir string, idx int) string {
	objectName, err := filepath.Rel("/", filepath.Join(o.BasePath, PartObjectName(checkpointDir, idx)))
	if err != nil {
		return ""
	}
	return presignOSSGetURL(o.BucketName, o.endpoint, objectName, o.accessKeyID, o.accessKeySecret)
}

// Restore downloads and extracts all shards under checkpointDir concurrently.
//
// checkpointDir is a dir (no longer a single tar file) containing part-00.tar
// ... The shard count comes from Range probing: trust the objects actually
// existing in storage rather than building names from a number recorded
// elsewhere. The expected "how many should there be" check is done by
// Migration-Controller with CheckpointAttributes.FileCount before issuing
// restore.
func (o *Oss) Restore(ctx context.Context, checkpointDir string, dstDir string) (*RestoreInfo, error) {
	o.StartTime = time.Now()

	indices, err := ProbeRemoteParts(ctx, func(idx int) string {
		return o.signedPartURL(checkpointDir, idx)
	})
	if err != nil {
		return nil, fmt.Errorf("discover checkpoint parts under %s: %w", checkpointDir, err)
	}

	if err := restoreIntoTemp(dstDir, "", nil, func(tmpDir string) error {
		return unpackPartsConcurrently(ctx, indices, tmpDir, o.openPart(checkpointDir),
			&migarchive.TarOptions{Compression: migarchive.Uncompressed})
	}); err != nil {
		return nil, fmt.Errorf("oss SDK restore failed: %w", err)
	}

	fileCount, err := restoreInfoOf(dstDir)
	if err != nil {
		return nil, err
	}
	return &RestoreInfo{
		FileCount: fileCount,
		FileSize:  o.DownloadReader.Size(),
	}, nil
}

// openPart returns a partOpener that pulls the given shard from OSS.
func (o *Oss) openPart(checkpointDir string) partOpener {
	return func(ctx context.Context, idx int) (io.Reader, func(), error) {
		objectName, err := filepath.Rel("/", filepath.Join(o.BasePath, PartObjectName(checkpointDir, idx)))
		if err != nil {
			return nil, nil, err
		}
		reader, err := o.OssClient.DownloadWithReader(ctx, o.BucketName, objectName)
		if err != nil {
			return nil, nil, fmt.Errorf("oss-sdk download failed: %w", err)
		}
		return o.DownloadReader.Wrap(reader), nil, nil
	}
}

func (o *Oss) CleanUpload(path string) error {
	return nil
}

func (o *Oss) GetUploadInfo() (*UploadInfo, error) {
	readDuration := o.OssClient.GetPouchReadInfo()
	return &UploadInfo{
		UploadedSize: o.UploadReader.Size(),
		ChunkSize:    o.ChunkSize,
		Parallelism:  o.Parallelism,
		ReadDuration: readDuration,
	}, nil
}
func (o *Oss) GetDownloadInfo() (*RestoreInfo, error) {
	return &RestoreInfo{
		FileSize: o.DownloadReader.Size(),
	}, nil
}

func init() {
	RegisterStoragePlugin("oss", NewOss)
}
