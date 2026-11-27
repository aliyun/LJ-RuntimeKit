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

package oss_sdk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	migutil "github.com/container-migration/worker/pkg/util"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
)

func (c *Client) UploadWithReader(reader io.Reader, bucketName, objectName string) (int64, error) {
	// Reset the per-upload state before anything else.
	//
	// A checkpoint is now uploaded as N tar parts, i.e. this method is called N
	// times on the same Client. parts / uploadId / errChan are Client fields, so
	// without this reset the second CompleteMultipartUpload would be handed the
	// first upload's part list (wrong ETags, wrong part numbers) and a stale error
	// from a previous attempt could fail an otherwise healthy upload.
	c.resetUploadState()

	c.bucketName = bucketName
	c.objectName = objectName
	initResult, err := c.initUpload()
	if err != nil {
		return 0, fmt.Errorf("failed to init upload: %v", err)
	}
	c.uploadId = *initResult.UploadId
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	partNumber := 1
	tarSize := int64(0)
	for {
		c.sem <- struct{}{}
		startTime := time.Now()
		buf := make([]byte, c.chunkSize)
		n, err := io.ReadFull(reader, buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				return 0, fmt.Errorf("failed to read file: %v", err)
			}
		}
		c.ReadDuration += time.Since(startTime)
		tarSize += int64(n)
		wg.Add(1)
		go c.uploadChunk(partNumber, buf[:n], &wg, ctx, cancel)
		partNumber++
	}
	wg.Wait()
	select {
	case err := <-c.errChan:
		return 0, fmt.Errorf("failed to upload chunk: %v", err)
	default:
	}
	completeRequest := &oss.CompleteMultipartUploadRequest{
		Bucket:   oss.Ptr(c.bucketName),
		Key:      oss.Ptr(c.objectName),
		UploadId: oss.Ptr(c.uploadId),
		CompleteMultipartUpload: &oss.CompleteMultipartUpload{
			Parts: c.parts,
		},
	}
	result, err := c.client.CompleteMultipartUpload(context.TODO(), completeRequest)
	if err != nil {
		return 0, fmt.Errorf("failed to complete multipart upload: %v", err)
	}
	if result.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("failed to complete multipart upload")
	}
	return tarSize, nil
}

// resetUploadState clears everything that belongs to a single multipart upload,
// so the same Client can be reused for the next checkpoint part.
func (c *Client) resetUploadState() {
	c.mu.Lock()
	c.parts = c.parts[:0]
	c.mu.Unlock()

	c.uploadId = ""

	// Drain any error left behind by a previous failed upload; errChan is
	// buffered with capacity 1 and is only read after wg.Wait().
	select {
	case <-c.errChan:
	default:
	}
}

func (c *Client) initUpload() (*oss.InitiateMultipartUploadResult, error) {
	initRequest := &oss.InitiateMultipartUploadRequest{
		Bucket: oss.Ptr(c.bucketName),
		Key:    oss.Ptr(c.objectName),
	}
	initResult, err := c.client.InitiateMultipartUpload(context.TODO(), initRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to initiate multipart upload: %v", err)
	}
	return initResult, nil
}

func (c *Client) uploadChunk(partNumber int, data []byte, wg *sync.WaitGroup, ctx context.Context, cancel context.CancelFunc) {
	defer func() {
		<-c.sem
		wg.Done()
	}()
	select {
	case <-ctx.Done():
		return
	default:
		partRequest := &oss.UploadPartRequest{
			Bucket:     oss.Ptr(c.bucketName),
			Key:        oss.Ptr(c.objectName),
			PartNumber: int32(partNumber),
			UploadId:   oss.Ptr(c.uploadId),
			Body:       bytes.NewReader(data),
		}
		localMD5 := fmt.Sprintf("\"%s\"", migutil.CalculateMD5(data))
		partResult, err := c.client.UploadPart(context.TODO(), partRequest)
		if err != nil {
			c.errChan <- fmt.Errorf("failed to upload part %d: %v", partNumber, err)
			cancel()
			return
		}
		c.mu.Lock()
		c.parts = append(c.parts, oss.UploadPart{
			PartNumber: partRequest.PartNumber,
			ETag:       partResult.ETag,
		})
		c.mu.Unlock()
		if localMD5 != strings.ToLower(*(partResult.ETag)) {
			c.errChan <- fmt.Errorf("local MD5 does not match ETag for part %d", partRequest.PartNumber)
			cancel()
			return
		}
		c.mux.Lock()
		c.mux.Unlock()
	}
}

func (c *Client) GetPouchReadInfo() time.Duration {
	c.mux.Lock()
	defer c.mux.Unlock()
	return c.ReadDuration
}
