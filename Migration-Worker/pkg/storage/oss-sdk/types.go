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
	"sync"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
)

type Client struct {
	client       *oss.Client
	chunkSize    int64
	parallelism  int
	maxRetries   int
	uploadId     string
	errChan      chan error
	sem          chan struct{}
	mu           sync.Mutex
	parts        []oss.UploadPart
	bucketName   string
	objectName   string
	ReadDuration time.Duration
	mux          sync.Mutex
}

func NewClient(client *oss.Client, chunkSize int64, parallelism, maxRetries int) *Client {
	return &Client{
		client:      client,
		chunkSize:   chunkSize,
		parallelism: parallelism,
		maxRetries:  maxRetries,
		errChan:     make(chan error, 1),
		sem:         make(chan struct{}, parallelism),
		parts:       make([]oss.UploadPart, 0, 100),
	}
}
