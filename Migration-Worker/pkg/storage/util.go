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
	"fmt"
	"net/url"
)

func extractOSSBucketAndObjectName(uri string) (string, string, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return "", "", fmt.Errorf("invalid URI format: %w", err)
	}

	if u.Scheme != "oss" {
		return "", "", fmt.Errorf("invalid scheme: %s, expected 'oss'", u.Scheme)
	}

	if u.Host == "" {
		return "", "", fmt.Errorf("missing bucket name in URI")
	}
	if u.Path == "" {
		return u.Host, "", nil
	}

	return u.Host, u.Path, nil
}

func spliceUri(bucketName, endpoint, accessKeyId, accessKeySecret, basePath string) string {
	uri := "oss://" + bucketName + "?OSS_ENDPOINT=" + endpoint
	uri += "&OSS_ACCESS_ID=" + accessKeyId
	uri += "&OSS_ACCESS_KEY=" + accessKeySecret
	if basePath == "" {
		uri += "/"
	} else {
		uri += basePath
	}
	return uri
}
