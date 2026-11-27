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

package dragonfly

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// DefaultOSSSignExpiry is the validity window of generated OSS pre-signed
// URLs. The preheat download starts right after the job is created, so a
// generous window only guards against clock skew and slow scheduling.
const DefaultOSSSignExpiry = 24 * time.Hour

// ossEndpointSuffix identifies aliyuncs OSS endpoints, e.g.
// "bucket.oss-cn-wulanchabu.aliyuncs.com".
const ossEndpointSuffix = ".aliyuncs.com"

// SignOSSURL returns a pre-signed GET URL (OSSAccessKeyId + Expires +
// Signature query parameters, OSS signature version 1) for the plain object
// URL. The signature parameters are stripped by the Dragonfly manager and
// dfdaemon when computing the task id (they are in the default
// filtered_query_params list), so a signed and an unsigned URL map to the
// same preheat task and the cache probe stays consistent.
//
// Only virtual-hosted style URLs ("bucket.endpoint/object") are supported;
// path-style URLs and URLs carrying their own query parameters are rejected
// because they need extra canonicalization.
func SignOSSURL(rawURL, accessKeyID, accessKeySecret string, expiry time.Duration) (string, error) {
	if accessKeyID == "" || accessKeySecret == "" {
		return "", fmt.Errorf("access key id and secret are required to sign OSS URL")
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("parse OSS URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("unsupported scheme %q in OSS URL", u.Scheme)
	}
	if !strings.HasSuffix(u.Host, ossEndpointSuffix) {
		return "", fmt.Errorf("host %q is not an OSS endpoint (*%s)", u.Host, ossEndpointSuffix)
	}
	// Virtual-hosted style: the first hostname label is the bucket name.
	bucket, _, ok := strings.Cut(u.Host, ".")
	if !ok || bucket == "" {
		return "", fmt.Errorf("cannot extract bucket from host %q", u.Host)
	}
	if u.Path == "" || u.Path == "/" {
		return "", fmt.Errorf("OSS URL %q has no object path", rawURL)
	}
	if u.RawQuery != "" {
		return "", fmt.Errorf("OSS URL %q already carries query parameters, expected a plain object path", rawURL)
	}

	if expiry <= 0 {
		expiry = DefaultOSSSignExpiry
	}
	expires := strconv.FormatInt(time.Now().Add(expiry).Unix(), 10)

	// Signature V1 for a plain GET without content headers or OSS
	// sub-resources:
	//   StringToSign = "GET\n\n\n{Expires}\n{CanonicalizedResource}"
	//   CanonicalizedResource = "/{bucket}{object}"
	stringToSign := "GET\n\n\n" + expires + "\n/" + bucket + u.Path
	mac := hmac.New(sha1.New, []byte(accessKeySecret))
	mac.Write([]byte(stringToSign))
	signature := base64.StdEncoding.EncodeToString(mac.Sum(nil))

	q := url.Values{}
	q.Set("OSSAccessKeyId", accessKeyID)
	q.Set("Expires", expires)
	q.Set("Signature", signature)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// IsOSSURL reports whether rawURL points at an aliyuncs OSS endpoint.
func IsOSSURL(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	return strings.HasSuffix(u.Host, ossEndpointSuffix)
}
