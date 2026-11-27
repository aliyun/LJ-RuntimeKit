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
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

// ============================================================================
// Checkpoint shard layout
//
// The checkpoint artifact is no longer a single tar, but N independent shards under one dir:
//
//	<podUID>/<version>/<container>/checkpoint/part-00.tar
//	                                        /part-01.tar
//	                                        ...
//
// Each shard is a complete, independently extractable tar (carrying the full
// dir skeleton), so the restore side can have N workers download and extract
// into the same target dir concurrently.
// ============================================================================

const (
	// PartNameFormat is the shard file name format. Zero-padded to two digits so lexicographic order matches numeric order.
	//
	// ⚠️ This format has one copy in each of three repos and must be verbatim
	// identical. Dragonfly's task_id is computed from the URL; a mismatch never
	// errors - the preheat cache just misses everything and silently degrades to
	// origin downloads. The other two copies:
	//   - the node-side XDM agent's checkpoint part layout
	//   - Migration-Controller/pkg/dragonfly/parts.go
	PartNameFormat = "part-%02d.tar"

	// CheckpointDirName is the dir holding all shards (relative to <podUID>/<version>/<container>/).
	CheckpointDirName = "checkpoint"

	// MaxProbeParts is the upper bound of shard indices when probing.
	//
	// ⚠️ Must match MaxCheckpointPartCount in X-Device-Manager. If XDM configures
	// more than this, some shards can never be probed and restore downloads less
	// data. XDM guards startup failure on exceeding the limit; this is one more
	// layer.
	MaxProbeParts = 64

	// partProbeConcurrency limits concurrent probe requests. A probe is a 1-byte
	// Range GET, very light, but no point opening 64 connections at once.
	partProbeConcurrency = 16

	// partProbeTimeout is the timeout of a single probe.
	partProbeTimeout = 15 * time.Second
)

// partNameRe filters valid shard names from a local dir, ignoring leftovers like .tmp.
var partNameRe = regexp.MustCompile(`^part-(\d{2})\.tar$`)

// PartName returns the file name of the idx-th shard.
func PartName(idx int) string {
	return fmt.Sprintf(PartNameFormat, idx)
}

// PartObjectName joins the checkpoint dir and the shard index into a relative object name.
func PartObjectName(checkpointDir string, idx int) string {
	return filepath.Join(checkpointDir, PartName(idx))
}

// validateContiguous verifies the shard indices start at 0 with no gaps.
//
// Note this check cannot catch "one missing at the tail": if 00..06 exist and
// 07 is missing, the indices are still contiguous and it is just judged as
// partCount=7. That blind spot is closed by Migration-Controller, which
// cross-checks CheckpointAttributes.FileCount (the authoritative expected value
// recorded by the producer) before issuing restore.
func validateContiguous(indices []int) error {
	for i, idx := range indices {
		if idx != i {
			return fmt.Errorf("checkpoint parts are not contiguous: expected %s at position %d but found %s",
				PartName(i), i, PartName(idx))
		}
	}
	return nil
}

// DiscoverLocalParts lists shards under the local (or mounted) checkpoint dir,
// ascending. localfs / nfs take this path: reading the dir directly is simpler
// and more accurate than probing.
func DiscoverLocalParts(dir string) ([]int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read checkpoint dir %s: %w", dir, err)
	}

	var indices []int
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		m := partNameRe.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		idx, err := strconv.Atoi(m[1])
		if err != nil {
			continue
		}
		indices = append(indices, idx)
	}
	sort.Ints(indices)

	if len(indices) == 0 {
		return nil, fmt.Errorf("no checkpoint part found under %s (expected %s ...)", dir, PartName(0))
	}
	if err := validateContiguous(indices); err != nil {
		return nil, err
	}
	return indices, nil
}

// ProbeRemoteParts probes which shards exist remotely with 1-byte Range GETs, returning ascending indices.
//
// Range GET instead of ListObjects because:
//   - no permission change. A Range GET needs oss:GetObject, exactly the same
//     as the existing presigned download; ListObjects needs an extra
//     bucket-level permission and a RAM policy change.
//   - no OSS SDK and no XML parsing; just reuse the existing signing logic.
//
// signedURL is provided by the caller because each storage signs differently (OSS presign / dfdaemon).
func ProbeRemoteParts(ctx context.Context, signedURL func(partIdx int) string) ([]int, error) {
	type result struct {
		idx    int
		exists bool
		err    error
	}

	results := make([]result, MaxProbeParts)
	sem := make(chan struct{}, partProbeConcurrency)
	var wg sync.WaitGroup

	for i := 0; i < MaxProbeParts; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			exists, err := objectExists(ctx, signedURL(idx))
			results[idx] = result{idx: idx, exists: exists, err: err}
		}(i)
	}
	wg.Wait()

	var indices []int
	for _, r := range results {
		if r.err != nil {
			// A single probe failure cannot be treated as "not exists": that would silently skip a shard in restore.
			return nil, fmt.Errorf("probe checkpoint part %s: %w", PartName(r.idx), r.err)
		}
		if r.exists {
			indices = append(indices, r.idx)
		}
	}

	if len(indices) == 0 {
		return nil, fmt.Errorf("no checkpoint part found (probed %s..%s)", PartName(0), PartName(MaxProbeParts-1))
	}
	if err := validateContiguous(indices); err != nil {
		return nil, err
	}
	klog.V(4).Infof("discovered %d checkpoint part(s) by probing", len(indices))
	return indices, nil
}

// objectExists sends a Range: bytes=0-0 GET to check whether an object exists.
//
// GET instead of HEAD: the OSS presigned URL's signature hard-codes the HTTP
// verb (the first line of StringToSign is "GET"), so a HEAD would fail the
// signature. A Range header pulls just 1 byte - cost equivalent to HEAD while
// 100% reusing the existing signing path.
func objectExists(ctx context.Context, url string) (bool, error) {
	reqCtx, cancel := context.WithTimeout(ctx, partProbeTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Range", "bytes=0-0")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK, http.StatusPartialContent:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
}

// presignOSSGetURL constructs an OSS V1 presigned GET URL:
//
//	https://{bucket}.{endpoint}/{object}?OSSAccessKeyId=..&Expires=..&Signature=..
//
// Expiry is "next UTC midnight" instead of now+24h, so the same object always
// gets exactly the same URL within its validity period. dfdaemon computes
// task_id from it, and only a stable URL can hit the P2P cache across
// requests.
//
// The signing params themselves are in Dragonfly's default filtered_query_params
// and are stripped when computing task_id, so what really needs to align
// verbatim with the preheat side is only the **object path** (i.e. the segment
// built by PartNameFormat), not query ordering or expiry.
func presignOSSGetURL(bucket, endpoint, objectName, accessKeyID, accessKeySecret string) string {
	now := time.Now().UTC()
	tomorrow := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, time.UTC)
	expires := tomorrow.Unix()

	stringToSign := fmt.Sprintf("GET\n\n\n%d\n/%s/%s", expires, bucket, objectName)

	mac := hmac.New(sha1.New, []byte(accessKeySecret))
	mac.Write([]byte(stringToSign))
	signature := base64.StdEncoding.EncodeToString(mac.Sum(nil))

	// AK and the + / = in the signature have special meaning in a query and must be escaped.
	return fmt.Sprintf("https://%s.%s/%s?OSSAccessKeyId=%s&Expires=%d&Signature=%s",
		bucket, endpoint, objectName,
		url.QueryEscape(accessKeyID), expires, url.QueryEscape(signature))
}
