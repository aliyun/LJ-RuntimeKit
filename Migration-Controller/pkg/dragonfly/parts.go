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
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ============================================================================
// Checkpoint part discovery.
//
// A checkpoint artifact is a directory containing N self-contained tar parts.
// The restore side downloads them in parallel to compress the restore time, so
// preheat must warm **every** part under the directory, while Dragonfly's
// preheat only accepts a single URL - the expansion can only be done by the
// controller.
//
// The part count is not written in the CR but discovered: the PreheatJob spec
// then only needs one top-level directory, and changing the part count from 8
// to 16 later requires no protocol change.
// ============================================================================

const (
	// PartNameFormat is the part file name format.
	//
	// NOTE: this format has one copy in each of three repos and must match
	// verbatim. Dragonfly's task_id is computed from the URL; a mismatch does
	// not error - preheat and download just compute different task_ids, no
	// cache hit at all and a silent fallback to origin download. The other two
	// places:
	//   - the node-side XDM agent's checkpoint part layout
	//   - Migration-Worker/pkg/storage/parts.go
	PartNameFormat = "part-%02d.tar"

	// MaxProbeParts is the upper bound of part indices when probing.
	//
	// NOTE: must match X-Device-Manager's MaxCheckpointPartCount. If XDM is
	// configured larger, the extra parts are never preheated: restore still
	// downloads correctly (origin fetch), but the preheat benefit is silently
	// lost with no error. The XDM side protects against over-limit at startup;
	// this is the second line of defense.
	MaxProbeParts = 64

	// partProbeConcurrency limits concurrent probe requests.
	partProbeConcurrency = 16

	// partProbeTimeout is the timeout of a single probe.
	partProbeTimeout = 15 * time.Second
)

// PartName returns the file name of the idx-th part.
func PartName(idx int) string {
	return fmt.Sprintf(PartNameFormat, idx)
}

// PartURL joins the checkpoint directory URL with the part index into the
// part's full URL.
//
// dirURL is exactly PreheatJob.spec.remoteURL, with or without a trailing
// slash.
func PartURL(dirURL string, idx int) string {
	return strings.TrimRight(dirURL, "/") + "/" + PartName(idx)
}

// DiscoverParts probes which parts actually exist under dirURL and returns
// the ascending index list.
//
// A 1-byte Range GET probe is used instead of ListObjects because:
//   - No permission change. A Range GET needs exactly oss:GetObject, the same
//     as the existing presigned origin fetch; ListObjects needs an extra
//     bucket-level permission and a RAM policy change.
//   - No OSS SDK and no XML parsing; SignOSSURL is reused directly. The
//     controller has never made any OSS API call so far, and this one feature
//     is not worth turning it into a direct OSS caller.
//
// Indices must be continuous from 0. A hole means the checkpoint upload is
// incomplete - error out rather than treating it as "that's all there is".
// Note this check cannot catch "the tail one is lost" (with 00..06 present and
// 07 missing the indices are still continuous); that blind spot is plugged by
// comparing against CheckpointAttributes.FileCount.
func DiscoverParts(ctx context.Context, dirURL, accessKeyID, accessKeySecret string) ([]int, error) {
	type probeResult struct {
		exists bool
		err    error
	}

	results := make([]probeResult, MaxProbeParts)
	sem := make(chan struct{}, partProbeConcurrency)
	var wg sync.WaitGroup

	for i := 0; i < MaxProbeParts; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			probeURL := PartURL(dirURL, idx)
			if IsOSSURL(probeURL) {
				// OSS sources need presigning; the http address of a PVC preheat source needs no signature, use directly.
				signed, err := SignOSSURL(probeURL, accessKeyID, accessKeySecret, DefaultOSSSignExpiry)
				if err != nil {
					results[idx] = probeResult{err: err}
					return
				}
				probeURL = signed
			}
			exists, err := objectExists(ctx, probeURL)
			results[idx] = probeResult{exists: exists, err: err}
		}(i)
	}
	wg.Wait()

	var indices []int
	for idx, r := range results {
		if r.err != nil {
			// A probe failure must not be treated as "not exists": that would skip preheating this part and silently fall back to origin fetch at restore.
			return nil, fmt.Errorf("probe checkpoint part %s: %w", PartName(idx), r.err)
		}
		if r.exists {
			indices = append(indices, idx)
		}
	}

	if len(indices) == 0 {
		return nil, fmt.Errorf("no checkpoint part found under %s (probed %s..%s)",
			dirURL, PartName(0), PartName(MaxProbeParts-1))
	}
	for i, idx := range indices {
		if idx != i {
			return nil, fmt.Errorf("checkpoint parts under %s are not contiguous: "+
				"expected %s at position %d but found %s", dirURL, PartName(i), i, PartName(idx))
		}
	}
	return indices, nil
}

// objectExists sends a Range: bytes=0-0 GET to check whether the object
// exists.
//
// GET instead of HEAD: the signature of an OSS presigned URL bakes in the HTTP
// verb (the first line of StringToSign is "GET"), sending HEAD with it would
// mismatch the signature. Adding a Range header pulls only 1 byte, costing the
// same as HEAD, while 100% reusing the tested SignOSSURL path.
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
