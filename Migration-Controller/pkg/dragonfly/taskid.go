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
	"crypto/sha256"
	"encoding/hex"
	neturl "net/url"
)

// taskTypeStandard is the string form of common.v2.TaskType_STANDARD that the
// Dragonfly scheduler appends when generating URL based task ids.
const taskTypeStandard = "STANDARD"

// defaultFilteredQueryParams mirrors the query params the Dragonfly manager
// strips by default before computing a preheat task id (dragonfly/v2
// pkg/idgen: S3/Gcs/Oss/Obs/Cos/Containerd filtered query params). Keeping
// this list in sync with the manager is required for the generated id to
// match the one used by the scheduler and dfdaemons.
var defaultFilteredQueryParams = []string{
	// S3
	"X-Amz-Algorithm",
	"X-Amz-Credential",
	"X-Amz-Date",
	"X-Amz-Expires",
	"X-Amz-SignedHeaders",
	"X-Amz-Signature",
	"X-Amz-Security-Token",
	"X-Amz-User-Agent",
	// GCS
	"X-Goog-Algorithm",
	"X-Goog-Credential",
	"X-Goog-Date",
	"X-Goog-Expires",
	"X-Goog-SignedHeaders",
	"X-Goog-Signature",
	// OSS
	"OSSAccessKeyId",
	"Expires",
	"Signature",
	"SecurityToken",
	// OBS
	"AccessKeyId",
	"X-Obs-Date",
	"X-Obs-Security-Token",
	// COS
	"q-sign-algorithm",
	"q-ak",
	"q-sign-time",
	"q-key-time",
	"q-header-list",
	"q-url-param-list",
	"q-signature",
	"x-cos-security-token",
	// containerd
	"ns",
}

// TaskIDFromURL computes the Dragonfly dfdaemon task id of a preheat URL. It
// reproduces scheduler idgen.TaskIDV2ByURLBased(url, nil, "", "", filter, "")
// with the manager's default filtered query params: the id is the hex encoded
// sha256 over url + tag + application + revision + TaskType, where the latter
// four reduce to "STANDARD" for the preheat jobs created by this controller.
// The resulting id is what dfdaemon.v2.DfdaemonUpload/StatLocalTask expects;
// the manager preheat job id (status.taskID) is NOT accepted by dfdaemons.
func TaskIDFromURL(rawURL string) string {
	h := sha256.New()
	h.Write([]byte(filterQueryParams(rawURL)))
	h.Write([]byte(taskTypeStandard))
	return hex.EncodeToString(h.Sum(nil))
}

// filterQueryParams removes the default filtered query params from rawURL,
// mirroring dragonfly/v2 pkg/net/url.FilterQueryParams: on parse failure the
// URL is treated as empty, and the remaining params are re-encoded so the
// output matches u.String() byte for byte.
func filterQueryParams(rawURL string) string {
	u, err := neturl.Parse(rawURL)
	if err != nil {
		return ""
	}

	hidden := make(map[string]struct{}, len(defaultFilteredQueryParams))
	for _, p := range defaultFilteredQueryParams {
		hidden[p] = struct{}{}
	}

	values := make(neturl.Values)
	for k, v := range u.Query() {
		if _, ok := hidden[k]; !ok {
			values[k] = v
		}
	}

	u.RawQuery = values.Encode()
	return u.String()
}
