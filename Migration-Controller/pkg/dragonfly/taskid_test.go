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

import "testing"

// The expected ids below were verified against a live Dragonfly cluster:
// after preheating the given URL, dfdaemon.v2.DfdaemonUpload/StatLocalTask
// answered with exactly these task ids.
func TestTaskIDFromURL(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want string
	}{
		{
			name: "plain oss url",
			url:  "https://example-bucket.oss-cn-hangzhou.aliyuncs.com/data/migration-worker/checkpoint.tar",
			want: "32b3c5282513838567c7d87eb850712df76ca08741db9c54e2845ef444c8677b",
		},
		{
			name: "in-cluster http url",
			url:  "http://dragonfly-manager.dragonfly.svc:8080/health",
			want: "83a355cbe720d82952c461e8f7a386787f3b31f89fd27562dfb4219f75d863fc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := TaskIDFromURL(tt.url); got != tt.want {
				t.Errorf("TaskIDFromURL(%q) = %s, want %s", tt.url, got, tt.want)
			}
		})
	}
}

// Signed URLs must map to the same task id as their unsigned form because the
// manager strips signature query params before the scheduler computes the id.
func TestTaskIDFromURLStripsSignatureParams(t *testing.T) {
	base := "https://bucket.oss-cn-hangzhou.aliyuncs.com/path/file.tar"
	signed := base + "?OSSAccessKeyId=ak&Expires=123&Signature=sig&version=2"

	if got, want := TaskIDFromURL(signed), TaskIDFromURL(base+"?version=2"); got != want {
		t.Errorf("signed url id = %s, want %s", got, want)
	}
}
