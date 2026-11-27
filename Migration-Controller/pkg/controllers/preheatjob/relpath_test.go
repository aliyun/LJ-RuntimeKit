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

package preheatjob

import "testing"

// TestValidRelPath pins the structural validation gauge of pvcPath / PV.path:
// any character inside a segment is allowed (checkpoint data may be moved by
// users to self-made directories); only "."/".." segments, empty segments and
// backslashes are rejected; empty values are structurally legal and judged by
// the caller.
func TestValidRelPath(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    string
		wantErr bool
	}{
		// Real formats and user self-made directories: all allowed
		{name: "real checkpoint path", raw: "pod-1/123/c0/checkpoint", want: "pod-1/123/c0/checkpoint"},
		{name: "leading and trailing slash", raw: "/pod-1/123/c0/checkpoint/", want: "pod-1/123/c0/checkpoint"},
		{name: "double leading slash", raw: "//a/b", want: "a/b"},
		{name: "uppercase segments", raw: "Pod-1/123/C0/Checkpoint", want: "Pod-1/123/C0/Checkpoint"},
		{name: "user dir with spaces and parens", raw: "my dir/backup (2026)/checkpoint", want: "my dir/backup (2026)/checkpoint"},
		{name: "shell metacharacters", raw: "a;touch x/b$c/`d`|e&f", want: "a;touch x/b$c/`d`|e&f"},
		{name: "url reserved characters", raw: "100%/a?b#c", want: "100%/a?b#c"},
		{name: "non-ascii segments", raw: "café/данные/checkpoint", want: "café/данные/checkpoint"},
		{name: "dot inside longer segment", raw: "a.b/c.d", want: "a.b/c.d"},

		// Empty value: structurally legal, returns empty string judged by the caller
		{name: "empty", raw: "", want: ""},
		{name: "single slash", raw: "/", want: ""},
		{name: "multiple slashes only", raw: "///", want: ""},

		// Structural rejections
		{name: "parent segment", raw: "../x", wantErr: true},
		{name: "trailing parent segment", raw: "a/..", wantErr: true},
		{name: "nested traversal", raw: "a/../../etc", wantErr: true},
		{name: "current dir segment", raw: "a/./b", wantErr: true},
		{name: "dot only", raw: ".", wantErr: true},
		{name: "dotdot only", raw: "..", wantErr: true},
		{name: "interior empty segment", raw: "a//b", wantErr: true},
		{name: "backslash", raw: `a\b`, wantErr: true},
		{name: "windows style traversal", raw: `..\..\etc`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := validRelPath(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("validRelPath(%q) = %q, want an error", tt.raw, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("validRelPath(%q) returned error: %v", tt.raw, err)
			}
			if got != tt.want {
				t.Fatalf("validRelPath(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}
