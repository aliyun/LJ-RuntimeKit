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

package collectorprotocol

import (
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func TestNormalizeContainerID(t *testing.T) {
	for _, tc := range []struct{ input, want string }{
		{"containerd://abc", "abc"}, {"docker://abc", "abc"}, {"cri-o://abc", "abc"},
		{"abc", "abc"}, {"", ""}, {"containerd://", ""},
	} {
		if got := NormalizeContainerID(tc.input); got != tc.want {
			t.Errorf("NormalizeContainerID(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestOutputFileName(t *testing.T) {
	for _, tc := range []struct{ id, want string }{
		{"c1", "ns_pod_c1_trace.json"},
		{"containerd://c1", "ns_pod_c1_trace.json"},
		{"docker://abcdefghijklmnop", "ns_pod_abcdefghijkl_trace.json"},
	} {
		if got := OutputFileName("ns", "pod", tc.id, "trace.json"); got != tc.want {
			t.Errorf("OutputFileName(%q) = %q, want %q", tc.id, got, tc.want)
		}
	}
}

func TestOutputFileNameUsesRemainingByteBudget(t *testing.T) {
	for _, longIdentity := range []bool{false, true} {
		namespace, pod, id := "ns", "pod", "containerd://c1"
		if longIdentity {
			namespace, pod, id = strings.Repeat("n", 253), strings.Repeat("p", 253), "containerd://"+strings.Repeat("c", 253)
		}
		prefix := fmt.Sprintf("%s_%s_%s_", boundedFilename(namespace, 64), boundedFilename(pod, 64), containerFilenameID(id))
		budget := 255 - len(prefix) - len(partialSuffix)
		for _, length := range []int{budget - 1, budget, budget + 1, 253, 255} {
			source := strings.Repeat("s", length)
			got := OutputFileName(namespace, pod, id, source)
			want := prefix + source
			if length > budget {
				hash := sha256.Sum256([]byte(source))
				want = prefix + fmt.Sprintf("%s-%x", source[:budget-17], hash[:8])
			}
			if got != want || len(got+partialSuffix) > 255 || filepath.Base(got) != got || OutputFileName(namespace, pod, id, source) != got {
				t.Fatalf("filename budget or deterministic hash violated: %q, want %q", got, want)
			}
			if length > budget && got == OutputFileName(namespace, pod, id, source[:length-1]+"t") {
				t.Fatalf("distinct truncated source names collided: %q", got)
			}
		}
	}
}
