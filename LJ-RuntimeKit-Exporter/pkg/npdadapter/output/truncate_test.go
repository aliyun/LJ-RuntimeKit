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

package output

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestTruncate(t *testing.T) {
	tests := []struct {
		input string
		limit int
		want  string
	}{
		{"", 160, ""},
		{"abc", 0, ""},
		{"abc", -1, ""},
		{"abc", 3, "abc"},
		{"abcd", 3, "abc"},
		{"€€€€", 7, "€€"},
		{"€€", 2, ""},
		{"a🙂b", 4, "a"},
		{"a🙂b", 5, "a🙂"},
		{"a\xffb", 4, "a\uFFFD"},
		{strings.Repeat("x", 161), 160, strings.Repeat("x", 160)},
	}
	for _, tt := range tests {
		if got := Truncate(tt.input, tt.limit); got != tt.want {
			t.Errorf("Truncate(%q, %d) = %q, want %q", tt.input, tt.limit, got, tt.want)
		}
	}
}

func FuzzTruncate(f *testing.F) {
	f.Add("€€🙂€€", 7)
	f.Add("\xff\xfe", 2)
	f.Fuzz(func(t *testing.T, input string, limit int) {
		got := Truncate(input, limit)
		if !utf8.ValidString(got) || (limit <= 0 && got != "") || (limit > 0 && len(got) > limit) {
			t.Fatalf("Truncate(%q, %d) = %q", input, limit, got)
		}
	})
}
