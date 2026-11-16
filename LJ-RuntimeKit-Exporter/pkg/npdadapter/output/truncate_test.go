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
