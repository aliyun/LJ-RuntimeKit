// Package output enforces the byte limit for NPD condition messages.
package output

import (
	"strings"
	"unicode/utf8"
)

// Truncate limits a message to maxBytes while preserving valid UTF-8.
func Truncate(message string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	message = strings.ToValidUTF8(message, "\uFFFD")
	if len(message) <= maxBytes {
		return message
	}
	for maxBytes > 0 && !utf8.RuneStart(message[maxBytes]) {
		maxBytes--
	}
	return message[:maxBytes]
}
