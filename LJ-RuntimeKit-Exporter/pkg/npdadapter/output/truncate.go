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
