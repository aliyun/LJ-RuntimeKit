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

package utils

import (
	"fmt"
	"time"
)

// TraceBegin prints a trace begin message with timestamp to stdout.
func TraceBegin(beginWords string) {
	fmt.Printf("BEGIN %s at %s\n", beginWords, time.Now().Local().String())
}

// TraceEnd prints a trace end message with timestamp to stdout.
func TraceEnd(endWords string) {
	fmt.Printf("END %s at %s\n", endWords, time.Now().Local().String())
}
