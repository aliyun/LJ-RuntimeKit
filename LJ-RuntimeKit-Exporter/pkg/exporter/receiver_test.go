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

package exporter

import (
	"strings"
	"testing"

	"github.com/lj-runtimekit/exporter/pkg/common"
)

func TestReceiverFactory_UnknownType(t *testing.T) {
	tests := []struct {
		name      string
		recvType  string
		wantError bool
	}{
		{
			name:      "unknown type returns error",
			recvType:  "Unknown",
			wantError: true,
		},
		{
			name:      "empty string returns error",
			recvType:  "",
			wantError: true,
		},
		{
			name:      "random string returns error",
			recvType:  "NotARealReceiver",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			receiver, err := ReceiverFactory(tt.recvType, nil, nil, []common.Counter{}, nil)

			if tt.wantError {
				if err == nil {
					t.Errorf("ReceiverFactory(%q) expected error, got nil", tt.recvType)
				}
				if receiver != nil {
					t.Errorf("ReceiverFactory(%q) expected nil receiver on error, got %v", tt.recvType, receiver)
				}
				if !strings.Contains(err.Error(), "unknown receiver type") {
					t.Errorf("ReceiverFactory(%q) error message should contain 'unknown receiver type', got: %v", tt.recvType, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("ReceiverFactory(%q) unexpected error: %v", tt.recvType, err)
				}
			}
		})
	}
}
