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

package standard

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/lj-runtimekit/scheduler/pkg/utils"

	"github.com/stretchr/testify/assert"
)

func TestPatchPodAssigned(t *testing.T) {
	tests := []struct {
		name        string
		idList      []int
		patchMap    map[string]string
		flags       []assignedFlag
		expectError bool
	}{
		{
			name:        "normal assigned only",
			idList:      []int{0, 1},
			patchMap:    nil,
			flags:       []assignedFlag{normalAssigned},
			expectError: false,
		},
		{
			name:        "multiple flags",
			idList:      []int{2},
			patchMap:    map[string]string{"custom.key": "value"},
			flags:       []assignedFlag{vcoreAssigned, vmemAssigned},
			expectError: false,
		},
		{
			name:        "empty id list",
			idList:      []int{},
			patchMap:    nil,
			flags:       []assignedFlag{},
			expectError: false,
		},
		{
			name:        "invalid flag",
			idList:      []int{0},
			patchMap:    nil,
			flags:       []assignedFlag{3}, // not in patchAnnotationRes2AssignedFlag
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := patchPodAssigned(tt.idList, tt.patchMap, tt.flags...)
			if tt.expectError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.NotNil(t, result)

			var patch map[string]interface{}
			err = json.Unmarshal(result, &patch)
			assert.NoError(t, err)

			meta, ok := patch["metadata"].(map[string]interface{})
			assert.True(t, ok)
			annos, ok := meta["annotations"].(map[string]interface{})
			assert.True(t, ok)

			// Check device index
			expectedIndex := ""
			if len(tt.idList) > 0 {
				strs := make([]string, len(tt.idList))
				for i, id := range tt.idList {
					strs[i] = string(rune('0' + id)) // simple int to string
				}
				expectedIndex = strings.Join(strs, ",")
			}
			assert.Equal(t, expectedIndex, annos[utils.AnnoDeviceIndex])

			// Check assume time is a valid timestamp
			timeStr, ok := annos[utils.AnnoResourceAssumeTime].(string)
			assert.True(t, ok)
			assert.NoError(t, err)
			assert.NotEmpty(t, timeStr)
			assert.Regexp(t, `^\d+$`, timeStr)

			// Check flags
			for _, flag := range tt.flags {
				annoKey, exists := patchAnnotationRes2AssignedFlag[flag]
				assert.True(t, exists)
				assert.Equal(t, "false", annos[annoKey])
			}

			// Check custom patchMap
			if tt.patchMap != nil {
				for k, v := range tt.patchMap {
					assert.Equal(t, v, annos[k])
				}
			}
		})
	}
}
