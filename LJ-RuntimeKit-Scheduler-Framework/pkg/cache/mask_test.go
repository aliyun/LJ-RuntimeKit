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

package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMask_AddMask(t *testing.T) {
	tests := []struct {
		name     string
		mm       Mask
		m        uint8
		expected Mask
	}{
		{"add to empty", Mask(0), 0b00000101, 0b00000101},
		{"add to existing", Mask(0b00000001), 0b00000010, 0b00000011},
		{"add overlapping bits", Mask(0b00000101), 0b00000110, 0b00000111},
		{"add all bits", Mask(0), 0b11111111, 0b11111111},
		{"add zero", Mask(0b10101010), 0, 0b10101010},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.mm.AddMask(tt.m))
		})
	}
}

func TestMask_DelMask(t *testing.T) {
	tests := []struct {
		name     string
		mm       Mask
		m        uint8
		expected Mask
	}{
		{"del from full", Mask(0b11111111), 0b00000101, 0b11111010},
		{"del non-existing bits", Mask(0b00000001), 0b00000010, 0b00000001},
		{"del all bits", Mask(0b11111111), 0b11111111, 0},
		{"del zero", Mask(0b10101010), 0, 0b10101010},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.mm.DelMask(tt.m))
		})
	}
}

func TestMask_Empty(t *testing.T) {
	tests := []struct {
		name     string
		mm       Mask
		ciCnt    int
		expected bool
	}{
		{"all empty", Mask(0), 4, true},
		{"all full", Mask(0b1111), 4, false},
		{"partial", Mask(0b0011), 4, false},
		{"ciCnt=0", Mask(0), 0, true},
		{"ciCnt=1 empty", Mask(0), 1, true},
		{"ciCnt=1 full", Mask(1), 1, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.mm.Empty(tt.ciCnt))
		})
	}
}

func TestMask_Full(t *testing.T) {
	tests := []struct {
		name     string
		mm       Mask
		ciCnt    int
		expected bool
	}{
		{"all full", Mask(0b1111), 4, true},
		{"all empty", Mask(0), 4, false},
		{"partial", Mask(0b1100), 4, false},
		{"ciCnt=0", Mask(0), 0, true},
		{"ciCnt=1 full", Mask(1), 1, true},
		{"ciCnt=1 empty", Mask(0), 1, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.mm.Full(tt.ciCnt))
		})
	}
}

func TestMask_PlacementEmpty(t *testing.T) {
	tests := []struct {
		name     string
		mm       Mask
		from     int
		size     int
		ciCnt    int
		expected bool
	}{
		{"empty range at start", Mask(0b0011), 0, 2, 4, true},
		{"empty range at end", Mask(0b1100), 2, 2, 4, true},
		{"non-empty range", Mask(0b0101), 0, 2, 4, false},
		{"out of bounds", Mask(0), 3, 2, 4, false},
		{"size=0", Mask(0b1111), 0, 0, 4, true},
		{"full range empty", Mask(0), 0, 4, 4, true},
		{"full range not empty", Mask(1), 0, 4, 4, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.mm.PlacementEmpty(tt.from, tt.size, tt.ciCnt))
		})
	}
}

func TestMask_StartEmpty(t *testing.T) {
	tests := []struct {
		name     string
		mm       Mask
		ciCnt    int
		expected int
	}{
		{"all empty", Mask(0), 4, 4},
		{"all full", Mask(0b1111), 4, 0},
		{"partial from start", Mask(0b0111), 4, 1},
		{"partial from end", Mask(0b1110), 4, 0},
		{"ciCnt=1 empty", Mask(0), 1, 1},
		{"ciCnt=1 full", Mask(1), 1, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.mm.StartEmpty(tt.ciCnt))
		})
	}
}

func TestMask_EndEmpty(t *testing.T) {
	tests := []struct {
		name     string
		mm       Mask
		ciCnt    int
		expected int
	}{
		{"all empty", Mask(0), 4, 4},
		{"all full", Mask(0b1111), 4, 0},
		{"partial from end", Mask(0b1110), 4, 1},
		{"partial from start", Mask(0b0111), 4, 0},
		{"ciCnt=1 empty", Mask(0), 1, 1},
		{"ciCnt=1 full", Mask(1), 1, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.mm.EndEmpty(tt.ciCnt))
		})
	}
}

func TestMask_FindLastSingleFitBit(t *testing.T) {
	tests := []struct {
		name     string
		mm       Mask
		ciCnt    int
		expected int
	}{
		{"all empty, ciCnt=4", Mask(0), 4, 3},
		{"all full", Mask(0b1111), 4, -1},
		{"first empty", Mask(0b1110), 4, 3},
		{"last empty", Mask(0b0111), 4, 0},
		{"middle empty", Mask(0b1101), 4, 2},
		{"ciCnt=1 empty", Mask(0), 1, 0},
		{"ciCnt=1 full", Mask(1), 1, -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.mm.FindLastSingleFitBit(tt.ciCnt))
		})
	}
}

func TestMask_GetLongestMatchedTopo(t *testing.T) {
	tests := []struct {
		name     string
		mm       Mask
		ciCnt    int
		expected int
	}{
		{"all empty", Mask(0), 4, 4},
		{"all full", Mask(0b1111), 4, 0},
		{"middle empty 2", Mask(0b1001), 4, 2},
		{"start empty 2", Mask(0b1100), 4, 2},
		{"end empty 1", Mask(0b1110), 4, 1},
		{"alternating", Mask(0b1010), 4, 1},
		{"ciCnt=1 empty", Mask(0), 1, 1},
		{"ciCnt=1 full", Mask(1), 1, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.mm.GetLongestMatchedTopo(tt.ciCnt))
		})
	}
}

func TestMask_CheckMigTopoMatch(t *testing.T) {
	tests := []struct {
		name     string
		mm       Mask
		cis      int
		ciCnt    int
		expected bool
	}{
		{"match, cis=1", Mask(0), 1, 4, true},
		{"match, cis=2", Mask(0), 2, 4, true},
		{"no match, full", Mask(0b1111), 1, 4, false},
		{"match, longest=2 >= cis=2", Mask(0b1100), 2, 4, true},
		{"no match, longest=2 < cis=3", Mask(0b1100), 3, 4, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.mm.CheckMigTopoMatch(tt.cis, tt.ciCnt))
		})
	}
}

func TestBuildMask(t *testing.T) {
	tests := []struct {
		name     string
		ciCnt    int
		start    int
		size     int
		expected uint8
	}{
		{"ciCnt=4, start=0, size=1", 4, 0, 1, 0b1000},
		{"ciCnt=4, start=0, size=2", 4, 0, 2, 0b1100},
		{"ciCnt=4, start=2, size=2", 4, 2, 2, 0b0011},
		{"ciCnt=7, start=0, size=7", 7, 0, 7, 0b1111111},
		{"ciCnt=4, start=3, size=1", 4, 3, 1, 0b0001},
		{"size=0", 4, 0, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, BuildMask(tt.ciCnt, tt.start, tt.size))
		})
	}
}

func TestIsDriverAtLeast525(t *testing.T) {
	tests := []struct {
		name         string
		majorVersion int
		expected     bool
	}{
		{"525", 525, true},
		{"530", 530, true},
		{"470", 470, false},
		{"0", 0, false},
		{"1000", 1000, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, IsDriverAtLeast525(tt.majorVersion))
		})
	}
}
