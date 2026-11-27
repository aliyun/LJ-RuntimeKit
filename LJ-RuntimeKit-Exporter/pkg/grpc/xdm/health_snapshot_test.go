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

package __

import (
	"math"
	"testing"
)

func TestSnapshotPick(t *testing.T) {
	var empty *HealthSnapshot
	if empty.Pick("new-category") != nil {
		t.Fatal("a nil snapshot must not report a category")
	}
	result := &CategoryHealth{Category: "new-category"}
	snapshot := &HealthSnapshot{Categories: []*CategoryHealth{nil, result}}
	if snapshot.Pick("new-category") != result || snapshot.Pick("other") != nil {
		t.Fatal("dynamic category names must match exactly")
	}
}

func TestFreshness(t *testing.T) {
	tests := []struct {
		name              string
		sampled, ttl, now int64
		age               int64
		stale             bool
	}{
		{"fresh", 1000, 100, 1050, 50, false},
		{"boundary", 1000, 100, 1100, 100, false},
		{"expired", 1000, 100, 1101, 101, true},
		{"missing timestamp", 0, 100, 1101, 0, false},
		{"missing ttl", 1000, 0, 1101, 101, false},
		{"negative timestamp", -1, 100, 1101, 0, false},
		{"negative ttl", 1000, -1, 1101, 101, false},
		{"future", 1200, 100, 1101, 0, false},
		{"negative now", 1000, 100, math.MinInt64, 0, false},
		{"overflow", math.MaxInt64 - 10, 100, math.MaxInt64, 10, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &GetAllHealthResponse{SampledAtMs: tt.sampled, TtlMs: tt.ttl}
			if got := result.AgeMs(tt.now); got != tt.age {
				t.Fatalf("AgeMs = %d, want %d", got, tt.age)
			}
			if got := result.Stale(tt.now); got != tt.stale {
				t.Fatalf("Stale = %v, want %v", got, tt.stale)
			}
		})
	}
	var absent *GetAllHealthResponse
	if absent.Stale(1000) || absent.AgeMs(1000) != 0 {
		t.Fatal("a nil snapshot must not be considered stale")
	}
}
