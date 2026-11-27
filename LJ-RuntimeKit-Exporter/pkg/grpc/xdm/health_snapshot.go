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

// HealthSnapshot aliases the full health response without additional state.
type HealthSnapshot = GetAllHealthResponse

// Pick matches a server-provided category name exactly without a predefined list.
func (s *GetAllHealthResponse) Pick(category string) *CategoryHealth {
	for _, result := range s.GetCategories() {
		if result != nil && result.GetCategory() == category {
			return result
		}
	}
	return nil
}

// AgeMs returns the sample age, or zero if its timestamp is missing or in the future.
func (r *GetAllHealthResponse) AgeMs(nowMs int64) int64 {
	if r.GetSampledAtMs() <= 0 || nowMs <= r.GetSampledAtMs() {
		return 0
	}
	return nowMs - r.GetSampledAtMs()
}

// Stale reports whether the validity period has elapsed, skipping missing time data.
func (r *GetAllHealthResponse) Stale(nowMs int64) bool {
	if r.GetSampledAtMs() <= 0 || r.GetTtlMs() <= 0 {
		return false
	}
	// Compare ages to avoid overflowing sampled_at_ms + ttl_ms.
	return r.AgeMs(nowMs) > r.GetTtlMs()
}
