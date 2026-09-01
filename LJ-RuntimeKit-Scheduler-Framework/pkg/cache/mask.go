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
	"math"

	"github.com/lj-runtimekit/scheduler/pkg/utils"

	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// Mask represents a bitmask for MIG compute instance placement on a GPU.
type Mask uint8

// AddMask sets the bits specified in m.
func (mm Mask) AddMask(m uint8) Mask {
	pm := mm
	for i := 0; i < 8; i++ {
		d := (m >> i) & 1
		if d == 1 {
			pm = pm | (1 << i)
		}
	}
	return pm
}

// DelMask clears the bits specified in m.
func (mm Mask) DelMask(m uint8) Mask {
	pm := mm
	for i := 0; i < 8; i++ {
		d := (m >> i) & 1
		if d == 1 {
			pm = pm &^ (1 << i)
		}
	}
	return pm
}

// CheckMigTopoMatch checks if there is a contiguous free region of at least 'cis' CIs.
func (mm Mask) CheckMigTopoMatch(cis, ciCnt int) bool {
	return mm.GetLongestMatchedTopo(ciCnt) >= cis
}

// CheckMigGiTopoMatch checks if there is a valid placement for a GI of size 'cis' with CI count 'ciCnt'.
// It accounts for driver version constraints and existing GI placements.
func (mm Mask) CheckMigGiTopoMatch(cis, ciCnt, majorVersion int, pods []*framework.PodInfo) bool {
	match := false
	switch cis {
	case utils.MigProfileSize1:
		match = !mm.Full(ciCnt)
	case utils.MigProfileSize2:
		if ciCnt == 4 {
			match = mm.PlacementEmpty(0, 2, ciCnt) || mm.PlacementEmpty(2, 2, ciCnt)
		} else {
			match = mm.PlacementEmpty(0, 2, ciCnt) || mm.PlacementEmpty(2, 2, ciCnt) || mm.PlacementEmpty(4, 2, ciCnt)
		}
	case utils.MigProfileSize3:
		if ciCnt == 4 {
			break
		}
		found4GiPod := false
		for _, podInfo := range pods {
			if utils.GetResourceReq(podInfo.Pod, utils.ResourceSlice) > 4 {
				found4GiPod = true
				break
			} else if utils.GetResourceReq(podInfo.Pod, utils.ResourceSlice) == 4 && !IsDriverAtLeast525(majorVersion) {
				found4GiPod = true
				break
			}
		}
		if !found4GiPod {
			match = mm.PlacementEmpty(0, 4, ciCnt) || mm.PlacementEmpty(4, 3, ciCnt)
		}
	case utils.MigProfileSize4:
		found3GiPod := false
		for _, podInfo := range pods {
			if utils.GetResourceReq(podInfo.Pod, utils.ResourceSlice) > 3 {
				found3GiPod = true
				break
			} else if utils.GetResourceReq(podInfo.Pod, utils.ResourceSlice) == 3 && !IsDriverAtLeast525(majorVersion) {
				found3GiPod = true
				break
			}
		}
		if !found3GiPod {
			match = mm.PlacementEmpty(0, 4, ciCnt)
		}
	case utils.MigProfileSize7:
		if ciCnt != 4 {
			match = mm.Empty(ciCnt)
		}
	default:
	}
	return match
}

// StartEmpty counts consecutive zeros starting from the most significant bit within ciCnt bits.
func (mm Mask) StartEmpty(ciCnt int) int {
	cnt := 0
	for i := ciCnt - 1; i >= 0; i-- {
		p := (mm >> i) & 1
		if p == 0 {
			cnt++
		} else {
			break
		}
	}
	return cnt
}

// EndEmpty counts consecutive zeros starting from the least significant bit within ciCnt bits.
func (mm Mask) EndEmpty(ciCnt int) int {
	cnt := 0
	for i := 0; i < ciCnt; i++ {
		p := (mm >> i) & 1
		if p == 0 {
			cnt++
		} else {
			break
		}
	}
	return cnt
}

// FindLastIdxCiReq finds the best placement for a CI request of 'reqCis' size within 'ciCnt' CIs.
// Returns the start index and size of the allocated region, or (-1, 0) if not found.
func (mm Mask) FindLastIdxCiReq(reqCis, ciCnt, majorVersion int, pods []*framework.PodInfo) (int, int) {
	switch reqCis {
	case utils.MigProfileSize1:
		return mm.FindLastSingleFitBit(ciCnt), 1
	case utils.MigProfileSize2:
		if ciCnt == 4 {
			if mm.PlacementEmpty(2, 2, 4) {
				return 2, 2
			}
			if mm.PlacementEmpty(0, 2, 4) {
				return 0, 2
			}
		} else {
			if mm.PlacementEmpty(4, 2, 7) {
				return 4, 2
			}
			if mm.PlacementEmpty(2, 2, 7) {
				return 2, 2
			}
			if mm.PlacementEmpty(0, 2, 7) {
				return 0, 2
			}
		}
	case utils.MigProfileSize3:
		found4GiPod := false
		for _, podInfo := range pods {
			if utils.GetResourceReq(podInfo.Pod, utils.ResourceSlice) > 4 {
				found4GiPod = true
				break
			} else if utils.GetResourceReq(podInfo.Pod, utils.ResourceSlice) == 4 && !IsDriverAtLeast525(majorVersion) {
				found4GiPod = true
				break
			}
		}
		if ciCnt != 4 && !found4GiPod {
			if mm.PlacementEmpty(4, 3, ciCnt) {
				return 4, 3
			}
			if mm.PlacementEmpty(0, 4, ciCnt) {
				return 0, 4
			}
		}
	case utils.MigProfileSize4:
		found3GiPod := false
		for _, podInfo := range pods {
			if utils.GetResourceReq(podInfo.Pod, utils.ResourceSlice) > 3 {
				found3GiPod = true
				break
			} else if utils.GetResourceReq(podInfo.Pod, utils.ResourceSlice) == 3 && !IsDriverAtLeast525(majorVersion) {
				found3GiPod = true
				break
			}
		}
		if !found3GiPod && mm.PlacementEmpty(0, 4, ciCnt) {
			return 0, 4
		}
	case utils.MigProfileSize7:
		if ciCnt != 4 && mm.Empty(ciCnt) {
			return 0, 7
		}
	default:
	}
	return -1, 0
}

// PlacementEmpty checks if a region of 'size' bits starting at 'from' is all zeros within 'ciCnt' bits.
func (mm Mask) PlacementEmpty(from int, size int, ciCnt int) bool {
	if from+size > ciCnt {
		return false
	}
	for i := ciCnt - from - 1; i >= ciCnt-(from+size); i-- {
		p := (mm >> i) & 1
		if p == 1 {
			return false
		}
	}
	return true
}

// Empty returns true if all bits in the first ciCnt positions are zero.
func (mm Mask) Empty(ciCnt int) bool {
	for i := 0; i < ciCnt; i++ {
		p := (mm >> i) & 1
		if p == 1 {
			return false
		}
	}
	return true
}

// Full returns true if all bits in the first ciCnt positions are set.
func (mm Mask) Full(ciCnt int) bool {
	for i := 0; i < ciCnt; i++ {
		p := (mm >> i) & 1
		if p == 0 {
			return false
		}
	}
	return true
}

// FindLastSingleFitBit finds the index of the last unset bit within ciCnt bits, counting from MSB.
// Returns -1 if all bits are set.
func (mm Mask) FindLastSingleFitBit(ciCnt int) int {
	idx := -1
	for i := 0; i < ciCnt; i++ {
		p := (mm >> i) & 1
		if p == 0 {
			idx = i
			break
		}
	}
	if idx == -1 {
		return idx
	}
	return ciCnt - idx - 1
}

// GetLongestMatchedTopo returns the length of the longest consecutive run of zeros.
func (mm Mask) GetLongestMatchedTopo(ciCnt int) int {
	ll := make([]uint8, ciCnt)
	for i := ciCnt - 1; i >= 0; i-- {
		p := (mm >> i) & 1
		ll[i] = uint8(p)
	}
	dp := make([]int, len(ll))
	maxLen := 0
	if ll[0] == 0 {
		maxLen = 1
		dp[0] = 1
	}
	for i := 1; i < len(dp); i++ {
		if ll[i] == 1 {
			dp[i] = 0
		} else {
			dp[i] = dp[i-1] + 1
		}
		if dp[i] > maxLen {
			maxLen = dp[i]
		}
	}
	return maxLen
}

// BuildMask creates a bitmask for MIG CI allocation with the given start position and size.
func BuildMask(ciCnt int, start int, size int) uint8 {
	whole := uint8(0)
	for idx := 0; idx < size; idx++ {
		whole += uint8(math.Pow(float64(2), float64(ciCnt-1-start-idx)))
	}
	return whole
}

// IsDriverAtLeast525 returns true if the major driver version is 525 or higher.
func IsDriverAtLeast525(majorVersion int) bool {
	return majorVersion >= utils.MajorDriverVersion525
}
