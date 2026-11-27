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
	"sort"

	"github.com/lj-runtimekit/scheduler/pkg/utils"
)

// gpuResources holds the memory and compute resources of a GPU device.
type gpuResources struct {
	memory  int64
	compute int64
}

// staffGPU represents a GPU device candidate for scheduling with its available and total resources.
// For time-sharing tasks "compute" is gpu-core, while for MPS tasks it is gpu-slice.
type staffGPU struct {
	devId     int
	available *gpuResources
	total     *gpuResources
}

// getResourceRatio calculates the weighted utilization ratio for binpack/spread decisions.
func (s staffGPU) getResourceRatio() int64 {
	if s.total.compute > 0 && s.total.memory > 0 {
		return 50*s.available.memory/s.total.memory + 50*s.available.compute/s.total.compute
	}

	if s.total.memory > 0 {
		return 100 * s.available.memory / s.total.memory
	}

	return 0
}

// sortStaffGPUs orders GPU candidates for the given strategy. BINPACK picks the
// least free devices and SPREAD the most free ones. The same rule applies to every
// candidate, including the devices that can already run MPS tasks.
func sortStaffGPUs(list []staffGPU, strategy string) {
	binpack := strategy == utils.ScheduleStrategyBinpack
	sort.SliceStable(list, func(i, j int) bool {
		if binpack {
			return list[i].getResourceRatio() < list[j].getResourceRatio()
		}
		return list[i].getResourceRatio() > list[j].getResourceRatio()
	})
}
