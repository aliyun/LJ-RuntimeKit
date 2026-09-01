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

// gpuResources holds the memory and compute resources of a GPU device.
type gpuResources struct {
	memory  int64
	compute int64
}

// staffGPU represents a GPU device candidate for scheduling with its available and total resources.
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

// resourceList sorts staff GPU candidates by ascending resource ratio (for binpack).
type resourceList []staffGPU

func (l resourceList) Len() int           { return len(l) }
func (l resourceList) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }
func (l resourceList) Less(i, j int) bool { return l[i].getResourceRatio() < l[j].getResourceRatio() }

// reverseResourceList sorts staff GPU candidates by descending resource ratio (for spread).
type reverseResourceList []staffGPU

func (l reverseResourceList) Len() int      { return len(l) }
func (l reverseResourceList) Swap(i, j int) { l[i], l[j] = l[j], l[i] }
func (l reverseResourceList) Less(i, j int) bool {
	return l[i].getResourceRatio() > l[j].getResourceRatio()
}
