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
	"github.com/lj-runtimekit/scheduler/pkg/utils"

	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// StandardTaskReq represents the resource request parsed from a pod during PreFilter.
type StandardTaskReq struct {
	// Vmem is the requested virtual GPU memory.
	Vmem int
	// Vcore is the requested virtual GPU compute (cores).
	Vcore int
	// Vslice is the requested MIG slice count.
	Vslice int
	// VReplicas is the number of GPU replicas required.
	VReplicas int
	// NormalGpuReqCnt is the number of whole GPU cards requested.
	NormalGpuReqCnt int
	// TaskMode is the scheduling mode of the pod (MPS, time-share, normal virtual, or unknown).
	TaskMode utils.TaskMode
	// AoaReplicas maps node names to required AOA replica counts.
	AoaReplicas map[string]int
}

// Clone returns a deep copy of the StandardTaskReq.
func (d StandardTaskReq) Clone() framework.StateData {
	target := StandardTaskReq{
		Vmem:            d.Vmem,
		Vcore:           d.Vcore,
		Vslice:          d.Vslice,
		VReplicas:       d.VReplicas,
		NormalGpuReqCnt: d.NormalGpuReqCnt,
		TaskMode:        d.TaskMode,
		AoaReplicas:     make(map[string]int, len(d.AoaReplicas)),
	}
	for nodeName, replicas := range d.AoaReplicas {
		target.AoaReplicas[nodeName] = replicas
	}
	return target
}
