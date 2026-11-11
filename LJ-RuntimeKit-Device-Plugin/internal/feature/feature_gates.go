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

package feature

import (
	"github.com/lj-runtimekit/device-plugin/internal/utils"

	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/component-base/featuregate"
)

var inited bool

const (
	PluginNvidiaGpu      featuregate.Feature = "PluginNvidia"
	PluginAliYunCore     featuregate.Feature = "PluginCore"
	PluginAliYunMemory   featuregate.Feature = "PluginMemory"
	PluginSlice          featuregate.Feature = "PluginSlice"
	PluginAliPossibility featuregate.Feature = "PluginPossibility"
	PluginGPU            featuregate.Feature = "PluginGPU"
	PluginGpuCount       featuregate.Feature = "PluginGpuCount"
	// CheckpointGC enables the periodic reconciliation of the dp checkpoint
	// records against the pods still present on the node (kubelet 10255):
	// records of deleted pods are dropped and their allocations released in xdm.
	CheckpointGC  featuregate.Feature = "CheckpointGC"
	ComputeDomain featuregate.Feature = "ComputeDomain"
)

var defaultFeatureGates = map[featuregate.Feature]featuregate.FeatureSpec{
	PluginNvidiaGpu:      {Default: true, PreRelease: featuregate.Beta},
	PluginAliYunCore:     {Default: true, PreRelease: featuregate.Beta},
	PluginAliYunMemory:   {Default: true, PreRelease: featuregate.Beta},
	PluginSlice:          {Default: true, PreRelease: featuregate.Beta},
	PluginGPU:            {Default: true, PreRelease: featuregate.Beta},
	PluginAliPossibility: {Default: true, PreRelease: featuregate.Beta},
	PluginGpuCount:       {Default: true, PreRelease: featuregate.Beta},
	CheckpointGC:         {Default: true, PreRelease: featuregate.Alpha},
	ComputeDomain:        {Default: false, PreRelease: featuregate.Alpha},
}

func PluginFeatureConvert(resourceName string) featuregate.Feature {
	switch resourceName {
	case utils.ResourceNvidia:
		return PluginNvidiaGpu
	case utils.ResourceCore:
		return PluginAliYunCore
	case utils.ResourceMemory:
		return PluginAliYunMemory
	case utils.ResourceGPU:
		return PluginGPU
	case utils.ResourceGPUCount:
		return PluginGpuCount
	case utils.ResourceSlice:
		return PluginSlice
	case utils.ResourcePossibility:
		return PluginAliPossibility
	default:
		return PluginNvidiaGpu
	}
}

func init() {
	if !inited {
		runtime.Must(utils.DefaultMutableFeatureGate.Add(defaultFeatureGates))
		inited = true
	}
}
