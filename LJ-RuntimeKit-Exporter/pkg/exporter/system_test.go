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

import "testing"

func TestCheckGpuInfoChange_NilInputs(t *testing.T) {
	info := &SystemGpuInfo{
		GpuCount: 1,
		Gpus:     []GpuInfo{{GPU: 0, UUID: "gpu-0", Model: "A100", Serial: "sn-0"}},
	}

	if got := checkGpuInfoChange(nil, nil); got != false {
		t.Errorf("checkGpuInfoChange(nil, nil) = %v, want false", got)
	}
	if got := checkGpuInfoChange(info, nil); got != false {
		t.Errorf("checkGpuInfoChange(info, nil) = %v, want false", got)
	}
	if got := checkGpuInfoChange(nil, info); got != false {
		t.Errorf("checkGpuInfoChange(nil, info) = %v, want false", got)
	}
}

func TestCheckGpuInfoChange_SameInfo(t *testing.T) {
	info := &SystemGpuInfo{
		GpuCount: 2,
		Gpus: []GpuInfo{
			{GPU: 0, UUID: "gpu-0", Model: "A100", Serial: "sn-0"},
			{GPU: 1, UUID: "gpu-1", Model: "A100", Serial: "sn-1"},
		},
	}

	if got := checkGpuInfoChange(info, info); got != false {
		t.Errorf("checkGpuInfoChange(info, info) = %v, want false (same pointer, no change)", got)
	}
}

func TestCheckGpuInfoChange_IdenticalCopy(t *testing.T) {
	oldInfo := &SystemGpuInfo{
		GpuCount: 2,
		Gpus: []GpuInfo{
			{GPU: 0, UUID: "gpu-0", Model: "A100", Serial: "sn-0"},
			{GPU: 1, UUID: "gpu-1", Model: "A100", Serial: "sn-1"},
		},
	}
	newInfo := &SystemGpuInfo{
		GpuCount: 2,
		Gpus: []GpuInfo{
			{GPU: 0, UUID: "gpu-0", Model: "A100", Serial: "sn-0"},
			{GPU: 1, UUID: "gpu-1", Model: "A100", Serial: "sn-1"},
		},
	}

	if got := checkGpuInfoChange(oldInfo, newInfo); got != false {
		t.Errorf("checkGpuInfoChange(identical copies) = %v, want false", got)
	}
}

func TestCheckGpuInfoChange_DifferentCount(t *testing.T) {
	oldInfo := &SystemGpuInfo{
		GpuCount: 2,
		Gpus: []GpuInfo{
			{GPU: 0, UUID: "gpu-0", Model: "A100", Serial: "sn-0"},
			{GPU: 1, UUID: "gpu-1", Model: "A100", Serial: "sn-1"},
		},
	}
	newInfo := &SystemGpuInfo{
		GpuCount: 1,
		Gpus:     []GpuInfo{{GPU: 0, UUID: "gpu-0", Model: "A100", Serial: "sn-0"}},
	}

	if got := checkGpuInfoChange(oldInfo, newInfo); got != true {
		t.Errorf("checkGpuInfoChange(different GpuCount) = %v, want true", got)
	}
}

func TestCheckGpuInfoChange_DifferentUUID(t *testing.T) {
	oldInfo := &SystemGpuInfo{
		GpuCount: 1,
		Gpus:     []GpuInfo{{GPU: 0, UUID: "gpu-0-old", Model: "A100", Serial: "sn-0"}},
	}
	newInfo := &SystemGpuInfo{
		GpuCount: 1,
		Gpus:     []GpuInfo{{GPU: 0, UUID: "gpu-0-new", Model: "A100", Serial: "sn-0"}},
	}

	if got := checkGpuInfoChange(oldInfo, newInfo); got != true {
		t.Errorf("checkGpuInfoChange(different UUID for same GPU index) = %v, want true", got)
	}
}

func TestCheckGpuInfoChange_DifferentModel(t *testing.T) {
	oldInfo := &SystemGpuInfo{
		GpuCount: 1,
		Gpus:     []GpuInfo{{GPU: 0, UUID: "gpu-0", Model: "A100", Serial: "sn-0"}},
	}
	newInfo := &SystemGpuInfo{
		GpuCount: 1,
		Gpus:     []GpuInfo{{GPU: 0, UUID: "gpu-0", Model: "V100", Serial: "sn-0"}},
	}

	if got := checkGpuInfoChange(oldInfo, newInfo); got != true {
		t.Errorf("checkGpuInfoChange(different model) = %v, want true", got)
	}
}

func TestCheckGpuInfoChange_GpuRemoved(t *testing.T) {
	oldInfo := &SystemGpuInfo{
		GpuCount: 2,
		Gpus: []GpuInfo{
			{GPU: 0, UUID: "gpu-0", Model: "A100", Serial: "sn-0"},
			{GPU: 1, UUID: "gpu-1", Model: "A100", Serial: "sn-1"},
		},
	}
	newInfo := &SystemGpuInfo{
		GpuCount: 1,
		Gpus:     []GpuInfo{{GPU: 0, UUID: "gpu-0", Model: "A100", Serial: "sn-0"}},
	}

	if got := checkGpuInfoChange(oldInfo, newInfo); got != true {
		t.Errorf("checkGpuInfoChange(GPU index removed) = %v, want true", got)
	}
}

func TestCheckGpuInfoChange_GpuAdded(t *testing.T) {
	oldInfo := &SystemGpuInfo{
		GpuCount: 1,
		Gpus:     []GpuInfo{{GPU: 0, UUID: "gpu-0", Model: "A100", Serial: "sn-0"}},
	}
	newInfo := &SystemGpuInfo{
		GpuCount: 2,
		Gpus: []GpuInfo{
			{GPU: 0, UUID: "gpu-0", Model: "A100", Serial: "sn-0"},
			{GPU: 1, UUID: "gpu-1", Model: "A100", Serial: "sn-1"},
		},
	}

	if got := checkGpuInfoChange(oldInfo, newInfo); got != true {
		t.Errorf("checkGpuInfoChange(GPU index added) = %v, want true", got)
	}
}

func TestCheckGpuInfoChange_DifferentDriver(t *testing.T) {
	oldInfo := &SystemGpuInfo{
		Driver:   "535.104.05",
		GpuCount: 1,
		Gpus:     []GpuInfo{{GPU: 0, UUID: "gpu-0", Model: "A100", Serial: "sn-0"}},
	}
	newInfo := &SystemGpuInfo{
		Driver:   "550.54.15",
		GpuCount: 1,
		Gpus:     []GpuInfo{{GPU: 0, UUID: "gpu-0", Model: "A100", Serial: "sn-0"}},
	}

	if got := checkGpuInfoChange(oldInfo, newInfo); got != false {
		t.Errorf("checkGpuInfoChange(identical GPUs, different driver) = %v, want false (driver not compared)", got)
	}
}
