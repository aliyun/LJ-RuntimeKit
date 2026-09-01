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

package plugin

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/lj-runtimekit/device-plugin/internal/utils"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"

	"github.com/stretchr/testify/assert"
)

// Test constants (ensure these match your utils package)
const (
	ResourceCore                    = "aliyun.com/gpu-core"
	ResourceMemory                  = "aliyun.com/gpu-memory"
	AnnotationNormalGpuAssignedFlag = "ALIYUN_COM_GPU_ASSIGNED"
	AssignedFlagCore                = "ALIYUN_COM_GPU_CORE_ASSIGNED"
	AssignedFlagMemory              = "ALIYUN_COM_GPU_MEM_ASSIGNED"
	AnnotationDeviceIndex           = "ALIYUN_COM_GPU_IDX_LIST"
	EnvNvGPU                        = "NVIDIA_VISIBLE_DEVICES"
	EnvXpuGPU                       = "XPU_VISIBLE_DEVICES"
)

// TestBuildErrResponse tests error response construction
func TestBuildErrResponse(t *testing.T) {
	dp := &XpuDevicePlugin{
		ResourceName: "gpu",
	}
	req := &pluginapi.AllocateRequest{
		ContainerRequests: []*pluginapi.ContainerAllocateRequest{
			{}, {}, // two containers
		},
	}

	resp := dp.buildErrResponse(req, 2)
	assert.Len(t, resp.ContainerResponses, 2)
	for _, cr := range resp.ContainerResponses {
		assert.Contains(t, cr.Envs, EnvNvGPU)
		assert.Contains(t, cr.Envs, EnvXpuGPU)
		assert.Equal(t, cr.Envs[EnvNvGPU], cr.Envs[EnvXpuGPU])
		assert.Contains(t, cr.Envs[EnvNvGPU], "no-gpu-has-2gpu-to-run")
	}
}

// TestAllocateRecordsCheckpoint verifies Allocate records the mapping in the
// dp checkpoint, returns the allocateID annotation and defers xdm allocate.
func TestAllocateRecordsCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()
	origPath := utils.DpCheckpointPath
	utils.DpCheckpointPath = filepath.Join(tmpDir, "deviceplugin_internal_checkpoint")
	defer func() { utils.DpCheckpointPath = origPath }()

	dp := &XpuDevicePlugin{
		ResourceName: ResourceCore,
		AssignedFlag: AssignedFlagCore,
	}
	req := &pluginapi.AllocateRequest{
		ContainerRequests: []*pluginapi.ContainerAllocateRequest{
			{DevicesIDs: []string{"COMP-0-_0", "COMP-0-_1"}},
		},
	}

	resp, err := dp.Allocate(context.Background(), req)
	assert.NoError(t, err)
	assert.Len(t, resp.ContainerResponses, 1)

	allocateID := resp.ContainerResponses[0].Annotations[utils.AnnotationAllocateID]
	assert.NotEmpty(t, allocateID)

	// the device dir is owned by xdm, the dp only returns the allocate id
	assert.Empty(t, resp.ContainerResponses[0].Mounts)

	// the record should be persisted and matchable by the kubelet device uuids
	record, err := utils.FindRecordByDeviceIDs(ResourceCore, []string{"COMP-0-_0", "COMP-0-_1"})
	assert.NoError(t, err)
	assert.Equal(t, allocateID, record.AllocateID)
	assert.Equal(t, utils.CheckpointStatusAllocated, record.Status)
	assert.Empty(t, record.PodUID)
}

// TestAllocateWithoutDeviceIDs verifies that Allocate fails gracefully when
// kubelet sends no device ids.
func TestAllocateWithoutDeviceIDs(t *testing.T) {
	tmpDir := t.TempDir()
	origPath := utils.DpCheckpointPath
	utils.DpCheckpointPath = filepath.Join(tmpDir, "deviceplugin_internal_checkpoint")
	defer func() { utils.DpCheckpointPath = origPath }()

	dp := &XpuDevicePlugin{
		ResourceName: ResourceCore,
		AssignedFlag: AssignedFlagCore,
	}
	req := &pluginapi.AllocateRequest{
		ContainerRequests: []*pluginapi.ContainerAllocateRequest{
			{},
		},
	}

	resp, err := dp.Allocate(context.Background(), req)
	assert.NoError(t, err)
	assert.Len(t, resp.ContainerResponses, 1)
	// error response injects the no-gpu envs instead of annotations
	assert.Contains(t, resp.ContainerResponses[0].Envs, EnvNvGPU)
	assert.Contains(t, resp.ContainerResponses[0].Envs, EnvXpuGPU)
}

// TestPreStartContainerIdempotentSkip verifies prestart/done records are
// skipped immediately, even when the kubelet checkpoint is missing.
func TestPreStartContainerIdempotentSkip(t *testing.T) {
	tmpDir := t.TempDir()
	origDpPath := utils.DpCheckpointPath
	utils.DpCheckpointPath = filepath.Join(tmpDir, "deviceplugin_internal_checkpoint")
	defer func() { utils.DpCheckpointPath = origDpPath }()
	origKubeletPath := utils.KubeletCheckpointPath
	utils.KubeletCheckpointPath = filepath.Join(tmpDir, "missing_kubelet_checkpoint")
	defer func() { utils.KubeletCheckpointPath = origKubeletPath }()

	dp := &XpuDevicePlugin{
		ResourceName: ResourceCore,
		AssignedFlag: AssignedFlagCore,
	}

	// record already finished by a previous PreStartContainer invocation
	assert.NoError(t, utils.SaveAllocateRecord(&utils.AllocateRecord{
		AllocateID:   "alloc-done",
		ResourceName: ResourceCore,
		DeviceIDs:    []string{"COMP-0-_0"},
		Status:       utils.CheckpointStatusDone,
	}))
	resp, err := dp.PreStartContainer(context.Background(), &pluginapi.PreStartContainerRequest{
		DevicesIDs: []string{"COMP-0-_0"},
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp)

	// record claimed by a concurrent/earlier invocation still in progress
	assert.NoError(t, utils.SaveAllocateRecord(&utils.AllocateRecord{
		AllocateID:   "alloc-inflight",
		ResourceName: ResourceCore,
		DeviceIDs:    []string{"COMP-1-_0"},
		Status:       utils.CheckpointStatusPreStart,
	}))
	resp, err = dp.PreStartContainer(context.Background(), &pluginapi.PreStartContainerRequest{
		DevicesIDs: []string{"COMP-1-_0"},
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
}

// TestPreStartContainerRollbackOnFailure verifies a pre-start failure before
// xdm allocate rolls the claim back so kubelet can retry.
func TestPreStartContainerRollbackOnFailure(t *testing.T) {
	tmpDir := t.TempDir()
	origDpPath := utils.DpCheckpointPath
	utils.DpCheckpointPath = filepath.Join(tmpDir, "deviceplugin_internal_checkpoint")
	defer func() { utils.DpCheckpointPath = origDpPath }()
	origKubeletPath := utils.KubeletCheckpointPath
	utils.KubeletCheckpointPath = filepath.Join(tmpDir, "missing_kubelet_checkpoint")
	defer func() { utils.KubeletCheckpointPath = origKubeletPath }()

	dp := &XpuDevicePlugin{
		ResourceName: ResourceCore,
		AssignedFlag: AssignedFlagCore,
	}

	assert.NoError(t, utils.SaveAllocateRecord(&utils.AllocateRecord{
		AllocateID:   "alloc-retry",
		ResourceName: ResourceCore,
		DeviceIDs:    []string{"COMP-2-_0"},
		Status:       utils.CheckpointStatusAllocated,
	}))

	// first invocation fails while reading the kubelet checkpoint
	_, err := dp.PreStartContainer(context.Background(), &pluginapi.PreStartContainerRequest{
		DevicesIDs: []string{"COMP-2-_0"},
	})
	assert.Error(t, err)

	// the claim must be rolled back so the record is claimable again
	record, err := utils.FindRecordByDeviceIDs(ResourceCore, []string{"COMP-2-_0"})
	assert.NoError(t, err)
	assert.Equal(t, utils.CheckpointStatusAllocated, record.Status)

	// the retry must win the claim again (still fails on the missing kubelet
	// checkpoint, proving it was not skipped by the idempotency guard)
	_, err = dp.PreStartContainer(context.Background(), &pluginapi.PreStartContainerRequest{
		DevicesIDs: []string{"COMP-2-_0"},
	})
	assert.Error(t, err)
	record, err = utils.FindRecordByDeviceIDs(ResourceCore, []string{"COMP-2-_0"})
	assert.NoError(t, err)
	assert.Equal(t, utils.CheckpointStatusAllocated, record.Status)
}

// TestPatchPodAnnotations tests annotation patch generation
func TestPatchPodAnnotations(t *testing.T) {
	dp := &XpuDevicePlugin{}
	patchedBytes, err := dp.patchPodAnnotations(nil, "flag1", "flag2")
	assert.NoError(t, err)

	var patch map[string]interface{}
	err = json.Unmarshal(patchedBytes, &patch)
	assert.NoError(t, err)

	annotations := patch["metadata"].(map[string]interface{})["annotations"].(map[string]interface{})
	assert.Equal(t, "true", annotations["flag1"])
	assert.Equal(t, "true", annotations["flag2"])
}

// TestAssignedFlagFor verifies a dimension checks its own flag, except whole
// card core requests which check the whole card flag.
func TestAssignedFlagFor(t *testing.T) {
	corePod := func(core string) *v1.Pod {
		return &v1.Pod{
			Spec: v1.PodSpec{Containers: []v1.Container{{
				Resources: v1.ResourceRequirements{Limits: v1.ResourceList{
					v1.ResourceName(ResourceCore): resource.MustParse(core),
				}},
			}}},
		}
	}
	for _, tc := range []struct {
		name         string
		resourceName string
		assignedFlag string
		core         string
		expected     string
	}{
		{name: "shared core keeps its own flag", resourceName: ResourceCore, assignedFlag: AssignedFlagCore, core: "50", expected: AssignedFlagCore},
		{name: "whole card core uses the whole card flag", resourceName: ResourceCore, assignedFlag: AssignedFlagCore, core: "100", expected: AnnotationNormalGpuAssignedFlag},
		{name: "invalid whole card core amount uses the whole card flag", resourceName: ResourceCore, assignedFlag: AssignedFlagCore, core: "150", expected: AnnotationNormalGpuAssignedFlag},
		{name: "memory dimension keeps its own flag even with whole card core", resourceName: ResourceMemory, assignedFlag: AssignedFlagMemory, core: "100", expected: AssignedFlagMemory},
		{name: "whole card plugin keeps its whole card flag", resourceName: "aliyun.com/gpu", assignedFlag: AnnotationNormalGpuAssignedFlag, core: "", expected: AnnotationNormalGpuAssignedFlag},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dp := &XpuDevicePlugin{ResourceName: tc.resourceName, AssignedFlag: tc.assignedFlag}
			pod := &v1.Pod{}
			if tc.core != "" {
				pod = corePod(tc.core)
			}
			assert.Equal(t, tc.expected, dp.assignedFlagFor(pod))
		})
	}
}

// TestXdmCallMode verifies the call contract: unflagged lj dimensions carry
// minors only and stay unflagged; flagged dimensions and community pods carry
// amounts and flip the flag. Whole card core checks the whole card flag.
func TestXdmCallMode(t *testing.T) {
	corePod := func(core string, annotations map[string]string) *v1.Pod {
		return &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Annotations: annotations},
			Spec: v1.PodSpec{Containers: []v1.Container{{
				Resources: v1.ResourceRequirements{Limits: v1.ResourceList{
					v1.ResourceName(ResourceCore): resource.MustParse(core),
				}},
			}}},
		}
	}
	lj := map[string]string{AnnotationDeviceIndex: "0"}
	withFlag := func(flag string) map[string]string {
		return map[string]string{AnnotationDeviceIndex: "0", flag: "false"}
	}
	for _, tc := range []struct {
		name             string
		pod              *v1.Pod
		wantAssignedFlag string
		wantMinorsOnly   bool
		wantFlagged      bool
	}{
		{
			name:             "community pod allocates with amounts and flips its own flag",
			pod:              corePod("50", nil),
			wantAssignedFlag: AssignedFlagCore,
			wantMinorsOnly:   false,
			wantFlagged:      true,
		},
		{
			name:             "lj shared core flagged by the scheduler",
			pod:              corePod("50", withFlag(AssignedFlagCore)),
			wantAssignedFlag: AssignedFlagCore,
			wantMinorsOnly:   false,
			wantFlagged:      true,
		},
		{
			name:             "lj core dimension left unflagged calls minors only",
			pod:              corePod("50", lj),
			wantAssignedFlag: AssignedFlagCore,
			wantMinorsOnly:   true,
			wantFlagged:      false,
		},
		{
			name:             "lj whole card core with the whole card flag",
			pod:              corePod("100", withFlag(AnnotationNormalGpuAssignedFlag)),
			wantAssignedFlag: AnnotationNormalGpuAssignedFlag,
			wantMinorsOnly:   true,
			wantFlagged:      true,
		},
		{
			name:             "lj whole card core without the whole card flag calls minors only",
			pod:              corePod("100", lj),
			wantAssignedFlag: AnnotationNormalGpuAssignedFlag,
			wantMinorsOnly:   true,
			wantFlagged:      false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dp := &XpuDevicePlugin{ResourceName: ResourceCore, AssignedFlag: AssignedFlagCore}
			assignedFlag, minorsOnly, flagged := dp.xdmCallMode(tc.pod)
			assert.Equal(t, tc.wantAssignedFlag, assignedFlag)
			assert.Equal(t, tc.wantMinorsOnly, minorsOnly)
			assert.Equal(t, tc.wantFlagged, flagged)
		})
	}
}
