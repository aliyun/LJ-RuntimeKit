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
	"strconv"
	"testing"

	xpugrpc "github.com/lj-runtimekit/device-plugin/internal/grpc"
	"github.com/lj-runtimekit/device-plugin/internal/utils"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
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
			{DevicesIds: []string{"COMP-0-_0", "COMP-0-_1"}},
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
		DevicesIds: []string{"COMP-0-_0"},
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
		DevicesIds: []string{"COMP-1-_0"},
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
		DevicesIds: []string{"COMP-2-_0"},
	})
	assert.Error(t, err)

	// the claim must be rolled back so the record is claimable again
	record, err := utils.FindRecordByDeviceIDs(ResourceCore, []string{"COMP-2-_0"})
	assert.NoError(t, err)
	assert.Equal(t, utils.CheckpointStatusAllocated, record.Status)

	// the retry must win the claim again (still fails on the missing kubelet
	// checkpoint, proving it was not skipped by the idempotency guard)
	_, err = dp.PreStartContainer(context.Background(), &pluginapi.PreStartContainerRequest{
		DevicesIds: []string{"COMP-2-_0"},
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

// TestSliceExtenderFor verifies the dual-mode gpu-slice contract: a pod with
// rt_mig_enable=true forwards the MIG topology the scheduler resolved into
// its annotations, while a pod without the label is an MPS slice request and
// must carry no Mig* extender at all.
func TestSliceExtenderFor(t *testing.T) {
	migPod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      "mig-pod",
		Namespace: "default",
		Labels:    map[string]string{utils.LabelMigEnable: "true"},
		Annotations: map[string]string{
			utils.AnnotationMigDeviceCIMask:  "3",
			utils.AnnotationMigTopologyStart: "1",
			utils.AnnotationMigTopologySize:  "2",
			utils.AnnotationMigDeviceType:    string(utils.TypeMigGi),
		},
	}}
	extender, err := sliceExtenderFor(migPod)
	assert.NoError(t, err)
	assert.Equal(t, map[string]string{
		utils.AllocateMigMask:  "3",
		utils.AllocateMigType:  strconv.Itoa(utils.MigType2Mode(utils.TypeMigGi)),
		utils.AllocateMigSize:  "2",
		utils.AllocateMigStart: "1",
	}, extender)

	// without the label the same gpu-slice request means MPS compute slices:
	// xdm only books the capacity, so no Mig* key may be forwarded
	mpsPod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "mps-pod", Namespace: "default"}}
	extender, err = sliceExtenderFor(mpsPod)
	assert.NoError(t, err)
	assert.Empty(t, extender)

	// a MIG pod missing the topology annotations must fail loudly instead of
	// silently degrading into an MPS allocation
	incompletePod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      "mig-pod-incomplete",
		Namespace: "default",
		Labels:    map[string]string{utils.LabelMigEnable: "true"},
	}}
	_, err = sliceExtenderFor(incompletePod)
	assert.Error(t, err)
}

// TestSliceFactorFor 验证 Factor 由 MIG 模式与设备实际携带的 Slice 共同决定。
func TestSliceFactorFor(t *testing.T) {
	for _, tc := range []struct {
		name       string
		migLabel   string
		device     *xpugrpc.AllocatedDevices
		wantFactor bool
	}{
		{name: "MIG 单 slice 设置 60", migLabel: "true", device: &xpugrpc.AllocatedDevices{Slice: wrapperspb.UInt32(1)}, wantFactor: true},
		{name: "MIG 多 slice 仍设置 60", migLabel: "true", device: &xpugrpc.AllocatedDevices{Slice: wrapperspb.UInt32(4)}, wantFactor: true},
		{name: "MIG 仅传 minor 不设置", migLabel: "true", device: &xpugrpc.AllocatedDevices{Minor: wrapperspb.UInt32(0)}},
		{name: "MIG core 维度不设置", migLabel: "true", device: &xpugrpc.AllocatedDevices{Core: wrapperspb.UInt32(50)}},
		{name: "MIG memory 维度不设置", migLabel: "true", device: &xpugrpc.AllocatedDevices{Memory: wrapperspb.UInt32(1024)}},
		{name: "MIG possibility 维度不设置", migLabel: "true", device: &xpugrpc.AllocatedDevices{Possibility: wrapperspb.UInt32(1)}},
		{name: "无 MIG 标签的 MPS slice 不设置", device: &xpugrpc.AllocatedDevices{Slice: wrapperspb.UInt32(1)}},
		{name: "关闭 MIG 的 MPS slice 不设置", migLabel: "false", device: &xpugrpc.AllocatedDevices{Slice: wrapperspb.UInt32(1)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := &v1.Pod{Spec: v1.PodSpec{Containers: []v1.Container{{
				Resources: v1.ResourceRequirements{Limits: v1.ResourceList{
					v1.ResourceName(utils.ResourceSlice): resource.MustParse("1"),
				}},
			}}}}
			if tc.migLabel != "" {
				pod.Labels = map[string]string{utils.LabelMigEnable: tc.migLabel}
			}
			// 即使 Pod 申请了 slice，仅传 minor 或其他维度的设备也不能填 Factor。
			tc.device.Factor = sliceFactorFor(pod, tc.device)
			if tc.wantFactor {
				if assert.NotNil(t, tc.device.Factor) {
					assert.Equal(t, uint32(60), tc.device.Factor.Value)
				}
			} else {
				assert.Nil(t, tc.device.Factor)
			}

			// 验证协议编码后仍区分未设置 Factor 和显式设置为 60。
			data, err := proto.Marshal(tc.device)
			if !assert.NoError(t, err) {
				return
			}
			var decoded xpugrpc.AllocatedDevices
			if !assert.NoError(t, proto.Unmarshal(data, &decoded)) {
				return
			}
			assert.True(t, proto.Equal(tc.device, &decoded))
		})
	}
}

// TestPcieSwitchIndexFor 验证整卡资源透传 RDMA 索引，共享 core 和其他维度不透传。
func TestPcieSwitchIndexFor(t *testing.T) {
	for _, tc := range []struct {
		name         string
		resourceName string
		core         string
		pcieIndex    string
		want         string
	}{
		{name: "aliyun GPU 透传索引", resourceName: utils.ResourceGPU, pcieIndex: "0,1", want: "0,1"},
		{name: "nvidia GPU 透传索引", resourceName: utils.ResourceNvidia, pcieIndex: "1", want: "1"},
		{name: "core 单卡透传索引", resourceName: utils.ResourceCore, core: "100", pcieIndex: "0", want: "0"},
		{name: "core 多卡透传索引", resourceName: utils.ResourceCore, core: "200", pcieIndex: " 0, 1 ", want: "0,1"},
		{name: "非整倍数沿用整卡判定", resourceName: utils.ResourceCore, core: "150", pcieIndex: "1", want: "1"},
		{name: "共享 core 不透传", resourceName: utils.ResourceCore, core: "50", pcieIndex: "0"},
		{name: "core 未达整卡阈值不透传", resourceName: utils.ResourceCore, core: "99", pcieIndex: "0"},
		{name: "无 core 请求不透传", resourceName: utils.ResourceCore, pcieIndex: "0"},
		{name: "core 整卡无索引不透传", resourceName: utils.ResourceCore, core: "100"},
		{name: "core 整卡空白索引不透传", resourceName: utils.ResourceCore, core: "100", pcieIndex: " , "},
		{name: "core 整卡非法索引不透传", resourceName: utils.ResourceCore, core: "100", pcieIndex: "invalid"},
		{name: "core 整卡过滤非法索引", resourceName: utils.ResourceCore, core: "100", pcieIndex: "0,invalid, 1", want: "0,1"},
		{name: "memory 维度不因整卡 core 透传", resourceName: utils.ResourceMemory, core: "100", pcieIndex: "0"},
		{name: "slice 维度不因整卡 core 透传", resourceName: utils.ResourceSlice, core: "100", pcieIndex: "0"},
		{name: "possibility 维度不因整卡 core 透传", resourceName: utils.ResourcePossibility, core: "100", pcieIndex: "0"},
		{name: "RDMA token 维度不透传", resourceName: utils.ResourceRDMA, core: "100", pcieIndex: "0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := &v1.Pod{}
			if tc.pcieIndex != "" {
				pod.Annotations = map[string]string{utils.AnnotationPcieSwitchIndex: tc.pcieIndex}
			}
			if tc.core != "" {
				pod.Spec.Containers = []v1.Container{{
					Resources: v1.ResourceRequirements{Limits: v1.ResourceList{
						v1.ResourceName(utils.ResourceCore): resource.MustParse(tc.core),
					}},
				}}
			}
			dp := &XpuDevicePlugin{ResourceName: tc.resourceName}
			assert.Equal(t, tc.want, dp.pcieSwitchIndexFor(pod))
		})
	}
}

// TestFeatureGatesFor verifies profiling is enabled only by
// rt-profiling-enable=true.
func TestFeatureGatesFor(t *testing.T) {
	for _, tc := range []struct {
		name   string
		labels map[string]string
		want   map[int32]bool
	}{
		{name: "label true enables profiling", labels: map[string]string{utils.LabelProfilingEnable: "true"}, want: map[int32]bool{int32(xpugrpc.FeatureFlag_TroubleShooting): true}},
		{name: "label false leaves gates empty", labels: map[string]string{utils.LabelProfilingEnable: "false"}},
		{name: "no label leaves gates empty"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: tc.labels}}
			assert.Equal(t, tc.want, featureGatesFor(pod))
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
