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
	"fmt"
	"strconv"
	"strings"
	"time"

	xpugrpc "github.com/lj-runtimekit/device-plugin/internal/grpc"
	"github.com/lj-runtimekit/device-plugin/internal/utils"

	"google.golang.org/protobuf/types/known/wrapperspb"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

// Allocate records the allocateID <-> device uuids mapping into the dp
// checkpoint and returns the allocateID as a container annotation; the xdm
// allocate call is deferred to PreStartContainer.
func (dp *XpuDevicePlugin) Allocate(ctx context.Context, reqs *pluginapi.AllocateRequest) (*pluginapi.AllocateResponse, error) {
	klog.Infof("allocating resource %s started", dp.ResourceName)

	// rdma devices are mounted by xdm itself, skip the whole flow here
	if dp.ResourceName == utils.ResourceRDMA {
		klog.Infof("rdma allocate returns an empty response, rdma devices are mounted by xdm")
		responses := &pluginapi.AllocateResponse{}
		for range reqs.ContainerRequests {
			responses.ContainerResponses = append(responses.ContainerResponses, &pluginapi.ContainerAllocateResponse{})
		}
		return responses, nil
	}

	allDeviceIDs := make([]string, 0)
	for _, cntReq := range reqs.ContainerRequests {
		klog.Infof("container request: %v", cntReq)
		allDeviceIDs = append(allDeviceIDs, cntReq.DevicesIds...)
	}
	if len(allDeviceIDs) == 0 {
		klog.Errorf("no device ids in allocate request for resource %s", dp.ResourceName)
		return dp.buildErrResponse(reqs, 0), nil
	}

	allocateID := utils.GenerateAllocateID()
	record := &utils.AllocateRecord{
		AllocateID:   allocateID,
		ResourceName: dp.ResourceName,
		DeviceIDs:    allDeviceIDs,
		Status:       utils.CheckpointStatusAllocated,
		CreateTime:   time.Now().UnixNano(),
	}
	if err := utils.SaveAllocateRecord(record); err != nil {
		klog.Errorf("save allocate record for resource %s failed: %v", dp.ResourceName, err)
		return dp.buildErrResponse(reqs, len(allDeviceIDs)), nil
	}

	responses := pluginapi.AllocateResponse{}
	// only the allocateID is handed back, the pod identity is unknown here
	for range reqs.ContainerRequests {
		responses.ContainerResponses = append(responses.ContainerResponses, &pluginapi.ContainerAllocateResponse{
			Annotations: map[string]string{
				utils.AnnotationAllocateID: allocateID,
			},
		})
	}

	klog.Infof("allocate resource %s recorded allocateID %s with device ids %v, xdm allocate deferred to pre-start container stage",
		dp.ResourceName, allocateID, allDeviceIDs)
	return &responses, nil
}

// PreStartContainer resolves the pod identity, records the allocateID <-> pod
// mapping for xdm, and performs the xdm allocate call.
func (dp *XpuDevicePlugin) PreStartContainer(ctx context.Context, req *pluginapi.PreStartContainerRequest) (*pluginapi.PreStartContainerResponse, error) {
	deviceIDs := req.DevicesIds
	if len(deviceIDs) == 0 {
		return &pluginapi.PreStartContainerResponse{}, nil
	}
	// rdma has no allocate record, nothing to do
	if dp.ResourceName == utils.ResourceRDMA {
		klog.Infof("pre-start container skipped for resource %s, rdma devices are mounted by xdm", dp.ResourceName)
		return &pluginapi.PreStartContainerResponse{}, nil
	}
	klog.Infof("pre-start container for resource %s with device ids %v", dp.ResourceName, deviceIDs)

	// 1. find the allocate record written in the Allocate stage
	record, err := utils.FindRecordByDeviceIDs(dp.ResourceName, deviceIDs)
	if err != nil {
		klog.Errorf("find allocate record for resource %s failed: %v", dp.ResourceName, err)
		return nil, fmt.Errorf("find allocate record for resource %s failed: %w", dp.ResourceName, err)
	}

	// 2. claim allocated -> prestart so the xdm allocate runs at most once
	acquired, err := utils.TryMarkRecordStatus(record.AllocateID, utils.CheckpointStatusAllocated, utils.CheckpointStatusPreStart)
	if err != nil {
		klog.Errorf("claim allocate record %s failed: %v", record.AllocateID, err)
		return nil, fmt.Errorf("claim allocate record %s failed: %w", record.AllocateID, err)
	}
	if !acquired {
		klog.Infof("pre-start container for resource %s skipped xdm allocate, allocate record %s already claimed or done", dp.ResourceName, record.AllocateID)
		return &pluginapi.PreStartContainerResponse{}, nil
	}

	// roll the claim back if the flow fails before the xdm call, so kubelet
	// retries can re-run it
	xdmAllocated := false
	defer func() {
		if !xdmAllocated {
			rolledBack, err := utils.TryMarkRecordStatus(record.AllocateID, utils.CheckpointStatusPreStart, utils.CheckpointStatusAllocated)
			if err != nil {
				klog.Errorf("roll back allocate record %s failed: %v", record.AllocateID, err)
			} else if rolledBack {
				klog.Infof("allocate record %s rolled back to %s for retry", record.AllocateID, utils.CheckpointStatusAllocated)
			}
		}
	}()

	// 3. read kubelet checkpoint file and match pod identity by device uuids
	podEntry, err := utils.FindPodInKubeletCheckpoint(dp.ResourceName, deviceIDs)
	if err != nil {
		klog.Errorf("find pod in kubelet checkpoint failed: %v", err)
		return nil, fmt.Errorf("find pod in kubelet checkpoint failed: %w", err)
	}
	klog.Infof("pre-start container matched pod %s/%s(%s)", podEntry.PodNamespace, podEntry.PodName, podEntry.PodUID)

	// 4. fetch the pod: annotations carry the xdm allocate parameters
	pod, err := utils.GetPodByUID(dp.K8sClient, podEntry.PodUID)
	if err != nil {
		klog.Errorf("resolve pod by uid %s failed: %v", podEntry.PodUID, err)
		return nil, fmt.Errorf("resolve pod by uid %s failed: %w", podEntry.PodUID, err)
	}
	podEntry.PodNamespace, podEntry.PodName = pod.Namespace, pod.Name
	klog.Infof("pre-start container resolved pod %s/%s by uid %s", pod.Namespace, pod.Name, podEntry.PodUID)

	// 5. record the allocateID <-> pod mapping for xdm to consume
	if err := utils.UpdateAllocateRecord(record.AllocateID, func(r *utils.AllocateRecord) {
		r.PodUID = podEntry.PodUID
		r.PodNamespace = podEntry.PodNamespace
		r.PodName = podEntry.PodName
	}); err != nil {
		klog.Errorf("update allocate record %s failed: %v", record.AllocateID, err)
	}

	// 6. call xdm: every dimension registers its allocateID there, xdmCallMode
	// decides whether the call carries capacity amounts
	podResourceReq := int(utils.GetPodResource(pod, dp.ResourceName))
	assignedFlag, minorsOnly, flagged := dp.xdmCallMode(pod)
	klog.Infof("pre-start container of resource %s for pod %s/%s calls xdm with assigned flag %s, minors only: %v, flag to patch: %v",
		dp.ResourceName, pod.Namespace, pod.Name, assignedFlag, minorsOnly, flagged)
	xdmResp, err := dp.xdmAllocate(ctx, pod, podResourceReq, deviceIDs, record.AllocateID, minorsOnly)
	if err != nil {
		klog.Errorf("xdm allocate for pod %s/%s failed: %v", pod.Namespace, pod.Name, err)
		return nil, fmt.Errorf("xdm allocate for pod %s/%s failed: %w", pod.Namespace, pod.Name, err)
	}
	// past the xdm call the claim is kept, even if later steps fail
	xdmAllocated = true

	// 7. patch the assigned flag (and mig uuid) back to the pod
	if flagged {
		var migUuids []string
		if dp.ResourceName == utils.ResourceSlice {
			if uuid, ok := xdmResp.GetAnnotations()[utils.AnnotationMigDeviceCiUUID]; ok {
				migUuids = append(migUuids, uuid)
			}
		}
		if err := dp.applyPodPatch(assignedFlag, migUuids, pod); err != nil {
			klog.Errorf("apply pod patch for pod %s/%s failed: %v", pod.Namespace, pod.Name, err)
			return nil, fmt.Errorf("apply pod patch failed: %w", err)
		}
	}

	// 8. mark the allocate record done
	if err := utils.UpdateAllocateRecord(record.AllocateID, func(r *utils.AllocateRecord) {
		r.Status = utils.CheckpointStatusDone
	}); err != nil {
		klog.Errorf("mark allocate record %s done failed: %v", record.AllocateID, err)
	}

	klog.Infof("pre-start container for resource %s of pod %s/%s ended", dp.ResourceName, pod.Namespace, pod.Name)
	return &pluginapi.PreStartContainerResponse{}, nil
}

// assignedFlagFor returns the assigned flag annotation this dimension checks.
// Whole card core requests are flagged with the whole card flag instead of the
// per-resource core flag.
func (dp *XpuDevicePlugin) assignedFlagFor(pod *v1.Pod) string {
	if dp.ResourceName == utils.ResourceCore && utils.IsWholeCardPod(pod) {
		return utils.AnnotationNormalGpuAssignedFlag
	}
	return dp.AssignedFlag
}

// xdmCallMode resolves this dimension's xdm call contract. Unflagged lj
// dimensions carry only device minors; others carry amounts and flip the flag.
// Whole-card core requests carry only minors too, because the whole-card
// assignment already implies full capacity.
func (dp *XpuDevicePlugin) xdmCallMode(pod *v1.Pod) (assignedFlag string, minorsOnly bool, flagged bool) {
	assignedFlag = dp.assignedFlagFor(pod)
	if utils.ScheduledByLjScheduler(pod) {
		if _, ok := pod.Annotations[assignedFlag]; !ok {
			return assignedFlag, true, false
		}
		if dp.ResourceName == utils.ResourceCore && utils.IsWholeCardPod(pod) {
			return assignedFlag, true, true
		}
	}
	return assignedFlag, false, true
}

func (dp *XpuDevicePlugin) applyPodPatch(assignedFlag string, migUuids []string, selectedPod *v1.Pod) error {
	patchedBytes, err := dp.patchPodAnnotations(migUuids, assignedFlag)
	if err != nil {
		return fmt.Errorf("failed to generate patch bytes: %w", err)
	}

	if err := utils.RetryOnConflictOrTooManyRequests(func() error {
		_, err := dp.K8sClient.CoreV1().Pods(selectedPod.Namespace).Patch(
			context.TODO(),
			selectedPod.Name,
			types.StrategicMergePatchType,
			patchedBytes,
			metav1.PatchOptions{},
		)
		return err
	}); err != nil {
		return fmt.Errorf("failed to patch pod %s/%s: %w", selectedPod.Namespace, selectedPod.Name, err)
	}
	return nil
}

// patchPodAnnotations builds the strategic merge patch flipping the assigned
// flags of the pod to true.
func (dp *XpuDevicePlugin) patchPodAnnotations(migUuid []string, assignedFlags ...string) ([]byte, error) {
	annotations := make(map[string]string)
	for _, assignedFlag := range assignedFlags {
		annotations[assignedFlag] = "true"
	}

	if len(migUuid) != 0 {
		annotations[utils.AnnotationMigDeviceCiUUID] = strings.Join(migUuid, ",")
	}

	patchAnnotations := map[string]interface{}{
		"metadata": map[string]map[string]string{"annotations": annotations},
	}
	return json.Marshal(patchAnnotations)
}

func (dp *XpuDevicePlugin) buildErrResponse(reqs *pluginapi.AllocateRequest, podReqGPU int) *pluginapi.AllocateResponse {
	responses := pluginapi.AllocateResponse{}
	for _, _ = range reqs.ContainerRequests {
		noGPUValue := fmt.Sprintf("no-gpu-has-%d%s-to-run", podReqGPU, dp.ResourceName)
		response := pluginapi.ContainerAllocateResponse{
			Envs: map[string]string{
				utils.EnvNvGPU:  noGPUValue,
				utils.EnvXpuGPU: noGPUValue,
			},
		}
		responses.ContainerResponses = append(responses.ContainerResponses, &response)
	}
	return &responses
}

// xdmAllocate performs the xdm gRPC allocate call; minorsOnly carries only
// device minors, without capacity amounts.
func (dp *XpuDevicePlugin) xdmAllocate(ctx context.Context, selectedPod *v1.Pod, podResourceReq int, kubeletDeviceIDs []string, allocateID string, minorsOnly bool) (*xpugrpc.AllocateResponse, error) {
	klog.Infof("xdm gRPC allocate resource %s started", dp.ResourceName)
	xmdClient, closer, err := utils.NewXdmGrpcClient()
	if err != nil {
		// never Fatalf here, it would take down the whole device plugin
		klog.Errorf("create xdm gRPC client failed: %v", err)
		return nil, fmt.Errorf("create xdm gRPC client failed: %w", err)
	}
	defer closer()

	ids, err := utils.GetScheduledDeviceIds(selectedPod)
	if err != nil {
		klog.Errorf("get device ids in pod %s/%s failed due to %v", selectedPod.Namespace, selectedPod.Name, err)
	}
	allocateDevices := make([]*xpugrpc.AllocatedDevices, 0, len(ids))

	var allocateCore *wrapperspb.UInt32Value
	var allocateMemory *wrapperspb.UInt32Value
	var allocatePossibility *wrapperspb.UInt32Value
	if !minorsOnly {
		switch dp.ResourceName {
		case utils.ResourceCore:
			coreValue := uint32(podResourceReq)
			allocateCore = wrapperspb.UInt32(coreValue)
		case utils.ResourceMemory:
			memoryValue := uint32(podResourceReq)
			if utils.GetMetric() == utils.GiBPrefix {
				memoryValue = memoryValue * 1024
			}
			allocateMemory = wrapperspb.UInt32(memoryValue)
		case utils.ResourcePossibility:
			possibilityValue := uint32(podResourceReq)
			allocatePossibility = wrapperspb.UInt32(possibilityValue)
		}
	}

	var allocateRequest xpugrpc.AllocateRequest
	allocateRequest.Id = string(selectedPod.UID)
	allocateRequest.RuntimeMode = xpugrpc.RuntimeMode_RUNC
	// carry the allocateID so xdm can correlate it with the dp checkpoint
	if allocateID != "" {
		allocateRequest.Annotations = map[string]string{
			utils.AnnotationKeyAllocateID: allocateID,
		}
	}

	if len(ids) == 0 {
		if podResourceReq <= 0 {
			return nil, fmt.Errorf("invalid resource request count: %d", podResourceReq)
		}
		// community pod: convert the kubelet device uuids into minors
		minors, err := utils.GetDeviceMinorsFromIDs(dp.deviceMeta, kubeletDeviceIDs)
		if err != nil {
			klog.Errorf("convert kubelet device ids %v to minors failed: %v", kubeletDeviceIDs, err)
			return nil, fmt.Errorf("convert kubelet device ids to minors failed: %w", err)
		}
		klog.Infof("community allocate %d %s (minors %v) for pod %s/%s", podResourceReq, dp.ResourceName, minors, selectedPod.Namespace, selectedPod.Name)
		for _, minor := range minors {
			device := &xpugrpc.AllocatedDevices{
				Minor:    wrapperspb.UInt32(uint32(minor)),
				Extender: make(map[string]string),
			}
			if !minorsOnly {
				switch dp.ResourceName {
				case utils.ResourceMemory:
					device.Memory = allocateMemory
				case utils.ResourceCore:
					device.Core = allocateCore
				case utils.ResourcePossibility:
					device.Possibility = allocatePossibility
				}
			}
			allocateDevices = append(allocateDevices, device)
		}
	} else {
		klog.Infof("xpu allocate %d %s for pod %s/%s", len(ids), dp.ResourceName, selectedPod.Namespace, selectedPod.Name)
		for _, id := range ids {
			device := &xpugrpc.AllocatedDevices{
				Minor:    wrapperspb.UInt32(uint32(id)),
				Extender: make(map[string]string),
			}
			if minorsOnly {
				allocateDevices = append(allocateDevices, device)
				continue
			}
			switch dp.ResourceName {
			case utils.ResourceMemory:
				device.Memory = allocateMemory
			case utils.ResourceCore:
				device.Core = allocateCore
			case utils.ResourceSlice:
				device.Slice = wrapperspb.UInt32(uint32(podResourceReq))
				mask, err := utils.GetMigMaskFromAnnotations(selectedPod)
				if err != nil {
					klog.Errorf("pod %s(%s) get MigMask annotation failed due to %v", selectedPod.Name, selectedPod.Namespace, err)
					return nil, fmt.Errorf("pod(%s) get MigMask annotation failed due to %v", selectedPod.Name, err)
				}

				start, size, err := utils.GetMigStartAndSizeFromAnnotations(selectedPod)
				if err != nil {
					klog.Errorf("pod %s(%s) get device topology start and size failed due to error: %v", selectedPod.Name, selectedPod.Namespace, err)
					return nil, fmt.Errorf("pod %s(%s) get device topology start and size failed due to error: %v", selectedPod.Name, selectedPod.Namespace, err)
				}

				migType := utils.GetMigTypeFromAnnotation(selectedPod)
				if migType == utils.MigDeviceType("") {
					klog.Errorf("pod (%s/%s) has empty migType", selectedPod.Namespace, selectedPod.Name)
					return nil, fmt.Errorf("pod(%s/%s) has empty migType", selectedPod.Namespace, selectedPod.Name)
				}

				device.Extender[utils.AllocateMigMask] = strconv.Itoa(int(mask))
				device.Extender[utils.AllocateMigType] = strconv.Itoa(utils.MigType2Mode(migType))
				device.Extender[utils.AllocateMigSize] = strconv.Itoa(size)
				device.Extender[utils.AllocateMigStart] = strconv.Itoa(start)
			case utils.ResourcePossibility:
				device.Possibility = allocatePossibility
			}
			allocateDevices = append(allocateDevices, device)
		}
	}
	// pass the pcie switch indexes to xdm so it mounts the rdma devices behind
	// those switches
	if dp.ResourceName == utils.ResourceGPU || dp.ResourceName == utils.ResourceNvidia {
		if pcieIndex := utils.GetPcieSwitchIndex(selectedPod); pcieIndex != "" {
			for _, device := range allocateDevices {
				device.Extender[utils.AllocatePcieIndex] = pcieIndex
			}
			klog.Infof("xdm allocate attached pcie switch index %q to %d devices of pod %s/%s",
				pcieIndex, len(allocateDevices), selectedPod.Namespace, selectedPod.Name)
		}
	}

	klog.V(4).Infof("xdmAllocate allocateDevices %v", allocateDevices)
	allocateRequest.Devices = allocateDevices

	frontend, err := utils.GetFrontendFromLabels(selectedPod)
	if err != nil {
		klog.Errorf("failed to get frontend info: %v", err)
	} else {
		frontendStr, err := utils.GetFrontendAnnotations(frontend, selectedPod)
		if err != nil {
			return nil, fmt.Errorf("failed to build frontend annotations: %w", err)
		}
		allocateRequest.Topology = map[int32]string{
			2: frontendStr,
		}
	}

	bluffing, err := utils.CheckBluffingPod(selectedPod)
	if err != nil {
		klog.Errorf("failed to get bluffing info: %v", err)
	} else {
		if bluffing {
			bluffingMemory, err := utils.GetBluffingMemory(selectedPod)
			if err != nil {
				return nil, fmt.Errorf("failed to get bluffing memory: %v", err)
			}
			allocateRequest.Topology = map[int32]string{
				2: bluffingMemory,
			}
		}
	}

	// invoke gRPC allocate
	resp, err := xmdClient.Allocate(ctx, &allocateRequest)
	if err != nil {
		klog.Errorf("xdm allocate gRPC call failed: %v", err)
		return nil, fmt.Errorf("gRPC Allocate call failed: %w", err)
	}
	// validate response
	if resp == nil {
		klog.Errorf("xdm allocate gRPC call received nil response from gRPC service")
		return nil, fmt.Errorf("received nil response from gRPC service")
	}

	if resp.Status != 0 {
		klog.Errorf("xdm allocate gRPC non-zero status from gRPC service: %d", resp.Status)
		return nil, fmt.Errorf("non-zero status from gRPC service: %d", resp.Status)
	}

	klog.Infof("xdm gRPC allocate succeeded")
	return resp, nil
}
