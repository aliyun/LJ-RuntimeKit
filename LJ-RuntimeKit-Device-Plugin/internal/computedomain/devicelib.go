/*
Copyright (c) 2026 Alibaba Cloud.

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

package computedomain

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"

	xpugrpc "github.com/lj-runtimekit/device-plugin/internal/grpc"
	"github.com/lj-runtimekit/device-plugin/internal/utils"

	"k8s.io/klog/v2"
)

// deviceLib 抽象了所有需要与 RuntimeKit 守护进程交互的 IMEX 设备操作。
//
// 它对应 dra-driver-nvidia-gpu 中 compute-domain-kubelet-plugin 的 nvlib.go
// (newDeviceLib)。NVIDIA 版本通过 NVML 直接访问驱动,本工程则改为通过
// RuntimeKit gRPC 服务完成等价操作。
//
// TODO(RuntimeKit): 所有方法当前都是未实现的 stub,方法体里标出了
// 需要接入 RuntimeKit 的调用点,请在 TODO 处填充具体实现。
type deviceLib interface {
	// GetNodeInfo 调 RuntimeKit 获取本节点信息，返回 map 至少包含 cliqueID 键；
	// 值为空字符串表示未接入 fabric。
	GetNodeInfo(ctx context.Context) (map[string]string, error)

	// StartIMEX 调 RuntimeKit 启动 IMEX 服务(对应 Prepare 流程)。
	StartIMEX(ctx context.Context, domainID string, maxNodes int) error

	// StopIMEX 调 RuntimeKit 停止 IMEX 服务(对应 Unprepare 流程)。
	StopIMEX(ctx context.Context, domainID string) error

	// AllocateChannel 调 RuntimeKit 分配 channel 设备(对应 Prepare 流程)。
	// 返回 (taskUID, error)，taskUID 用于后续 Release。
	AllocateChannel(ctx context.Context, domainID string, channel int, claimUID string, podUID string, claimName string, claimNamespace string) error

	// ReleaseChannel 调 RuntimeKit 释放 channel 设备(对应 Unprepare 流程)。
	ReleaseChannel(ctx context.Context, claimUID string) error

	// RefreshIMEX 通知 RuntimeKit 刷新 IMEX DNS 映射（更新 /etc/hosts 并触发热重载），
	// 入参为完整的 slot→IP 映射列表。
	RefreshIMEX(ctx context.Context, mappings []DNSNameMapping) error

	// GetMaxImexChannelCount 返回本节点支持的 IMEX channel 总数。
	GetMaxImexChannelCount() int
}

// runtimekitDeviceLib 是 deviceLib 的 RuntimeKit 实现(当前为 stub)。
type runtimekitDeviceLib struct {
	endpoint  string
	taskStore *claimTaskStore
}

// newDeviceLib 创建 deviceLib 实例。
//
// TODO(RuntimeKit): 在这里建立与 RuntimeKit 守护进程的 gRPC 连接。
// 建议参考本工程 internal/utils/xdmGrpc.go 中连接 x-device-manager 的
// 方式(grpc.Dial + 认证/重试参数),把 config.Flags.RuntimeKitEndpoint
// 传给 RuntimeKit 客户端,并在 Preparing 流程中通过该连接调用:
//   - 获取 cliqueID
//   - 启动 / 停止 IMEX
//   - 分配 / 释放 channel 设备
//   - 渲染 nodes.cfg
func newDeviceLib(config *Config, taskStore *claimTaskStore) (deviceLib, error) {
	klog.Infof("creating device library with RuntimeKit endpoint: %v", config.Flags.RuntimeKitEndpoint)

	d := &runtimekitDeviceLib{
		endpoint:  config.Flags.RuntimeKitEndpoint,
		taskStore: taskStore,
	}

	// TODO(RuntimeKit): 校验与 RuntimeKit 的连接,例如:
	//
	//	conn, err := grpc.NewClient(config.Flags.RuntimeKitEndpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	//	if err != nil {
	//		return nil, fmt.Errorf("error connecting to RuntimeKit: %w", err)
	//	}
	//	d.client = xdmapb.NewXpuDeviceManagerServiceClient(conn)
	//	// 必要时扩展 api_outer.proto,新增 IMEX 相关 RPC。

	return d, nil
}

// GetNodeInfo 调 RuntimeKit 获取本节点信息，返回 map 至少包含 cliqueID 键。
func (l *runtimekitDeviceLib) GetNodeInfo(ctx context.Context) (map[string]string, error) {
	// TODO(RuntimeKit): 调用 RuntimeKit 获取节点信息。
	// 建议在 api_outer.proto 中新增(或复用)类似如下的 RPC:
	//
	//   rpc GetNodeInfo(GetNodeInfoRequest) returns (GetNodeInfoResponse);
	//
	// 返回的 map 至少包含 "cliqueID" 键,值为空字符串表示未接入 fabric。
	xmdClient, closer, err := utils.NewXdmGrpcClient()
	if err != nil {
		klog.Errorf("create xdm gRPC client failed: %v", err)
		return nil, err
	}
	defer closer()
	resp, err := xmdClient.GetNodeInfo(ctx, &xpugrpc.GetNodeInfoRequest{})
	if err != nil {
		klog.Errorf("failed to get node info: %v", err)
		return nil, err
	}
	if resp.Status != 0 {
		return nil, fmt.Errorf("GetNodeInfo returned non-zero status %d: %s", resp.Status, resp.Message)
	}
	return resp.Details, nil
}

// StartIMEX 调 RuntimeKit 启动 IMEX 服务。
func (l *runtimekitDeviceLib) StartIMEX(ctx context.Context, domainID string, maxNodes int) error {
	// TODO(RuntimeKit): 调用 RuntimeKit 启动本节点的 IMEX 守护进程,
	// domainID 为 ComputeDomain 的 UID。
	xmdClient, closer, err := utils.NewXdmGrpcClient()
	if err != nil {
		klog.Errorf("create xdm gRPC client failed: %v", err)
		return err
	}
	defer closer()
	resp, err := xmdClient.StartIMEX(ctx, &xpugrpc.StartIMEXRequest{DomainID: domainID, MaxNodes: int32(maxNodes)})
	if err != nil {
		klog.Errorf("failed to start IMEX for domain %s: %v", domainID, err)
		return err
	}
	if resp.Status != 0 {
		return fmt.Errorf("StartIMEX(%s) returned non-zero status %d: %s", domainID, resp.Status, resp.Message)
	}
	klog.V(2).Infof("StartIMEX(%s) succeeded", domainID)
	return nil
}

// StopIMEX 调 RuntimeKit 停止 IMEX 服务。
func (l *runtimekitDeviceLib) StopIMEX(ctx context.Context, domainID string) error {
	// TODO(RuntimeKit): 调用 RuntimeKit 停止本节点的 IMEX 守护进程。
	xmdClient, closer, err := utils.NewXdmGrpcClient()
	if err != nil {
		klog.Errorf("create xdm gRPC client failed: %v", err)
		return err
	}
	defer closer()
	resp, err := xmdClient.StopIMEX(ctx, &xpugrpc.StopIMEXRequest{DomainID: domainID})
	if err != nil {
		klog.Errorf("failed to stop IMEX for domain %s: %v", domainID, err)
		return err
	}
	if resp.Status != 0 {
		return fmt.Errorf("StopIMEX(%s) returned non-zero status %d: %s", domainID, resp.Status, resp.Message)
	}
	klog.V(2).Infof("StopIMEX(%s) succeeded", domainID)
	return nil
}

func generateTaskUID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("failed to generate task UID: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// AllocateChannel 调 RuntimeKit 分配 channel 设备。
func (l *runtimekitDeviceLib) AllocateChannel(ctx context.Context, domainID string, channelID int, claimUID string, podUID string, claimName string, claimNamespace string) error {
	xmdClient, closer, err := utils.NewXdmGrpcClient()
	if err != nil {
		klog.Errorf("create xdm gRPC client failed: %v", err)
		return err
	}
	defer closer()

	taskUID, err := generateTaskUID()
	if err != nil {
		return err
	}
	klog.V(2).Infof("AllocateChannel(domain=%s, channel=%d, claim=%s, pod=%s): taskUID=%s", domainID, channelID, claimUID, podUID, taskUID)

	imexList, _ := json.Marshal([]int{channelID})
	device := &xpugrpc.AllocatedDevices{
		Extender: map[string]string{
			fmt.Sprintf("%s/imex-index", DriverName): string(imexList),
			"rt-podID":                               podUID,
		},
	}
	req := &xpugrpc.AllocateRequest{
		Id:      taskUID,
		Devices: []*xpugrpc.AllocatedDevices{device},
	}
	resp, err := xmdClient.Allocate(ctx, req)
	if err != nil {
		klog.Errorf("failed to allocate channel %d for claim %s: %v", channelID, claimUID, err)
		return err
	}
	if resp == nil || resp.GetStatus() != 0 {
		return fmt.Errorf("allocate channel %d for claim %s returned non-zero status %d: %s", channelID, claimUID, resp.GetStatus(), resp.GetMessage())
	}

	if putErr := l.taskStore.Put(claimUID, ClaimTaskEntry{
		TaskUID:   taskUID,
		PodUID:    podUID,
		Name:      claimName,
		Namespace: claimNamespace,
	}); putErr != nil {
		klog.Warningf("AllocateChannel: failed to persist claim-task mapping for claim %s: %v", claimUID, putErr)
	}

	return nil
}

// ReleaseChannel 调 RuntimeKit 释放 channel 设备。
func (l *runtimekitDeviceLib) ReleaseChannel(ctx context.Context, claimUID string) error {
	entry, err := l.taskStore.Get(claimUID)
	if err != nil {
		return fmt.Errorf("error reading claim-task mapping for claim %s: %w", claimUID, err)
	}
	if entry == nil {
		// 幂等：条目不存在（可能已释放或从未分配）
		klog.V(2).Infof("ReleaseChannel: no task mapping for claim %s, skipping", claimUID)
		return nil
	}

	xmdClient, closer, err := utils.NewXdmGrpcClient()
	if err != nil {
		return fmt.Errorf("create xdm gRPC client failed: %w", err)
	}
	defer closer()

	klog.V(2).Infof("ReleaseChannel(claim=%s): releasing taskUID=%s", claimUID, entry.TaskUID)
	resp, err := xmdClient.Release(ctx, &xpugrpc.ReleaseRequest{Id: entry.TaskUID})
	if err != nil {
		return fmt.Errorf("failed to release task %s for claim %s: %w", entry.TaskUID, claimUID, err)
	}
	if resp == nil || resp.GetStatus() != 0 {
		return fmt.Errorf("release task %s for claim %s returned non-zero status %d: %s", entry.TaskUID, claimUID, resp.GetStatus(), resp.GetMessage())
	}

	if delErr := l.taskStore.Delete(claimUID); delErr != nil {
		klog.Warningf("ReleaseChannel: failed to delete claim-task mapping for claim %s: %v", claimUID, delErr)
	}
	return nil
}

// RefreshIMEX 通知 RuntimeKit 刷新 IMEX DNS 映射（更新 /etc/hosts 并触发热重载），
// 入参为完整的 slot→IP 映射列表。
func (l *runtimekitDeviceLib) RefreshIMEX(ctx context.Context, mappings []DNSNameMapping) error {
	xmdClient, closer, err := utils.NewXdmGrpcClient()
	if err != nil {
		klog.Errorf("create xdm gRPC client failed: %v", err)
		return err
	}
	defer closer()

	items := make([]*xpugrpc.RefreshIMEXItem, 0, len(mappings))
	for _, mapping := range mappings {
		items = append(items, &xpugrpc.RefreshIMEXItem{
			DomainID: mapping.DomainID,
			Index:    int32(mapping.Index),
			IP:       mapping.IP,
		})
	}
	resp, err := xmdClient.RefreshIMEX(ctx, &xpugrpc.RefreshIMEXRequest{Items: items})
	if err != nil {
		klog.Errorf("failed to refresh IMEX DNS mappings: %v", err)
		return err
	}
	if resp.Status != 0 {
		return fmt.Errorf("RefreshIMEX returned non-zero status %d: %s", resp.Status, resp.Message)
	}
	klog.V(2).Infof("RefreshIMEX succeeded")
	return nil
}

// GetMaxImexChannelCount 返回本节点支持的 IMEX channel 总数。
func (l *runtimekitDeviceLib) GetMaxImexChannelCount() int {
	// TODO(RuntimeKit): 从 RuntimeKit(或驱动参数)获取 channel 总数。
	// NVIDIA 驱动固定为 2048,这里沿用该默认值。
	return 2048
}
