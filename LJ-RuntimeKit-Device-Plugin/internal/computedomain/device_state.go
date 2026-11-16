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
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	resourceapi "k8s.io/api/resource/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
)

// permanentError 表示不可重试的错误。默认情况下所有错误都会被 kubelet
// 重试;标记为 permanent 的错误不会。
type permanentError struct{ error }

func isPermanentError(err error) bool {
	for e := err; e != nil; {
		if _, ok := e.(permanentError); ok {
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := e.(unwrapper)
		if !ok {
			return false
		}
		e = u.Unwrap()
	}
	return false
}

// readBootID 读取 Linux 内核 boot ID,每次系统重启后会生成新值。
func readBootID() (string, error) {
	data, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", fmt.Errorf("error reading boot_id: %w", err)
	}
	return strings.TrimSpace(string(data)), nil
}

// DeviceState 持有插件全部运行时状态(对应 NVIDIA 的 DeviceState)。
type DeviceState struct {
	sync.Mutex
	computeDomainManager *ComputeDomainManager
	allocatable          AllocatableDevices
	config               *Config
	devlib               deviceLib

	checkpointManager *checkpointStore
	managerRefCount   int
}

// NewDeviceState 创建 DeviceState。
//
// 其中的 newDeviceLib 被留出来,用于通过 RuntimeKit 获取本节点的 IMEX
// 设备信息(见 devicelib.go 中的 TODO(RuntimeKit))。
func NewDeviceState(ctx context.Context, config *Config, taskStore *claimTaskStore) (*DeviceState, error) {
	// TODO(RuntimeKit): newDeviceLib 在此接入 RuntimeKit。
	devlib, err := newDeviceLib(config, taskStore)
	if err != nil {
		return nil, fmt.Errorf("failed to create device library: %w", err)
	}

	allocatable := enumerateAllPossibleDevices(devlib)
	if len(allocatable) == 0 {
		return nil, fmt.Errorf("no allocatable devices enumerated")
	}

	checkpointManager := newCheckpointStore(config.DriverPluginPath())

	state := &DeviceState{
		allocatable:       allocatable,
		config:            config,
		devlib:            devlib,
		checkpointManager: checkpointManager,
	}

	// === 第一段：显式列出 checkpoint 文件 ===
	checkpoints, err := state.checkpointManager.ListCheckpoints()
	if err != nil {
		return nil, fmt.Errorf("error listing checkpoints: %w", err)
	}

	// 读取当前 boot ID
	currentBootID, err := readBootID()
	if err != nil {
		return nil, fmt.Errorf("error reading boot ID: %w", err)
	}

	// === 第二段：遍历 checkpoint 文件，加载并校验 boot ID ===
	for _, c := range checkpoints {
		if c == DriverPluginCheckpointFileBasename {
			cp, err := state.getCheckpoint()
			if err != nil {
				return nil, fmt.Errorf("error loading checkpoint: %w", err)
			}

			storedBootID := cp.GetNodeBootID()

			switch {
			case storedBootID == "":
				// 旧版 checkpoint 无 boot ID → 注入当前 boot ID，保留 claims
				klog.Infof("No boot ID in checkpoint, initializing with current boot ID")
				if err := state.updateCheckpoint(func(checkpoint *Checkpoint) {
					checkpoint.V2.NodeBootID = currentBootID
				}); err != nil {
					return nil, fmt.Errorf("unable to update checkpoint with boot ID: %w", err)
				}
				return state, nil

			case storedBootID == currentBootID:
				// Boot ID 匹配 → checkpoint 有效，直接恢复（不写文件）
				klog.Infof("Boot ID matches, restoring %d prepared claims from checkpoint",
					len(cp.V2.PreparedClaims))
				return state, nil

			default:
				// Boot ID 变化（节点重启）→ 废弃旧 checkpoint，fall through 创建新的
				klog.Infof("Boot ID changed (%s -> %s), invalidating all %d prepared claims",
					storedBootID, currentBootID, len(cp.V2.PreparedClaims))
			}
		}
	}

	// === 第三段：没有找到有效 checkpoint 或 boot ID 不匹配 → 创建全新 checkpoint ===
	newCp := &Checkpoint{
		V2: &CheckpointV2{
			NodeBootID:     currentBootID,
			PreparedClaims: make(PreparedClaimsByUID),
		},
	}
	if err := state.createCheckpoint(newCp); err != nil {
		return nil, fmt.Errorf("unable to initialize checkpoint: %w", err)
	}

	return state, nil
}

// ensureComputeDomainManager 懒初始化 ComputeDomainManager。
// 若已存在则直接返回；否则创建 manager 并启动 informer。
func (s *DeviceState) ensureComputeDomainManager(ctx context.Context, cliqueID string) error {
	if s.computeDomainManager != nil {
		return nil
	}
	m := newComputeDomainManager(s.config, cliqueID, s.devlib)
	if err := m.Start(context.Background()); err != nil {
		return fmt.Errorf("error starting ComputeDomainManager: %w", err)
	}
	s.computeDomainManager = m
	klog.Infof("ComputeDomainManager lazily initialized (cliqueID=%s)", cliqueID)
	return nil
}

// destroyComputeDomainManager 停止并销毁 ComputeDomainManager。
// 仅停止 informer，不做远程清理（调用方已完成 RemoveCliqueEntry）。
func (s *DeviceState) destroyComputeDomainManager() {
	if s.computeDomainManager == nil {
		return
	}
	s.computeDomainManager.stopInformer()
	s.computeDomainManager = nil
	klog.Infof("ComputeDomainManager destroyed (no active claims)")
}

// enumerateAllPossibleDevices 枚举节点上所有可分配的 IMEX channel 设备。
//
// 按照本 driver 的设计,只发布 channel-0(channel-1..N 由网络资源驱动
// 或后续版本发布),与 dra-driver-nvidia-gpu 的做法保持一致。
func enumerateAllPossibleDevices(devlib deviceLib) AllocatableDevices {
	devices := make(AllocatableDevices)
	info := &ComputeDomainChannelInfo{ID: 0}
	devices[info.CanonicalName()] = &AllocatableDevice{Channel: info}
	return devices
}

// createCheckpoint 直接调用 store（无 cplock，依赖 pulock）。
func (s *DeviceState) createCheckpoint(cp *Checkpoint) error {
	return s.checkpointManager.CreateCheckpoint(DriverPluginCheckpointFileBasename, cp)
}

// getCheckpoint 读取 checkpoint 并迁移到最新版本。
// 文件不存在时返回带空 V2 的 checkpoint（首次启动场景）。
func (s *DeviceState) getCheckpoint() (*Checkpoint, error) {
	cp := &Checkpoint{}
	if err := s.checkpointManager.GetCheckpoint(DriverPluginCheckpointFileBasename, cp); err != nil {
		if os.IsNotExist(err) {
			return &Checkpoint{V2: &CheckpointV2{PreparedClaims: make(PreparedClaimsByUID)}}, nil
		}
		return nil, fmt.Errorf("error getting checkpoint: %w", err)
	}
	return cp.ToLatestVersion(), nil
}

// updateCheckpoint 读-改-写（无 cplock，依赖 pulock 做跨进程互斥）。
func (s *DeviceState) updateCheckpoint(mutate func(*Checkpoint)) error {
	cp, err := s.getCheckpoint()
	if err != nil {
		return fmt.Errorf("unable to get checkpoint: %w", err)
	}
	mutate(cp)
	if err := s.createCheckpoint(cp); err != nil {
		return fmt.Errorf("unable to create checkpoint: %w", err)
	}
	return nil
}

// deleteClaimFromCheckpoint 从 checkpoint 中删除指定 claim。
func (s *DeviceState) deleteClaimFromCheckpoint(claimRef NamespacedObject) error {
	err := s.updateCheckpoint(func(cp *Checkpoint) {
		delete(cp.V2.PreparedClaims, claimRef.UID)
	})
	if err != nil {
		return fmt.Errorf("unable to update checkpoint: %w", err)
	}
	klog.V(6).Infof("Deleted claim from checkpoint: %s", claimRef.String())
	return nil
}

// Prepare 为一个 claim 准备 channel 设备(必须幂等)。
func (s *DeviceState) Prepare(ctx context.Context, claim *resourceapi.ResourceClaim) ([]Device, error) {
	s.Lock()
	defer s.Unlock()

	claimUID := string(claim.UID)

	checkpoint, err := s.getCheckpoint()
	if err != nil {
		return nil, fmt.Errorf("unable to get checkpoint: %w", err)
	}

	preparedClaim, exists := checkpoint.V2.PreparedClaims[claimUID]
	if exists && preparedClaim.CheckpointState == ClaimCheckpointStatePrepareCompleted {
		if err := s.ensureManagerRecoveredForClaim(ctx, claim, &preparedClaim); err != nil {
			return nil, fmt.Errorf("error recovering manager state for completed claim %s: %w",
				ResourceClaimToString(claim), err)
		}
		klog.V(4).Infof("Skip prepare: claim already in PrepareCompleted state: %s", ResourceClaimToString(claim))
		return preparedClaim.PreparedDevices.GetDevices(), nil
	}

	if exists && preparedClaim.CheckpointState == ClaimCheckpointStatePrepareAborted && claimMatchesPreparedClaim(preparedClaim, claim) {
		return nil, permanentError{fmt.Errorf("stale prepare for claim %s: claim prepare was already aborted", ResourceClaimToString(claim))}
	}

	if exists && preparedClaim.CheckpointState == ClaimCheckpointStatePrepareStarted {
		// 上次 Prepare 半途失败，先回滚部分资源再重试
		klog.Warningf("Found PrepareStarted state for claim %s, rolling back before retry", claimUID)
		if err := s.unprepareDevices(ctx, claimUID, &preparedClaim); err != nil {
			// 回滚失败：保留 PrepareStarted 状态供后续重试或 cleanup manager 处理
			return nil, fmt.Errorf("error rolling back PrepareStarted claim %s: %w", claimUID, err)
		}
		// 回滚成功，从 checkpoint 删除旧条目
		if err := s.deleteClaimFromCheckpoint(NamespacedObject{
			UID:            claimUID,
			NamespacedName: types.NamespacedName{Name: preparedClaim.Name, Namespace: preparedClaim.Namespace},
		}); err != nil {
			return nil, fmt.Errorf("unable to delete rolled-back claim from checkpoint: %w", err)
		}
		// 重新读取 checkpoint 后走正常路径
		checkpoint, err = s.getCheckpoint()
		if err != nil {
			return nil, fmt.Errorf("unable to reload checkpoint after rollback: %w", err)
		}
	}

	// 防止同一设备被并发分配给多个 claim(scheduler 竞态 / pod 强删场景)。
	if err := s.validateNoOverlappingPreparedDevices(checkpoint, claim); err != nil {
		return nil, fmt.Errorf("unable to prepare claim %v: %w", claimUID, err)
	}

	err = s.updateCheckpoint(func(checkpoint *Checkpoint) {
		checkpoint.V2.PreparedClaims[claimUID] = PreparedClaim{
			CheckpointState: ClaimCheckpointStatePrepareStarted,
			Status:          claim.Status,
			Name:            claim.Name,
			Namespace:       claim.Namespace,
		}
	})
	if err != nil {
		return nil, fmt.Errorf("unable to update checkpoint: %w", err)
	}
	klog.V(6).Infof("checkpoint updated for claim %v", claimUID)

	preparedDevices, err := s.prepareDevices(ctx, claim)
	if err != nil {
		return nil, fmt.Errorf("prepare devices failed: %w", err)
	}

	// Some errors above along the Prepare() path leave the claim in the
	// checkpoint, in the 'PrepareStarted' state. That's deliberate, to annotate
	// potentially partially prepared claims.
	err = s.updateCheckpoint(func(checkpoint *Checkpoint) {
		checkpoint.V2.PreparedClaims[claimUID] = PreparedClaim{
			CheckpointState: ClaimCheckpointStatePrepareCompleted,
			Status:          claim.Status,
			Name:            claim.Name,
			Namespace:       claim.Namespace,
			//nolint:gocritic // deliberately stored via named local variable
			PreparedDevices: preparedDevices,
		}
	})
	if err != nil {
		return nil, fmt.Errorf("unable to update checkpoint: %w", err)
	}
	klog.V(6).Infof("checkpoint updated for claim %v", claimUID)

	return preparedDevices.GetDevices(), nil
}

// Unprepare 释放一个 claim 的 channel 设备(必须幂等)。
func (s *DeviceState) Unprepare(ctx context.Context, claimRef NamespacedObject) error {
	s.Lock()
	defer s.Unlock()

	klog.V(6).Infof("Unprepare() for claim '%s'", claimRef.String())

	// Rely on local checkpoint state for ability to clean up.
	checkpoint, err := s.getCheckpoint()
	if err != nil {
		return fmt.Errorf("unable to get checkpoint: %w", err)
	}

	pc, exists := checkpoint.V2.PreparedClaims[claimRef.UID]
	if !exists {
		// Not an error: if this claim UID is not in the checkpoint then this
		// device was never prepared or has already been unprepared.
		klog.V(2).Infof("Unprepare noop: claim not found in checkpoint data: %v", claimRef.String())
		return nil
	}

	switch pc.CheckpointState {
	case ClaimCheckpointStatePrepareCompleted:
		if err := s.unprepareDevices(ctx, claimRef.UID, &pc); err != nil {
			return fmt.Errorf("unprepare devices failed: %w", err)
		}
		if err := s.markClaimPrepareAbortedInCheckpoint(claimRef, pc); err != nil {
			return fmt.Errorf("error marking completed claim PrepareAborted in checkpoint: %w", err)
		}
	case ClaimCheckpointStatePrepareStarted:
		if err := s.unprepareDevices(ctx, claimRef.UID, &pc); err != nil {
			return fmt.Errorf("unprepare devices failed: %w", err)
		}
		// Keep a short-lived PrepareAborted entry so stale prepare retries for the
		// same claim version cannot recreate device state after unprepare.
		if err := s.markClaimPrepareAbortedInCheckpoint(claimRef, pc); err != nil {
			return fmt.Errorf("error marking claim PrepareAborted in checkpoint: %w", err)
		}
	case ClaimCheckpointStatePrepareAborted:
		klog.V(2).Infof("Unprepare noop: claim in PrepareAborted state: %v", claimRef.String())
		return nil
	default:
		return fmt.Errorf("unsupported ClaimCheckpointState: %v", pc.CheckpointState)
	}
	return nil
}

func getPodUID(claim *resourceapi.ResourceClaim) (string, error) {
	var podUID string
	for _, ref := range claim.Status.ReservedFor {
		if ref.APIGroup == "" && ref.Resource == "pods" {
			if podUID != "" {
				return "", fmt.Errorf("multiple pod consumers found in claim %s ReservedFor", claim.Name)
			}
			podUID = string(ref.UID)
		}
	}
	if podUID == "" {
		return "", fmt.Errorf("no pod consumer found in claim %s ReservedFor", claim.Name)
	}
	return podUID, nil
}

// prepareDevices 实现核心 Prepare 流程:
//
//	校验 channel 独占
//	→ 调 RuntimeKit 获取 cliqueID
//	→ 创建 ComputeDomainClique 条目
//	→ 调 RuntimeKit 启动 IMEX
//	→ 检查 IMEX 就绪并更新 daemon 状态
//	→ 调 RuntimeKit 分配 channel 设备
//	→ 组装 CDI 设备 ID(TODO(CDI))
func (s *DeviceState) prepareDevices(ctx context.Context, claim *resourceapi.ResourceClaim) (PreparedDeviceList, error) {
	claimUID := string(claim.UID)
	domainID := getDomainIDFromClaimStatus(&claim.Status, claimUID)
	channelID := 0
	podUID, err := getPodUID(claim)
	if err != nil {
		return nil, fmt.Errorf("unable to get pod UID: %w", err)
	}
	klog.V(2).Infof("Prepare() for claim '%s' (pod %s)", claimUID, podUID)

	// 1. 校验 channel 独占:channel 只能同时分配给一个 claim。
	if err := s.assertChannelNotAllocated(channelID); err != nil {
		return nil, fmt.Errorf("allocation failed: %w", err)
	}

	// 2. 调 RuntimeKit 获取 cliqueID。
	nodeInfo, err := s.devlib.GetNodeInfo(ctx)
	if err != nil {
		return nil, fmt.Errorf("error getting node info from RuntimeKit: %w", err)
	}
	cliqueID := nodeInfo["cliqueID"]
	if cliqueID == "" {
		return nil, fmt.Errorf("no cliqueID for this node; cannot bring up IMEX for claim %v", claimUID)
	}

	// 3. 创建 ComputeDomainClique 条目(并把本节点加入 daemons)。

	// 2.5 懒初始化 ComputeDomainManager（首次 Prepare 才建立 informer 连接）。
	if err := s.ensureComputeDomainManager(ctx, cliqueID); err != nil {
		return nil, fmt.Errorf("error initializing ComputeDomainManager: %w", err)
	}

	if err := s.computeDomainManager.AddCliqueEntry(ctx, domainID, cliqueID); err != nil {
		return nil, fmt.Errorf("error adding clique entry for domain %v: %w", domainID, err)
	}
	s.managerRefCount++

	// 4. 调 RuntimeKit 启动 IMEX。
	if err := s.devlib.StartIMEX(ctx, domainID, s.config.Flags.MaxNodesPerIMEXDomain); err != nil {
		return nil, fmt.Errorf("error starting IMEX via RuntimeKit: %w", err)
	}

	// 5. StartIMEX 成功后标记本节点 Ready。
	if err := s.computeDomainManager.UpdateDaemonStatus(ctx, domainID, ComputeDomainStatusReady); err != nil {
		klog.Warningf("failed to update daemon status to Ready: %v", err)
		// 不阻塞 Prepare 流程，状态更新失败不影响功能
	}

	// 6. 调 RuntimeKit 分配 channel 设备。
	if err := s.devlib.AllocateChannel(ctx, domainID, channelID, claimUID, podUID, claim.Name, claim.Namespace); err != nil {
		return nil, fmt.Errorf("error allocating channel %d via RuntimeKit: %w", channelID, err)
	}

	// 7. 组装返回给 kubelet 的设备列表(含 CDI 设备 ID)。
	var preparedDevices PreparedDeviceList
	for _, result := range claim.Status.Allocation.Devices.Results {
		if result.Driver != DriverName {
			continue
		}
		allocatable, exists := s.allocatable[result.Device]
		if !exists {
			return nil, fmt.Errorf("requested device is not allocatable: %v", result.Device)
		}

		device := Device{
			Requests:   []string{result.Request},
			PoolName:   result.Pool,
			DeviceName: result.Device,
		}

		preparedDevices = append(preparedDevices, PreparedDevice{
			Channel: &PreparedComputeDomainChannel{
				Info:   allocatable.Channel,
				Device: &device,
			},
		})
	}

	return preparedDevices, nil
}

// unprepareDevices 实现核心 Unprepare 流程:
//
//	释放 channel(调 RuntimeKit)
//	→ 停止 IMEX(调 RuntimeKit)
//	→ 删除 ComputeDomainClique 条目
func (s *DeviceState) unprepareDevices(ctx context.Context, claimUID string, pc *PreparedClaim) error {
	domainID := getDomainIDFromClaimStatus(&pc.Status, claimUID)

	// 确保 manager 存在（重启恢复场景：checkpoint 有残留 claim 但 manager 尚未创建）。
	if s.computeDomainManager == nil {
		nodeInfo, err := s.devlib.GetNodeInfo(ctx)
		if err != nil {
			klog.Errorf("cannot get cliqueID for clique cleanup, skipping RemoveCliqueEntry: %v", err)
		} else if cid := nodeInfo["cliqueID"]; cid != "" {
			if initErr := s.ensureComputeDomainManager(ctx, cid); initErr != nil {
				klog.Errorf("cannot initialize ComputeDomainManager for cleanup: %v", initErr)
			}
		}
	}

	// 1. 调 RuntimeKit 释放 channel 设备。
	if err := s.devlib.ReleaseChannel(ctx, claimUID); err != nil {
		return fmt.Errorf("error releasing channel via RuntimeKit: %w", err)
	}

	// 如果这是最后一个引用，提前 shutdown informer，防止后续
	// RemoveCliqueEntry 修改 Clique CR 触发 onCliqueChange → RefreshIMEX
	// 把刚停的 IMEX 又拉起来。shutdown() 是幂等的（atomic.Store +
	// context.CancelFunc），destroyComputeDomainManager 再调 stopInformer 无副作用。
	if s.computeDomainManager != nil && s.managerRefCount <= 1 {
		s.computeDomainManager.shutdown()
	}

	// 2. 调 RuntimeKit 停止 IMEX。
	if err := s.devlib.StopIMEX(ctx, domainID); err != nil {
		klog.Errorf("error stopping IMEX via RuntimeKit (continuing to remove clique entry): %v", err)
	}

	// 3. 删除 ComputeDomainClique 条目(并把本节点从 daemons 移除)。
	if s.computeDomainManager != nil {
		if err := s.computeDomainManager.RemoveCliqueEntry(ctx, domainID); err != nil {
			return fmt.Errorf("error removing clique entry for domain %v: %w", domainID, err)
		}
	}

	s.managerRefCount--
	if s.managerRefCount <= 0 {
		s.managerRefCount = 0
		s.destroyComputeDomainManager()
	}

	return nil
}

// markClaimPrepareAbortedInCheckpoint 将指定 claim 标记为 PrepareAborted，
// 保留条目一段时间（TTL），使 stale Prepare 重试能识别并返回 permanentError。
func (s *DeviceState) markClaimPrepareAbortedInCheckpoint(claimRef NamespacedObject, pc PreparedClaim) error {
	abortedAt := metav1.Now()
	pc.CheckpointState = ClaimCheckpointStatePrepareAborted
	pc.PreparedDevices = nil
	pc.Name = claimRef.NamespacedName.Name
	pc.Namespace = claimRef.NamespacedName.Namespace
	pc.AbortedAt = &abortedAt

	err := s.updateCheckpoint(func(cp *Checkpoint) {
		cp.V2.PreparedClaims[claimRef.UID] = pc
	})
	if err != nil {
		return fmt.Errorf("unable to update checkpoint: %w", err)
	}
	klog.V(6).Infof("Marked claim PrepareAborted in checkpoint: %s", claimRef.String())
	return nil
}

// deleteExpiredPrepareAbortedClaimsFromCheckpoint 删除 TTL 已过期的
// PrepareAborted 条目，在 cleanup manager 中周期性调用。
func (s *DeviceState) deleteExpiredPrepareAbortedClaimsFromCheckpoint(now time.Time, ttl time.Duration) (int, error) {
	s.Lock()
	defer s.Unlock()

	checkpoint, err := s.getCheckpoint()
	if err != nil {
		return 0, fmt.Errorf("unable to get checkpoint: %w", err)
	}

	expired := expiredPrepareAbortedClaimEntries(checkpoint, now, ttl)
	if len(expired) == 0 {
		return 0, nil
	}

	err = s.updateCheckpoint(func(cp *Checkpoint) {
		for _, uid := range expired {
			delete(cp.V2.PreparedClaims, uid)
		}
	})
	if err != nil {
		return 0, fmt.Errorf("unable to update checkpoint: %w", err)
	}
	return len(expired), nil
}

// expiredPrepareAbortedClaimEntries 返回 checkpoint 中已过期的 PrepareAborted
// 条目的 UID 列表。AbortedAt 为 nil 的条目视为已过期（异常兜底）。
func expiredPrepareAbortedClaimEntries(cp *Checkpoint, now time.Time, ttl time.Duration) []string {
	var expired []string
	if cp == nil || cp.V2 == nil {
		return expired
	}
	for uid, claim := range cp.V2.PreparedClaims {
		if claim.CheckpointState != ClaimCheckpointStatePrepareAborted {
			continue
		}
		if claim.AbortedAt == nil || now.Sub(claim.AbortedAt.Time) >= ttl {
			expired = append(expired, uid)
		}
	}
	slices.Sort(expired)
	return expired
}

// claimMatchesPreparedClaim 比较 checkpoint 中的 prepared claim 与
// 传入 claim 的 Status 是否语义相等（判断是否为同一次 allocation）。
func claimMatchesPreparedClaim(pc PreparedClaim, claim *resourceapi.ResourceClaim) bool {
	return apiequality.Semantic.DeepEqual(pc.Status, claim.Status)
}

// ensureManagerRecoveredForClaim 在 Completed 快速路径返回缓存前，
// 确保 ComputeDomainManager 和 Clique 成员身份已恢复（重启场景）。
// 稳态下（Manager 存在且 refCount > 0）直接返回，不做额外操作。
func (s *DeviceState) ensureManagerRecoveredForClaim(ctx context.Context, claim *resourceapi.ResourceClaim, pc *PreparedClaim) error {
	// 已有 Manager 且引用计数 > 0 → 稳态，无需恢复
	if s.computeDomainManager != nil && s.managerRefCount > 0 {
		return nil
	}

	claimUID := string(claim.UID)
	domainID := getDomainIDFromClaimStatus(&pc.Status, claimUID)

	// 1. 获取 cliqueID
	nodeInfo, err := s.devlib.GetNodeInfo(ctx)
	if err != nil {
		return fmt.Errorf("error getting node info: %w", err)
	}
	cliqueID := nodeInfo["cliqueID"]
	if cliqueID == "" {
		return fmt.Errorf("no cliqueID available for recovery")
	}

	// 2. 确保 Manager 存在
	if err := s.ensureComputeDomainManager(ctx, cliqueID); err != nil {
		return fmt.Errorf("error initializing manager during recovery: %w", err)
	}

	// 3. 幂等加入 Clique
	if err := s.computeDomainManager.AddCliqueEntry(ctx, domainID, cliqueID); err != nil {
		return fmt.Errorf("error re-adding clique entry during recovery: %w", err)
	}

	// 4. 恢复 Ready 状态
	if err := s.computeDomainManager.UpdateDaemonStatus(ctx, domainID, ComputeDomainStatusReady); err != nil {
		klog.Warningf("failed to update daemon status to Ready during recovery: %v", err)
	}

	// 5. 从 checkpoint 统计 Completed 条目数，赋值（非递增）引用计数
	cp, err := s.getCheckpoint()
	if err != nil {
		return fmt.Errorf("error reading checkpoint for ref count recovery: %w", err)
	}
	count := 0
	for _, c := range cp.V2.PreparedClaims {
		if c.CheckpointState == ClaimCheckpointStatePrepareCompleted {
			count++
		}
	}
	s.managerRefCount = count

	klog.Infof("Manager recovered: domain=%s, cliqueID=%s, refCount=%d", domainID, cliqueID, count)
	return nil
}

// recoverManagerFromCheckpoint 在启动阶段主动恢复 Manager/Informer。
// 扫描 checkpoint 中的 Completed claim，恢复 Manager 和 Clique 成员身份。
// 调用时须持有 DeviceState.Mutex。
// 所有操作幂等，失败只返回 error，不产生部分副作用。
func (s *DeviceState) recoverManagerFromCheckpoint(ctx context.Context) error {
	// 1. 幂等检查：Manager 存在且未停止且有引用 → 已恢复
	if s.computeDomainManager != nil && !s.computeDomainManager.stopped.Load() && s.managerRefCount > 0 {
		return nil
	}

	// 2. 读 checkpoint，收集 Completed 候选
	cp, err := s.getCheckpoint()
	if err != nil {
		return fmt.Errorf("error reading checkpoint for recovery: %w", err)
	}

	var domainID string
	completedCount := 0
	for uid, pc := range cp.V2.PreparedClaims {
		if pc.CheckpointState == ClaimCheckpointStatePrepareCompleted {
			completedCount++
			if domainID == "" {
				domainID = getDomainIDFromClaimStatus(&pc.Status, uid)
			}
		}
	}
	if completedCount == 0 || domainID == "" {
		return nil // 无需恢复
	}

	// 3. 获取 cliqueID
	nodeInfo, err := s.devlib.GetNodeInfo(ctx)
	if err != nil {
		return fmt.Errorf("error getting node info for recovery: %w", err)
	}
	cliqueID := nodeInfo["cliqueID"]
	if cliqueID == "" {
		return fmt.Errorf("no cliqueID available for recovery")
	}

	// 4. 若 Manager 存在但已停止，先清除再重建
	if s.computeDomainManager != nil && s.computeDomainManager.stopped.Load() {
		s.computeDomainManager = nil
		s.managerRefCount = 0
	}

	// 5. 确保 Manager（包含 informer 启动）
	if err := s.ensureComputeDomainManager(ctx, cliqueID); err != nil {
		return fmt.Errorf("error creating Manager for recovery: %w", err)
	}

	// 6. 幂等加入 Clique（恢复远程成员身份）
	if err := s.computeDomainManager.AddCliqueEntry(ctx, domainID, cliqueID); err != nil {
		return fmt.Errorf("error re-adding clique entry during recovery: %w", err)
	}

	// 7. 恢复 Ready 状态（尽力而为）
	if err := s.computeDomainManager.UpdateDaemonStatus(ctx, domainID, ComputeDomainStatusReady); err != nil {
		klog.Warningf("recovery: failed to update daemon status: %v", err)
	}

	// 8. 从 checkpoint 赋值引用计数
	s.managerRefCount = completedCount

	klog.Infof("Manager recovered from checkpoint: domain=%s, cliqueID=%s, refCount=%d",
		domainID, cliqueID, completedCount)
	return nil
}

// assertChannelNotAllocated 以 checkpoint 为节点本地的唯一事实来源,
// 校验编号为 id 的 IMEX channel 尚未被其他 claim 占用。
func (s *DeviceState) assertChannelNotAllocated(id int) error {
	cp, err := s.getCheckpoint()
	if err != nil {
		return fmt.Errorf("unable to get checkpoint: %w", err)
	}

	for claimUID, claim := range cp.V2.PreparedClaims {
		// Ignore non-completed preparations: file-based locking guarantees
		// that only one Prepare() runs at any given time.
		if claim.CheckpointState != ClaimCheckpointStatePrepareCompleted {
			continue
		}

		for _, d := range claim.PreparedDevices {
			if d.Channel != nil && d.Channel.Info.ID == id {
				return fmt.Errorf("channel %d already allocated by claim %s (according to checkpoint)", id, claimUID)
			}
		}
	}
	return nil
}

// requestedNonAdminDevices 返回 claim 请求的设备名集合(排除 admin access)。
func requestedNonAdminDevices(claim *resourceapi.ResourceClaim) map[string]struct{} {
	requested := make(map[string]struct{}, len(claim.Status.Allocation.Devices.Results))

	for _, r := range claim.Status.Allocation.Devices.Results {
		if r.Driver != DriverName {
			continue
		}
		if r.AdminAccess != nil && *r.AdminAccess {
			continue
		}
		requested[r.Device] = struct{}{}
	}
	return requested
}

// validateNoOverlappingPreparedDevices 检查 claim 请求的设备是否已被其他
// 完成分配的 claim(非 admin)持有。
func (s *DeviceState) validateNoOverlappingPreparedDevices(checkpoint *Checkpoint, claim *resourceapi.ResourceClaim) error {
	claimUID := string(claim.UID)

	requestedDevices := requestedNonAdminDevices(claim)
	if len(requestedDevices) == 0 {
		return nil
	}

	for existingClaimUID, pc := range checkpoint.V2.PreparedClaims {
		if existingClaimUID == claimUID {
			continue
		}
		if pc.CheckpointState != ClaimCheckpointStatePrepareCompleted {
			continue
		}

		preparedDevices := pc.GetNonAdminDevices()
		if len(preparedDevices) == 0 {
			continue
		}

		for device := range requestedDevices {
			if _, found := preparedDevices[device]; found {
				return fmt.Errorf("requested device %s is already allocated to different claim %s",
					device, existingClaimUID)
			}
		}
	}
	return nil
}

// getDomainIDFromClaimStatus 从 claim 的 opaque 参数中解出 DomainID,
// 解析失败或未配置时退化为 claimUID,保证 Prepare / Unprepare 流程总有
// 一个可用的 domain 标识。
func getDomainIDFromClaimStatus(status *resourceapi.ResourceClaimStatus, claimUID string) string {
	if status == nil || status.Allocation == nil {
		return ""
	}
	configs, err := getOpaqueDeviceConfigs(channelConfigDecoder, DriverName, status.Allocation.Devices.Config)
	if err != nil {
		klog.V(4).Infof("error decoding opaque configs for claim %v (falling back to claim UID as domainID): %v", claimUID, err)
		return claimUID
	}

	// 取优先级最高的 channel config(列表后面的优先级更高)。
	for i := len(configs) - 1; i >= 0; i-- {
		if c, ok := configs[i].Config.(*nvapi.ComputeDomainChannelConfig); ok && c.DomainID != "" {
			return c.DomainID
		}
	}

	return claimUID
}

// OpaqueDeviceConfig couples a decoded opaque config with the requests it
// applies to.
type OpaqueDeviceConfig struct {
	Requests []string
	Config   runtime.Object
}

// getOpaqueDeviceConfigs 返回 claim 中属于本 driver 的 opaque 参数列表,
// 按优先级从低到高排列(DeviceClass 中的 config 优先级低于 claim 中的)。
func getOpaqueDeviceConfigs(
	decoder runtime.Decoder,
	driverName string,
	possibleConfigs []resourceapi.DeviceAllocationConfiguration,
) ([]*OpaqueDeviceConfig, error) {
	// Collect all configs in order of reverse precedence.
	var classConfigs []resourceapi.DeviceAllocationConfiguration
	var claimConfigs []resourceapi.DeviceAllocationConfiguration
	var candidateConfigs []resourceapi.DeviceAllocationConfiguration
	for _, config := range possibleConfigs {
		switch config.Source {
		case resourceapi.AllocationConfigSourceClass:
			classConfigs = append(classConfigs, config)
		case resourceapi.AllocationConfigSourceClaim:
			claimConfigs = append(claimConfigs, config)
		default:
			return nil, fmt.Errorf("invalid config source: %v", config.Source)
		}
	}
	candidateConfigs = append(candidateConfigs, classConfigs...)
	candidateConfigs = append(candidateConfigs, claimConfigs...)

	// Decode all configs that are relevant for the driver.
	var resultConfigs []*OpaqueDeviceConfig
	for _, config := range candidateConfigs {
		if config.Opaque == nil {
			return nil, fmt.Errorf("only opaque parameters are supported by this driver")
		}

		// Configs for different drivers may have been specified because a
		// single request can be satisfied by different drivers. This is not
		// an error -- drivers must skip over other driver's configs.
		if config.Opaque.Driver != driverName {
			continue
		}

		decodedConfig, err := runtime.Decode(decoder, config.Opaque.Parameters.Raw)
		if err != nil {
			// Bad opaque config: return a permanent error so the kubelet can
			// play this error back to the user so that it becomes actionable.
			return nil, permanentError{fmt.Errorf("error decoding config parameters: %w", err)}
		}

		resultConfigs = append(resultConfigs, &OpaqueDeviceConfig{
			Requests: config.Requests,
			Config:   decodedConfig,
		})
	}

	return resultConfigs, nil
}
