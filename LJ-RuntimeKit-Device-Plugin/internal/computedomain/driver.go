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
	"path/filepath"
	"time"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/klog/v2"
)

// ErrorRetryMaxTimeout 限制 Prepare / Unprepare 请求处理的总时长,
// 保证 kubelet 能在可预期的时间内收到响应。
const ErrorRetryMaxTimeout = 45 * time.Second

// driver 是 compute-domain-kubelet-plugin 的顶层结构。
//
// 它持有全部运行时状态,并把 kubelet 的 DRA gRPC 请求分发到
// DeviceState。与 dra-driver-nvidia-gpu 不同的是,本实现没有使用
// k8s.io/dynamic-resource-allocation/kubeletplugin 的 Helper,而是自研了
// 两个 gRPC 服务(draService / registrationService),协议与 kubelet
// 保持一致。
type driver struct {
	config *Config
	state  *DeviceState
	pulock *flock

	draService          *DRAService
	registrationService *RegistrationService
	sliceSyncer         *resourceSliceSyncer
	cleanupManager      *checkpointCleanupManager
}

// NewDriver 创建 driver。
//
// 这是 compute-domain-kubelet-plugin 的核心构造函数:
//   - NewDeviceState 里的 newDeviceLib 被留出来用于接入 RuntimeKit
//     (见 devicelib.go 中的 TODO(RuntimeKit));
//   - Start() 只启动 draService 和 registrationService 两个逻辑
//     (rollingUpdateUID / enableDeviceMetadata 相关逻辑均已去掉)。
func NewDriver(ctx context.Context, config *Config) (*driver, error) {
	taskStore := newClaimTaskStore(config.DriverPluginPath())

	state, err := NewDeviceState(ctx, config, taskStore)
	if err != nil {
		return nil, err
	}

	puLockPath := filepath.Join(config.DriverPluginPath(), DriverPrepUprepFlockFileName)

	driver := &driver{
		config: config,
		state:  state,
		pulock: newFlock(puLockPath),
	}

	driver.draService = NewDRAService(driver, config)
	driver.registrationService = NewRegistrationService(config, driver.draService.SocketPath())
	driver.sliceSyncer = newResourceSliceSyncer(driver)
	driver.cleanupManager = newCheckpointCleanupManager(state, config, taskStore, state.devlib)

	return driver, nil
}

// Start 启动 driver。
//
// 包含四个逻辑:
//  1. draService:启动 ComputeDomainClique informer、监听 DRA socket
//     提供 NodePrepareResources / NodeUnprepareResources;
//  2. sliceSyncer:后台持续发布并调和 ResourceSlice(channel-0 设备);
//  3. registrationService:监听注册 socket,把 DRA socket 注册给 kubelet;
//  4. cleanupManager:周期对账 checkpoint 中 PrepareStarted 状态的僵尸
//     claim 并自愈。
//
// rollingUpdateUID 相关逻辑已去掉(注册 socket 固定为
// "<driverName>-reg.sock"),enableDeviceMetadata 相关逻辑也已去掉。
func (d *driver) Start(ctx context.Context) error {
	// DRA gRPC 服务先启动,kubelet 注册后立刻可用。
	if err := d.draService.Start(ctx); err != nil {
		return fmt.Errorf("error starting DRA service: %w", err)
	}

	// 后台持续发布/调和 ResourceSlice(被删/被改时自愈,多余 slice
	// 自动回收;发布失败不会导致插件退出,而是按间隔重试)。
	if err := d.sliceSyncer.start(ctx); err != nil {
		return fmt.Errorf("error starting ResourceSlice syncer: %w", err)
	}

	// 再启动注册服务,把 DRA socket 暴露给 kubelet。
	if err := d.registrationService.Start(ctx); err != nil {
		return fmt.Errorf("error starting registration service: %w", err)
	}

	// 主动恢复：检查 checkpoint 中的存量 Completed claim，恢复 Manager/Informer。
	// 失败不阻塞启动，由后续 cleanup 周期重试。
	d.tryRecoverManager(ctx)

	// 最后启动 checkpoint 清理管理器:周期对账 PrepareStarted 状态的
	// 僵尸 claim(claim 已删除/被替换时自发 Unprepare 自愈)。
	d.cleanupManager.recoverFn = d.tryRecoverManager
	if err := d.cleanupManager.Start(ctx, func(ctx context.Context, claimRef NamespacedObject) error {
		_, err := d.nodeUnprepareResource(ctx, claimRef)
		return err
	}, d.deleteExpiredPrepareAbortedClaimEntries, d.releaseAndCleanMapping); err != nil {
		return fmt.Errorf("error starting checkpoint cleanup manager: %w", err)
	}

	return nil
}

// Shutdown 停止 driver,顺序与 Start 相反。
func (d *driver) Shutdown() error {
	if d == nil {
		return nil
	}

	// 先停注册服务,kubelet 不再发起新请求。
	d.registrationService.Stop()

	// 再停 checkpoint 清理管理器(停止后台对账)。
	if err := d.cleanupManager.Stop(); err != nil {
		klog.Errorf("error stopping checkpoint cleanup manager: %v", err)
	}

	// 再停 ResourceSlice 调和器。
	d.sliceSyncer.stop()

	// 再停 DRA gRPC 服务。
	d.draService.Stop()

	if d.state.computeDomainManager != nil {
		if err := d.state.computeDomainManager.Stop(); err != nil {
			return fmt.Errorf("error stopping ComputeDomainManager: %w", err)
		}
	}

	return nil
}

// nodePrepareResource 为单个 claim 准备设备(pu.lock 保证节点级别互斥)。
//
// 返回值 done==true 表示处理完成(不论成功与否,不应重试);
// done==false 表示瞬态失败,workqueue 应按退避策略重新入队。
func (d *driver) nodePrepareResource(ctx context.Context, claim *resourceapi.ResourceClaim) (done bool, result PrepareResult) {
	ctx, cancel := context.WithTimeout(ctx, ErrorRetryMaxTimeout)
	defer cancel()

	release, err := d.pulock.acquire(ctx, 10*time.Second)
	if err != nil {
		return false, PrepareResult{Err: fmt.Errorf("error acquiring prep/unprep lock: %w", err)}
	}
	defer release()

	if claim.Status.Allocation == nil {
		return true, PrepareResult{Err: permanentError{fmt.Errorf("no allocation set in ResourceClaim %s in namespace %s", claim.Name, claim.Namespace)}}
	}

	devs, err := d.state.Prepare(ctx, claim)
	if err != nil {
		err = fmt.Errorf("error preparing devices for claim '%s': %w", ResourceClaimToString(claim), err)
		if isPermanentError(err) {
			klog.Infof("Permanent error preparing devices for claim %v: %v", claim.UID, err)
			return true, PrepareResult{Err: err}
		}
		return false, PrepareResult{Err: err}
	}

	klog.V(1).Infof("Prepared devices for claim '%s': %v", ResourceClaimToString(claim), devs)
	return true, PrepareResult{Devices: devs}
}

// nodeUnprepareResource 释放单个 claim 的设备(pu.lock 保证节点级别互斥)。
//
// 返回值 done==true 表示处理完成(不论成功与否,不应重试);
// done==false 表示瞬态失败,workqueue 应按退避策略重新入队。
func (d *driver) nodeUnprepareResource(ctx context.Context, claimRef NamespacedObject) (done bool, err error) {
	ctx, cancel := context.WithTimeout(ctx, ErrorRetryMaxTimeout)
	defer cancel()

	release, err := d.pulock.acquire(ctx, 10*time.Second)
	if err != nil {
		return false, fmt.Errorf("error acquiring prep/unprep lock: %w", err)
	}
	defer release()

	if err := d.state.Unprepare(ctx, claimRef); err != nil {
		err = fmt.Errorf("error unpreparing devices for claim '%v': %w", claimRef.String(), err)
		if isPermanentError(err) {
			klog.V(0).Infof("Permanent error unpreparing devices for claim %v: %v", claimRef.UID, err)
			return true, err
		}
		return false, err
	}

	klog.V(1).Infof("Unprepared devices for claim '%v'", claimRef.String())
	return true, nil
}

// releaseAndCleanMapping 在 pulock 内执行 ReleaseChannel + store.Delete。
// 用于孤儿映射清理回调。
func (d *driver) releaseAndCleanMapping(ctx context.Context, claimUID string) error {
	release, err := d.pulock.acquire(ctx, 10*time.Second)
	if err != nil {
		return fmt.Errorf("error acquiring prep/unprep lock: %w", err)
	}
	defer release()

	return d.state.devlib.ReleaseChannel(ctx, claimUID)
}

// deleteExpiredPrepareAbortedClaimEntries 删除 checkpoint 中 TTL 过期的
// PrepareAborted 条目（pu.lock 保证节点级别互斥）。
func (d *driver) deleteExpiredPrepareAbortedClaimEntries(ctx context.Context, now time.Time, ttl time.Duration) (int, error) {
	release, err := d.pulock.acquire(ctx, 10*time.Second)
	if err != nil {
		return 0, fmt.Errorf("error acquiring prep/unprep lock: %w", err)
	}
	defer release()

	return d.state.deleteExpiredPrepareAbortedClaimsFromCheckpoint(now, ttl)
}

// tryRecoverManager 尝试从 checkpoint 恢复 Manager/Informer。
// 失败仅记录日志，不阻塞启动流程。
func (d *driver) tryRecoverManager(ctx context.Context) {
	release, err := d.pulock.acquire(ctx, 10*time.Second)
	if err != nil {
		klog.Warningf("Manager recovery failed (cannot acquire pulock): %v", err)
		return
	}
	defer release()

	d.state.Mutex.Lock()
	defer d.state.Mutex.Unlock()

	if err := d.state.recoverManagerFromCheckpoint(ctx); err != nil {
		klog.Warningf("Manager recovery failed (will retry next cleanup cycle): %v", err)
	}
}
