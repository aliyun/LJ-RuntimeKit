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
	"sync"
	"time"

	resourceapi "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	cgoresource "k8s.io/client-go/kubernetes/typed/resource/v1"
	"k8s.io/klog/v2"
)

// resourceClaimCleanupInterval 是 checkpoint 清理的周期:每隔该时长对账
// 一次 checkpoint 中所有 claim(与 NVIDIA 的 CheckpointCleanupManager
// 语义一致)。
const resourceClaimCleanupInterval = 10 * time.Minute

// PrepareAbortedClaimEntryTTL 是 PrepareAborted 条目在 checkpoint 中保留的
// 时间上限。过期后由 cleanup manager 删除。取 2×ErrorRetryMaxTimeout,
// 保证 kubelet workqueue 中的 stale Prepare 重试在 TTL 内一定能命中屏障。
const PrepareAbortedClaimEntryTTL = 2 * ErrorRetryMaxTimeout

// checkpointCleanupManager 周期性地把 checkpoint 中所有已 Prepare 的
// claim 与 API server 对账:claim 已不存在或 UID 已变(被删除后同名重建)
// 则调用 nodeUnprepareResource 自愈清理,防止:
//   - 僵尸条目永久残留在 checkpoint 文件里;
//   - 已释放/已消失的 claim 持续占用 channel,阻塞后续分配。
//
// 覆盖 PrepareStarted(Prepare 半途失败)和 PrepareCompleted(kubelet
// 崩溃后 claim 被外部删除)两种状态的僵尸条目。
type checkpointCleanupManager struct {
	waitGroup     sync.WaitGroup
	cancelContext context.CancelFunc
	queue         chan struct{}
	devicestate   *DeviceState
	resourceCli   resourceAPI
	taskStore     *claimTaskStore
	devlib        deviceLib

	// unprepfunc 在 Start 时注入，即 driver.nodeUnprepareResource。
	unprepfunc func(ctx context.Context, claimRef NamespacedObject) error

	// expiredEntryCleanupFn 在 Start 时注入，用于删除 TTL 过期的
	// PrepareAborted 条目，即 driver.deleteExpiredPrepareAbortedClaimEntries。
	expiredEntryCleanupFn func(ctx context.Context, now time.Time, ttl time.Duration) (int, error)

	// releaseAndCleanFunc 在 Start 时注入，在 pulock 内执行 Release + Delete。
	releaseAndCleanFunc func(ctx context.Context, claimUID string) error

	// recoverFn 由 driver 注入，每轮 cleanup 开始时调用，
	// 尝试从 checkpoint 恢复 Manager/Informer（幂等，已恢复时立即返回）。
	recoverFn func(ctx context.Context)
}

// resourceAPI 抽象清理管理器所需的 ResourceClaim 读接口(便于测试)。
//
// 注意:方法返回值必须用 client-go 的具体接口类型 cgoresource.
// ResourceClaimInterface(而非下方的 resourceClaimInterface),否则与
// ResourceV1Interface 的方法签名不匹配、无法直接赋值。
type resourceAPI interface {
	ResourceClaims(namespace string) cgoresource.ResourceClaimInterface
}

type resourceClaimInterface interface {
	Get(ctx context.Context, name string, options metav1.GetOptions) (*resourceapi.ResourceClaim, error)
}

// newCheckpointCleanupManager 创建清理管理器。
func newCheckpointCleanupManager(s *DeviceState, config *Config, taskStore *claimTaskStore, devlib deviceLib) *checkpointCleanupManager {
	// queue: 带缓冲的 channel，确保同一时刻至多入队一个清理任务。
	return &checkpointCleanupManager{
		devicestate: s,
		resourceCli: config.CoreClient.ResourceV1(),
		taskStore:   taskStore,
		devlib:      devlib,
		queue:       make(chan struct{}, 1),
	}
}

// Start 启动清理管理器。unprepfunc 即 driver.nodeUnprepareResource;
// expiredEntryCleanupFn 即 driver.deleteExpiredPrepareAbortedClaimEntries。
func (m *checkpointCleanupManager) Start(
	ctx context.Context,
	unprepfunc func(ctx context.Context, claimRef NamespacedObject) error,
	expiredEntryCleanupFn func(ctx context.Context, now time.Time, ttl time.Duration) (int, error),
	releaseAndCleanFunc func(ctx context.Context, claimUID string) error,
) error {
	ctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	m.cancelContext = cancel
	m.unprepfunc = unprepfunc
	m.expiredEntryCleanupFn = expiredEntryCleanupFn
	m.releaseAndCleanFunc = releaseAndCleanFunc

	m.waitGroup.Add(1)
	go func() {
		defer m.waitGroup.Done()
		// 生产者:周期性投递清理任务。
		m.triggerPeriodically(ctx)
	}()

	m.waitGroup.Add(1)
	go func() {
		defer m.waitGroup.Done()
		// 消费者:执行清理任务。
		m.worker(ctx)
	}()

	klog.V(6).Infof("CheckpointCleanupManager started")
	return nil
}

// Stop 停止清理管理器并等待 goroutine 退出。
func (m *checkpointCleanupManager) Stop() error {
	if m.cancelContext != nil {
		m.cancelContext()
	}
	m.waitGroup.Wait()
	return nil
}

// cleanup 是高层清理例程:插件启动时执行一次,之后周期执行。
// 取出 checkpoint 中所有 claim,逐个与 API server 对账(unprepareIfStale)。
// 每次调用都是尽力而为:错误不向上传播(只记录日志)。
//
// 注意:本函数读 checkpoint 时不持有 DeviceState 锁:
//  1. 初次读 checkpoint 只是为了发现候选 claim,不需要强一致;
//     并发写导致的遗漏由下一个周期兜底;
//  2. 每个 candidate 与 API server(权威事实源)核对,而非本地状态;
//  3. 真正的 checkpoint 变更发生在 nodeUnprepareResource 里,它会正确地
//     持有 pulock 完成原子的读-改-写。
func (m *checkpointCleanupManager) cleanup(ctx context.Context) {
	// 尝试恢复 Manager（幂等，已恢复时 short-circuit）。
	if m.recoverFn != nil {
		m.recoverFn(ctx)
	}

	cp, err := m.devicestate.getCheckpoint()
	if err != nil {
		klog.Errorf("Checkpointed RC cleanup: unable to get checkpoint: %s", err)
		return
	}

	filtered := make(PreparedClaimsByUID)
	for uid, claim := range cp.V2.PreparedClaims {
		// PrepareAborted 条目由 TTL 过期清理,不参与 staleness 检查。
		if claim.CheckpointState == ClaimCheckpointStatePrepareAborted {
			continue
		}
		filtered[uid] = claim
	}

	klog.V(4).Infof("Checkpointed RC cleanup: claims to check for staleness: %d", len(filtered))

	for cpuid, cpclaim := range filtered {
		m.unprepareIfStale(ctx, cpuid, cpclaim)
	}

	// 删除 TTL 过期的 PrepareAborted 条目。
	expiredEntries, err := m.expiredEntryCleanupFn(ctx, time.Now(), PrepareAbortedClaimEntryTTL)
	if err != nil {
		klog.Warningf("Checkpointed RC cleanup: unable to delete expired PrepareAborted claim entries: %s", err)
		return
	}
	if expiredEntries > 0 {
		klog.V(4).Infof("Checkpointed RC cleanup: deleted expired PrepareAborted claim entries: %d", expiredEntries)
	}

	// 映射孤儿清理：检查 claim-task store 中是否有已失效的映射条目。
	m.cleanupOrphanMappings(ctx, cp)
}

// unprepareIfStale 检测 claim 是否已经失效(API server 中不存在)。
// 失效则调用 unprepare 清理。
//
// 按 UID 精确查找 API server 有两种方式:按 metadata.uid 的 FieldSelector
// List(跨 namespace 过于昂贵,不能例行执行),或按 name/namespace Get 后
// 比较 UID(廉价)。因此 checkpoint 中必须存有 name/namespace。
func (m *checkpointCleanupManager) unprepareIfStale(ctx context.Context, cpuid string, cpclaim PreparedClaim) {
	if cpclaim.Name == "" {
		klog.V(4).Infof("Checkpointed RC cleanup: skip checkpointed claim '%s': RC name not in checkpoint", cpuid)
		return
	}

	claim, err := m.getClaimByName(ctx, cpclaim.Name, cpclaim.Namespace)
	if err != nil && apierrors.IsNotFound(err) {
		klog.V(4).Infof(
			"Checkpointed RC cleanup: partially prepared claim '%s/%s:%s' is stale: not found in API server",
			cpclaim.Namespace, cpclaim.Name, cpuid)
		m.unprepare(ctx, cpuid, cpclaim)
		return
	}

	// 查询 API server 的瞬态错误:无需显式重试,下个周期隐式重试。
	if err != nil {
		klog.Infof("Checkpointed RC cleanup: skip for checkpointed claim %s: getClaimByName failed (retry later): %s", cpuid, err)
		return
	}

	if string(claim.UID) != cpuid {
		// 同 namespace 下同名 claim 的 UID 不同,说明旧对象已删除、
		// 新对象同名重建:checkpoint 里的条目已失效。
		klog.V(4).Infof("Checkpointed RC cleanup: partially prepared claim '%s/%s' is stale: UID changed (checkpoint: %s, API server: %s)", cpclaim.Namespace, cpclaim.Name, cpuid, claim.UID)
		m.unprepare(ctx, cpuid, cpclaim)
		return
	}

	klog.V(4).Infof("Checkpointed RC cleanup: partially prepared claim not stale: %s", ResourceClaimToString(claim))
}

// unprepare 对失效 claim 发起"自发 Unprepare"。
// 预期副作用:Unprepare 成功后该 claim 从 checkpoint 中移除。
func (m *checkpointCleanupManager) unprepare(ctx context.Context, uid string, claim PreparedClaim) {
	claimRef := NamespacedObject{UID: uid}
	claimRef.NamespacedName.Namespace = claim.Namespace
	claimRef.NamespacedName.Name = claim.Name

	// 执行一次 Unprepare 尝试即可:跨周期的隐式重试已足够。
	// 从 checkpoint 删除条目的动作依赖 Unprepare() 自身完成。
	if err := m.unprepfunc(ctx, claimRef); err != nil {
		klog.Warningf("Checkpointed RC cleanup: error during unprepare for %s (retried later): %s", claimRef.String(), err)
		return
	}

	klog.Infof("Checkpointed RC cleanup: unprepared stale claim: %s", claimRef.String())
}

// getClaimByName 直接从 API server 读取 ResourceClaim。
func (m *checkpointCleanupManager) getClaimByName(ctx context.Context, name string, ns string) (*resourceapi.ResourceClaim, error) {
	// 该 API 调用应当低延迟;超时视为病态,直接放弃本次。
	childctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	claim, err := m.resourceCli.ResourceClaims(ns).Get(childctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("error getting resource claim %s/%s: %w", name, ns, err)
	}

	return claim, nil
}

// worker 持续消费清理任务,直到 context 被取消。
func (m *checkpointCleanupManager) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.queue:
			m.cleanup(ctx)
		}
	}
}

// triggerPeriodically 立即执行一次清理，然后周期性投递清理任务。
func (m *checkpointCleanupManager) triggerPeriodically(ctx context.Context) {
	ticker := time.NewTicker(resourceClaimCleanupInterval)
	defer ticker.Stop()

	m.cleanup(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			select {
			case m.queue <- struct{}{}:
				klog.V(6).Infof("Checkpointed RC cleanup: task submitted")
			default:
				// 上一轮清理仍在进行(不正常)，跳过本轮。
				klog.Warningf("Checkpointed RC cleanup: ongoing, skipped")
			}
		}
	}
}

// cleanupOrphanMappings 清理 claim-task store 中已失效的映射条目。
// 在现有 checkpoint 清理之后调用，尽力而为。
func (m *checkpointCleanupManager) cleanupOrphanMappings(ctx context.Context, cp *Checkpoint) {
	entries, err := m.taskStore.ListAll()
	if err != nil {
		klog.Warningf("Orphan mapping cleanup: failed to list claim-task entries: %v", err)
		return
	}
	if len(entries) == 0 {
		return
	}

	// 收集 checkpoint 中仍活跃的 claim UID。
	activeClaims := make(map[string]bool)
	if cp != nil && cp.V2 != nil {
		for uid, pc := range cp.V2.PreparedClaims {
			if pc.CheckpointState == ClaimCheckpointStatePrepareCompleted ||
				pc.CheckpointState == ClaimCheckpointStatePrepareStarted {
				activeClaims[uid] = true
			}
		}
	}

	for claimUID, entry := range entries {
		if activeClaims[claimUID] {
			continue
		}

		if entry.Name == "" {
			klog.V(4).Infof("Orphan mapping cleanup: skip entry %s: no claim name", claimUID)
			continue
		}

		claim, err := m.getClaimByName(ctx, entry.Name, entry.Namespace)
		if err != nil && apierrors.IsNotFound(err) {
			klog.V(4).Infof("Orphan mapping cleanup: claim %s/%s not found, releasing", entry.Namespace, entry.Name)
			if relErr := m.releaseAndCleanFunc(ctx, claimUID); relErr != nil {
				klog.Warningf("Orphan mapping cleanup: failed to release claim %s: %v", claimUID, relErr)
			}
			continue
		}
		if err != nil {
			klog.V(4).Infof("Orphan mapping cleanup: skip claim %s: API error (retry later): %v", claimUID, err)
			continue
		}

		if string(claim.UID) != claimUID {
			klog.V(4).Infof("Orphan mapping cleanup: claim %s/%s UID mismatch (store=%s, API=%s), releasing",
				entry.Namespace, entry.Name, claimUID, claim.UID)
			if relErr := m.releaseAndCleanFunc(ctx, claimUID); relErr != nil {
				klog.Warningf("Orphan mapping cleanup: failed to release claim %s: %v", claimUID, relErr)
			}
			continue
		}

		klog.V(6).Infof("Orphan mapping cleanup: claim %s still valid, keeping", claimUID)
	}
}
