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
	"errors"
	"fmt"
	"hash/fnv"
	"maps"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
	nvinformers "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/nvidia.com/informers/externalversions/resource/v1beta1"
)

const (
	// informerResyncPeriod 是 ComputeDomainClique informer 的 resync 周期。
	informerResyncPeriod = 10 * time.Minute

	// assertReadyTimeout / assertReadyRetryInterval 控制等待 Clique 就绪
	// 的最大时长与重试间隔。
	assertReadyTimeout       = 30 * time.Second
	assertReadyRetryInterval = 500 * time.Millisecond
)

// ComputeDomainManager 监听 ComputeDomainClique 变化(informer),并在变化时
// 调 RuntimeKit 渲染 nodes.cfg;同时负责在 Prepare / Unprepare 时创建 /
// 删除 Clique 条目。
type ComputeDomainManager struct {
	config   *Config
	devlib   deviceLib
	cliqueID string
	informer cache.SharedIndexInformer

	waitGroup      sync.WaitGroup
	cancelContext  context.CancelFunc
	previousIPMap  map[string]map[string]string // cliqueName → (Index→IP 映射缓存)
	ipMapMu        sync.Mutex
	activeDomainID string
	stopped        atomic.Bool
}

// newComputeDomainManager 创建 ComputeDomainManager(模仿 NVIDIA 的
// NewComputeDomainManager)。
func newComputeDomainManager(config *Config, cliqueID string, devlib deviceLib) *ComputeDomainManager {
	m := &ComputeDomainManager{
		config:        config,
		devlib:        devlib,
		cliqueID:      cliqueID,
		previousIPMap: make(map[string]map[string]string),
	}
	m.informer = m.newComputeDomainCliqueInformer()
	return m
}

// newComputeDomainCliqueInformer 基于 NVIDIA typed informer 构造
// ComputeDomainClique 的 SharedIndexInformer。
func (m *ComputeDomainManager) newComputeDomainCliqueInformer() cache.SharedIndexInformer {
	labelSelector := fmt.Sprintf("%s=%s", CliqueLabelClique, m.cliqueID)

	informer := nvinformers.NewFilteredComputeDomainCliqueInformer(
		m.config.NvidiaCli,
		m.config.Flags.Namespace,
		informerResyncPeriod,
		cache.Indexers{},
		func(opts *metav1.ListOptions) {
			opts.LabelSelector = labelSelector
		},
	)

	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			m.onCliqueChange(obj)
		},
		UpdateFunc: func(_, obj any) {
			m.onCliqueChange(obj)
		},
		DeleteFunc: func(obj any) {
			if clique, ok := obj.(*nvapi.ComputeDomainClique); ok {
				klog.V(2).Infof("ComputeDomainClique deleted: %v", clique.Name)
				m.ipMapMu.Lock()
				delete(m.previousIPMap, clique.Name)
				m.ipMapMu.Unlock()
			}
		},
	})

	return informer
}

// onCliqueChange 直接 type assert ComputeDomainClique,检测 IP 变化并
// 构建 DNS 映射通知 RuntimeKit 刷新 IMEX 配置。
func (m *ComputeDomainManager) onCliqueChange(obj any) {
	if m.stopped.Load() {
		return
	}

	clique, ok := obj.(*nvapi.ComputeDomainClique)
	if !ok {
		klog.Errorf("unexpected informer object type: %T", obj)
		return
	}

	// 防御性 namespace 校验。
	if clique.Namespace != "" && clique.Namespace != m.config.Flags.Namespace {
		return
	}

	// 只处理本节点参与的 Clique（informer 不再按 node 过滤）。
	if !isDaemonMember(clique.Daemons, m.config.Flags.NodeName) {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 检测 IP 变化。
	newIPMap := buildIPMap(clique.Daemons)
	m.ipMapMu.Lock()
	oldIPMap := m.previousIPMap[clique.Name]
	if oldIPMap != nil && maps.Equal(oldIPMap, newIPMap) {
		m.ipMapMu.Unlock()
		// IP 映射未变化，无需刷新。
		return
	}
	m.ipMapMu.Unlock()

	// 构建 DNS 映射并通知 RuntimeKit 刷新 IMEX 配置。
	if err := m.RestartIMEX(ctx, clique); err != nil {
		klog.Errorf("error refreshing IMEX for clique %s: %v", clique.Name, err)
		// 失败不更新缓存，下次事件仍可检测到变化并重试。
		return
	}
	m.ipMapMu.Lock()
	m.previousIPMap[clique.Name] = newIPMap
	m.ipMapMu.Unlock()
}

// isDaemonMember 判断指定节点是否在 Daemons 列表中。
func isDaemonMember(daemons []*nvapi.ComputeDomainDaemonInfo, nodeName string) bool {
	for _, d := range daemons {
		if d.NodeName == nodeName {
			return true
		}
	}
	return false
}

// buildIPMap 从 Daemons 列表构建 Index→IP 映射。
func buildIPMap(daemons []*nvapi.ComputeDomainDaemonInfo) map[string]string {
	m := make(map[string]string, len(daemons))
	for _, d := range daemons {
		m[fmt.Sprint(d.Index)] = d.IPAddress
	}
	return m
}

// RestartIMEX 构建 DNS 名称映射并通知 RuntimeKit 刷新 IMEX 配置。
// 模仿社区 dnsNameManager 的逻辑：为 [0, maxNodesPerIMEXDomain) 的每个 slot
// 生成 DNS 名称，有 daemon 的 slot 映射到其 IP，其余映射到 SentinelIPAddress。
func (m *ComputeDomainManager) RestartIMEX(ctx context.Context, clique *nvapi.ComputeDomainClique) error {
	mappings, err := m.buildDNSNameMappings(clique)
	if err != nil {
		return fmt.Errorf("error building DNS name mappings: %w", err)
	}
	return m.devlib.RefreshIMEX(ctx, mappings)
}

// buildDNSNameMappings 为每个 maxNodesPerIMEXDomain slot 构建一个 DNS 名称映射。
func (m *ComputeDomainManager) buildDNSNameMappings(clique *nvapi.ComputeDomainClique) ([]DNSNameMapping, error) {
	//maxNodes := m.config.Flags.MaxNodesPerIMEXDomain

	// 按 Index 索引本 Clique 中的 daemon。
	seenByIndex := make(map[int]struct{})
	domainList := make([]*nvapi.ComputeDomainDaemonInfo, 0)
	for _, daemon := range clique.Daemons {
		if daemon.CliqueID != m.cliqueID {
			continue
		}
		if _, exists := seenByIndex[daemon.Index]; exists {
			return nil, fmt.Errorf("multiple daemons at index %d in clique %q (and %s)",
				daemon.Index, clique.Name, daemon.NodeName)
		}
		seenByIndex[daemon.Index] = struct{}{}
		domainList = append(domainList, daemon)
	}

	// 生成 DNS 名称格式（参考社区 computeDomainHash + dnsNameFormat）。
	domainID := ""
	if parts := strings.SplitN(clique.Name, ".", 2); len(parts) >= 1 {
		domainID = parts[0]
	}
	klog.V(2).Infof("building DNS name mappings for clique %q (domainID %q)", clique.Name, domainID)

	mappings := make([]DNSNameMapping, 0)
	for i := 0; i < len(domainList); i++ {
		mappings = append(mappings, DNSNameMapping{
			Index:    i,
			DomainID: domainID,
			IP:       domainList[i].IPAddress,
		})
	}
	return mappings, nil
}

const dnsNamePrefix = "compute-domain-daemon-"

// dnsNameFormat 返回 DNS 名称的格式字符串，如 "compute-domain-daemon-bcs2h4g-%04d"。
// domainHash 从 Clique 名称中解析出 domainID 后用 FNV-32a 哈希生成。
func (m *ComputeDomainManager) dnsNameFormat(clique *nvapi.ComputeDomainClique) string {
	// 从 Clique name "<domainID>.<cliqueID>" 中提取 domainID。
	domainID := ""
	if parts := strings.SplitN(clique.Name, ".", 2); len(parts) >= 1 {
		domainID = parts[0]
	}
	return dnsNamePrefix + computeDomainHash(domainID) + "-%04d"
}

// computeDomainHash 对 ComputeDomain UID 做 FNV-32a 哈希，生成 DNS-label-safe 的短字符串。
func computeDomainHash(cdUID string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(cdUID))
	return fmt.Sprintf("%x", h.Sum32())
}

// Start 启动 informer 并等待缓存同步。
func (m *ComputeDomainManager) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	m.cancelContext = cancel

	m.waitGroup.Add(1)
	go func() {
		defer m.waitGroup.Done()
		m.informer.Run(ctx.Done())
	}()

	syncCtx, syncCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer syncCancel()
	if !cache.WaitForCacheSync(syncCtx.Done(), m.informer.HasSynced) {
		m.stopInformer()
		return fmt.Errorf("timed out waiting for informer cache sync")
	}

	return nil
}

// stopInformer 仅停止 informer goroutine，不做远程 Clique 清理。
func (m *ComputeDomainManager) stopInformer() {
	if m.cancelContext != nil {
		m.cancelContext()
	}
	m.waitGroup.Wait()
}

// shutdown 先标记 stopped 防止 informer 回调竞态，再停止 informer。
func (m *ComputeDomainManager) shutdown() {
	m.stopped.Store(true)
	m.stopInformer()
}

// Stop 停止 informer，并在退出前清理远程 Clique 条目。
func (m *ComputeDomainManager) Stop() error {
	m.shutdown()
	return nil
}

// cliqueName 返回 "<computeDomainUID>.<cliqueID>" 形式的 Clique 名字。
func (m *ComputeDomainManager) cliqueName(domainID string) string {
	return fmt.Sprintf("%s.%s", domainID, m.cliqueID)
}

// AddCliqueEntry 在 Prepare 流程中创建 Clique 条目:若 ComputeDomainClique
// 不存在则创建,并把本节点加入 Daemons 列表(幂等)。
func (m *ComputeDomainManager) AddCliqueEntry(ctx context.Context, domainID string, cliqueID string) error {
	name := fmt.Sprintf("%s.%s", domainID, cliqueID)

	ipAddress, err := m.getNodeInternalIP(ctx)
	if err != nil {
		return fmt.Errorf("error getting node internal IP: %w", err)
	}

	// mutateFn 追加本节点 daemon（update / AlreadyExists fallback 共用）。
	mutateFn := func(c *nvapi.ComputeDomainClique) (bool, error) {
		for _, d := range c.Daemons {
			if d.NodeName == m.config.Flags.NodeName {
				return false, nil // 已存在，幂等
			}
		}
		index, indexErr := nextDaemonIndex(c.Daemons, m.config.Flags.MaxNodesPerIMEXDomain)
		if indexErr != nil {
			return false, fmt.Errorf("error allocating daemon index: %w", indexErr)
		}
		c.Daemons = append(c.Daemons, &nvapi.ComputeDomainDaemonInfo{
			NodeName:  m.config.Flags.NodeName,
			IPAddress: ipAddress,
			CliqueID:  cliqueID,
			Index:     index,
			Status:    ComputeDomainStatusNotReady,
		})
		return true, nil
	}

	// 尝试更新已有 Clique（追加本节点）。
	err = m.updateCliqueWithRetry(ctx, name, mutateFn)

	// Clique 不存在时创建。
	if errors.Is(err, errCliqueNotFound) {
		clique := &nvapi.ComputeDomainClique{
			TypeMeta: metav1.TypeMeta{
				APIVersion: ComputeDomainCliqueGroup + "/" + ComputeDomainCliqueVersion,
				Kind:       "ComputeDomainClique",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: m.config.Flags.Namespace,
				Labels: map[string]string{
					CliqueLabelDomain: domainID,
					CliqueLabelClique: cliqueID,
				},
			},
			Daemons: []*nvapi.ComputeDomainDaemonInfo{
				{
					NodeName:  m.config.Flags.NodeName,
					IPAddress: ipAddress,
					CliqueID:  cliqueID,
					Index:     0,
					Status:    ComputeDomainStatusNotReady,
				},
			},
		}

		_, createErr := m.config.NvidiaCli.ResourceV1beta1().ComputeDomainCliques(m.config.Flags.Namespace).Create(ctx, clique, metav1.CreateOptions{})
		if createErr != nil {
			if apierrors.IsAlreadyExists(createErr) {
				// 并发竞态：另一个节点已创建该 Clique，回退到 update 路径追加 daemon。
				if err := m.updateCliqueWithRetry(ctx, name, mutateFn); err != nil {
					return err
				}
				m.activeDomainID = domainID
				return nil
			}
			return fmt.Errorf("error creating ComputeDomainClique %v: %w", name, createErr)
		}
		klog.V(2).Infof("Created ComputeDomainClique entry: %v", name)
		err = nil
	}

	if err == nil {
		m.activeDomainID = domainID
	}
	return err
}

// RemoveCliqueEntry 在 Unprepare 流程中删除 Clique 条目:把本节点从
// Daemons 列表移除,列表为空时删除整个 ComputeDomainClique(幂等)。
func (m *ComputeDomainManager) RemoveCliqueEntry(ctx context.Context, domainID string) error {
	name := m.cliqueName(domainID)

	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		// 1. 每轮重新 Get 最新对象
		existing, err := m.getComputeDomainCliqueFromAPIServer(ctx, name)
		if err != nil {
			return err
		}
		if existing == nil {
			return nil // 对象不存在，幂等成功
		}

		// 2. 检查本节点是否在成员列表中
		var keep []*nvapi.ComputeDomainDaemonInfo
		for _, d := range existing.Daemons {
			if d.NodeName != m.config.Flags.NodeName {
				keep = append(keep, d)
			}
		}
		if len(keep) == len(existing.Daemons) {
			return nil // 本节点不在列表中，幂等成功
		}

		// 3. 移除后仍有其他成员 → Update
		if len(keep) > 0 {
			clone := existing.DeepCopyObject().(*nvapi.ComputeDomainClique)
			clone.Daemons = keep
			_, err = m.config.NvidiaCli.ResourceV1beta1().ComputeDomainCliques(m.config.Flags.Namespace).Update(ctx, clone, metav1.UpdateOptions{})
			if apierrors.IsNotFound(err) {
				return nil // 并发被删，幂等成功
			}
			return err // Conflict 会触发下一轮重试
		}

		// 4. 无剩余成员 → Delete，携带 UID + ResourceVersion 前置条件
		uid := existing.UID
		rv := existing.ResourceVersion
		err = m.config.NvidiaCli.ResourceV1beta1().ComputeDomainCliques(m.config.Flags.Namespace).Delete(ctx, name, metav1.DeleteOptions{
			Preconditions: &metav1.Preconditions{
				UID:             &uid,
				ResourceVersion: &rv,
			},
		})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if apierrors.IsConflict(err) {
			return err // 触发重试（可能有新成员加入）
		}
		return err
	})

	if err == nil {
		m.ipMapMu.Lock()
		delete(m.previousIPMap, name)
		m.ipMapMu.Unlock()
	}
	return err
}

// UpdateDaemonStatus 更新本节点在 Clique 中的状态（如 Ready / NotReady）。
// 状态更新为尽力而为（eventual consistency），失败不阻塞主流程。
func (m *ComputeDomainManager) UpdateDaemonStatus(ctx context.Context, domainID string, status string) error {
	name := m.cliqueName(domainID)
	return m.updateCliqueWithRetry(ctx, name, func(c *nvapi.ComputeDomainClique) (bool, error) {
		for i, d := range c.Daemons {
			if d.NodeName == m.config.Flags.NodeName {
				if d.Status == status {
					return false, nil
				}
				c.Daemons[i].Status = status
				return true, nil
			}
		}
		return false, fmt.Errorf("daemon entry for node %v not found in ComputeDomainClique %v", m.config.Flags.NodeName, name)
	})
}

var (
	errCliqueNotFound = errors.New("clique not found")
)

// updateCliqueWithRetry 用 Get→DeepCopy→mutate→Update + RetryOnConflict 模式更新 Clique。
// mutateFn 在 DeepCopy 后的副本上做修改，返回 (true, nil) 表示有变化需要 Update，
// (false, nil) 表示无变化跳过，(_, err) 表示错误中止。
func (m *ComputeDomainManager) updateCliqueWithRetry(ctx context.Context, name string, mutateFn func(*nvapi.ComputeDomainClique) (bool, error)) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		clique, err := m.getComputeDomainCliqueFromAPIServer(ctx, name)
		if err != nil {
			return err
		}
		if clique == nil {
			return errCliqueNotFound
		}

		clone := clique.DeepCopyObject().(*nvapi.ComputeDomainClique)
		changed, err := mutateFn(clone)
		if err != nil {
			return err
		}
		if !changed {
			return nil
		}

		_, err = m.config.NvidiaCli.ResourceV1beta1().ComputeDomainCliques(m.config.Flags.Namespace).Update(ctx, clone, metav1.UpdateOptions{})
		return err
	})
}

// AssertComputeDomainReady 确认 Clique 中本节点的 daemon 已 Ready。
//
// daemon 的 Ready 状态由外部维护(compute-domain-controller 或
// RuntimeKit 上报),此处仅做等待式校验。
func (m *ComputeDomainManager) AssertComputeDomainReady(ctx context.Context, domainID string) error {
	deadline := time.Now().Add(assertReadyTimeout)
	for {
		clique, err := m.getComputeDomainCliqueFromAPIServer(ctx, m.cliqueName(domainID))
		if err != nil {
			// 瞬态错误:继续在超时窗口内重试,但记录日志(否则会
			// 静默耗尽 30s 等待窗口,难以定位)。
			klog.Warningf("error getting ComputeDomainClique %v (retrying): %v", m.cliqueName(domainID), err)
		} else if clique != nil {
			for _, daemon := range clique.Daemons {
				if daemon.NodeName == m.config.Flags.NodeName {
					if daemon.Status == ComputeDomainStatusReady {
						return nil
					}
					break
				}
			}
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("current node not ready in ComputeDomainClique %v", m.cliqueName(domainID))
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled waiting for ComputeDomainClique %v readiness: %w", m.cliqueName(domainID), ctx.Err())
		case <-time.After(assertReadyRetryInterval):
			// retry
		}
	}
}

// getComputeDomainCliqueFromAPIServer 直接从 API server 读取
// ComputeDomainClique(不走 informer 缓存,确保读到最新状态)。
//
// 返回值约定:Clique 不存在时返回 (nil, nil);其余错误(网络、权限等)
// 全部上抛,由调用方决定重试或失败——瞬态错误绝不能被当成"不存在"
// 处理,否则会导致 Clique 条目静默泄漏。
func (m *ComputeDomainManager) getComputeDomainCliqueFromAPIServer(ctx context.Context, name string) (*nvapi.ComputeDomainClique, error) {
	clique, err := m.config.NvidiaCli.ResourceV1beta1().ComputeDomainCliques(m.config.Flags.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("error getting ComputeDomainClique %v: %w", name, err)
	}
	return clique, nil
}

// nextDaemonIndex 为新 daemon 计算最小空闲 Index(补洞式分配)。
//
// 不能直接用 len(Daemons):并发 Prepare(多节点同时加入同一 Clique)
// 或历史上发生过节点退出时,都会出现 len 与实际 Index 集合不一致,导致
// 不同节点撞号——(CliqueID, Index) 二元组必须全局唯一,它直接决定
// IMEX 域内 IP 到 DNS 名的映射。
func nextDaemonIndex(daemons []*nvapi.ComputeDomainDaemonInfo, maxNodes int) (int, error) {
	used := make(map[int]bool)
	for _, d := range daemons {
		used[d.Index] = true
	}
	for i := 0; i < maxNodes; i++ {
		if !used[i] {
			return i, nil
		}
	}
	return -1, fmt.Errorf("no available index within maxNodesPerIMEXDomain (%d)", maxNodes)
}

// getNodeInternalIP 返回本节点 InternalIP。
func (m *ComputeDomainManager) getNodeInternalIP(ctx context.Context) (string, error) {
	node, err := m.config.CoreClient.CoreV1().Nodes().Get(ctx, m.config.Flags.NodeName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("error retrieving Node: %w", err)
	}

	for _, address := range node.Status.Addresses {
		if address.Type == "InternalIP" {
			return address.Address, nil
		}
	}
	return "", fmt.Errorf("no InternalIP found for node %v", m.config.Flags.NodeName)
}
