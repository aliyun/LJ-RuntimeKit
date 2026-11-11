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
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
)

const (
	// resourceSliceResyncPeriod 是兜底的周期性调和间隔:即便没有任何
	// informer 事件(例如 slice 被外部删除但 watch 断连期间错过了事件),
	// 也定期把期望状态重新同步到集群。
	resourceSliceResyncPeriod = 10 * time.Minute

	// resourceSliceRetryDelay 是调和失败后的重试间隔(发布失败不会让
	// 插件退出,而是在此间隔后重试,与官方 resourceslice controller 的
	// rate-limited 重试语义一致)。
	resourceSliceRetryDelay = 30 * time.Second
)

// resourceSliceSyncer 持续把节点上可分配的 IMEX channel 设备同步成
// ResourceSlice,语义对齐 dra-driver-nvidia-gpu 通过
// k8s.io/dynamic-resource-allocation/resourceslice controller 达成的行为:
//
//   - informer 监听本 driver + 本节点的全部 ResourceSlice
//     (fieldSelector spec.driver / spec.nodeName);
//   - 期望的 slice 被误删或被外部篡改时自动恢复(自愈);
//   - 不属于期望的多余 slice(例如旧版本插件用 GenerateName 发布的、
//     或手工遗留的)被自动清理(孤儿回收);
//   - 创建/更新失败只记录错误并重试,不会导致插件启动失败退出;
//   - ResourceSlice 以 Node 为 owner,节点删除时由垃圾回收自动清理。
//
// 与官方 controller 相比有意简化的部分(单池单 slice 的确定性场景):
//   - 池名固定为节点名、slice 名固定为 "<DriverName>.<nodeName>",
//     Get-then-Create/Update 天然幂等,无需 MutationCache 防重复创建;
//   - 不支持多池/多 slice 的匹配与重新分组;
//   - 不使用 taints / sharedCounters / perDeviceNodeSelection 等
//     高级字段(也就无需处理 apiserver 丢弃字段的边角情况)。
type resourceSliceSyncer struct {
	driver *driver

	slices cache.SharedIndexInformer

	cancel context.CancelFunc
	wg     sync.WaitGroup

	// trigger 用于在收到 informer 事件后立即唤醒调和循环。
	trigger chan struct{}

	// nodeUID 是 Node 对象的 UID(首次调和时查询并缓存),
	// 用作 ResourceSlice 的 ownerReference。
	nodeUID types.UID
}

// newResourceSliceSyncer 创建 ResourceSlice 调和器。
func newResourceSliceSyncer(d *driver) *resourceSliceSyncer {
	return &resourceSliceSyncer{
		driver:  d,
		trigger: make(chan struct{}, 1),
	}
}

// start 启动 informer 与调和循环。
//
// 初始调和失败不会返回错误(与官方 controller 一致:发布是后台异步
// 进行的,失败会被记录并重试);只有 informer 本身起不来才报错。
func (s *resourceSliceSyncer) start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	s.cancel = cancel

	// 只监听本 driver 在本节点上发布的 ResourceSlice。
	selector := fields.Set{
		resourceapi.ResourceSliceSelectorDriver:   DriverName,
		resourceapi.ResourceSliceSelectorNodeName: s.driver.config.Flags.NodeName,
	}
	tweakListOptions := func(options *metav1.ListOptions) {
		options.FieldSelector = selector.String()
	}

	s.slices = cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
				tweakListOptions(&options)
				return s.driver.config.CoreClient.ResourceV1().ResourceSlices().List(ctx, options)
			},
			WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
				tweakListOptions(&options)
				return s.driver.config.CoreClient.ResourceV1().ResourceSlices().Watch(ctx, options)
			},
		},
		&resourceapi.ResourceSlice{},
		// 无需 resync:调和是幂等的,定期兜底由 reconcile 循环的
		// ticker 完成。
		0,
		cache.Indexers{},
	)

	if _, err := s.slices.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) { s.signal() },
		UpdateFunc: func(old, new any) {
			oldSlice, ok1 := old.(*resourceapi.ResourceSlice)
			newSlice, ok2 := new.(*resourceapi.ResourceSlice)
			if !ok1 || !ok2 || oldSlice.ResourceVersion == newSlice.ResourceVersion {
				return
			}
			s.signal()
		},
		DeleteFunc: func(obj any) { s.signal() },
	}); err != nil {
		cancel()
		return fmt.Errorf("error registering event handler on the ResourceSlice informer: %w", err)
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.slices.Run(ctx.Done())
	}()

	// 等待 informer 缓存完成初次同步;等不来则说明 apiserver 不可达,
	// 此时返回错误(后台会由上层决定退出重试)。
	if !cache.WaitForCacheSync(ctx.Done(), s.slices.HasSynced) {
		cancel()
		s.wg.Wait()
		return fmt.Errorf("error syncing ResourceSlice informer: %w", context.Cause(ctx))
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.run(ctx)
	}()

	return nil
}

// stop 停止调和器并等待全部 goroutine 退出。
func (s *resourceSliceSyncer) stop() {
	if s.cancel == nil {
		return
	}
	s.cancel()
	s.wg.Wait()
}

// signal 非阻塞地触发一次立即调和。
func (s *resourceSliceSyncer) signal() {
	select {
	case s.trigger <- struct{}{}:
	default:
	}
}

// run 是后台调和循环:初始立即调和一次,之后由 informer 事件或
// resync ticker 唤醒;失败则按 resourceSliceRetryDelay 重试。
func (s *resourceSliceSyncer) run(ctx context.Context) {
	retry := time.NewTimer(0)
	defer retry.Stop()

	for {
		if err := s.reconcile(ctx); err != nil {
			klog.Errorf("error reconciling ResourceSlice: %v", err)
			retry.Reset(resourceSliceRetryDelay)
		} else {
			retry.Reset(resourceSliceResyncPeriod)
		}

		select {
		case <-ctx.Done():
			return
		case <-s.trigger:
		case <-retry.C:
		}
	}
}

// reconcile 把期望状态同步到集群:确保期望的 ResourceSlice 存在且内容
// 正确,并删除同池中不属于期望的多余 slice。
//
// 该函数幂等,可安全地反复执行。
func (s *resourceSliceSyncer) reconcile(ctx context.Context) error {
	desired := s.driver.constructResourceSlice()
	if len(desired.Spec.Devices) == 0 {
		return fmt.Errorf("no devices set in ResourceSlice for driver %v", DriverName)
	}

	if err := s.ensureNodeUID(ctx); err != nil {
		return err
	}
	desired.OwnerReferences = []metav1.OwnerReference{
		{
			APIVersion: "v1",
			Kind:       "Node",
			Name:       s.driver.config.Flags.NodeName,
			UID:        s.nodeUID,
			Controller: ptr.To(true),
		},
	}

	// 从 informer 缓存里收集本 driver + 本节点当前的全部 slice。
	var (
		desiredExisting *resourceapi.ResourceSlice
		obsolete        []*resourceapi.ResourceSlice
	)
	for _, obj := range s.slices.GetStore().List() {
		slice, ok := obj.(*resourceapi.ResourceSlice)
		if !ok {
			continue
		}
		if slice.Name == desired.Name {
			desiredExisting = slice
			continue
		}
		// 同池(节点名)下名字不匹配期望的 slice 一律视为过时:
		// 可能来自旧版本插件(GenerateName 随机名)或人工遗留。
		if slice.Spec.Pool.Name == desired.Spec.Pool.Name {
			obsolete = append(obsolete, slice)
		}
	}

	if err := s.removeObsoleteSlices(ctx, obsolete); err != nil {
		return fmt.Errorf("remove obsolete ResourceSlices: %w", err)
	}

	if err := s.ensureResourceSlice(ctx, desired, desiredExisting); err != nil {
		return fmt.Errorf("ensure ResourceSlice %v: %w", desired.Name, err)
	}

	return nil
}

// ensureNodeUID 查询并缓存 Node 的 UID(ownerReference 使用)。
func (s *resourceSliceSyncer) ensureNodeUID(ctx context.Context) error {
	if s.nodeUID != "" {
		return nil
	}
	node, err := s.driver.config.CoreClient.CoreV1().Nodes().Get(ctx, s.driver.config.Flags.NodeName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("error retrieving Node %v: %w", s.driver.config.Flags.NodeName, err)
	}
	s.nodeUID = node.UID
	return nil
}

// removeObsoleteSlices 删除多余的 ResourceSlice。
//
// 删除时带上 UID + ResourceVersion 前置条件,防止删除期间对象已被
// 替换成别的实例;"not found" 视为成功。
func (s *resourceSliceSyncer) removeObsoleteSlices(ctx context.Context, slices []*resourceapi.ResourceSlice) error {
	client := s.driver.config.CoreClient.ResourceV1().ResourceSlices()

	for _, slice := range slices {
		options := metav1.DeleteOptions{
			Preconditions: &metav1.Preconditions{
				UID:             &slice.UID,
				ResourceVersion: &slice.ResourceVersion,
			},
		}
		klog.Infof("Deleting obsolete ResourceSlice '%v' for driver '%v'", slice.Name, DriverName)
		err := client.Delete(ctx, slice.Name, options)
		switch {
		case err == nil:
		case apierrors.IsNotFound(err):
			klog.V(1).Infof("Obsolete ResourceSlice '%v' was already deleted", slice.Name)
		default:
			return fmt.Errorf("delete ResourceSlice %v: %w", slice.Name, err)
		}
	}

	return nil
}

// ensureResourceSlice 确保期望的 ResourceSlice 存在且内容正确:
// 不存在则创建,存在但内容不一致则就地更新。
func (s *resourceSliceSyncer) ensureResourceSlice(ctx context.Context, desired, existing *resourceapi.ResourceSlice) error {
	client := s.driver.config.CoreClient.ResourceV1().ResourceSlices()

	if existing == nil {
		got, err := client.Get(ctx, desired.Name, metav1.GetOptions{})
		if err == nil {
			existing = got
		} else if !apierrors.IsNotFound(err) {
			return fmt.Errorf("get ResourceSlice %v: %w", desired.Name, err)
		}
	}

	if existing == nil {
		klog.Infof("Publishing ResourceSlice '%v' for driver '%v'", desired.Name, DriverName)
		if _, err := client.Create(ctx, desired, metav1.CreateOptions{}); err != nil {
			if apierrors.IsAlreadyExists(err) {
				return nil
			}
			return fmt.Errorf("create ResourceSlice %v: %w", desired.Name, err)
		}
		return nil
	}

	if s.sliceUpToDate(existing, desired) {
		return nil
	}

	klog.Infof("Updating ResourceSlice '%v' for driver '%v'", desired.Name, DriverName)
	updated := existing.DeepCopy()
	updated.Spec = *desired.Spec.DeepCopy()

	if updated.Spec.Pool.Generation < 1 {
		updated.Spec.Pool.Generation = 1
	}
	updated.OwnerReferences = desired.OwnerReferences
	if _, err := client.Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update ResourceSlice %v: %w", desired.Name, err)
	}

	return nil
}

func (s *resourceSliceSyncer) sliceUpToDate(existing, desired *resourceapi.ResourceSlice) bool {
	if !apiequality.Semantic.DeepEqual(existing.Spec.NodeName, desired.Spec.NodeName) ||
		existing.Spec.Pool.Name != desired.Spec.Pool.Name ||
		existing.Spec.Pool.ResourceSliceCount != desired.Spec.Pool.ResourceSliceCount ||
		len(existing.OwnerReferences) == 0 ||
		existing.OwnerReferences[0].UID != s.nodeUID {
		return false
	}
	return apiequality.Semantic.DeepEqual(existing.Spec.Devices, desired.Spec.Devices)
}

// constructResourceSlice 根据当前可分配设备构造期望的 ResourceSlice。
//
// 与 dra-driver-nvidia-gpu 的做法一致:
//   - 每个 driver 资源池以节点名命名(pool = node name);
//   - 只发布 channel-0(channel-1..N 作为网络资源由控制面或其他驱动
//     发布,因此此处显式跳过 ID != 0 的 channel)。
func (d *driver) constructResourceSlice() *resourceapi.ResourceSlice {
	nodeName := d.config.Flags.NodeName

	var devices []resourceapi.Device
	for _, device := range d.state.allocatable {
		// 显式跳过 channel-0 以外的 channel:它们作为网络资源由控制面
		// 或后续版本发布(与 NVIDIA 的做法保持一致)。
		if device.Type() == ComputeDomainChannelType && device.Channel.ID != 0 {
			continue
		}
		devices = append(devices, device.GetDevice())
	}

	resourceSlice := &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name: resourceSliceName(nodeName),
		},
		Spec: resourceapi.ResourceSliceSpec{
			Driver:   DriverName,
			NodeName: &nodeName,
			Pool: resourceapi.ResourcePool{
				Name:               nodeName,
				Generation:         1,
				ResourceSliceCount: 1,
			},
			Devices: devices,
		},
	}

	return resourceSlice
}

// resourceSliceName 返回 ResourceSlice 的确定性名字:
// "<DriverName>.<nodeName>"(同池同 driver 唯一)。
func resourceSliceName(nodeName string) string {
	return fmt.Sprintf("%s.%s", DriverName, nodeName)
}
