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
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/lj-runtimekit/device-plugin/internal/utils"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

var (
	evictGraceSeconds = int64(3)               // DeleteOptions.GracePeriodSeconds for the graceful delete
	evictGraceWait    = 10 * time.Second       // bounded wait for graceful exit; exceeds the Pod's 5s grace period
	evictForceWait    = 3 * time.Second        // bounded wait after a force delete before failing closed
	evictPollInterval = 300 * time.Millisecond // poll interval while waiting for minors to clear
)

var (
	// evictRegistryMu guards minorLocks.
	evictRegistryMu sync.Mutex
	// minorLocks serializes eviction per GPU minor across all plugin instances on the node. A
	// caller locks every target minor in ascending order (deadlock-free) and releases each the
	// moment the shared poll sees it clear, so a stuck minor never holds its healthy peers' locks.
	minorLocks = make(map[int]*sync.Mutex)
)

// targetMinorsFor returns the GPU minors a business Pod will occupy, mirroring xdmAllocate's
// source: an lj-scheduled Pod's ALIYUN_COM_GPU_IDX_LIST is authoritative, else the kubelet device ids.
func (dp *XpuDevicePlugin) targetMinorsFor(pod *v1.Pod, deviceIDs []string) ([]int, error) {
	if ids, err := utils.GetScheduledDeviceIds(pod); err == nil && len(ids) > 0 {
		return ids, nil
	}
	return utils.GetDeviceMinorsFromIDs(dp.deviceMeta, deviceIDs)
}

// Before a business Pod's minors go to xdm, any detection Pod still pinned to them is deleted (grace
// -> force) and the DP waits for them to clear.
func (dp *XpuDevicePlugin) evictActiveMonitoringPodsOnMinors(ctx context.Context, minors []int) error {
	if len(minors) == 0 {
		return nil
	}

	hits, err := dp.matchActiveMonitoringOnMinors(minors)
	if err != nil {
		return err
	}
	if len(hits) == 0 {
		return nil
	}

	uniq := sortedUnique(minors)
	held := lockMinors(uniq)
	defer func() {
		for _, unlock := range held {
			unlock()
		}
	}()

	klog.Infof("pre-start container evicting %d activemonitoring pod(s) pinned to minors %v", len(hits), uniq)
	dp.deleteActiveMonitoringPods(ctx, hits, evictGraceSeconds)
	pinned := dp.waitReleaseClear(ctx, uniq, held, evictGraceWait)
	if len(pinned) == 0 {
		return nil
	}

	dp.deleteActiveMonitoringPods(ctx, utils.MatchActiveMonitoringByMinor(hits, pinned), 0)
	pinned = dp.waitReleaseClear(ctx, pinned, held, evictForceWait)
	if len(pinned) == 0 {
		return nil
	}

	return fmt.Errorf("activemonitoring pod(s) still pinned to minors %v after force delete, refusing to allocate", pinned)
}

// matchActiveMonitoringOnMinors returns the node's detection Pods pinned to any of the given minors.
func (dp *XpuDevicePlugin) matchActiveMonitoringOnMinors(minors []int) ([]*v1.Pod, error) {
	pods, err := utils.ListActiveMonitoringPodsOnNode(dp.K8sClient)
	if err != nil {
		return nil, fmt.Errorf("list activemonitoring pods failed: %w", err)
	}
	return utils.MatchActiveMonitoringByMinor(pods, minors), nil
}

// waitReleaseClear runs ONE shared poll over the minors until all clear or the wait elapses,
// returning those still pinned.
func (dp *XpuDevicePlugin) waitReleaseClear(ctx context.Context, minors []int, held map[int]func(), bound time.Duration) []int {
	pending := minors
	deadline := time.Now().Add(bound)
	for {
		hits, err := dp.matchActiveMonitoringOnMinors(pending)
		if err != nil {
			klog.Warningf("re-check activemonitoring pods on minors %v failed: %v", pending, err)
		} else {
			pinned := make(map[int]bool, len(hits))
			for _, pod := range hits {
				if m, mErr := utils.GetActiveMonitoringTargetMinor(pod); mErr == nil {
					pinned[m] = true
				}
			}
			var still []int
			for _, m := range pending {
				if pinned[m] {
					still = append(still, m)
					continue
				}
				if unlock, ok := held[m]; ok {
					unlock()
					delete(held, m)
				}
			}
			pending = still
			if len(pending) == 0 {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return pending
		}
		select {
		case <-ctx.Done():
			return pending
		case <-time.After(evictPollInterval):
		}
	}
}

// deleteActiveMonitoringPods deletes each pod with the given grace period; NotFound is success.
func (dp *XpuDevicePlugin) deleteActiveMonitoringPods(ctx context.Context, pods []*v1.Pod, graceSeconds int64) {
	for _, pod := range pods {
		grace := graceSeconds
		opts := metav1.DeleteOptions{GracePeriodSeconds: &grace}
		if err := dp.K8sClient.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, opts); err != nil && !apierrors.IsNotFound(err) {
			klog.Warningf("delete activemonitoring pod %s/%s (grace %ds) failed: %v", pod.Namespace, pod.Name, grace, err)
		}
	}
}

// minorMutex returns the per-minor eviction lock, creating it on first use.
func minorMutex(minor int) *sync.Mutex {
	evictRegistryMu.Lock()
	defer evictRegistryMu.Unlock()
	mu, ok := minorLocks[minor]
	if !ok {
		mu = &sync.Mutex{}
		minorLocks[minor] = mu
	}
	return mu
}

// lockMinor acquires the eviction lock for a single minor and returns its unlock func.
func lockMinor(minor int) func() {
	mu := minorMutex(minor)
	mu.Lock()
	return mu.Unlock
}

// lockMinors locks all given minors in ascending order and returns a map from minor to unlock func.
func lockMinors(minors []int) map[int]func() {
	uniq := sortedUnique(minors)
	held := make(map[int]func(), len(uniq))
	for _, m := range uniq {
		held[m] = lockMinor(m)
	}
	return held
}

// sortedUnique returns the minors sorted ascending with duplicates removed.
func sortedUnique(minors []int) []int {
	out := make([]int, 0, len(minors))
	seen := make(map[int]struct{}, len(minors))
	for _, m := range minors {
		if _, ok := seen[m]; ok {
			continue
		}
		seen[m] = struct{}{}
		out = append(out, m)
	}
	sort.Ints(out)
	return out
}
