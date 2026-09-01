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

package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	xpugrpc "github.com/lj-runtimekit/device-plugin/internal/grpc"

	"google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

const (
	// CheckpointGCInterval is how often the checkpoint GC reconciles the
	// checkpoint records against the pods still present on the node.
	CheckpointGCInterval = 5 * time.Minute

	// CheckpointGCGracePeriod bounds consecutive absence rounds before a record
	// is treated as orphaned. The timer resets when the record reappears.
	CheckpointGCGracePeriod = 30 * time.Minute

	// kubelet 10255 readonly port pre-check parameters
	kubeletCheckRetries  = 3
	kubeletCheckInterval = time.Second
	kubeletCheckTimeout  = 5 * time.Second

	// backoff between the pre-check rounds of Start while the kubelet
	// readonly port is not yet reachable
	kubeletCheckBackoffInit = 10 * time.Second
	kubeletCheckBackoffMax  = 5 * time.Minute

	// xdmReleaseTimeout bounds a single xdm Release gRPC call of the GC
	xdmReleaseTimeout = 30 * time.Second
)

var (
	// KubeletPodsEndpoint is the kubelet readonly /pods endpoint, resolved at
	// init from env vars; declared as var so tests can override it.
	KubeletPodsEndpoint = resolveKubeletPodsEndpoint()
)

// resolveKubeletPodsEndpoint derives the kubelet readonly /pods endpoint.
// ENV_KUBELET_PODS_ENDPOINT overrides it; otherwise ENV_HOST_IP is used,
// falling back to 127.0.0.1.
func resolveKubeletPodsEndpoint() string {
	if endpoint := os.Getenv(EnvKubeletPodsEndpoint); endpoint != "" {
		return endpoint
	}
	if hostIP := os.Getenv(EnvHostIP); hostIP != "" {
		return fmt.Sprintf("http://%s:10255/pods", hostIP)
	}
	return "http://127.0.0.1:10255/pods"
}

// XdmReleaser is the xdm Release gRPC interface used by the GC. Release
// accepts either a podUID or an allocateID.
type XdmReleaser interface {
	Release(ctx context.Context, in *xpugrpc.ReleaseRequest, opts ...grpc.CallOption) (*xpugrpc.ReleaseResponse, error)
}

// CheckpointGC reconciles dp checkpoint records against running pods. For
// records of deleted pods it calls xdm Release and removes the record.
type CheckpointGC struct {
	releaser          XdmReleaser
	fetchPods         func() (map[string]struct{}, error)
	fetchKubeletState func() (*KubeletCheckpointState, error)
	interval          time.Duration
	// firstMissing records when each record was first observed absent. The
	// timer advances only across consecutive absent rounds and resets on
	// reappearance. RunOnce is serial, so no lock is needed.
	firstMissing map[string]time.Time
}

// NewCheckpointGC builds a CheckpointGC backed by the given xdm client.
func NewCheckpointGC(releaser XdmReleaser) *CheckpointGC {
	return &CheckpointGC{
		releaser:          releaser,
		fetchPods:         fetchRunningPodUIDsFromKubelet,
		fetchKubeletState: LoadKubeletCheckpointState,
		interval:          CheckpointGCInterval,
		firstMissing:      make(map[string]time.Time),
	}
}

// Start runs the GC loop. It first waits for the kubelet /pods endpoint to
// become reachable, retrying with backoff, so it never runs on a stale list.
func (gc *CheckpointGC) Start() {
	backoff := kubeletCheckBackoffInit
	for {
		if err := checkKubeletPodsEndpoint(); err == nil {
			break
		} else {
			klog.Errorf("kubelet pods endpoint check failed, retry checkpoint GC pre-check in %v: %v", backoff, err)
			time.Sleep(backoff)
			backoff *= 2
			if backoff > kubeletCheckBackoffMax {
				backoff = kubeletCheckBackoffMax
			}
		}
	}
	klog.Infof("checkpoint GC started, interval %v", gc.interval)

	ticker := time.NewTicker(gc.interval)
	defer ticker.Stop()
	for range ticker.C {
		if err := gc.RunOnce(); err != nil {
			klog.Errorf("checkpoint GC round failed: %v", err)
		}
	}
}

// RunOnce performs one GC round. Exported for tests.
func (gc *CheckpointGC) RunOnce() error {
	runningPodUIDs, err := gc.fetchPods()
	if err != nil {
		// A broken pod list breaks absence continuity; discard timers so we do
		// not release records while kubelet is still rebuilding its state.
		gc.firstMissing = make(map[string]time.Time)
		return fmt.Errorf("fetch pods from kubelet failed: %w", err)
	}

	records, err := ListAllRecords()
	if err != nil {
		return fmt.Errorf("list checkpoint records failed: %w", err)
	}

	// kubeletState is parsed from the on-disk checkpoint and stays authoritative
	// while the pod list may be transiently incomplete.
	kubeletState, stateErr := gc.fetchKubeletState()
	if stateErr != nil {
		klog.Warningf("checkpoint GC: read kubelet checkpoint failed, fall back to the grace period: %v", stateErr)
	}

	now := time.Now()
	var toDelete []string
	for _, record := range records {
		// alive resets absence timers; certainGone allows immediate release;
		// everything else requires the full grace period of consecutive absence.
		alive := false
		certainGone := false
		if record.PodUID != "" {
			// records with pod identity are reconciled against the pod list
			if _, running := runningPodUIDs[record.PodUID]; running {
				alive = true
			} else if stateErr == nil {
				// cross-check by pod uid; reassigned devices do not resurrect
				// the old uid entry.
				if _, holds := kubeletState.PodUIDs[record.PodUID]; !holds {
					// absent from both independent sources: the pod is gone
					certainGone = true
				}
			}
		} else {
			// records without pod identity have no pod list entry; keep them
			// alive while kubelet still holds their devices.
			if stateErr != nil || kubeletStillAssignsDevices(kubeletState.AssignedDevices, record.ResourceName, record.DeviceIDs) {
				alive = true
			}
		}

		if alive {
			delete(gc.firstMissing, record.AllocateID)
			continue
		}

		if !certainGone {
			// uncertain records need the full grace period of consecutive
			// absence; reappearance resets the timer.
			missingSince, seen := gc.firstMissing[record.AllocateID]
			if !seen {
				gc.firstMissing[record.AllocateID] = now
				continue
			}
			if now.Sub(missingSince) < CheckpointGCGracePeriod {
				continue
			}
		}

		// Release cleans the xdm checkpoint and device directories. It is
		// idempotent; on failure keep the record so the next round retries.
		if err := gc.xdmRelease(record); err != nil {
			klog.Errorf("checkpoint GC: xdm release for allocateID %s (pod %s) failed, will retry next round: %v",
				record.AllocateID, record.PodUID, err)
			continue
		}
		delete(gc.firstMissing, record.AllocateID)
		klog.Infof("checkpoint GC: releasing %s record %s of deleted pod %s", record.Status, record.AllocateID, record.PodUID)
		toDelete = append(toDelete, record.AllocateID)
	}

	// hygiene: drop absence timers whose record no longer exists, so the
	// map cannot grow beyond the live record set
	if len(gc.firstMissing) > 0 {
		existing := make(map[string]struct{}, len(records))
		for _, record := range records {
			existing[record.AllocateID] = struct{}{}
		}
		for id := range gc.firstMissing {
			if _, ok := existing[id]; !ok {
				delete(gc.firstMissing, id)
			}
		}
	}

	if len(toDelete) == 0 {
		return nil
	}
	if err := DeleteAllocateRecords(toDelete); err != nil {
		return fmt.Errorf("delete checkpoint records failed: %w", err)
	}
	return nil
}

// kubeletStillAssignsDevices reports whether kubelet still holds all the
// given device ids of the resource in allocation. Records without device
// ids carry nothing kubelet could hold, so they are never considered alive.
func kubeletStillAssignsDevices(assignedDevices map[string]map[string]struct{}, resourceName string, deviceIDs []string) bool {
	if len(deviceIDs) == 0 {
		return false
	}
	deviceSet := assignedDevices[resourceName]
	if deviceSet == nil {
		return false
	}
	for _, deviceID := range deviceIDs {
		if _, assigned := deviceSet[deviceID]; !assigned {
			return false
		}
	}
	return true
}

// xdmRelease releases the record in xdm. Prefer podUID so one call covers
// all allocations of the pod; fall back to allocateID for pod-less records.
func (gc *CheckpointGC) xdmRelease(record *AllocateRecord) error {
	if gc.releaser == nil {
		return nil
	}
	id := record.PodUID
	if id == "" {
		id = record.AllocateID
	}
	ctx, cancel := context.WithTimeout(context.Background(), xdmReleaseTimeout)
	defer cancel()

	resp, err := gc.releaser.Release(ctx, &xpugrpc.ReleaseRequest{Id: id})
	if err != nil {
		return fmt.Errorf("xdm Release rpc failed: %w", err)
	}
	if resp.GetStatus() != 0 {
		return fmt.Errorf("xdm Release rpc returned status %d: %s", resp.GetStatus(), resp.GetMessage())
	}
	return nil
}

// checkKubeletPodsEndpoint verifies the kubelet 10255 readonly /pods endpoint,
// retrying to tolerate kubelet startup.
func checkKubeletPodsEndpoint() error {
	httpClient := &http.Client{Timeout: kubeletCheckTimeout}

	var lastErr error
	for i := 0; i < kubeletCheckRetries; i++ {
		lastErr = doCheckKubeletPodsEndpoint(httpClient)
		if lastErr == nil {
			return nil
		}
		klog.Warningf("kubelet pods endpoint check attempt %d/%d failed: %v", i+1, kubeletCheckRetries, lastErr)
		time.Sleep(kubeletCheckInterval)
	}
	return lastErr
}

func doCheckKubeletPodsEndpoint(httpClient *http.Client) error {
	resp, err := httpClient.Get(KubeletPodsEndpoint)
	if err != nil {
		return fmt.Errorf("failed to access kubelet pods endpoint %s: %w", KubeletPodsEndpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("kubelet pods endpoint %s returned status %d: %s", KubeletPodsEndpoint, resp.StatusCode, string(body))
	}

	var podList v1.PodList
	if err := json.NewDecoder(resp.Body).Decode(&podList); err != nil {
		return fmt.Errorf("failed to decode kubelet pods endpoint %s response: %w", KubeletPodsEndpoint, err)
	}
	klog.V(4).Infof("kubelet pods endpoint check passed, pods in response: %d", len(podList.Items))
	return nil
}

// fetchRunningPodUIDsFromKubelet returns pod UIDs from kubelet. An empty list
// is rejected as unreliable because the plugin pod itself is always present.
func fetchRunningPodUIDsFromKubelet() (map[string]struct{}, error) {
	httpClient := &http.Client{Timeout: kubeletCheckTimeout}
	resp, err := httpClient.Get(KubeletPodsEndpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to kubelet: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("kubelet /pods returned status %d: %s", resp.StatusCode, string(body))
	}

	var podList v1.PodList
	if err := json.NewDecoder(resp.Body).Decode(&podList); err != nil {
		return nil, fmt.Errorf("failed to decode kubelet /pods response: %w", err)
	}
	if len(podList.Items) == 0 {
		return nil, fmt.Errorf("kubelet /pods returned an empty pod list, treat as unreliable")
	}

	runningPods := make(map[string]struct{}, len(podList.Items))
	for _, pod := range podList.Items {
		runningPods[string(pod.UID)] = struct{}{}
	}
	return runningPods, nil
}
