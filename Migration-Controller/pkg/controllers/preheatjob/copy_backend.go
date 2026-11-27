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

package preheatjob

import (
	"context"
	"fmt"
	"strings"

	"github.com/container-migration/controller/api/v1beta1"
	"github.com/container-migration/controller/pkg/controllers"
	"github.com/container-migration/controller/pkg/preheat"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// copyBackend adapts copy preheat into PreheatBackend: one preheat is one
// "source PVC -> target PVC" wholesale copy carried by a one-shot copy pod
// (see SharedContext's buildPreheatCopyPod for the pod construction); the
// controller only creates the pod and polls its phase.
//
// Differences from dragonfly/EFC:
//   - No cache concept: after the copy the data lands permanently on the target
//     PVC (until the user deletes it), so there is no ProbeCache semantics -
//     always returns ErrNoProbeTargets (a defensive implementation; the normal
//     flow never calls it: copy mode skips Probing and ProbeController skips
//     copy jobs at the entry);
//   - No parts, no BIO: taskID is "podNamespace/podName"; the pod name is
//     decided by the PreheatJob UID, so creation is naturally idempotent;
//   - No credentials: RequiresStorageCredentials=false, the data is copied
//     file-to-file inside the pod.
type copyBackend struct {
	sharedContext *controllers.SharedContext
}

var _ preheat.PreheatBackend = copyBackend{}

func (copyBackend) Name() string { return preheat.BackendCopy }

// RequiresStorageCredentials: the copy happens file-to-file inside the pod, no origin credentials needed.
func (copyBackend) RequiresStorageCredentials() bool { return false }

// TaskLabel: one task for the whole directory copy.
func (copyBackend) TaskLabel(int) string { return "copy" }

// StartPreheat creates the copy pod, taskID = "podNamespace/podName"
// (GetPreheatTask splits out the namespace from it to locate the pod). The
// source PVC coordinates come with req.Pvc (spec.pvc filled directly or
// resolved from checkpointUID, PvcPath normalized); the target coordinates
// come from spec.options.
func (b copyBackend) StartPreheat(ctx context.Context, req preheat.StartPreheatRequest) ([]string, error) {
	if req.Pvc == nil {
		return nil, fmt.Errorf("copy preheat requires a PVC source: fill spec.pvc or a checkpointUID resolving to pvc")
	}
	dstName := preheat.TargetPvcName(req.Job)
	dstNamespace := preheat.TargetPvcNamespace(req.Job)
	if dstName == "" || dstNamespace == "" {
		return nil, fmt.Errorf("copy preheat requires both target-pvc-name and target-pvc-namespace in spec.options")
	}
	podName, err := b.sharedContext.CreatePreheatCopyPod(ctx, string(req.Job.UID),
		req.Pvc.PvcName, dstName, dstNamespace, req.Pvc.PvcPath)
	if err != nil {
		return nil, err
	}
	return []string{dstNamespace + "/" + podName}, nil
}

// GetPreheatTask polls the copy pod's phase: Succeeded -> Success, Failed ->
// Failure (the message suggests checking the pod log), anything else
// (Pending/Running/Unknown) -> Running, keep waiting.
func (b copyBackend) GetPreheatTask(ctx context.Context, taskID string) (preheat.Task, error) {
	namespace, name, err := splitCopyTaskID(taskID)
	if err != nil {
		return preheat.Task{}, err
	}
	pod := &corev1.Pod{}
	if err := b.sharedContext.Client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, pod); err != nil {
		return preheat.Task{}, fmt.Errorf("get copy pod %s/%s: %w", namespace, name, err)
	}
	switch pod.Status.Phase {
	case corev1.PodSucceeded:
		return preheat.Task{ID: taskID, State: preheat.TaskStateSuccess}, nil
	case corev1.PodFailed:
		return preheat.Task{ID: taskID, State: preheat.TaskStateFailure,
			Message: "copy pod failed (source path missing, copy error or verification mismatch; check pod logs)"}, nil
	default:
		return preheat.Task{ID: taskID, State: preheat.TaskStateRunning}, nil
	}
}

// ProbeCache: copy has no cache to probe and always returns
// ErrNoProbeTargets; the caller skips this round without updating status. The
// normal flow never reaches here - copy mode sets Ready directly after all
// tasks succeed in handleRunning, and ProbeController skips copy jobs at the
// entry.
func (copyBackend) ProbeCache(context.Context, *v1beta1.PreheatJob) ([]v1beta1.PreheatPeerInfo, []preheat.CacheState, error) {
	return nil, nil, preheat.ErrNoProbeTargets
}

// splitCopyTaskID splits a copy taskID of the form "namespace/name".
func splitCopyTaskID(taskID string) (string, string, error) {
	parts := strings.SplitN(taskID, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("invalid copy task id %q, want namespace/podName", taskID)
	}
	return parts[0], parts[1], nil
}
