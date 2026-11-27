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

package handler

import (
	"context"
	"fmt"

	"github.com/container-migration/controller/api/v1beta1"
	"github.com/container-migration/controller/pkg/controllers"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// EnsureWorkerHandler makes sure the related nodes run the Migration Worker
// pod.
//
// Checkpoint stage (phase=Pending, checkpoint!=nil):
//   - ensure the source pod's node runs a worker pod
//   - must run before CheckpointHandler
//
// Restore stage: guaranteed per target node by PrepareDataHandler; this
// handler skips directly.
type EnsureWorkerHandler struct {
	sharedContext *controllers.SharedContext
}

var _ = (controllers.Handler[*v1beta1.ContainerStateMigrationJobRequest])(&EnsureWorkerHandler{})

func NewEnsureWorkerHandler(sharedContext *controllers.SharedContext) *EnsureWorkerHandler {
	return &EnsureWorkerHandler{sharedContext: sharedContext}
}

func (h *EnsureWorkerHandler) GetName() string {
	return "EnsureWorkerHandler"
}

func (h *EnsureWorkerHandler) Handle(ctx context.Context, jobReq *v1beta1.ContainerStateMigrationJobRequest) (result controllers.Result) {
	// Boilerplate logs are emitted uniformly by controller.go; not repeated here.

	status := &jobReq.Status
	spec := &jobReq.Spec

	// ========== Checkpoint stage: ensure the source node has a worker ==========
	if status.Phase == v1beta1.JobPhasePending && spec.Checkpoint != nil {
		return h.ensureSourceNodeWorker(ctx, jobReq)
	}

	// ========== Restore stage: the worker is guaranteed per target node by PrepareDataHandler; skip here ==========
	if status.Phase == v1beta1.JobPhaseRestoring && spec.Restore != nil {
		return controllers.NewSkipResult("restore phase: worker ensured per-dest by PrepareDataHandler")
	}

	return controllers.NewSkipResult(fmt.Sprintf("not applicable for phase %s", status.Phase))
}

// ensureSourceNodeWorker makes sure the source pod's node runs a worker pod.
func (h *EnsureWorkerHandler) ensureSourceNodeWorker(ctx context.Context, jobReq *v1beta1.ContainerStateMigrationJobRequest) (result controllers.Result) {
	ctxLog := log.FromContext(ctx)
	checkpointSpec := jobReq.Spec.Checkpoint

	pod := &corev1.Pod{}
	err := h.sharedContext.Client.Get(ctx, client.ObjectKey{
		Namespace: checkpointSpec.SourcePodNamespace,
		Name:      checkpointSpec.SourcePodName,
	}, pod)
	if err != nil {
		ctxLog.Error(err, "Failed to get source pod for worker check", "podName", checkpointSpec.SourcePodName)
		return controllers.NewErrorResult(err)
	}

	if pod.Spec.NodeName == "" {
		ctxLog.V(2).Info("Source pod not yet scheduled, waiting")
		return controllers.NewAbortWithRetryAfterResult(h.sharedContext.Config.RetryWaitTime)
	}

	nodeName := pod.Spec.NodeName
	nodeIP := pod.Status.HostIP

	// The worker entry before dispatching checkpoint: PVC mode uses the
	// task-level worker, other modes use the shared worker (replacement allowed
	// on config drift when the node is idle).
	var res controllers.WorkerEnsureResult
	if isPVCStorage(&jobReq.Spec) {
		res = h.sharedContext.EnsureTaskWorker(ctx, nodeName, nodeIP, string(jobReq.UID), jobReq.Spec.Storage.PVC)
	} else {
		res = h.sharedContext.EnsureWorkerUpToDate(ctx, nodeName, nodeIP, string(jobReq.UID), true)
	}
	switch res.State {
	case controllers.WorkerFailed:
		ctxLog.Error(res.Err, "Failed to ensure migration worker on source node", "node", nodeName)
		return controllers.NewErrorResult(res.Err)
	case controllers.WorkerWaiting:
		h.applyWorkerBlockedCondition(jobReq, nodeIP, res)
		ctxLog.Info("Worker not ready on source node, will retry",
			"node", nodeName, "nodeIP", nodeIP, "reason", res.Reason, "blocked", res.Blocked)
		return controllers.NewAbortWithRetryAfterResult(h.sharedContext.Config.WorkerDrainPollInterval)
	}

	// Worker ready: clear the possibly-residual WorkerBlocked marker and let through.
	clearWorkerBlockedCondition(&jobReq.Status)
	ctxLog.Info("Ensured migration worker on source node", "node", nodeName, "nodeIP", nodeIP)
	return controllers.NewContinueResult()
}

// applyWorkerBlockedCondition writes the job-level WorkerBlocked condition and
// status.Message when the task is blocked by worker drift + busy node; ordinary
// waits (creating / waiting for ready) are not written. State / wording change
// is checked before writing, guaranteeing the same blocked state does not
// refresh LastTransitionTime every round.
//
// nodeIP is passed by the caller: the source node IP is only written into
// CheckpointStatus.SourceNodeIP in CheckpointHandler; reading it in this
// handler's stage is always empty.
func (h *EnsureWorkerHandler) applyWorkerBlockedCondition(jobReq *v1beta1.ContainerStateMigrationJobRequest,
	nodeIP string, res controllers.WorkerEnsureResult) {
	if !res.Blocked {
		return
	}
	msg := controllers.FormatWorkerBlockedMessage(nodeIP, res.Users)
	if setWorkerBlockedConditionOnce(&jobReq.Status, msg) {
		h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeWarning, "WorkerBlocked",
			"Waiting for stale worker to drain before replacement: %s", msg)
	}
	jobReq.Status.Message = "Waiting for stale worker to drain: " + msg
}
