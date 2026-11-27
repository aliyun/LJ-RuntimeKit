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
	"github.com/container-migration/controller/pkg/migrationworker"
	migrationstatus "github.com/container-migration/controller/pkg/status"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// WaitRestoreHandler waits for each replica's main container to become
// Running and confirms the restore actually took effect.
//
// Container Running only means rootfs + memory are restored and the process is
// up; XPU device state restoration is driven by the next handler in the chain
// (RestoreXPUHandler) after it observes Running, so once past here it is
// handed over, and this handler no longer judges the task terminal state
// directly.
//
// Each replica advances independently: only replicas already in InitReleased
// are processed, then unconditionally Continue. A replica that reaches Running
// first can enter RestoreXPU directly this round without waiting for slow
// replicas.
type WaitRestoreHandler struct {
	sharedContext *controllers.SharedContext
}

var _ = (controllers.Handler[*v1beta1.ContainerStateMigrationJobRequest])(&WaitRestoreHandler{})

func NewWaitRestoreHandler(sharedContext *controllers.SharedContext) *WaitRestoreHandler {
	return &WaitRestoreHandler{sharedContext: sharedContext}
}

func (h *WaitRestoreHandler) GetName() string {
	return "WaitRestoreHandler"
}

func (h *WaitRestoreHandler) Handle(ctx context.Context, jobReq *v1beta1.ContainerStateMigrationJobRequest) (result controllers.Result) {
	status := &jobReq.Status
	if jobReq.Spec.Restore == nil || status.Phase != v1beta1.JobPhaseRestoring ||
		status.RestoreStatus == nil || status.RestoreStatus.Phase != v1beta1.JobPhaseRestoring {
		return controllers.NewSkipResult("not waiting for restore")
	}

	// First collect the replicas to advance this round, then process them concurrently.
	//
	// restoreTookEffect inside checkOneDest sends one blocking gRPC to the
	// worker on that replica's node, waiting up to --rpc-timeout (default
	// 2min) in the worst case. A serial traversal would accumulate these
	// waits, and the traversal here is per replica (not per node), so multiple
	// replicas on one node pay repeatedly - one dead worker on a node can stall
	// the advancement of other healthy replicas for a long time.
	//
	// Each goroutine only writes its own dest (non-overlapping pointers);
	// EventRecorder and the controller-runtime client are concurrency-safe, so
	// no lock is needed.
	pending := make([]*v1beta1.JobDestinationStatus, 0, len(status.RestoreStatus.Destinations))
	for i := range status.RestoreStatus.Destinations {
		dest := &status.RestoreStatus.Destinations[i]
		if dest.Phase != v1beta1.DestPhaseInitReleased {
			continue
		}
		pending = append(pending, dest)
	}
	forEachConcurrently(ctx, pending, func(_ int, dest *v1beta1.JobDestinationStatus) {
		h.checkOneDest(ctx, jobReq, dest)
	})

	// Replicas advanced to XPURestoring this round are immediately triggered
	// for XPU restore by RestoreXPUHandler in the same reconcile; replicas
	// still waiting for their containers to settle do not block them and are
	// rechecked on the next requeue.
	return controllers.NewContinueResult()
}

// checkOneDest advances a single replica: confirms the container is up and
// the restore really took effect, then hands it to XPU restore. Only reads and
// writes the passed dest, never touching other replicas, so it is safe to call
// concurrently.
//
// Verdict order: judge success first, then failure/exhaustion - every round
// first checks whether the container has settled and restoreTookEffect, and
// advances to XPU if so (even if restartCount has reached the threshold); only
// when the current state is failed and restartCount is exhausted does it enter
// cold start.
func (h *WaitRestoreHandler) checkOneDest(ctx context.Context,
	jobReq *v1beta1.ContainerStateMigrationJobRequest, dest *v1beta1.JobDestinationStatus) {
	ctxLog := log.FromContext(ctx)

	pod := &corev1.Pod{}
	err := h.sharedContext.Client.Get(ctx, client.ObjectKey{
		Namespace: dest.Pod.Namespace,
		Name:      dest.Pod.Name,
	}, pod)
	if errors.IsNotFound(err) {
		enterColdStart(dest, "PodDisappeared", fmt.Sprintf("Target pod %s disappeared during restore", dest.Pod.String()))
		h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeWarning, "ColdStartFallback",
			"Pod %s disappeared during restore, falling back to cold start", dest.Pod.String())
		return
	}
	if err != nil {
		ctxLog.Error(err, "Failed to get target pod", "pod", dest.Pod.String())
		return
	}

	containerNames := destContainerNames(dest)

	// 1. Judge success first: container settled and restore took effect -> advance to XPU.
	if containersSettled(pod, containerNames) {
		// A running container does not mean the restore took effect; one more positive proof from the node side is required.
		if attempted, known := h.restoreTookEffect(ctx, jobReq, dest, containerNames); known && !attempted {
			// Case B: container Running but the runtime never took over (it
			// actually started from scratch). Retrying with a config error is
			// useless - skip the retry and go straight to cold start (delete
			// and rebuild a clean cold-start pod).
			msg := fmt.Sprintf("containers of pod %s are running but xpu-container-runtime never took over their creation: "+
				"the processes were started from scratch instead of being restored from the checkpoint "+
				"(check containerd pod_annotations for migration.io/* and the runtime installation on node %s)",
				dest.Pod.String(), dest.Pod.NodeName)
			enterColdStart(dest, "RestoreNotEffective", msg)
			h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeWarning, "ColdStartFallback",
				"Pod %s: %s", dest.Pod.String(), msg)
			ctxLog.Info("Restore did not take effect, falling back to cold start", "pod", dest.Pod.String())
			return
		}

		for j := range dest.ContainerMigrationStatuses {
			cms := &dest.ContainerMigrationStatuses[j]
			cms.ContainerID = containerIDOf(pod, cms.Name)
		}
		dest.Pod.IP = pod.Status.PodIP
		// The container is confirmed running from the checkpoint restore.
		// XPU device state is not restored yet at this moment: hand it to
		// RestoreXPUHandler (controller -> worker -> XDM RestoreXPU).
		dest.Phase = v1beta1.DestPhaseXPURestoring
		dest.Message = "Containers restored and running, restoring XPU state"
		ctxLog.Info("Restored containers are running, handing over to XPU restore",
			"pod", dest.Pod.String())
		h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeNormal, "RestoreContainersRunning",
			"Containers of pod %s restored and running, restoring XPU state", dest.Pod.String())
		return
	}

	// 2. Container failed to come up: runc-restore failure (case A). The
	// retry is left to kubelet's in-place restart; the controller only counts
	// restartCount and judges exhaustion.
	if failed, msg := hasFailedContainer(pod, containerNames); failed {
		h.handleRuncRestoreFailure(ctx, jobReq, dest, pod, msg)
		return
	}

	dest.Message = "Waiting for containers to be running"
}

// handleRuncRestoreFailure judges a runc-restore stage failure (container
// cannot come up):
//   - source restartPolicy=Never: kubelet will not restart the failed
//     container -> no retry, cold start directly;
//   - pod in Failed terminal state (eviction / node pressure, etc.): kubelet
//     likewise restarts no container, restartCount stays 0, waiting would only
//     stall forever -> cold start directly;
//   - restartCount < retryCount: wait for kubelet's in-place restart (the
//     retry is initiated by kubelet, the controller only observes the count);
//   - restartCount >= retryCount (exhausted): cold start.
//
// The count display uses the pod's restartCount (kubelet's authoritative
// source); the History in PhaseRetries is appended only when restartCount
// grows, avoiding repeated counting every reconcile round.
func (h *WaitRestoreHandler) handleRuncRestoreFailure(ctx context.Context,
	jobReq *v1beta1.ContainerStateMigrationJobRequest, dest *v1beta1.JobDestinationStatus,
	pod *corev1.Pod, msg string) {
	ctxLog := log.FromContext(ctx)
	retryCount := retryCountOf(jobReq)

	if pod.Spec.RestartPolicy == corev1.RestartPolicyNever {
		// Never source -> kubelet does not restart, retry cannot happen, cold start directly.
		enterColdStart(dest, "RuncRestoreFailedNeverPolicy", msg)
		h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeWarning, "ColdStartFallback",
			"Pod %s: restartPolicy=Never, runc restore failed, falling back to cold start: %s",
			dest.Pod.String(), msg)
		return
	}

	// Pod Failed is terminal: kubelet will not restart any container in it
	// (in eviction / node pressure scenarios restartCount is usually 0);
	// waiting for restartCount to grow only stalls forever, cold start
	// directly. The exhaustion path only handles kubelet restarts triggered by
	// container exits (phase stays Running).
	if pod.Status.Phase == corev1.PodFailed {
		enterColdStart(dest, "PodFailedTerminal", msg)
		h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeWarning, "ColdStartFallback",
			"Pod %s: pod entered Failed phase, kubelet will not restart it, falling back to cold start: %s",
			dest.Pod.String(), msg)
		return
	}

	restartCount := mainContainerRestartCount(pod, destContainerNames(dest))

	// Display: sync kubelet's restartCount into PhaseRetries (visible via
	// describe); History appends one entry only when restartCount grows, to
	// avoid duplicate appends every reconcile round. added reports whether
	// History grew this round: the RuncRestoreRetry event is emitted only on
	// growth, avoiding repeated event spam every round.
	entry := retryStatusOf(dest, v1beta1.RetryPhaseRuncRestore)
	added := false
	if entry == nil {
		entry = &v1beta1.PhaseRetryStatus{
			Phase:    v1beta1.RetryPhaseRuncRestore,
			Attempts: int(restartCount) + 1,
			MaxRetry: retryCount,
		}
		// Backfill to the same count as restartCount on first creation: the
		// controller may have missed the first few restarts (own restart /
		// slow reconcile); if it only wrote one entry, the next round's len
		// change from backfill would be misjudged as restartCount growth
		// and emit the event again.
		for i := 0; i < int(restartCount)+1; i++ {
			entry.History = append(entry.History, v1beta1.RestoreAttempt{
				Reason:  "RuncRestoreFailed",
				Message: msg,
				Time:    metav1.Now(),
			})
		}
		dest.PhaseRetries = append(dest.PhaseRetries, *entry)
		added = true
	} else {
		entry.Attempts = int(restartCount) + 1
		entry.MaxRetry = retryCount
		before := len(entry.History)
		for len(entry.History) < int(restartCount)+1 {
			entry.History = append(entry.History, v1beta1.RestoreAttempt{
				Reason:  "RuncRestoreFailed",
				Message: msg,
				Time:    metav1.Now(),
			})
		}
		added = len(entry.History) > before
	}

	if int(restartCount) >= retryCount {
		// restartCount reaching retryCount means the failure that round enters cold start (first attempt + N retries).
		enterColdStart(dest,
			fmt.Sprintf("Runc restore retries exhausted (restartCount=%d, retryCount=%d)", restartCount, retryCount),
			msg)
		h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeWarning, "ColdStartFallback",
			"Pod %s: runc restore failed and retries exhausted (restartCount=%d/%d), falling back to cold start: %s",
			dest.Pod.String(), restartCount, retryCount, msg)
		ctxLog.Info("runc restore retries exhausted, falling back to cold start",
			"pod", dest.Pod.String(), "restartCount", restartCount, "retryCount", retryCount)
		return
	}

	// Not exhausted: wait for kubelet's in-place restart, then runc restore again.
	dest.Message = fmt.Sprintf("runc restore failed, waiting kubelet restart (restartCount=%d/%d): %s",
		restartCount, retryCount, msg)
	if added {
		h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeWarning, "RuncRestoreRetry",
			"Pod %s: %s (attempt %d/%d)", dest.Pod.String(), msg, restartCount+1, retryCount)
	}
	ctxLog.Info("runc restore failed, waiting for kubelet restart",
		"pod", dest.Pod.String(), "restartCount", restartCount, "retryCount", retryCount)
}

// mainContainerRestartCount takes the restartCount of the target main
// container (only the main container counts, never read InitContainerStatuses).
// In a multi-container scenario take the max (currently only single-container
// is supported).
func mainContainerRestartCount(pod *corev1.Pod, containerNames []string) int32 {
	wanted := make(map[string]bool, len(containerNames))
	for _, name := range containerNames {
		wanted[name] = true
	}
	var restartCount int32
	for i := range pod.Status.ContainerStatuses {
		cs := &pod.Status.ContainerStatuses[i]
		if !wanted[cs.Name] {
			continue
		}
		if cs.RestartCount > restartCount {
			restartCount = cs.RestartCount
		}
	}
	return restartCount
}

// restoreTookEffect asks the worker on the target node to confirm that
// xpu-container-runtime really took over the creation of these containers.
//
// Why one more query is necessary: a Running container only proves the
// process is up, and cannot tell whether it was restored from the checkpoint
// or started from scratch as a normal container. When the annotation was not
// passed through (containerd's pod_annotations whitelist matches by
// path.Match, so writing migration.io* does not match migration.io/xxx), the
// runtime is not installed on the node, or the runtime handler is
// misconfigured - the migration did not take effect at all while the pod looks
// perfectly normal.
//
// When known=false the caller must keep the original verdict: this check is
// to plug silent failures, not to wrongly judge an actually-successful
// migration as failed because of worker flapping or version mismatch (old
// workers do not report this field).
func (h *WaitRestoreHandler) restoreTookEffect(ctx context.Context,
	jobReq *v1beta1.ContainerStateMigrationJobRequest, dest *v1beta1.JobDestinationStatus,
	containerNames []string) (attempted bool, known bool) {
	ctxLog := log.FromContext(ctx)

	if dest.NodeIP == "" {
		return false, false
	}
	workerClient, err := h.sharedContext.WorkerClientFor(ctx, string(jobReq.UID), dest.NodeIP)
	if err != nil {
		return false, false
	}

	response, err := workerClient.QueryRestoreFromStorageStatus(ctx,
		&migrationworker.QueryRestoreFromStorageStatusRequest{
			RequestID: nodeRestoreRequestID(jobReq.Status.RequestID, dest.NodeIP),
		})
	if err != nil {
		if isTransientError(err) {
			_ = h.sharedContext.RemoveWorkerClientForWithCallback(string(jobReq.UID), dest.NodeIP, func() error { return nil })
		}
		ctxLog.Info("Cannot verify whether restore took effect, keeping the current verdict",
			"pod", dest.Pod.String(), "error", err)
		return false, false
	}
	// A task already cleaned up (StatusNotFound) also counts as "cannot tell"; no verdict.
	if response.Status != migrationstatus.StatusOK {
		return false, false
	}

	for _, name := range containerNames {
		attrs, ok := response.Data[name]
		if !ok || attrs == nil || attrs.Extras == nil {
			return false, false
		}
		value, ok := attrs.Extras[extraKeyRestoreAttempted]
		if !ok {
			return false, false
		}
		if value != "true" {
			return false, true
		}
	}
	return true, true
}

// containersSettled judges whether the target containers have all "settled":
// either running, or already finished normally.
//
// Running alone is not enough: the target pod's restartPolicy is Never, and a
// process restored from the checkpoint may well finish normally and exit soon
// (ExitCode 0). That is also a restore success - if it still required
// "must be Running", such tasks would wait until timeout.
func containersSettled(pod *corev1.Pod, containerNames []string) bool {
	statuses := make(map[string]*corev1.ContainerStatus, len(pod.Status.ContainerStatuses))
	for i := range pod.Status.ContainerStatuses {
		statuses[pod.Status.ContainerStatuses[i].Name] = &pod.Status.ContainerStatuses[i]
	}

	for _, name := range containerNames {
		cs, ok := statuses[name]
		if !ok {
			return false
		}
		switch {
		case cs.State.Running != nil:
		case cs.State.Terminated != nil && cs.State.Terminated.ExitCode == 0:
		default:
			return false
		}
	}
	return true
}

// containerRunning judges whether the given main container is currently Running.
//
// XPU device state can only be restored onto a running process (cr-tool needs
// a live pid); a container that already exited normally (process finished
// after a successful restore, restartPolicy=Never) needs no device state
// restoration and cannot get one.
func containerRunning(pod *corev1.Pod, containerName string) bool {
	for i := range pod.Status.ContainerStatuses {
		if pod.Status.ContainerStatuses[i].Name == containerName {
			return pod.Status.ContainerStatuses[i].State.Running != nil
		}
	}
	return false
}

func destContainerNames(dest *v1beta1.JobDestinationStatus) []string {
	names := make([]string, 0, len(dest.ContainerMigrationStatuses))
	for i := range dest.ContainerMigrationStatuses {
		names = append(names, dest.ContainerMigrationStatuses[i].Name)
	}
	return names
}

// hasFailedContainer checks whether any target container has definitively failed to come up.
func hasFailedContainer(pod *corev1.Pod, containerNames []string) (bool, string) {
	// Must be judged first when the pod enters a terminal state as a whole.
	// Under eviction / node loss / init container failure, containerStatuses
	// may be empty at all; looking only at container states would wait until
	// timeout.
	if pod.Status.Phase == corev1.PodFailed {
		return true, fmt.Sprintf("pod entered Failed phase: %s %s",
			pod.Status.Reason, pod.Status.Message)
	}

	wanted := make(map[string]bool, len(containerNames))
	for _, name := range containerNames {
		wanted[name] = true
	}
	for i := range pod.Status.ContainerStatuses {
		cs := &pod.Status.ContainerStatuses[i]
		if !wanted[cs.Name] {
			continue
		}
		if cs.State.Terminated != nil && cs.State.Terminated.ExitCode != 0 {
			return true, fmt.Sprintf("container %s terminated with exit code %d: %s",
				cs.Name, cs.State.Terminated.ExitCode, cs.State.Terminated.Reason)
		}
		if cs.State.Waiting != nil &&
			(cs.State.Waiting.Reason == "CrashLoopBackOff" || cs.State.Waiting.Reason == "RunContainerError") {
			return true, fmt.Sprintf("container %s is in %s: %s",
				cs.Name, cs.State.Waiting.Reason, cs.State.Waiting.Message)
		}
	}
	return false, ""
}

func containerIDOf(pod *corev1.Pod, containerName string) string {
	for i := range pod.Status.ContainerStatuses {
		if pod.Status.ContainerStatuses[i].Name == containerName {
			return pod.Status.ContainerStatuses[i].ContainerID
		}
	}
	return ""
}
