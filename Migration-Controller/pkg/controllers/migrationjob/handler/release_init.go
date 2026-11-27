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
	"time"

	"github.com/container-migration/controller/api/v1beta1"
	"github.com/container-migration/controller/pkg/controllers"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// initReleaseMaxRetry is the controller's own independent retry cap after
// exec/touch release failures (fixed at 3, independent of retryCount). An exec
// failure does not trigger a kubelet restart (exec is CRI ExecSync, the
// container itself did not exit, restartCount does not grow), so this line can
// only rely on the controller's own counting as the fallback; otherwise a
// persistent exec failure would retry forever with no terminal exit.
const initReleaseMaxRetry = 3

// ReleaseInitHandler releases the blocked init containers after the data is
// ready.
//
// Each replica advances independently: only replicas already in
// SnapshotPrepared are processed, then unconditionally Continue. A replica
// whose init container is not Running yet does not block others from entering
// WaitRestore; requeueing is owned by RestoreXPUHandler at the chain tail.
//
// Release (exec touch of the signal file) is decoupled from the init exit
// code: a successful exec touch counts as a successful release; a non-zero
// exit of the init itself no longer cold-starts across the board - under
// restartPolicy=Always/OnFailure wait for kubelet's in-place restart of the
// init and touch again, counting by the init's restartCount, cold start only
// when retryCount is exhausted; under restartPolicy=Never kubelet will not
// restart the init, cold start immediately. exec/touch failures fall back to
// the controller's own independent counting (fixed initReleaseMaxRetry times +
// 3s backoff), cold start when exhausted. Pod gone stays direct cold start.
//
// The mechanism is pods/exec into the init container to touch a signal file -
// the only viable path under community K8s: a pod's container env / command
// cannot be changed after creation, and kubelet creates the main container
// only after the init container exits, at which point xpu-container-runtime
// intercepts the runc create to complete the restore.
type ReleaseInitHandler struct {
	sharedContext *controllers.SharedContext
}

var _ = (controllers.Handler[*v1beta1.ContainerStateMigrationJobRequest])(&ReleaseInitHandler{})

func NewReleaseInitHandler(sharedContext *controllers.SharedContext) *ReleaseInitHandler {
	return &ReleaseInitHandler{sharedContext: sharedContext}
}

func (h *ReleaseInitHandler) GetName() string {
	return "ReleaseInitHandler"
}

func (h *ReleaseInitHandler) Handle(ctx context.Context, jobReq *v1beta1.ContainerStateMigrationJobRequest) (result controllers.Result) {
	ctxLog := log.FromContext(ctx)

	status := &jobReq.Status
	if jobReq.Spec.Restore == nil || status.Phase != v1beta1.JobPhaseRestoring ||
		status.RestoreStatus == nil || status.RestoreStatus.Phase != v1beta1.JobPhaseRestoring {
		return controllers.NewSkipResult("restore data not ready yet")
	}

	for i := range status.RestoreStatus.Destinations {
		dest := &status.RestoreStatus.Destinations[i]
		if dest.Phase != v1beta1.DestPhaseSnapshotPrepared {
			continue
		}

		// 3s backoff after an exec/touch failure (same gauge as the download
		// redispatch backoff): no exec attempts within the backoff window, retry
		// on the next reconcile round after the window passes - exec failures are
		// mostly transient failures during the init restart window, give it some
		// time for kubelet to bring the init up.
		if destInInitReleaseBackoff(dest) {
			continue
		}

		pod := &corev1.Pod{}
		err := h.sharedContext.Client.Get(ctx, client.ObjectKey{
			Namespace: dest.Pod.Namespace,
			Name:      dest.Pod.Name,
		}, pod)
		if errors.IsNotFound(err) {
			// Pod is gone; the cold-start handler will create a new pod directly.
			enterColdStart(dest, "PodDisappeared", fmt.Sprintf(
				"Target pod %s disappeared before release", dest.Pod.String()))
			h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeWarning, "ColdStartFallback",
				"Pod %s disappeared before init release, falling back to cold start", dest.Pod.String())
			continue
		}
		if err != nil {
			ctxLog.Error(err, "Failed to get target pod for init release", "pod", dest.Pod.String())
			continue
		}

		// The init container must already be Running for exec to work.
		// Having exited normally means the signal file was already written - only
		// we write it - this is the re-entry path after "exec succeeded but status
		// update failed"; it must not wait for Running again (it never will).
		running, exitCode, restartCount := initContainerState(pod, InitContainerName)
		if exitCode != nil {
			if *exitCode != 0 {
				// The init itself died (no sh in the image, OOM, etc.): no
				// longer cold-start across the board - restartPolicy=Never cold
				// starts directly (kubelet does not restart the init);
				// Always/OnFailure waits for kubelet's in-place restart of the
				// init and touches again, counting by restartCount, cold start
				// only when retryCount is exhausted.
				h.handleInitContainerFailure(ctx, jobReq, dest, pod, *exitCode, restartCount)
				continue
			}
			dest.Phase = v1beta1.DestPhaseInitReleased
			dest.Message = "Init container already released"
			ctxLog.Info("Init container had already exited, treating as released",
				"pod", dest.Pod.String())
			continue
		}
		if !running {
			// Pod Failed is terminal: kubelet will not bring up any container
			// in it (under eviction / node pressure the init status entries may be
			// empty and exitCode unreadable); waiting for the init to become
			// Running only stalls forever, cold start directly.
			if pod.Status.Phase == corev1.PodFailed {
				enterColdStart(dest, "PodFailedTerminal", fmt.Sprintf(
					"Pod %s entered Failed phase, kubelet will not restart its init container",
					dest.Pod.String()))
				h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeWarning, "ColdStartFallback",
					"Pod %s: pod entered Failed phase, kubelet will not restart its init container, falling back to cold start",
					dest.Pod.String())
				continue
			}
			// Waiting may be image pulling / first start (restartCount=0) or
			// kubelet backoff of a crash loop (restartCount>0). Under
			// CrashLoopBackOff the init spends most of its time in backoff
			// Waiting and the Terminated window is extremely short, so the
			// controller rarely observes it - the exhaustion verdict must also be
			// made here (only when restartCount>0, so image-pull waits are
			// unaffected), otherwise a crash-looping init never gets a cold-start
			// exit.
			if restartCount > 0 && pod.Spec.RestartPolicy != corev1.RestartPolicyNever &&
				int(restartCount) >= retryCountOf(jobReq) {
				// Same display gauge as handleInitContainerFailure:
				// restartCount synced into PhaseRetries (visible via describe).
				syncInitRestartStatus(dest, "InitContainerCrashLoop",
					"init container is crash-looping (backoff)", restartCount, retryCountOf(jobReq))
				enterColdStart(dest, "InitContainerFailed", fmt.Sprintf(
					"Migration init container of pod %s crashed %d times (retryCount=%d), falling back to cold start",
					dest.Pod.String(), restartCount, retryCountOf(jobReq)))
				h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeWarning, "ColdStartFallback",
					"Pod %s: init container crash-looping and retries exhausted (restartCount=%d/%d), falling back to cold start",
					dest.Pod.String(), restartCount, retryCountOf(jobReq))
				continue
			}
			dest.Message = "Waiting for init container to be running"
			continue
		}

		signalPath := fmt.Sprintf("%s/%s", SignalMountPath, SignalFileName)
		if err := h.sharedContext.ExecInPod(ctx, pod.Namespace, pod.Name, InitContainerName,
			[]string{"touch", signalPath}); err != nil {
			// exec/touch failures do not go down the kubelet restart line
			// (an exec failure does not exit the container, restartCount does not
			// grow); they go down the controller's own independent counting: fixed
			// initReleaseMaxRetry times + 3s backoff, cold start only when
			// exhausted.
			message := fmt.Sprintf("Failed to release init container of pod %s: %v", dest.Pod.String(), err)
			attempts := recordAttempt(dest, v1beta1.RetryPhaseInitRelease, "InitReleaseFailed",
				message, initReleaseMaxRetry)
			if attempts > initReleaseMaxRetry {
				enterColdStart(dest, "InitReleaseFailed", fmt.Sprintf(
					"Init release exec failed %d times (max %d), falling back to cold start: %v",
					attempts, initReleaseMaxRetry, err))
				h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeWarning, "ColdStartFallback",
					"Pod %s: init release exec failed %d times, falling back to cold start: %v",
					dest.Pod.String(), attempts, err)
				continue
			}
			dest.Message = fmt.Sprintf("Failed to release init container (attempt %d/%d), will retry: %v",
				attempts, initReleaseMaxRetry, err)
			h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeWarning, "InitReleaseRetry",
				"Pod %s: %s (attempt %d/%d)", dest.Pod.String(), err, attempts, initReleaseMaxRetry)
			continue
		}

		dest.Phase = v1beta1.DestPhaseInitReleased
		dest.Message = "Init container released, waiting for containers to restore"
		ctxLog.Info("Released init container", "pod", dest.Pod.String())
		h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeNormal, "InitReleased",
			"Released init container of pod %s", dest.Pod.String())
	}

	// Released replicas are handed downstream to WaitRestore; replicas not
	// released this round (init not Running / exec flapping) do not block
	// others and retry on the next requeue round.
	return controllers.NewContinueResult()
}

// initContainerState returns whether the given init container is Running, its
// exit code when it has exited, and kubelet's authoritative restartCount.
// exitCode nil means not exited yet; running false with exitCode nil means
// still Waiting (image pull, etc.).
func initContainerState(pod *corev1.Pod, name string) (running bool, exitCode *int32, restartCount int32) {
	for i := range pod.Status.InitContainerStatuses {
		cs := &pod.Status.InitContainerStatuses[i]
		if cs.Name != name {
			continue
		}
		if cs.State.Terminated != nil {
			code := cs.State.Terminated.ExitCode
			return false, &code, cs.RestartCount
		}
		return cs.State.Running != nil, nil, cs.RestartCount
	}
	return false, nil, 0
}

// handleInitContainerFailure handles the fallback for a non-zero exit of
// migration-init-signal itself:
//   - restartPolicy=Never: kubelet will not restart the init and the pod fails
//     directly, waiting is meaningless, cold start immediately;
//   - pod in Failed terminal state (eviction / node pressure, etc.): kubelet
//     likewise will not restart the init container, restartCount stays 0,
//     waiting only stalls forever -> cold start directly;
//   - restartPolicy=Always/OnFailure: kubelet restarts the init in place
//     (exponential backoff); the controller waits for the restart and touches
//     again, counting by the init's restartCount, cold start only when
//     restartCount >= retryCount (with retryCount=0 the first failure already
//     cold-starts).
//
// Same display gauge as runc case A (handleRuncRestoreFailure): kubelet's
// restartCount synced into PhaseRetries (visible via describe), History
// appended only on restartCount growth and the event emitted only on growth,
// avoiding duplicate appends / event spam every reconcile round.
func (h *ReleaseInitHandler) handleInitContainerFailure(ctx context.Context,
	jobReq *v1beta1.ContainerStateMigrationJobRequest, dest *v1beta1.JobDestinationStatus,
	pod *corev1.Pod, exitCode int32, restartCount int32) {
	ctxLog := log.FromContext(ctx)
	retryCount := retryCountOf(jobReq)

	if pod.Spec.RestartPolicy == corev1.RestartPolicyNever {
		message := fmt.Sprintf(
			"Migration init container of pod %s exited with code %d and restartPolicy=Never, kubelet will not restart it",
			dest.Pod.String(), exitCode)
		enterColdStart(dest, "InitContainerFailed", message)
		h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeWarning, "ColdStartFallback",
			"Pod %s: init container exited with code %d and restartPolicy=Never, falling back to cold start",
			dest.Pod.String(), exitCode)
		return
	}

	// Pod Failed is terminal: kubelet will not restart the init container
	// (under eviction / node pressure restartCount stays 0); waiting for
	// restartCount to grow only stalls forever, cold start directly.
	if pod.Status.Phase == corev1.PodFailed {
		enterColdStart(dest, "PodFailedTerminal", fmt.Sprintf(
			"Migration init container of pod %s failed and pod entered Failed phase, kubelet will not restart it (exit code %d)",
			dest.Pod.String(), exitCode))
		h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeWarning, "ColdStartFallback",
			"Pod %s: pod entered Failed phase, kubelet will not restart its init container, falling back to cold start (exit code %d)",
			dest.Pod.String(), exitCode)
		return
	}

	added := syncInitRestartStatus(dest, "InitContainerFailed",
		fmt.Sprintf("init container exited with code %d", exitCode), restartCount, retryCount)

	if int(restartCount) >= retryCount {
		// restartCount reaching retryCount means the failure that round cold-starts (first attempt + N retries).
		enterColdStart(dest,
			fmt.Sprintf("Init container retries exhausted (restartCount=%d, retryCount=%d)", restartCount, retryCount),
			fmt.Sprintf("Migration init container of pod %s exited with code %d", dest.Pod.String(), exitCode))
		h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeWarning, "ColdStartFallback",
			"Pod %s: init container failed and retries exhausted (restartCount=%d/%d), falling back to cold start",
			dest.Pod.String(), restartCount, retryCount)
		ctxLog.Info("init container retries exhausted, falling back to cold start",
			"pod", dest.Pod.String(), "restartCount", restartCount, "retryCount", retryCount)
		return
	}

	// Not exhausted: wait for kubelet's in-place restart of the init, then touch to release.
	dest.Message = fmt.Sprintf("Init container exited with code %d, waiting kubelet restart (restartCount=%d/%d)",
		exitCode, restartCount, retryCount)
	if added {
		h.sharedContext.EventRecorder.Eventf(jobReq, corev1.EventTypeWarning, "InitContainerRestarting",
			"Pod %s: init container exited with code %d (attempt %d/%d), waiting for kubelet to restart it",
			dest.Pod.String(), exitCode, restartCount+1, retryCount)
	}
	ctxLog.Info("init container failed, waiting for kubelet restart",
		"pod", dest.Pod.String(), "exitCode", exitCode, "restartCount", restartCount, "retryCount", retryCount)
}

// syncInitRestartStatus syncs kubelet's init restartCount into the InitRestart
// entry of PhaseRetries (visible via describe); History is appended only when
// restartCount grows. Returns whether History grew this round (callers emit the
// event only on growth, avoiding per-round spam). Same display gauge as runc
// case A (handleRuncRestoreFailure).
func syncInitRestartStatus(dest *v1beta1.JobDestinationStatus, reason, message string,
	restartCount int32, retryCount int) bool {
	entry := retryStatusOf(dest, v1beta1.RetryPhaseInitRestart)
	if entry == nil {
		entry = &v1beta1.PhaseRetryStatus{
			Phase:    v1beta1.RetryPhaseInitRestart,
			Attempts: int(restartCount) + 1,
			MaxRetry: retryCount,
		}
		// Backfill to the same count as restartCount on first creation: the
		// controller may have missed the first few restarts (own restart / slow
		// reconcile); if it only wrote one entry, the next round's len change from
		// backfill would be misjudged as restartCount growth and emit the event
		// again.
		for i := 0; i < int(restartCount)+1; i++ {
			entry.History = append(entry.History, v1beta1.RestoreAttempt{
				Reason:  reason,
				Message: message,
				Time:    metav1.Now(),
			})
		}
		dest.PhaseRetries = append(dest.PhaseRetries, *entry)
		return true
	}
	entry.Attempts = int(restartCount) + 1
	entry.MaxRetry = retryCount
	before := len(entry.History)
	for len(entry.History) < int(restartCount)+1 {
		entry.History = append(entry.History, v1beta1.RestoreAttempt{
			Reason:  reason,
			Message: message,
			Time:    metav1.Now(),
		})
	}
	return len(entry.History) > before
}

// destInInitReleaseBackoff reports whether the replica is in the 3s backoff
// period after an exec/touch failure. Only effective for replicas in
// SnapshotPrepared that already have an InitRelease failure record; within the
// backoff window no exec is attempted, waiting for kubelet to bring the init
// up.
func destInInitReleaseBackoff(dest *v1beta1.JobDestinationStatus) bool {
	if dest.Phase != v1beta1.DestPhaseSnapshotPrepared {
		return false
	}
	entry := retryStatusOf(dest, v1beta1.RetryPhaseInitRelease)
	if entry == nil || len(entry.History) == 0 {
		return false
	}
	last := entry.History[len(entry.History)-1].Time.Time
	return time.Since(last) < phaseRetryBackoff
}
