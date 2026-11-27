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
	"time"

	"github.com/container-migration/controller/api/v1beta1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// phaseRetryBackoff is the fixed backoff between stage retries (3s): only
// applies to controller-initiated retry actions (download redispatch, XPU
// cr-tool rerun); the runc-restore retry pace is controlled by kubelet's
// CrashLoopBackOff and does not apply here.
const phaseRetryBackoff = 3 * time.Second

// retryCountOf takes the restore stage retry count of this CR; 0 (no retry) when restore is not configured.
func retryCountOf(jobReq *v1beta1.ContainerStateMigrationJobRequest) int {
	if jobReq.Spec.Restore == nil {
		return 0
	}
	return jobReq.Spec.Restore.RetryCount
}

// retryStatusOf takes the count entry of a stage (may be nil).
func retryStatusOf(dest *v1beta1.JobDestinationStatus, phase v1beta1.RetryPhase) *v1beta1.PhaseRetryStatus {
	for i := range dest.PhaseRetries {
		if dest.PhaseRetries[i].Phase == phase {
			return &dest.PhaseRetries[i]
		}
	}
	return nil
}

// recordAttempt records one failure of a stage into dest.PhaseRetries (visible
// via describe) and returns the stage's accumulated failure count (including
// this one). Repeated calls for the same dest within one reconcile round count
// repeatedly - callers must guarantee at most one record per round per dest
// per stage.
func recordAttempt(dest *v1beta1.JobDestinationStatus, phase v1beta1.RetryPhase,
	reason, message string, maxRetry int) int {
	entry := retryStatusOf(dest, phase)
	if entry == nil {
		dest.PhaseRetries = append(dest.PhaseRetries, v1beta1.PhaseRetryStatus{
			Phase:    phase,
			Attempts: 1,
			MaxRetry: maxRetry,
			History: []v1beta1.RestoreAttempt{{
				Reason:  reason,
				Message: message,
				Time:    metav1.Now(),
			}},
		})
		return 1
	}
	entry.Attempts++
	entry.MaxRetry = maxRetry
	entry.History = append(entry.History, v1beta1.RestoreAttempt{
		Reason:  reason,
		Message: message,
		Time:    metav1.Now(),
	})
	return entry.Attempts
}

// coldStartStage are the cold-start sub-stage constants, corresponding to api/v1beta1.ColdStartStatus.Stage.
const (
	coldStartStageDeletePod   = "DeletingPod"
	coldStartStageCreatePod   = "CreatingPod"
	coldStartStageWaitRunning = "WaitingRunning"
)

// enterColdStart switches a replica into the cold-start fallback. The caller
// must have confirmed the replica is currently failed (judge success first,
// then exhaustion; re-read the state once more before deleting).
//
// Progress is then driven by ColdStartHandler: delete the failed pod -> wait
// NotFound -> build a same-name cold-start pod -> wait Running -> set
// Succeeded (startupMode=ColdStart).
func enterColdStart(dest *v1beta1.JobDestinationStatus, reason, message string) {
	dest.Phase = v1beta1.DestPhaseColdStarting
	dest.ColdStart = &v1beta1.ColdStartStatus{
		Stage:     coldStartStageDeletePod,
		Reason:    reason,
		StartedAt: metav1.Now(),
	}
	dest.Message = message
}
