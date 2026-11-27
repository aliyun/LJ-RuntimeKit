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

package activemonitoring

import (
	"encoding/json"
	"fmt"
	"strings"

	"k8s.io/klog/v2"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	gpuv1alpha1 "github.com/lj-runtimekit/job-controller/pkg/apis/gpu/v1alpha1"
)

// terminationResult is the JSON schema the detection container writes to
// /dev/termination-log: a verdict token plus an optional detail message, e.g.
// {"status":"ERROR","message":"environment_check_failed: ..."}.
type terminationResult struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

// detectionVerdict is the parsed outcome of a single detection run.
type detectionVerdict struct {
	// Status is the raw verdict token reported by the container
	// (detectionStatusPass / detectionStatusFail / detectionStatusError).
	Status string
	// Result carries Passed/Duration for mergeResult. For a non-PASS run its
	// Errors hold the raw container message so annotateFailedRun can prefix it
	// in place exactly once.
	Result *gpuv1alpha1.TaskResult
}

// parseTerminationMessage reads the first container's terminated.message and
// parses it into a detectionVerdict. Passed is derived from the status token
// (only PASS counts as passed); a missing/empty/invalid message or an unknown
// status returns an error so the caller can treat it as an infrastructure
// failure and retry.
func parseTerminationMessage(obj *unstructured.Unstructured) (*detectionVerdict, error) {
	if obj == nil {
		return nil, fmt.Errorf("nil pod")
	}
	var pod v1.Pod
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &pod); err != nil {
		return nil, err
	}
	if len(pod.Status.ContainerStatuses) == 0 {
		return nil, fmt.Errorf("no container status")
	}
	term := pod.Status.ContainerStatuses[0].State.Terminated
	if term == nil || term.Message == "" {
		return nil, fmt.Errorf("empty termination message")
	}
	var tr terminationResult
	if err := json.Unmarshal([]byte(term.Message), &tr); err != nil {
		return nil, err
	}
	switch tr.Status {
	case detectionStatusPass, detectionStatusFail, detectionStatusError:
	default:
		return nil, fmt.Errorf("unknown detection status %q", tr.Status)
	}
	res := &gpuv1alpha1.TaskResult{
		Passed:   tr.Status == detectionStatusPass,
		Duration: terminatedDuration(term),
	}
	if !res.Passed && tr.Message != "" {
		res.Errors = []string{tr.Message}
	}
	return &detectionVerdict{Status: tr.Status, Result: res}, nil
}

func podFailureReason(obj *unstructured.Unstructured) string {
	if obj == nil {
		return "unknown reason"
	}
	var pod v1.Pod
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &pod); err != nil {
		klog.ErrorS(err, "cannot read pod failure reason")
		return "unknown reason"
	}
	formatReason := func(reason, message string) string {
		reason, message = strings.TrimSpace(reason), strings.TrimSpace(message)
		if reason == "" {
			return message
		}
		if message == "" || message == reason {
			return reason
		}
		return reason + ": " + message
	}
	if detail := formatReason(pod.Status.Reason, pod.Status.Message); detail != "" {
		return detail
	}

	var failures []string
	for _, statuses := range [][]v1.ContainerStatus{pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses} {
		for _, status := range statuses {
			term := status.State.Terminated
			if term == nil || term.ExitCode == 0 && term.Signal == 0 {
				continue
			}
			exit := fmt.Sprintf("exit code %d", term.ExitCode)
			if term.Signal != 0 {
				exit += fmt.Sprintf(", signal %d", term.Signal)
			}
			detail := formatReason(term.Reason, term.Message)
			if detail == "" {
				detail = exit
			} else {
				detail += " (" + exit + ")"
			}
			failures = append(failures, fmt.Sprintf("container %s: %s", status.Name, detail))
		}
	}
	if len(failures) > 0 {
		return strings.Join(failures, "; ")
	}
	return "unknown reason"
}

// terminatedDuration computes the detection wall time in seconds from the
// container's terminated timestamps, since the image no longer reports it.
func terminatedDuration(term *v1.ContainerStateTerminated) int64 {
	if term.StartedAt.IsZero() || term.FinishedAt.IsZero() {
		return 0
	}
	if d := term.FinishedAt.Sub(term.StartedAt.Time); d > 0 {
		return int64(d.Seconds())
	}
	return 0
}
