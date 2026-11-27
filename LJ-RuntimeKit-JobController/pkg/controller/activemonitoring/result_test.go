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
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// testTerminatedBase is the fixed StartedAt base used to derive the detection
// wall time: FinishedAt = base + runSecs, so Duration must equal runSecs.
var testTerminatedBase = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func succeededPodWithMessage(message string, withContainerStatus bool, runSecs int64) *v1.Pod {
	pod := &v1.Pod{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default"},
		Status:     v1.PodStatus{Phase: v1.PodSucceeded},
	}
	if withContainerStatus {
		pod.Status.ContainerStatuses = []v1.ContainerStatus{{
			Name: ActiveMonitoringContainerName,
			State: v1.ContainerState{
				Terminated: &v1.ContainerStateTerminated{
					Message:    message,
					ExitCode:   0,
					StartedAt:  metav1.NewTime(testTerminatedBase),
					FinishedAt: metav1.NewTime(testTerminatedBase.Add(time.Duration(runSecs) * time.Second)),
				},
			},
		}}
	}
	return pod
}

func TestParseTerminationMessage(t *testing.T) {
	tests := []struct {
		name       string
		message    string
		withStatus bool
		runSecs    int64
		wantErr    bool
		wantPassed bool
		wantDur    int64
		wantErrLen int
	}{
		{name: "pass verdict", message: `{"status":"PASS","message":""}`, withStatus: true, runSecs: 300, wantPassed: true, wantDur: 300},
		{name: "fail verdict carries message", message: `{"status":"FAIL","message":"SDC detected"}`, withStatus: true, runSecs: 120, wantPassed: false, wantDur: 120, wantErrLen: 1},
		{
			name:       "error verdict carries message",
			message:    `{"status":"ERROR","message":"environment_check_failed: Environment validation failed; diagnosis was not started."}`,
			withStatus: true, runSecs: 2, wantPassed: false, wantDur: 2, wantErrLen: 1,
		},
		{name: "fail verdict without message", message: `{"status":"FAIL"}`, withStatus: true, runSecs: 1, wantPassed: false, wantDur: 1},
		{name: "unknown status", message: `{"status":"WEIRD","message":"x"}`, withStatus: true, wantErr: true},
		{name: "missing status", message: `{"message":"x"}`, withStatus: true, wantErr: true},
		{name: "invalid json", message: `not-json`, withStatus: true, wantErr: true},
		{name: "empty message", message: ``, withStatus: true, wantErr: true},
		{name: "no container status", withStatus: false, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj := mustUnstructured(t, succeededPodWithMessage(tt.message, tt.withStatus, tt.runSecs))
			verdict, err := parseTerminationMessage(obj)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got verdict %+v", verdict)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if verdict.Result.Passed != tt.wantPassed || verdict.Result.Duration != tt.wantDur {
				t.Fatalf("result = %+v, want passed=%v duration=%d", verdict.Result, tt.wantPassed, tt.wantDur)
			}
			if len(verdict.Result.Errors) != tt.wantErrLen {
				t.Fatalf("errors len = %d, want %d", len(verdict.Result.Errors), tt.wantErrLen)
			}
		})
	}
}

func TestParseTerminationMessageNil(t *testing.T) {
	if _, err := parseTerminationMessage(nil); err == nil {
		t.Fatal("expected error for nil pod")
	}
}
