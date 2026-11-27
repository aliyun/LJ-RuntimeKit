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

// Package v1alpha1 contains the ActiveMonitoring CRD types (group runtimekit.io).
// The controller operates on these objects through DynamicClient +
// unstructured; the strongly-typed Go definitions here are used for
// conversion and deepcopy only.
package v1alpha1

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CR-level Phase values (ActiveMonitoringStatus.Phase).
const (
	PhasePending   = "Pending"
	PhaseRunning   = "Running"
	PhaseCompleted = "Completed"
	PhaseFailed    = "Failed"
)

// Per-task State values (TaskStatus.State).
const (
	TaskStatePending   = "Pending"
	TaskStateRunning   = "Running"
	TaskStatePreempted = "Preempted"
	TaskStateCompleted = "Completed"
	TaskStateFailed    = "Failed"
)

// Condition types reported on ActiveMonitoringStatus.Conditions.
const (
	ConditionQueueInitialized = "QueueInitialized"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:subresource:status

// ActiveMonitoring declares a per-card GPU detection policy. The controller materialises
// a queue of pinned detection Pods (one per target minor), watches their
// lifecycle, and reports aggregated results on Status.
type ActiveMonitoring struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ActiveMonitoringSpec   `json:"spec,omitempty"`
	Status ActiveMonitoringStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ActiveMonitoringList provides a list of ActiveMonitorings.
type ActiveMonitoringList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []ActiveMonitoring `json:"items"`
}

// ActiveMonitoringSpec is the desired detection policy.
type ActiveMonitoringSpec struct {
	AllLjRuntimeKitEnabled bool         `json:"allLjRuntimeKitEnabled,omitempty"`
	Affinity               *v1.Affinity `json:"affinity,omitempty"`
	// Schedule controls cycle/round timing.
	Schedule SchedulePolicy `json:"schedule,omitempty"`
	// Concurrency caps the number of detection Pods that effectively occupy a
	// slot (Running + non-timed-out Pending). 0 means unlimited.
	Concurrency int `json:"concurrency,omitempty"`
}

// SchedulePolicy controls cycle and round timing.
type SchedulePolicy struct {
	Interval       string `json:"interval,omitempty"`
	RoundsPerCycle int    `json:"roundsPerCycle,omitempty"`
	TotalCycles    int    `json:"totalCycles,omitempty"`
}

// ActiveMonitoringStatus is the observed state, written only by the controller.
type ActiveMonitoringStatus struct {
	Phase          string             `json:"phase,omitempty"`
	TotalTasks     int                `json:"totalTasks,omitempty"`
	CompletedTasks int                `json:"completedTasks,omitempty"`
	RunningTasks   int                `json:"runningTasks,omitempty"`
	PendingTasks   int                `json:"pendingTasks,omitempty"`
	PreemptedTasks int                `json:"preemptedTasks,omitempty"`
	FailedTasks    int                `json:"failedTasks,omitempty"`
	Queue          []TaskStatus       `json:"queue,omitempty"`
	Conditions     []metav1.Condition `json:"conditions,omitempty"`
}

// TaskStatus is the per-card detection task state.
type TaskStatus struct {
	// ID is the stable primary key: {nodeName}-gpu-{minor}.
	ID              string       `json:"id"`
	NodeName        string       `json:"nodeName"`
	GPUMinor        int          `json:"gpuMinor"`
	CurrentCycle    int          `json:"currentCycle,omitempty"`
	State           string       `json:"state"`
	PodName         string       `json:"podName,omitempty"`
	Attempts        int          `json:"attempts,omitempty"`
	CompletedRounds int          `json:"completedRounds,omitempty"`
	Result          *TaskResult  `json:"result,omitempty"`
	StartTime       *metav1.Time `json:"startTime,omitempty"`
	EndTime         *metav1.Time `json:"endTime,omitempty"`
	LastPreemptTime *metav1.Time `json:"lastPreemptTime,omitempty"`
}

type TaskResult struct {
	Passed   bool  `json:"passed"`
	Duration int64 `json:"duration,omitempty"`
	// Errors accumulates failure detail across rounds/attempts within a cycle.
	Errors []string `json:"errors,omitempty"`
}
