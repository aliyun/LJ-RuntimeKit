/*
Copyright 2026.

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

package v1alpha1

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ProfilingEnableLabel marks a Pod as profiling-capable; the device plugin
// injects the Runtime Kit profiling environment at Allocate time.
const ProfilingEnableLabel = "rt-profiling-enable"

// ProfilingMode is the collection granularity.
// +kubebuilder:validation:Enum=light;full
type ProfilingMode string

const (
	// ProfilingModeLight 表示低开销、近乎无损的轻量采集。
	ProfilingModeLight ProfilingMode = "light"
	// ProfilingModeFull is lossy but covers the full call chain.
	ProfilingModeFull ProfilingMode = "full"
)

const (
	// MaxDurationFull is the longest collection the profiler accepts in full mode.
	MaxDurationFull = 15 * time.Second
	// MaxDurationLight 是 light 模式允许的最长采集时长。
	MaxDurationLight = time.Minute
)

// MaxDuration returns the collection limit of the mode.
func (m ProfilingMode) MaxDuration() time.Duration {
	if m == ProfilingModeFull {
		return MaxDurationFull
	}
	return MaxDurationLight
}

// ProfilingPhase is the lifecycle phase of the current profiling round.
// +kubebuilder:validation:Enum=Pending;Running;Completed;Failed;Canceled
type ProfilingPhase string

const (
	PhasePending   ProfilingPhase = "Pending"
	PhaseRunning   ProfilingPhase = "Running"
	PhaseCompleted ProfilingPhase = "Completed"
	PhaseFailed    ProfilingPhase = "Failed"
	PhaseCanceled  ProfilingPhase = "Canceled"
)

// IsTerminal reports whether the round has finished and will not progress
// further without a new runIndex.
func (p ProfilingPhase) IsTerminal() bool {
	return p == PhaseCompleted || p == PhaseFailed
}

// TargetStage is the progress of one target Pod within a round.
// +kubebuilder:validation:Enum=Queued;Collecting;Retrying;Completed;Failed;Canceled
type TargetStage string

const (
	StageQueued     TargetStage = "Queued"
	StageCollecting TargetStage = "Collecting"
	StageRetrying   TargetStage = "Retrying"
	StageCompleted  TargetStage = "Completed"
	StageFailed     TargetStage = "Failed"
	StageCanceled   TargetStage = "Canceled"
)

// IsTerminal reports whether the target needs no further work in this collection.
func (s TargetStage) IsTerminal() bool {
	return s == StageCompleted || s == StageFailed || s == StageCanceled
}

// TargetSelector selects the Pods to profile. Exactly one of matchLabels or
// podName+podNamespace must be set. matchLabels matches Pods in the
// ProfilingJob's own namespace.
// +kubebuilder:validation:XValidation:rule="(has(self.matchLabels) && size(self.matchLabels) > 0) != has(self.podName)",message="exactly one of matchLabels or podName+podNamespace must be specified"
// +kubebuilder:validation:XValidation:rule="has(self.podName) == has(self.podNamespace)",message="podName and podNamespace must be specified together"
type TargetSelector struct {
	// MatchLabels selects target Pods by label.
	// +optional
	MatchLabels map[string]string `json:"matchLabels,omitempty"`

	// PodName is the name of a single target Pod.
	// +kubebuilder:validation:MinLength=1
	// +optional
	PodName string `json:"podName,omitempty"`

	// PodNamespace is the namespace of the Pod referenced by podName.
	// +kubebuilder:validation:MinLength=1
	// +optional
	PodNamespace string `json:"podNamespace,omitempty"`
}

// ProfilingJobSpec defines the desired state of ProfilingJob.
type ProfilingJobSpec struct {
	// TargetSelector selects the Pods to profile. Immutable.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="targetSelector is immutable"
	TargetSelector TargetSelector `json:"targetSelector"`

	// ProfilingRules references a ProfilingRule that defines the collection
	// policy (kernels to collect, trigger conditions, ...). Reserved for a
	// future version.
	// +optional
	ProfilingRules string `json:"profilingRules,omitempty"`

	// RunIndex numbers profiling rounds. The value at creation, including 0,
	// starts the first round; each later increase starts a new round. Each
	// update may keep it unchanged or increase it by exactly 1.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:XValidation:rule="self == oldSelf || self == oldSelf + 1",message="runIndex can only stay unchanged or increase by 1"
	RunIndex int64 `json:"runIndex"`

	// Duration is the full collection time for every target retry and resume.
	// 超过 spec.profilingMode 上限（full 15s、light 60s）的时长，
	// 在本轮冻结 status.runConfig 时截断至该上限。
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MaxLength=32
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ns|us|µs|ms|s|m|h))+$`
	// +kubebuilder:validation:XValidation:rule="duration(self) <= duration('2562047h')",message="duration must parse into int64 nanoseconds (at most 2562047h)"
	Duration metav1.Duration `json:"duration"`

	// ProfilingMode is the collection granularity.
	// +kubebuilder:default=light
	// +optional
	ProfilingMode ProfilingMode `json:"profilingMode,omitempty"`

	// PersistentVolumeClaimName is mounted directly by collector Pods. Immutable.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="persistentVolumeClaimName is immutable"
	PersistentVolumeClaimName string `json:"persistentVolumeClaimName"`

	// PersistentVolumeClaimNamespace is the namespace of the PVC and collectors. Immutable.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="persistentVolumeClaimNamespace is immutable"
	PersistentVolumeClaimNamespace string `json:"persistentVolumeClaimNamespace"`

	// Cancel stops active rounds as Canceled; CleanupComplete reports Pod deletion.
	// Resumes and new rounds wait until all associated collectors are removed.
	// +kubebuilder:default=false
	// +optional
	Cancel bool `json:"cancel,omitempty"`
}

// HistoryEntry records a finished profiling round.
type HistoryEntry struct {
	RunIndex int64          `json:"runIndex"`
	Phase    ProfilingPhase `json:"phase"`
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`
	// +optional
	EndTime *metav1.Time `json:"endTime,omitempty"`
	// +optional
	OutputLocation string `json:"outputLocation,omitempty"`
	// CollectedDuration is the collected time in milliseconds.
	// +optional
	CollectedDuration int64 `json:"collectedDuration,omitempty"`
}

// TargetStatus tracks one selected Pod. The target set is fixed when a round
// (or a resumed collection) resolves its Pods.
type TargetStatus struct {
	TargetNamespace string `json:"targetNamespace"`
	TargetName      string `json:"targetName"`
	TargetUID       string `json:"targetUID"`
	TargetNodeName  string `json:"targetNodeName"`
	// TargetContainerIds 是待采集 GPU 容器的运行时 ID，按 ID 排序。
	TargetContainerIds []string    `json:"targetContainerIds"`
	Stage              TargetStage `json:"stage"`
	// +optional
	CollectorPodName string `json:"collectorPodName,omitempty"`
	// +optional
	CollectorPodUID string `json:"collectorPodUID,omitempty"`
	// +optional
	RetryCount int32 `json:"retryCount,omitempty"`
	// +optional
	RetryAt *metav1.Time `json:"retryAt,omitempty"`
	// +optional
	CollectorCreatedAt *metav1.Time `json:"collectorCreatedAt,omitempty"`
	// +optional
	Reason string `json:"reason,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
	// OutputFiles are this target's data files, as paths relative to the PVC
	// volume root, set when the target completes.
	// +optional
	OutputFiles []string `json:"outputFiles,omitempty"`
	// CollectedDuration is the duration reported by Runtime Kit in milliseconds.
	// +optional
	CollectedDuration int64 `json:"collectedDuration,omitempty"`
}

type RunConfiguration struct {
	// +optional
	ProfilingRules string `json:"profilingRules,omitempty"`
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MaxLength=32
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ns|us|µs|ms|s|m|h))+$`
	// +kubebuilder:validation:XValidation:rule="duration(self) <= duration('2562047h')",message="duration must parse into int64 nanoseconds (at most 2562047h)"
	Duration      metav1.Duration `json:"duration"`
	ProfilingMode ProfilingMode   `json:"profilingMode"`
}

type ProfilingJobStatus struct {
	// RunConfig freezes mutable CR inputs across retries and canceled-round resumes.
	// +optional
	RunConfig *RunConfiguration `json:"runConfig,omitempty"`
	// Phase of the current round.
	// +optional
	Phase ProfilingPhase `json:"phase,omitempty"`

	// Message 描述当前进度、等待或失败原因；清理诊断附加在结果之后。
	// 仅用于诊断，不作为恢复、新轮次或 finalizer 放行的依据。
	// +optional
	Message string `json:"message,omitempty"`

	// LastRunIndex is the latest spec.runIndex that has been picked up; unset
	// until the first round starts.
	// +optional
	LastRunIndex *int64 `json:"lastRunIndex,omitempty"`

	// StartTime 是本次采集第一个 collector 容器开始运行的时间；恢复时清空并重记。
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// EndTime 是本次采集的结束时间；恢复时清空。
	// +optional
	EndTime *metav1.Time `json:"endTime,omitempty"`

	// OutputLocation is the current round's directory relative to the PVC
	// volume root, set once its first collector starts running.
	// +optional
	OutputLocation string `json:"outputLocation,omitempty"`

	// CollectedDuration 是本次采集从 startTime 到当前时间或终态 endTime 的墙钟耗时（毫秒）。
	// collector 尚未运行时为 0，不累加此前采集或取消后等待恢复的时间。
	// +optional
	CollectedDuration int64 `json:"collectedDuration,omitempty"`

	// Targets is the per-Pod progress of the latest collection.
	// +optional
	Targets []TargetStatus `json:"targets,omitempty"`

	// History keeps the most recent finished rounds; older ones are pruned.
	// +optional
	History []HistoryEntry `json:"history,omitempty"`

	// ObservedGeneration is the most recent generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=pj
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="RunIndex",type=integer,JSONPath=`.spec.runIndex`
// +kubebuilder:printcolumn:name="LastRun",type=integer,JSONPath=`.status.lastRunIndex`
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=`.spec.profilingMode`
// +kubebuilder:printcolumn:name="Duration",type=string,JSONPath=`.spec.duration`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ProfilingJob controls GPU profiling and direct PVC output for a set of Pods.
type ProfilingJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +kubebuilder:validation:Required
	Spec   ProfilingJobSpec   `json:"spec"`
	Status ProfilingJobStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ProfilingJobList contains a list of ProfilingJob.
type ProfilingJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ProfilingJob `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ProfilingJob{}, &ProfilingJobList{})
}
