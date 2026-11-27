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
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

const ControllerName = "activemonitoring"

const (
	APIGroup     = "runtimekit.io"
	APIVersion   = "v1alpha1"
	ResourceName = "activemonitorings"
	Kind         = "ActiveMonitoring"
)

const (
	// LabelActiveMonitoring marks a Pod as a detection Pod (=true). Shared by all three.
	LabelActiveMonitoring = "rt-diag"
	// LabelActiveMonitoringCR associates a Pod with its owning ActiveMonitoring CR name.
	LabelActiveMonitoringCR = "rt-diag-cr"
	// LabelActiveMonitoringTask associates a Pod with a specific TaskID.
	LabelActiveMonitoringTask = "rt-diag-task"
	// LabelActiveMonitoringRound records the cycle-round ("{cycle}-{completedRounds}") a
	// detection Pod belongs to, so Pod adoption (findTaskPod) matches only the
	// current round's Pod and never a stale one from a previous round/cycle.
	LabelActiveMonitoringRound   = "rt-diag-round"
	LabelActiveMonitoringAttempt = "rt-diag-attempt"

	// AnnoActiveMonitoringTargetMinor pins the detection Pod onto a specific GPU minor.
	AnnoActiveMonitoringTargetMinor = "rt-diag-target-minor"

	EnvXPUVisibleDevices = "XPU_VISIBLE_DEVICES"

	// AnnoNodeDevices is the node annotation carrying per-card device info (GPU minor).
	AnnoNodeDevices = "aliyun.com/device"
)

const (
	DefaultSchedulerName = "lj-runtimekit-scheduler"
	// ActiveMonitoringPriorityClassName is the low-priority PriorityClass assigned to
	// detection Pods. Detection Pods request no GPU resource, so they never starve business
	// Pods on the GPU pool; the low priority still lets a business Pod preempt a detection
	// Pod for the CPU/memory it does request when the node is tight.
	ActiveMonitoringPriorityClassName = "runtimekit-activemonitoring"
	// HostnameLabelKey is the well-known node label used for node pinning.
	HostnameLabelKey      = "kubernetes.io/hostname"
	NodeEnabledLabelKey   = "lj_runtimekit_enable"
	NodeEnabledLabelValue = "true"
	// ActiveMonitoringContainerName is the single container name in a detection Pod.
	ActiveMonitoringContainerName = "diag"
	// LJRuntimeKitRuntimeClassName is the RuntimeClass assigned to detection Pods
	// so they run under the lj-runtimekit container runtime handler.
	LJRuntimeKitRuntimeClassName = "lj-runtime"
)

const (
	defaultGPUResourceName = "aliyun.com/gpu"
	defaultMaxRetries      = 3
	defaultRoundsPerCycle  = 1
	defaultPendingTimeout  = "5m"
)

const (
	defaultTestImage                     = "lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-diag:v1.14.0-k8s1.34"
	defaultTerminationGracePeriodSeconds = int64(5)
	// defaultRuntimeKitSocket is the hostPath of the x-device-manager dp2mgr unix socket,
	// mounted into detection Pods for RuntimeKit IPC.
	defaultRuntimeKitSocket = "/run/amp/eKitC/xdm/sockets/dp2mgr.sock"
	// runtimeKitSocketVolumeName is the Pod Volume / VolumeMount name for the RuntimeKit socket.
	runtimeKitSocketVolumeName = "runtimekit-socket"
)

// Detection verdict tokens written by the detection image into
// /dev/termination-log ("status" field). Only PASS yields TaskResult.Passed=true;
// FAIL/ERROR both mean passed=false but are kept distinct in Result.Errors.
const (
	// detectionStatusPass: report complete, all checks passed, tool exited cleanly.
	detectionStatusPass = "PASS"
	// detectionStatusFail: report explicitly flags failed checks (may carry bad-card info).
	detectionStatusFail = "FAIL"
	// detectionStatusError: environment/startup/report-parsing anomaly or interrupted
	// diagnosis; pass cannot be confirmed.
	detectionStatusError = "ERROR"
)

const (
	podNamePrefix = "rt-diag"
	// randSuffixLen is the random suffix length for unique Pod names.
	randSuffixLen = 5
	// maxTaskErrors caps the accumulated error entries per Task within a cycle,
	// dropping the oldest beyond the cap to prevent unbounded growth.
	maxTaskErrors = 32
	// deleteGraceSeconds is the small grace period used when deleting detection
	// Pods (finishRound / retry paths) so they exit quickly for DP preemption.
	deleteGraceSeconds = int64(3)
	// periodicResyncInterval drives the ticker that re-enqueues every CR, so
	// cycle-interval elapsing, pendingTimeout exemption and node availability
	// are re-evaluated even without Pod/CR events.
	periodicResyncInterval = 15 * time.Second
	// indexByCR is the Pod informer indexer name mapping to "namespace/crName".
	indexByCR = "byActiveMonitoringCR"
)

var (
	activemonitoringGVR = schema.GroupVersionResource{Group: APIGroup, Version: APIVersion, Resource: ResourceName}
	podGVR              = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
	nodeGVR             = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "nodes"}
	configMapGVR        = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}
)
