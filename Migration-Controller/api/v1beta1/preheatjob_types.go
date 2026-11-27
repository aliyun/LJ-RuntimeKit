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

package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ===================== Phase types =====================

// PreheatJobPhase defines the phase of a preheat task.
// +kubebuilder:validation:Enum=Pending;Running;Probing;Ready;Failed;Stale
type PreheatJobPhase string

const (
	// PreheatJobPhasePending: CR just created, preheat not started yet.
	PreheatJobPhasePending PreheatJobPhase = "Pending"
	// PreheatJobPhaseRunning: preheat in progress.
	PreheatJobPhaseRunning PreheatJobPhase = "Running"
	// PreheatJobPhaseProbing: the Dragonfly preheat task completed, waiting for
	// ProbeController's first probe to confirm the cache state before turning
	// into Ready or Stale.
	PreheatJobPhaseProbing PreheatJobPhase = "Probing"
	// PreheatJobPhaseReady: preheated and usable.
	PreheatJobPhaseReady PreheatJobPhase = "Ready"
	// PreheatJobPhaseFailed: preheat failed.
	PreheatJobPhaseFailed PreheatJobPhase = "Failed"
	// PreheatJobPhaseStale: the preheat has gone stale and is unusable.
	PreheatJobPhaseStale PreheatJobPhase = "Stale"
)

// ===================== Spec types =====================

// PreheatJobSpec defines the desired state of PreheatJob
//
// RemoteURL, CheckpointUID and Pvc are mutually exclusive: when only
// CheckpointUID is filled, the controller resolves the preheat source from the
// referenced checkpoint CR at reconcile time (storageType oss/dragonfly
// resolves RemoteURL and OSS credentials; pvc resolves pvcName/pvcNamespace
// and the PVC-internal path); the resolved result is used only for that round
// and never written back to this CR's spec. Filling more than one is rejected
// by controller validation in the Pending phase.
//
// The mutual exclusion is not enforced at the CRD level: CEL
// x-kubernetes-validations requires K8s 1.25+ while the target cluster is
// 1.23, so all fields stay optional and the controller validates in the
// Pending phase.
type PreheatJobSpec struct {
	// RemoteURL is the full http(s) path of the remote OSS object, e.g.
	// https://{bucket}.{ossEndpoint}/{objectName}.
	// +optional
	RemoteURL string `json:"remoteURL,omitempty"`

	// CheckpointUID is the metadata.uid of the referenced checkpoint CR
	// (ContainerStateMigrationJobRequest), same semantics as
	// spec.restore.checkpointUID. Filling it avoids writing RemoteURL and the
	// accessKeyId / accessKeySecret options.
	//
	// A checkpoint only dumps a single container, so the referenced checkpoint
	// maps to exactly one object; no container needs to be specified.
	// +optional
	CheckpointUID string `json:"checkpointUID,omitempty"`

	// Pvc directly specifies a PVC as the preheat source: under the Dragonfly
	// backend the controller starts a preheat source pod mounting the PVC to
	// serve the http origin and preheats the checkpoint directory inside the
	// PVC into the seed peers; the EFC backend (efc-enable=true) starts no pod,
	// the target_path is resolved from the PVC and goes through the warmup
	// endpoint; the copy backend (options filled with
	// target-pvc-name/target-pvc-namespace) starts a one-shot copy pod that
	// copies the checkpoint directory inside the PVC to the same path on the
	// target PVC.
	// +optional
	Pvc *PreheatJobPVC `json:"pvc,omitempty"`

	// Options are extra optional parameters passed through to the preheat task.
	// The dragonfly backend needs accessKeyId / accessKeySecret; if filled here
	// they are used, otherwise taken from the storage options of the referenced
	// checkpoint CR.
	//
	// The preheat mode is selected by these options: when target-pvc-name and
	// target-pvc-namespace are both filled (non-empty), copy preheat (the copy
	// backend, mutually exclusive with efc-enable); when efc-enable is "true"
	// (case-insensitive), EFC preheat; otherwise the default Dragonfly preheat
	// (dragonfly-enable is deprecated and ignored).
	Options map[string]string `json:"options,omitempty"`
}

// PreheatJobPVC directly specifies a PVC as the preheat source.
type PreheatJobPVC struct {
	// PvcName is the PVC name.
	PvcName string `json:"pvcName"`
	// PvcNamespace is the PVC namespace.
	PvcNamespace string `json:"pvcNamespace"`
	// PvcPath is the relative path of the checkpoint directory inside the PVC
	// (below the mount point /mnt/pv), e.g.
	// {sourcePodUID}/{version}/{container}/checkpoint. It must be relative and
	// contain no "."/".." segments, no empty segments and no backslashes
	// (otherwise the preheat task fails directly); any character is allowed
	// inside a segment - checkpoint data may be moved to user directories,
	// paths containing spaces etc. are equally legal.
	PvcPath string `json:"pvcPath"`
}

// ===================== Status types =====================

// PreheatPeerInfo describes one node that successfully preheated.
type PreheatPeerInfo struct {
	// IP is the node IP.
	IP string `json:"ip,omitempty"`
	// NodeName is the node name.
	NodeName string `json:"nodeName,omitempty"`
	// State is the registration state of the seed peer in the Dragonfly
	// scheduler; appearing in ListHosts results means active.
	State string `json:"state,omitempty"`
	// Cached reports whether the file is still cached.
	Cached bool `json:"cached,omitempty"`
	// LastSeen is the last probe time.
	LastSeen *metav1.Time `json:"lastSeen,omitempty"`
	// ExpiredAt is the expiry time.
	ExpiredAt *metav1.Time `json:"expiredAt,omitempty"`
}

// PreheatJobStatus defines the observed state of PreheatJob
type PreheatJobStatus struct {
	// Phase is the current phase: Pending / Running / Ready / Failed / Stale.
	Phase PreheatJobPhase `json:"phase,omitempty"`
	// TaskID is the Dragonfly preheat task ID, used to poll the task status.
	TaskID string `json:"taskID,omitempty"`
	// RemoteURL is the remote address actually used for preheat. When spec only
	// fills pvc or checkpointUID, the controller resolves/generates it and pins
	// it here (a PVC source looks like http://{preheat source Service
	// ClusterIP}:{fixed port}/{pvcPath}), so ProbeController keeps probing the
	// same URL after the preheat source pod is deleted (the task_id is
	// computed from the URL and must match the preheat one exactly). The
	// restore-side worker does not read this field: it derives the ClusterIP
	// of the same Service from (pvcNamespace, pvcName, pvcPath) and rebuilds
	// the URL.
	RemoteURL string `json:"remoteURL,omitempty"`
	// EFCPath is the preheat directory composed by the EFC backend (relative
	// path inside the bucket, e.g.
	// chanke/migration-worker/<sourcePodUID>/<ts>/<container>/checkpoint). Only
	// filled for EFC preheat; the restore-side worker reads it to compose the
	// read path under /mnt/efc.
	EFCPath string `json:"efcPath,omitempty"`
	// Message is the status description or failure reason.
	Message string `json:"message,omitempty"`
	// PeerList is the list of nodes that successfully preheated.
	PeerList []PreheatPeerInfo `json:"peerList,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=phj
// +kubebuilder:printcolumn:name="phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="age",type="date",JSONPath=".metadata.creationTimestamp"

// PreheatJob is the Schema for the preheatjobs API
type PreheatJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PreheatJobSpec   `json:"spec,omitempty"`
	Status PreheatJobStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PreheatJobList contains a list of PreheatJob
type PreheatJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PreheatJob `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PreheatJob{}, &PreheatJobList{})
}
