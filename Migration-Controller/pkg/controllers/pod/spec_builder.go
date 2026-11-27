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

package pod

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/container-migration/controller/api/v1beta1"
	"github.com/container-migration/controller/pkg/labels"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	// checkpointCRNamePrefix is the CR name prefix; the full name is
	// checkpoint-<podUID>. The CR is created in the source pod's own namespace
	// (see buildJobRequest), sharing the pod's lifecycle boundary.
	checkpointCRNamePrefix = "checkpoint-"
)

// checkpointCRName returns the CR name for this pod; podUID and CR are one-to-one.
func checkpointCRName(podUID types.UID) string {
	return checkpointCRNamePrefix + string(podUID)
}

// getCheckpointContainers returns the container names to checkpoint declared
// in the annotation. Passed through as-is; validity is judged by InitHandler.
func getCheckpointContainers(pod *corev1.Pod) []string {
	return splitAndTrim(pod.Annotations[labels.AnnotationCheckpointContainers])
}

// buildJobRequest best-effort constructs a CR that runs the checkpoint stage
// and, when the restore annotations declare it (restore-replicas being a
// positive integer), the restore stage as well, based on the pod's
// annotations.
//
// There is deliberately no "don't create when validation fails" interception:
// as long as the pod declares AnnotationCheckpointEnable="true", a CR must
// land. Field completeness and validity are judged uniformly by the MigrationJob
// controller's InitHandler and written back to status.message - the one place
// users can always look up and that never expires. Intercepting here would
// leave the user with only an Event GCed an hour later; kubectl get csmjr
// would show nothing.
//
// The returned warnings cover the items that cannot be fully expressed in
// the CR. Illegal timeout / automode values are reported on the CR's
// status.message by InitHandler (their raw values are carried over via
// passthrough annotations); other degraded items (policy not valid JSON,
// malformed options section) can only be dropped, so an extra Warning Event
// is added on the pod for all of them.
func buildJobRequest(pod *corev1.Pod) (*v1beta1.ContainerStateMigrationJobRequest, []string) {
	storageType, storage, warnings := buildStorage(pod.Annotations)
	checkpointSpec, checkpointAnnotations, checkpointWarnings := buildCheckpointSpec(pod)
	warnings = append(warnings, checkpointWarnings...)
	restoreSpec, restoreWarnings := buildRestoreSpec(pod)
	warnings = append(warnings, restoreWarnings...)

	annotations := map[string]string{
		labels.AnnotationAutoCreated: "true",
	}
	for k, v := range checkpointAnnotations {
		annotations[k] = v
	}

	return &v1beta1.ContainerStateMigrationJobRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:        checkpointCRName(pod.UID),
			Namespace:   pod.Namespace,
			Annotations: annotations,
		},
		Spec: v1beta1.ContainerStateMigrationJobRequestSpec{
			Checkpoint:  checkpointSpec,
			Restore:     restoreSpec,
			StorageType: storageType,
			Storage:     storage,
		},
	}, warnings
}

// buildCheckpointSpec assembles spec.checkpoint.
//
// The three identity fields of the source pod (required in the CRD) are always
// available; the container list is passed through as-is - empty list, multiple
// containers, or a non-existent container name are all left for InitHandler to
// report.
//
// Timeout and auto mode only land on the spec when the annotation is declared:
// unset leaves the pointer fields nil (no --timeout for cr-tool; auto mode
// defaults to true). Illegal values that the Go types cannot express are not
// silently dropped: a negative timeout is written as-is and rejected by
// InitHandler, a non-integer timeout and a non-boolean automode are carried
// onto the CR via passthrough annotations for InitHandler to report.
func buildCheckpointSpec(pod *corev1.Pod) (*v1beta1.CheckpointSpec, map[string]string, []string) {
	warnings := make([]string, 0)
	passthrough := make(map[string]string)
	spec := &v1beta1.CheckpointSpec{
		SourcePodName:      pod.Name,
		SourcePodNamespace: pod.Namespace,
		SourcePodUID:       string(pod.UID),
		Containers:         getCheckpointContainers(pod),
	}

	// Timeout: only a declared value lands on the spec (0 included); undeclared
	// leaves the field nil and cr-tool runs without --timeout. An explicit 0
	// blocks the checkpoint stage on the auto-created CR (InitHandler).
	if raw, ok := trimmedAnnotation(pod.Annotations, labels.AnnotationCheckpointTimeoutMinutes); ok {
		timeout, err := strconv.Atoi(raw)
		if err != nil {
			// A non-integer cannot be expressed in the int field; carry the raw
			// value onto the CR for InitHandler to report.
			passthrough[labels.AnnotationAutoCheckpointInvalidTimeout] = raw
			warnings = append(warnings, fmt.Sprintf(
				"annotation %s must be a non-negative integer, got %q; the error is reported on the CR status",
				labels.AnnotationCheckpointTimeoutMinutes, raw))
		} else {
			if timeout < 0 {
				warnings = append(warnings, fmt.Sprintf(
					"annotation %s must be a non-negative integer, got %q; the error is reported on the CR status",
					labels.AnnotationCheckpointTimeoutMinutes, raw))
			}
			// Negative values are written as-is and rejected by InitHandler.
			spec.TimeoutInMinute = &timeout
		}
	}

	// Auto mode: nil (undeclared) defaults to true on the controller side; only
	// true / false land on the spec.
	if raw, ok := trimmedAnnotation(pod.Annotations, labels.AnnotationCheckpointAutoModeEnable); ok {
		switch raw {
		case "true", "false":
			autoMode := raw == "true"
			spec.EnableCheckpointAutoMode = &autoMode
		default:
			passthrough[labels.AnnotationAutoCheckpointInvalidAutoMode] = raw
			warnings = append(warnings, fmt.Sprintf(
				"annotation %s must be true or false, got %q; the error is reported on the CR status",
				labels.AnnotationCheckpointAutoModeEnable, raw))
		}
	}

	// Per-container policy: empty when undeclared or the JSON is illegal.
	if raw, ok := trimmedAnnotation(pod.Annotations, labels.AnnotationCheckpointPolicy); ok {
		policy := map[string]*v1beta1.CheckpointPolicy{}
		if err := json.Unmarshal([]byte(raw), &policy); err != nil {
			warnings = append(warnings, fmt.Sprintf(
				"annotation %s is not a valid JSON object (%v), ignored",
				labels.AnnotationCheckpointPolicy, err))
		} else {
			spec.ContainerCheckpointPolicy = policy
		}
	}

	return spec, passthrough, warnings
}

// buildRestoreSpec assembles spec.restore from the restore annotations.
//
// restore-replicas is the trigger: only a positive integer produces a restore
// stage; 0 / undeclared / illegal values keep the CR checkpoint-only (illegal
// values are reported as warnings - 0 and undeclared are the documented
// no-restore case). restore-retry-count and
// restore-disable-cold-start-fallback are only read when the restore stage is
// triggered: an illegal value falls back to the CRD default with a warning.
func buildRestoreSpec(pod *corev1.Pod) (*v1beta1.RestoreSpec, []string) {
	raw, ok := trimmedAnnotation(pod.Annotations, labels.AnnotationRestoreReplicas)
	if !ok {
		return nil, nil
	}
	replicas, err := strconv.Atoi(raw)
	if err != nil || replicas < 0 {
		return nil, []string{fmt.Sprintf(
			"annotation %s must be a positive integer to trigger restore, got %q, restore stage not created",
			labels.AnnotationRestoreReplicas, raw)}
	}
	if replicas == 0 {
		return nil, nil
	}

	restore := &v1beta1.RestoreSpec{Replicas: replicas}
	warnings := make([]string, 0)
	if retryRaw, ok := trimmedAnnotation(pod.Annotations, labels.AnnotationRestoreRetryCount); ok {
		retryCount, err := strconv.Atoi(retryRaw)
		if err != nil || retryCount < 0 {
			warnings = append(warnings, fmt.Sprintf(
				"annotation %s must be a non-negative integer, got %q, fell back to no retry",
				labels.AnnotationRestoreRetryCount, retryRaw))
		} else {
			restore.RetryCount = retryCount
		}
	}
	if raw, ok := trimmedAnnotation(pod.Annotations, labels.AnnotationRestoreDisableColdStartFallback); ok {
		switch raw {
		case "true":
			restore.DisableColdStartFallback = true
		case "false":
			// The fallback stays on (the bool zero value), nothing to set.
		default:
			warnings = append(warnings, fmt.Sprintf(
				"annotation %s must be true or false, got %q, fell back to keeping the fallback on",
				labels.AnnotationRestoreDisableColdStartFallback, raw))
		}
	}
	return restore, warnings
}

// buildStorage best-effort assembles spec.storageType and spec.storage from
// the storage-type annotation.
//
// storageType is passed through as-is (possibly empty or an unknown value) and
// missing required fields are all left empty for InitHandler to report. The
// returned storage is guaranteed non-nil: spec.storage in the CRD is required
// with type=object, and writing null would be rejected by the apiserver so the
// CR could not land.
func buildStorage(annotations map[string]string) (string, *v1beta1.JobStorage, []string) {
	storageType, _ := trimmedAnnotation(annotations, labels.AnnotationStorageType)

	switch storageType {
	case v1beta1.StorageTypeLocal:
		path, _ := trimmedAnnotation(annotations, labels.AnnotationStorageLocalPath)
		return storageType, &v1beta1.JobStorage{
			Local: &v1beta1.JobLocal{Path: path},
		}, nil

	case v1beta1.StorageTypeNFS:
		server, _ := trimmedAnnotation(annotations, labels.AnnotationStorageNFSServer)
		path, _ := trimmedAnnotation(annotations, labels.AnnotationStorageNFSPath)
		options, warnings := parseKVAnnotation(annotations, labels.AnnotationStorageNFSOptions)
		return storageType, &v1beta1.JobStorage{
			NFS: &v1beta1.JobNFS{Server: server, Path: path, Options: options},
		}, warnings

	case v1beta1.StorageTypeOSS:
		endpoint, _ := trimmedAnnotation(annotations, labels.AnnotationStorageOSSEndpoint)
		mountPoint, _ := trimmedAnnotation(annotations, labels.AnnotationStorageOSSMountPoint)
		options, warnings := parseKVAnnotation(annotations, labels.AnnotationStorageOSSOptions)
		return storageType, &v1beta1.JobStorage{
			OSS: &v1beta1.JobOSS{Endpoint: endpoint, MountPoint: mountPoint, Options: options},
		}, warnings

	case v1beta1.StorageTypeDragonfly:
		endpoint, _ := trimmedAnnotation(annotations, labels.AnnotationStorageDragonflyOSSEndpoint)
		mountPoint, _ := trimmedAnnotation(annotations, labels.AnnotationStorageDragonflyOSSMountPoint)
		options, warnings := parseKVAnnotation(annotations, labels.AnnotationStorageDragonflyOptions)
		// Endpoint is not declared in the protocol; left empty so the worker side uses its default dfdaemon address.
		dragonfly := &v1beta1.JobDragonfly{
			OSSEndpoint:   endpoint,
			OSSMountPoint: mountPoint,
			Options:       options,
		}
		return storageType, &v1beta1.JobStorage{Dragonfly: dragonfly}, warnings

	case v1beta1.StorageTypeEFC:
		bucketName, _ := trimmedAnnotation(annotations, labels.AnnotationStorageEFCBucketName)
		options, warnings := parseKVAnnotation(annotations, labels.AnnotationStorageEFCOptions)
		// No data-location fields other than the bucket: mounting and origin fetching are entirely handled by the node-side runtimekit.
		efc := &v1beta1.JobEFC{BucketName: bucketName, Options: options}
		return storageType, &v1beta1.JobStorage{EFC: efc}, warnings

	case v1beta1.StorageTypePVC:
		// PVC direct-attach only declares pvcName / pvcNamespace (preheat
		// coordinates are not in the pod watch protocol; preheat is created on
		// demand by a separate PreheatJob CR after the checkpoint completes and
		// referenced in the restore stage).
		pvcName, _ := trimmedAnnotation(annotations, labels.AnnotationStoragePVCName)
		pvcNamespace, _ := trimmedAnnotation(annotations, labels.AnnotationStoragePVCNamespace)
		return storageType, &v1beta1.JobStorage{
			PVC: &v1beta1.JobPVC{PvcName: pvcName, PvcNamespace: pvcNamespace},
		}, nil

	default:
		// storageType empty or unknown: no backend can be filled; give only an
		// empty object to satisfy the CRD's required + type=object, and
		// InitHandler reports unsupported storageType.
		return storageType, &v1beta1.JobStorage{}, nil
	}
}

// parseKVAnnotation parses a "k1=v1,k2=v2" style annotation; nil when
// undeclared.
//
// Malformed sections are only dropped and recorded as warnings, not blocking
// CR creation: options is a map in the CR, a malformed string cannot land
// as-is and can only be told to the user via an Event. An empty map caused by
// missing or fully malformed input is reported by InitHandler as a
// required-field error per storage type.
func parseKVAnnotation(annotations map[string]string, key string) (map[string]string, []string) {
	raw, ok := trimmedAnnotation(annotations, key)
	if !ok {
		return nil, nil
	}

	options := map[string]string{}
	warnings := make([]string, 0)
	for _, pair := range splitAndTrim(raw) {
		name, value, found := strings.Cut(pair, "=")
		name = strings.TrimSpace(name)
		if !found || name == "" {
			warnings = append(warnings, fmt.Sprintf(
				"annotation %s expects comma separated k=v pairs, dropped invalid item %q", key, pair))
			continue
		}
		options[name] = strings.TrimSpace(value)
	}
	if len(options) == 0 {
		return nil, warnings
	}
	return options, warnings
}

// trimmedAnnotation returns the whitespace-trimmed annotation value; empty means undeclared.
func trimmedAnnotation(annotations map[string]string, key string) (string, bool) {
	if len(annotations) == 0 {
		return "", false
	}
	value := strings.TrimSpace(annotations[key])
	if value == "" {
		return "", false
	}
	return value, true
}

// splitAndTrim splits by comma and drops empty sections.
func splitAndTrim(raw string) []string {
	items := make([]string, 0)
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item != "" {
			items = append(items, item)
		}
	}
	return items
}
