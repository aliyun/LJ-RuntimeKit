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

package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

const (
	// LabelKuberGPU is the label key to watch on nodes
	LabelKuberGPU = "lj_runtimekit_enable"
	// LabelValueTrue is the expected label value
	LabelValueTrue = "true"
	// JobActionEnable is the action to enable the feature
	JobActionEnable = "enable"
	// JobActionDisable is the action to disable the feature
	JobActionDisable = "disable"
	// JobLabelNode is the label on the job to identify the target node
	JobLabelNode = "lj-runtimekit-controller/target-node"
	// JobLabelAction is the label on the job to identify the action
	JobLabelAction = "lj-runtimekit-controller/action"
	// LastActionAnnotation tracks the last action taken by the controller on a node
	LastActionAnnotation = "lj-runtimekit-controller/last-action"
	// LastActionStatusAnnotation tracks whether the last action completed successfully
	LastActionStatusAnnotation = "lj-runtimekit-controller/last-action-status"
	// LastActionStatusPending means the action has been initiated but not yet completed
	LastActionStatusPending = "pending"
	// LastActionStatusSuccess means the action completed successfully
	LastActionStatusSuccess = "success"
	// jobNamePrefix is the prefix of the agent job names created per node
	jobNamePrefix = "lj-agent-"
	// maxJobNameLen caps job names so that pod names generated from
	// "<job-name>-" (the API server truncates the generateName base to 58
	// chars before appending the random suffix) never lose the action suffix.
	maxJobNameLen = 57
)

// NodeReconciler reconciles a Node object
type NodeReconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	Recorder     record.EventRecorder
	JobNamespace string
	JobImage     string
}

// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile handles node events and ensures a Job is created to enable or disable
// the feature based on the lj_runtimekit_enable label. It uses node annotations to track
// the last action and its completion status so that operations are idempotent
// and recover correctly after controller restarts.
func (r *NodeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the Node instance
	node := &corev1.Node{}
	if err := r.Get(ctx, req.NamespacedName, node); err != nil {
		if errors.IsNotFound(err) {
			// Node was deleted, nothing to do
			logger.Info("Node not found, probably deleted", "node", req.Name)
			return ctrl.Result{}, nil
		}
		logger.Error(err, "unable to fetch Node")
		return ctrl.Result{}, err
	}

	nodeName := node.Name

	// Determine the desired action based on the current label
	labelValue, hasLabel := node.Labels[LabelKuberGPU]
	shouldEnable := hasLabel && labelValue == LabelValueTrue

	desiredAction := JobActionDisable
	if shouldEnable {
		desiredAction = JobActionEnable
	}

	// Read the last action from annotation
	lastAction := node.Annotations[LastActionAnnotation]
	lastStatus := node.Annotations[LastActionStatusAnnotation]

	logger.Info("Processing node",
		"node", nodeName,
		"desiredAction", desiredAction,
		"lastAction", lastAction,
		"lastStatus", lastStatus,
		"labelValue", labelValue,
		"hasLabel", hasLabel)

	// Determine if we need to (re-)create the desired job.
	// - enable: only when the node has never been enabled (last!="enable").
	// - disable: only when the node was previously enabled (last=="enable").
	// - In addition, if the desired state matches last but the job has not yet
	//   completed (status!="success"), ensure the desired job exists. This handles
	//   controller crashes after annotation update but before job creation.
	isStateTransition := false
	switch desiredAction {
	case JobActionEnable:
		isStateTransition = lastAction != JobActionEnable
	case JobActionDisable:
		isStateTransition = lastAction == JobActionEnable
	}

	needDesiredJob := isStateTransition || (lastAction == desiredAction && lastStatus != LastActionStatusSuccess)

	if !needDesiredJob {
		logger.Info("Node is fully reconciled, skipping",
			"node", nodeName,
			"desiredAction", desiredAction,
			"lastAction", lastAction,
			"lastStatus", lastStatus)
		return ctrl.Result{}, nil
	}

	// Always remove any job for the opposite action to avoid conflicts.
	oppositeAction := JobActionDisable
	if desiredAction == JobActionDisable {
		oppositeAction = JobActionEnable
	}
	oppositeJobName := buildJobName(nodeName, oppositeAction)
	oppositeJob := &batchv1.Job{}
	oppositeJobKey := client.ObjectKey{Namespace: r.JobNamespace, Name: oppositeJobName}

	if err := r.Get(ctx, oppositeJobKey, oppositeJob); err == nil {
		logger.Info("Deleting opposite action job", "job", oppositeJobName, "node", nodeName, "action", oppositeAction)
		propagation := metav1.DeletePropagationBackground
		if err := r.Delete(ctx, oppositeJob, &client.DeleteOptions{
			PropagationPolicy: &propagation,
		}); err != nil && !errors.IsNotFound(err) {
			logger.Error(err, "failed to delete opposite job", "job", oppositeJobName)
			return ctrl.Result{}, err
		}
		logger.Info("Deleted opposite job", "job", oppositeJobName, "node", nodeName)
	} else if !errors.IsNotFound(err) {
		logger.Error(err, "failed to check opposite job", "job", oppositeJobName)
		return ctrl.Result{}, err
	}

	// Ensure the desired action job exists.
	jobName := buildJobName(nodeName, desiredAction)
	desiredJob := &batchv1.Job{}
	jobKey := client.ObjectKey{Namespace: r.JobNamespace, Name: jobName}

	if err := r.Get(ctx, jobKey, desiredJob); err != nil {
		if !errors.IsNotFound(err) {
			logger.Error(err, "failed to check desired job", "job", jobName)
			return ctrl.Result{}, err
		}

		job := r.buildJob(jobName, nodeName, desiredAction)
		if err := r.Create(ctx, job); err != nil {
			if errors.IsAlreadyExists(err) {
				logger.Info("Job already exists", "job", jobName)
			} else {
				logger.Error(err, "failed to create job", "job", jobName, "node", nodeName)
				r.Recorder.Eventf(node, corev1.EventTypeWarning, "JobCreationFailed",
					"Failed to create job %s for node %s: %v", jobName, nodeName, err)
				return ctrl.Result{}, err
			}
		} else {
			logger.Info("Created job", "job", jobName, "node", nodeName, "action", desiredAction)
			r.Recorder.Eventf(node, corev1.EventTypeNormal, "JobCreated",
				"Created job %s for node %s with action %s", jobName, nodeName, desiredAction)
		}
	} else {
		logger.Info("Desired job already exists, skipping creation",
			"job", jobName, "node", nodeName, "action", desiredAction)
	}

	// Record the desired action and mark it as pending.
	// JobCleanupReconciler will update the status to success once the job completes.
	if node.Annotations == nil {
		node.Annotations = make(map[string]string)
	}
	node.Annotations[LastActionAnnotation] = desiredAction
	node.Annotations[LastActionStatusAnnotation] = LastActionStatusPending
	if err := r.Update(ctx, node); err != nil {
		logger.Error(err, "failed to update node annotation", "node", nodeName)
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// buildJobName returns "<prefix><node>-<action>". When the node name is too
// long it is truncated and a short hash of the full node name is appended so
// that names stay unique and the trailing action suffix is always preserved
// in both the job name and the pod names generated from it.
func buildJobName(nodeName, action string) string {
	name := fmt.Sprintf("%s%s-%s", jobNamePrefix, nodeName, action)
	if len(name) <= maxJobNameLen {
		return name
	}
	sum := sha256.Sum256([]byte(nodeName))
	// 40-bit hash keeps the collision probability negligible (~4.5e-5)
	// even for clusters with 10k nodes sharing the same truncated prefix.
	hash := hex.EncodeToString(sum[:])[:10]
	// budget for the node part, keeping room for "-<hash>" and "-<action>"
	budget := maxJobNameLen - len(jobNamePrefix) - len(hash) - len(action) - 2
	trunc := strings.TrimRight(nodeName[:budget], "-.")
	return fmt.Sprintf("%s%s-%s-%s", jobNamePrefix, trunc, hash, action)
}

// buildJob creates a Job spec that runs on the target node
func (r *NodeReconciler) buildJob(jobName, nodeName, action string) *batchv1.Job {
	backoffLimit := int32(math.MaxInt32) // Retry indefinitely until success
	hostPathType := corev1.HostPathDirectoryOrCreate
	socketPath := "/run/amp/eKitC/xdm/sockets"

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: r.JobNamespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "lj-runtimekit-job-agent",
				"app.kubernetes.io/managed-by": "lj-runtimekit-controller",
				JobLabelNode:                   nodeName,
				JobLabelAction:                 action,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app.kubernetes.io/name":       "lj-runtimekit-job-agent",
						"app.kubernetes.io/managed-by": "lj-runtimekit-controller",
						JobLabelNode:                   nodeName,
						JobLabelAction:                 action,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyOnFailure,
					// Schedule the pod on the target node
					NodeName: nodeName,
					// Use hostNetwork to access the node's socket
					HostNetwork: true,
					// Run as root to access root-owned Unix sockets on the host
					SecurityContext: &corev1.PodSecurityContext{
						RunAsUser:  ptr.To(int64(0)),
						RunAsGroup: ptr.To(int64(0)),
					},
					Containers: []corev1.Container{
						{
							Name:  "job-agent",
							Image: r.JobImage,
							Args:  []string{"--action", action},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "socket-dir",
									MountPath: socketPath,
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "socket-dir",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: socketPath,
									Type: &hostPathType,
								},
							},
						},
					},
				},
			},
		},
	}
}

// isLabelMeaningfulChanged returns true when the lj_runtimekit_enable label is added, removed, or its value changed.
func isLabelMeaningfulChanged(oldLabels, newLabels map[string]string) bool {
	oldValue, oldExists := oldLabels[LabelKuberGPU]
	newValue, newExists := newLabels[LabelKuberGPU]

	// label added or removed
	if oldExists != newExists {
		return true
	}

	// both exist, compare values
	if oldExists && newExists && oldValue != newValue {
		return true
	}

	return false
}

// LabelChangedPredicate filters node events so Reconcile only runs when
// the lj_runtimekit_enable label is added, removed, or its value changes.
type LabelChangedPredicate struct {
	predicate.Funcs
}

// Update returns true only when the target label changed meaningfully.
func (LabelChangedPredicate) Update(e event.UpdateEvent) bool {
	oldNode, ok1 := e.ObjectOld.(*corev1.Node)
	newNode, ok2 := e.ObjectNew.(*corev1.Node)
	if !ok1 || !ok2 {
		return false
	}
	return isLabelMeaningfulChanged(oldNode.Labels, newNode.Labels)
}

// Create returns true for new nodes so the controller can reconcile initial state.
func (LabelChangedPredicate) Create(e event.CreateEvent) bool {
	return true
}

// Delete returns false; no action is needed when a node is deleted.
func (LabelChangedPredicate) Delete(e event.DeleteEvent) bool {
	return false
}

// Generic returns false.
func (LabelChangedPredicate) Generic(e event.GenericEvent) bool {
	return false
}

// SetupWithManager sets up the controller with the Manager.
func (r *NodeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Node{}).
		WithEventFilter(LabelChangedPredicate{}).
		Complete(r)
}
