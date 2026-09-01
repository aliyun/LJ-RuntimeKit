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
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// JobCleanupReconciler watches Jobs created by AMP-Controller and deletes
// them once they complete successfully. Failed Jobs are kept so that the
// OnFailure restart policy and BackoffLimit can retry the pod.
type JobCleanupReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;update;patch

// Reconcile handles Job status changes.
func (r *JobCleanupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	job := &batchv1.Job{}
	if err := r.Get(ctx, req.NamespacedName, job); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		logger.Error(err, "unable to fetch Job")
		return ctrl.Result{}, err
	}

	// Only handle Jobs managed by this controller
	if job.Labels["app.kubernetes.io/managed-by"] != "lj-runtimekit-controller" {
		return ctrl.Result{}, nil
	}

	// Delete the Job as soon as it succeeds.
	// Before deleting, mark the last action as successful on the target node so
	// NodeReconciler does not recreate an already-completed job after restart.
	if job.Status.Succeeded > 0 {
		nodeName := job.Labels[JobLabelNode]
		action := job.Labels[JobLabelAction]
		if nodeName != "" && action != "" {
			node := &corev1.Node{}
			if err := r.Get(ctx, client.ObjectKey{Name: nodeName}, node); err == nil {
				if node.Annotations[LastActionAnnotation] == action &&
					node.Annotations[LastActionStatusAnnotation] != LastActionStatusSuccess {
					if node.Annotations == nil {
						node.Annotations = make(map[string]string)
					}
					node.Annotations[LastActionStatusAnnotation] = LastActionStatusSuccess
					if err := r.Update(ctx, node); err != nil {
						logger.Error(err, "failed to mark last action as successful", "node", nodeName, "action", action)
						return ctrl.Result{}, err
					}
					logger.Info("Marked last action as successful", "node", nodeName, "action", action)
				}

				// Emit a Kubernetes event summarizing the successful action.
				if job.Status.CompletionTime != nil {
					duration := job.Status.CompletionTime.Time.Sub(job.CreationTimestamp.Time).Round(time.Second)
					r.Recorder.Eventf(node, corev1.EventTypeNormal, "ActionSucceeded",
						"AMP action %q succeeded on node %q (duration=%s, failures=%d, completedAt=%s)",
						action, nodeName, duration, job.Status.Failed, job.Status.CompletionTime.Time.Format(time.RFC3339))
				}
			} else if !errors.IsNotFound(err) {
				logger.Error(err, "failed to fetch node for job cleanup", "node", nodeName)
				return ctrl.Result{}, err
			}
		}

		logger.Info("Job succeeded, deleting", "job", job.Name, "namespace", job.Namespace)
		propagation := metav1.DeletePropagationBackground
		if err := r.Delete(ctx, job, &client.DeleteOptions{
			PropagationPolicy: &propagation,
		}); err != nil {
			if errors.IsNotFound(err) {
				return ctrl.Result{}, nil
			}
			logger.Error(err, "failed to delete successful job", "job", job.Name)
			return ctrl.Result{}, err
		}
		logger.Info("Successfully deleted job after completion", "job", job.Name)
		return ctrl.Result{}, nil
	}

	// Failed Jobs are left intact so operators can inspect logs.
	// The Job's RestartPolicy=OnFailure + BackoffLimit will retry the pod.
	if job.Status.Failed > 0 {
		logger.Info("Job has failed pods, leaving for retry/inspection",
			"job", job.Name,
			"failed", job.Status.Failed,
			"backoffLimit", *job.Spec.BackoffLimit)
	}

	return ctrl.Result{}, nil
}

// ManagedByAMPController returns true for objects labeled as managed by this controller.
type ManagedByAMPController struct {
	predicate.Funcs
}

// Create returns true for managed jobs.
func (ManagedByAMPController) Create(e event.CreateEvent) bool {
	return e.Object.GetLabels()["app.kubernetes.io/managed-by"] == "lj-runtimekit-controller"
}

// Update returns true for managed jobs.
func (ManagedByAMPController) Update(e event.UpdateEvent) bool {
	return e.ObjectNew.GetLabels()["app.kubernetes.io/managed-by"] == "lj-runtimekit-controller"
}

// Delete returns true for managed jobs.
func (ManagedByAMPController) Delete(e event.DeleteEvent) bool {
	return e.Object.GetLabels()["app.kubernetes.io/managed-by"] == "lj-runtimekit-controller"
}

// Generic returns true for managed jobs.
func (ManagedByAMPController) Generic(e event.GenericEvent) bool {
	return e.Object.GetLabels()["app.kubernetes.io/managed-by"] == "lj-runtimekit-controller"
}

// SetupWithManager sets up the Job cleanup controller with the Manager.
func (r *JobCleanupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1.Job{}).
		WithEventFilter(ManagedByAMPController{}).
		Complete(r)
}
