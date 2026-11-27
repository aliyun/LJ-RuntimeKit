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

// Package pod provides the Pod controller that implements the automatic
// checkpoint protocol.
//
// The controller watches all pods in the cluster; when a pod carries
// migration.io/container-state-checkpoint-enable="true" and all containers to
// checkpoint are Running and Ready, it automatically creates a
// ContainerStateMigrationJobRequest that runs the checkpoint stage and, when
// the pod declares migration.io/restore-replicas as a positive integer, the
// restore stage as well. The CR's storage backend and checkpoint / restore
// parameters are all read from that pod's annotations.
//
// A wrong annotation does not block CR creation: parsing here is best-effort,
// missing or illegal fields are left empty / passed through as-is, and field
// validity is judged by the MigrationJob controller's InitHandler and written
// back to the CR's status.message. This way users can always find the failure
// reason on the CR instead of relying on an Event that gets GCed.
//
// This controller only adds the ability to auto-create CRs; it does not
// change the processing flow of ContainerStateMigrationJobRequest itself.
package pod

import (
	"context"

	"github.com/container-migration/controller/api/v1beta1"
	"github.com/container-migration/controller/pkg/controllers"
	"github.com/container-migration/controller/pkg/labels"
	"github.com/container-migration/controller/pkg/util"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// Event reason
const (
	// reasonConfigDegraded: some annotations could not be parsed and were dropped as degraded (not blocking CR creation).
	reasonConfigDegraded = "AutoCheckpointConfigDegraded"
	// reasonCreated: CR created successfully.
	reasonCreated = "AutoCheckpointCreated"
)

// PodReconciler watches pods and creates CRs per the automatic checkpoint protocol.
type PodReconciler struct {
	scheme        *runtime.Scheme
	sharedContext *controllers.SharedContext
}

// NewPodReconciler creates a PodReconciler instance.
func NewPodReconciler(scheme *runtime.Scheme, sharedContext *controllers.SharedContext) *PodReconciler {
	return &PodReconciler{
		scheme:        scheme,
		sharedContext: sharedContext,
	}
}

// GetName returns the controller name.
func (r *PodReconciler) GetName() string {
	return "ContainerStateCheckpointPodController"
}

// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=migration.io,resources=containerstatemigrationjobrequests,verbs=get;list;watch;create

// Reconcile creates the checkpoint CR for pods meeting the conditions.
//
// Idempotency: the CR name is fixed as checkpoint-<podUID>; when it already
// exists, return directly without updating. This also means that after fixing a
// wrong annotation the pod must be deleted and recreated (a new podUID yields
// a new CR); no self-healing here - updating the spec in place would require
// judging whether the task was dispatched and resetting status, which is less
// clean than having one CR correspond to one attempt of one podUID.
//
// A wrong protocol annotation does not block creation: as long as the switch
// is "true" a CR is always written, and field errors are written back to the
// CR's status.message by InitHandler.
func (r *PodReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	ctxLog := log.FromContext(ctx)

	pod := &corev1.Pod{}
	err := r.sharedContext.Client.Get(ctx, req.NamespacedName, pod)
	if errors.IsNotFound(err) {
		return ctrl.Result{}, nil
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	// Second confirmation of the admission conditions: the pod may have changed after the event was enqueued.
	if !shouldHandlePod(pod) {
		return ctrl.Result{}, nil
	}

	// CR already exists: return idempotently, neither re-triggering nor updating (the CR lives in the source pod's own namespace).
	crName := checkpointCRName(pod.UID)
	existing := &v1beta1.ContainerStateMigrationJobRequest{}
	err = r.sharedContext.Client.Get(ctx,
		client.ObjectKey{Namespace: pod.Namespace, Name: crName}, existing)
	if err == nil {
		ctxLog.V(2).Info("Checkpoint CR already exists, skip", "cr", crName, "pod", req.NamespacedName)
		return ctrl.Result{}, nil
	}
	if !errors.IsNotFound(err) {
		return ctrl.Result{}, err
	}

	// Do not create the CR when the containers are not all Running and Ready; wait for later events.
	if !readyForCheckpoint(pod) {
		ctxLog.V(2).Info("Checkpoint containers not all running and ready, waiting", "pod", req.NamespacedName)
		return ctrl.Result{}, nil
	}

	// Annotation parsing does not intercept: warnings only contain degraded items that cannot land on CR fields.
	jobReq, warnings := buildJobRequest(pod)

	if err := r.sharedContext.Client.Create(ctx, jobReq); err != nil {
		if errors.IsAlreadyExists(err) {
			return ctrl.Result{}, nil
		}
		ctxLog.Error(err, "Failed to create checkpoint CR", "cr", crName, "pod", req.NamespacedName)
		return ctrl.Result{}, err
	}

	ctxLog.Info("Created checkpoint CR for pod", "cr", crName, "pod", req.NamespacedName,
		"storageType", jobReq.Spec.StorageType, "containers", jobReq.Spec.Checkpoint.Containers)
	r.sharedContext.EventRecorder.Eventf(pod, corev1.EventTypeNormal, reasonCreated,
		"Created checkpoint request %s/%s", pod.Namespace, crName)

	// Tell degraded items separately: these values were already dropped, the CR
	// does not show what the user originally wrote. The CR was created above;
	// later reconciles return directly at the "CR already exists" branch and
	// will not send it again.
	for _, warning := range warnings {
		ctxLog.Info("Auto checkpoint annotation degraded", "pod", req.NamespacedName, "warning", warning)
		r.sharedContext.EventRecorder.Eventf(pod, corev1.EventTypeWarning, reasonConfigDegraded, "%s", warning)
	}

	return ctrl.Result{}, nil
}

// shouldHandlePod judges whether the pod enters automatic checkpoint management, short-circuiting from low to high cost.
func shouldHandlePod(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	// Automatic checkpoint not enabled.
	if pod.Annotations[labels.AnnotationCheckpointEnable] != "true" {
		return false
	}
	// Being deleted.
	if pod.DeletionTimestamp != nil {
		return false
	}
	// Pod created by this project's restore; blocks the checkpoint -> restore -> checkpoint cascade.
	if pod.Annotations[labels.AnnotationRestoreCreated] == "true" {
		return false
	}
	// This project's own worker pod.
	if isMigrationWorkerPod(pod) {
		return false
	}
	// Not yet bound to a node.
	if pod.Spec.NodeName == "" {
		return false
	}
	return true
}

// readyForCheckpoint judges whether all containers to checkpoint are Running
// and Ready.
//
// All containers Running and Ready is a precondition of the checkpoint task
// (InitHandler would otherwise fail the task directly with no retry), so the
// verdict is moved ahead of CR creation and unready pods are not created yet.
//
// Only container names that actually exist in the pod are counted: a
// non-existent container name in the annotation is a config error; if it
// participated in the verdict it would never be ready, all events would be
// dropped by the predicate, Reconcile would never even be called, and the user
// would get neither a CR nor an Event - the error would be totally silent. A
// missing annotation is likewise let through and reported by InitHandler
// uniformly.
func readyForCheckpoint(pod *corev1.Pod) bool {
	declared := getCheckpointContainers(pod)
	if len(declared) == 0 {
		return true
	}

	podContainers := sets.NewString()
	for i := range pod.Spec.Containers {
		podContainers.Insert(pod.Spec.Containers[i].Name)
	}
	existing := make([]string, 0, len(declared))
	for _, name := range declared {
		if podContainers.Has(name) {
			existing = append(existing, name)
		}
	}
	if len(existing) == 0 {
		return true
	}
	return util.IsPodContainersRunning(pod, existing...)
}

// isMigrationWorkerPod judges whether this pod is this project's migration worker pod.
func isMigrationWorkerPod(pod *corev1.Pod) bool {
	component := pod.Labels[labels.LabelComponent]
	return component == labels.MigrationWorkerComponentName ||
		component == labels.MigrationWorkerComponentNameAlias
}

// SetupWithManager registers the pod controller with the Manager.
//
// The predicate only reduces enqueueing: pod events with the switch off or
// containers unready are dropped directly and never enter the workqueue, so
// Reconcile is not triggered by status updates of the many unrelated pods in
// the cluster. But For(&corev1.Pod{}) builds a full pod informer with no cache
// selector configured; all pods are still list/watch'ed and deserialized into
// the cache, so that memory and CPU overhead remains.
func (r *PodReconciler) SetupWithManager(mgr ctrl.Manager) error {
	accept := func(obj client.Object) bool {
		pod, ok := obj.(*corev1.Pod)
		if !ok {
			return false
		}
		return shouldHandlePod(pod) && readyForCheckpoint(pod)
	}

	return ctrl.NewControllerManagedBy(mgr).
		Named(r.GetName()).
		For(&corev1.Pod{}, builder.WithPredicates(
			predicate.Funcs{
				CreateFunc: func(createEvent event.CreateEvent) bool {
					return accept(createEvent.Object)
				},
				UpdateFunc: func(updateEvent event.UpdateEvent) bool {
					return accept(updateEvent.ObjectNew)
				},
				DeleteFunc: func(event.DeleteEvent) bool {
					return false
				},
				GenericFunc: func(event.GenericEvent) bool {
					return false
				},
			},
		)).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: r.sharedContext.Config.MaxConcurrentReconciles,
			RateLimiter:             workqueue.DefaultControllerRateLimiter(),
		}).
		Complete(r)
}
