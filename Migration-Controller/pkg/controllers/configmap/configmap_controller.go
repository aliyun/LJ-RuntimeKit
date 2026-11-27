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

// Package configmap provides the ConfigMap controller that keeps the worker
// configuration cached in SharedContext up to date.
//
// It does exactly one thing: when the worker config ConfigMap (default
// kube-system/lj-runtimekit-migration-controller-config) changes, sync the worker
// pod template and gRPC port inside it into the SharedContext cache for later
// worker pod creation and gRPC connection setup.
//
// This controller does NOT manage worker pod lifecycles: workers are created
// on demand by EnsureWorkerHandler / PrepareDataHandler only when a migration
// task needs them. After a ConfigMap change, existing workers on nodes keep
// running with the old config until the next migration task needs a worker on
// that node; the drift detection at the entry (comparing the
// migration.io/worker-config-hash label on the worker pod with the current
// config fingerprint) finds the mismatch and only then deletes the old pod and
// rebuilds when the node is idle, before dispatching the task; when the node
// still has in-flight tasks it waits for them to drain, never interrupting an
// ongoing checkpoint / restore. Manual deletion of worker pods is therefore
// unnecessary.
//
// History note: there used to be a "candidate node" mechanism here - find
// nodes needing a worker by the label
// migration.io/container-state-migration-enable, and periodically delete
// workers "not in the candidate set" or "whose template changed". But nothing
// in this project ever wrote that label (automatic checkpoint uses the
// annotation migration.io/container-state-checkpoint-enable), so the candidate
// set was always empty and every worker was judged expired and deleted every
// sync period, killing ongoing checkpoints / restores midway. That mechanism
// was removed entirely; replacement is now driven on demand at the task entry,
// per node busy/idle state.
package configmap

import (
	"context"
	"fmt"

	"github.com/container-migration/controller/pkg/controllers"
	"github.com/container-migration/controller/pkg/labels"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// ConfigMapReconciler reconciles the migration worker ConfigMap.
//
// It watches ConfigMaps carrying the migration-specific label and refreshes the
// cached worker configuration whenever they change.
type ConfigMapReconciler struct {
	scheme        *runtime.Scheme
	sharedContext *controllers.SharedContext
}

// NewConfigMapReconciler creates a new ConfigMapReconciler instance.
//
// Parameters:
//   - scheme: Runtime scheme for type registration
//   - sharedContext: Shared context containing client and configuration
//
// Returns:
//   - A new ConfigMapReconciler instance
func NewConfigMapReconciler(scheme *runtime.Scheme, sharedContext *controllers.SharedContext) *ConfigMapReconciler {
	return &ConfigMapReconciler{
		scheme:        scheme,
		sharedContext: sharedContext,
	}
}

// GetName returns the name of this controller.
func (r *ConfigMapReconciler) GetName() string {
	return "ContainerStateMigrationConfigMapController"
}

//+kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch

// Reconcile refreshes the cached worker configuration from the ConfigMap.
//
// Only the one ConfigMap named in the config is processed; other ConfigMaps
// carrying the label are ignored directly. The trailing RequeueAfter is a
// fallback: even when watch events are lost, the cache is resynced once per
// WorkerSyncTime period.
//
// Parameters:
//   - ctx: Context for the operation
//   - req: Reconciliation request containing the ConfigMap name and namespace
//
// Returns:
//   - Result containing requeue information
//   - Error if any operation fails
func (r *ConfigMapReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	ctxLog := log.FromContext(ctx)
	ctxLog.V(2).Info(fmt.Sprintf("Starting to reconcile request %s", req.NamespacedName))
	defer ctxLog.V(2).Info(fmt.Sprintf("Finished to reconcile request %s", req.NamespacedName))

	configMap := &corev1.ConfigMap{}
	err := r.sharedContext.Client.Get(ctx, req.NamespacedName, configMap)
	if errors.IsNotFound(err) {
		return ctrl.Result{}, nil
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	if configMap.Name != r.sharedContext.Config.ConfigMapName {
		return ctrl.Result{}, nil
	}

	if err = r.sharedContext.SyncConfigMap(ctx); err != nil {
		// A sync failure does not return an error: the next RequeueAfter period
		// retries, and the cache still holds the previous usable config, not
		// affecting ongoing migration tasks.
		ctxLog.Error(err, "Failed to sync worker configmap")
		return ctrl.Result{RequeueAfter: r.sharedContext.Config.WorkerSyncTime}, nil
	}
	ctxLog.V(2).Info("Refreshed worker configuration from configmap", "configMap", configMap.Name)

	return ctrl.Result{RequeueAfter: r.sharedContext.Config.WorkerSyncTime}, nil
}

// SetupWithManager sets up the configmap controller with the Manager.
//
// It configures the controller to watch for ConfigMap events with the
// migration-specific label and applies predicates to filter events.
//
// Parameters:
//   - mgr: Controller manager instance
//
// Returns:
//   - Error if setup fails
func (r *ConfigMapReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named(r.GetName()).
		For(&corev1.ConfigMap{}, builder.WithPredicates(
			predicate.Funcs{
				CreateFunc: func(createEvent event.CreateEvent) bool {
					objLabels := createEvent.Object.GetLabels()
					val, ok := objLabels[labels.LabelContainerStateMigrationConfigMap]
					return ok && val == "true"
				},
				UpdateFunc: func(updateEvent event.UpdateEvent) bool {
					if updateEvent.ObjectNew.GetDeletionTimestamp() != nil {
						return false
					}
					objLabels := updateEvent.ObjectNew.GetLabels()
					val, ok := objLabels[labels.LabelContainerStateMigrationConfigMap]
					return ok && val == "true"
				},
				DeleteFunc: func(deleteEvent event.DeleteEvent) bool {
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
