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

package migrationjob

import (
	"context"
	"fmt"
	"time"

	"github.com/container-migration/controller/api/v1beta1"
	"github.com/container-migration/controller/pkg/controllers"
	"github.com/container-migration/controller/pkg/controllers/migrationjob/handler"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// MigrationJobReconciler reconciles a ContainerStateMigrationJobRequest object
type MigrationJobReconciler struct {
	scheme        *runtime.Scheme
	sharedContext *controllers.SharedContext
	handlers      []controllers.Handler[*v1beta1.ContainerStateMigrationJobRequest]
}

func NewMigrationJobReconciler(scheme *runtime.Scheme, sharedContext *controllers.SharedContext) *MigrationJobReconciler {
	r := &MigrationJobReconciler{
		scheme:        scheme,
		sharedContext: sharedContext,
	}
	r.registerHandlers()
	return r
}

func (r *MigrationJobReconciler) registerHandlers() {
	handlers := make([]controllers.Handler[*v1beta1.ContainerStateMigrationJobRequest], 0)
	handlers = append(handlers, handler.NewInitHandler(r.sharedContext))
	handlers = append(handlers, handler.NewRoundCheckHandler(r.sharedContext))
	handlers = append(handlers, handler.NewEnsureWorkerHandler(r.sharedContext))
	handlers = append(handlers, handler.NewCheckpointHandler(r.sharedContext))
	handlers = append(handlers, handler.NewQueryCheckpointHandler(r.sharedContext))
	handlers = append(handlers, handler.NewLookupCheckpointHandler(r.sharedContext))
	handlers = append(handlers, handler.NewCreatePodsHandler(r.sharedContext))
	handlers = append(handlers, handler.NewPrepareDataHandler(r.sharedContext))
	handlers = append(handlers, handler.NewReleaseInitHandler(r.sharedContext))
	handlers = append(handlers, handler.NewColdStartHandler(r.sharedContext))
	handlers = append(handlers, handler.NewWaitRestoreHandler(r.sharedContext))
	handlers = append(handlers, handler.NewRestoreXPUHandler(r.sharedContext))
	handlers = append(handlers, handler.NewCleanHandler(r.sharedContext))
	r.handlers = handlers
}

func (r *MigrationJobReconciler) GetName() string {
	return "ContainerStateMigrationJobController"
}

// +kubebuilder:rbac:groups=migration.io,resources=containerstatemigrationjobrequests,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=migration.io,resources=containerstatemigrationjobrequests/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=migration.io,resources=containerstatemigrationjobrequests/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop
func (r *MigrationJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	ctxLog := log.FromContext(ctx)

	jobReq := &v1beta1.ContainerStateMigrationJobRequest{}
	err := r.sharedContext.Client.Get(ctx, req.NamespacedName, jobReq)
	if errors.IsNotFound(err) {
		return ctrl.Result{}, nil
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	mutatedJobReq := jobReq.DeepCopy()

	// Deletion path: the normal handler chain must no longer advance the task; only clean up and allow the deletion through.
	if !jobReq.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, mutatedJobReq)
	}

	// Do not repeat req.NamespacedName in logs: the KV injected by
	// controller-runtime already carries the same three identity fields
	// (ContainerStateMigrationJobRequest / namespace / name).
	reconcileStart := time.Now()
	ctxLog.Info(fmt.Sprintf("=== Reconcile START phase=%q ===", jobReq.Status.Phase))

	var result controllers.Result
	defer func() {
		ctxLog.Info(fmt.Sprintf("=== Reconcile END phase=%q result=%s took=%s ===",
			mutatedJobReq.Status.Phase, handlerOutcome(result),
			time.Since(reconcileStart).Round(time.Millisecond)))
	}()

	for _, h := range r.handlers {
		hStart := time.Now()
		result = h.Handle(ctx, mutatedJobReq)
		dur := time.Since(hStart).Round(time.Millisecond)

		switch {
		case result.IsSkip():
			// Skipped stages get no log line at all: only handlers that actually executed appear in the log.
			//
			// Of the 12 stages per round, usually more than 10 are unrelated to
			// the current phase; printing each would take half the total lines, and
			// every line also carries controller-runtime's 250-char KV boilerplate -
			// real migration records would be pushed out of the log buffer by this
			// noise, and when a problem is reviewed afterwards the window no longer
			// holds the scene.
			//
			// This does not leave any round "empty between START/END":
			// RoundCheckHandler and CleanHandler split the phase space into two
			// complementary halves - in non-terminal phases RoundCheck always
			// returns Continue/Abort, in terminal phases Clean always returns
			// Done, so every round has at least one non-Skip stage leaving a
			// line.
			//
			// The reason deciding this round's requeue pace is not lost either:
			// it is carried by the last handler's result into the END line's
			// result=SKIP(reason).
		case result.IsError():
			ctxLog.Error(result.Err(), fmt.Sprintf("[%s] ERROR (took=%s)", h.GetName(), dur))
		default:
			// Only stages that actually did work take a line.
			ctxLog.Info(fmt.Sprintf("[%s] %s (took=%s)", h.GetName(), handlerOutcome(result), dur))
		}

		if result.IsAbort() || result.IsDone() {
			break
		}
		if result.IsError() {
			// The status may already record the failure reason; persist first, then return the error to retry.
			_ = r.updateStatusIfChanged(ctx, jobReq, mutatedJobReq)
			return result.ToCtrlResult()
		}
	}

	// Use apiequality.Semantic.DeepEqual to detect status changes, avoiding
	// redundant Status.Update from assigning equal JSON strings (timestamp
	// drift / map serialization order).
	//
	// A write failure must bubble up as an error: the whole chain's advancement
	// uses status as the cursor, silently dropping one update means losing one
	// state transition, and a Done result does not requeue - nothing later
	// corrects a terminal CR.
	if err := r.updateStatusIfChanged(ctx, jobReq, mutatedJobReq); err != nil {
		return ctrl.Result{}, err
	}
	return result.ToCtrlResult()
}

// updateStatusIfChanged writes back only when the status really changed.
func (r *MigrationJobReconciler) updateStatusIfChanged(ctx context.Context,
	old, mutated *v1beta1.ContainerStateMigrationJobRequest) error {
	if apiequality.Semantic.DeepEqual(mutated.Status, old.Status) {
		return nil
	}
	if err := r.sharedContext.Client.Status().Update(ctx, mutated); err != nil {
		log.FromContext(ctx).Error(err, "Failed to update CRD status")
		return err
	}
	return nil
}

// handleDeletion does the final cleanup and then removes the finalizer.
//
// Cleanup is best-effort: even when the target node is unreachable and the
// worker cannot be reached, the finalizer must still be removed, otherwise the
// CR stays in Terminating forever and blocks namespace deletion along with
// it.
func (r *MigrationJobReconciler) handleDeletion(ctx context.Context,
	jobReq *v1beta1.ContainerStateMigrationJobRequest) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(jobReq, handler.MigrationJobFinalizer) {
		return ctrl.Result{}, nil
	}

	handler.CleanupOnDelete(ctx, r.sharedContext, jobReq)

	controllerutil.RemoveFinalizer(jobReq, handler.MigrationJobFinalizer)
	if err := r.sharedContext.Client.Update(ctx, jobReq); err != nil {
		log.FromContext(ctx).Error(err, "Failed to remove finalizer")
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// handlerOutcome normalizes the Result returned by a handler into the result
// label of the timing log.
//
// SKIP carries the reason: now that per-handler SKIP lines are no longer
// printed, the last handler's skip reason is the only clue of "why this round
// ended this way and requeues at this pace".
func handlerOutcome(r controllers.Result) string {
	switch {
	case r.IsError():
		return "ERROR"
	case r.IsAbort():
		return fmt.Sprintf("ABORT(requeueAfter=%s)", r.RetryAfter())
	case r.IsDone():
		return "DONE"
	case r.IsSkip():
		if msg := r.Message(); msg != "" {
			return fmt.Sprintf("SKIP(%s)", msg)
		}
		return "SKIP"
	default:
		return "CONTINUE"
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *MigrationJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named(r.GetName()).
		For(&v1beta1.ContainerStateMigrationJobRequest{}).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: r.sharedContext.Config.MaxConcurrentReconciles,
			RateLimiter:             workqueue.DefaultControllerRateLimiter(),
		}).
		Complete(r)
}
