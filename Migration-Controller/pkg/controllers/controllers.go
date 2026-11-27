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

package controllers

import (
	"context"
	"errors"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Result represents the event processing result; do not initialize it directly, create it via the New functions.
type Result struct {
	done bool
	err  error

	abortWithRetryAfter time.Duration

	// skipped means this handler was skipped because its entry condition was
	// not met (no actual logic executed). Semantically identical to Continue
	// (keep running subsequent handlers), only used for log timeline display.
	skipped bool
	// message is the skip reason, only meaningful when skipped.
	message string
}

// NewContinueResult returns a continue result.
func NewContinueResult() Result {
	return Result{}
}

// NewSkipResult returns a skip result: the entry condition was not met and the
// current handler needs no execution. Behavior is identical to Continue (move
// to the next handler), only making it easy to mark the SKIP stage and its
// reason explicitly in the reconcile timeline log.
func NewSkipResult(reason string) Result {
	return Result{skipped: true, message: reason}
}

// NewDoneResult returns a done result; it should and must only be used after the event is fully consumed.
func NewDoneResult() Result {
	return Result{done: true}
}

// NewErrorResult returns an err result for the retry-on-error case; the current event is re-enqueued for the next processing.
func NewErrorResult(err error) Result {
	return Result{err: err}
}

// NewAbortWithRetryAfterResult returns a retry result for when the Reconciler
// logic must exit; the current event is re-enqueued for the next processing.
// Typically used in long event tasks, e.g. when multiple CRD updates are needed
// within one event processing; exit early to keep the CRD data consistent, then
// fetch the latest values on the next processing round.
func NewAbortWithRetryAfterResult(abortWithRetryAfter time.Duration) Result {
	return Result{abortWithRetryAfter: abortWithRetryAfter}
}

func (r Result) IsAbort() bool {
	return r.abortWithRetryAfter > 0
}

func (r Result) IsError() bool {
	return r.err != nil
}

func (r Result) IsDone() bool {
	return r.done
}

// IsSkip reports whether this handler was skipped because its entry condition was not met.
func (r Result) IsSkip() bool {
	return r.skipped
}

// Message returns the description attached to the result (currently used for skip reasons).
func (r Result) Message() string {
	return r.message
}

// RetryAfter returns the re-enqueue wait duration of an abort.
func (r Result) RetryAfter() time.Duration {
	return r.abortWithRetryAfter
}

// Err returns the error carried by an error result.
func (r Result) Err() error {
	return r.err
}

func (r Result) ToCtrlResult() (ctrl.Result, error) {
	if r.IsError() {
		return ctrl.Result{}, r.err
	}
	if r.IsAbort() {
		return ctrl.Result{RequeueAfter: r.abortWithRetryAfter}, nil
	}
	if r.IsDone() {
		return ctrl.Result{}, nil
	}
	return ctrl.Result{RequeueAfter: time.Second}, nil
}

func (r Result) Equal(a Result) bool {
	return r.done == a.done && r.abortWithRetryAfter == a.abortWithRetryAfter && errors.Is(r.err, a.err)
}

// NameGetter implements the ability to get a name.
type NameGetter interface {
	// GetName gets the name.
	GetName() string
}

// Handler does the concrete event processing.
type Handler[T client.Object] interface {
	NameGetter
	// Handle processes the event of the corresponding object.
	Handle(ctx context.Context, object T) Result
}
