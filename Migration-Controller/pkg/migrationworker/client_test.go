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

package migrationworker

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
)

// invokeWithInterceptor runs the interceptor once and returns the ctx the invoker actually saw.
func invokeWithInterceptor(t *testing.T, ctx context.Context, timeout time.Duration) context.Context {
	t.Helper()

	var seen context.Context
	err := deadlineInterceptor(timeout)(ctx, "/migration.Service/Method", nil, nil, nil,
		func(inner context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
			seen = inner
			return nil
		})
	if err != nil {
		t.Fatalf("interceptor returned error: %v", err)
	}
	if seen == nil {
		t.Fatal("interceptor did not call the invoker")
	}
	return seen
}

// Calls without a deadline must be given the fallback timeout - that is the
// whole point of the fix: reconcile uses a bare ctx, and a worker that is
// TCP-reachable but not responding would block the RPC forever. 6 of the
// current 7 call sites fall into this case.
func TestDeadlineInterceptor_AddsDeadlineWhenAbsent(t *testing.T) {
	const timeout = 2 * time.Minute

	seen := invokeWithInterceptor(t, context.Background(), timeout)

	deadline, ok := seen.Deadline()
	if !ok {
		t.Fatal("expected the interceptor to attach a fallback deadline")
	}
	remaining := time.Until(deadline)
	if remaining <= 0 || remaining > timeout {
		t.Fatalf("fallback deadline out of range: remaining=%v, want (0, %v]", remaining, timeout)
	}
}

// A ctx marked with WithNoRPCTimeout must pass through unchanged, even
// without a deadline: long-running calls (RestoreXPUOnNode) are bounded by
// the remote side, and the fallback timeout must not cut them short.
func TestDeadlineInterceptor_NoRPCTimeoutMarkPassesThrough(t *testing.T) {
	seen := invokeWithInterceptor(t, WithNoRPCTimeout(context.Background()), 2*time.Minute)

	if _, ok := seen.Deadline(); ok {
		t.Fatal("expected no deadline on a WithNoRPCTimeout-marked ctx")
	}
}

// A caller-provided deadline must never be overridden - RestoreXPUOnNode is let through exactly by this.
//
// Its deadline is computed as "container count x 10min + 5min", i.e. 15
// minutes for a single container, deliberately larger than the worker->XDM
// inner timeout. If squeezed by the 2min fallback, the controller would hang
// up before the worker while cr-tool is still running inside XDM; the
// following DeadlineExceeded would then be judged transient by isTransientError
// and trigger a resend - running cr-tool a second time on the same
// process.
func TestDeadlineInterceptor_NeverOverridesLongerCallerDeadline(t *testing.T) {
	callerCtx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	want, ok := callerCtx.Deadline()
	if !ok {
		t.Fatal("caller ctx should carry a deadline")
	}

	seen := invokeWithInterceptor(t, callerCtx, 2*time.Minute)

	got, ok := seen.Deadline()
	if !ok {
		t.Fatal("caller deadline was dropped")
	}
	if !got.Equal(want) {
		t.Fatalf("interceptor overrode the caller deadline: got %v, want %v", got, want)
	}
}

// A caller deadline shorter than the fallback is likewise let through
// unchanged, without extension: the interceptor only fills the gap, never
// adjusts any caller decision.
func TestDeadlineInterceptor_KeepsShorterCallerDeadline(t *testing.T) {
	callerCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	want, _ := callerCtx.Deadline()
	seen := invokeWithInterceptor(t, callerCtx, 2*time.Minute)

	got, ok := seen.Deadline()
	if !ok {
		t.Fatal("caller deadline was dropped")
	}
	if !got.Equal(want) {
		t.Fatalf("interceptor extended the caller deadline: got %v, want %v", got, want)
	}
}

// The ctx filled by the interceptor must be cancelled after the invoker returns; no timer leak.
func TestDeadlineInterceptor_CancelsAfterInvoke(t *testing.T) {
	seen := invokeWithInterceptor(t, context.Background(), 2*time.Minute)

	select {
	case <-seen.Done():
	case <-time.After(time.Second):
		t.Fatal("interceptor leaked the derived context (not cancelled after invoke)")
	}
}

// The interceptor must pass the invoker error through unchanged, never swallowing it.
func TestDeadlineInterceptor_PropagatesInvokerError(t *testing.T) {
	wantErr := context.Canceled
	err := deadlineInterceptor(time.Minute)(context.Background(), "/m", nil, nil, nil,
		func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			return wantErr
		})
	if err != wantErr {
		t.Fatalf("invoker error not propagated: got %v, want %v", err, wantErr)
	}
}

// The fallback timeout treats all methods alike: the criterion is whether the ctx has a deadline, not the method name.
func TestDeadlineInterceptor_AppliesToEveryMethod(t *testing.T) {
	methods := []string{
		"CheckpointToStorage",
		"QueryCheckpointToStorageStatus",
		"RestoreFromStorage",
		"QueryRestoreFromStorageStatus",
		"CleanTask",
		"RestoreXPUOnNode",
	}
	for _, m := range methods {
		t.Run(m, func(t *testing.T) {
			seen := invokeWithInterceptor(t, context.Background(), 2*time.Minute)
			if _, ok := seen.Deadline(); !ok {
				t.Fatalf("%s should have received the fallback deadline when called with a bare ctx", m)
			}
		})
	}
}

// rpcTimeout <= 0 means no fallback: revert to the old behavior of "entirely decided by the caller ctx", and the client must still build normally.
func TestNewDefaultClient_TimeoutOptional(t *testing.T) {
	for name, timeout := range map[string]time.Duration{
		"disabled": 0,
		"negative": -time.Second,
		"enabled":  2 * time.Minute,
	} {
		t.Run(name, func(t *testing.T) {
			c, err := NewDefaultClient("127.0.0.1", 50051, timeout)
			if err != nil {
				t.Fatalf("NewDefaultClient(%v) failed: %v", timeout, err)
			}
			if c == nil {
				t.Fatal("NewDefaultClient returned a nil client")
			}
			defer c.Close()
		})
	}
}
