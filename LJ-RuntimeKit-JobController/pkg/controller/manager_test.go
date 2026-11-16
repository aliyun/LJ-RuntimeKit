package controller

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// fakeController — test-only Interface implementation
// ---------------------------------------------------------------------------

// fakeController implements Interface with a caller-supplied runFunc and an
// explicit readyCh that the test controls.
type fakeController struct {
	name    string
	runFunc func(ctx context.Context) error
	readyCh chan struct{} // closed by the test or runFunc to signal Ready
}

func (f *fakeController) Name() string                  { return f.name }
func (f *fakeController) Run(ctx context.Context) error { return f.runFunc(ctx) }
func (f *fakeController) Ready() <-chan struct{}        { return f.readyCh }

// Verify that fakeController satisfies Interface at compile time.
var _ Interface = (*fakeController)(nil)

// ---------------------------------------------------------------------------
// Registry tests (unchanged)
// ---------------------------------------------------------------------------

func TestRegisterAndGet(t *testing.T) {
	resetRegistry()
	Register("test-ctrl", func(SharedClients) (Interface, error) {
		return &fakeController{name: "test-ctrl", readyCh: make(chan struct{})}, nil
	})
	f, ok := Get("test-ctrl")
	if !ok {
		t.Fatal("expected factory to be registered")
	}
	ctrl, err := f(SharedClients{})
	if err != nil {
		t.Fatalf("factory error: %v", err)
	}
	if ctrl.Name() != "test-ctrl" {
		t.Errorf("expected name test-ctrl, got %s", ctrl.Name())
	}
}

func TestGetUnknown(t *testing.T) {
	resetRegistry()
	_, ok := Get("no-such-controller")
	if ok {
		t.Error("expected false for unregistered controller")
	}
}

func TestRegisterDuplicatePanics(t *testing.T) {
	resetRegistry()
	Register("dup", func(SharedClients) (Interface, error) { return nil, nil })
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on duplicate registration")
		}
		if !strings.Contains(fmt.Sprint(r), "already registered") {
			t.Errorf("unexpected panic message: %v", r)
		}
	}()
	Register("dup", func(SharedClients) (Interface, error) { return nil, nil })
}

func TestKnownControllersSorted(t *testing.T) {
	resetRegistry()
	Register("beta", func(SharedClients) (Interface, error) { return nil, nil })
	Register("alpha", func(SharedClients) (Interface, error) { return nil, nil })
	names := KnownControllers()
	if len(names) != 2 || names[0] != "alpha" || names[1] != "beta" {
		t.Errorf("expected [alpha beta], got %v", names)
	}
}

// ---------------------------------------------------------------------------
// Manager tests — existing scenarios adapted to new phase model
// ---------------------------------------------------------------------------

func TestManagerRunMultipleControllers(t *testing.T) {
	var started atomic.Int32
	readyA := make(chan struct{})
	readyB := make(chan struct{})
	blocker := func(readyCh chan struct{}) func(ctx context.Context) error {
		return func(ctx context.Context) error {
			close(readyCh)
			started.Add(1)
			<-ctx.Done()
			return nil
		}
	}
	ctrls := []Interface{
		&fakeController{name: "a", readyCh: readyA, runFunc: blocker(readyA)},
		&fakeController{name: "b", readyCh: readyB, runFunc: blocker(readyB)},
	}
	mgr := NewManager(ctrls)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- mgr.Run(ctx) }()

	// Wait for both controllers to start and become ready.
	waitForPhase(t, mgr, PhaseRunning, 2*time.Second)

	if started.Load() < 2 {
		t.Fatal("controllers did not start in time")
	}
	if !mgr.IsHealthy() {
		t.Error("expected healthy after start")
	}

	cancel()
	if err := <-done; err != nil {
		t.Errorf("expected nil error on graceful shutdown, got %v", err)
	}
	if mgr.Phase() != PhaseStopped {
		t.Errorf("expected PhaseStopped, got %s", mgr.Phase())
	}
}

func TestManagerRunFirstErrorCancelsAll(t *testing.T) {
	var bCancelled atomic.Bool
	readyFail := make(chan struct{})
	readyWait := make(chan struct{})
	ctrls := []Interface{
		&fakeController{name: "fail", readyCh: readyFail, runFunc: func(ctx context.Context) error {
			close(readyFail)
			return fmt.Errorf("boom")
		}},
		&fakeController{name: "wait", readyCh: readyWait, runFunc: func(ctx context.Context) error {
			close(readyWait)
			<-ctx.Done()
			bCancelled.Store(true)
			return nil
		}},
	}
	mgr := NewManager(ctrls)
	err := mgr.Run(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Errorf("expected 'boom' in error, got %v", err)
	}
	if mgr.Phase() != PhaseFailed {
		t.Errorf("expected PhaseFailed, got %s", mgr.Phase())
	}
	time.Sleep(50 * time.Millisecond)
	if !bCancelled.Load() {
		t.Error("expected 'wait' controller to be cancelled when sibling fails")
	}
}

func TestManagerRunNoControllers(t *testing.T) {
	mgr := NewManager(nil)
	err := mgr.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "no controllers") {
		t.Errorf("expected 'no controllers' error, got %v", err)
	}
	if mgr.Phase() != PhaseFailed {
		t.Errorf("expected PhaseFailed, got %s", mgr.Phase())
	}
}

func TestManagerEarlyNilReturnIsError(t *testing.T) {
	readyCh := make(chan struct{})
	mgr := NewManager([]Interface{
		&fakeController{name: "early-exit", readyCh: readyCh, runFunc: func(ctx context.Context) error {
			close(readyCh)
			return nil // unexpected early exit
		}},
	})
	err := mgr.Run(context.Background())
	if err == nil {
		t.Fatal("expected error for early nil return")
	}
	if !strings.Contains(err.Error(), "unexpected early exit") {
		t.Errorf("expected 'unexpected early exit' in error, got: %v", err)
	}
	if mgr.Phase() != PhaseFailed {
		t.Errorf("expected PhaseFailed, got %s", mgr.Phase())
	}
}

func TestManagerEarlyExitCancelsSiblings(t *testing.T) {
	var siblingCancelled atomic.Bool
	readyA := make(chan struct{})
	readyB := make(chan struct{})
	ctrls := []Interface{
		&fakeController{name: "early-exit", readyCh: readyA, runFunc: func(ctx context.Context) error {
			close(readyA)
			return nil
		}},
		&fakeController{name: "sibling", readyCh: readyB, runFunc: func(ctx context.Context) error {
			close(readyB)
			<-ctx.Done()
			siblingCancelled.Store(true)
			return nil
		}},
	}
	mgr := NewManager(ctrls)
	err := mgr.Run(context.Background())
	if err == nil {
		t.Fatal("expected error from early exit")
	}
	if !siblingCancelled.Load() {
		t.Error("expected sibling controller to be cancelled")
	}
	if mgr.Phase() != PhaseFailed {
		t.Errorf("expected PhaseFailed, got %s", mgr.Phase())
	}
}

// ---------------------------------------------------------------------------
// New tests for task-45 phase model
// ---------------------------------------------------------------------------

// TestManagerStartingDuringInit verifies that the Manager stays in
// PhaseStarting while controllers have not yet signalled Ready.
func TestManagerStartingDuringInit(t *testing.T) {
	readyCh := make(chan struct{}) // never closed until we say so
	ctrls := []Interface{
		&fakeController{name: "slow-init", readyCh: readyCh, runFunc: func(ctx context.Context) error {
			// readyCh will be closed by the test, not by runFunc
			<-ctx.Done()
			return nil
		}},
	}

	mgr := NewManager(ctrls)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- mgr.Run(ctx) }()

	// Must reach PhaseStarting but NOT PhaseRunning.
	waitForPhase(t, mgr, PhaseStarting, 2*time.Second)
	// Confirm it's still Starting (not Running).
	time.Sleep(50 * time.Millisecond)
	if mgr.Phase() != PhaseStarting {
		t.Errorf("expected PhaseStarting while init blocked, got %s", mgr.Phase())
	}
	if mgr.IsHealthy() {
		t.Error("expected not healthy during Starting")
	}
	if mgr.ReadinessOK() {
		t.Error("expected readiness=false during Starting")
	}

	// Release: signal ready, then cancel to stop.
	close(readyCh)
	waitForPhase(t, mgr, PhaseRunning, 2*time.Second)
	cancel()
	<-done
}

// TestManagerRunningAfterAllReady verifies that the phase transitions from
// Starting → Running once every controller signals Ready.
func TestManagerRunningAfterAllReady(t *testing.T) {
	readyA := make(chan struct{})
	readyB := make(chan struct{})

	ctrls := []Interface{
		&fakeController{name: "a", readyCh: readyA, runFunc: func(ctx context.Context) error {
			close(readyA)
			<-ctx.Done()
			return nil
		}},
		&fakeController{name: "b", readyCh: readyB, runFunc: func(ctx context.Context) error {
			// Delay ready signal.
			time.Sleep(50 * time.Millisecond)
			close(readyB)
			<-ctx.Done()
			return nil
		}},
	}

	mgr := NewManager(ctrls)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- mgr.Run(ctx) }()

	// While B hasn't signalled Ready, phase should be Starting.
	waitForPhase(t, mgr, PhaseStarting, 2*time.Second)
	// Eventually both are ready → Running.
	waitForPhase(t, mgr, PhaseRunning, 2*time.Second)

	if !mgr.IsHealthy() {
		t.Error("expected healthy in PhaseRunning")
	}
	if !mgr.ReadinessOK() {
		t.Error("expected readiness=true in PhaseRunning")
	}

	cancel()
	<-done
	if mgr.Phase() != PhaseStopped {
		t.Errorf("expected PhaseStopped, got %s", mgr.Phase())
	}
}

// TestManagerFailedBeforeReady verifies that a controller failing before
// it signals Ready transitions directly to PhaseFailed.
func TestManagerFailedBeforeReady(t *testing.T) {
	readyCh := make(chan struct{}) // never closed — controller fails first
	mgr := NewManager([]Interface{
		&fakeController{name: "init-fail", readyCh: readyCh, runFunc: func(ctx context.Context) error {
			return fmt.Errorf("init error")
		}},
	})

	err := mgr.Run(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "init error") {
		t.Errorf("expected 'init error', got: %v", err)
	}
	if mgr.Phase() != PhaseFailed {
		t.Errorf("expected PhaseFailed, got %s", mgr.Phase())
	}
}

// TestManagerSetFailedIsAbsorbing is a unit test proving that once
// PhaseFailed is set, no advancePhase call can overwrite it.
// This eliminates the errCh-vs-ctx.Done() select race.
func TestManagerSetFailedIsAbsorbing(t *testing.T) {
	mgr := NewManager(nil) // phase field is all we need

	// Start from a non-Failed state.
	mgr.advancePhase(PhaseStarting)
	mgr.setFailed()
	if mgr.Phase() != PhaseFailed {
		t.Fatalf("expected PhaseFailed, got %s", mgr.Phase())
	}

	// None of these should overwrite Failed.
	for _, p := range []ManagerPhase{PhaseStarting, PhaseRunning, PhaseStopping, PhaseStopped} {
		mgr.advancePhase(p)
		if mgr.Phase() != PhaseFailed {
			t.Errorf("advancePhase(%s) overwrote PhaseFailed", p)
		}
	}

	// setFailed again should be a harmless no-op.
	mgr.setFailed()
	if mgr.Phase() != PhaseFailed {
		t.Error("double setFailed changed phase")
	}
}

// TestManagerFailedNotOverwrittenByCtxDone runs a scenario where errCh and
// ctx.Done are both ready. Regardless of which the select picks, the final
// phase must be Failed — never Stopping or Stopped.
func TestManagerFailedNotOverwrittenByCtxDone(t *testing.T) {
	// Run 20 iterations to exercise select non-determinism.
	for i := 0; i < 20; i++ {
		readyFail := make(chan struct{})
		readySib := make(chan struct{})
		ctrls := []Interface{
			&fakeController{name: "fast-fail", readyCh: readyFail, runFunc: func(ctx context.Context) error {
				close(readyFail)
				return fmt.Errorf("boom")
			}},
			&fakeController{name: "sibling", readyCh: readySib, runFunc: func(ctx context.Context) error {
				close(readySib)
				<-ctx.Done()
				return nil
			}},
		}
		mgr := NewManager(ctrls)
		_ = mgr.Run(context.Background())
		if mgr.Phase() != PhaseFailed {
			t.Fatalf("iteration %d: expected PhaseFailed, got %s", i, mgr.Phase())
		}
	}
}

// TestManagerSlowDrainStaysFailed verifies that during slow sibling drain
// (wg.Wait), the phase remains PhaseFailed.
func TestManagerSlowDrainStaysFailed(t *testing.T) {
	slowExit := make(chan struct{})
	readyFast := make(chan struct{})
	readySlow := make(chan struct{})
	ctrls := []Interface{
		&fakeController{name: "fast-fail", readyCh: readyFast, runFunc: func(ctx context.Context) error {
			close(readyFast)
			return fmt.Errorf("instant failure")
		}},
		&fakeController{name: "slow-drain", readyCh: readySlow, runFunc: func(ctx context.Context) error {
			close(readySlow)
			<-ctx.Done()
			<-slowExit // stall to simulate slow shutdown
			return nil
		}},
	}

	mgr := NewManager(ctrls)
	done := make(chan error, 1)
	go func() { done <- mgr.Run(context.Background()) }()

	waitForPhase(t, mgr, PhaseFailed, 2*time.Second)
	// Slow sibling is still blocked; phase must remain Failed.
	time.Sleep(50 * time.Millisecond)
	if mgr.Phase() != PhaseFailed {
		t.Errorf("expected PhaseFailed while draining, got %s", mgr.Phase())
	}
	if mgr.IsHealthy() {
		t.Error("expected not healthy while Failed")
	}

	close(slowExit)
	if err := <-done; err == nil {
		t.Fatal("expected error from fast-fail")
	}
	if mgr.Phase() != PhaseFailed {
		t.Errorf("expected PhaseFailed after Run returns, got %s", mgr.Phase())
	}
}

// TestManagerStoppingOnCancel verifies that external context cancellation
// transitions to PhaseStopping while goroutines drain, then PhaseStopped.
func TestManagerStoppingOnCancel(t *testing.T) {
	slowExit := make(chan struct{})
	readyCh := make(chan struct{})
	ctrls := []Interface{
		&fakeController{name: "slow", readyCh: readyCh, runFunc: func(ctx context.Context) error {
			close(readyCh)
			<-ctx.Done()
			<-slowExit // stall to simulate slow shutdown
			return nil
		}},
	}

	mgr := NewManager(ctrls)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- mgr.Run(ctx) }()

	waitForPhase(t, mgr, PhaseRunning, 2*time.Second)
	cancel()

	waitForPhase(t, mgr, PhaseStopping, 2*time.Second)
	if mgr.ReadinessOK() {
		t.Error("expected readiness=false during Stopping")
	}

	close(slowExit)
	<-done

	if mgr.Phase() != PhaseStopped {
		t.Errorf("expected PhaseStopped, got %s", mgr.Phase())
	}
}

// TestManagerPhaseIdleReadiness verifies that PhaseIdle passes readiness
// (standby) and that non-ready phases do not.
func TestManagerPhaseIdleReadiness(t *testing.T) {
	mgr := NewManager([]Interface{
		&fakeController{name: "dummy", readyCh: make(chan struct{}), runFunc: func(ctx context.Context) error {
			<-ctx.Done()
			return nil
		}},
	})

	if mgr.Phase() != PhaseIdle {
		t.Errorf("expected PhaseIdle, got %s", mgr.Phase())
	}
	if mgr.IsHealthy() {
		t.Error("expected IsHealthy()=false in PhaseIdle")
	}
	if !mgr.ReadinessOK() {
		t.Error("PhaseIdle should pass readiness")
	}

	// Verify non-ready phases.
	for _, p := range []ManagerPhase{PhaseStarting, PhaseStopping, PhaseStopped, PhaseFailed} {
		mgr2 := NewManager(nil)
		mgr2.phase.Store(int32(p))
		if mgr2.ReadinessOK() {
			t.Errorf("Phase %s should NOT pass readiness", p)
		}
	}
}

// TestManagerPhaseString verifies the Stringer implementation.
func TestManagerPhaseString(t *testing.T) {
	cases := []struct {
		phase ManagerPhase
		want  string
	}{
		{PhaseIdle, "Idle"},
		{PhaseStarting, "Starting"},
		{PhaseRunning, "Running"},
		{PhaseStopping, "Stopping"},
		{PhaseStopped, "Stopped"},
		{PhaseFailed, "Failed"},
		{ManagerPhase(99), "Unknown(99)"},
	}
	for _, tc := range cases {
		if got := tc.phase.String(); got != tc.want {
			t.Errorf("Phase(%d).String() = %q, want %q", int32(tc.phase), got, tc.want)
		}
	}
}

// TestManagerHealthTransitions exercises the full lifecycle:
// Idle → Starting → Running → Stopping → Stopped.
func TestManagerHealthTransitions(t *testing.T) {
	readyCh := make(chan struct{})
	slowExit := make(chan struct{})
	ctrls := []Interface{
		&fakeController{name: "x", readyCh: readyCh, runFunc: func(ctx context.Context) error {
			close(readyCh)
			<-ctx.Done()
			<-slowExit
			return nil
		}},
	}
	mgr := NewManager(ctrls)

	// Idle
	if mgr.Phase() != PhaseIdle {
		t.Errorf("expected PhaseIdle, got %s", mgr.Phase())
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- mgr.Run(ctx) }()

	// Starting → Running
	waitForPhase(t, mgr, PhaseRunning, 2*time.Second)
	if !mgr.IsHealthy() {
		t.Error("expected healthy in PhaseRunning")
	}

	cancel()

	// Stopping (while slow exit blocks)
	waitForPhase(t, mgr, PhaseStopping, 2*time.Second)

	close(slowExit)
	<-done

	// Stopped
	if mgr.Phase() != PhaseStopped {
		t.Errorf("expected PhaseStopped, got %s", mgr.Phase())
	}
	if mgr.IsHealthy() {
		t.Error("expected not healthy after stop")
	}
}

// TestManagerConcurrentReady verifies that awaitReady correctly handles
// controllers that signal Ready at different times.
func TestManagerConcurrentReady(t *testing.T) {
	const n = 5
	var ctrls []Interface
	var readyChans []chan struct{}
	var wgStarted sync.WaitGroup

	for i := 0; i < n; i++ {
		ch := make(chan struct{})
		readyChans = append(readyChans, ch)
		wgStarted.Add(1)
		localCh := ch
		ctrls = append(ctrls, &fakeController{
			name:    fmt.Sprintf("ctrl-%d", i),
			readyCh: localCh,
			runFunc: func(ctx context.Context) error {
				wgStarted.Done()
				<-ctx.Done()
				return nil
			},
		})
	}

	mgr := NewManager(ctrls)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- mgr.Run(ctx) }()

	// Wait for all goroutines to start.
	wgStarted.Wait()

	// Still Starting — no Ready signals yet.
	if mgr.Phase() != PhaseStarting {
		t.Errorf("expected PhaseStarting, got %s", mgr.Phase())
	}

	// Close Ready channels one by one; should not be Running until all done.
	for i := 0; i < n-1; i++ {
		close(readyChans[i])
		time.Sleep(10 * time.Millisecond)
		if mgr.Phase() == PhaseRunning {
			t.Fatal("should not be Running until all controllers are Ready")
		}
	}

	// Close last one.
	close(readyChans[n-1])
	waitForPhase(t, mgr, PhaseRunning, 2*time.Second)

	cancel()
	<-done
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// waitForPhase polls until the manager reaches the target phase or the
// deadline expires.
func waitForPhase(t *testing.T, mgr *Manager, target ManagerPhase, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for mgr.Phase() != target {
		select {
		case <-deadline:
			t.Fatalf("manager did not reach %s in time (current: %s)", target, mgr.Phase())
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
}
