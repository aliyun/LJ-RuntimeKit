package computedomain

import (
	"context"
	"errors"
	goruntime "runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Readiness publication vs. shutdown
//
// Readiness is published by Controller.publishReady inside the same startMu
// critical section that shutdown uses for its closed transition. That gives
// exactly two orders, both covered here without relying on sleeps:
//
//	shutdown first   -> closed is observed under the lock, OnReady never runs,
//	                    and Run returns an error instead of draining the queue.
//	readiness first  -> closed cannot be published until OnReady returned, so no
//	                    manager set is torn down underneath the callback.
//
// The interleaving is driven through the beforeReady test seam (which runs
// between "last manager set published as started" and "readiness published"),
// through the OnReady callback itself and through lock ownership (TryLock), not
// through timing.
// ---------------------------------------------------------------------------

// controllerReadiness is a snapshot of the guarded readiness state.
type controllerReadiness struct {
	ready   bool
	closed  bool
	started int
}

func readinessOf(ctrl *Controller) controllerReadiness {
	ctrl.startMu.Lock()
	defer ctrl.startMu.Unlock()
	return controllerReadiness{ready: ctrl.ready, closed: ctrl.closed, started: len(ctrl.startedManagers)}
}

// shutdownHasStarted observes the shutdown linearization point without taking
// startMu, which a readiness callback may hold. shutdown sets closed and closes
// this signal inside one and the same critical section (closed first), so an
// open signal proves that closed has not been published.
func shutdownHasStarted(ctrl *Controller) bool {
	select {
	case <-ctrl.shutdownStarted:
		return true
	default:
		return false
	}
}

// assertNoTeardownVisible asserts that no part of the teardown has become
// observable. It is only meaningful while the caller has established that the
// shutdown path cannot be inside the lifecycle critical section (the readiness
// callback holds startMu): in that state closed is provably false, and the
// channel below is provably still open.
func assertNoTeardownVisible(t *testing.T, ctrl *Controller, when string) {
	t.Helper()
	if shutdownHasStarted(ctrl) {
		t.Errorf("%s: the shutdown linearization point (closed) was published before the readiness callback returned", when)
	}
	if ctrl.queue.ShuttingDown() {
		t.Errorf("%s: the shared callback queue was shut down before the readiness callback returned", when)
	}
}

// waitForReadiness observes the published readiness flag, yielding instead of
// sleeping. It is only used by tests that have no callback to synchronize on;
// every interleaving assertion in this file is driven by channels or lock
// ownership.
func waitForReadiness(t *testing.T, ctrl *Controller) controllerReadiness {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		state := readinessOf(ctrl)
		if state.ready {
			return state
		}
		if time.Now().After(deadline) {
			t.Fatal("readiness was never published")
		}
		goruntime.Gosched()
	}
}

// TestControllerRunDoesNotPublishReadyWhenShutdownWins is the regression test for
// the reported interleaving: the last manager set has already published its
// start, and Stop completes (closed, cancel, managers stopped) before Run gets
// to publish readiness. OnReady must never fire and Run must not drain the
// queue.
func TestControllerRunDoesNotPublishReadyWhenShutdownWins(t *testing.T) {
	queueBaseline := stableGoroutineCount(t, "waitingLoop")
	informerBaseline := stableGoroutineCount(t, "(*Reflector)")
	workerBaseline := stableGoroutineCount(t, "processLoop")

	ctrl := mustNewController(t, Config{DynamicClient: newDualGroupFakeClient(t)})
	var readyCalls int32
	ctrl.OnReady = func() { atomic.AddInt32(&readyCalls, 1) }

	var (
		hookCalls     int32
		stopRanInHook atomic.Bool
	)
	// Deterministic control of the exact window QA reported: the hook runs after
	// the last manager set was published as started and before readiness is
	// published, so Stop reliably wins the linearization race.
	ctrl.beforeReady = func() {
		if atomic.AddInt32(&hookCalls, 1) != 1 {
			t.Errorf("beforeReady ran %d times, want exactly once", hookCalls)
		}
		ctrl.Stop()
		// Stop returned, so the whole teardown is done: the lifecycle state and
		// the shared queue must be observable as shut down together.
		if !shutdownHasStarted(ctrl) {
			t.Error("Stop returned without publishing the shutdown linearization point")
		}
		if !ctrl.queue.ShuttingDown() {
			t.Error("Stop returned while the shared callback queue was still open")
		}
		stopRanInHook.Store(true)
	}

	done, cancel := runControllerAsync(ctrl)
	defer cancel()

	var err error
	select {
	case err = <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("Run did not return after Stop won the readiness race")
	}
	if !stopRanInHook.Load() {
		t.Fatal("the deterministic Stop seam did not run")
	}
	if err == nil {
		t.Fatal("Run must fail instead of reporting a stopped controller as ready")
	}
	if !strings.Contains(err.Error(), "shut down before readiness was published") {
		t.Errorf("Run error = %v, want a shutdown-before-readiness error", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Run error = %v, want it to carry the cancelled run context", err)
	}
	if got := atomic.LoadInt32(&readyCalls); got != 0 {
		t.Fatalf("OnReady was called %d times after Stop won the race, want 0", got)
	}

	state := readinessOf(ctrl)
	if state.ready {
		t.Error("readiness must not be published once shutdown won")
	}
	if !state.closed || state.started != 2 {
		t.Errorf("state after the race = %+v, want closed with both manager sets stopped", state)
	}
	if !ctrl.queue.ShuttingDown() {
		t.Error("the shared callback queue must be shut down")
	}
	if !shutdownHasStarted(ctrl) {
		t.Error("the shutdown linearization point must be published when shutdown won the race")
	}
	// Run returned an error; it never reached queue.Run. Reaching it would have
	// returned nil immediately (the queue is already shut down), so the error
	// above is exactly the observable proof that the queue was not drained.
	assertNoLeakedGoroutines(t, queueBaseline, informerBaseline, workerBaseline)
}

// TestControllerRunPublishesReadyExactlyOnce covers the healthy path: readiness
// is published exactly once, both manager sets are running, readiness stays
// published through a normal cancellation, and a single-use controller never
// publishes twice.
func TestControllerRunPublishesReadyExactlyOnce(t *testing.T) {
	ctrl := mustNewController(t, Config{
		DynamicClient:                newDualGroupFakeClient(t),
		Namespace:                    "kube-system",
		ComputeDomainCliqueNamespace: "default",
	})
	var readyCalls int32
	ready := make(chan struct{}, 1)
	ctrl.OnReady = func() {
		atomic.AddInt32(&readyCalls, 1)
		ready <- struct{}{}
	}

	done, cancel := runControllerAsync(ctrl)
	defer cancel()
	select {
	case <-ready:
	case <-time.After(20 * time.Second):
		t.Fatal("the controller did not publish readiness with both groups served")
	}

	state := readinessOf(ctrl)
	if !state.ready || state.closed || state.started != 2 {
		t.Fatalf("state after readiness = %+v, want published, open and both manager sets started", state)
	}
	if got := atomic.LoadInt32(&readyCalls); got != 1 {
		t.Fatalf("OnReady was called %d times on a healthy startup, want 1", got)
	}
	// Readiness is published from a state machine, not from the callback: a
	// second publication attempt is rejected instead of calling OnReady again.
	if err := ctrl.publishReady(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "already published") {
		t.Errorf("second publishReady = %v, want an already-published error", err)
	}
	if got := atomic.LoadInt32(&readyCalls); got != 1 {
		t.Fatalf("OnReady was called %d times after a second publication attempt, want 1", got)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v after a normal cancellation, want nil", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Run did not return after the context was cancelled")
	}
	if got := atomic.LoadInt32(&readyCalls); got != 1 {
		t.Fatalf("OnReady was called %d times, want exactly 1 for the whole run", got)
	}
	if state := readinessOf(ctrl); !state.ready || !state.closed {
		t.Errorf("state after shutdown = %+v, want readiness published before the closed transition", state)
	}
}

// TestControllerShutdownWaitsForReadyCallbackToReturn pins the other direction of
// the protocol: once readiness is being published, shutdown cannot publish closed
// (and therefore cannot stop a manager set) until the callback returned. The
// ordering is asserted with lock ownership and a happens-before event log, not
// with timing.
func TestControllerShutdownWaitsForReadyCallbackToReturn(t *testing.T) {
	ctrl := mustNewController(t, Config{DynamicClient: newDualGroupFakeClient(t)})

	var (
		eventsMu sync.Mutex
		events   []string
	)
	record := func(event string) {
		eventsMu.Lock()
		events = append(events, event)
		eventsMu.Unlock()
	}
	eventIndex := func(event string) int {
		eventsMu.Lock()
		defer eventsMu.Unlock()
		for i, got := range events {
			if got == event {
				return i
			}
		}
		return -1
	}

	callbackEntered := make(chan struct{})
	releaseCallback := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseCallback) }) }
	defer release()

	var readyCalls int32
	ctrl.OnReady = func() {
		atomic.AddInt32(&readyCalls, 1)
		record("readiness-callback-enter")
		close(callbackEntered)
		<-releaseCallback
		record("readiness-callback-exit")
	}

	done, cancel := runControllerAsync(ctrl)
	defer cancel()
	select {
	case <-callbackEntered:
	case <-time.After(20 * time.Second):
		t.Fatal("the controller never published readiness")
	}

	// The callback runs inside the lifecycle critical section, so shutdown cannot
	// even reach the closed transition while it is running. TryLock proves the
	// lock is owned by the publishing goroutine; it is not a timing heuristic.
	if ctrl.startMu.TryLock() {
		ctrl.startMu.Unlock()
		t.Fatal("the lifecycle lock must be held while OnReady runs, otherwise shutdown could publish closed under the callback")
	}

	stopReturned := make(chan struct{})
	go func() {
		ctrl.Stop()
		record("stop-returned")
		close(stopReturned)
	}()

	// Give a lock-free implementation every chance to stop the manager sets
	// underneath the callback. No assertion below depends on this:
	//
	//   - the lifecycle lock is held by the publishing goroutine (TryLock above),
	//     and Stop cannot publish closed without it, so it cannot have returned;
	//   - the teardown signals (closed, queue) are published inside that same
	//     critical section, so they cannot be visible either.
	select {
	case <-stopReturned:
		t.Fatal("Stop returned while OnReady was still running: shutdown must wait for the readiness callback")
	default:
	}
	assertNoTeardownVisible(t, ctrl, "while OnReady holds the lifecycle lock")

	release()
	select {
	case <-stopReturned:
	case <-time.After(20 * time.Second):
		t.Fatal("Stop deadlocked against the readiness publication")
	}
	var err error
	select {
	case err = <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("Run did not return after Stop")
	}
	if err != nil {
		t.Fatalf("Run returned %v after a clean Stop, want nil", err)
	}
	if got := atomic.LoadInt32(&readyCalls); got != 1 {
		t.Fatalf("OnReady was called %d times, want 1", got)
	}
	exitAt, stopAt := eventIndex("readiness-callback-exit"), eventIndex("stop-returned")
	if exitAt < 0 || stopAt < 0 || exitAt > stopAt {
		t.Fatalf("event order = %v, want the readiness callback to exit (index %d) before Stop returned (index %d)",
			events, exitAt, stopAt)
	}
	if state := readinessOf(ctrl); !state.ready || !state.closed || state.started != 2 {
		t.Errorf("state after shutdown = %+v, want published readiness, closed and both manager sets recorded", state)
	}
	if !ctrl.queue.ShuttingDown() || !shutdownHasStarted(ctrl) {
		t.Error("after Stop returned, closed and the shared callback queue must both be observable as shut down")
	}
	for _, manager := range ctrl.startedManagers {
		if manager.cancelContext != nil {
			t.Errorf("%s manager set was not stopped after the readiness callback returned",
				manager.config.Contract.APIGroup)
		}
	}
}

// TestControllerReadinessKeepsQueueOpenUntilCallbackReturns is the regression test
// for the third blocking finding: shutdown used to shut the shared queue down
// BEFORE it took the lifecycle lock, so a Stop that lost the readiness race still
// closed the queue underneath a running OnReady callback. Every teardown effect,
// the queue included, must now be published only after the callback returned.
//
// The interleaving is driven by two private test seams and by lock ownership,
// never by sleeps or yields:
//
//   - beforeShutdownLock stops the Stop goroutine at the very beginning of the
//     one-time teardown, i.e. before it contends for the lifecycle lock. Teardown
//     work done before that point is therefore detected deterministically.
//   - after being released, Stop's only possible next action is startMu.Lock,
//     which TryLock proves is held by the readiness callback, so Stop is provably
//     waiting and cannot have returned: teardown work done between the seam and
//     the lock is detected without any timing assumption.
//   - the shutdown linearization point is observed through the lock-free
//     shutdownStarted signal, which gives the happens-before order of "callback
//     returned" vs "shutdown started".
func TestControllerReadinessKeepsQueueOpenUntilCallbackReturns(t *testing.T) {
	ctrl := mustNewController(t, Config{DynamicClient: newDualGroupFakeClient(t)})

	var (
		eventsMu sync.Mutex
		events   []string
	)
	record := func(event string) {
		eventsMu.Lock()
		events = append(events, event)
		eventsMu.Unlock()
	}
	eventIndex := func(event string) int {
		eventsMu.Lock()
		defer eventsMu.Unlock()
		for i, got := range events {
			if got == event {
				return i
			}
		}
		return -1
	}

	callbackEntered := make(chan struct{})
	releaseCallback := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseCallback) }) }
	defer release()

	var readyCalls int32
	ctrl.OnReady = func() {
		atomic.AddInt32(&readyCalls, 1)
		close(callbackEntered)
		<-releaseCallback
		record("readiness-callback-exit")
	}

	// Private test seam: runs at the very start of the one-time teardown, before
	// shutdown contends for the lifecycle lock.
	stopAtLock := make(chan struct{})
	proceedToLock := make(chan struct{})
	var proceedOnce sync.Once
	proceed := func() { proceedOnce.Do(func() { close(proceedToLock) }) }
	defer proceed()
	ctrl.beforeShutdownLock = func() {
		record("stop-entered-shutdown")
		close(stopAtLock)
		<-proceedToLock
	}

	done, cancel := runControllerAsync(ctrl)
	defer cancel()

	select {
	case <-callbackEntered:
	case <-time.After(20 * time.Second):
		t.Fatal("the controller never published readiness")
	}

	// The readiness callback owns the lifecycle lock; shutdown cannot be inside
	// the critical section, so nothing of the teardown may be observable.
	if ctrl.startMu.TryLock() {
		ctrl.startMu.Unlock()
		t.Fatal("the lifecycle lock must be held while OnReady runs")
	}
	assertNoTeardownVisible(t, ctrl, "while OnReady holds the lifecycle lock")

	stopReturned := make(chan struct{})
	go func() {
		ctrl.Stop()
		record("stop-returned")
		close(stopReturned)
	}()

	// Handshake: the Stop goroutine is now inside the shutdown path, immediately
	// before it contends for the lifecycle lock.
	select {
	case <-stopAtLock:
	case <-time.After(20 * time.Second):
		t.Fatal("Stop never reached the shutdown handshake")
	}
	assertNoTeardownVisible(t, ctrl, "while Stop waits before the lifecycle lock")

	// Let it contend for the lock. It cannot obtain it, and publishing closed
	// requires it, so Stop provably cannot have returned; the queue must still be
	// open at this point as well.
	proceed()
	select {
	case <-stopReturned:
		t.Fatal("Stop returned before the readiness callback finished")
	default:
	}
	assertNoTeardownVisible(t, ctrl, "while Stop is waiting for the lifecycle lock")

	// A goroutine that only learns about the shutdown through the lock-free
	// signal records the order of the two linearization points.
	linearizationSeen := make(chan struct{})
	go func() {
		<-ctrl.shutdownStarted
		record("shutdown-linearized")
		close(linearizationSeen)
	}()

	release()
	select {
	case <-stopReturned:
	case <-time.After(20 * time.Second):
		t.Fatal("Stop deadlocked against the readiness publication")
	}
	select {
	case <-linearizationSeen:
	case <-time.After(20 * time.Second):
		t.Fatal("the shutdown linearization point was never published")
	}

	var err error
	select {
	case err = <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("Run did not return after Stop")
	}
	if err != nil {
		t.Fatalf("Run returned %v after a clean Stop, want nil", err)
	}
	if got := atomic.LoadInt32(&readyCalls); got != 1 {
		t.Fatalf("OnReady was called %d times, want 1", got)
	}

	exitAt, linearAt := eventIndex("readiness-callback-exit"), eventIndex("shutdown-linearized")
	if exitAt < 0 || linearAt < 0 || exitAt > linearAt {
		t.Fatalf("event order = %v, want the readiness callback to exit (index %d) before shutdown linearized (index %d)",
			events, exitAt, linearAt)
	}

	// Once shutdown completed, closed and the queue are observable as one state
	// again.
	state := readinessOf(ctrl)
	if !state.ready || !state.closed || state.started != 2 {
		t.Errorf("state after shutdown = %+v, want published readiness, closed and both manager sets recorded", state)
	}
	if !shutdownHasStarted(ctrl) {
		t.Error("the shutdown linearization signal must be published once Stop returned")
	}
	if !ctrl.queue.ShuttingDown() {
		t.Error("the shared callback queue must be shut down once Stop returned")
	}
	for _, manager := range ctrl.startedManagers {
		if manager.cancelContext != nil {
			t.Errorf("%s manager set was not stopped after the readiness callback returned",
				manager.config.Contract.APIGroup)
		}
	}
}

// TestControllerRunDoesNotPublishReadyWhenContextIsCancelled pins the context
// check at the linearization point: a run context that was cancelled before
// readiness could be published fails the start instead of reporting Ready.
func TestControllerRunDoesNotPublishReadyWhenContextIsCancelled(t *testing.T) {
	queueBaseline := stableGoroutineCount(t, "waitingLoop")
	informerBaseline := stableGoroutineCount(t, "(*Reflector)")
	workerBaseline := stableGoroutineCount(t, "processLoop")

	ctrl := mustNewController(t, Config{DynamicClient: newDualGroupFakeClient(t)})
	var readyCalls int32
	ctrl.OnReady = func() { atomic.AddInt32(&readyCalls, 1) }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Deterministic seam: cancel after both manager sets started and before
	// readiness is published.
	ctrl.beforeReady = func() { cancel() }

	err := ctrl.Run(ctx, 1)
	if err == nil {
		t.Fatal("Run must fail when the run context is cancelled before readiness is published")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Run error = %v, want it to carry the cancelled context", err)
	}
	if got := atomic.LoadInt32(&readyCalls); got != 0 {
		t.Fatalf("OnReady was called %d times for a cancelled context, want 0", got)
	}
	if state := readinessOf(ctrl); state.ready || !state.closed || state.started != 2 {
		t.Errorf("state = %+v, want unpublished readiness for two stopped manager sets", state)
	}
	if !ctrl.queue.ShuttingDown() {
		t.Error("the shared callback queue must be shut down when readiness cannot be published")
	}
	assertNoLeakedGoroutines(t, queueBaseline, informerBaseline, workerBaseline)
}

// TestControllerReadyIsPublishedWithNilOnReadyCallback shows that readiness is a
// state on the controller, not a side effect of the callback: a nil OnReady is a
// valid configuration and startup still succeeds.
func TestControllerReadyIsPublishedWithNilOnReadyCallback(t *testing.T) {
	ctrl := mustNewController(t, Config{DynamicClient: newDualGroupFakeClient(t)})
	if ctrl.OnReady != nil {
		t.Fatal("this test requires a nil OnReady callback")
	}

	done, cancel := runControllerAsync(ctrl)
	defer cancel()
	state := waitForReadiness(t, ctrl)
	if state.closed || state.started != 2 {
		t.Fatalf("state after nil-callback readiness = %+v, want open with both manager sets started", state)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v with a nil OnReady callback, want nil", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Run did not return after the context was cancelled")
	}
}

// TestControllerPublishReadyRequiresStartedManagerSets pins the inventory check
// of publishReady: readiness cannot be published before every manager set is in
// the start inventory, so a partially started controller is never Ready.
func TestControllerPublishReadyRequiresStartedManagerSets(t *testing.T) {
	ctrl := mustNewController(t, Config{DynamicClient: newDualGroupFakeClient(t)})
	var readyCalls int32
	ctrl.OnReady = func() { atomic.AddInt32(&readyCalls, 1) }

	err := ctrl.publishReady(context.Background())
	if err == nil || !strings.Contains(err.Error(), "0 of 2 ComputeDomain manager sets started") {
		t.Fatalf("publishReady without started managers = %v, want a partial-startup error", err)
	}
	if got := atomic.LoadInt32(&readyCalls); got != 0 {
		t.Fatalf("OnReady was called %d times without started managers, want 0", got)
	}
	if state := readinessOf(ctrl); state.ready {
		t.Error("readiness must not be published without the full start inventory")
	}
}

// isAcceptableRacyRunError reports whether err is a legitimate outcome of a
// shutdown/cancel that raced readiness publication: the controller may have been
// stopped before Run began, while a manager set was starting, or between the last
// manager start and the readiness publication. Anything else is a bug.
func isAcceptableRacyRunError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return true
	}
	message := err.Error()
	for _, fragment := range []string{
		"already been shut down",
		"already started",
		"shut down before readiness was published",
		"shut down before the ComputeDomain manager",
		"shut down while the ComputeDomain manager",
		"was aborted",
	} {
		if strings.Contains(message, fragment) {
			return true
		}
	}
	return false
}

// TestControllerConcurrentStopOrCancelNeverPublishesReadyLate repeatedly races
// Stop and context cancellation against readiness publication: readiness may be
// published at most once, never after Run returned, and the controller must
// always tear down without deadlocking.
func TestControllerConcurrentStopOrCancelNeverPublishesReadyLate(t *testing.T) {
	queueBaseline := stableGoroutineCount(t, "waitingLoop")

	const iterations = 8
	for iteration := 1; iteration <= iterations; iteration++ {
		ctrl := mustNewController(t, Config{DynamicClient: newDualGroupFakeClient(t)})
		var (
			readyCalls  int32
			runReturned int32
		)
		ctrl.OnReady = func() {
			if atomic.LoadInt32(&runReturned) == 1 {
				t.Errorf("OnReady was called after Run returned: readiness must never be published late")
			}
			atomic.AddInt32(&readyCalls, 1)
		}

		done, cancel := runControllerAsync(ctrl)
		var wg sync.WaitGroup
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 3; j++ {
					ctrl.Stop()
				}
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			cancel()
		}()
		wg.Wait()

		var err error
		select {
		case err = <-done:
		case <-time.After(30 * time.Second):
			t.Fatalf("iteration %d: Run did not return after concurrent Stop calls", iteration)
		}
		atomic.StoreInt32(&runReturned, 1)
		cancel()

		if got := atomic.LoadInt32(&readyCalls); got > 1 {
			t.Fatalf("iteration %d: OnReady was called %d times, want at most once", iteration, got)
		}
		if err != nil && !isAcceptableRacyRunError(err) {
			t.Errorf("iteration %d: Run error = %v, want nil or a cancellation/shutdown error", iteration, err)
		}
		if !ctrl.queue.ShuttingDown() {
			t.Errorf("iteration %d: the shared callback queue must be shut down", iteration)
		}
		state := readinessOf(ctrl)
		if !state.closed {
			t.Errorf("iteration %d: state = %+v, want closed", iteration, state)
		}
		if state.ready && state.started != 2 {
			t.Errorf("iteration %d: readiness published for %d manager sets, want 2", iteration, state.started)
		}
	}

	if got := waitForGoroutineCount(t, "waitingLoop", queueBaseline, 15*time.Second); got != queueBaseline {
		t.Errorf("waitingLoop goroutines = %d, want %d: a shared callback queue was left running", got, queueBaseline)
	}
}
