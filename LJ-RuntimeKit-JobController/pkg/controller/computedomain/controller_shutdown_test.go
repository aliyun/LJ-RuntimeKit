package computedomain

import (
	"context"
	"fmt"
	goruntime "runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clienttesting "k8s.io/client-go/testing"

	cdapi "github.com/lj-runtimekit/job-controller/pkg/computedomain"
)

// ---------------------------------------------------------------------------
// Goroutine accounting helpers
//
// NewController constructs the shared client-go rate limiting queue, and that
// constructor starts waitingLoop immediately. The queue therefore has to be shut
// down on every exit path, which these tests observe directly instead of only
// asserting that Run returned an error.
// ---------------------------------------------------------------------------

// countGoroutines counts live goroutines whose own stack names marker. The
// "created by ..." trailer is stripped first so a goroutine is never attributed
// to the function that created it.
func countGoroutines(t *testing.T, marker string) int {
	t.Helper()
	buf := make([]byte, 1<<20)
	for {
		n := goruntime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	count := 0
	for _, block := range strings.Split(string(buf), "\n\n") {
		if i := strings.Index(block, "\ncreated by "); i >= 0 {
			block = block[:i]
		}
		if strings.Contains(block, marker) {
			count++
		}
	}
	return count
}

// stableGoroutineCount samples until two consecutive samples agree, so a
// baseline taken while an earlier test's goroutines are still winding down
// cannot make the later comparison flaky.
func stableGoroutineCount(t *testing.T, marker string) int {
	t.Helper()
	previous := countGoroutines(t, marker)
	for i := 0; i < 150; i++ {
		time.Sleep(10 * time.Millisecond)
		next := countGoroutines(t, marker)
		if next == previous {
			return next
		}
		previous = next
	}
	return previous
}

// waitForGoroutineCount polls until exactly want goroutines match marker.
func waitForGoroutineCount(t *testing.T, marker string, want int, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	got := countGoroutines(t, marker)
	for got != want && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		got = countGoroutines(t, marker)
	}
	return got
}

// assertGoroutinesRunning proves the leak assertion is not vacuous: while the
// controller runs, all three goroutine families the teardown assertions look for
// must actually be present.
func assertGoroutinesRunning(t *testing.T, queueBaseline, informerBaseline, workerBaseline int) {
	t.Helper()
	if got := countGoroutines(t, "waitingLoop"); got <= queueBaseline {
		t.Fatalf("waitingLoop goroutines = %d, baseline %d: the queue goroutine is not observable", got, queueBaseline)
	}
	if got := countGoroutines(t, "(*Reflector)"); got <= informerBaseline {
		t.Fatalf("informer reflector goroutines = %d, baseline %d: the reflector goroutine is not observable", got, informerBaseline)
	}
	if got := countGoroutines(t, "processLoop"); got <= workerBaseline {
		t.Fatalf("informer worker goroutines = %d, baseline %d: the informer worker goroutine is not observable", got, workerBaseline)
	}
}

func assertNoLeakedGoroutines(t *testing.T, queueBaseline, informerBaseline, workerBaseline int) {
	t.Helper()
	if got := waitForGoroutineCount(t, "waitingLoop", queueBaseline, 15*time.Second); got != queueBaseline {
		t.Errorf("waitingLoop goroutines = %d, want %d: the shared callback queue was not shut down", got, queueBaseline)
	}
	if got := waitForGoroutineCount(t, "(*Reflector)", informerBaseline, 20*time.Second); got != informerBaseline {
		t.Errorf("informer reflector goroutines = %d, want %d: a manager informer was left running", got, informerBaseline)
	}
	if got := waitForGoroutineCount(t, "processLoop", workerBaseline, 20*time.Second); got != workerBaseline {
		t.Errorf("informer worker goroutines = %d, want %d: a manager informer was left running", got, workerBaseline)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// denyList fails every list of resource inside group, as if the CRD were not
// installed. Lists of the same resource in the other group are passed through.
func denyList(group, resource string) clienttesting.ReactionFunc {
	return func(action clienttesting.Action) (bool, runtime.Object, error) {
		if action.GetResource().Group != group {
			return false, nil, nil
		}
		return true, nil, apierrors.NewNotFound(schema.GroupResource{Group: group, Resource: resource}, resource)
	}
}

// newControllerWithMissingResource builds a controller whose cluster lacks one
// of the four required CRDs.
func newControllerWithMissingResource(t *testing.T, group, resource string) *Controller {
	t.Helper()
	client := newDualGroupFakeClient(t)
	client.PrependReactor("list", resource, denyList(group, resource))
	ctrl := NewController(Config{DynamicClient: client})
	t.Cleanup(ctrl.Stop)
	for _, manager := range ctrl.managers {
		manager.config.CacheSyncTimeout = 300 * time.Millisecond
	}
	return ctrl
}

// runControllerAsync runs the controller in the background.
func runControllerAsync(ctrl *Controller) (<-chan error, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ctrl.Run(ctx, 1) }()
	return done, cancel
}

// controllerShutdownState is a snapshot of the guarded shutdown state.
type controllerShutdownState struct {
	closed  bool
	started int
}

func shutdownStateOf(ctrl *Controller) controllerShutdownState {
	ctrl.startMu.Lock()
	defer ctrl.startMu.Unlock()
	return controllerShutdownState{closed: ctrl.closed, started: len(ctrl.startedManagers)}
}

// ---------------------------------------------------------------------------
// Queue level: ShutDown is idempotent and releases the waitingLoop
// ---------------------------------------------------------------------------

// TestCallbackWorkQueueShutDownIsIdempotentAndReleasesWaitingLoop pins the queue
// contract the controller relies on: the client-go waitingLoop goroutine starts
// with the queue and is observably gone once ShutDown ran, repeated ShutDown is
// safe, and work enqueued after ShutDown is dropped instead of growing the
// coalescing map forever.
func TestCallbackWorkQueueShutDownIsIdempotentAndReleasesWaitingLoop(t *testing.T) {
	baseline := stableGoroutineCount(t, "waitingLoop")

	queue := newCallbackWorkQueue()
	if queue.ShuttingDown() {
		t.Fatal("a freshly built queue must not report ShuttingDown")
	}
	if got := countGoroutines(t, "waitingLoop"); got != baseline+1 {
		t.Fatalf("waitingLoop goroutines = %d, want %d: constructing the queue must start exactly one waitingLoop", got, baseline+1)
	}

	obj := &unstructured.Unstructured{}
	obj.SetNamespace("default")
	obj.SetName("cd")
	obj.SetUID("uid-shutdown")
	noop := func(context.Context, interface{}) error { return nil }
	queue.EnqueueWithKey(obj, "key", noop)

	for i := 0; i < 5; i++ {
		queue.ShutDown()
		if !queue.ShuttingDown() {
			t.Fatalf("ShuttingDown() = false after ShutDown call %d", i+1)
		}
	}

	// Work arriving after shutdown must be dropped, otherwise activeOps would
	// grow without ever being drained again.
	queue.EnqueueWithKey(obj, "late", noop)
	queue.mu.Lock()
	late := queue.activeOps["late"]
	queue.mu.Unlock()
	if late != nil {
		t.Fatal("EnqueueWithKey must drop work once the queue is shut down")
	}

	if got := waitForGoroutineCount(t, "waitingLoop", baseline, 15*time.Second); got != baseline {
		t.Fatalf("waitingLoop goroutines = %d, want %d: ShutDown must release the client-go waitingLoop", got, baseline)
	}
}

// TestCallbackWorkQueueRunExitsOnShutDown pins that Run does not depend on its
// context: shutting the queue down from outside makes it return.
func TestCallbackWorkQueueRunExitsOnShutDown(t *testing.T) {
	baseline := stableGoroutineCount(t, "waitingLoop")

	queue := newCallbackWorkQueue()
	ran := make(chan struct{})
	go func() {
		// Never cancelled on purpose.
		queue.Run(context.Background())
		close(ran)
	}()

	queue.ShutDown()
	select {
	case <-ran:
	case <-time.After(15 * time.Second):
		t.Fatal("queue.Run did not return after ShutDown even though its context was never cancelled")
	}

	if got := waitForGoroutineCount(t, "waitingLoop", baseline, 15*time.Second); got != baseline {
		t.Fatalf("waitingLoop goroutines = %d, want %d after Run returned", got, baseline)
	}
}

// ---------------------------------------------------------------------------
// Failure path: a missing CRD must tear the shared queue down
// ---------------------------------------------------------------------------

// TestControllerShutdownReleasesQueueWhenOneGroupCRDIsMissing is the regression
// test for the P1 finding: all four CRDs are hard startup dependencies, and a
// manager that fails to start must not leave the shared rate limiting queue (and
// therefore its waitingLoop goroutine) running. Every combination is repeated so
// a shutdown path that only works once cannot pass.
func TestControllerShutdownReleasesQueueWhenOneGroupCRDIsMissing(t *testing.T) {
	queueBaseline := stableGoroutineCount(t, "waitingLoop")
	informerBaseline := stableGoroutineCount(t, "(*Reflector)")
	workerBaseline := stableGoroutineCount(t, "processLoop")

	const repeats = 3
	for _, resource := range []string{cdapi.ComputeDomainResource, cdapi.ComputeDomainCliqueResource} {
		for _, contract := range cdapi.SupportedContracts() {
			missing := contract
			wantStarted := 0
			if missing.APIGroup != cdapi.GroupAliyun {
				// The Aliyun manager set already started and must be stopped by
				// the same shutdown that reports the NVIDIA failure.
				wantStarted = 1
			}
			for iteration := 1; iteration <= repeats; iteration++ {
				name := fmt.Sprintf("%s/%s/repeat-%d", missing.APIGroup, resource, iteration)
				t.Run(name, func(t *testing.T) {
					ctrl := newControllerWithMissingResource(t, missing.APIGroup, resource)
					if got := countGoroutines(t, "waitingLoop"); got <= queueBaseline {
						t.Fatalf("waitingLoop goroutines = %d, baseline %d: the controller queue is not observable",
							got, queueBaseline)
					}
					ready := make(chan struct{}, 1)
					ctrl.OnReady = func() { ready <- struct{}{} }

					done, cancel := runControllerAsync(ctrl)
					defer cancel()

					var err error
					select {
					case err = <-done:
					case <-time.After(20 * time.Second):
						t.Fatalf("Run did not fail after %s %s could not be listed", missing.APIGroup, resource)
					}
					if err == nil {
						t.Fatalf("Run must fail when %s %s is missing", missing.APIGroup, resource)
					}
					if !strings.Contains(err.Error(), missing.APIGroup) {
						t.Errorf("error %v should name the failing API group %s", err, missing.APIGroup)
					}
					if !strings.Contains(err.Error(), "cache sync failed") {
						t.Errorf("error %v should report the bounded cache sync failure", err)
					}
					select {
					case <-ready:
						t.Fatal("OnReady must not fire while one group failed to sync")
					default:
					}

					if !ctrl.queue.ShuttingDown() {
						t.Error("the shared callback queue must be shut down when a manager fails to start")
					}
					state := shutdownStateOf(ctrl)
					if !state.closed {
						t.Error("the controller must be marked shut down after a failed startup")
					}
					if state.started != wantStarted {
						t.Errorf("started manager sets = %d, want %d (one group must have started before the other failed)",
							state.started, wantStarted)
					}
					for _, manager := range ctrl.startedManagers {
						if manager.cancelContext != nil {
							t.Errorf("%s manager set was not stopped: its run context is still live",
								manager.config.Contract.APIGroup)
						}
					}
					// Repeated Stop after a failed Run must be a harmless no-op.
					ctrl.Stop()
					ctrl.Stop()

					// The controller is single use: the queue cannot be revived.
					if err := ctrl.Run(context.Background(), 1); err == nil ||
						!strings.Contains(err.Error(), "already been shut down") {
						t.Errorf("Run after a failed startup = %v, want an already-shut-down error", err)
					}

					assertNoLeakedGoroutines(t, queueBaseline, informerBaseline, workerBaseline)
				})
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Stop before Run, Stop during startup, repeated Stop
// ---------------------------------------------------------------------------

// TestControllerStopBeforeRunShutsDownQueue pins that an external Stop that
// arrives before Run still releases the queue and makes the controller single
// use, instead of leaking the waitingLoop started by NewController.
func TestControllerStopBeforeRunShutsDownQueue(t *testing.T) {
	queueBaseline := stableGoroutineCount(t, "waitingLoop")

	ctrl := mustNewController(t, Config{DynamicClient: newDualGroupFakeClient(t)})
	triggered := make(chan struct{}, 1)
	ctrl.OnReady = func() { triggered <- struct{}{} }

	if ctrl.queue.ShuttingDown() {
		t.Fatal("a controller that was never stopped must keep its queue open")
	}
	if got := countGoroutines(t, "waitingLoop"); got <= queueBaseline {
		t.Fatalf("waitingLoop goroutines = %d, baseline %d: the controller queue is not observable", got, queueBaseline)
	}
	for i := 0; i < 3; i++ {
		ctrl.Stop()
	}
	if !ctrl.queue.ShuttingDown() {
		t.Fatal("Stop before Run must shut the shared callback queue down")
	}
	if state := shutdownStateOf(ctrl); !state.closed || state.started != 0 {
		t.Fatalf("state after Stop before Run = %+v, want closed with no started manager set", state)
	}

	err := ctrl.Run(context.Background(), 1)
	if err == nil || !strings.Contains(err.Error(), "already been shut down") {
		t.Fatalf("Run after Stop = %v, want an already-shut-down error", err)
	}
	select {
	case <-triggered:
		t.Fatal("OnReady must not fire after the controller was stopped")
	default:
	}

	if got := waitForGoroutineCount(t, "waitingLoop", queueBaseline, 15*time.Second); got != queueBaseline {
		t.Fatalf("waitingLoop goroutines = %d, want %d after Stop before Run", got, queueBaseline)
	}
}

// TestControllerStopDuringManagerStartIsRaceFree covers an external Stop that
// lands while the second manager set is still syncing: the shutdown cancel must
// abort the in-flight start, the started set must be stopped, and Stop must not
// deadlock against the manager start.
func TestControllerStopDuringManagerStartIsRaceFree(t *testing.T) {
	queueBaseline := stableGoroutineCount(t, "waitingLoop")
	informerBaseline := stableGoroutineCount(t, "(*Reflector)")
	workerBaseline := stableGoroutineCount(t, "processLoop")

	client := newDualGroupFakeClient(t)
	client.PrependReactor("list", cdapi.ComputeDomainResource, denyList(cdapi.GroupNVIDIA, cdapi.ComputeDomainResource))
	ctrl := NewController(Config{DynamicClient: client})
	t.Cleanup(ctrl.Stop)
	// Deliberately long: only the shutdown cancel can end the second manager's
	// cache sync within the test timeout, which is exactly the ordering the
	// shutdown state machine has to guarantee.
	for _, manager := range ctrl.managers {
		manager.config.CacheSyncTimeout = 30 * time.Second
	}

	done, cancel := runControllerAsync(ctrl)
	defer cancel()

	startDeadline := time.Now().Add(20 * time.Second)
	for shutdownStateOf(ctrl).started != 1 {
		if time.Now().After(startDeadline) {
			t.Fatal("the first manager set never started")
		}
		time.Sleep(5 * time.Millisecond)
	}

	stopped := make(chan struct{})
	go func() {
		ctrl.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(15 * time.Second):
		t.Fatal("Stop deadlocked while a manager set was still starting")
	}

	var err error
	select {
	case err = <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("Run did not return after Stop: the shutdown cancel did not abort the bounded cache sync")
	}
	if err == nil {
		t.Fatal("Run must fail when one of the four CRDs is missing")
	}
	if !strings.Contains(err.Error(), cdapi.GroupNVIDIA) {
		t.Errorf("error %v should name the API group that could not sync", err)
	}
	if !ctrl.queue.ShuttingDown() {
		t.Error("the shared callback queue must be shut down when Stop interrupts a startup")
	}
	state := shutdownStateOf(ctrl)
	if !state.closed || state.started != 1 {
		t.Errorf("state after Stop during startup = %+v, want closed with the one started manager set", state)
	}
	for _, manager := range ctrl.startedManagers {
		if manager.cancelContext != nil {
			t.Errorf("%s manager set was not stopped: its run context is still live",
				manager.config.Contract.APIGroup)
		}
	}

	assertNoLeakedGoroutines(t, queueBaseline, informerBaseline, workerBaseline)
}

// ---------------------------------------------------------------------------
// Run creating its run context while Stop completes
// ---------------------------------------------------------------------------

// bridgeGoroutineMarker names the goroutine context.WithCancel starts to bridge
// a child context to a parent that is not itself a *cancelCtx. It lives exactly
// as long as the child context, so its disappearance proves the child was
// released. The closure number is left out on purpose: the Go runtime numbers the
// anonymous functions of propagateCancel, and only the enclosing name is stable.
const bridgeGoroutineMarker = "(*cancelCtx).propagateCancel.func"

// gatedParentContext parks context.WithCancel deterministically: its first Done
// call blocks until Release, which places Run between "the child context is
// being created" and "the child context is published in c.cancel". Done reports
// a channel that never closes, so the parent is never cancelled and
// context.WithCancel has to start the bridge goroutine described above; that is
// what makes the child context observable. Later Done calls return immediately,
// which is required because context.WithCancel probes the parent again inside
// parentCancelCtx.
type gatedParentContext struct {
	entered     chan struct{}
	release     chan struct{}
	done        chan struct{}
	calls       atomic.Int32
	releaseOnce sync.Once
}

func newGatedParentContext() *gatedParentContext {
	return &gatedParentContext{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		done:    make(chan struct{}),
	}
}

func (c *gatedParentContext) Deadline() (time.Time, bool) { return time.Time{}, false }

func (c *gatedParentContext) Done() <-chan struct{} {
	if c.calls.Add(1) == 1 {
		close(c.entered)
		<-c.release
	}
	return c.done
}

func (c *gatedParentContext) Err() error { return nil }

func (c *gatedParentContext) Value(any) any { return nil }

// Release unblocks the parked context.WithCancel. It is idempotent so a failed
// test cannot leave Run parked forever.
func (c *gatedParentContext) Release() {
	c.releaseOnce.Do(func() { close(c.release) })
}

// runContextState snapshots the mu-guarded run context publication.
func runContextState(ctrl *Controller) (context.CancelFunc, bool) {
	ctrl.mu.Lock()
	defer ctrl.mu.Unlock()
	return ctrl.cancel, ctrl.started
}

// TestControllerRunCreatingContextAcrossStopReleasesChildContext is the
// regression test for the reported leak: Run passed the closed check, Stop then
// completed the one-shot teardown with no run context to cancel, and Run went on
// to create and publish a fresh cancel that nothing would ever invoke. The child
// context - and the bridge goroutine context.WithCancel started for it - then
// outlived the controller.
//
// The window is driven entirely by handshakes: the parked gatedParentContext
// proves Run is inside context.WithCancel, Stop runs to completion there, and
// startMu is held by the test so Run cannot finish its lifecycle check until the
// bridge goroutine has been observed. No ordering depends on sleeps. In the fixed
// state machine the closed check and the publication share one startMu section,
// so this window (child context created, nothing published yet) is exactly the
// one QA reported, reached one step earlier; a run context published after the
// completed Stop would be the unfixable dangling cancel.
func TestControllerRunCreatingContextAcrossStopReleasesChildContext(t *testing.T) {
	queueBaseline := stableGoroutineCount(t, "waitingLoop")
	informerBaseline := stableGoroutineCount(t, "(*Reflector)")
	workerBaseline := stableGoroutineCount(t, "processLoop")
	bridgeBaseline := stableGoroutineCount(t, bridgeGoroutineMarker)

	ctrl := mustNewController(t, Config{DynamicClient: newDualGroupFakeClient(t)})
	var readyCalls int32
	ctrl.OnReady = func() { atomic.AddInt32(&readyCalls, 1) }

	parent := newGatedParentContext()
	defer parent.Release()

	done := make(chan error, 1)
	go func() { done <- ctrl.Run(parent, 1) }()

	// Run is provably creating the run context: context.WithCancel is parked on
	// the parent's first Done call, so the child exists but is not yet published.
	select {
	case <-parent.entered:
	case <-time.After(20 * time.Second):
		t.Fatal("Run never reached context.WithCancel")
	}
	if got := countGoroutines(t, bridgeGoroutineMarker); got != bridgeBaseline {
		t.Fatalf("bridge goroutines = %d, baseline %d while Run is parked: the parent must not be bridged yet", got, bridgeBaseline)
	}

	// The whole one-shot teardown completes while Run is parked, so shutdown
	// can never observe a run context to cancel.
	ctrl.Stop()
	if !shutdownHasStarted(ctrl) || !ctrl.queue.ShuttingDown() {
		t.Fatal("Stop did not publish the teardown")
	}
	if cancel, started := runContextState(ctrl); cancel != nil || started {
		t.Fatalf("teardown saw a published run context (cancel=%p, started=%v)", cancel, started)
	}

	// Hold the lifecycle lock so Run stops right after context.WithCancel
	// returned, with its child context still live: that is what keeps the bridge
	// goroutine observable instead of making this assertion a race.
	ctrl.startMu.Lock()
	parent.Release()
	if got := waitForGoroutineCount(t, bridgeGoroutineMarker, bridgeBaseline+1, 15*time.Second); got != bridgeBaseline+1 {
		ctrl.startMu.Unlock()
		t.Fatalf("bridge goroutines = %d, want %d: the run context must be bridged while it is alive", got, bridgeBaseline+1)
	}
	// Run is blocked on startMu here, so the child context cannot have been
	// released yet.
	ctrl.startMu.Unlock()

	var err error
	select {
	case err = <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("Run did not return after the gated parent was released")
	}
	if err == nil || !strings.Contains(err.Error(), "already been shut down") {
		t.Fatalf("Run = %v, want an already-shut-down error", err)
	}

	// Nothing may be published after shutdown: a cancel stored in c.cancel at
	// this point could never be invoked again, because shutdown already ran.
	if cancel, started := runContextState(ctrl); cancel != nil || started {
		t.Errorf("Run left a published run context behind (cancel=%p, started=%v)", cancel, started)
	}
	if got := atomic.LoadInt32(&readyCalls); got != 0 {
		t.Errorf("OnReady ran %d times, want 0: a stopped controller must never become ready", got)
	}
	if state := shutdownStateOf(ctrl); !state.closed || state.started != 0 {
		t.Errorf("state after the race = %+v, want closed with no started manager set", state)
	}

	// The bridge goroutine ends only when the child context it watches is
	// cancelled, so reaching the baseline proves the run context was released on
	// this failing return path.
	if got := waitForGoroutineCount(t, bridgeGoroutineMarker, bridgeBaseline, 15*time.Second); got != bridgeBaseline {
		t.Fatalf("bridge goroutines = %d, want %d: the run context was never cancelled", got, bridgeBaseline)
	}
	assertNoLeakedGoroutines(t, queueBaseline, informerBaseline, workerBaseline)
}

// ---------------------------------------------------------------------------
// Normal cancellation and repeated/concurrent Stop
// ---------------------------------------------------------------------------

// TestControllerContextCancelTearsDownQueue covers the normal exit path: the
// caller cancels the run context, Run returns nil, and both the queue and the
// manager sets are torn down.
func TestControllerContextCancelTearsDownQueue(t *testing.T) {
	queueBaseline := stableGoroutineCount(t, "waitingLoop")
	informerBaseline := stableGoroutineCount(t, "(*Reflector)")
	workerBaseline := stableGoroutineCount(t, "processLoop")

	ctrl := mustNewController(t, Config{
		DynamicClient:                newDualGroupFakeClient(t),
		Namespace:                    "kube-system",
		ComputeDomainCliqueNamespace: "default",
	})
	ready := make(chan struct{}, 1)
	ctrl.OnReady = func() { ready <- struct{}{} }

	done, cancel := runControllerAsync(ctrl)
	defer cancel()
	select {
	case <-ready:
	case <-time.After(20 * time.Second):
		t.Fatal("controller did not become ready with both groups served")
	}
	if state := shutdownStateOf(ctrl); state.closed || state.started != 2 {
		t.Fatalf("state while running = %+v, want open with both manager sets started", state)
	}
	assertGoroutinesRunning(t, queueBaseline, informerBaseline, workerBaseline)

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v after a normal cancellation, want nil", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Run did not return after the context was cancelled")
	}
	if !ctrl.queue.ShuttingDown() {
		t.Error("the shared callback queue must be shut down after a normal cancellation")
	}
	for _, manager := range ctrl.startedManagers {
		if manager.cancelContext != nil {
			t.Errorf("%s manager set was not stopped after a normal cancellation",
				manager.config.Contract.APIGroup)
		}
	}

	// A shut down controller cannot be run again: the queue is gone for good.
	if err := ctrl.Run(context.Background(), 1); err == nil ||
		!strings.Contains(err.Error(), "already been shut down") {
		t.Errorf("Run after shutdown = %v, want an already-shut-down error", err)
	}

	assertNoLeakedGoroutines(t, queueBaseline, informerBaseline, workerBaseline)
}

// TestControllerConcurrentRepeatedStopTearsDownBothGroups fires many concurrent
// Stop calls at a running controller: the single teardown must neither panic nor
// run twice, and both manager sets must end up stopped.
func TestControllerConcurrentRepeatedStopTearsDownBothGroups(t *testing.T) {
	queueBaseline := stableGoroutineCount(t, "waitingLoop")
	informerBaseline := stableGoroutineCount(t, "(*Reflector)")
	workerBaseline := stableGoroutineCount(t, "processLoop")

	ctrl := mustNewController(t, Config{
		DynamicClient:                newDualGroupFakeClient(t),
		Namespace:                    "kube-system",
		ComputeDomainCliqueNamespace: "default",
	})
	ready := make(chan struct{}, 1)
	ctrl.OnReady = func() { ready <- struct{}{} }

	done, cancel := runControllerAsync(ctrl)
	defer cancel()
	select {
	case <-ready:
	case <-time.After(20 * time.Second):
		t.Fatal("controller did not become ready with both groups served")
	}
	assertGoroutinesRunning(t, queueBaseline, informerBaseline, workerBaseline)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				ctrl.Stop()
			}
		}()
	}
	wg.Wait()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v after concurrent Stop calls, want nil", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Run did not return after concurrent Stop calls")
	}

	if !ctrl.queue.ShuttingDown() {
		t.Error("the shared callback queue must be shut down after concurrent Stop calls")
	}
	state := shutdownStateOf(ctrl)
	if !state.closed || state.started != 2 {
		t.Errorf("state after concurrent Stop = %+v, want closed with both manager sets", state)
	}
	for _, manager := range ctrl.startedManagers {
		if manager.cancelContext != nil {
			t.Errorf("%s manager set was not stopped: its run context is still live",
				manager.config.Contract.APIGroup)
		}
	}

	assertNoLeakedGoroutines(t, queueBaseline, informerBaseline, workerBaseline)
}

// ---------------------------------------------------------------------------
// Shutdown vs. a manager set whose Start succeeded but was not published yet
// ---------------------------------------------------------------------------

// firstCallGate parks the first caller and lets every later caller through. It is
// the deterministic rendezvous of the two seam tests below: the test drives the
// interleaving by hand instead of waiting for it, and Release is idempotent so a
// failing test is always unwound from a defer.
type firstCallGate struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	leaving sync.Once
}

func newFirstCallGate() *firstCallGate {
	return &firstCallGate{entered: make(chan struct{}), release: make(chan struct{})}
}

// Enter reports the first entry and blocks it until Release.
func (g *firstCallGate) Enter() {
	first := false
	g.once.Do(func() {
		first = true
		close(g.entered)
	})
	if first {
		<-g.release
	}
}

func (g *firstCallGate) Release() {
	g.leaving.Do(func() { close(g.release) })
}

// waitForShutdownStarted waits until one Stop call published the shutdown. It
// polls a channel that only closes once, so it waits for a decision that is
// already determined instead of relying on a timing window.
func waitForShutdownStarted(t *testing.T, ctrl *Controller) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !shutdownHasStarted(ctrl) {
		if time.Now().After(deadline) {
			t.Fatal("no Stop call reached the shutdown path")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// assertTeardownStillInFlight asserts that not all Stop calls returned. It is
// called only at points where the teardown is provably blocked inside a manager
// Stop, so a completed AllReturned would mean Stop returned while a manager set
// was still being torn down - the premature return these tests exist to catch.
func assertTeardownStillInFlight(t *testing.T, allReturned <-chan struct{}) {
	t.Helper()
	select {
	case <-allReturned:
		t.Fatal("Controller.Stop returned while a manager set was still being stopped")
	case <-time.After(150 * time.Millisecond):
	}
}

// TestControllerStopWaitsForSuccessfulManagerStop is the regression test for the
// start/publish window: a manager set whose Start succeeded but whose outcome had
// not been published when shutdown began. beforeStartPublish parks that start
// while starting is still elevated, so the ordering is produced by handshakes.
//
// The shape this fix replaced decremented starting and broadcast first and only
// then stopped the manager locally, so shutdown could snapshot the inventory
// (which did not contain the manager) and return while the local Stop was still
// running. Now the successful manager is recorded in the same critical section
// that decrements starting and shutdown owns the Stop, so the test observes
// manager.Stop blocked and every Controller.Stop call blocked with it.
func TestControllerStopWaitsForSuccessfulManagerStop(t *testing.T) {
	queueBaseline := stableGoroutineCount(t, "waitingLoop")
	informerBaseline := stableGoroutineCount(t, "(*Reflector)")
	workerBaseline := stableGoroutineCount(t, "processLoop")

	ctrl := mustNewController(t, Config{
		DynamicClient:                newDualGroupFakeClient(t),
		Namespace:                    "kube-system",
		ComputeDomainCliqueNamespace: "default",
	})

	publishGate := newFirstCallGate()
	defer publishGate.Release()
	ctrl.beforeStartPublish = publishGate.Enter

	stopGate := newFirstCallGate()
	defer stopGate.Release()
	var stopCalls atomic.Int32
	ctrl.managers[0].beforeStop = func() {
		stopCalls.Add(1)
		stopGate.Enter()
	}
	var readyCalls atomic.Int32
	ctrl.OnReady = func() { readyCalls.Add(1) }
	done, cancel := runControllerAsync(ctrl)
	defer cancel()

	// The first manager set has started successfully, but its outcome is not
	// published yet: starting is elevated and the inventory is still empty.
	select {
	case <-publishGate.entered:
	case <-time.After(20 * time.Second):
		t.Fatal("the first manager set never reached the publish point")
	}
	if state := shutdownStateOf(ctrl); state.closed || state.started != 0 {
		t.Fatalf("state while the first start is unpublished = %+v, want open with an empty inventory", state)
	}

	// Shutdown arrives in exactly that window and must wait for the start to
	// publish. The start is still parked, so nothing may return yet.
	const stoppers = 4
	var stopWG sync.WaitGroup
	allReturned := make(chan struct{})
	for i := 0; i < stoppers; i++ {
		stopWG.Add(1)
		go func() {
			defer stopWG.Done()
			ctrl.Stop()
		}()
	}
	go func() {
		stopWG.Wait()
		close(allReturned)
	}()
	waitForShutdownStarted(t, ctrl)
	assertTeardownStillInFlight(t, allReturned)

	// Let the start publish. shutdown now finds the manager in the inventory and
	// stops it; the manager's Stop is parked in the seam.
	publishGate.Release()
	select {
	case <-stopGate.entered:
	case <-time.After(20 * time.Second):
		t.Fatal("shutdown never stopped the manager set that had just started")
	}
	// The decisive assertion: manager.Stop has begun but not finished, so
	// Controller.Stop must still be blocked.
	assertTeardownStillInFlight(t, allReturned)

	stopGate.Release()
	select {
	case <-allReturned:
	case <-time.After(20 * time.Second):
		t.Fatal("Controller.Stop calls did not return after the manager teardown finished")
	}

	var err error
	select {
	case err = <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("Run did not return after the shutdown")
	}
	if err == nil || !strings.Contains(err.Error(), "shut down while the ComputeDomain manager") {
		t.Fatalf("Run = %v, want a shut-down-while-starting error", err)
	}
	if got := readyCalls.Load(); got != 0 {
		t.Errorf("OnReady ran %d times, want 0: a stopped controller must never become ready", got)
	}
	if got := stopCalls.Load(); got != 1 {
		t.Errorf("manager Stop calls = %d, want exactly 1: shutdown must own the single Stop", got)
	}
	state := shutdownStateOf(ctrl)
	if !state.closed || state.started != 1 {
		t.Errorf("state after the race = %+v, want closed with the one started manager set", state)
	}
	if ctrl.managers[0].cancelContext != nil {
		t.Error("the started manager set was not stopped: its run context is still live")
	}

	assertNoLeakedGoroutines(t, queueBaseline, informerBaseline, workerBaseline)
}

// TestControllerStopWaitsForFailedManagerStartCleanup covers the other half of
// the same protocol: a manager set whose Start fails after partially initialising
// itself. ComputeDomainManager.Start stops what it created before returning the
// error, and that cleanup happens-before the starting decrement, so Stop cannot
// return while it is in progress. The manager must also not be recorded in the
// inventory, otherwise shutdown would stop it a second time.
func TestControllerStopWaitsForFailedManagerStartCleanup(t *testing.T) {
	queueBaseline := stableGoroutineCount(t, "waitingLoop")
	informerBaseline := stableGoroutineCount(t, "(*Reflector)")
	workerBaseline := stableGoroutineCount(t, "processLoop")

	// The Aliyun ComputeDomain list is refused, so the first manager set fails to
	// sync its cache after creating its context, informer and mutation cache.
	ctrl := newControllerWithMissingResource(t, cdapi.GroupAliyun, cdapi.ComputeDomainResource)

	stopGate := newFirstCallGate()
	defer stopGate.Release()
	var stopCalls atomic.Int32
	ctrl.managers[0].beforeStop = func() {
		stopCalls.Add(1)
		stopGate.Enter()
	}

	done, cancel := runControllerAsync(ctrl)
	defer cancel()

	// The failing start is inside its own cleanup, still holding starting up.
	select {
	case <-stopGate.entered:
	case <-time.After(20 * time.Second):
		t.Fatal("the failing manager set never began its cleanup")
	}

	const stoppers = 4
	var stopWG sync.WaitGroup
	allReturned := make(chan struct{})
	for i := 0; i < stoppers; i++ {
		stopWG.Add(1)
		go func() {
			defer stopWG.Done()
			ctrl.Stop()
		}()
	}
	go func() {
		stopWG.Wait()
		close(allReturned)
	}()
	waitForShutdownStarted(t, ctrl)
	assertTeardownStillInFlight(t, allReturned)

	stopGate.Release()
	select {
	case <-allReturned:
	case <-time.After(20 * time.Second):
		t.Fatal("Controller.Stop calls did not return after the failed cleanup completed")
	}

	var err error
	select {
	case err = <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("Run did not return after the failed startup")
	}
	if err == nil || !strings.Contains(err.Error(), "cache sync failed") {
		t.Fatalf("Run = %v, want the bounded cache sync failure", err)
	}
	if got := stopCalls.Load(); got != 1 {
		t.Errorf("manager Stop calls = %d, want exactly 1: a failed start must not be stopped twice", got)
	}
	if state := shutdownStateOf(ctrl); !state.closed || state.started != 0 {
		t.Errorf("state after a failed startup = %+v, want closed with an empty inventory", state)
	}

	assertNoLeakedGoroutines(t, queueBaseline, informerBaseline, workerBaseline)
}
