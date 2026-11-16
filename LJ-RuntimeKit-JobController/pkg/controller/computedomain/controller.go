package computedomain

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	v1beta1 "github.com/lj-runtimekit/job-controller/pkg/apis/resource/v1beta1"
	cdapi "github.com/lj-runtimekit/job-controller/pkg/computedomain"
	"github.com/lj-runtimekit/job-controller/pkg/constants"
)

// Group-independent GVRs. ComputeDomain and ComputeDomainClique GVRs are
// per-API-group and live on computedomain.GroupContract instead of package
// globals, so no runtime code can switch the served group.
var (
	nodeGVR                  = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "nodes"}
	resourceClaimTemplateGVR = schema.GroupVersionResource{Group: "resource.k8s.io", Version: "v1", Resource: "resourceclaimtemplates"}
)

// Config holds the public controller configuration. KubeClient is retained for
// entry-point compatibility; all controller resource operations use DynamicClient.
type Config struct {
	KubeClient                   kubernetes.Interface
	DynamicClient                dynamic.Interface
	Namespace                    string // leader election namespace from --namespace; the Lease itself is created by the entry point (default kube-system)
	ComputeDomainCliqueNamespace string // CDClique informer, Get/List cache, live List, Delete (default "default")
	MaxNodes                     int
	IMEXIsolation                string
	IMEXConfigOverrides          string
}

// ClientSets groups clients shared by all managers.
type ClientSets struct {
	Dynamic dynamic.Interface
}

// ManagerConfig is the common configuration shared by all managers. Exactly one
// GroupContract is bound per manager set: every group-dependent value (GVRs,
// labels, finalizer, opaque apiVersion) is read from it and never from a global.
type ManagerConfig struct {
	Contract                     cdapi.GroupContract
	DriverName                   string
	ComputeDomainCliqueNamespace string // default: CDClique informer, cache, live List, Delete
	MaxNodesPerIMEXDomain        int
	IMEXIsolation                string
	ClientSets                   ClientSets
	WorkQueue                    *CallbackWorkQueue

	// CacheSyncTimeout bounds the initial informer cache sync. A missing CRD or
	// missing list/watch permission makes the informer retry forever, so the wait
	// is bounded to turn that into a startup failure. Zero means
	// constants.CacheSyncTimeout.
	CacheSyncTimeout time.Duration
}

type workItem struct {
	key      string
	object   interface{}
	callback func(context.Context, interface{}) error
}

// CallbackWorkQueue serializes manager mutations and retries failed callbacks.
//
// The backing client-go rate limiting queue starts its waitingLoop goroutine as
// soon as it is constructed, so the queue MUST be shut down on every controller
// exit path (see Controller.shutdown). ShutDown is idempotent.
type CallbackWorkQueue struct {
	mu        sync.Mutex
	queue     workqueue.TypedRateLimitingInterface[interface{}]
	activeOps map[string]*workItem

	shutOnce sync.Once
	stopped  chan struct{}
}

func newCallbackWorkQueue() *CallbackWorkQueue {
	return &CallbackWorkQueue{
		queue:     workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[interface{}]()),
		activeOps: make(map[string]*workItem),
		stopped:   make(chan struct{}),
	}
}

// ShutDown stops the queue: the client-go waitingLoop goroutine exits, queued
// items are dropped and every consumer blocked in Get returns immediately. It is
// safe to call concurrently and any number of times.
func (q *CallbackWorkQueue) ShutDown() {
	q.shutOnce.Do(func() {
		close(q.stopped)
		q.queue.ShutDown()
	})
}

// ShuttingDown reports whether ShutDown has been called.
func (q *CallbackWorkQueue) ShuttingDown() bool {
	return q.queue.ShuttingDown()
}

func (q *CallbackWorkQueue) Enqueue(obj interface{}, callback func(context.Context, interface{}) error) {
	q.EnqueueWithKey(obj, "", callback)
}

func (q *CallbackWorkQueue) EnqueueWithKey(obj interface{}, key string, callback func(context.Context, interface{}) error) {
	if runtimeObj, ok := obj.(runtime.Object); ok {
		obj = runtimeObj.DeepCopyObject()
	}
	item := &workItem{key: key, object: obj, callback: callback}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.queue.ShuttingDown() {
		// Never grow activeOps after shutdown: those entries would never be
		// drained again.
		return
	}
	if key != "" {
		q.activeOps[key] = item
	}
	q.queue.Add(item)
}

// Run drains the queue until it is shut down. The shutdown is driven by the
// caller (Controller.shutdown always calls ShutDown explicitly); the watcher
// below only mirrors a cancelled context and exits as soon as either signal
// arrives, so it cannot leak when the shutdown comes from somewhere else.
func (q *CallbackWorkQueue) Run(ctx context.Context) {
	go func() {
		select {
		case <-ctx.Done():
			q.ShutDown()
		case <-q.stopped:
		}
	}()
	for q.processNext(ctx) {
	}
	q.ShutDown()
}

func (q *CallbackWorkQueue) processNext(ctx context.Context) bool {
	item, shutdown := q.queue.Get()
	if shutdown {
		return false
	}
	defer q.queue.Done(item)
	work, ok := item.(*workItem)
	if !ok {
		q.queue.Forget(item)
		return true
	}
	if work.callback == nil {
		q.queue.Forget(item)
		return true
	}
	if err := work.callback(ctx, work.object); err != nil {
		klog.V(1).InfoS("Manager callback will be retried", "key", work.key, "attempt", q.queue.NumRequeues(item), "error", err)
		q.mu.Lock()
		newer := work.key != "" && q.activeOps[work.key] != nil && q.activeOps[work.key] != work
		q.mu.Unlock()
		if newer {
			q.queue.Forget(item)
		} else {
			q.queue.AddRateLimited(item)
		}
		return true
	}
	q.mu.Lock()
	if work.key != "" && q.activeOps[work.key] == work {
		delete(q.activeOps, work.key)
	}
	q.mu.Unlock()
	q.queue.Forget(item)
	return true
}

// Controller owns the shared queue, one Lease and one manager set per served
// ComputeDomain API group. Both groups are always served; the manager sets are
// fully isolated (own informers, caches, indexers and mutation caches); only the
// callback queue and the leader election Lease are shared.
type Controller struct {
	config    Config
	contracts []cdapi.GroupContract
	managers  []*ComputeDomainManager
	queue     *CallbackWorkQueue

	// OnReady is called at most once, and only when readiness was actually
	// published: every manager set has synced its informer caches, the
	// controller is not shut down and the run context is still live. It is
	// never called if startup fails or if a Stop won the readiness race. Set by
	// the adapter/factory before Run is called. May be nil.
	//
	// It runs synchronously from Run while the lifecycle lock is held, which is
	// what makes "readiness published" and "shutdown started" mutually
	// exclusive (see publishReady). It must therefore return promptly and must
	// not call Stop or Run - doing so from this goroutine would self-deadlock on
	// that lock. Trigger shutdown from another goroutine instead.
	OnReady func()

	mu      sync.Mutex
	cancel  context.CancelFunc
	started bool

	// startMu is the controller lifecycle lock. It is never held across
	// manager.Start: a cache sync can block for a long time and Stop must be able
	// to interrupt it. Instead, shutdown closes the controller and waits on
	// startCond until every in-flight start published its decision, so a manager
	// can neither start after the inventory was taken nor be forgotten by it.
	//
	// startMu is also the single linearization domain of the shutdown readiness
	// protocol: publishing closed, publishing the shutdownStarted signal,
	// cancelling the run context, shutting the shared queue down and publishing
	// readiness (publishReady) all happen inside this one critical section. A
	// concurrent Stop therefore cannot make ANY part of the teardown observable
	// while a readiness callback holds the lock. closed is written and read under
	// startMu only.
	startMu         sync.Mutex
	startCond       *sync.Cond
	starting        int
	closed          bool
	ready           bool
	startedManagers []*ComputeDomainManager

	shutdownOnce sync.Once

	// shutdownStarted is closed inside the same startMu critical section that
	// publishes closed, i.e. it is the externally observable form of "shutdown
	// started". It exists so the linearization point can be observed without
	// taking startMu (which a readiness callback may hold): after receiving from
	// it, closed is already true and the teardown is under way.
	shutdownStarted chan struct{}

	// beforeReady, when non-nil, runs after the last manager set published its
	// start and before readiness is published. It is a test seam that lets the
	// "last manager started" / "readiness published" interleaving be driven
	// deterministically instead of with sleeps. Production code never sets it.
	beforeReady func()

	// beforeShutdownLock, when non-nil, runs at the very start of the one-time
	// teardown, before shutdown contends for startMu. It is a test seam that
	// proves the Stop goroutine reached the shutdown path and is about to wait
	// for the lifecycle lock. Production code never sets it.
	beforeShutdownLock func()

	// beforeStartPublish, when non-nil, runs inside startManager after the
	// manager set returned from Start and before its outcome is published while
	// c.starting is still elevated. It is a test seam that parks a start in the
	// exact window the shutdown protocol has to cover: the manager has already
	// started (or already cleaned itself up after a failure) but the shutdown
	// inventory does not know about it yet. Production code never sets it.
	beforeStartPublish func()
}

// DefaultDriverNamespace is the safe default for Config.Namespace, the
// namespace the entry point uses for the leader election Lease. It MUST be
// non-empty to prevent the Lease lock from degrading to an invalid
// cluster-scoped name.
const DefaultDriverNamespace = "kube-system"

// DefaultCliqueNamespace is the safe default for
// Config.ComputeDomainCliqueNamespace (CDClique informer, cache, live List,
// Delete).
const DefaultCliqueNamespace = "default"

// NormalizeNamespaces fills empty Namespace / ComputeDomainCliqueNamespace
// with their safe defaults. It is called by NewController and can also be
// called from CLI validation paths to ensure empty values never reach
// namespaced informers or clients.
func NormalizeNamespaces(cfg *Config) {
	if cfg.Namespace == "" {
		cfg.Namespace = DefaultDriverNamespace
	}
	if cfg.ComputeDomainCliqueNamespace == "" {
		cfg.ComputeDomainCliqueNamespace = DefaultCliqueNamespace
	}
}

// NewController builds one isolated manager set per served ComputeDomain API
// group on top of a single shared callback queue. Both resource.aliyun.com and
// resource.nvidia.com are always served: there is no configuration knob that can
// enable, disable or default a group.
func NewController(config Config) *Controller {
	NormalizeNamespaces(&config)
	contracts := cdapi.SupportedContracts()
	queue := newCallbackWorkQueue()
	controller := &Controller{
		config:          config,
		contracts:       contracts,
		queue:           queue,
		shutdownStarted: make(chan struct{}),
	}
	controller.startCond = sync.NewCond(&controller.startMu)
	for _, contract := range contracts {
		managerConfig := &ManagerConfig{
			Contract:                     contract,
			DriverName:                   constants.DriverName,
			ComputeDomainCliqueNamespace: config.ComputeDomainCliqueNamespace,
			MaxNodesPerIMEXDomain:        config.MaxNodes,
			IMEXIsolation:                config.IMEXIsolation,
			ClientSets:                   ClientSets{Dynamic: config.DynamicClient},
			WorkQueue:                    queue,
		}
		controller.managers = append(controller.managers, NewComputeDomainManager(managerConfig))
	}
	return controller
}

// ServedAPIGroups returns the API groups this controller serves, in stable
// order. It is always every supported group.
func (c *Controller) ServedAPIGroups() []string {
	groups := make([]string, 0, len(c.contracts))
	for _, contract := range c.contracts {
		groups = append(groups, contract.APIGroup)
	}
	return groups
}

// Run starts every manager set, publishes readiness, runs the shared callback
// queue, and stops cleanly. Both API groups are hard startup dependencies: a
// cache sync failure in either group (missing CRD, missing list/watch
// permission, indexer error) fails startup, so a partially ready controller is
// never reported as Ready. Readiness is published through publishReady, which
// competes with shutdown for the same critical section: a controller that was
// stopped while its last manager set was starting returns an error instead of
// calling OnReady or draining the queue. workers is retained for compatibility;
// callback execution is intentionally serialized.
func (c *Controller) Run(ctx context.Context, workers int) error {
	if workers != 1 {
		klog.V(2).InfoS("Callback work queue uses one serialized worker", "requestedWorkers", workers)
	}
	// The child context is released by the defer below on every return path,
	// including the paths that fail before it is ever published. Without it a
	// Stop that completed while this goroutine was creating the context would
	// leak it: the one-shot shutdown already ran (c.cancel was nil), so the
	// deferred shutdown could not cancel it, and c.cancel would be cleared on
	// return without ever being invoked.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// The controller is single-use: NewController starts the client-go
	// waitingLoop, and shutdown tears it down permanently. The closed check and
	// the publication of the run context share one startMu critical section, so
	// the two orders are exhaustive: either shutdown ran first and this call
	// publishes nothing (it must never publish a cancel that shutdown can no
	// longer invoke), or this call ran first and shutdown observes the context
	// in c.cancel and cancels it.
	c.startMu.Lock()
	if c.closed {
		c.startMu.Unlock()
		return fmt.Errorf("controller has already been shut down")
	}
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		c.startMu.Unlock()
		return fmt.Errorf("controller already started")
	}
	c.started = true
	c.cancel = cancel
	c.mu.Unlock()
	c.startMu.Unlock()
	defer func() {
		c.mu.Lock()
		c.started = false
		c.cancel = nil
		c.mu.Unlock()
	}()
	// Every exit path - normal cancellation, a manager that failed to start, a
	// concurrent or earlier Stop - funnels through the single idempotent
	// shutdown, so the shared queue is always shut down and no started manager
	// set is ever left running.
	defer c.shutdown()

	for _, manager := range c.managers {
		if err := c.startManager(ctx, manager); err != nil {
			return err
		}
	}
	// Readiness is published through the same linearization protocol as
	// shutdown. If shutdown already won, Run fails here (through the deferred
	// shutdown above) instead of reporting a stopped controller as ready and
	// instead of draining a queue that is already gone.
	if err := c.publishReady(ctx); err != nil {
		return err
	}
	klog.InfoS("ComputeDomain controller started", "apiGroups", c.ServedAPIGroups())
	c.queue.Run(ctx)
	return nil
}

// startManager starts one manager set and hands it to the shutdown inventory.
// The start itself runs without holding startMu so Stop stays responsive; the
// outcome is published under startMu in one critical section that both records
// the manager (on success) and decrements c.starting.
//
// Ordering inside that section is what makes Controller.Stop's return value
// meaningful, so it is not interchangeable:
//
//   - A manager set whose Start succeeded is appended to startedManagers BEFORE
//     starting is decremented. shutdown only snapshots the inventory after it
//     observed starting == 0 under the same lock, so a successful start can never
//     be missing from the snapshot: either this call published first and shutdown
//     stops the manager, or shutdown already published closed and this call still
//     records the manager for the same shutdown to stop. Decrementing (or
//     broadcasting) first and stopping locally afterwards, as an earlier revision
//     did, let Controller.Stop return while the manager was still tearing down.
//   - A manager set whose Start failed is deliberately NOT recorded:
//     ComputeDomainManager.Start stops whatever it partially initialised before
//     it returns the error, so shutdown must not stop it a second time. That
//     cleanup happens-before the decrement below, and shutdown cannot pass the
//     starting > 0 wait before it, so a concurrent Stop never returns while the
//     failed start is still being cleaned up either.
//
// Neither path stops the manager here: shutdown owns every Stop, which is what
// keeps "Controller.Stop returned" equivalent to "every manager set that could
// have started is stopped".
func (c *Controller) startManager(ctx context.Context, manager *ComputeDomainManager) error {
	c.startMu.Lock()
	if c.closed {
		c.startMu.Unlock()
		return fmt.Errorf("controller shut down before the ComputeDomain manager for API group %s started",
			manager.config.Contract.APIGroup)
	}
	c.starting++
	c.startMu.Unlock()

	startErr := manager.Start(ctx)
	if hook := c.beforeStartPublish; hook != nil {
		hook()
	}

	c.startMu.Lock()
	if startErr == nil {
		c.startedManagers = append(c.startedManagers, manager)
	}
	c.starting--
	if c.starting == 0 {
		c.startCond.Broadcast()
	}
	shutDown := c.closed
	c.startMu.Unlock()

	if startErr != nil {
		return fmt.Errorf("error starting ComputeDomain manager for API group %s: %w",
			manager.config.Contract.APIGroup, startErr)
	}
	if shutDown {
		return fmt.Errorf("controller shut down while the ComputeDomain manager for API group %s was starting",
			manager.config.Contract.APIGroup)
	}
	return nil
}

// publishReady publishes controller readiness inside the very same startMu
// critical section that shutdown uses for its closed transition, so the two can
// only be ordered, never interleaved:
//
//   - shutdown took the critical section first: closed is set and the run
//     context is already cancelled, so OnReady is never called and Run returns
//     an error instead of continuing into queue.Run.
//   - readiness took the critical section first: closed cannot be set until
//     this method returns, so no manager set is ever stopped while OnReady runs
//     and no cancellation can land underneath it.
//
// The checks are made at the linearization point, under the lock: the closed
// flag, the run context (a cancel that happens-before the lock acquisition is
// always observed) and the start inventory (both manager sets must have been
// published as started by startManager). Neither the queue nor the manager sets
// need a separate check: shutdown closes the queue and publishes closed inside
// one and the same critical section, so a queue that is closed always has closed
// published, and closed is the single authoritative state.
//
// OnReady is invoked while startMu is held; that is the ordering guarantee, and
// the price is the callback contract documented on Controller.OnReady (no Stop
// or Run from the callback's own goroutine, no blocking). The production wiring
// only closes a channel, and the readiness callback never re-enters shutdown, so
// no lock-order deadlock is possible between Run and Stop.
func (c *Controller) publishReady(ctx context.Context) error {
	if hook := c.beforeReady; hook != nil {
		hook()
	}

	c.startMu.Lock()
	defer c.startMu.Unlock()

	if c.closed {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("controller shut down before readiness was published: %w", ctxErr)
		}
		return errors.New("controller shut down before readiness was published")
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return fmt.Errorf("controller did not become ready: %w", ctxErr)
	}
	if len(c.startedManagers) != len(c.managers) {
		return fmt.Errorf("controller did not become ready: %d of %d ComputeDomain manager sets started",
			len(c.startedManagers), len(c.managers))
	}
	if c.ready {
		return errors.New("controller readiness was already published")
	}

	// Linearization point: from here on shutdown waits for this method (and
	// therefore for OnReady) to return before it can publish closed.
	c.ready = true
	if c.OnReady != nil {
		c.OnReady()
	}
	return nil
}

// shutdown performs the single idempotent controller teardown. It publishes the
// shutdown state, cancels the run context, shuts the shared callback queue down
// (releasing the client-go waitingLoop goroutine that starts with the queue,
// which is exactly the goroutine the previous failure path leaked) and stops
// every manager set that was successfully started, waiting for its goroutines.
//
// # Linearization
//
// Shutdown starts when it acquires startMu and sets closed; that is the same
// lock publishReady holds while it calls OnReady. Every teardown effect is
// published inside that one critical section, in this order:
//
//  1. closed = true                           (linearization point)
//  2. close(shutdownStarted)                  (lock-free observable form of 1)
//  3. cancel the run context
//  4. ShutDown the shared callback queue
//  5. wait for in-flight manager starts to publish their decision (including
//     the cleanup Start performs on failure and the recording of a successful
//     start in the inventory)
//  6. snapshot the start inventory, release the lock
//  7. stop every snapshotted manager set, outside the lock, before returning
//
// Consequently a readiness callback that already holds startMu finishes before
// any of 2-6 becomes observable: in particular the shared queue stays OPEN while
// OnReady runs, and no cancellation can land underneath it. The reverse order is
// equally strict: once shutdown published closed, readiness is never published
// again (see publishReady).
//
// # Blocking / lock order
//
// Shutting the queue down inside the critical section is safe: the queue's
// ShutDown only closes two channels, stops a ticker and takes the queue's own
// control mutex to broadcast on its cond, and every consumer waits on that cond
// in a way that releases the mutex, so it cannot block behind application work.
// The resulting lock order is startMu -> (mu) -> queue control mutex; the queue
// never calls back into the controller and CallbackWorkQueue.mu is never taken
// while startMu is held, so the order stays acyclic and Run/Stop cannot
// deadlock. startCond.Wait releases startMu, so in-flight startManager calls can
// always publish their decision while shutdown waits.
//
// It is safe to call from Run (via defer), from Stop, concurrently, and any
// number of times: sync.Once guarantees a single teardown, so nothing is closed
// twice and no manager is stopped twice. Because step 7 runs inside sync.Once,
// concurrent or repeated Stop calls block until the same teardown finished, so
// none of them can return while a manager set is still being stopped.
func (c *Controller) shutdown() {
	c.shutdownOnce.Do(func() {
		if hook := c.beforeShutdownLock; hook != nil {
			hook()
		}

		c.startMu.Lock()
		c.closed = true
		close(c.shutdownStarted)
		c.mu.Lock()
		cancel := c.cancel
		c.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		// Shutting the queue down inside this critical section is what keeps the
		// queue open for the whole duration of a readiness callback: it cannot
		// become observable before closed was published.
		c.queue.ShutDown()
		for c.starting > 0 {
			c.startCond.Wait()
		}
		started := append([]*ComputeDomainManager(nil), c.startedManagers...)
		c.startMu.Unlock()

		if err := stopManagers(started); err != nil {
			klog.ErrorS(err, "Failed to stop ComputeDomain manager sets during shutdown")
		}
	})
}

// stopManagers stops every started manager set in starts order and returns the
// first error. Managers stop their own informers, indexers and cleanup
// goroutines, so no goroutine outlives Run.
func stopManagers(managers []*ComputeDomainManager) error {
	var firstErr error
	for _, manager := range managers {
		if err := manager.Stop(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Stop requests controller shutdown and blocks until the shared queue and every
// started manager set are torn down. It is idempotent and safe to call before
// Run, during Run (including during a slow manager start) and repeatedly; Run
// performs the same teardown through the same sync.Once.
func (c *Controller) Stop() {
	c.shutdown()
}

func toComputeDomain(obj *unstructured.Unstructured) (*v1beta1.ComputeDomain, error) {
	cd := &v1beta1.ComputeDomain{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, cd); err != nil {
		return nil, err
	}
	return cd, nil
}

func fromComputeDomain(cd *v1beta1.ComputeDomain) (*unstructured.Unstructured, error) {
	object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(cd)
	if err != nil {
		return nil, err
	}
	return &unstructured.Unstructured{Object: object}, nil
}

func updateDynamic(ctx context.Context, client dynamic.Interface, gvr schema.GroupVersionResource, obj *unstructured.Unstructured, status bool) (*unstructured.Unstructured, error) {
	resource := client.Resource(gvr).Namespace(obj.GetNamespace())
	if status {
		return resource.UpdateStatus(ctx, obj, metav1.UpdateOptions{})
	}
	return resource.Update(ctx, obj, metav1.UpdateOptions{})
}

func resourceKey(obj metav1.Object) string {
	return fmt.Sprintf("%s/%s/%s", obj.GetNamespace(), obj.GetName(), obj.GetUID())
}

// queueKey qualifies a work item with its source API group. Both manager sets
// share one callback queue, so the key must carry the group: otherwise two
// synthetic objects with the same namespace/name/UID in different groups would
// coalesce and one group's retry could be dropped for the other.
func queueKey(contract cdapi.GroupContract, obj metav1.Object) string {
	return contract.APIGroup + "#" + resourceKey(obj)
}
