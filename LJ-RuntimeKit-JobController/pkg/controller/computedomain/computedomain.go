package computedomain

import (
	"context"
	"fmt"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	v1beta1 "github.com/lj-runtimekit/job-controller/pkg/apis/resource/v1beta1"
	"github.com/lj-runtimekit/job-controller/pkg/constants"
)

type GetComputeDomainFunc func(uid string) (*v1beta1.ComputeDomain, error)
type ListComputeDomainsFunc func() ([]*v1beta1.ComputeDomain, error)
type UpdateComputeDomainStatusFunc func(context.Context, *v1beta1.ComputeDomain) (*v1beta1.ComputeDomain, error)

// waitForInformerSync waits for one informer cache to sync, bounded by
// ManagerConfig.CacheSyncTimeout. Without the bound a missing CRD (or a denied
// list/watch) makes the reflector retry forever and the process would hang
// instead of failing startup, so both ComputeDomain API groups would never be
// enforced as hard startup dependencies.
func waitForInformerSync(ctx context.Context, config *ManagerConfig, resource string, hasSynced cache.InformerSynced) error {
	timeout := config.CacheSyncTimeout
	if timeout <= 0 {
		timeout = constants.CacheSyncTimeout
	}
	syncCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if !cache.WaitForCacheSync(syncCtx.Done(), hasSynced) {
		// Distinguish an aborted sync from a timeout: a shutdown cancel reaches
		// this point immediately, and reporting the full timeout there would
		// hide the real cause (the run context was cancelled).
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("%s informer cache sync for API group %s was aborted: %w",
				resource, config.Contract.APIGroup, ctxErr)
		}
		return fmt.Errorf("%s informer cache sync failed for API group %s after %s: the CRD may be missing or list/watch may be denied",
			resource, config.Contract.APIGroup, timeout)
	}
	return nil
}

// ComputeDomainManager owns the top-level informer and child managers.
type ComputeDomainManager struct {
	config *ManagerConfig

	// beforeStop, when non-nil, runs at the very start of every Stop call. It is
	// a test seam that makes "this manager set is being torn down right now"
	// observable, so a test can prove Controller.Stop does not return before the
	// teardown finished. Production code never sets it.
	beforeStop func()

	waitGroup     sync.WaitGroup
	cancelContext context.CancelFunc
	factory       dynamicinformer.DynamicSharedInformerFactory
	informer      cache.SharedIndexInformer
	mutationCache cache.MutationCache

	resourceClaimTemplateManager *WorkloadResourceClaimTemplateManager
	nodeManager                  *NodeManager
	cliqueManager                *ComputeDomainCliqueManager
	statusManager                *ComputeDomainStatusManager
}

// NewComputeDomainManager builds one isolated manager set (informer, indexer and
// mutation cache) for the single API group carried by config.Contract.
func NewComputeDomainManager(config *ManagerConfig) *ComputeDomainManager {
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(
		config.ClientSets.Dynamic,
		constants.InformerResyncPeriod,
		metav1.NamespaceAll,
		nil,
	)
	manager := &ComputeDomainManager{
		config:   config,
		factory:  factory,
		informer: factory.ForResource(config.Contract.ComputeDomainGVR).Informer(),
	}
	manager.resourceClaimTemplateManager = NewWorkloadResourceClaimTemplateManager(config, manager.Get, manager.LiveConfirmAbsent, manager.LiveListUIDs)
	manager.nodeManager = NewNodeManager(config, manager.Get, manager.LiveConfirmAbsent, manager.LiveListUIDs)
	manager.cliqueManager = NewComputeDomainCliqueManager(config)
	manager.statusManager = NewComputeDomainStatusManager(manager.cliqueManager, manager.List, manager.UpdateStatus)
	return manager
}

func (m *ComputeDomainManager) Start(ctx context.Context) (returnErr error) {
	ctx, cancel := context.WithCancel(ctx)
	m.cancelContext = cancel
	defer func() {
		if returnErr != nil {
			_ = m.Stop()
		}
	}()
	if err := m.informer.AddIndexers(cache.Indexers{indexByUID: uidIndexer}); err != nil {
		return fmt.Errorf("add ComputeDomain UID indexer: %w", err)
	}
	m.mutationCache = newComputeDomainMutationCache(m.informer.GetStore(), m.informer.GetIndexer())
	if _, err := m.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if u := unstructuredObject(obj); u != nil {
				m.config.WorkQueue.EnqueueWithKey(u, queueKey(m.config.Contract, u), m.onAddOrUpdate)
			}
		},
		UpdateFunc: func(_, obj interface{}) {
			if u := unstructuredObject(obj); u != nil {
				m.config.WorkQueue.EnqueueWithKey(u, queueKey(m.config.Contract, u), m.onAddOrUpdate)
			}
		},
		DeleteFunc: func(obj interface{}) {
			if u := unstructuredObject(obj); u != nil {
				ForgetComputeDomain(m.metricKey(string(u.GetUID())))
			}
		},
	}); err != nil {
		return fmt.Errorf("add ComputeDomain event handler: %w", err)
	}
	m.waitGroup.Add(1)
	go func() {
		defer m.waitGroup.Done()
		m.factory.Start(ctx.Done())
		<-ctx.Done()
	}()
	if err := waitForInformerSync(ctx, m.config, "ComputeDomain", m.informer.HasSynced); err != nil {
		return err
	}
	if err := m.resourceClaimTemplateManager.Start(ctx); err != nil {
		return fmt.Errorf("start workload ResourceClaimTemplate manager: %w", err)
	}
	if err := m.nodeManager.Start(ctx); err != nil {
		return fmt.Errorf("start Node manager: %w", err)
	}
	if err := m.cliqueManager.Start(ctx); err != nil {
		return fmt.Errorf("start ComputeDomainClique manager: %w", err)
	}
	if err := m.statusManager.Start(ctx); err != nil {
		return fmt.Errorf("start ComputeDomain status manager: %w", err)
	}
	return nil
}

func (m *ComputeDomainManager) Stop() error {
	if m.beforeStop != nil {
		m.beforeStop()
	}
	var firstErr error
	if m.statusManager != nil {
		if err := m.statusManager.Stop(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if m.cliqueManager != nil {
		if err := m.cliqueManager.Stop(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if m.resourceClaimTemplateManager != nil {
		if err := m.resourceClaimTemplateManager.Stop(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if m.nodeManager != nil {
		if err := m.nodeManager.Stop(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if m.cancelContext != nil {
		m.cancelContext()
		m.cancelContext = nil
	}
	m.waitGroup.Wait()
	return firstErr
}

func newComputeDomainMutationCache(backing cache.Store, indexer cache.Indexer) cache.MutationCache {
	// ComputeDomainManager updates existing ComputeDomains but never creates
	// them. Excluding mutation-only additions prevents a deleted CD from
	// remaining visible until the mutation TTL expires.
	return cache.NewIntegerResourceVersionMutationCache(
		klog.Background(), backing, indexer, constants.MutationCacheTTL, false,
	)
}

func (m *ComputeDomainManager) Get(uid string) (*v1beta1.ComputeDomain, error) {
	objects, err := m.mutationCache.ByIndex(indexByUID, uid)
	if err != nil {
		return nil, fmt.Errorf("get ComputeDomain by UID: %w", err)
	}
	matches := make([]*unstructured.Unstructured, 0, len(objects))
	for _, obj := range objects {
		u, ok := obj.(*unstructured.Unstructured)
		if !ok {
			return nil, fmt.Errorf("unexpected ComputeDomain cache object %T", obj)
		}
		// ByIndex obtains keys and objects in separate steps. A same-name
		// recreation can replace the object between those steps, so re-check
		// the immutable UID before returning it to deletion/finalizer paths.
		if string(u.GetUID()) != uid {
			continue
		}
		matches = append(matches, u)
	}
	if len(matches) == 0 {
		return nil, nil
	}
	if len(matches) != 1 {
		return nil, fmt.Errorf("multiple ComputeDomains have UID %s", uid)
	}
	return toComputeDomain(matches[0].DeepCopy())
}

// hasComputeDomain reports whether uid still exists in the informer-backed
// ComputeDomain cache. The mutation cache excludes additions, so its index keys
// come from the informer store; Get also revalidates the immutable UID.
func (m *ComputeDomainManager) hasComputeDomain(uid string) bool {
	cd, err := m.Get(uid)
	return err == nil && cd != nil
}

// LiveConfirmAbsent performs a live API List across all namespaces to confirm
// that no ComputeDomain with the given UID exists. This is used by cleanup
// managers to avoid false-positive orphan detection caused by cache misses.
func (m *ComputeDomainManager) LiveConfirmAbsent(ctx context.Context, uid string) (bool, error) {
	list, err := m.config.ClientSets.Dynamic.Resource(m.config.Contract.ComputeDomainGVR).Namespace("").List(
		ctx, metav1.ListOptions{},
	)
	if err != nil {
		return false, err
	}
	for _, item := range list.Items {
		if string(item.GetUID()) == uid {
			return false, nil // still exists
		}
	}
	return true, nil // confirmed absent
}

// LiveListUIDs performs a single live API List across all namespaces and returns
// the set of all existing ComputeDomain UIDs. Used by CleanupManager to
// batch-check orphans with one API call per cleanup round.
func (m *ComputeDomainManager) LiveListUIDs(ctx context.Context) (map[string]struct{}, error) {
	list, err := m.config.ClientSets.Dynamic.Resource(m.config.Contract.ComputeDomainGVR).Namespace("").List(
		ctx, metav1.ListOptions{},
	)
	if err != nil {
		return nil, err
	}
	uids := make(map[string]struct{}, len(list.Items))
	for _, item := range list.Items {
		uids[string(item.GetUID())] = struct{}{}
	}
	return uids, nil
}

func (m *ComputeDomainManager) List() ([]*v1beta1.ComputeDomain, error) {
	objects := m.informer.GetStore().List()
	result := make([]*v1beta1.ComputeDomain, 0, len(objects))
	for _, obj := range objects {
		u, ok := obj.(*unstructured.Unstructured)
		if !ok {
			continue
		}
		cd, err := toComputeDomain(u)
		if err != nil {
			return nil, err
		}
		result = append(result, cd)
	}
	return result, nil
}

func (m *ComputeDomainManager) UpdateStatus(ctx context.Context, cd *v1beta1.ComputeDomain) (*v1beta1.ComputeDomain, error) {
	copy := cd.DeepCopy()
	copy.Status.Status = m.calculateGlobalStatus(copy)
	u, err := fromComputeDomain(copy)
	if err != nil {
		return nil, err
	}
	updated, err := updateDynamic(ctx, m.config.ClientSets.Dynamic, m.config.Contract.ComputeDomainGVR, u, true)
	if err != nil {
		return nil, err
	}
	m.mutationCache.Mutation(updated)
	result, err := toComputeDomain(updated)
	if err != nil {
		return nil, err
	}
	uid := string(result.UID)
	ObserveComputeDomainStatus(m.metricKey(uid), result.Status.Status, func(string) bool { return m.hasComputeDomain(uid) })
	return result, nil
}

func (m *ComputeDomainManager) update(ctx context.Context, cd *v1beta1.ComputeDomain) (*v1beta1.ComputeDomain, error) {
	u, err := fromComputeDomain(cd)
	if err != nil {
		return nil, err
	}
	updated, err := updateDynamic(ctx, m.config.ClientSets.Dynamic, m.config.Contract.ComputeDomainGVR, u, false)
	if err != nil {
		return nil, err
	}
	m.mutationCache.Mutation(updated)
	return toComputeDomain(updated)
}

func (m *ComputeDomainManager) addFinalizer(ctx context.Context, cd *v1beta1.ComputeDomain) error {
	for _, finalizer := range cd.Finalizers {
		if finalizer == m.config.Contract.Finalizer {
			return nil
		}
	}
	copy := cd.DeepCopy()
	copy.Finalizers = append(copy.Finalizers, m.config.Contract.Finalizer)
	_, err := m.update(ctx, copy)
	return err
}

func (m *ComputeDomainManager) removeFinalizer(ctx context.Context, uid string) error {
	cd, err := m.Get(uid)
	if err != nil || cd == nil {
		return err
	}
	if string(cd.UID) != uid {
		return fmt.Errorf("refusing to remove ComputeDomain finalizer: UID changed from %s to %s", uid, cd.UID)
	}
	if cd.DeletionTimestamp == nil {
		return fmt.Errorf("attempting to remove finalizer before ComputeDomain deletion")
	}
	copy := cd.DeepCopy()
	copy.Finalizers = removeString(copy.Finalizers, m.config.Contract.Finalizer)
	if len(copy.Finalizers) == len(cd.Finalizers) {
		return nil
	}
	_, err = m.update(ctx, copy)
	return err
}

// metricKey namespaces a ComputeDomain UID by API group so that two manager
// sets can never share a gauge slot, even for synthetic objects that reuse a
// UID across groups.
func (m *ComputeDomainManager) metricKey(uid string) string {
	return m.config.Contract.APIGroup + "#" + uid
}

func (m *ComputeDomainManager) calculateGlobalStatus(cd *v1beta1.ComputeDomain) string {
	// numNodes is deprecated and forced to 0; check all nodes are Ready.
	for _, node := range cd.Status.Nodes {
		if node.Status == constants.NodeStatusNotReady {
			return constants.StatusNotReady
		}
	}
	return constants.StatusReady
}

func (m *ComputeDomainManager) updateGlobalStatus(ctx context.Context, cd *v1beta1.ComputeDomain) error {
	status := m.calculateGlobalStatus(cd)
	if cd.Status.Status == status {
		uid := string(cd.UID)
		ObserveComputeDomainStatus(m.metricKey(uid), status, func(string) bool { return m.hasComputeDomain(uid) })
		return nil
	}
	copy := cd.DeepCopy()
	copy.Status.Status = status
	_, err := m.UpdateStatus(ctx, copy)
	return err
}

func (m *ComputeDomainManager) onAddOrUpdate(ctx context.Context, obj interface{}) error {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return fmt.Errorf("expected ComputeDomain, got %T", obj)
	}
	cd, err := m.Get(string(u.GetUID()))
	if err != nil || cd == nil {
		return err
	}
	if cd.DeletionTimestamp != nil {
		return m.reconcileDelete(ctx, cd)
	}
	if cd.Spec.NumNodes != 0 {
		klog.Warningf("ComputeDomain %s/%s has deprecated numNodes=%d; value is ignored (treated as 0)", cd.Namespace, cd.Name, cd.Spec.NumNodes)
	}
	if err := m.addFinalizer(ctx, cd); err != nil {
		return fmt.Errorf("add ComputeDomain finalizer: %w", err)
	}
	m.nodeManager.RemoveStaleComputeDomainLabelsAsync()
	if cd.Spec.Channel == nil {
		return fmt.Errorf("ComputeDomain spec.channel is required")
	}
	if _, err := m.resourceClaimTemplateManager.Create(ctx, cd.Namespace, cd.Spec.Channel.ResourceClaimTemplate.Name, cd); err != nil {
		return fmt.Errorf("create workload ResourceClaimTemplate: %w", err)
	}
	if err := m.updateGlobalStatus(ctx, cd); err != nil {
		return fmt.Errorf("update ComputeDomain global status: %w", err)
	}
	return nil
}

// reconcileDelete removes controller-owned resources in dependency order. A
// non-empty CDClique returns a retryable error instead of blocking this worker.
func (m *ComputeDomainManager) reconcileDelete(ctx context.Context, cd *v1beta1.ComputeDomain) error {
	uid := string(cd.UID)
	if err := m.resourceClaimTemplateManager.Delete(ctx, uid, cd.Namespace); err != nil {
		return fmt.Errorf("delete workload ResourceClaimTemplate: %w", err)
	}
	if err := m.nodeManager.RemoveComputeDomainLabels(ctx, uid); err != nil {
		return fmt.Errorf("remove ComputeDomain node labels: %w", err)
	}
	if err := m.cliqueManager.AssertDaemonsRemoved(ctx, uid); err != nil {
		return fmt.Errorf("wait for ComputeDomainClique daemons to be removed: %w", err)
	}
	if err := m.resourceClaimTemplateManager.RemoveFinalizer(ctx, uid, cd.Namespace); err != nil {
		return fmt.Errorf("remove workload ResourceClaimTemplate finalizer: %w", err)
	}
	if err := m.resourceClaimTemplateManager.AssertRemoved(ctx, uid, cd.Namespace); err != nil {
		return fmt.Errorf("assert workload ResourceClaimTemplate removed: %w", err)
	}
	if err := m.removeFinalizer(ctx, uid); err != nil {
		return fmt.Errorf("remove ComputeDomain finalizer: %w", err)
	}
	ForgetComputeDomain(m.metricKey(uid))
	return nil
}

func removeString(values []string, target string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value != target {
			result = append(result, value)
		}
	}
	return result
}

func unstructuredObject(obj interface{}) *unstructured.Unstructured {
	if u, ok := obj.(*unstructured.Unstructured); ok {
		return u
	}
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		u, _ := tombstone.Obj.(*unstructured.Unstructured)
		return u
	}
	return nil
}
