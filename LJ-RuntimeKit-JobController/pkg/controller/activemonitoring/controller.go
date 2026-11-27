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

package activemonitoring

import (
	"context"
	"fmt"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	gpuv1alpha1 "github.com/lj-runtimekit/job-controller/pkg/apis/gpu/v1alpha1"
	"github.com/lj-runtimekit/job-controller/pkg/constants"
)

type Config struct {
	KubeClient    kubernetes.Interface
	DynamicClient dynamic.Interface
	Namespace     string // leader-election namespace, kept for parity
	ConfigMapName string
}

type Controller struct {
	config Config

	crFactory  dynamicinformer.DynamicSharedInformerFactory
	crInformer cache.SharedIndexInformer

	nodeFactory  dynamicinformer.DynamicSharedInformerFactory
	nodeInformer cache.SharedIndexInformer

	// podFactory backs the detection Pod informer (filtered to rt-diag=true).
	podFactory  dynamicinformer.DynamicSharedInformerFactory
	podInformer cache.SharedIndexInformer

	// cmFactory backs the shared controller ConfigMap informer (single object
	// in config.Namespace via field selector). nil when ConfigMapName is empty.
	cmFactory  dynamicinformer.DynamicSharedInformerFactory
	cmInformer cache.SharedIndexInformer

	// queue holds CR keys ("namespace/name"). The same key is never processed
	// concurrently, so per-CR reconcile is serialised while distinct CRs run in
	// parallel across workers.
	queue workqueue.TypedRateLimitingInterface[string]

	// OnReady is called once all informer caches are synced. Set by the factory
	// before Run; may be nil.
	OnReady func()

	// cfgMu guards the pre-resolved detection settings cache (see config.go):
	// the last good image/socketPath parsed from the shared controller ConfigMap.
	cfgMu        sync.Mutex
	lastGoodAM   *detectionSettings
	lastGoodAMRV string
	lastBadAMRV  string

	mu      sync.Mutex
	cancel  context.CancelFunc
	started bool
}

func NewController(config Config) *Controller {
	crFactory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(
		config.DynamicClient, constants.InformerResyncPeriod, metav1.NamespaceAll, nil,
	)
	nodeFactory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(
		config.DynamicClient, constants.InformerResyncPeriod, metav1.NamespaceAll,
		func(options *metav1.ListOptions) {
			options.LabelSelector = fmt.Sprintf("%s=%s", NodeEnabledLabelKey, NodeEnabledLabelValue)
		},
	)
	podFactory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(
		config.DynamicClient, constants.InformerResyncPeriod, metav1.NamespaceAll,
		func(options *metav1.ListOptions) {
			options.LabelSelector = fmt.Sprintf("%s=true", LabelActiveMonitoring)
		},
	)
	c := &Controller{
		config:       config,
		crFactory:    crFactory,
		crInformer:   crFactory.ForResource(activemonitoringGVR).Informer(),
		nodeFactory:  nodeFactory,
		nodeInformer: nodeFactory.ForResource(nodeGVR).Informer(),
		podFactory:   podFactory,
		podInformer:  podFactory.ForResource(podGVR).Informer(),
		queue:        workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]()),
	}
	if config.ConfigMapName != "" {
		c.cmFactory = dynamicinformer.NewFilteredDynamicSharedInformerFactory(
			config.DynamicClient, constants.InformerResyncPeriod, config.Namespace,
			func(options *metav1.ListOptions) {
				options.FieldSelector = "metadata.name=" + config.ConfigMapName
			},
		)
		c.cmInformer = c.cmFactory.ForResource(configMapGVR).Informer()
	}
	return c
}

func (c *Controller) Run(ctx context.Context, workers int) error {
	ctx, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		cancel()
		return fmt.Errorf("activemonitoring controller already started")
	}
	c.started = true
	c.cancel = cancel
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.started = false
		c.cancel = nil
		c.mu.Unlock()
		cancel()
	}()

	if err := c.startInformers(ctx); err != nil {
		return err
	}
	if c.OnReady != nil {
		c.OnReady()
	}
	if workers < 1 {
		workers = 1
	}
	klog.InfoS("ActiveMonitoring controller started", "workers", workers)

	go c.periodicResync(ctx)
	go func() {
		<-ctx.Done()
		c.queue.ShutDown()
	}()

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c.processNext(ctx) {
			}
		}()
	}
	wg.Wait()
	return nil
}

func (c *Controller) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cancel != nil {
		c.cancel()
	}
}

func (c *Controller) startInformers(ctx context.Context) error {
	if err := c.podInformer.AddIndexers(cache.Indexers{indexByCR: crIndexFunc}); err != nil {
		return fmt.Errorf("add detection pod CR indexer: %w", err)
	}
	if _, err := c.crInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { c.enqueueCR(obj) },
		UpdateFunc: func(_, obj interface{}) { c.enqueueCR(obj) },
	}); err != nil {
		return fmt.Errorf("add activemonitoring event handler: %w", err)
	}
	if _, err := c.podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { c.enqueueOwnerCR(obj) },
		UpdateFunc: func(_, obj interface{}) { c.enqueueOwnerCR(obj) },
		DeleteFunc: func(obj interface{}) { c.enqueueOwnerCR(obj) },
	}); err != nil {
		return fmt.Errorf("add detection pod event handler: %w", err)
	}
	if c.cmInformer != nil {
		if _, err := c.cmInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj interface{}) { c.onConfigMapEvent(obj) },
			UpdateFunc: func(_, obj interface{}) { c.onConfigMapEvent(obj) },
		}); err != nil {
			return fmt.Errorf("add controller configmap event handler: %w", err)
		}
	}

	c.crFactory.Start(ctx.Done())
	c.nodeFactory.Start(ctx.Done())
	c.podFactory.Start(ctx.Done())
	if c.cmFactory != nil {
		c.cmFactory.Start(ctx.Done())
	}

	synced := []cache.InformerSynced{c.crInformer.HasSynced, c.nodeInformer.HasSynced, c.podInformer.HasSynced}
	if c.cmInformer != nil {
		synced = append(synced, c.cmInformer.HasSynced)
	}
	if !cache.WaitForCacheSync(ctx.Done(), synced...) {
		return fmt.Errorf("activemonitoring informer cache sync failed")
	}
	// Seed the detection settings synchronously from the synced store, so the
	// first Pod creation never races the asynchronous informer event delivery.
	if c.cmInformer != nil {
		c.loadDetectionConfigFromStore()
	}
	return nil
}

// periodicResync re-enqueues every CR on a ticker so time-driven transitions
// (cycle interval elapsing, pendingTimeout exemption, node availability) happen
// even without CR/Pod events.
func (c *Controller) periodicResync(ctx context.Context) {
	ticker := time.NewTicker(periodicResyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, key := range c.allCRKeys() {
				c.queue.Add(key)
			}
		}
	}
}

func (c *Controller) allCRKeys() []string {
	objs := c.crInformer.GetStore().List()
	keys := make([]string, 0, len(objs))
	for _, obj := range objs {
		if u, ok := obj.(*unstructured.Unstructured); ok {
			keys = append(keys, cacheKey(u.GetNamespace(), u.GetName()))
		}
	}
	return keys
}

func (c *Controller) processNext(ctx context.Context) bool {
	key, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(key)
	if err := c.reconcile(ctx, key); err != nil {
		klog.ErrorS(err, "reconcile activemonitoring failed, will retry", "key", key)
		c.queue.AddRateLimited(key)
		return true
	}
	c.queue.Forget(key)
	return true
}

func (c *Controller) enqueueCR(obj interface{}) {
	u := unstructuredObject(obj)
	if u == nil {
		return
	}
	c.queue.Add(cacheKey(u.GetNamespace(), u.GetName()))
}

func (c *Controller) enqueueOwnerCR(obj interface{}) {
	u := unstructuredObject(obj)
	if u == nil {
		return
	}
	crName := u.GetLabels()[LabelActiveMonitoringCR]
	if crName == "" {
		return
	}
	c.queue.Add(cacheKey(u.GetNamespace(), crName))
}

// ---------------------------------------------------------------------------
// Store accessors (reads come from informer caches; writes go to the API).
// ---------------------------------------------------------------------------

// getCR returns a deep, typed copy of the CR for key, or nil if absent.
func (c *Controller) getCR(key string) *gpuv1alpha1.ActiveMonitoring {
	obj, exists, err := c.crInformer.GetStore().GetByKey(key)
	if err != nil || !exists {
		return nil
	}
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil
	}
	cr, err := toActiveMonitoring(u.DeepCopy())
	if err != nil {
		klog.ErrorS(err, "convert activemonitoring", "key", key)
		return nil
	}
	return cr
}

// getPod returns a deep copy of a detection Pod from the informer cache, or nil.
func (c *Controller) getPod(namespace, name string) *unstructured.Unstructured {
	if name == "" {
		return nil
	}
	obj, exists, err := c.podInformer.GetStore().GetByKey(cacheKey(namespace, name))
	if err != nil || !exists {
		return nil
	}
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil
	}
	return u.DeepCopy()
}

// listCRPods returns the CR's live (non-terminating) detection Pods via the
// byActiveMonitoringCR index.
func (c *Controller) listCRPods(cr *gpuv1alpha1.ActiveMonitoring) []*unstructured.Unstructured {
	objs, err := c.podInformer.GetIndexer().ByIndex(indexByCR, cacheKey(cr.Namespace, cr.Name))
	if err != nil {
		return nil
	}
	pods := make([]*unstructured.Unstructured, 0, len(objs))
	for _, obj := range objs {
		u, ok := obj.(*unstructured.Unstructured)
		if !ok || u.GetDeletionTimestamp() != nil {
			continue
		}
		pods = append(pods, u)
	}
	return pods
}

// listNodes returns typed copies of all cached nodes.
func (c *Controller) listNodes() []*v1.Node {
	objs := c.nodeInformer.GetStore().List()
	nodes := make([]*v1.Node, 0, len(objs))
	for _, obj := range objs {
		u, ok := obj.(*unstructured.Unstructured)
		if !ok {
			continue
		}
		node := &v1.Node{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, node); err != nil {
			klog.V(4).InfoS("convert node failed", "node", u.GetName(), "err", err)
			continue
		}
		nodes = append(nodes, node)
	}
	return nodes
}

// ---------------------------------------------------------------------------
// API writes.
// ---------------------------------------------------------------------------

// createPod creates a detection Pod via the dynamic client.
func (c *Controller) createPod(ctx context.Context, pod *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	return c.config.DynamicClient.Resource(podGVR).Namespace(pod.GetNamespace()).Create(ctx, pod, metav1.CreateOptions{})
}

// deleteResidualPod deletes a detection Pod with a small grace period, ignoring
// NotFound (idempotent).
func (c *Controller) deleteResidualPod(ctx context.Context, namespace, name string) {
	if name == "" {
		return
	}
	grace := deleteGraceSeconds
	err := c.config.DynamicClient.Resource(podGVR).Namespace(namespace).Delete(ctx, name, metav1.DeleteOptions{GracePeriodSeconds: &grace})
	if err != nil && !apierrors.IsNotFound(err) {
		klog.ErrorS(err, "delete detection pod failed", "pod", namespace+"/"+name)
	}
}

// sweepResidualPods deletes detection Pods that are no longer wanted, and MUST
// be called only after updateStatus has durably persisted the new status.
func (c *Controller) sweepResidualPods(ctx context.Context, cr *gpuv1alpha1.ActiveMonitoring) {
	wanted := make(map[string]struct{}, len(cr.Status.Queue))
	for i := range cr.Status.Queue {
		if name := cr.Status.Queue[i].PodName; name != "" {
			wanted[name] = struct{}{}
		}
	}
	for _, pod := range c.listCRPods(cr) {
		name := pod.GetName()
		if _, ok := wanted[name]; ok {
			continue
		}
		if phase := podPhase(pod); phase != v1.PodSucceeded && phase != v1.PodFailed {
			continue
		}
		c.deleteResidualPod(ctx, cr.Namespace, name)
	}
}

func (c *Controller) updateStatus(ctx context.Context, cr *gpuv1alpha1.ActiveMonitoring) error {
	u, err := fromActiveMonitoring(cr)
	if err != nil {
		return err
	}
	ri := c.config.DynamicClient.Resource(activemonitoringGVR).Namespace(cr.Namespace)
	_, err = ri.UpdateStatus(ctx, u, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("update activemonitoring status %s/%s: %w", cr.Namespace, cr.Name, err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Conversions and small helpers.
// ---------------------------------------------------------------------------

func toActiveMonitoring(obj *unstructured.Unstructured) (*gpuv1alpha1.ActiveMonitoring, error) {
	cr := &gpuv1alpha1.ActiveMonitoring{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, cr); err != nil {
		return nil, err
	}
	return cr, nil
}

func fromActiveMonitoring(cr *gpuv1alpha1.ActiveMonitoring) (*unstructured.Unstructured, error) {
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(cr)
	if err != nil {
		return nil, err
	}
	u := &unstructured.Unstructured{Object: obj}
	u.SetGroupVersionKind(activemonitoringGVR.GroupVersion().WithKind(Kind))
	return u, nil
}

func crIndexFunc(obj interface{}) ([]string, error) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, nil
	}
	crName := u.GetLabels()[LabelActiveMonitoringCR]
	if crName == "" {
		return nil, nil
	}
	return []string{cacheKey(u.GetNamespace(), crName)}, nil
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

func cacheKey(namespace, name string) string {
	if namespace == "" {
		return name
	}
	return namespace + "/" + name
}
