package computedomain

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"text/template"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	v1beta1 "github.com/lj-runtimekit/job-controller/pkg/apis/resource/v1beta1"
	cdapi "github.com/lj-runtimekit/job-controller/pkg/computedomain"
	"github.com/lj-runtimekit/job-controller/pkg/constants"
)

const workloadRCTTemplatePath = "/templates/workload-claim-template.tmpl.yaml"

// resourceClaimTemplateData is the render input for the RCT template. Every
// group-dependent value is injected from the bound GroupContract so the
// template itself stays group-agnostic.
type resourceClaimTemplateData struct {
	Namespace string
	Name      string
	CDUID     string
	// AllocationMode is the requested DRA allocation mode (All/Single).
	AllocationMode string
	// Target is the RCT target label value (Workload).
	Target string
	// LabelComputeDomain / LabelTarget / Finalizer / OpaqueAPIVersion are the
	// group-scoped contract values.
	LabelComputeDomain string
	LabelTarget        string
	Finalizer          string
	OpaqueAPIVersion   string
}

func newResourceClaimTemplateData(contract cdapi.GroupContract, namespace, name, uid, allocationMode, target string) resourceClaimTemplateData {
	return resourceClaimTemplateData{
		Namespace:          namespace,
		Name:               name,
		CDUID:              uid,
		AllocationMode:     allocationMode,
		Target:             target,
		LabelComputeDomain: contract.LabelComputeDomain,
		LabelTarget:        contract.LabelTarget,
		Finalizer:          contract.Finalizer,
		OpaqueAPIVersion:   contract.OpaqueAPIVersion,
	}
}

type BaseResourceClaimTemplateManager struct {
	config *ManagerConfig
	target string

	waitGroup      sync.WaitGroup
	cancelContext  context.CancelFunc
	factory        dynamicinformer.DynamicSharedInformerFactory
	informer       cache.SharedIndexInformer
	mutationCache  cache.MutationCache
	cleanupManager *CleanupManager
}

type WorkloadResourceClaimTemplateManager struct {
	*BaseResourceClaimTemplateManager
}

func newBaseResourceClaimTemplateManager(config *ManagerConfig, target, namespace string) *BaseResourceClaimTemplateManager {
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(
		config.ClientSets.Dynamic,
		constants.InformerResyncPeriod,
		namespace,
		func(options *metav1.ListOptions) {
			options.LabelSelector = fmt.Sprintf("%s,%s=%s", config.Contract.LabelComputeDomain, config.Contract.LabelTarget, target)
		},
	)
	return &BaseResourceClaimTemplateManager{
		config:   config,
		target:   target,
		factory:  factory,
		informer: factory.ForResource(resourceClaimTemplateGVR).Informer(),
	}
}

func (m *BaseResourceClaimTemplateManager) Start(ctx context.Context) (returnErr error) {
	ctx, cancel := context.WithCancel(ctx)
	m.cancelContext = cancel
	defer func() {
		if returnErr != nil {
			_ = m.Stop()
		}
	}()
	if err := addComputeDomainLabelIndexer(m.informer, m.config.Contract.LabelComputeDomain); err != nil {
		return fmt.Errorf("add ResourceClaimTemplate indexer: %w", err)
	}
	m.mutationCache = cache.NewIntegerResourceVersionMutationCache(
		klog.Background(), m.informer.GetStore(), m.informer.GetIndexer(), constants.MutationCacheTTL, true,
	)
	m.waitGroup.Add(1)
	go func() {
		defer m.waitGroup.Done()
		m.factory.Start(ctx.Done())
		<-ctx.Done()
	}()
	if err := waitForInformerSync(ctx, m.config, "ResourceClaimTemplate", m.informer.HasSynced); err != nil {
		return fmt.Errorf("%w (target %s)", err, m.target)
	}
	return m.cleanupManager.Start(ctx)
}

func (m *BaseResourceClaimTemplateManager) Stop() error {
	if m.cleanupManager != nil {
		_ = m.cleanupManager.Stop()
	}
	if m.cancelContext != nil {
		m.cancelContext()
		m.cancelContext = nil
	}
	m.waitGroup.Wait()
	return nil
}

func (m *BaseResourceClaimTemplateManager) create(ctx context.Context, path string, data resourceClaimTemplateData, deviceClass, kind string) (*unstructured.Unstructured, error) {
	rct, err := renderUnstructuredTemplate(path, data)
	if err != nil {
		return nil, err
	}
	configureResourceClaimTemplate(rct, m.config.Contract, deviceClass, kind, data)
	created, err := m.config.ClientSets.Dynamic.Resource(resourceClaimTemplateGVR).Namespace(data.Namespace).Create(ctx, rct, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}
	m.mutationCache.Mutation(created)
	return created, nil
}

func NewWorkloadResourceClaimTemplateManager(config *ManagerConfig, getComputeDomain GetComputeDomainFunc, liveConfirmAbsent LiveConfirmAbsentFunc, liveListUIDs LiveListUIDsFunc) *WorkloadResourceClaimTemplateManager {
	m := &WorkloadResourceClaimTemplateManager{newBaseResourceClaimTemplateManager(config, constants.RCTTargetWorkload, metav1.NamespaceAll)}
	// The namespace-aware cleanup handles cross-namespace UID label collisions.
	m.cleanupManager = NewCleanupManager(m.informer, getComputeDomain, liveConfirmAbsent, liveListUIDs, config.Contract.LabelComputeDomain, m.cleanup)
	return m
}

// getInNamespace returns the single RCT that matches the given CD UID within
// the specified namespace. It filters out objects in other namespaces so that
// cross-namespace UID label collisions do not produce spurious "multiple
// ResourceClaimTemplates" errors.
func (m *WorkloadResourceClaimTemplateManager) getInNamespace(uid string, source indexer, namespace string) (*unstructured.Unstructured, error) {
	objects, err := getByComputeDomainUID(source, uid, m.config.Contract.LabelComputeDomain)
	if err != nil {
		return nil, err
	}
	var filtered []*unstructured.Unstructured
	for _, obj := range objects {
		if obj.GetNamespace() == namespace {
			filtered = append(filtered, obj)
		}
	}
	if len(filtered) > 1 {
		return nil, fmt.Errorf("multiple %s ResourceClaimTemplates for ComputeDomain %s in namespace %s", m.target, uid, namespace)
	}
	if len(filtered) == 0 {
		return nil, nil
	}
	// Every candidate is returned to a mutation path (Create reuse, Delete,
	// RemoveFinalizer, AssertRemoved), so reject ambiguous ownership here: an
	// object carrying another group's owner or target label belongs to two
	// manager sets at once.
	for _, obj := range filtered {
		if err := m.config.Contract.VerifyExclusiveOwnership(obj.GetLabels(), uid, m.target); err != nil {
			return nil, fmt.Errorf("ResourceClaimTemplate %s/%s: %w", obj.GetNamespace(), obj.GetName(), err)
		}
	}
	return filtered[0], nil
}

func (m *WorkloadResourceClaimTemplateManager) Create(ctx context.Context, namespace, name string, cd *v1beta1.ComputeDomain) (*unstructured.Unstructured, error) {
	existing, err := m.getInNamespace(string(cd.UID), m.mutationCache, namespace)
	if err != nil || existing != nil {
		return existing, err
	}
	if name == "" {
		return nil, fmt.Errorf("workload ResourceClaimTemplate name is empty")
	}
	// A same-named RCT may already exist in this namespace. It is either ours
	// (merely not visible in the informer cache yet) or it belongs to another
	// ComputeDomain, possibly in the other API group where the same name is a
	// legitimate configuration. Never adopt or overwrite a foreign object.
	adopted, err := m.adoptOrReject(ctx, namespace, name, cd)
	if err != nil || adopted != nil {
		return adopted, err
	}
	allocationMode := string(v1beta1.AllocationModeAll)
	if cd.Spec.Channel != nil && cd.Spec.Channel.AllocationMode != "" {
		allocationMode = string(cd.Spec.Channel.AllocationMode)
	}
	data := newResourceClaimTemplateData(m.config.Contract, namespace, name, string(cd.UID), allocationMode, m.target)
	return m.create(ctx, workloadRCTTemplatePath, data, constants.ChannelDeviceClassName, "ComputeDomainChannelConfig")
}

// adoptOrReject looks up an RCT by name before Create. It returns the existing
// object only when it is exclusively owned by the same ComputeDomain in the same
// API group; an object carrying any other supported group's owner or target
// label (even with the same UID), owned by another ComputeDomain, or owned by
// none is reported as an explicit, non-retryable conflict so the controller
// never takes over or overwrites it.
func (m *WorkloadResourceClaimTemplateManager) adoptOrReject(ctx context.Context, namespace, name string, cd *v1beta1.ComputeDomain) (*unstructured.Unstructured, error) {
	live, err := m.config.ClientSets.Dynamic.Resource(resourceClaimTemplateGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get ResourceClaimTemplate %s/%s: %w", namespace, name, err)
	}
	owner := live.GetLabels()[m.config.Contract.LabelComputeDomain]
	if err := m.config.Contract.VerifyExclusiveOwnership(live.GetLabels(), owner, m.target); err != nil {
		return nil, fmt.Errorf("ResourceClaimTemplate %s/%s: %w; refusing to adopt or overwrite it for ComputeDomain %s (API group %s)",
			namespace, name, err, cd.UID, m.config.Contract.APIGroup)
	}
	if owner != string(cd.UID) {
		return nil, fmt.Errorf("ResourceClaimTemplate %s/%s is already owned by ComputeDomain %s; refusing to adopt or overwrite it for ComputeDomain %s (API group %s)",
			namespace, name, owner, cd.UID, m.config.Contract.APIGroup)
	}
	return live, nil // our own object, created before the cache caught up
}

// Delete deletes the workload RCT for the given CD UID, scoped to namespace.
func (m *WorkloadResourceClaimTemplateManager) Delete(ctx context.Context, uid, namespace string) error {
	rct, err := m.getInNamespace(uid, m.mutationCache, namespace)
	if err != nil || rct == nil {
		return err
	}
	if rct.GetDeletionTimestamp() != nil {
		return nil
	}
	rctUID := rct.GetUID()
	deleteOpts := metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{
			UID: &rctUID,
		},
	}
	err = m.config.ClientSets.Dynamic.Resource(resourceClaimTemplateGVR).Namespace(rct.GetNamespace()).Delete(ctx, rct.GetName(), deleteOpts)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// RemoveFinalizer removes the finalizer from the workload RCT, scoped to namespace.
func (m *WorkloadResourceClaimTemplateManager) RemoveFinalizer(ctx context.Context, uid, namespace string) error {
	rct, err := m.getInNamespace(uid, m.mutationCache, namespace)
	if err != nil || rct == nil {
		return err
	}
	if rct.GetDeletionTimestamp() == nil {
		return fmt.Errorf("attempting to remove ResourceClaimTemplate finalizer before deletion")
	}
	copy := rct.DeepCopy()
	copy.SetFinalizers(removeString(copy.GetFinalizers(), m.config.Contract.Finalizer))
	if len(copy.GetFinalizers()) == len(rct.GetFinalizers()) {
		return nil
	}
	updated, err := m.config.ClientSets.Dynamic.Resource(resourceClaimTemplateGVR).Namespace(copy.GetNamespace()).Update(ctx, copy, metav1.UpdateOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if err == nil {
		m.mutationCache.Mutation(updated)
	}
	return nil
}

// AssertRemoved verifies the workload RCT no longer exists in the given namespace.
func (m *WorkloadResourceClaimTemplateManager) AssertRemoved(_ context.Context, uid, namespace string) error {
	rct, err := m.getInNamespace(uid, m.informer.GetIndexer(), namespace)
	if err != nil {
		return err
	}
	if rct != nil {
		return fmt.Errorf("ResourceClaimTemplate %s/%s still exists", rct.GetNamespace(), rct.GetName())
	}
	return nil
}

// cleanup handles orphan cleanup across all namespaces. It deletes each
// matching RCT directly by namespace/name to avoid the "multiple
// ResourceClaimTemplates" error that getInNamespace would return when
// duplicate UID labels exist in the same namespace.
//
// Every object is re-validated for exclusive ownership before any mutation: an
// orphan whose labels also belong to another supported API group is left alone
// and reported, because deleting it would destroy the other group's object.
func (m *WorkloadResourceClaimTemplateManager) cleanup(ctx context.Context, uid string) error {
	objects, err := getByComputeDomainUID(m.mutationCache, uid, m.config.Contract.LabelComputeDomain)
	if err != nil {
		return err
	}
	var firstErr error
	for _, rct := range objects {
		ns := rct.GetNamespace()
		name := rct.GetName()
		rctUID := rct.GetUID()

		if err := m.config.Contract.VerifyExclusiveOwnership(rct.GetLabels(), uid, m.target); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("ResourceClaimTemplate %s/%s: %w", ns, name, err)
			}
			continue
		}

		// Delete the specific object using UID precondition to protect identity.
		if rct.GetDeletionTimestamp() == nil {
			deleteOpts := metav1.DeleteOptions{
				Preconditions: &metav1.Preconditions{
					UID: &rctUID,
				},
			}
			if err := m.config.ClientSets.Dynamic.Resource(resourceClaimTemplateGVR).
				Namespace(ns).Delete(ctx, name, deleteOpts); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}

		// Re-fetch by specific name to get the latest version for finalizer removal.
		latest, err := m.config.ClientSets.Dynamic.Resource(resourceClaimTemplateGVR).
			Namespace(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				continue // already fully deleted (no finalizer held it)
			}
			return err
		}
		// Labels can change between the cache read and this live re-fetch, so
		// validate the object that would actually be mutated.
		if err := m.config.Contract.VerifyExclusiveOwnership(latest.GetLabels(), uid, m.target); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("ResourceClaimTemplate %s/%s: %w", ns, name, err)
			}
			continue
		}
		if latest.GetDeletionTimestamp() == nil {
			continue
		}
		// Verify object identity before removing finalizer.
		if latest.GetUID() != types.UID("") && latest.GetUID() != rctUID {
			continue // identity mismatch, skip
		}
		updated := latest.DeepCopy()
		updated.SetFinalizers(removeString(updated.GetFinalizers(), m.config.Contract.Finalizer))
		if len(updated.GetFinalizers()) == len(latest.GetFinalizers()) {
			continue // no finalizer to remove
		}
		result, err := m.config.ClientSets.Dynamic.Resource(resourceClaimTemplateGVR).
			Namespace(ns).Update(ctx, updated, metav1.UpdateOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		if err == nil {
			m.mutationCache.Mutation(result)
		}
	}
	return firstErr
}

func renderUnstructuredTemplate(path string, data interface{}) (*unstructured.Unstructured, error) {
	contents, err := readTemplate(path)
	if err != nil {
		return nil, fmt.Errorf("read template %s: %w", path, err)
	}
	tmpl, err := template.New(filepath.Base(path)).Parse(string(contents))
	if err != nil {
		return nil, fmt.Errorf("parse template %s: %w", path, err)
	}
	var rendered bytes.Buffer
	if err := tmpl.Execute(&rendered, data); err != nil {
		return nil, fmt.Errorf("execute template %s: %w", path, err)
	}
	jsonData, err := yaml.ToJSON(rendered.Bytes())
	if err != nil {
		return nil, fmt.Errorf("convert rendered template to JSON: %w", err)
	}
	object := map[string]interface{}{}
	if err := json.Unmarshal(jsonData, &object); err != nil {
		return nil, err
	}
	return &unstructured.Unstructured{Object: object}, nil
}

func readTemplate(path string) ([]byte, error) {
	candidates := []string{path, stringsTrimLeadingSlash(path), filepath.Join("templates", filepath.Base(path))}
	var lastErr error
	for _, candidate := range candidates {
		contents, err := os.ReadFile(candidate)
		if err == nil {
			return contents, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

func stringsTrimLeadingSlash(path string) string {
	for len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}
	return path
}

// configureResourceClaimTemplate applies the bound GroupContract to a rendered
// RCT: the group-scoped finalizer, owner/target labels and the opaque apiVersion
// are written explicitly (never inherited from the template), while nil maps are
// initialised instead of being dereferenced.
func configureResourceClaimTemplate(rct *unstructured.Unstructured, contract cdapi.GroupContract, deviceClass, kind string, data resourceClaimTemplateData) {
	rct.SetFinalizers([]string{contract.Finalizer})
	labels := rct.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels[contract.LabelComputeDomain] = data.CDUID
	if data.Target != "" {
		labels[contract.LabelTarget] = data.Target
	}
	rct.SetLabels(labels)
	requests, _, _ := unstructured.NestedSlice(rct.Object, "spec", "spec", "devices", "requests")
	if len(requests) != 0 {
		if request, ok := requests[0].(map[string]interface{}); ok {
			exactly, _ := request["exactly"].(map[string]interface{})
			if exactly == nil {
				exactly = map[string]interface{}{}
			}
			exactly["deviceClassName"] = deviceClass
			// Set DRA native allocationMode on the ExactDeviceRequest.
			switch data.AllocationMode {
			case string(v1beta1.AllocationModeAll):
				exactly["allocationMode"] = "All"
				delete(exactly, "count")
			case string(v1beta1.AllocationModeSingle):
				exactly["allocationMode"] = "ExactCount"
				exactly["count"] = int64(1)
			default:
				// Default to All when no explicit mode is set.
				if data.AllocationMode == "" {
					exactly["allocationMode"] = "All"
					delete(exactly, "count")
				}
			}
			request["exactly"] = exactly
			requests[0] = request
			_ = unstructured.SetNestedSlice(rct.Object, requests, "spec", "spec", "devices", "requests")
		}
	}
	configs, _, _ := unstructured.NestedSlice(rct.Object, "spec", "spec", "devices", "config")
	if len(configs) == 0 {
		return
	}
	config, ok := configs[0].(map[string]interface{})
	if !ok {
		return
	}
	opaque, _ := config["opaque"].(map[string]interface{})
	if opaque == nil {
		opaque = map[string]interface{}{}
	}
	opaque["driver"] = constants.DriverName
	parameters, _ := opaque["parameters"].(map[string]interface{})
	if parameters == nil {
		parameters = map[string]interface{}{}
	}
	parameters["apiVersion"] = contract.OpaqueAPIVersion
	parameters["kind"] = kind
	parameters["domainID"] = data.CDUID
	if data.AllocationMode != "" {
		parameters["allocationMode"] = data.AllocationMode
	}
	opaque["parameters"] = parameters
	config["opaque"] = opaque
	configs[0] = config
	_ = unstructured.SetNestedSlice(rct.Object, configs, "spec", "spec", "devices", "config")
}
