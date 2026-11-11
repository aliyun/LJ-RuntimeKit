package computedomain

import (
	"context"
	"fmt"
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	v1beta1 "github.com/lj-runtimekit/job-controller/pkg/apis/resource/v1beta1"
	"github.com/lj-runtimekit/job-controller/pkg/constants"
)

// ComputeDomainCliqueManager provides cached read access to ComputeDomainClique
// objects plus the deletion-path assertion that waits for the Device Plugin to
// deregister daemon entries. It deliberately exposes no write path: the Device
// Plugin is the sole owner of daemon entries. CDClique objects live in the
// configured ComputeDomainCliqueNamespace (default "default"), which is
// independent of the driver / leader-election namespace.
type ComputeDomainCliqueManager struct {
	config *ManagerConfig

	waitGroup     sync.WaitGroup
	cancelContext context.CancelFunc
	factory       dynamicinformer.DynamicSharedInformerFactory
	informer      cache.SharedIndexInformer
	mutationCache cache.MutationCache
}

func NewComputeDomainCliqueManager(config *ManagerConfig) *ComputeDomainCliqueManager {
	cliqueNS := config.ComputeDomainCliqueNamespace
	if cliqueNS == "" {
		cliqueNS = DefaultCliqueNamespace
	}
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(
		config.ClientSets.Dynamic,
		constants.InformerResyncPeriod,
		cliqueNS,
		func(options *metav1.ListOptions) {
			options.LabelSelector = config.Contract.LabelCliqueDomain
		},
	)
	return &ComputeDomainCliqueManager{
		config:   config,
		factory:  factory,
		informer: factory.ForResource(config.Contract.ComputeDomainCliqueGVR).Informer(),
	}
}

func (m *ComputeDomainCliqueManager) Start(ctx context.Context) (returnErr error) {
	ctx, cancel := context.WithCancel(ctx)
	m.cancelContext = cancel
	defer func() {
		if returnErr != nil {
			_ = m.Stop()
		}
	}()
	if err := addComputeDomainLabelIndexer(m.informer, m.config.Contract.LabelCliqueDomain); err != nil {
		return fmt.Errorf("add ComputeDomainClique indexer: %w", err)
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
	if err := waitForInformerSync(ctx, m.config, "ComputeDomainClique", m.informer.HasSynced); err != nil {
		return err
	}
	return nil
}

func (m *ComputeDomainCliqueManager) Stop() error {
	if m.cancelContext != nil {
		m.cancelContext()
		m.cancelContext = nil
	}
	m.waitGroup.Wait()
	return nil
}

func (m *ComputeDomainCliqueManager) Get(cdUID, cliqueID string) (*v1beta1.ComputeDomainClique, error) {
	objects, err := getByComputeDomainUID(m.mutationCache, cdUID, m.config.Contract.LabelCliqueDomain)
	if err != nil {
		return nil, err
	}
	var result *v1beta1.ComputeDomainClique
	for _, object := range objects {
		if object.GetLabels()[m.config.Contract.LabelClique] != cliqueID {
			continue
		}
		if result != nil {
			return nil, fmt.Errorf("multiple ComputeDomainCliques for ComputeDomain %s and clique %s", cdUID, cliqueID)
		}
		result, err = toComputeDomainClique(object)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (m *ComputeDomainCliqueManager) List() ([]*v1beta1.ComputeDomainClique, error) {
	objects := m.informer.GetStore().List()
	result := make([]*v1beta1.ComputeDomainClique, 0, len(objects))
	for _, obj := range objects {
		u, ok := obj.(*unstructured.Unstructured)
		if !ok {
			continue
		}
		clique, err := toComputeDomainClique(u)
		if err != nil {
			return nil, err
		}
		result = append(result, clique)
	}
	return result, nil
}

// ListByComputeDomainUID returns all cliques associated with a ComputeDomain.
func (m *ComputeDomainCliqueManager) ListByComputeDomainUID(uid string) ([]*v1beta1.ComputeDomainClique, error) {
	objects, err := getByComputeDomainUID(m.informer.GetIndexer(), uid, m.config.Contract.LabelCliqueDomain)
	if err != nil {
		return nil, err
	}
	result := make([]*v1beta1.ComputeDomainClique, 0, len(objects))
	for _, object := range objects {
		clique, err := toComputeDomainClique(object)
		if err != nil {
			return nil, err
		}
		result = append(result, clique)
	}
	return result, nil
}

// AssertDaemonsRemoved verifies that every clique for the ComputeDomain has no
// daemon entries by performing a live API list in the configured clique namespace
// (not trusting the informer cache). Empty cliques are deleted with
// UID/ResourceVersion preconditions to avoid racing with concurrent daemon writers.
func (m *ComputeDomainCliqueManager) AssertDaemonsRemoved(ctx context.Context, uid string) error {
	cliqueNS := m.config.ComputeDomainCliqueNamespace
	if cliqueNS == "" {
		cliqueNS = DefaultCliqueNamespace
	}
	// The List is always scoped to the source group's GVR: the clique labels are
	// shared by both API groups, but the objects are not.
	labelSelector := fmt.Sprintf("%s=%s", m.config.Contract.LabelCliqueDomain, uid)
	list, err := m.config.ClientSets.Dynamic.Resource(m.config.Contract.ComputeDomainCliqueGVR).Namespace(cliqueNS).List(
		ctx, metav1.ListOptions{LabelSelector: labelSelector},
	)
	if err != nil {
		return fmt.Errorf("live list ComputeDomainCliques: %w", err)
	}
	pending := make([]string, 0)
	for i := range list.Items {
		clique, err := toComputeDomainClique(&list.Items[i])
		if err != nil {
			return fmt.Errorf("convert ComputeDomainClique: %w", err)
		}
		if clique.Namespace != cliqueNS {
			return fmt.Errorf("refusing to delete ComputeDomainClique %s/%s: namespace %q does not match configured clique namespace %q",
				clique.Namespace, clique.Name, clique.Namespace, cliqueNS)
		}
		if len(clique.Daemons) != 0 {
			pending = append(pending, clique.Name)
			continue
		}
		if clique.DeletionTimestamp != nil {
			continue
		}
		cliqueUID := types.UID(clique.UID)
		rv := clique.ResourceVersion
		err = m.config.ClientSets.Dynamic.Resource(m.config.Contract.ComputeDomainCliqueGVR).Namespace(clique.Namespace).Delete(
			ctx, clique.Name, metav1.DeleteOptions{
				Preconditions: &metav1.Preconditions{
					UID:             &cliqueUID,
					ResourceVersion: &rv,
				},
			},
		)
		if apierrors.IsNotFound(err) {
			continue // already gone
		}
		if apierrors.IsConflict(err) {
			return fmt.Errorf("delete empty ComputeDomainClique %s/%s: precondition conflict (will retry): %w", clique.Namespace, clique.Name, err)
		}
		if err != nil {
			return fmt.Errorf("delete empty ComputeDomainClique %s/%s: %w", clique.Namespace, clique.Name, err)
		}
	}
	if len(pending) != 0 {
		return fmt.Errorf("ComputeDomainCliques still contain daemon entries: %v", pending)
	}
	return nil
}

func toComputeDomainClique(obj *unstructured.Unstructured) (*v1beta1.ComputeDomainClique, error) {
	clique := &v1beta1.ComputeDomainClique{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, clique); err != nil {
		return nil, err
	}
	return clique, nil
}
