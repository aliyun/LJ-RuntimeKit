package computedomain

import (
	"context"
	"fmt"
	"strings"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"github.com/lj-runtimekit/job-controller/pkg/constants"
)

// NodeManager removes ComputeDomain labels and cleans stale labels asynchronously.
type NodeManager struct {
	config *ManagerConfig

	waitGroup      sync.WaitGroup
	cancelContext  context.CancelFunc
	factory        dynamicinformer.DynamicSharedInformerFactory
	informer       cache.SharedIndexInformer
	cleanupManager *CleanupManager
}

// NewNodeManager builds the Node manager for one API group contract. The
// informer is filtered by the group-scoped ComputeDomain label, so a Node
// labelled by the other group is never seen, cleaned or rewritten here.
func NewNodeManager(config *ManagerConfig, getComputeDomain GetComputeDomainFunc, liveConfirmAbsent LiveConfirmAbsentFunc, liveListUIDs LiveListUIDsFunc) *NodeManager {
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(
		config.ClientSets.Dynamic,
		constants.InformerResyncPeriod,
		metav1.NamespaceAll,
		func(options *metav1.ListOptions) {
			options.LabelSelector = config.Contract.LabelComputeDomain
		},
	)
	manager := &NodeManager{
		config:   config,
		factory:  factory,
		informer: factory.ForResource(nodeGVR).Informer(),
	}
	manager.cleanupManager = NewCleanupManager(manager.informer, getComputeDomain, liveConfirmAbsent, liveListUIDs, config.Contract.LabelComputeDomain, manager.cleanupLabels)
	return manager
}

func (m *NodeManager) Start(ctx context.Context) (returnErr error) {
	ctx, cancel := context.WithCancel(ctx)
	m.cancelContext = cancel
	defer func() {
		if returnErr != nil {
			_ = m.Stop()
		}
	}()
	m.waitGroup.Add(1)
	go func() {
		defer m.waitGroup.Done()
		m.factory.Start(ctx.Done())
		<-ctx.Done()
	}()
	if err := waitForInformerSync(ctx, m.config, "Node", m.informer.HasSynced); err != nil {
		return err
	}
	return m.cleanupManager.Start(ctx)
}

func (m *NodeManager) Stop() error {
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

func (m *NodeManager) RemoveComputeDomainLabels(ctx context.Context, cdUID string) error {
	labelKey := m.config.Contract.LabelComputeDomain
	selector := fmt.Sprintf("%s=%s", labelKey, cdUID)
	nodes, err := m.config.ClientSets.Dynamic.Resource(nodeGVR).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return fmt.Errorf("list ComputeDomain nodes: %w", err)
	}
	names := make([]string, 0, len(nodes.Items))
	for i := range nodes.Items {
		node := nodes.Items[i].DeepCopy()
		labels := node.GetLabels()
		delete(labels, labelKey)
		node.SetLabels(labels)
		if _, err := m.config.ClientSets.Dynamic.Resource(nodeGVR).Update(ctx, node, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("update node %s: %w", node.GetName(), err)
		}
		names = append(names, node.GetName())
	}
	if len(names) != 0 {
		klog.V(1).InfoS("Removed ComputeDomain node labels", "apiGroup", m.config.Contract.APIGroup, "uid", cdUID, "nodes", strings.Join(names, ","))
	}
	return nil
}

func (m *NodeManager) RemoveStaleComputeDomainLabelsAsync() bool {
	return m.cleanupManager.EnqueueCleanup()
}

func (m *NodeManager) cleanupLabels(ctx context.Context, cdUID string) error {
	return m.RemoveComputeDomainLabels(ctx, cdUID)
}
