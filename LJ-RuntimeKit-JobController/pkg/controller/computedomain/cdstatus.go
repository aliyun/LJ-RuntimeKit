package computedomain

import (
	"context"
	"maps"
	"sync"
	"time"

	"k8s.io/klog/v2"

	v1beta1 "github.com/lj-runtimekit/job-controller/pkg/apis/resource/v1beta1"
	"github.com/lj-runtimekit/job-controller/pkg/constants"
)

// ComputeDomainStatusManager aggregates ComputeDomain status exclusively from
// ComputeDomainClique daemon entries.
//
// The Device Plugin is the only writer of CDClique daemon entries: it creates,
// updates and removes them. This manager therefore treats CDCliques as strictly
// read-only — it never adds, mutates or removes a daemon entry, regardless of
// the observed Device Plugin pod state (missing pods, selector drift, IP
// changes, terminating pods). Daemon deregistration is signalled by the Device
// Plugin itself, and the ComputeDomain deletion path waits for it through
// ComputeDomainCliqueManager.AssertDaemonsRemoved.
type ComputeDomainStatusManager struct {
	cliqueManager             *ComputeDomainCliqueManager
	listComputeDomains        ListComputeDomainsFunc
	updateComputeDomainStatus UpdateComputeDomainStatusFunc
	// labelCliqueDomain is the ComputeDomainClique label key this manager
	// aggregates on. It is taken from the bound GroupContract so a manager set
	// only ever reads the labels of its own API group contract.
	labelCliqueDomain string

	waitGroup     sync.WaitGroup
	cancelContext context.CancelFunc
}

func NewComputeDomainStatusManager(cliqueManager *ComputeDomainCliqueManager, listComputeDomains ListComputeDomainsFunc, updateStatus UpdateComputeDomainStatusFunc) *ComputeDomainStatusManager {
	labelCliqueDomain := constants.LabelComputeDomainCliqueDomain
	if cliqueManager != nil && cliqueManager.config != nil && cliqueManager.config.Contract.LabelCliqueDomain != "" {
		labelCliqueDomain = cliqueManager.config.Contract.LabelCliqueDomain
	}
	return &ComputeDomainStatusManager{
		cliqueManager:             cliqueManager,
		listComputeDomains:        listComputeDomains,
		updateComputeDomainStatus: updateStatus,
		labelCliqueDomain:         labelCliqueDomain,
	}
}

func (m *ComputeDomainStatusManager) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	m.cancelContext = cancel
	m.waitGroup.Add(1)
	go func() {
		defer m.waitGroup.Done()
		m.periodicSync(ctx)
	}()
	return nil
}

func (m *ComputeDomainStatusManager) Stop() error {
	if m.cancelContext != nil {
		m.cancelContext()
		m.cancelContext = nil
	}
	m.waitGroup.Wait()
	return nil
}

func (m *ComputeDomainStatusManager) periodicSync(ctx context.Context) {
	ticker := time.NewTicker(constants.StatusSyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.syncAll(ctx)
		}
	}
}

// syncAll refreshes ComputeDomain status from the current CDClique view. It
// performs no CDClique mutation: daemon entries are owned by the Device Plugin.
func (m *ComputeDomainStatusManager) syncAll(ctx context.Context) {
	computeDomains, err := m.listComputeDomains()
	if err != nil {
		klog.ErrorS(err, "Status sync failed to list ComputeDomains")
		return
	}
	cliques, err := m.cliqueManager.List()
	if err != nil {
		klog.ErrorS(err, "Status sync failed to list ComputeDomainCliques")
		return
	}

	cliquesByCD := groupCliquesByComputeDomainUID(cliques, m.labelCliqueDomain)
	var waitGroup sync.WaitGroup
	for _, cd := range computeDomains {
		cd := cd
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			m.syncComputeDomain(ctx, cd, cliquesByCD[string(cd.UID)])
		}()
	}
	waitGroup.Wait()
}

func groupCliquesByComputeDomainUID(cliques []*v1beta1.ComputeDomainClique, labelCliqueDomain string) map[string][]*v1beta1.ComputeDomainClique {
	result := make(map[string][]*v1beta1.ComputeDomainClique)
	for _, clique := range cliques {
		uid := clique.GetLabels()[labelCliqueDomain]
		if uid != "" {
			result[uid] = append(result[uid], clique)
		}
	}
	return result
}

func (m *ComputeDomainStatusManager) syncComputeDomain(ctx context.Context, cd *v1beta1.ComputeDomain, cliques []*v1beta1.ComputeDomainClique) {
	nodes := dedupeNodesByName(m.buildNodesFromCliques(cliques))
	if nodesEqual(cd.Status.Nodes, nodes) {
		return
	}
	copy := cd.DeepCopy()
	copy.Status.Nodes = nodes
	if _, err := m.updateComputeDomainStatus(ctx, copy); err != nil {
		klog.ErrorS(err, "Status sync failed to update ComputeDomain", "namespace", cd.Namespace, "name", cd.Name)
	}
}

func (m *ComputeDomainStatusManager) buildNodesFromCliques(cliques []*v1beta1.ComputeDomainClique) []v1beta1.ComputeDomainNode {
	var result []v1beta1.ComputeDomainNode
	for _, clique := range cliques {
		for _, daemon := range clique.Daemons {
			status := daemon.Status
			if status == "" {
				status = constants.NodeStatusNotReady
			}
			result = append(result, v1beta1.ComputeDomainNode{
				Name:      daemon.NodeName,
				IPAddress: daemon.IPAddress,
				CliqueID:  daemon.CliqueID,
				Index:     daemon.Index,
				Status:    status,
			})
		}
	}
	return result
}

func dedupeNodesByName(nodes []v1beta1.ComputeDomainNode) []v1beta1.ComputeDomainNode {
	seen := make(map[string]struct{}, len(nodes))
	result := make([]v1beta1.ComputeDomainNode, 0, len(nodes))
	for _, node := range nodes {
		if node.Name == "" {
			continue
		}
		if _, exists := seen[node.Name]; exists {
			continue
		}
		seen[node.Name] = struct{}{}
		result = append(result, node)
	}
	return result
}

func nodesEqual(a, b []v1beta1.ComputeDomainNode) bool {
	aMap := make(map[string]v1beta1.ComputeDomainNode, len(a))
	for _, node := range a {
		aMap[node.Name] = node
	}
	bMap := make(map[string]v1beta1.ComputeDomainNode, len(b))
	for _, node := range b {
		bMap[node.Name] = node
	}
	return maps.Equal(aMap, bMap)
}
