package computedomain

import (
	"context"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"github.com/lj-runtimekit/job-controller/pkg/constants"
)

type CleanupCallback func(context.Context, string) error

// LiveConfirmAbsentFunc performs a live API check to confirm a ComputeDomain
// truly does not exist. Returns (true, nil) when confirmed absent,
// (false, nil) when still present, or (false, err) on API failure.
type LiveConfirmAbsentFunc func(ctx context.Context, uid string) (bool, error)

// LiveListUIDsFunc performs a single live API List and returns the set of all
// existing ComputeDomain UIDs. Used by CleanupManager to batch-check orphans
// with one API call per cleanup round instead of one per object.
type LiveListUIDsFunc func(ctx context.Context) (map[string]struct{}, error)

// CleanupManager periodically scans a filtered informer for workload RCTs or
// Nodes whose parent ComputeDomain no longer exists. EnqueueCleanup coalesces bursts.
//
// labelKey is the GroupContract label key of the owning ComputeDomain: only the
// source group key is ever inspected, so Node/RCT cleanup can never act on
// objects owned by the other API group.
type CleanupManager struct {
	informer          cache.SharedIndexInformer
	getComputeDomain  GetComputeDomainFunc
	liveConfirmAbsent LiveConfirmAbsentFunc
	liveListUIDs      LiveListUIDsFunc
	callback          CleanupCallback
	labelKey          string
	interval          time.Duration

	waitGroup     sync.WaitGroup
	cancelContext context.CancelFunc
	queue         chan struct{}
}

func NewCleanupManager(informer cache.SharedIndexInformer, getComputeDomain GetComputeDomainFunc, liveConfirmAbsent LiveConfirmAbsentFunc, liveListUIDs LiveListUIDsFunc, labelKey string, callback CleanupCallback) *CleanupManager {
	return &CleanupManager{
		informer:          informer,
		getComputeDomain:  getComputeDomain,
		liveConfirmAbsent: liveConfirmAbsent,
		liveListUIDs:      liveListUIDs,
		callback:          callback,
		labelKey:          labelKey,
		interval:          constants.CleanupInterval,
		queue:             make(chan struct{}, 1),
	}
}

func (m *CleanupManager) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	m.cancelContext = cancel
	m.waitGroup.Add(2)
	go func() {
		defer m.waitGroup.Done()
		m.periodicCleanup(ctx)
	}()
	go func() {
		defer m.waitGroup.Done()
		m.cleanupWorker(ctx)
	}()
	return nil
}

func (m *CleanupManager) Stop() error {
	if m.cancelContext != nil {
		m.cancelContext()
		m.cancelContext = nil
	}
	m.waitGroup.Wait()
	return nil
}

func (m *CleanupManager) EnqueueCleanup() bool {
	select {
	case m.queue <- struct{}{}:
		return true
	default:
		return false
	}
}

// isOrphan returns true only when the ComputeDomain with the given UID is
// confirmed absent from both the informer cache AND a live API check.
// On any error it returns false (fail-safe: do not clean up).
func (m *CleanupManager) isOrphan(ctx context.Context, uid string) bool {
	cd, err := m.getComputeDomain(uid)
	if err != nil {
		klog.ErrorS(err, "Cleanup manager failed to get ComputeDomain from cache", "uid", uid)
		return false
	}
	if cd != nil {
		return false // cache hit → still exists
	}
	// Cache miss → live confirmation required
	if m.liveConfirmAbsent == nil {
		return true // no live check available, assume orphan (backward compat)
	}
	absent, err := m.liveConfirmAbsent(ctx, uid)
	if err != nil {
		klog.V(0).InfoS("Cleanup manager skipping cleanup: live API confirmation failed", "uid", uid, "err", err)
		return false
	}
	return absent
}

func (m *CleanupManager) cleanup(ctx context.Context) {
	items := m.informer.GetStore().List()

	// Phase 1: Collect candidate objects whose parent CD is a cache miss.
	type candidate struct {
		uid  string
		name string
	}
	var candidates []candidate
	cacheMissUIDs := make(map[string]struct{})

	for _, item := range items {
		u, ok := item.(*unstructured.Unstructured)
		if !ok {
			continue
		}
		uid := u.GetLabels()[m.labelKey]
		if uid == "" {
			continue
		}
		cd, err := m.getComputeDomain(uid)
		if err != nil {
			klog.ErrorS(err, "Cleanup manager failed to get ComputeDomain from cache", "uid", uid)
			continue
		}
		if cd != nil {
			continue // cache hit → still exists
		}
		candidates = append(candidates, candidate{uid: uid, name: u.GetName()})
		cacheMissUIDs[uid] = struct{}{}
	}

	if len(candidates) == 0 {
		return
	}

	// Phase 2: One live List to build the set of existing CD UIDs.
	// This replaces per-object LiveConfirmAbsent calls, reducing N API
	// calls to exactly one regardless of how many orphan candidates exist.
	var liveExistingUIDs map[string]struct{}
	if m.liveListUIDs != nil {
		var err error
		liveExistingUIDs, err = m.liveListUIDs(ctx)
		if err != nil {
			klog.Warningf("Cleanup manager skipping entire cleanup round: live CD List failed: %v", err)
			return
		}
		klog.V(4).InfoS("Cleanup manager live CD List completed",
			"existingCDs", len(liveExistingUIDs), "cacheMissUIDs", len(cacheMissUIDs))
	}

	// Phase 3: Execute callbacks for confirmed orphans, deduplicated by UID.
	// For Node cleanup the callback does a live List by UID (idempotent),
	// and for RCT cleanup the callback internally iterates all objects for
	// the UID, so calling once per UID is both correct and efficient.
	processedUIDs := make(map[string]struct{})
	for _, c := range candidates {
		if liveExistingUIDs != nil {
			if _, exists := liveExistingUIDs[c.uid]; exists {
				continue // live confirms it still exists
			}
		}
		if _, done := processedUIDs[c.uid]; done {
			continue
		}
		processedUIDs[c.uid] = struct{}{}
		if err := m.callback(ctx, c.uid); err != nil {
			klog.ErrorS(err, "Cleanup manager callback failed", "uid", c.uid, "resource", c.name)
		}
	}
}

func (m *CleanupManager) cleanupWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.queue:
			m.cleanup(ctx)
		}
	}
}

func (m *CleanupManager) periodicCleanup(ctx context.Context) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	m.cleanup(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.EnqueueCleanup()
		}
	}
}
