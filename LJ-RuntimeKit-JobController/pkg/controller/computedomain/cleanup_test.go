package computedomain

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"

	v1beta1 "github.com/lj-runtimekit/job-controller/pkg/apis/resource/v1beta1"
	"github.com/lj-runtimekit/job-controller/pkg/constants"
)

// fakeStore implements cache.Store for a fixed set of objects.
type fakeStore struct {
	items []interface{}
}

func (s *fakeStore) Add(obj interface{}) error                      { return nil }
func (s *fakeStore) Update(obj interface{}) error                   { return nil }
func (s *fakeStore) Delete(obj interface{}) error                   { return nil }
func (s *fakeStore) List() []interface{}                            { return s.items }
func (s *fakeStore) ListKeys() []string                             { return nil }
func (s *fakeStore) Get(obj interface{}) (interface{}, bool, error) { return nil, false, nil }
func (s *fakeStore) GetByKey(key string) (interface{}, bool, error) { return nil, false, nil }
func (s *fakeStore) Replace(list []interface{}, rv string) error    { return nil }
func (s *fakeStore) Resync() error                                  { return nil }

// fakeCleanupInformer is a minimal SharedIndexInformer that only returns a
// fakeStore. Used by CleanupManager tests.
type fakeCleanupInformer struct {
	cache.SharedIndexInformer // embed to satisfy the full interface
	store                     *fakeStore
}

func (f *fakeCleanupInformer) GetStore() cache.Store { return f.store }

// makeItem creates an Unstructured object with the given name and CD UID label.
func makeItem(name, uid string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Node",
		"metadata": map[string]interface{}{
			"name":   name,
			"labels": map[string]interface{}{constants.LabelComputeDomain: uid},
		},
	}}
}

// ---------- CleanupManager isOrphan with live confirmation ----------

// TestCleanupCacheMissLivePresent verifies that when the cache reports a CD
// as absent but the live API confirms it still exists, cleanup is NOT performed.
func TestCleanupCacheMissLivePresent(t *testing.T) {
	uid := "cache-miss-live-present"
	item := makeItem("orphan-node", uid)

	var cleanedUp atomic.Bool
	mgr := &CleanupManager{
		informer: &fakeCleanupInformer{store: &fakeStore{items: []interface{}{item}}},
		getComputeDomain: func(u string) (*v1beta1.ComputeDomain, error) {
			return nil, nil // cache miss
		},
		liveListUIDs: func(ctx context.Context) (map[string]struct{}, error) {
			return map[string]struct{}{uid: {}}, nil // live says: still exists
		},
		callback: func(ctx context.Context, u string) error {
			cleanedUp.Store(true)
			return nil
		},
		labelKey: constants.LabelComputeDomain,
		interval: constants.CleanupInterval,
		queue:    make(chan struct{}, 1),
	}

	mgr.cleanup(context.Background())
	if cleanedUp.Load() {
		t.Error("cleanup should NOT have run: live API confirmed CD still exists")
	}
}

// TestCleanupCacheMissLiveAbsent verifies that when both cache and live API
// confirm a CD is absent, cleanup IS performed.
func TestCleanupCacheMissLiveAbsent(t *testing.T) {
	uid := "cache-miss-live-absent"
	item := makeItem("orphan-node", uid)

	var cleanedUp atomic.Bool
	mgr := &CleanupManager{
		informer: &fakeCleanupInformer{store: &fakeStore{items: []interface{}{item}}},
		getComputeDomain: func(u string) (*v1beta1.ComputeDomain, error) {
			return nil, nil // cache miss
		},
		liveListUIDs: func(ctx context.Context) (map[string]struct{}, error) {
			return map[string]struct{}{}, nil // confirmed absent (empty set)
		},
		callback: func(ctx context.Context, u string) error {
			cleanedUp.Store(true)
			return nil
		},
		labelKey: constants.LabelComputeDomain,
		interval: constants.CleanupInterval,
		queue:    make(chan struct{}, 1),
	}

	mgr.cleanup(context.Background())
	if !cleanedUp.Load() {
		t.Error("cleanup should have run: both cache and live confirm CD absent")
	}
}

// TestCleanupCacheMissLiveAPIFailure verifies that when the live API check
// fails, cleanup is skipped (fail-safe behavior).
func TestCleanupCacheMissLiveAPIFailure(t *testing.T) {
	uid := "cache-miss-live-fail"
	item := makeItem("orphan-node", uid)

	var cleanedUp atomic.Bool
	mgr := &CleanupManager{
		informer: &fakeCleanupInformer{store: &fakeStore{items: []interface{}{item}}},
		getComputeDomain: func(u string) (*v1beta1.ComputeDomain, error) {
			return nil, nil // cache miss
		},
		liveListUIDs: func(ctx context.Context) (map[string]struct{}, error) {
			return nil, fmt.Errorf("simulated API failure")
		},
		callback: func(ctx context.Context, u string) error {
			cleanedUp.Store(true)
			return nil
		},
		labelKey: constants.LabelComputeDomain,
		interval: constants.CleanupInterval,
		queue:    make(chan struct{}, 1),
	}

	mgr.cleanup(context.Background())
	if cleanedUp.Load() {
		t.Error("cleanup should NOT have run: live API failed, must skip")
	}
}

// ---------- Batch dedup and API call count tests ----------

// TestCleanupBatchDedupSingleList verifies that multiple orphan objects
// referencing the same parent CD UID (or different UIDs) trigger exactly
// one live List call, not one per object.
func TestCleanupBatchDedupSingleList(t *testing.T) {
	sharedUID := "shared-uid-123"
	items := []interface{}{
		makeItem("orphan-1", sharedUID),
		makeItem("orphan-2", sharedUID),
		makeItem("orphan-3", sharedUID),
		makeItem("orphan-4", "other-uid-456"),
		makeItem("orphan-5", "other-uid-456"),
	}

	var listCallCount atomic.Int32
	var callbackCount atomic.Int32

	mgr := &CleanupManager{
		informer: &fakeCleanupInformer{store: &fakeStore{items: items}},
		getComputeDomain: func(u string) (*v1beta1.ComputeDomain, error) {
			return nil, nil // all cache misses
		},
		liveListUIDs: func(ctx context.Context) (map[string]struct{}, error) {
			listCallCount.Add(1)
			return map[string]struct{}{}, nil // all absent
		},
		callback: func(ctx context.Context, u string) error {
			callbackCount.Add(1)
			return nil
		},
		labelKey: constants.LabelComputeDomain,
		interval: constants.CleanupInterval,
		queue:    make(chan struct{}, 1),
	}

	mgr.cleanup(context.Background())

	if got := listCallCount.Load(); got != 1 {
		t.Errorf("expected exactly 1 List call, got %d", got)
	}
	// 5 objects but only 2 distinct UIDs — callback fires once per UID.
	if got := callbackCount.Load(); got != 2 {
		t.Errorf("expected 2 callback invocations (one per unique UID), got %d", got)
	}
}

// TestCleanupBatchListFailureSkipsAll verifies that when the live List fails,
// no cleanup callbacks are executed for ANY orphan candidate.
func TestCleanupBatchListFailureSkipsAll(t *testing.T) {
	items := []interface{}{
		makeItem("orphan-1", "uid-a"),
		makeItem("orphan-2", "uid-b"),
		makeItem("orphan-3", "uid-c"),
	}

	var callbackCount atomic.Int32

	mgr := &CleanupManager{
		informer: &fakeCleanupInformer{store: &fakeStore{items: items}},
		getComputeDomain: func(u string) (*v1beta1.ComputeDomain, error) {
			return nil, nil // all cache misses
		},
		liveListUIDs: func(ctx context.Context) (map[string]struct{}, error) {
			return nil, fmt.Errorf("simulated network error")
		},
		callback: func(ctx context.Context, u string) error {
			callbackCount.Add(1)
			return nil
		},
		labelKey: constants.LabelComputeDomain,
		interval: constants.CleanupInterval,
		queue:    make(chan struct{}, 1),
	}

	mgr.cleanup(context.Background())

	if got := callbackCount.Load(); got != 0 {
		t.Errorf("expected 0 callbacks when List fails, got %d", got)
	}
}

// TestCleanupBatchPartialOrphan verifies that when some UIDs exist in the
// live set and some don't, only truly orphaned objects get cleaned up.
func TestCleanupBatchPartialOrphan(t *testing.T) {
	items := []interface{}{
		makeItem("node-alive-1", "uid-alive"),
		makeItem("node-alive-2", "uid-alive"),
		makeItem("node-dead-1", "uid-dead"),
		makeItem("node-dead-2", "uid-dead"),
	}

	cleanedUIDs := make(map[string]int)
	mgr := &CleanupManager{
		informer: &fakeCleanupInformer{store: &fakeStore{items: items}},
		getComputeDomain: func(u string) (*v1beta1.ComputeDomain, error) {
			return nil, nil // all cache misses
		},
		liveListUIDs: func(ctx context.Context) (map[string]struct{}, error) {
			return map[string]struct{}{"uid-alive": {}}, nil // uid-alive still exists
		},
		callback: func(ctx context.Context, u string) error {
			cleanedUIDs[u]++
			return nil
		},
		labelKey: constants.LabelComputeDomain,
		interval: constants.CleanupInterval,
		queue:    make(chan struct{}, 1),
	}

	mgr.cleanup(context.Background())

	if _, found := cleanedUIDs["uid-alive"]; found {
		t.Error("uid-alive should NOT have been cleaned up")
	}
	if cleanedUIDs["uid-dead"] != 1 {
		t.Errorf("expected 1 callback for uid-dead (deduplicated by UID), got %d", cleanedUIDs["uid-dead"])
	}
}
