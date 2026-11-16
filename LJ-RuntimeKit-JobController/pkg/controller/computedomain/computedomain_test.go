package computedomain

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/component-base/metrics/legacyregistry"

	v1beta1 "github.com/lj-runtimekit/job-controller/pkg/apis/resource/v1beta1"
	"github.com/lj-runtimekit/job-controller/pkg/constants"
)

// ---------- removeString ----------

func TestRemoveStringPresent(t *testing.T) {
	result := removeString([]string{"a", "b", "c"}, "b")
	want := []string{"a", "c"}
	if diff := cmp.Diff(want, result); diff != "" {
		t.Errorf("removeString mismatch (-want +got):\n%s", diff)
	}
}

func TestRemoveStringAbsent(t *testing.T) {
	result := removeString([]string{"a", "b"}, "z")
	want := []string{"a", "b"}
	if diff := cmp.Diff(want, result); diff != "" {
		t.Errorf("removeString mismatch (-want +got):\n%s", diff)
	}
}

func TestRemoveStringEmpty(t *testing.T) {
	result := removeString(nil, "a")
	if len(result) != 0 {
		t.Errorf("expected empty slice, got %v", result)
	}
}

func TestRemoveStringDuplicates(t *testing.T) {
	result := removeString([]string{"a", "b", "a", "c"}, "a")
	want := []string{"b", "c"}
	if diff := cmp.Diff(want, result); diff != "" {
		t.Errorf("removeString mismatch (-want +got):\n%s", diff)
	}
}

// ---------- calculateGlobalStatus ----------

func newTestComputeDomainManager() *ComputeDomainManager {
	return &ComputeDomainManager{config: &ManagerConfig{}}
}

func TestCalculateGlobalStatusReady(t *testing.T) {
	m := newTestComputeDomainManager()
	cd := &v1beta1.ComputeDomain{}
	cd.Spec.NumNodes = 0
	cd.Status.Nodes = []v1beta1.ComputeDomainNode{
		{Name: "node-1", Status: constants.NodeStatusReady},
		{Name: "node-2", Status: constants.NodeStatusReady},
	}
	status := m.calculateGlobalStatus(cd)
	if status != constants.StatusReady {
		t.Errorf("expected %s, got %s", constants.StatusReady, status)
	}
}

func TestCalculateGlobalStatusNumNodesIgnored(t *testing.T) {
	// numNodes is deprecated and ignored; even with numNodes=4 and only 1
	// node, status is Ready because all present nodes are Ready.
	m := newTestComputeDomainManager()
	cd := &v1beta1.ComputeDomain{}
	cd.Spec.NumNodes = 4 // deprecated, ignored
	cd.Status.Nodes = []v1beta1.ComputeDomainNode{
		{Name: "node-1", Status: constants.NodeStatusReady},
	}
	status := m.calculateGlobalStatus(cd)
	if status != constants.StatusReady {
		t.Errorf("expected %s (numNodes is ignored), got %s", constants.StatusReady, status)
	}
}

func TestCalculateGlobalStatusOneNotReady(t *testing.T) {
	m := newTestComputeDomainManager()
	cd := &v1beta1.ComputeDomain{}
	cd.Spec.NumNodes = 0
	cd.Status.Nodes = []v1beta1.ComputeDomainNode{
		{Name: "node-1", Status: constants.NodeStatusReady},
		{Name: "node-2", Status: constants.NodeStatusNotReady},
	}
	status := m.calculateGlobalStatus(cd)
	if status != constants.StatusNotReady {
		t.Errorf("expected %s, got %s", constants.StatusNotReady, status)
	}
}

func TestCalculateGlobalStatusZeroNumNodes(t *testing.T) {
	m := newTestComputeDomainManager()
	cd := &v1beta1.ComputeDomain{}
	cd.Spec.NumNodes = 0
	cd.Status.Nodes = []v1beta1.ComputeDomainNode{
		{Name: "node-1", Status: constants.NodeStatusReady},
	}
	status := m.calculateGlobalStatus(cd)
	if status != constants.StatusReady {
		t.Errorf("expected %s (numNodes=0 means no minimum), got %s", constants.StatusReady, status)
	}
}

func TestCalculateGlobalStatusZeroNodesZeroRequirement(t *testing.T) {
	m := newTestComputeDomainManager()
	cd := &v1beta1.ComputeDomain{}
	cd.Spec.NumNodes = 0
	cd.Status.Nodes = nil
	status := m.calculateGlobalStatus(cd)
	if status != constants.StatusReady {
		t.Errorf("expected %s (numNodes=0, no nodes), got %s", constants.StatusReady, status)
	}
}

// ---------- toComputeDomain / fromComputeDomain round-trip ----------

func TestGetFiltersRecreatedObjectWithDifferentUID(t *testing.T) {
	const (
		oldUID = "old-uid"
		newUID = "new-uid"
	)
	recreated := &v1beta1.ComputeDomain{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "same-name",
			Namespace: "default",
			UID:       types.UID(newUID),
		},
	}
	u, err := fromComputeDomain(recreated)
	if err != nil {
		t.Fatalf("convert recreated ComputeDomain: %v", err)
	}
	m := &ComputeDomainManager{
		mutationCache: &directMutationCache{indexer: &fakeIndexer{
			// Simulate ByIndex selecting the old UID just before the informer
			// replaces the same namespace/name key with the recreated object.
			objects: map[string][]interface{}{oldUID: {u}},
		}},
	}

	got, err := m.Get(oldUID)
	if err != nil {
		t.Fatalf("Get(%q): %v", oldUID, err)
	}
	if got != nil {
		t.Fatalf("Get(%q) returned recreated object with UID %q", oldUID, got.UID)
	}
}

func TestRemoveFinalizerDoesNotTouchRecreatedObject(t *testing.T) {
	const (
		oldUID = "deleted-uid"
		newUID = "recreated-uid"
	)
	now := metav1.Now()
	recreated := &v1beta1.ComputeDomain{
		TypeMeta: metav1.TypeMeta{
			APIVersion: constants.APIGroup + "/" + constants.APIVersion,
			Kind:       "ComputeDomain",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              "same-name",
			Namespace:         "default",
			UID:               types.UID(newUID),
			ResourceVersion:   "2",
			Finalizers:        []string{constants.ComputeDomainFinalizer},
			DeletionTimestamp: &now,
		},
	}
	u, err := fromComputeDomain(recreated)
	if err != nil {
		t.Fatalf("convert recreated ComputeDomain: %v", err)
	}
	scheme := runtime.NewScheme()
	_ = v1beta1.AddToScheme(scheme)
	client := dynamicfake.NewSimpleDynamicClient(scheme, u)
	m := &ComputeDomainManager{
		config: &ManagerConfig{Contract: NVIDIAContract, ClientSets: ClientSets{Dynamic: client}},
		mutationCache: &directMutationCache{indexer: &fakeIndexer{
			objects: map[string][]interface{}{oldUID: {u}},
		}},
	}

	if err := m.removeFinalizer(context.Background(), oldUID); err != nil {
		t.Fatalf("removeFinalizer(%q): %v", oldUID, err)
	}
	got, err := client.Resource(computeDomainGVR).Namespace("default").Get(
		context.Background(), recreated.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get recreated ComputeDomain: %v", err)
	}
	if len(got.GetFinalizers()) != 1 || got.GetFinalizers()[0] != constants.ComputeDomainFinalizer {
		t.Fatalf("recreated object finalizers changed: %v", got.GetFinalizers())
	}
}

func TestComputeDomainMutationCacheDoesNotReturnDeletedMutation(t *testing.T) {
	backing := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{indexByUID: uidIndexer})
	base := &unstructured.Unstructured{}
	base.SetNamespace("default")
	base.SetName("test-cd")
	base.SetUID(types.UID("cache-uid"))
	base.SetResourceVersion("1")
	if err := backing.Add(base); err != nil {
		t.Fatalf("add backing object: %v", err)
	}

	mutationCache := newComputeDomainMutationCache(backing, backing)
	updated := base.DeepCopy()
	updated.SetResourceVersion("2")
	mutationCache.Mutation(updated)

	objects, err := mutationCache.ByIndex(indexByUID, "cache-uid")
	if err != nil {
		t.Fatalf("ByIndex before delete: %v", err)
	}
	if len(objects) != 1 || objects[0].(*unstructured.Unstructured).GetResourceVersion() != "2" {
		t.Fatalf("expected newer mutation while backing object exists, got %#v", objects)
	}

	if err := backing.Delete(base); err != nil {
		t.Fatalf("delete backing object: %v", err)
	}
	objects, err = mutationCache.ByIndex(indexByUID, "cache-uid")
	if err != nil {
		t.Fatalf("ByIndex after delete: %v", err)
	}
	if len(objects) != 0 {
		t.Fatalf("deleted ComputeDomain mutation remained visible: %#v", objects)
	}
}

func TestToFromComputeDomainRoundTrip(t *testing.T) {
	original := &v1beta1.ComputeDomain{
		TypeMeta: metav1.TypeMeta{
			APIVersion: constants.APIGroup + "/" + constants.APIVersion,
			Kind:       "ComputeDomain",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-cd",
			Namespace:  "default",
			UID:        types.UID("test-uid-123"),
			Finalizers: []string{constants.ComputeDomainFinalizer},
		},
		Spec: v1beta1.ComputeDomainSpec{
			NumNodes: 0,
			Channel: &v1beta1.ComputeDomainChannelSpec{
				ResourceClaimTemplate: v1beta1.ComputeDomainResourceClaimTemplate{
					Name: "my-rct",
				},
				AllocationMode: v1beta1.AllocationModeAll,
			},
		},
		Status: v1beta1.ComputeDomainStatus{
			Status: constants.StatusReady,
			Nodes: []v1beta1.ComputeDomainNode{
				{Name: "node-1", IPAddress: "10.0.0.1", CliqueID: "rack-a", Index: 0, Status: constants.NodeStatusReady},
			},
		},
	}

	u, err := fromComputeDomain(original)
	if err != nil {
		t.Fatalf("fromComputeDomain failed: %v", err)
	}
	roundTripped, err := toComputeDomain(u)
	if err != nil {
		t.Fatalf("toComputeDomain failed: %v", err)
	}

	if roundTripped.Name != original.Name {
		t.Errorf("Name mismatch: %s vs %s", roundTripped.Name, original.Name)
	}
	if roundTripped.Namespace != original.Namespace {
		t.Errorf("Namespace mismatch: %s vs %s", roundTripped.Namespace, original.Namespace)
	}
	if string(roundTripped.UID) != string(original.UID) {
		t.Errorf("UID mismatch: %s vs %s", roundTripped.UID, original.UID)
	}
	if roundTripped.Spec.NumNodes != 0 {
		t.Errorf("NumNodes should be 0, got %d", roundTripped.Spec.NumNodes)
	}
	if roundTripped.Status.Status != original.Status.Status {
		t.Errorf("Status mismatch: %s vs %s", roundTripped.Status.Status, original.Status.Status)
	}
	if roundTripped.Spec.Channel == nil {
		t.Fatal("Channel should not be nil")
	}
	if string(roundTripped.Spec.Channel.AllocationMode) != string(original.Spec.Channel.AllocationMode) {
		t.Errorf("AllocationMode mismatch: %s vs %s", roundTripped.Spec.Channel.AllocationMode, original.Spec.Channel.AllocationMode)
	}
}

// ---------- resourceKey ----------

func TestResourceKey(t *testing.T) {
	obj := &unstructured.Unstructured{}
	obj.SetNamespace("ns")
	obj.SetName("my-name")
	obj.SetUID(types.UID("uid-1"))
	key := resourceKey(obj)
	want := "ns/my-name/uid-1"
	if key != want {
		t.Errorf("resourceKey = %q, want %q", key, want)
	}
}

// ---------- unstructuredObject ----------

func TestUnstructuredObjectDirect(t *testing.T) {
	u := &unstructured.Unstructured{}
	u.SetName("test")
	result := unstructuredObject(u)
	if result == nil {
		t.Fatal("expected non-nil")
	}
	if result.GetName() != "test" {
		t.Errorf("expected name 'test', got %q", result.GetName())
	}
}

func TestUnstructuredObjectNonUnstructured(t *testing.T) {
	result := unstructuredObject("not-unstructured")
	if result != nil {
		t.Errorf("expected nil for non-unstructured input, got %v", result)
	}
}

// ---------- Metrics ----------

// resetMetricsState clears the per-UID metric tracking map so each test starts
// with a clean slate. The underlying GaugeVec is shared (registered once via
// sync.Once) but tests compare relative deltas or inspect the map directly.
func resetMetricsState() {
	computeDomainStatusMu.Lock()
	computeDomainLastStatus = nil
	computeDomainStatusMu.Unlock()
}

func alwaysComputeDomainExists(string) bool {
	return true
}

// nvidiaMetricKey is the group-qualified metric key used by a manager bound to
// the NVIDIA contract. Metrics are namespaced per API group so that two
// ComputeDomains with the same UID in different groups never share an entry.
func nvidiaMetricKey(uid string) string {
	return NVIDIAContract.APIGroup + "#" + uid
}

// getLastMetricStatus returns the last recorded metric status for a UID.
func getLastMetricStatus(uid string) (string, bool) {
	computeDomainStatusMu.Lock()
	defer computeDomainStatusMu.Unlock()
	status, ok := computeDomainLastStatus[uid]
	return status, ok
}

func TestObserveComputeDomainStatusIncrementsGauge(t *testing.T) {
	// Reset the last-status map for a clean test.
	resetMetricsState()

	uid := "test-metric-uid"
	ObserveComputeDomainStatus(uid, constants.StatusNotReady, alwaysComputeDomainExists)
	ObserveComputeDomainStatus(uid, constants.StatusNotReady, alwaysComputeDomainExists) // duplicate, should not double-count

	computeDomainStatusMu.Lock()
	status, ok := computeDomainLastStatus[uid]
	computeDomainStatusMu.Unlock()
	if !ok || status != constants.StatusNotReady {
		t.Errorf("expected last status %s, got %s (exists=%v)", constants.StatusNotReady, status, ok)
	}

	ObserveComputeDomainStatus(uid, constants.StatusReady, alwaysComputeDomainExists)
	computeDomainStatusMu.Lock()
	status = computeDomainLastStatus[uid]
	computeDomainStatusMu.Unlock()
	if status != constants.StatusReady {
		t.Errorf("expected status transition to Ready, got %s", status)
	}
}

func TestForgetComputeDomainRemovesFromTracker(t *testing.T) {
	uid := "test-forget-uid"
	ObserveComputeDomainStatus(uid, constants.StatusReady, alwaysComputeDomainExists)
	ForgetComputeDomain(uid)

	computeDomainStatusMu.Lock()
	_, exists := computeDomainLastStatus[uid]
	computeDomainStatusMu.Unlock()
	if exists {
		t.Error("expected UID to be removed from tracker after ForgetComputeDomain")
	}
}

func TestForgetPreventsLateStatusObserve(t *testing.T) {
	resetMetricsState()
	const uid = "delete-status-race-uid"
	exists := true
	before := computeDomainInfoValue(t, constants.StatusReady)
	ObserveComputeDomainStatus(uid, constants.StatusReady, func(string) bool { return exists })

	observeStarted := make(chan struct{})
	allowObserve := make(chan struct{})
	observeDone := make(chan struct{})
	go func() {
		close(observeStarted)
		<-allowObserve
		ObserveComputeDomainStatus(uid, constants.StatusNotReady, func(string) bool { return exists })
		close(observeDone)
	}()

	<-observeStarted
	// Simulate the informer store removing the object before its delete handler
	// forgets the metric, then let the delayed status observation resume.
	exists = false
	ForgetComputeDomain(uid)
	close(allowObserve)
	<-observeDone

	if _, exists := getLastMetricStatus(uid); exists {
		t.Fatal("late status observation resurrected deleted ComputeDomain metric")
	}
	after := computeDomainInfoValue(t, constants.StatusReady)
	if after != before {
		t.Fatalf("Ready gauge leaked after forced delete/status interleaving: before=%v after=%v", before, after)
	}
}

func TestObserveRejectsUIDMissingFromInformer(t *testing.T) {
	resetMetricsState()
	const uid = "deleted-uid"
	exists := true
	readyBefore := computeDomainInfoValue(t, constants.StatusReady)
	ObserveComputeDomainStatus(uid, constants.StatusReady, func(string) bool { return exists })

	// The informer removes the object before invoking the delete handler.
	exists = false
	ForgetComputeDomain(uid)
	notReadyBefore := computeDomainInfoValue(t, constants.StatusNotReady)
	ObserveComputeDomainStatus(uid, constants.StatusNotReady, func(string) bool { return exists })

	if _, found := getLastMetricStatus(uid); found {
		t.Fatal("late Observe restored metric for UID missing from informer")
	}
	if readyAfter := computeDomainInfoValue(t, constants.StatusReady); readyAfter != readyBefore {
		t.Fatalf("Ready gauge was not cleared: before=%v after=%v", readyBefore, readyAfter)
	}
	if notReadyAfter := computeDomainInfoValue(t, constants.StatusNotReady); notReadyAfter != notReadyBefore {
		t.Fatalf("late Observe changed NotReady gauge: before=%v after=%v", notReadyBefore, notReadyAfter)
	}
}

func TestForgetComputeDomainEmptyUID(t *testing.T) {
	// Should not panic
	ForgetComputeDomain("")
}

func TestObserveComputeDomainStatusEmptyUID(t *testing.T) {
	// Should not panic
	ObserveComputeDomainStatus("", "Ready", alwaysComputeDomainExists)
}

// ---------- UpdateStatus metric timing ----------

func TestUpdateStatusFailureDoesNotChangeMetrics(t *testing.T) {
	resetMetricsState()

	uid := "fail-update-metric-uid"
	// Seed the tracker so we can detect unwanted changes.
	ObserveComputeDomainStatus(uid, constants.StatusReady, alwaysComputeDomainExists)

	scheme := runtime.NewScheme()
	_ = v1beta1.AddToScheme(scheme)

	cd := &v1beta1.ComputeDomain{
		TypeMeta: metav1.TypeMeta{
			APIVersion: constants.APIGroup + "/" + constants.APIVersion,
			Kind:       "ComputeDomain",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test-cd",
			Namespace:       "default",
			UID:             types.UID(uid),
			ResourceVersion: "1",
		},
		Status: v1beta1.ComputeDomainStatus{
			Status: constants.StatusReady,
			Nodes: []v1beta1.ComputeDomainNode{
				{Name: "node-1", Status: constants.NodeStatusNotReady},
			},
		},
	}

	u, _ := fromComputeDomain(cd)
	client := dynamicfake.NewSimpleDynamicClient(scheme, u)

	// Inject a Conflict error for status sub-resource updates.
	client.PrependReactor("update", "computedomains", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() == "status" {
			return true, nil, apierrors.NewConflict(
				schema.GroupResource{Group: constants.APIGroup, Resource: constants.ComputeDomainResource},
				"test-cd",
				fmt.Errorf("resource version conflict"),
			)
		}
		return false, nil, nil
	})

	m := &ComputeDomainManager{
		config: &ManagerConfig{
			Contract:   NVIDIAContract,
			ClientSets: ClientSets{Dynamic: client},
		},
	}
	fakeIdx := newFakeIndexer(u)
	m.mutationCache = &directMutationCache{indexer: fakeIdx}

	// calculateGlobalStatus will return NotReady (node-1 is NotReady),
	// so UpdateStatus would try to transition Ready → NotReady.
	_, err := m.UpdateStatus(context.Background(), cd)
	if err == nil {
		t.Fatal("expected UpdateStatus to fail with Conflict")
	}

	// Metric must still show the original Ready status.
	status, ok := getLastMetricStatus(uid)
	if !ok {
		t.Fatal("expected UID to remain in metrics tracker after failed update")
	}
	if status != constants.StatusReady {
		t.Errorf("metric should still be %s after failed update, got %s", constants.StatusReady, status)
	}
}

func TestUpdateStatusSuccessMigratesMetrics(t *testing.T) {
	resetMetricsState()

	uid := "success-update-metric-uid"
	ObserveComputeDomainStatus(nvidiaMetricKey(uid), constants.StatusReady, alwaysComputeDomainExists)

	scheme := runtime.NewScheme()
	_ = v1beta1.AddToScheme(scheme)

	cd := &v1beta1.ComputeDomain{
		TypeMeta: metav1.TypeMeta{
			APIVersion: constants.APIGroup + "/" + constants.APIVersion,
			Kind:       "ComputeDomain",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test-cd",
			Namespace:       "default",
			UID:             types.UID(uid),
			ResourceVersion: "1",
		},
		Status: v1beta1.ComputeDomainStatus{
			Status: constants.StatusReady,
			Nodes: []v1beta1.ComputeDomainNode{
				{Name: "node-1", Status: constants.NodeStatusNotReady},
			},
		},
	}

	u, _ := fromComputeDomain(cd)
	client := dynamicfake.NewSimpleDynamicClient(scheme, u)

	m := &ComputeDomainManager{
		config: &ManagerConfig{
			Contract:   NVIDIAContract,
			ClientSets: ClientSets{Dynamic: client},
		},
	}
	fakeIdx := newFakeIndexer(u)
	m.mutationCache = &directMutationCache{indexer: fakeIdx}

	result, err := m.UpdateStatus(context.Background(), cd)
	if err != nil {
		t.Fatalf("UpdateStatus should succeed: %v", err)
	}

	// calculateGlobalStatus returns NotReady because node-1 is NotReady.
	if result.Status.Status != constants.StatusNotReady {
		t.Errorf("expected returned status %s, got %s", constants.StatusNotReady, result.Status.Status)
	}

	// Metric must have migrated from Ready to NotReady.
	status, ok := getLastMetricStatus(nvidiaMetricKey(uid))
	if !ok {
		t.Fatal("expected UID in metrics tracker after successful update")
	}
	if status != constants.StatusNotReady {
		t.Errorf("metric should be %s after successful update, got %s", constants.StatusNotReady, status)
	}
}

func TestUpdateGlobalStatusUnchangedSyncsMetrics(t *testing.T) {
	resetMetricsState()

	uid := "unchanged-sync-metric-uid"

	scheme := runtime.NewScheme()
	_ = v1beta1.AddToScheme(scheme)

	// CD with NotReady node whose global status is already NotReady.
	cd := &v1beta1.ComputeDomain{
		TypeMeta: metav1.TypeMeta{
			APIVersion: constants.APIGroup + "/" + constants.APIVersion,
			Kind:       "ComputeDomain",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cd",
			Namespace: "default",
			UID:       types.UID(uid),
		},
		Status: v1beta1.ComputeDomainStatus{
			Status: constants.StatusNotReady,
			Nodes: []v1beta1.ComputeDomainNode{
				{Name: "node-1", Status: constants.NodeStatusNotReady},
			},
		},
	}

	u, _ := fromComputeDomain(cd)
	client := dynamicfake.NewSimpleDynamicClient(scheme, u)

	m := &ComputeDomainManager{
		config: &ManagerConfig{
			Contract:   NVIDIAContract,
			ClientSets: ClientSets{Dynamic: client},
		},
	}
	fakeIdx := newFakeIndexer(u)
	m.mutationCache = &directMutationCache{indexer: fakeIdx}

	// No metric entry exists yet for this UID.
	_, exists := getLastMetricStatus(nvidiaMetricKey(uid))
	if exists {
		t.Fatal("precondition: UID should not be in metrics tracker yet")
	}

	// Status is unchanged (already NotReady), so no API update happens,
	// but the metric must still be synced.
	if err := m.updateGlobalStatus(context.Background(), cd); err != nil {
		t.Fatalf("updateGlobalStatus: %v", err)
	}

	status, ok := getLastMetricStatus(nvidiaMetricKey(uid))
	if !ok {
		t.Fatal("expected metric to be synced for unchanged status")
	}
	if status != constants.StatusNotReady {
		t.Errorf("expected metric %s, got %s", constants.StatusNotReady, status)
	}
}

// ---------- updateGlobalStatus with metrics ----------

func computeDomainInfoValue(t *testing.T, status string) float64 {
	t.Helper()
	mfs, err := legacyregistry.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Gather metrics failed: %v", err)
	}
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			if mf.GetName() == "ppu_dra_compute_domain_info" && m.GetLabel()[0].GetValue() == status {
				return m.GetGauge().GetValue()
			}
		}
	}
	return 0
}

func TestUpdateGlobalStatusObservesUnchangedStatus(t *testing.T) {
	// Clear metrics state
	computeDomainStatusMu.Lock()
	computeDomainLastStatus = nil
	computeDomainStatusMu.Unlock()

	scheme := runtime.NewScheme()
	_ = v1beta1.AddToScheme(scheme)

	cd := &v1beta1.ComputeDomain{
		TypeMeta: metav1.TypeMeta{
			APIVersion: constants.APIGroup + "/" + constants.APIVersion,
			Kind:       "ComputeDomain",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cd",
			Namespace: "default",
			UID:       types.UID("obs-test-uid"),
		},
		Spec: v1beta1.ComputeDomainSpec{
			NumNodes: 0,
		},
		Status: v1beta1.ComputeDomainStatus{
			Status: constants.StatusNotReady,
			Nodes: []v1beta1.ComputeDomainNode{
				{Name: "node-1", Status: constants.NodeStatusNotReady},
			},
		},
	}

	u, _ := fromComputeDomain(cd)
	client := dynamicfake.NewSimpleDynamicClient(scheme, u)

	m := &ComputeDomainManager{
		config: &ManagerConfig{
			Contract:   NVIDIAContract,
			ClientSets: ClientSets{Dynamic: client},
		},
	}
	// Provide a fakeInformer-like mutation cache. We use the real
	// cache with a simple indexer to satisfy the Get path.
	fakeIndexer := newFakeIndexer(u)
	m.mutationCache = &directMutationCache{indexer: fakeIndexer}

	before := computeDomainInfoValue(t, constants.StatusNotReady)
	if err := m.updateGlobalStatus(context.Background(), cd); err != nil {
		t.Fatalf("first updateGlobalStatus: %v", err)
	}
	// Second call with same status: should not double-count.
	if err := m.updateGlobalStatus(context.Background(), cd); err != nil {
		t.Fatalf("second updateGlobalStatus: %v", err)
	}
	after := computeDomainInfoValue(t, constants.StatusNotReady)
	if after != before+1 {
		t.Errorf("expected metric to increment by 1, got delta=%f", after-before)
	}
}

// ---------- Helpers for fake mutation cache / indexer ----------

type fakeIndexer struct {
	objects map[string][]interface{}
}

func newFakeIndexer(objects ...*unstructured.Unstructured) *fakeIndexer {
	idx := &fakeIndexer{objects: make(map[string][]interface{})}
	for _, o := range objects {
		uid := string(o.GetUID())
		idx.objects[uid] = append(idx.objects[uid], o)
	}
	return idx
}

func (f *fakeIndexer) ByIndex(indexName, indexedValue string) ([]interface{}, error) {
	return f.objects[indexedValue], nil
}

// directMutationCache wraps a simple indexer to fulfill the cache.MutationCache
// interface needed by ComputeDomainManager.Get.
type directMutationCache struct {
	indexer *fakeIndexer
}

func (d *directMutationCache) GetByKey(key string) (interface{}, bool, error) {
	return nil, false, nil
}
func (d *directMutationCache) ByIndex(name, indexValue string) ([]interface{}, error) {
	return d.indexer.ByIndex(name, indexValue)
}
func (d *directMutationCache) Mutation(interface{}) {}

// Verify interface conformance at compile time.
var _ interface {
	GetByKey(string) (interface{}, bool, error)
	ByIndex(string, string) ([]interface{}, error)
	Mutation(interface{})
} = &directMutationCache{}

// ---------- addFinalizer ----------

func TestAddFinalizerNew(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = v1beta1.AddToScheme(scheme)

	cd := &v1beta1.ComputeDomain{
		TypeMeta: metav1.TypeMeta{
			APIVersion: constants.APIGroup + "/" + constants.APIVersion,
			Kind:       "ComputeDomain",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test-cd",
			Namespace:       "default",
			UID:             types.UID("add-fin-uid"),
			ResourceVersion: "1",
		},
	}

	u, _ := fromComputeDomain(cd)
	client := dynamicfake.NewSimpleDynamicClient(scheme, u)

	m := &ComputeDomainManager{
		config: &ManagerConfig{
			Contract:   NVIDIAContract,
			ClientSets: ClientSets{Dynamic: client},
		},
	}
	fakeIdx := newFakeIndexer(u)
	m.mutationCache = &directMutationCache{indexer: fakeIdx}

	if err := m.addFinalizer(context.Background(), cd); err != nil {
		t.Fatalf("addFinalizer: %v", err)
	}

	// Verify the finalizer was added via the dynamic client.
	updated, err := client.Resource(schema.GroupVersionResource{
		Group:    constants.APIGroup,
		Version:  constants.APIVersion,
		Resource: constants.ComputeDomainResource,
	}).Namespace("default").Get(context.Background(), "test-cd", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get after addFinalizer: %v", err)
	}
	finalizers := updated.GetFinalizers()
	found := false
	for _, f := range finalizers {
		if f == constants.ComputeDomainFinalizer {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected finalizer %s in %v", constants.ComputeDomainFinalizer, finalizers)
	}
}

func TestAddFinalizerIdempotent(t *testing.T) {
	cd := &v1beta1.ComputeDomain{
		ObjectMeta: metav1.ObjectMeta{
			Finalizers: []string{constants.ComputeDomainFinalizer},
		},
	}
	m := &ComputeDomainManager{config: &ManagerConfig{Contract: NVIDIAContract}}
	// Should return nil immediately without touching client.
	if err := m.addFinalizer(context.Background(), cd); err != nil {
		t.Fatalf("addFinalizer (already present): %v", err)
	}
}

// ---------- GVR constants ----------

func TestGVRConstants(t *testing.T) {
	tests := []struct {
		name    string
		gvr     metav1.GroupVersionResource
		wantGrp string
		wantVer string
		wantRes string
	}{
		{"ComputeDomain", metav1.GroupVersionResource(computeDomainGVR), constants.APIGroup, constants.APIVersion, constants.ComputeDomainResource},
		{"ComputeDomainClique", metav1.GroupVersionResource(computeDomainCliqueGVR), constants.APIGroup, constants.APIVersion, constants.ComputeDomainCliqueResource},
		{"Node", metav1.GroupVersionResource(nodeGVR), "", "v1", "nodes"},
		{"ResourceClaimTemplate", metav1.GroupVersionResource(resourceClaimTemplateGVR), "resource.k8s.io", "v1", "resourceclaimtemplates"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.gvr.Group != tt.wantGrp {
				t.Errorf("Group: got %s, want %s", tt.gvr.Group, tt.wantGrp)
			}
			if tt.gvr.Version != tt.wantVer {
				t.Errorf("Version: got %s, want %s", tt.gvr.Version, tt.wantVer)
			}
			if tt.gvr.Resource != tt.wantRes {
				t.Errorf("Resource: got %s, want %s", tt.gvr.Resource, tt.wantRes)
			}
		})
	}
}

// ---------- AssertDaemonsRemoved (live API regression tests) ----------

// newDirectCliqueManager creates a ComputeDomainCliqueManager with only the
// config set (no informer/cache). Sufficient for AssertDaemonsRemoved which
// now uses the live API path exclusively.
func newDirectCliqueManager(ns string, client *dynamicfake.FakeDynamicClient) *ComputeDomainCliqueManager {
	return &ComputeDomainCliqueManager{
		config: &ManagerConfig{
			Contract:                     NVIDIAContract,
			ComputeDomainCliqueNamespace: ns,
			ClientSets:                   ClientSets{Dynamic: client},
		},
	}
}

// TestAssertDaemonsRemovedLiveDaemonsCacheMiss verifies that even when the
// informer cache is empty, the live API detects cliques with daemon entries.
func TestAssertDaemonsRemovedLiveDaemonsCacheMiss(t *testing.T) {
	scheme := runtime.NewScheme()
	clique := &v1beta1.ComputeDomainClique{
		TypeMeta: metav1.TypeMeta{
			APIVersion: constants.APIGroup + "/" + constants.APIVersion,
			Kind:       "ComputeDomainClique",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "live-uid.rack-a",
			Namespace: "test-ns",
			Labels:    map[string]string{constants.LabelComputeDomainCliqueDomain: "live-uid"},
		},
		Daemons: []*v1beta1.ComputeDomainDaemonInfo{
			{NodeName: "node-1", IPAddress: "10.0.0.1", CliqueID: "rack-a", Index: 0},
		},
	}
	u, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(clique)
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			computeDomainCliqueGVR: "ComputeDomainCliqueList",
		}, &unstructured.Unstructured{Object: u})

	// Use newDirectCliqueManager: NO informer started, so cache is empty.
	mgr := newDirectCliqueManager("test-ns", client)

	err := mgr.AssertDaemonsRemoved(context.Background(), "live-uid")
	if err == nil {
		t.Fatal("expected error: cache empty but live API has daemon entries")
	}
	if !strings.Contains(err.Error(), "still contain daemon entries") {
		t.Errorf("unexpected error: %v", err)
	}
	listAction, ok := client.Actions()[0].(clienttesting.ListAction)
	if !ok {
		t.Fatalf("expected first action to be a list, got %T", client.Actions()[0])
	}
	wantSelector := fmt.Sprintf("%s=%s", constants.LabelComputeDomainCliqueDomain, "live-uid")
	if got := listAction.GetListRestrictions().Labels.String(); got != wantSelector {
		t.Errorf("live list selector = %q, want %q", got, wantSelector)
	}
}

// TestAssertDaemonsRemovedPreconditionConflict verifies that a precondition
// conflict during delete returns an error (does not panic).
func TestAssertDaemonsRemovedPreconditionConflict(t *testing.T) {
	scheme := runtime.NewScheme()
	clique := &v1beta1.ComputeDomainClique{
		TypeMeta: metav1.TypeMeta{
			APIVersion: constants.APIGroup + "/" + constants.APIVersion,
			Kind:       "ComputeDomainClique",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "conflict-uid.rack-a",
			Namespace: "test-ns",
			Labels:    map[string]string{constants.LabelComputeDomainCliqueDomain: "conflict-uid"},
		},
		Daemons: nil, // empty clique
	}
	u, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(clique)
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			computeDomainCliqueGVR: "ComputeDomainCliqueList",
		}, &unstructured.Unstructured{Object: u})

	// Inject a reactor that returns a Conflict error for deletes.
	client.PrependReactor("delete", "computedomaincliques", func(action clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewConflict(
			schema.GroupResource{Group: constants.APIGroup, Resource: constants.ComputeDomainCliqueResource},
			"conflict-uid.rack-a",
			fmt.Errorf("resource version changed"),
		)
	})

	mgr := newDirectCliqueManager("test-ns", client)
	err := mgr.AssertDaemonsRemoved(context.Background(), "conflict-uid")
	if err == nil {
		t.Fatal("expected error on precondition conflict")
	}
	if !strings.Contains(err.Error(), "precondition conflict") {
		t.Errorf("expected precondition conflict error, got: %v", err)
	}
}

// TestAssertDaemonsRemovedLiveListFailure verifies that a failed live API List
// returns an error (does not silently succeed).
func TestAssertDaemonsRemovedLiveListFailure(t *testing.T) {
	scheme := runtime.NewScheme()
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			computeDomainCliqueGVR: "ComputeDomainCliqueList",
		})

	client.PrependReactor("list", "computedomaincliques", func(action clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("simulated API server unreachable")
	})

	mgr := newDirectCliqueManager("test-ns", client)
	err := mgr.AssertDaemonsRemoved(context.Background(), "any-uid")
	if err == nil {
		t.Fatal("expected error when live API List fails")
	}
	if !strings.Contains(err.Error(), "live list ComputeDomainCliques") {
		t.Errorf("unexpected error: %v", err)
	}
}

// ---------- AssertDaemonsRemoved ----------

// newFakeCliqueManager builds a ComputeDomainCliqueManager backed by a fake
// dynamic client. The given cliques are pre-loaded into the informer cache.
func newFakeCliqueManager(t *testing.T, ns string, cliques ...*v1beta1.ComputeDomainClique) (*ComputeDomainCliqueManager, context.CancelFunc) {
	t.Helper()
	// Use an empty scheme so the dynamic fake client stores and returns
	// unstructured objects as-is, matching real dynamic informer behaviour.
	scheme := runtime.NewScheme()

	objects := make([]runtime.Object, 0, len(cliques))
	for _, clique := range cliques {
		copy := clique.DeepCopy()
		if copy.APIVersion == "" {
			copy.APIVersion = constants.APIGroup + "/" + constants.APIVersion
		}
		if copy.Kind == "" {
			copy.Kind = "ComputeDomainClique"
		}
		u, err := runtime.DefaultUnstructuredConverter.ToUnstructured(copy)
		if err != nil {
			t.Fatalf("convert clique to unstructured: %v", err)
		}
		objects = append(objects, &unstructured.Unstructured{Object: u})
	}

	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			computeDomainCliqueGVR: "ComputeDomainCliqueList",
		}, objects...)

	mgr := NewComputeDomainCliqueManager(&ManagerConfig{
		Contract:                     NVIDIAContract,
		ComputeDomainCliqueNamespace: ns,
		ClientSets:                   ClientSets{Dynamic: client},
	})

	ctx, cancel := context.WithCancel(context.Background())
	if err := mgr.Start(ctx); err != nil {
		cancel()
		t.Fatalf("start clique manager: %v", err)
	}
	return mgr, cancel
}

func TestListByComputeDomainUIDUsesAliyunLabelOnly(t *testing.T) {
	matching := &v1beta1.ComputeDomainClique{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "name-does-not-encode-ownership",
			Namespace: "test-ns",
			Labels: map[string]string{
				constants.LabelComputeDomainCliqueDomain: "test-uid",
				constants.LabelComputeDomainClique:       "rack-a",
			},
		},
	}
	legacyOnly := &v1beta1.ComputeDomainClique{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-uid.rack-b",
			Namespace: "test-ns",
			Labels:    map[string]string{constants.LabelComputeDomain: "test-uid"},
		},
	}

	mgr, cancel := newFakeCliqueManager(t, "test-ns", matching, legacyOnly)
	defer cancel()
	defer mgr.Stop()

	cliques, err := mgr.ListByComputeDomainUID("test-uid")
	if err != nil {
		t.Fatalf("ListByComputeDomainUID: %v", err)
	}
	if len(cliques) != 1 || cliques[0].Name != matching.Name {
		t.Fatalf("expected only aliyun-labeled clique %q, got %#v", matching.Name, cliques)
	}
	got, err := mgr.Get("test-uid", "rack-a")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil || got.Name != matching.Name {
		t.Fatalf("Get should resolve by labels, got %#v", got)
	}
}

func TestAssertDaemonsRemovedNonEmpty(t *testing.T) {
	clique := &v1beta1.ComputeDomainClique{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-uid.rack-a",
			Namespace: "test-ns",
			Labels:    map[string]string{constants.LabelComputeDomainCliqueDomain: "test-uid"},
		},
		Daemons: []*v1beta1.ComputeDomainDaemonInfo{
			{NodeName: "node-1", IPAddress: "10.0.0.1", CliqueID: "rack-a", Index: 0},
		},
	}

	mgr, cancel := newFakeCliqueManager(t, "test-ns", clique)
	defer cancel()
	defer mgr.Stop()

	err := mgr.AssertDaemonsRemoved(context.Background(), "test-uid")
	if err == nil {
		t.Fatal("expected error when clique still has daemon entries")
	}
	if !strings.Contains(err.Error(), "still contain daemon entries") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestAssertDaemonsRemovedEmptyCliqueDeleted(t *testing.T) {
	clique := &v1beta1.ComputeDomainClique{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-uid.rack-a",
			Namespace: "test-ns",
			Labels:    map[string]string{constants.LabelComputeDomainCliqueDomain: "test-uid"},
		},
		Daemons: nil, // empty
	}

	mgr, cancel := newFakeCliqueManager(t, "test-ns", clique)
	defer cancel()
	defer mgr.Stop()

	err := mgr.AssertDaemonsRemoved(context.Background(), "test-uid")
	if err != nil {
		t.Fatalf("expected success for empty clique, got %v", err)
	}
}

func TestAssertDaemonsRemovedNoCLiques(t *testing.T) {
	mgr, cancel := newFakeCliqueManager(t, "test-ns")
	defer cancel()
	defer mgr.Stop()

	err := mgr.AssertDaemonsRemoved(context.Background(), "no-such-uid")
	if err != nil {
		t.Fatalf("expected success with no cliques, got %v", err)
	}
}

func TestAssertDaemonsRemovedAPIDeleteFailure(t *testing.T) {
	// Build empty clique so AssertDaemonsRemoved tries to delete it.
	clique := &v1beta1.ComputeDomainClique{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-uid.rack-a",
			Namespace: "test-ns",
			Labels:    map[string]string{constants.LabelComputeDomainCliqueDomain: "test-uid"},
		},
		Daemons: nil,
	}

	scheme := runtime.NewScheme()
	cliqueObj := clique.DeepCopy()
	cliqueObj.APIVersion = constants.APIGroup + "/" + constants.APIVersion
	cliqueObj.Kind = "ComputeDomainClique"
	u, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(cliqueObj)

	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			computeDomainCliqueGVR: "ComputeDomainCliqueList",
		}, &unstructured.Unstructured{Object: u})

	// Inject a reactor that fails deletes.
	client.PrependReactor("delete", "computedomaincliques", func(action clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("simulated API server failure")
	})

	mgr := NewComputeDomainCliqueManager(&ManagerConfig{
		Contract:                     NVIDIAContract,
		ComputeDomainCliqueNamespace: "test-ns",
		ClientSets:                   ClientSets{Dynamic: client},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("start clique manager: %v", err)
	}
	defer mgr.Stop()

	err := mgr.AssertDaemonsRemoved(ctx, "test-uid")
	if err == nil {
		t.Fatal("expected error when API delete fails")
	}
	if !strings.Contains(err.Error(), "simulated API server failure") {
		t.Errorf("unexpected error: %v", err)
	}
}

// ---------- reconcileDelete chain ----------

// TestReconcileDeleteBlockedByNonEmptyClique verifies that reconcileDelete
// returns an error (not removing the CD finalizer) when CDClique daemons have
// not been removed yet.
func TestReconcileDeleteBlockedByNonEmptyClique(t *testing.T) {
	scheme := runtime.NewScheme()

	cd := &v1beta1.ComputeDomain{
		TypeMeta: metav1.TypeMeta{
			APIVersion: constants.APIGroup + "/" + constants.APIVersion,
			Kind:       "ComputeDomain",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test-cd",
			Namespace:       "default",
			UID:             types.UID("del-test-uid"),
			ResourceVersion: "1",
			Finalizers:      []string{constants.ComputeDomainFinalizer},
			DeletionTimestamp: func() *metav1.Time {
				now := metav1.Now()
				return &now
			}(),
		},
	}

	clique := &v1beta1.ComputeDomainClique{
		TypeMeta: metav1.TypeMeta{
			APIVersion: constants.APIGroup + "/" + constants.APIVersion,
			Kind:       "ComputeDomainClique",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "del-test-uid.rack-a",
			Namespace: "test-ns",
			Labels:    map[string]string{constants.LabelComputeDomainCliqueDomain: "del-test-uid"},
		},
		Daemons: []*v1beta1.ComputeDomainDaemonInfo{
			{NodeName: "node-1", IPAddress: "10.0.0.1", CliqueID: "rack-a", Index: 0},
		},
	}

	cdU, _ := fromComputeDomain(cd)
	cliqueU, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(clique)

	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			computeDomainGVR:         "ComputeDomainList",
			computeDomainCliqueGVR:   "ComputeDomainCliqueList",
			resourceClaimTemplateGVR: "ResourceClaimTemplateList",
			nodeGVR:                  "NodeList",
		},
		cdU, &unstructured.Unstructured{Object: cliqueU})

	mcfg := &ManagerConfig{
		Contract:                     NVIDIAContract,
		DriverName:                   constants.DriverName,
		ComputeDomainCliqueNamespace: "test-ns",
		ClientSets:                   ClientSets{Dynamic: client},
		WorkQueue:                    newCallbackWorkQueue(),
	}

	mgr := NewComputeDomainManager(mcfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("start ComputeDomainManager: %v", err)
	}
	defer mgr.Stop()

	err := mgr.reconcileDelete(ctx, cd)
	if err == nil {
		t.Fatal("expected reconcileDelete to fail when CDClique has daemons")
	}
	// The error should come from AssertDaemonsRemoved.
	if !strings.Contains(err.Error(), "daemons") {
		t.Errorf("error should mention daemons: %v", err)
	}

	// The CD finalizer must still be present.
	cdAfter, err := mgr.Get("del-test-uid")
	if err != nil {
		t.Fatalf("Get after reconcileDelete: %v", err)
	}
	if cdAfter == nil {
		t.Fatal("CD should still exist")
	}
	for _, f := range cdAfter.Finalizers {
		if f == constants.ComputeDomainFinalizer {
			return // success – finalizer is retained
		}
	}
	t.Error("CD finalizer was removed prematurely")
}

// TestReconcileDeleteSucceedsWhenCliquesEmpty verifies that reconcileDelete
// completes (removes CD finalizer) when all CDClique daemons have been removed.
func TestReconcileDeleteSucceedsWhenCliquesEmpty(t *testing.T) {
	scheme := runtime.NewScheme()

	cd := &v1beta1.ComputeDomain{
		TypeMeta: metav1.TypeMeta{
			APIVersion: constants.APIGroup + "/" + constants.APIVersion,
			Kind:       "ComputeDomain",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test-cd",
			Namespace:       "default",
			UID:             types.UID("del-ok-uid"),
			ResourceVersion: "1",
			Finalizers:      []string{constants.ComputeDomainFinalizer},
			DeletionTimestamp: func() *metav1.Time {
				now := metav1.Now()
				return &now
			}(),
		},
	}

	// Empty clique – daemons already removed.
	clique := &v1beta1.ComputeDomainClique{
		TypeMeta: metav1.TypeMeta{
			APIVersion: constants.APIGroup + "/" + constants.APIVersion,
			Kind:       "ComputeDomainClique",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "del-ok-uid.rack-a",
			Namespace: "test-ns",
			Labels:    map[string]string{constants.LabelComputeDomainCliqueDomain: "del-ok-uid"},
		},
		Daemons: nil,
	}

	cdU, _ := fromComputeDomain(cd)
	cliqueU, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(clique)

	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			computeDomainGVR:         "ComputeDomainList",
			computeDomainCliqueGVR:   "ComputeDomainCliqueList",
			resourceClaimTemplateGVR: "ResourceClaimTemplateList",
			nodeGVR:                  "NodeList",
		},
		cdU, &unstructured.Unstructured{Object: cliqueU})

	mcfg := &ManagerConfig{
		Contract:                     NVIDIAContract,
		DriverName:                   constants.DriverName,
		ComputeDomainCliqueNamespace: "test-ns",
		ClientSets:                   ClientSets{Dynamic: client},
		WorkQueue:                    newCallbackWorkQueue(),
	}

	mgr := NewComputeDomainManager(mcfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("start ComputeDomainManager: %v", err)
	}
	defer mgr.Stop()

	err := mgr.reconcileDelete(ctx, cd)
	if err != nil {
		t.Fatalf("reconcileDelete should succeed with empty cliques: %v", err)
	}

	// Verify via the dynamic client that the CD finalizer was removed.
	// We read directly from the fake API instead of the mutation cache
	// because informer caches may not have synced yet in a test.
	updated, getErr := client.Resource(computeDomainGVR).Namespace("default").Get(
		ctx, "test-cd", metav1.GetOptions{})
	if getErr != nil {
		t.Fatalf("Get CD from API after reconcileDelete: %v", getErr)
	}
	for _, f := range updated.GetFinalizers() {
		if f == constants.ComputeDomainFinalizer {
			t.Error("CD finalizer should have been removed after successful reconcileDelete")
		}
	}
}

// ==========================================================================
// Namespace-separation tests: CDClique=default, DP Pod=kube-system
// ==========================================================================

// TestCDCliqueInformerUsesCliqueNamespace verifies that the CDClique informer
// scopes to ComputeDomainCliqueNamespace, independently of the driver /
// leader-election namespace carried by Config.Namespace.
func TestCDCliqueInformerUsesCliqueNamespace(t *testing.T) {
	scheme := runtime.NewScheme()

	cliqueNS := "default"

	// CDClique lives in "default".
	clique := &v1beta1.ComputeDomainClique{
		TypeMeta: metav1.TypeMeta{
			APIVersion: constants.APIGroup + "/" + constants.APIVersion,
			Kind:       "ComputeDomainClique",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ns-test-uid.rack-a",
			Namespace: cliqueNS,
			Labels:    map[string]string{constants.LabelComputeDomainCliqueDomain: "ns-test-uid"},
		},
		Daemons: nil, // empty
	}
	cliqueU, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(clique)

	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			computeDomainCliqueGVR: "ComputeDomainCliqueList",
		}, &unstructured.Unstructured{Object: cliqueU})

	cfg := &ManagerConfig{
		Contract:                     NVIDIAContract,
		ComputeDomainCliqueNamespace: cliqueNS,
		ClientSets:                   ClientSets{Dynamic: client},
	}

	// CDClique manager uses ComputeDomainCliqueNamespace.
	cliqueMgr := NewComputeDomainCliqueManager(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := cliqueMgr.Start(ctx); err != nil {
		t.Fatalf("start clique manager: %v", err)
	}
	defer cliqueMgr.Stop()

	// CDClique should be visible in informer (namespace=default).
	cliques, err := cliqueMgr.ListByComputeDomainUID("ns-test-uid")
	if err != nil {
		t.Fatalf("ListByComputeDomainUID: %v", err)
	}
	if len(cliques) != 1 {
		t.Fatalf("expected 1 clique in %q, got %d", cliqueNS, len(cliques))
	}
}

// TestAssertDaemonsRemovedUsesCliqueNamespace verifies that AssertDaemonsRemoved
// performs the live List in ComputeDomainCliqueNamespace ("default") rather than
// the driver / leader-election namespace.
func TestAssertDaemonsRemovedUsesCliqueNamespace(t *testing.T) {
	scheme := runtime.NewScheme()

	cliqueNS := "default"

	clique := &v1beta1.ComputeDomainClique{
		TypeMeta: metav1.TypeMeta{
			APIVersion: constants.APIGroup + "/" + constants.APIVersion,
			Kind:       "ComputeDomainClique",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "assert-uid.rack-a",
			Namespace: cliqueNS,
			Labels:    map[string]string{constants.LabelComputeDomainCliqueDomain: "assert-uid"},
		},
		Daemons: nil, // empty – should be deleted
	}
	cliqueU, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(clique)

	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			computeDomainCliqueGVR: "ComputeDomainCliqueList",
		}, &unstructured.Unstructured{Object: cliqueU})

	mgr := &ComputeDomainCliqueManager{
		config: &ManagerConfig{
			Contract:                     NVIDIAContract,
			ComputeDomainCliqueNamespace: cliqueNS,
			ClientSets:                   ClientSets{Dynamic: client},
		},
	}

	err := mgr.AssertDaemonsRemoved(context.Background(), "assert-uid")
	if err != nil {
		t.Fatalf("expected success for empty clique in %q: %v", cliqueNS, err)
	}

	// Verify the live list targeted cliqueNS.
	for _, action := range client.Actions() {
		if la, ok := action.(clienttesting.ListAction); ok {
			if la.GetNamespace() != cliqueNS {
				t.Errorf("live list namespace = %q, want %q", la.GetNamespace(), cliqueNS)
			}
		}
	}
}

// TestDeleteRejectsWrongNamespaceInAssertDaemonsRemoved verifies that
// AssertDaemonsRemoved's namespace guard rejects a CDClique whose namespace
// does not match the configured clique namespace. The guard is defense-in-depth;
// in practice the live list is already namespace-scoped.
// We inject a reactor to return a cross-namespace object in the list response.
func TestDeleteRejectsWrongNamespaceInAssertDaemonsRemoved(t *testing.T) {
	cliqueNS := "default"

	scheme := runtime.NewScheme()
	wrongClique := &v1beta1.ComputeDomainClique{
		TypeMeta: metav1.TypeMeta{
			APIVersion: constants.APIGroup + "/" + constants.APIVersion,
			Kind:       "ComputeDomainClique",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "del-guard-uid.rack-a",
			Namespace: "kube-system", // wrong — not cliqueNS
			Labels:    map[string]string{constants.LabelComputeDomainCliqueDomain: "del-guard-uid"},
		},
		Daemons: nil,
	}
	wrongU, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(wrongClique)

	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			computeDomainCliqueGVR: "ComputeDomainCliqueList",
		})

	// Inject a reactor that returns the wrong-namespace object regardless of
	// the requested namespace, simulating a hypothetical API bug.
	client.PrependReactor("list", "computedomaincliques", func(action clienttesting.Action) (bool, runtime.Object, error) {
		return true, &unstructured.UnstructuredList{
			Object: map[string]interface{}{"apiVersion": "v1", "kind": "List"},
			Items:  []unstructured.Unstructured{{Object: wrongU}},
		}, nil
	})

	mgr := &ComputeDomainCliqueManager{
		config: &ManagerConfig{
			Contract:                     NVIDIAContract,
			ComputeDomainCliqueNamespace: cliqueNS,
			ClientSets:                   ClientSets{Dynamic: client},
		},
	}

	err := mgr.AssertDaemonsRemoved(context.Background(), "del-guard-uid")
	if err == nil {
		t.Fatal("expected error when clique namespace mismatches clique NS config")
	}
	if !strings.Contains(err.Error(), "refusing to delete") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestConfigPropagationNamespacesIndependent verifies that NewController
// propagates both namespace fields independently into ManagerConfig.
func TestConfigPropagationNamespacesIndependent(t *testing.T) {
	cfg := Config{
		Namespace:                    "kube-system",
		ComputeDomainCliqueNamespace: "default",
		MaxNodes:                     18,
		IMEXIsolation:                "domain",
	}
	ctrl := NewController(cfg)
	if ctrl.config.Namespace != "kube-system" {
		t.Errorf("Config.Namespace = %q, want kube-system", ctrl.config.Namespace)
	}
	if ctrl.config.ComputeDomainCliqueNamespace != "default" {
		t.Errorf("Config.ComputeDomainCliqueNamespace = %q, want default", ctrl.config.ComputeDomainCliqueNamespace)
	}
}

// TestConfigPropagationDefaultCliqueNamespace verifies that an empty
// ComputeDomainCliqueNamespace in Config is defaulted to "default" by
// NewController.
func TestConfigPropagationDefaultCliqueNamespace(t *testing.T) {
	cfg := Config{
		Namespace: "kube-system",
		// ComputeDomainCliqueNamespace omitted
	}
	ctrl := NewController(cfg)
	if ctrl.config.ComputeDomainCliqueNamespace != "default" {
		t.Errorf("expected default clique namespace 'default', got %q", ctrl.config.ComputeDomainCliqueNamespace)
	}
}

// TestLeaseStillUsesDriverNamespace verifies at the Config level that the leader
// election namespace (carried by --namespace and used by main.go to build the
// Lease) stays independent of ComputeDomainCliqueNamespace. The Lease itself is
// created in main.go from the --namespace flag, not from manager configuration.
func TestLeaseStillUsesDriverNamespace(t *testing.T) {
	cfg := Config{
		Namespace:                    "kube-system",
		ComputeDomainCliqueNamespace: "default",
	}
	ctrl := NewController(cfg)
	if ctrl.config.Namespace != "kube-system" {
		t.Errorf("Config.Namespace = %q, want kube-system", ctrl.config.Namespace)
	}
	if ctrl.managers[0].config.ComputeDomainCliqueNamespace != "default" {
		t.Errorf("ManagerConfig.ComputeDomainCliqueNamespace = %q, want default", ctrl.managers[0].config.ComputeDomainCliqueNamespace)
	}
}

// TestReconcileDeleteProceedsAfterDaemonDeregistration pins the deletion
// handshake end to end: a non-empty daemon list blocks ComputeDomain deletion,
// and once the Device Plugin deregisters the daemon the controller continues
// without ever mutating daemon entries itself.
func TestReconcileDeleteProceedsAfterDaemonDeregistration(t *testing.T) {
	scheme := runtime.NewScheme()

	cd := &v1beta1.ComputeDomain{
		TypeMeta: metav1.TypeMeta{
			APIVersion: constants.APIGroup + "/" + constants.APIVersion,
			Kind:       "ComputeDomain",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test-cd",
			Namespace:       "default",
			UID:             types.UID("dereg-uid"),
			ResourceVersion: "1",
			Finalizers:      []string{constants.ComputeDomainFinalizer},
			DeletionTimestamp: func() *metav1.Time {
				now := metav1.Now()
				return &now
			}(),
		},
	}
	clique := &v1beta1.ComputeDomainClique{
		TypeMeta: metav1.TypeMeta{
			APIVersion: constants.APIGroup + "/" + constants.APIVersion,
			Kind:       "ComputeDomainClique",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "dereg-uid.rack-a",
			Namespace:       "test-ns",
			Labels:          map[string]string{constants.LabelComputeDomainCliqueDomain: "dereg-uid"},
			ResourceVersion: "1",
		},
		Daemons: []*v1beta1.ComputeDomainDaemonInfo{
			{NodeName: "node-1", IPAddress: "10.0.0.1", CliqueID: "rack-a", Index: 0},
		},
	}

	cdU, _ := fromComputeDomain(cd)
	cliqueU, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(clique)

	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			computeDomainGVR:         "ComputeDomainList",
			computeDomainCliqueGVR:   "ComputeDomainCliqueList",
			resourceClaimTemplateGVR: "ResourceClaimTemplateList",
			nodeGVR:                  "NodeList",
		},
		cdU, &unstructured.Unstructured{Object: cliqueU})

	mcfg := &ManagerConfig{
		Contract:                     NVIDIAContract,
		DriverName:                   constants.DriverName,
		ComputeDomainCliqueNamespace: "test-ns",
		ClientSets:                   ClientSets{Dynamic: client},
		WorkQueue:                    newCallbackWorkQueue(),
	}

	mgr := NewComputeDomainManager(mcfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("start ComputeDomainManager: %v", err)
	}
	defer mgr.Stop()

	// 1. A registered daemon blocks deletion and the CD finalizer stays put.
	err := mgr.reconcileDelete(ctx, cd)
	if err == nil {
		t.Fatal("expected deletion to be blocked while a daemon is registered")
	}
	if !strings.Contains(err.Error(), "daemons") {
		t.Errorf("error should mention daemons: %v", err)
	}

	// The controller never removes daemon entries by itself.
	stored, err := client.Resource(computeDomainCliqueGVR).Namespace("test-ns").Get(ctx, clique.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get clique after blocked delete: %v", err)
	}
	storedClique, err := toComputeDomainClique(stored)
	if err != nil {
		t.Fatalf("convert clique: %v", err)
	}
	if len(storedClique.Daemons) != 1 {
		t.Fatalf("controller must not remove daemon entries, got %#v", storedClique.Daemons)
	}

	// 2. The Device Plugin deregisters its daemon entry.
	deregistered := stored.DeepCopy()
	if err := unstructured.SetNestedSlice(deregistered.Object, []interface{}{}, "daemons"); err != nil {
		t.Fatalf("clear daemons: %v", err)
	}
	if _, err := client.Resource(computeDomainCliqueGVR).Namespace("test-ns").Update(ctx, deregistered, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("deregister daemon: %v", err)
	}

	// 3. Deletion continues and the empty clique is removed safely.
	if err := mgr.reconcileDelete(ctx, cd); err != nil {
		t.Fatalf("deletion should proceed after DP deregistration: %v", err)
	}
	afterCD, err := client.Resource(computeDomainGVR).Namespace("default").Get(ctx, "test-cd", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get CD after successful delete: %v", err)
	}
	for _, f := range afterCD.GetFinalizers() {
		if f == constants.ComputeDomainFinalizer {
			t.Error("CD finalizer should have been removed after DP deregistration")
		}
	}
	if _, err := client.Resource(computeDomainCliqueGVR).Namespace("test-ns").Get(ctx, clique.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("expected empty ComputeDomainClique to be deleted, got err=%v", err)
	}
}
