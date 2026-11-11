package computedomain

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	v1beta1 "github.com/lj-runtimekit/job-controller/pkg/apis/resource/v1beta1"
	cdapi "github.com/lj-runtimekit/job-controller/pkg/computedomain"
	"github.com/lj-runtimekit/job-controller/pkg/constants"
)

// ---------------------------------------------------------------------------
// Shared helpers for the dual API group matrix
// ---------------------------------------------------------------------------

// chdirRepoRoot makes the packaged workload RCT template
// (templates/workload-claim-template.tmpl.yaml) reachable from tests that
// exercise the real Create path. Tests in this package never run in parallel,
// so the working directory change cannot leak into another test.
func chdirRepoRoot(t *testing.T) {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	root, err := filepath.Abs(filepath.Join(wd, "..", "..", ".."))
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "templates", "workload-claim-template.tmpl.yaml")); err != nil {
		t.Fatalf("repo root template not found: %v", err)
	}
	if err := os.Chdir(root); err != nil {
		t.Fatalf("chdir %s: %v", root, err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(wd); err != nil {
			t.Errorf("restore working directory: %v", err)
		}
	})
}

// newDualGroupFakeClient builds a fake dynamic client that knows the list kinds
// of both ComputeDomain API groups plus the group-independent resources, so a
// single client can back two isolated manager sets. The scheme is intentionally
// left empty (unstructured only): registering the typed API types would make the
// tracker build typed list objects it cannot fill from unstructured items.
func newDualGroupFakeClient(t *testing.T, objects ...runtime.Object) *dynamicfake.FakeDynamicClient {
	t.Helper()
	scheme := runtime.NewScheme()
	listKinds := map[schema.GroupVersionResource]string{
		resourceClaimTemplateGVR: "ResourceClaimTemplateList",
		nodeGVR:                  "NodeList",
	}
	for _, contract := range cdapi.SupportedContracts() {
		listKinds[contract.ComputeDomainGVR] = "ComputeDomainList"
		listKinds[contract.ComputeDomainCliqueGVR] = "ComputeDomainCliqueList"
	}
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, objects...)
}

func newGroupComputeDomain(contract cdapi.GroupContract, name, namespace, uid, rctName string) *v1beta1.ComputeDomain {
	return &v1beta1.ComputeDomain{
		TypeMeta: metav1.TypeMeta{
			APIVersion: contract.GroupVersion.String(),
			Kind:       "ComputeDomain",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       namespace,
			UID:             types.UID(uid),
			ResourceVersion: "1",
		},
		Spec: v1beta1.ComputeDomainSpec{
			Channel: &v1beta1.ComputeDomainChannelSpec{
				ResourceClaimTemplate: v1beta1.ComputeDomainResourceClaimTemplate{Name: rctName},
			},
		},
	}
}

func newGroupClique(contract cdapi.GroupContract, name, namespace, uid string, daemons []*v1beta1.ComputeDomainDaemonInfo) *v1beta1.ComputeDomainClique {
	return &v1beta1.ComputeDomainClique{
		TypeMeta: metav1.TypeMeta{
			APIVersion: contract.GroupVersion.String(),
			Kind:       "ComputeDomainClique",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       namespace,
			UID:             types.UID("clique-" + contract.APIGroup),
			ResourceVersion: "1",
			Labels:          map[string]string{contract.LabelCliqueDomain: uid},
		},
		Daemons: daemons,
	}
}

// newGroupManagerConfig builds a manager config bound to one contract.
//
// The shared callback queue is shut down when the test ends: constructing it
// already starts the client-go waitingLoop goroutine, and a test that never runs
// or stops the manager would otherwise leak it.
func newGroupManagerConfig(t *testing.T, contract cdapi.GroupContract, client dynamic.Interface, cliqueNS string) *ManagerConfig {
	t.Helper()
	config := &ManagerConfig{
		Contract:                     contract,
		DriverName:                   constants.DriverName,
		ComputeDomainCliqueNamespace: cliqueNS,
		ClientSets:                   ClientSets{Dynamic: client},
		WorkQueue:                    newCallbackWorkQueue(),
	}
	t.Cleanup(config.WorkQueue.ShutDown)
	return config
}

// startGroupManager starts one isolated manager set and registers its teardown.
func startGroupManager(t *testing.T, contract cdapi.GroupContract, client dynamic.Interface, cliqueNS string) *ComputeDomainManager {
	t.Helper()
	manager := NewComputeDomainManager(newGroupManagerConfig(t, contract, client, cliqueNS))
	ctx, cancel := context.WithCancel(context.Background())
	if err := manager.Start(ctx); err != nil {
		cancel()
		t.Fatalf("start %s ComputeDomain manager: %v", contract.APIGroup, err)
	}
	t.Cleanup(func() {
		_ = manager.Stop()
		cancel()
	})
	return manager
}

// ---------------------------------------------------------------------------
// Fixed dual group wiring (no --compute-domain-api-groups flag exists)
// ---------------------------------------------------------------------------

// TestNewControllerAlwaysServesBothGroups pins that a controller built from a
// zero Config still serves both API groups: there is no flag, default or subset
// selection left in the wiring.
func TestNewControllerAlwaysServesBothGroups(t *testing.T) {
	ctrl := mustNewController(t, Config{})

	if got, want := strings.Join(ctrl.ServedAPIGroups(), ","),
		cdapi.GroupAliyun+","+cdapi.GroupNVIDIA; got != want {
		t.Fatalf("ServedAPIGroups() = %q, want %q", got, want)
	}
	if len(ctrl.managers) != 2 {
		t.Fatalf("controller has %d manager sets, want 2", len(ctrl.managers))
	}
	if ctrl.managers[0].config.Contract != AliyunContract {
		t.Fatalf("first manager contract = %q, want %q",
			ctrl.managers[0].config.Contract.APIGroup, cdapi.GroupAliyun)
	}
	if ctrl.managers[1].config.Contract != NVIDIAContract {
		t.Fatalf("second manager contract = %q, want %q",
			ctrl.managers[1].config.Contract.APIGroup, cdapi.GroupNVIDIA)
	}
}

// TestNewControllerDualGroupsAreIsolated pins the isolation contract: two
// group manager sets with distinct informers, caches and indexers, sharing only
// the callback queue.
func TestNewControllerDualGroupsAreIsolated(t *testing.T) {
	ctrl := mustNewController(t, Config{})

	aliyun, nvidia := ctrl.managers[0], ctrl.managers[1]
	if aliyun.config.Contract != AliyunContract || nvidia.config.Contract != NVIDIAContract {
		t.Fatalf("manager contracts = %q,%q want aliyun,nvidia",
			aliyun.config.Contract.APIGroup, nvidia.config.Contract.APIGroup)
	}

	if aliyun.informer == nvidia.informer {
		t.Fatal("ComputeDomain informers must not be shared across groups")
	}
	if aliyun.factory == nvidia.factory {
		t.Fatal("informer factories must not be shared across groups")
	}
	if aliyun.nodeManager.informer == nvidia.nodeManager.informer {
		t.Fatal("Node informers must not be shared across groups")
	}
	if aliyun.cliqueManager.informer == nvidia.cliqueManager.informer {
		t.Fatal("ComputeDomainClique informers must not be shared across groups")
	}
	if aliyun.resourceClaimTemplateManager.informer == nvidia.resourceClaimTemplateManager.informer {
		t.Fatal("ResourceClaimTemplate informers must not be shared across groups")
	}

	if ctrl.queue == nil {
		t.Fatal("controller must own a callback queue")
	}
	if aliyun.config.WorkQueue != ctrl.queue || nvidia.config.WorkQueue != ctrl.queue {
		t.Fatal("both manager sets must share the single controller callback queue")
	}
}

// ---------------------------------------------------------------------------
// Group-qualified queue keys and metric keys
// ---------------------------------------------------------------------------

func TestQueueKeyIsQualifiedByAPIGroup(t *testing.T) {
	obj := &unstructured.Unstructured{}
	obj.SetNamespace("default")
	obj.SetName("cd")
	obj.SetUID("shared-uid")

	aliyunKey := queueKey(AliyunContract, obj)
	nvidiaKey := queueKey(NVIDIAContract, obj)

	if aliyunKey == nvidiaKey {
		t.Fatalf("queue keys must differ per group, both %q", aliyunKey)
	}
	if want := cdapi.GroupAliyun + "#default/cd/shared-uid"; aliyunKey != want {
		t.Errorf("aliyun queue key = %q, want %q", aliyunKey, want)
	}
	if want := cdapi.GroupNVIDIA + "#default/cd/shared-uid"; nvidiaKey != want {
		t.Errorf("nvidia queue key = %q, want %q", nvidiaKey, want)
	}
}

// TestSharedQueueKeepsSameUIDItemsApart proves that two synthetic objects with
// the same namespace/name/UID in different groups are queued as separate items
// and both callbacks run.
func TestSharedQueueKeepsSameUIDItemsApart(t *testing.T) {
	queue := newCallbackWorkQueue()
	t.Cleanup(queue.ShutDown)
	ctx := context.Background()

	newObject := func(group string) *unstructured.Unstructured {
		obj := &unstructured.Unstructured{}
		obj.SetNamespace("default")
		obj.SetName("cd")
		obj.SetUID("shared-uid")
		obj.SetLabels(map[string]string{"group": group})
		return obj
	}

	var mu sync.Mutex
	var seen []string
	callback := func(_ context.Context, obj interface{}) error {
		u := obj.(*unstructured.Unstructured)
		mu.Lock()
		seen = append(seen, u.GetLabels()["group"])
		mu.Unlock()
		return nil
	}

	aliyunObj, nvidiaObj := newObject(cdapi.GroupAliyun), newObject(cdapi.GroupNVIDIA)
	queue.EnqueueWithKey(aliyunObj, queueKey(AliyunContract, aliyunObj), callback)
	queue.EnqueueWithKey(nvidiaObj, queueKey(NVIDIAContract, nvidiaObj), callback)

	for i := 0; i < 2; i++ {
		if !queue.processNext(ctx) {
			t.Fatalf("queue shut down after %d items", i)
		}
	}

	sort.Strings(seen)
	if strings.Join(seen, ",") != cdapi.GroupAliyun+","+cdapi.GroupNVIDIA {
		t.Fatalf("callbacks seen = %v, want one per group", seen)
	}

	queue.mu.Lock()
	defer queue.mu.Unlock()
	if len(queue.activeOps) != 0 {
		t.Fatalf("activeOps not drained: %v", queue.activeOps)
	}
}

func TestMetricKeyIsQualifiedByAPIGroup(t *testing.T) {
	resetMetricsState()
	ctrl := mustNewController(t, Config{})
	aliyun, nvidia := ctrl.managers[0], ctrl.managers[1]

	const uid = "shared-uid"
	aliyunKey, nvidiaKey := aliyun.metricKey(uid), nvidia.metricKey(uid)
	if aliyunKey == nvidiaKey {
		t.Fatalf("metric keys collide: %q", aliyunKey)
	}
	if want := cdapi.GroupAliyun + "#" + uid; aliyunKey != want {
		t.Errorf("aliyun metric key = %q, want %q", aliyunKey, want)
	}
	if want := cdapi.GroupNVIDIA + "#" + uid; nvidiaKey != want {
		t.Errorf("nvidia metric key = %q, want %q", nvidiaKey, want)
	}

	ObserveComputeDomainStatus(aliyunKey, constants.StatusReady, alwaysComputeDomainExists)
	ObserveComputeDomainStatus(nvidiaKey, constants.StatusNotReady, alwaysComputeDomainExists)

	if status, ok := getLastMetricStatus(aliyunKey); !ok || status != constants.StatusReady {
		t.Errorf("aliyun metric = %q,%v want %q", status, ok, constants.StatusReady)
	}
	if status, ok := getLastMetricStatus(nvidiaKey); !ok || status != constants.StatusNotReady {
		t.Errorf("nvidia metric = %q,%v want %q", status, ok, constants.StatusNotReady)
	}
}

// ---------------------------------------------------------------------------
// Create / status path per group
// ---------------------------------------------------------------------------

// TestDualGroupReconcileCreatesGroupScopedRCTs drives the real reconcile path
// for both groups against one cluster: each ResourceClaimTemplate must carry
// only its own group's labels, finalizer and opaque apiVersion, and each
// ComputeDomain status must be written through its own group GVR.
func TestDualGroupReconcileCreatesGroupScopedRCTs(t *testing.T) {
	chdirRepoRoot(t)

	aliyunCD := newGroupComputeDomain(AliyunContract, "cd-aliyun", "default", "uid-aliyun", "rct-aliyun")
	nvidiaCD := newGroupComputeDomain(NVIDIAContract, "cd-nvidia", "default", "uid-nvidia", "rct-nvidia")

	client := newDualGroupFakeClient(t, mustUnstructured(t, aliyunCD), mustUnstructured(t, nvidiaCD))
	aliyunMgr := startGroupManager(t, AliyunContract, client, "default")
	nvidiaMgr := startGroupManager(t, NVIDIAContract, client, "default")

	ctx := context.Background()
	if err := aliyunMgr.onAddOrUpdate(ctx, mustUnstructured(t, aliyunCD)); err != nil {
		t.Fatalf("reconcile aliyun ComputeDomain: %v", err)
	}
	if err := nvidiaMgr.onAddOrUpdate(ctx, mustUnstructured(t, nvidiaCD)); err != nil {
		t.Fatalf("reconcile nvidia ComputeDomain: %v", err)
	}

	assertRCTBoundToContract(t, ctx, client, "default", "rct-aliyun", AliyunContract, "uid-aliyun")
	assertRCTBoundToContract(t, ctx, client, "default", "rct-nvidia", NVIDIAContract, "uid-nvidia")

	assertComputeDomainStatus(t, ctx, client, AliyunContract, "cd-aliyun", constants.StatusReady)
	assertComputeDomainStatus(t, ctx, client, NVIDIAContract, "cd-nvidia", constants.StatusReady)
}

// TestComputeDomainFinalizerIsGroupScoped pins the group-scoped finalizer that
// the controller writes on ComputeDomains. It is asserted on the addFinalizer
// path because the fake client rewrites metadata on status updates.
func TestComputeDomainFinalizerIsGroupScoped(t *testing.T) {
	ctx := context.Background()
	for _, contract := range cdapi.SupportedContracts() {
		source := contract
		t.Run(source.APIGroup, func(t *testing.T) {
			cd := newGroupComputeDomain(source, "cd-"+source.APIGroup, "default", "uid-"+source.APIGroup, "rct-"+source.APIGroup)
			client := newDualGroupFakeClient(t, mustUnstructured(t, cd))
			mgr := startGroupManager(t, source, client, "default")

			if err := mgr.addFinalizer(ctx, cd); err != nil {
				t.Fatalf("addFinalizer: %v", err)
			}
			assertComputeDomainFinalizer(t, ctx, client, source, cd.Name, source.Finalizer)
		})
	}
}

func assertRCTBoundToContract(t *testing.T, ctx context.Context, client dynamic.Interface, namespace, name string, contract cdapi.GroupContract, uid string) {
	t.Helper()
	rct, err := client.Resource(resourceClaimTemplateGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get ResourceClaimTemplate %s/%s: %v", namespace, name, err)
	}
	labels := rct.GetLabels()
	if got := labels[contract.LabelComputeDomain]; got != uid {
		t.Errorf("%s label = %q, want %q", contract.LabelComputeDomain, got, uid)
	}
	if got := labels[contract.LabelTarget]; got != constants.RCTTargetWorkload {
		t.Errorf("%s label = %q, want %q", contract.LabelTarget, got, constants.RCTTargetWorkload)
	}
	for _, other := range cdapi.SupportedContracts() {
		if other.APIGroup == contract.APIGroup {
			continue
		}
		if _, found := labels[other.LabelComputeDomain]; found {
			t.Errorf("RCT %s carries the foreign %s label", name, other.LabelComputeDomain)
		}
		if _, found := labels[other.LabelTarget]; found {
			t.Errorf("RCT %s carries the foreign %s label", name, other.LabelTarget)
		}
	}
	finalizers := rct.GetFinalizers()
	if len(finalizers) != 1 || finalizers[0] != contract.Finalizer {
		t.Errorf("RCT %s finalizers = %v, want [%s]", name, finalizers, contract.Finalizer)
	}

	configs, _, _ := unstructured.NestedSlice(rct.Object, "spec", "spec", "devices", "config")
	if len(configs) == 0 {
		t.Fatalf("RCT %s has no device config", name)
	}
	config := configs[0].(map[string]interface{})
	opaque := config["opaque"].(map[string]interface{})
	parameters := opaque["parameters"].(map[string]interface{})
	if got := parameters["apiVersion"]; got != contract.OpaqueAPIVersion {
		t.Errorf("RCT %s opaque apiVersion = %v, want %q", name, got, contract.OpaqueAPIVersion)
	}
	if got := parameters["domainID"]; got != uid {
		t.Errorf("RCT %s opaque domainID = %v, want %q", name, got, uid)
	}
}

func assertComputeDomainStatus(t *testing.T, ctx context.Context, client dynamic.Interface, contract cdapi.GroupContract, name, want string) {
	t.Helper()
	cd, err := client.Resource(contract.ComputeDomainGVR).Namespace("default").Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get ComputeDomain %s in %s: %v", name, contract.APIGroup, err)
	}
	status, _, _ := unstructured.NestedString(cd.Object, "status", "status")
	if status != want {
		t.Errorf("ComputeDomain %s status = %q, want %q", name, status, want)
	}
}

func assertComputeDomainFinalizer(t *testing.T, ctx context.Context, client dynamic.Interface, contract cdapi.GroupContract, name, want string) {
	t.Helper()
	cd, err := client.Resource(contract.ComputeDomainGVR).Namespace("default").Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get ComputeDomain %s in %s: %v", name, contract.APIGroup, err)
	}
	found := false
	for _, finalizer := range cd.GetFinalizers() {
		if finalizer == want {
			found = true
		}
		if finalizer != want {
			t.Errorf("ComputeDomain %s carries unexpected finalizer %q", name, finalizer)
		}
	}
	if !found {
		t.Errorf("ComputeDomain %s finalizers = %v, want [%s]", name, cd.GetFinalizers(), want)
	}
}

// ---------------------------------------------------------------------------
// Delete path per group (same namespace/name/UID in both groups)
// ---------------------------------------------------------------------------

// TestDualGroupDeleteIsGroupScoped pins that a delete reconcile for one group
// only touches that group's objects, even when the other group holds a
// synthetic ComputeDomain with the same namespace/name/UID.
func TestDualGroupDeleteIsGroupScoped(t *testing.T) {
	const (
		namespace = "default"
		cliqueNS  = "test-ns"
		sharedUID = "shared-uid"
	)

	aliyunCD := newGroupComputeDomain(AliyunContract, "cd-shared", namespace, sharedUID, "rct-shared")
	aliyunCD.Finalizers = []string{AliyunContract.Finalizer}
	aliyunCD.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	nvidiaCD := newGroupComputeDomain(NVIDIAContract, "cd-shared", namespace, sharedUID, "rct-shared")
	nvidiaCD.Finalizers = []string{NVIDIAContract.Finalizer}
	nvidiaCD.DeletionTimestamp = &metav1.Time{Time: time.Now()}

	aliyunClique := newGroupClique(AliyunContract, sharedUID+".rack-a", cliqueNS, sharedUID, nil)
	nvidiaClique := newGroupClique(NVIDIAContract, sharedUID+".rack-a", cliqueNS, sharedUID, nil)

	node := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Node",
		"metadata": map[string]interface{}{
			"name": "node-1",
			"labels": map[string]interface{}{
				AliyunContract.LabelComputeDomain: sharedUID,
				NVIDIAContract.LabelComputeDomain: sharedUID,
			},
		},
	}}

	client := newDualGroupFakeClient(t,
		mustUnstructured(t, aliyunCD), mustUnstructured(t, nvidiaCD),
		mustUnstructured(t, aliyunClique), mustUnstructured(t, nvidiaClique),
		node)

	aliyunMgr := startGroupManager(t, AliyunContract, client, cliqueNS)
	ctx := context.Background()
	cd, err := aliyunMgr.Get(sharedUID)
	if err != nil || cd == nil {
		t.Fatalf("aliyun manager Get(%q) = %+v,%v", sharedUID, cd, err)
	}
	if err := aliyunMgr.reconcileDelete(ctx, cd); err != nil {
		t.Fatalf("reconcileDelete aliyun: %v", err)
	}

	// The source group's CD finalizer is gone.
	stillTagged, err := client.Resource(AliyunContract.ComputeDomainGVR).Namespace(namespace).Get(ctx, "cd-shared", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get aliyun ComputeDomain: %v", err)
	}
	for _, finalizer := range stillTagged.GetFinalizers() {
		if finalizer == AliyunContract.Finalizer {
			t.Error("aliyun ComputeDomain finalizer was not removed")
		}
	}

	// The other group's CD keeps its own finalizer.
	other, err := client.Resource(NVIDIAContract.ComputeDomainGVR).Namespace(namespace).Get(ctx, "cd-shared", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get nvidia ComputeDomain: %v", err)
	}
	found := false
	for _, finalizer := range other.GetFinalizers() {
		if finalizer == NVIDIAContract.Finalizer {
			found = true
		}
	}
	if !found {
		t.Errorf("nvidia ComputeDomain finalizers = %v, want [%s]", other.GetFinalizers(), NVIDIAContract.Finalizer)
	}

	// Only the source group's clique is deleted.
	if _, err := client.Resource(AliyunContract.ComputeDomainCliqueGVR).Namespace(cliqueNS).Get(ctx, aliyunClique.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("aliyun clique should be deleted, got err=%v", err)
	}
	if _, err := client.Resource(NVIDIAContract.ComputeDomainCliqueGVR).Namespace(cliqueNS).Get(ctx, nvidiaClique.Name, metav1.GetOptions{}); err != nil {
		t.Errorf("nvidia clique must not be touched by the aliyun manager: %v", err)
	}

	// Only the source group's node label is removed.
	storedNode, err := client.Resource(nodeGVR).Get(ctx, "node-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get node: %v", err)
	}
	if _, found := storedNode.GetLabels()[AliyunContract.LabelComputeDomain]; found {
		t.Errorf("aliyun node label still present: %v", storedNode.GetLabels())
	}
	if storedNode.GetLabels()[NVIDIAContract.LabelComputeDomain] != sharedUID {
		t.Errorf("nvidia node label was removed: %v", storedNode.GetLabels())
	}
}

// ---------------------------------------------------------------------------
// ResourceClaimTemplate conflict handling across groups
// ---------------------------------------------------------------------------

func newSeededRCT(namespace, name string, labels map[string]string, finalizers ...string) *unstructured.Unstructured {
	rct := &unstructured.Unstructured{}
	rct.SetAPIVersion("resource.k8s.io/v1")
	rct.SetKind("ResourceClaimTemplate")
	rct.SetNamespace(namespace)
	rct.SetName(name)
	rct.SetUID("rct-object-uid")
	rct.SetResourceVersion("1")
	rct.SetLabels(labels)
	rct.SetFinalizers(finalizers)
	return rct
}

// TestResourceClaimTemplateCrossGroupConflictIsRejected pins that a same-named
// ResourceClaimTemplate owned by another API group is never adopted or
// overwritten, while a same-group same-UID object is safely reused.
func TestResourceClaimTemplateCrossGroupConflictIsRejected(t *testing.T) {
	const (
		namespace = "default"
		name      = "rct-shared"
	)
	ctx := context.Background()

	t.Run("owned by the other group", func(t *testing.T) {
		seeded := newSeededRCT(namespace, name, map[string]string{
			AliyunContract.LabelComputeDomain: "uid-aliyun",
			AliyunContract.LabelTarget:        constants.RCTTargetWorkload,
		}, AliyunContract.Finalizer)
		cd := newGroupComputeDomain(NVIDIAContract, "cd-nvidia", namespace, "uid-nvidia", name)

		client := newDualGroupFakeClient(t, seeded, mustUnstructured(t, cd))
		mgr := startGroupManager(t, NVIDIAContract, client, namespace)

		_, err := mgr.resourceClaimTemplateManager.Create(ctx, namespace, name, cd)
		if err == nil {
			t.Fatal("Create must reject an RCT owned by the other API group")
		}
		for _, want := range []string{"foreign owner label", "uid-aliyun", cdapi.GroupAliyun} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %v should contain %q", err, want)
			}
		}

		stored, getErr := client.Resource(resourceClaimTemplateGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
		if getErr != nil {
			t.Fatalf("get stored RCT: %v", getErr)
		}
		if got := stored.GetLabels()[AliyunContract.LabelComputeDomain]; got != "uid-aliyun" {
			t.Errorf("stored RCT owner label changed to %q", got)
		}
		if _, found := stored.GetLabels()[NVIDIAContract.LabelComputeDomain]; found {
			t.Error("stored RCT was overwritten with the nvidia owner label")
		}
		assertNoResourceClaimTemplateWrites(t, client)
	})

	t.Run("existing object without owner label", func(t *testing.T) {
		seeded := newSeededRCT(namespace, name, map[string]string{"unrelated": "value"})
		cd := newGroupComputeDomain(NVIDIAContract, "cd-nvidia", namespace, "uid-nvidia", name)

		client := newDualGroupFakeClient(t, seeded, mustUnstructured(t, cd))
		mgr := startGroupManager(t, NVIDIAContract, client, namespace)

		_, err := mgr.resourceClaimTemplateManager.Create(ctx, namespace, name, cd)
		if err == nil {
			t.Fatal("Create must refuse to take over an unowned RCT")
		}
		if !strings.Contains(err.Error(), "carries no owner label") {
			t.Errorf("error %v should explain the missing owner label", err)
		}
		assertNoResourceClaimTemplateWrites(t, client)
	})

	t.Run("same group same UID is reused", func(t *testing.T) {
		seeded := newSeededRCT(namespace, name, map[string]string{
			NVIDIAContract.LabelComputeDomain: "uid-nvidia",
			NVIDIAContract.LabelTarget:        constants.RCTTargetWorkload,
		}, NVIDIAContract.Finalizer)
		cd := newGroupComputeDomain(NVIDIAContract, "cd-nvidia", namespace, "uid-nvidia", name)

		client := newDualGroupFakeClient(t, seeded, mustUnstructured(t, cd))
		mgr := startGroupManager(t, NVIDIAContract, client, namespace)

		existing, err := mgr.resourceClaimTemplateManager.Create(ctx, namespace, name, cd)
		if err != nil {
			t.Fatalf("Create should adopt its own RCT: %v", err)
		}
		if existing == nil || existing.GetUID() != seeded.GetUID() {
			t.Fatalf("Create returned %+v, want the existing RCT", existing)
		}
		assertNoResourceClaimTemplateWrites(t, client)
	})
}

func assertNoResourceClaimTemplateWrites(t *testing.T, client *dynamicfake.FakeDynamicClient) {
	t.Helper()
	for _, action := range client.Actions() {
		if action.GetResource() != resourceClaimTemplateGVR {
			continue
		}
		switch action.GetVerb() {
		case "create", "update", "patch":
			t.Errorf("unexpected %s on ResourceClaimTemplate: %+v", action.GetVerb(), action)
		}
	}
}

// ---------------------------------------------------------------------------
// Node labels and orphan cleanup stay inside the source group
// ---------------------------------------------------------------------------

func TestNodeLabelRemovalOnlyTouchesSourceGroup(t *testing.T) {
	const sharedUID = "shared-uid"
	ctx := context.Background()

	for _, contract := range cdapi.SupportedContracts() {
		source := contract
		other := AliyunContract
		if source.APIGroup == cdapi.GroupAliyun {
			other = NVIDIAContract
		}
		t.Run(source.APIGroup, func(t *testing.T) {
			node := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "Node",
				"metadata": map[string]interface{}{
					"name": "node-1",
					"labels": map[string]interface{}{
						source.LabelComputeDomain: sharedUID,
						other.LabelComputeDomain:  sharedUID,
					},
				},
			}}
			client := newDualGroupFakeClient(t, node)
			mgr := NewNodeManager(newGroupManagerConfig(t, source, client, "default"), nil, nil, nil)

			if err := mgr.RemoveComputeDomainLabels(ctx, sharedUID); err != nil {
				t.Fatalf("RemoveComputeDomainLabels: %v", err)
			}

			stored, err := client.Resource(nodeGVR).Get(ctx, "node-1", metav1.GetOptions{})
			if err != nil {
				t.Fatalf("get node: %v", err)
			}
			if _, found := stored.GetLabels()[source.LabelComputeDomain]; found {
				t.Errorf("source label %s still present: %v", source.LabelComputeDomain, stored.GetLabels())
			}
			if got := stored.GetLabels()[other.LabelComputeDomain]; got != sharedUID {
				t.Errorf("foreign label %s was modified: %v", other.LabelComputeDomain, stored.GetLabels())
			}
		})
	}
}

// TestCleanupOrphanDetectionIgnoresOtherGroupUIDs pins that a live UID set is
// computed per group: a ComputeDomain in the other group must never mask an
// orphan of the source group, even when both use the same UID.
func TestCleanupOrphanDetectionIgnoresOtherGroupUIDs(t *testing.T) {
	const sharedUID = "shared-uid"
	ctx := context.Background()

	nvidiaCD := newGroupComputeDomain(NVIDIAContract, "cd-nvidia", "default", sharedUID, "rct-nvidia")
	client := newDualGroupFakeClient(t, mustUnstructured(t, nvidiaCD))

	aliyunMgr := NewComputeDomainManager(newGroupManagerConfig(t, AliyunContract, client, "default"))
	aliyunUIDs, err := aliyunMgr.LiveListUIDs(ctx)
	if err != nil {
		t.Fatalf("aliyun LiveListUIDs: %v", err)
	}
	if len(aliyunUIDs) != 0 {
		t.Fatalf("aliyun live UID set leaked other-group ComputeDomains: %v", aliyunUIDs)
	}

	nvidiaMgr := NewComputeDomainManager(newGroupManagerConfig(t, NVIDIAContract, client, "default"))
	nvidiaUIDs, err := nvidiaMgr.LiveListUIDs(ctx)
	if err != nil {
		t.Fatalf("nvidia LiveListUIDs: %v", err)
	}
	if _, ok := nvidiaUIDs[sharedUID]; !ok {
		t.Fatalf("nvidia live UID set = %v, want %q", nvidiaUIDs, sharedUID)
	}

	aliyunNode := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Node",
		"metadata": map[string]interface{}{
			"name":   "node-aliyun",
			"labels": map[string]interface{}{AliyunContract.LabelComputeDomain: sharedUID},
		},
	}}
	nvidiaNode := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Node",
		"metadata": map[string]interface{}{
			"name":   "node-nvidia",
			"labels": map[string]interface{}{NVIDIAContract.LabelComputeDomain: sharedUID},
		},
	}}

	var cleaned []string
	cleanup := &CleanupManager{
		informer:         &fakeCleanupInformer{store: &fakeStore{items: []interface{}{aliyunNode, nvidiaNode}}},
		getComputeDomain: func(string) (*v1beta1.ComputeDomain, error) { return nil, nil },
		liveListUIDs:     aliyunMgr.LiveListUIDs,
		callback: func(_ context.Context, uid string) error {
			cleaned = append(cleaned, uid)
			return nil
		},
		labelKey: AliyunContract.LabelComputeDomain,
		interval: constants.CleanupInterval,
		queue:    make(chan struct{}, 1),
	}
	cleanup.cleanup(ctx)

	if len(cleaned) != 1 || cleaned[0] != sharedUID {
		t.Fatalf("cleaned UIDs = %v, want exactly the aliyun orphan %q", cleaned, sharedUID)
	}
}

// ---------------------------------------------------------------------------
// ComputeDomainClique stays read-only and group-scoped for both groups
// ---------------------------------------------------------------------------

func TestAssertDaemonsRemovedIsGroupScopedAndReadOnly(t *testing.T) {
	const (
		cliqueNS   = "test-ns"
		sharedUID  = "shared-uid"
		cliqueName = sharedUID + ".rack-a"
	)
	ctx := context.Background()

	for _, contract := range cdapi.SupportedContracts() {
		source := contract
		other := AliyunContract
		if source.APIGroup == cdapi.GroupAliyun {
			other = NVIDIAContract
		}
		t.Run(source.APIGroup, func(t *testing.T) {
			sourceClique := newGroupClique(source, cliqueName, cliqueNS, sharedUID, nil)
			otherClique := newGroupClique(other, cliqueName, cliqueNS, sharedUID, nil)
			client := newDualGroupFakeClient(t, mustUnstructured(t, sourceClique), mustUnstructured(t, otherClique))

			mgr := &ComputeDomainCliqueManager{config: newGroupManagerConfig(t, source, client, cliqueNS)}
			if err := mgr.AssertDaemonsRemoved(ctx, sharedUID); err != nil {
				t.Fatalf("AssertDaemonsRemoved: %v", err)
			}

			if _, err := client.Resource(source.ComputeDomainCliqueGVR).Namespace(cliqueNS).Get(ctx, cliqueName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				t.Errorf("empty source clique should be deleted, got err=%v", err)
			}
			if _, err := client.Resource(other.ComputeDomainCliqueGVR).Namespace(cliqueNS).Get(ctx, cliqueName, metav1.GetOptions{}); err != nil {
				t.Errorf("other group clique must not be touched: %v", err)
			}

			deletes := 0
			for _, action := range client.Actions() {
				if action.GetResource().Resource != constants.ComputeDomainCliqueResource {
					continue
				}
				switch action.GetVerb() {
				case "create", "update", "patch":
					t.Errorf("controller must not %s ComputeDomainClique daemon entries: %+v", action.GetVerb(), action)
				case "delete":
					deletes++
					if action.GetResource() != source.ComputeDomainCliqueGVR {
						t.Errorf("delete targeted %v, want %v", action.GetResource(), source.ComputeDomainCliqueGVR)
					}
				}
			}
			if deletes != 1 {
				t.Errorf("delete count = %d, want 1", deletes)
			}
		})
	}
}

func TestAssertDaemonsRemovedWaitsInBothGroups(t *testing.T) {
	const cliqueNS = "test-ns"
	ctx := context.Background()
	daemons := []*v1beta1.ComputeDomainDaemonInfo{
		{NodeName: "node-1", IPAddress: "10.0.0.1", CliqueID: "rack-a", Index: 0, Status: constants.NodeStatusReady},
	}

	for _, contract := range cdapi.SupportedContracts() {
		source := contract
		t.Run(source.APIGroup, func(t *testing.T) {
			clique := newGroupClique(source, "wait-uid.rack-a", cliqueNS, "wait-uid", daemons)
			client := newDualGroupFakeClient(t, mustUnstructured(t, clique))

			mgr := &ComputeDomainCliqueManager{config: newGroupManagerConfig(t, source, client, cliqueNS)}
			err := mgr.AssertDaemonsRemoved(ctx, "wait-uid")
			if err == nil {
				t.Fatal("AssertDaemonsRemoved must fail while daemons are registered")
			}
			if !strings.Contains(err.Error(), "daemon") {
				t.Errorf("error %v should mention daemons", err)
			}
			for _, action := range client.Actions() {
				if action.GetVerb() == "delete" {
					t.Errorf("controller must not delete a clique with daemons: %+v", action)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Controller lifecycle with two group manager sets
// ---------------------------------------------------------------------------

// TestControllerRunStartsBothGroupsAndStopsCleanly verifies that a dual group
// controller becomes ready only after both manager sets synced, and that Stop
// makes Run return without leaving goroutines behind.
//
// The queue/manager teardown itself is asserted in detail by
// controller_shutdown_test.go, which also covers the failure and repeat paths.
func TestControllerRunStartsBothGroupsAndStopsCleanly(t *testing.T) {
	client := newDualGroupFakeClient(t)
	ctrl := mustNewController(t, Config{
		DynamicClient:                client,
		Namespace:                    "kube-system",
		ComputeDomainCliqueNamespace: "default",
	})

	ready := make(chan struct{}, 1)
	ctrl.OnReady = func() { ready <- struct{}{} }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- ctrl.Run(ctx, 1) }()

	select {
	case <-ready:
	case <-time.After(15 * time.Second):
		t.Fatal("controller did not become ready with both groups served")
	}

	ctrl.Stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Run did not return after Stop")
	}
}
