package computedomain

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	v1beta1 "github.com/lj-runtimekit/job-controller/pkg/apis/resource/v1beta1"
	"github.com/lj-runtimekit/job-controller/pkg/constants"
)

// ---------- buildNodesFromCliques ----------

func newTestStatusManager() *ComputeDomainStatusManager {
	return &ComputeDomainStatusManager{}
}

func TestBuildNodesFromCliquesBasic(t *testing.T) {
	m := newTestStatusManager()
	cliques := []*v1beta1.ComputeDomainClique{
		{
			Daemons: []*v1beta1.ComputeDomainDaemonInfo{
				{NodeName: "node-1", IPAddress: "10.0.0.1", CliqueID: "rack-a", Index: 0, Status: constants.NodeStatusReady},
				{NodeName: "node-2", IPAddress: "10.0.0.2", CliqueID: "rack-a", Index: 1, Status: constants.NodeStatusNotReady},
			},
		},
	}

	nodes := m.buildNodesFromCliques(cliques)
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(nodes))
	}
	if nodes[0].Name != "node-1" || nodes[0].Status != constants.NodeStatusReady {
		t.Errorf("node-1: expected Ready, got %s", nodes[0].Status)
	}
	if nodes[1].Name != "node-2" || nodes[1].Status != constants.NodeStatusNotReady {
		t.Errorf("node-2: expected NotReady, got %s", nodes[1].Status)
	}
}

func TestBuildNodesFromCliquesEmptyStatus(t *testing.T) {
	m := newTestStatusManager()
	cliques := []*v1beta1.ComputeDomainClique{
		{
			Daemons: []*v1beta1.ComputeDomainDaemonInfo{
				{NodeName: "node-x", IPAddress: "10.0.0.5", CliqueID: "rack-a", Index: 3},
			},
		},
	}

	nodes := m.buildNodesFromCliques(cliques)
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}
	if nodes[0].Status != constants.NodeStatusNotReady {
		t.Errorf("expected empty status to default to NotReady, got %s", nodes[0].Status)
	}
}

func TestBuildNodesFromCliquesMultipleCliques(t *testing.T) {
	m := newTestStatusManager()
	cliques := []*v1beta1.ComputeDomainClique{
		{
			Daemons: []*v1beta1.ComputeDomainDaemonInfo{
				{NodeName: "node-1", IPAddress: "10.0.0.1", CliqueID: "rack-a", Index: 0, Status: constants.NodeStatusReady},
			},
		},
		{
			Daemons: []*v1beta1.ComputeDomainDaemonInfo{
				{NodeName: "node-2", IPAddress: "10.0.0.2", CliqueID: "rack-b", Index: 0, Status: constants.NodeStatusReady},
			},
		},
	}

	nodes := m.buildNodesFromCliques(cliques)
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes from 2 cliques, got %d", len(nodes))
	}
}

func TestBuildNodesFromCliquesEmpty(t *testing.T) {
	m := newTestStatusManager()
	nodes := m.buildNodesFromCliques(nil)
	if len(nodes) != 0 {
		t.Errorf("expected 0 nodes, got %d", len(nodes))
	}
}

func TestBuildNodesFromCliquesPreservesIndex(t *testing.T) {
	m := newTestStatusManager()
	cliques := []*v1beta1.ComputeDomainClique{
		{
			Daemons: []*v1beta1.ComputeDomainDaemonInfo{
				{NodeName: "node-1", IPAddress: "10.0.0.1", CliqueID: "rack-a", Index: 7, Status: constants.NodeStatusReady},
			},
		},
	}

	nodes := m.buildNodesFromCliques(cliques)
	if nodes[0].Index != 7 {
		t.Errorf("expected index 7, got %d", nodes[0].Index)
	}
	if nodes[0].CliqueID != "rack-a" {
		t.Errorf("expected cliqueID rack-a, got %s", nodes[0].CliqueID)
	}
}

// ---------- dedupeNodesByName ----------

func TestDedupeNodesByNameNoDuplicates(t *testing.T) {
	nodes := []v1beta1.ComputeDomainNode{
		{Name: "node-1", IPAddress: "10.0.0.1"},
		{Name: "node-2", IPAddress: "10.0.0.2"},
	}
	result := dedupeNodesByName(nodes)
	if len(result) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(result))
	}
}

func TestDedupeNodesByNameWithDuplicates(t *testing.T) {
	nodes := []v1beta1.ComputeDomainNode{
		{Name: "node-1", IPAddress: "10.0.0.1", CliqueID: "rack-a"},
		{Name: "node-1", IPAddress: "10.0.0.2", CliqueID: "rack-b"},
		{Name: "node-2", IPAddress: "10.0.0.3"},
	}
	result := dedupeNodesByName(nodes)
	if len(result) != 2 {
		t.Errorf("expected 2 unique nodes, got %d", len(result))
	}
	// First occurrence wins
	if result[0].IPAddress != "10.0.0.1" {
		t.Errorf("expected first node-1 to win (IP 10.0.0.1), got %s", result[0].IPAddress)
	}
}

func TestDedupeNodesByNameSkipsEmptyName(t *testing.T) {
	nodes := []v1beta1.ComputeDomainNode{
		{Name: "", IPAddress: "10.0.0.1"},
		{Name: "node-1", IPAddress: "10.0.0.2"},
	}
	result := dedupeNodesByName(nodes)
	if len(result) != 1 {
		t.Errorf("expected 1 node (skipping empty name), got %d", len(result))
	}
}

func TestDedupeNodesByNameEmpty(t *testing.T) {
	result := dedupeNodesByName(nil)
	if len(result) != 0 {
		t.Errorf("expected 0 nodes, got %d", len(result))
	}
}

// ---------- nodesEqual ----------

func TestNodesEqualSame(t *testing.T) {
	a := []v1beta1.ComputeDomainNode{
		{Name: "node-1", IPAddress: "10.0.0.1", Status: constants.NodeStatusReady},
		{Name: "node-2", IPAddress: "10.0.0.2", Status: constants.NodeStatusNotReady},
	}
	b := []v1beta1.ComputeDomainNode{
		{Name: "node-2", IPAddress: "10.0.0.2", Status: constants.NodeStatusNotReady},
		{Name: "node-1", IPAddress: "10.0.0.1", Status: constants.NodeStatusReady},
	}
	if !nodesEqual(a, b) {
		t.Error("expected equal (order-independent)")
	}
}

func TestNodesEqualDifferentStatus(t *testing.T) {
	a := []v1beta1.ComputeDomainNode{
		{Name: "node-1", IPAddress: "10.0.0.1", Status: constants.NodeStatusReady},
	}
	b := []v1beta1.ComputeDomainNode{
		{Name: "node-1", IPAddress: "10.0.0.1", Status: constants.NodeStatusNotReady},
	}
	if nodesEqual(a, b) {
		t.Error("expected not equal (different status)")
	}
}

func TestNodesEqualDifferentLength(t *testing.T) {
	a := []v1beta1.ComputeDomainNode{
		{Name: "node-1"},
	}
	b := []v1beta1.ComputeDomainNode{
		{Name: "node-1"},
		{Name: "node-2"},
	}
	if nodesEqual(a, b) {
		t.Error("expected not equal (different length)")
	}
}

func TestNodesEqualBothNil(t *testing.T) {
	if !nodesEqual(nil, nil) {
		t.Error("expected nil == nil to be equal")
	}
}

func TestNodesEqualBothEmpty(t *testing.T) {
	if !nodesEqual([]v1beta1.ComputeDomainNode{}, []v1beta1.ComputeDomainNode{}) {
		t.Error("expected empty slices to be equal")
	}
}

// ---------- numNodes validation ----------

func TestNumNodesValidation(t *testing.T) {
	m := newTestComputeDomainManager()
	tests := []struct {
		name     string
		numNodes int
		nodes    []v1beta1.ComputeDomainNode
		want     string
	}{
		{
			name:     "all ready",
			numNodes: 0,
			nodes: []v1beta1.ComputeDomainNode{
				{Name: "n1", Status: constants.NodeStatusReady},
				{Name: "n2", Status: constants.NodeStatusReady},
			},
			want: constants.StatusReady,
		},
		{
			name:     "numNodes ignored - still ready",
			numNodes: 3, // deprecated, ignored
			nodes: []v1beta1.ComputeDomainNode{
				{Name: "n1", Status: constants.NodeStatusReady},
			},
			want: constants.StatusReady,
		},
		{
			name:     "zero required, zero nodes",
			numNodes: 0,
			nodes:    nil,
			want:     constants.StatusReady,
		},
		{
			name:     "all not ready",
			numNodes: 0,
			nodes: []v1beta1.ComputeDomainNode{
				{Name: "n1", Status: constants.NodeStatusNotReady},
				{Name: "n2", Status: constants.NodeStatusNotReady},
			},
			want: constants.StatusNotReady,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cd := &v1beta1.ComputeDomain{}
			cd.Spec.NumNodes = tt.numNodes
			cd.Status.Nodes = tt.nodes
			got := m.calculateGlobalStatus(cd)
			if got != tt.want {
				t.Errorf("calculateGlobalStatus = %s, want %s", got, tt.want)
			}
		})
	}
}

// ---------- Constants verification ----------

func TestPPUConstantValues(t *testing.T) {
	if constants.DriverName != "compute-domain.aliyun.com" {
		t.Errorf("DriverName = %s, want compute-domain.aliyun.com", constants.DriverName)
	}
	if constants.ChannelDeviceClassName != "compute-domain-default-channel.aliyun.com" {
		t.Errorf("ChannelDeviceClassName = %s, want compute-domain-default-channel.aliyun.com", constants.ChannelDeviceClassName)
	}
	if constants.LabelComputeDomainCliqueDomain != "aliyun.com/clique-domain" {
		t.Errorf("LabelComputeDomainCliqueDomain = %s, want aliyun.com/clique-domain", constants.LabelComputeDomainCliqueDomain)
	}
	if constants.LabelComputeDomainClique != "aliyun.com/clique" {
		t.Errorf("LabelComputeDomainClique = %s, want aliyun.com/clique", constants.LabelComputeDomainClique)
	}
	if constants.ComputeDomainFinalizer != constants.LabelComputeDomain {
		t.Errorf("ComputeDomainFinalizer should equal LabelComputeDomain")
	}
	_ = cmp.Diff // silence unused import
}

func TestStatusAggregationUsesAliyunCliqueDomainLabelOnly(t *testing.T) {
	cd := &v1beta1.ComputeDomain{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cd", Namespace: "default", UID: "cd-uid"},
	}
	onSiteClique := &v1beta1.ComputeDomainClique{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "unrelated-object-name",
			Namespace: "default",
			Labels:    map[string]string{constants.LabelComputeDomainCliqueDomain: "cd-uid"},
		},
		Daemons: []*v1beta1.ComputeDomainDaemonInfo{
			{NodeName: "node-onsite", IPAddress: "10.0.0.1", CliqueID: "rack-a", Status: constants.NodeStatusReady},
		},
	}
	legacyOnlyClique := &v1beta1.ComputeDomainClique{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cd-uid.rack-b",
			Namespace: "default",
			Labels:    map[string]string{constants.LabelComputeDomain: "cd-uid"},
		},
		Daemons: []*v1beta1.ComputeDomainDaemonInfo{
			{NodeName: "node-legacy", IPAddress: "10.0.0.2", CliqueID: "rack-b", Status: constants.NodeStatusReady},
		},
	}

	grouped := groupCliquesByComputeDomainUID([]*v1beta1.ComputeDomainClique{onSiteClique, legacyOnlyClique}, constants.LabelComputeDomainCliqueDomain)
	if len(grouped["cd-uid"]) != 1 || grouped["cd-uid"][0].Name != onSiteClique.Name {
		t.Fatalf("expected only onsite-labeled clique in aggregation, got %#v", grouped["cd-uid"])
	}

	var updated *v1beta1.ComputeDomain
	manager := &ComputeDomainStatusManager{
		updateComputeDomainStatus: func(_ context.Context, cd *v1beta1.ComputeDomain) (*v1beta1.ComputeDomain, error) {
			updated = cd
			return cd, nil
		},
	}
	manager.syncComputeDomain(context.Background(), cd, grouped[string(cd.UID)])
	if updated == nil {
		t.Fatal("expected status aggregation to update the ComputeDomain")
	}
	if len(updated.Status.Nodes) != 1 || updated.Status.Nodes[0].Name != "node-onsite" {
		t.Fatalf("expected only onsite CDClique node, got %#v", updated.Status.Nodes)
	}
}

// ---------- daemon ownership regression tests ----------
//
// The Device Plugin is the sole writer of ComputeDomainClique daemon entries.
// The controller's periodic status sync must therefore be read-only with
// respect to CDCliques: a missing Device Plugin pod, a stale label selector, an
// IP change or a terminating pod must never cause a daemon entry to be added,
// rewritten or removed.

// testPodGVR is used only to seed fake Device Plugin pod fixtures. The
// controller itself must never access pods.
var testPodGVR = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}

func dpPodFixture(name, nodeName, podIP string, labels map[string]string, deleting *metav1.Time) *corev1.Pod {
	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "test-ns",
			Labels:            labels,
			DeletionTimestamp: deleting,
		},
		Spec:   corev1.PodSpec{NodeName: nodeName},
		Status: corev1.PodStatus{PodIP: podIP},
	}
}

func mustUnstructured(t *testing.T, obj interface{}) *unstructured.Unstructured {
	t.Helper()
	object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		t.Fatalf("convert to unstructured: %v", err)
	}
	return &unstructured.Unstructured{Object: object}
}

// TestStatusSyncNeverMutatesCliques pins the ownership boundary: whatever the
// Device Plugin pod state looks like, the controller's status sync only reads
// CDCliques and aggregates ComputeDomain status.
func TestStatusSyncNeverMutatesCliques(t *testing.T) {
	now := metav1.Now()
	dpLabels := map[string]string{"app.kubernetes.io/name": "lj-runtimekit-device-plugin"}

	scenarios := []struct {
		name string
		pods []*corev1.Pod
	}{
		{
			name: "device plugin pod missing",
			pods: nil,
		},
		{
			name: "device plugin pod selector mismatch",
			pods: []*corev1.Pod{dpPodFixture("unrelated-pod", "node-1", "10.0.0.1",
				map[string]string{"app.kubernetes.io/name": "unrelated"}, nil)},
		},
		{
			name: "device plugin pod IP changed",
			pods: []*corev1.Pod{dpPodFixture("dp-pod", "node-1", "10.0.0.99", dpLabels, nil)},
		},
		{
			name: "device plugin pod terminating",
			pods: []*corev1.Pod{dpPodFixture("dp-pod", "node-1", "10.0.0.1", dpLabels, &now)},
		},
		{
			name: "device plugin pod matching",
			pods: []*corev1.Pod{dpPodFixture("dp-pod", "node-1", "10.0.0.1", dpLabels, nil)},
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			cliqueNS := "test-ns"

			cd := &v1beta1.ComputeDomain{
				TypeMeta: metav1.TypeMeta{
					APIVersion: constants.APIGroup + "/" + constants.APIVersion,
					Kind:       "ComputeDomain",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:            "ownership-cd",
					Namespace:       "test-ns",
					UID:             types.UID("ownership-uid"),
					ResourceVersion: "1",
				},
			}
			clique := &v1beta1.ComputeDomainClique{
				TypeMeta: metav1.TypeMeta{
					APIVersion: constants.APIGroup + "/" + constants.APIVersion,
					Kind:       "ComputeDomainClique",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:            "ownership-uid.rack-a",
					Namespace:       cliqueNS,
					Labels:          map[string]string{constants.LabelComputeDomainCliqueDomain: "ownership-uid"},
					ResourceVersion: "1",
				},
				Daemons: []*v1beta1.ComputeDomainDaemonInfo{
					{NodeName: "node-1", IPAddress: "10.0.0.1", CliqueID: "rack-a", Index: 0, Status: constants.NodeStatusReady},
				},
			}

			objects := []runtime.Object{mustUnstructured(t, cd), mustUnstructured(t, clique)}
			for _, pod := range scenario.pods {
				objects = append(objects, mustUnstructured(t, pod))
			}

			client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
				map[schema.GroupVersionResource]string{
					computeDomainGVR:       "ComputeDomainList",
					computeDomainCliqueGVR: "ComputeDomainCliqueList",
					testPodGVR:             "PodList",
				}, objects...)

			cliqueManager := NewComputeDomainCliqueManager(&ManagerConfig{
				Contract:                     NVIDIAContract,
				ComputeDomainCliqueNamespace: cliqueNS,
				ClientSets:                   ClientSets{Dynamic: client},
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if err := cliqueManager.Start(ctx); err != nil {
				t.Fatalf("start clique manager: %v", err)
			}
			defer cliqueManager.Stop()

			var aggregated []*v1beta1.ComputeDomain
			statusManager := NewComputeDomainStatusManager(
				cliqueManager,
				func() ([]*v1beta1.ComputeDomain, error) {
					return []*v1beta1.ComputeDomain{cd.DeepCopy()}, nil
				},
				func(_ context.Context, updated *v1beta1.ComputeDomain) (*v1beta1.ComputeDomain, error) {
					aggregated = append(aggregated, updated.DeepCopy())
					return updated, nil
				},
			)

			// Starting the manager must not create a Device Plugin pod informer:
			// any pod access at all would show up in the action log below.
			if err := statusManager.Start(ctx); err != nil {
				t.Fatalf("start status manager: %v", err)
			}
			defer statusManager.Stop() // idempotent; safety net for early failures

			// Stop the background periodic sync before driving syncAll by hand. The
			// status callback appends to `aggregated`, so a background worker still
			// ticking (StatusSyncInterval) would race with the manual calls below.
			// Stop() waits on the manager waitGroup, so its return proves the
			// periodic goroutine has exited; the bounded wait turns a hung stop into
			// a test failure instead of a timeout kill.
			stopped := make(chan struct{})
			go func() {
				defer close(stopped)
				if err := statusManager.Stop(); err != nil {
					t.Errorf("stop status manager: %v", err)
				}
			}()
			select {
			case <-stopped:
			case <-time.After(10 * time.Second):
				t.Fatal("status manager background sync did not exit after Stop")
			}

			// From here on the status manager is single-threaded: syncAll runs the
			// per-ComputeDomain work in goroutines but joins them before returning.
			for i := 0; i < 3; i++ {
				statusManager.syncAll(ctx)
			}

			for _, action := range client.Actions() {
				switch action.GetResource().Resource {
				case constants.ComputeDomainCliqueResource:
					switch action.GetVerb() {
					case "create", "update", "patch", "delete", "deletecollection":
						t.Fatalf("controller must never mutate ComputeDomainCliques, saw %s", action.GetVerb())
					}
				case "pods":
					t.Fatalf("controller must not access Device Plugin pods, saw %s", action.GetVerb())
				}
			}

			// The daemon entry written by the Device Plugin is untouched.
			live, err := client.Resource(computeDomainCliqueGVR).Namespace(cliqueNS).Get(ctx, clique.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("get clique: %v", err)
			}
			liveClique, err := toComputeDomainClique(live)
			if err != nil {
				t.Fatalf("convert clique: %v", err)
			}
			if len(liveClique.Daemons) != 1 {
				t.Fatalf("daemon entries must not be removed by the controller, got %#v", liveClique.Daemons)
			}
			if liveClique.Daemons[0].NodeName != "node-1" || liveClique.Daemons[0].IPAddress != "10.0.0.1" {
				t.Fatalf("daemon entry must not be rewritten: %#v", liveClique.Daemons[0])
			}

			// Status aggregation from CDCliques is preserved.
			if len(aggregated) == 0 {
				t.Fatal("expected ComputeDomain status aggregation to run")
			}
			last := aggregated[len(aggregated)-1]
			if len(last.Status.Nodes) != 1 {
				t.Fatalf("expected 1 aggregated node, got %#v", last.Status.Nodes)
			}
			if last.Status.Nodes[0].Name != "node-1" || last.Status.Nodes[0].IPAddress != "10.0.0.1" {
				t.Fatalf("unexpected aggregated node: %#v", last.Status.Nodes[0])
			}
		})
	}
}
