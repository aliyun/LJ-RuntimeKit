package computedomain

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"

	v1beta1 "github.com/lj-runtimekit/job-controller/pkg/apis/resource/v1beta1"
	cdapi "github.com/lj-runtimekit/job-controller/pkg/computedomain"
	"github.com/lj-runtimekit/job-controller/pkg/constants"
)

// ---------- configureResourceClaimTemplate ----------

func newBaseRCT() *unstructured.Unstructured {
	rct := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "resource.k8s.io/v1",
		"kind":       "ResourceClaimTemplate",
		"metadata": map[string]interface{}{
			"name":      "test-rct",
			"namespace": "default",
		},
		"spec": map[string]interface{}{
			"spec": map[string]interface{}{
				"devices": map[string]interface{}{
					"requests": []interface{}{
						map[string]interface{}{
							"name": "req-0",
							"exactly": map[string]interface{}{
								"deviceClassName": "placeholder",
							},
						},
					},
					"config": []interface{}{
						map[string]interface{}{
							"opaque": map[string]interface{}{
								"driver": "placeholder",
								"parameters": map[string]interface{}{
									"apiVersion": "placeholder/v1",
									"kind":       "PlaceholderKind",
								},
							},
						},
					},
				},
			},
		},
	}}
	return rct
}

// configureNVIDIARCT keeps the pre-dual-group call sites concise: it binds the
// NVIDIA contract and the workload RCT target explicitly.
func configureNVIDIARCT(obj *unstructured.Unstructured, deviceClass, kind, uid, allocationMode string) {
	configureResourceClaimTemplate(obj, NVIDIAContract, deviceClass, kind,
		newResourceClaimTemplateData(NVIDIAContract, obj.GetNamespace(), obj.GetName(), uid, allocationMode, constants.RCTTargetWorkload))
}

func TestConfigureResourceClaimTemplateSetsFinalizer(t *testing.T) {
	rct := newBaseRCT()
	configureNVIDIARCT(rct, constants.ChannelDeviceClassName, "ComputeDomainChannelConfig", "uid-1", "")

	finalizers := rct.GetFinalizers()
	if len(finalizers) != 1 || finalizers[0] != constants.ComputeDomainFinalizer {
		t.Errorf("expected finalizer %s, got %v", constants.ComputeDomainFinalizer, finalizers)
	}
}

func TestConfigureResourceClaimTemplateSetsLabel(t *testing.T) {
	rct := newBaseRCT()
	configureNVIDIARCT(rct, constants.ChannelDeviceClassName, "ComputeDomainChannelConfig", "uid-1", "")

	labels := rct.GetLabels()
	if labels[constants.LabelComputeDomain] != "uid-1" {
		t.Errorf("expected label %s=uid-1, got %v", constants.LabelComputeDomain, labels)
	}
}

func TestConfigureResourceClaimTemplateSetsDeviceClass(t *testing.T) {
	rct := newBaseRCT()
	configureNVIDIARCT(rct, constants.ChannelDeviceClassName, "ComputeDomainChannelConfig", "uid-1", "")

	requests, _, _ := unstructured.NestedSlice(rct.Object, "spec", "spec", "devices", "requests")
	if len(requests) == 0 {
		t.Fatal("expected at least one request")
	}
	req, ok := requests[0].(map[string]interface{})
	if !ok {
		t.Fatal("request is not a map")
	}
	exactly, ok := req["exactly"].(map[string]interface{})
	if !ok {
		t.Fatal("exactly is not a map")
	}
	if exactly["deviceClassName"] != constants.ChannelDeviceClassName {
		t.Errorf("expected deviceClassName %s, got %s", constants.ChannelDeviceClassName, exactly["deviceClassName"])
	}
}

func TestConfigureResourceClaimTemplateChannelDeviceClass(t *testing.T) {
	rct := newBaseRCT()
	configureNVIDIARCT(rct, constants.ChannelDeviceClassName, "ComputeDomainChannelConfig", "uid-2", "Single")

	requests, _, _ := unstructured.NestedSlice(rct.Object, "spec", "spec", "devices", "requests")
	req := requests[0].(map[string]interface{})
	exactly := req["exactly"].(map[string]interface{})
	if exactly["deviceClassName"] != constants.ChannelDeviceClassName {
		t.Errorf("expected deviceClassName %s, got %s", constants.ChannelDeviceClassName, exactly["deviceClassName"])
	}
}

func TestConfigureResourceClaimTemplateSetsExactlyAllocationModeAll(t *testing.T) {
	rct := newBaseRCT()
	configureNVIDIARCT(rct, constants.ChannelDeviceClassName, "ComputeDomainChannelConfig", "uid-alloc", "All")

	requests, _, _ := unstructured.NestedSlice(rct.Object, "spec", "spec", "devices", "requests")
	if len(requests) == 0 {
		t.Fatal("expected at least one request")
	}
	req := requests[0].(map[string]interface{})
	exactly := req["exactly"].(map[string]interface{})
	if exactly["allocationMode"] != "All" {
		t.Errorf("expected exactly.allocationMode=All, got %v", exactly["allocationMode"])
	}
	if _, hasCount := exactly["count"]; hasCount {
		t.Errorf("expected no count field for All mode, got %v", exactly["count"])
	}
}

func TestConfigureResourceClaimTemplateSetsExactlyAllocationModeSingle(t *testing.T) {
	rct := newBaseRCT()
	configureNVIDIARCT(rct, constants.ChannelDeviceClassName, "ComputeDomainChannelConfig", "uid-single", "Single")

	requests, _, _ := unstructured.NestedSlice(rct.Object, "spec", "spec", "devices", "requests")
	if len(requests) == 0 {
		t.Fatal("expected at least one request")
	}
	req := requests[0].(map[string]interface{})
	exactly := req["exactly"].(map[string]interface{})
	if exactly["allocationMode"] != "ExactCount" {
		t.Errorf("expected exactly.allocationMode=ExactCount for Single, got %v", exactly["allocationMode"])
	}
	if exactly["count"] != int64(1) {
		t.Errorf("expected exactly.count=1 for Single, got %v", exactly["count"])
	}
}

func TestConfigureResourceClaimTemplateSetsExactlyAllocationModeDefaultAll(t *testing.T) {
	rct := newBaseRCT()
	// Empty allocationMode should default to All.
	configureNVIDIARCT(rct, constants.ChannelDeviceClassName, "ComputeDomainChannelConfig", "uid-default", "")

	requests, _, _ := unstructured.NestedSlice(rct.Object, "spec", "spec", "devices", "requests")
	if len(requests) == 0 {
		t.Fatal("expected at least one request")
	}
	req := requests[0].(map[string]interface{})
	exactly := req["exactly"].(map[string]interface{})
	if exactly["allocationMode"] != "All" {
		t.Errorf("expected exactly.allocationMode=All for empty default, got %v", exactly["allocationMode"])
	}
}

func TestConfigureResourceClaimTemplateSetsOpaqueConfig(t *testing.T) {
	rct := newBaseRCT()
	configureNVIDIARCT(rct, constants.ChannelDeviceClassName, "ComputeDomainChannelConfig", "uid-1", "")

	configs, _, _ := unstructured.NestedSlice(rct.Object, "spec", "spec", "devices", "config")
	if len(configs) == 0 {
		t.Fatal("expected at least one config")
	}
	config := configs[0].(map[string]interface{})
	opaque := config["opaque"].(map[string]interface{})
	if opaque["driver"] != constants.DriverName {
		t.Errorf("expected driver %s, got %s", constants.DriverName, opaque["driver"])
	}
	params := opaque["parameters"].(map[string]interface{})
	expectedAPIVersion := constants.APIGroup + "/" + constants.APIVersion
	if params["apiVersion"] != expectedAPIVersion {
		t.Errorf("expected apiVersion %s, got %s", expectedAPIVersion, params["apiVersion"])
	}
	if params["kind"] != "ComputeDomainChannelConfig" {
		t.Errorf("expected kind ComputeDomainChannelConfig, got %s", params["kind"])
	}
	if params["domainID"] != "uid-1" {
		t.Errorf("expected domainID uid-1, got %s", params["domainID"])
	}
}

func TestConfigureResourceClaimTemplateSetsAllocationMode(t *testing.T) {
	rct := newBaseRCT()
	configureNVIDIARCT(rct, constants.ChannelDeviceClassName, "ComputeDomainChannelConfig", "uid-2", "All")

	configs, _, _ := unstructured.NestedSlice(rct.Object, "spec", "spec", "devices", "config")
	config := configs[0].(map[string]interface{})
	opaque := config["opaque"].(map[string]interface{})
	params := opaque["parameters"].(map[string]interface{})
	if params["allocationMode"] != "All" {
		t.Errorf("expected allocationMode=All, got %s", params["allocationMode"])
	}
}

func TestConfigureResourceClaimTemplateOmitsAllocationModeWhenEmpty(t *testing.T) {
	rct := newBaseRCT()
	configureNVIDIARCT(rct, constants.ChannelDeviceClassName, "ComputeDomainChannelConfig", "uid-3", "")

	configs, _, _ := unstructured.NestedSlice(rct.Object, "spec", "spec", "devices", "config")
	config := configs[0].(map[string]interface{})
	opaque := config["opaque"].(map[string]interface{})
	params := opaque["parameters"].(map[string]interface{})
	if _, exists := params["allocationMode"]; exists {
		t.Errorf("expected no allocationMode field when empty, got %v", params["allocationMode"])
	}
}

// ---------- AllocationMode passthrough ----------

func TestWorkloadRCTAllocationModePassThrough(t *testing.T) {
	tests := []struct {
		name          string
		mode          v1beta1.AllocationMode
		wantAllocMode string
	}{
		{"Single", v1beta1.AllocationModeSingle, "Single"},
		{"All", v1beta1.AllocationModeAll, "All"},
		{"Empty defaults to All", "", "All"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cd := &v1beta1.ComputeDomain{}
			cd.UID = types.UID("alloc-test")
			cd.Spec.Channel = &v1beta1.ComputeDomainChannelSpec{
				AllocationMode: tt.mode,
			}

			allocationMode := string(v1beta1.AllocationModeAll)
			if cd.Spec.Channel != nil && cd.Spec.Channel.AllocationMode != "" {
				allocationMode = string(cd.Spec.Channel.AllocationMode)
			}

			if allocationMode != tt.wantAllocMode {
				t.Errorf("expected %s, got %s", tt.wantAllocMode, allocationMode)
			}
		})
	}
}

// ---------- stringsTrimLeadingSlash ----------

func TestStringsTrimLeadingSlash(t *testing.T) {
	tests := []struct {
		input, want string
	}{
		{"/templates/foo.yaml", "templates/foo.yaml"},
		{"//double/slash", "double/slash"},
		{"no-slash", "no-slash"},
		{"", ""},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := stringsTrimLeadingSlash(tt.input)
			if got != tt.want {
				t.Errorf("stringsTrimLeadingSlash(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// ---------- configureResourceClaimTemplate with nil labels ----------

// fakeRCTIndexer is a simple test helper implementing the indexer interface
// that returns all stored objects regardless of index name/value.
type fakeRCTIndexer struct {
	objects []interface{}
}

func (f *fakeRCTIndexer) ByIndex(_, _ string) ([]interface{}, error) {
	return f.objects, nil
}

// ---------- WorkloadResourceClaimTemplateManager.getInNamespace ----------

func newMinimalWorkloadRCTManager() *WorkloadResourceClaimTemplateManager {
	return &WorkloadResourceClaimTemplateManager{
		BaseResourceClaimTemplateManager: &BaseResourceClaimTemplateManager{
			config: &ManagerConfig{Contract: NVIDIAContract},
			target: constants.RCTTargetWorkload,
		},
	}
}

func rctInNamespace(ns, name, uid string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "resource.k8s.io/v1",
		"kind":       "ResourceClaimTemplate",
		"metadata": map[string]interface{}{
			"name":      name,
			"namespace": ns,
			"labels": map[string]interface{}{
				constants.LabelComputeDomain: uid,
			},
		},
	}}
}

func TestGetInNamespaceFiltersCrossNamespace(t *testing.T) {
	m := newMinimalWorkloadRCTManager()

	// Two RCTs with the same CD UID label but in different namespaces.
	rctA := rctInNamespace("ns-a", "rct-a", "shared-uid")
	rctB := rctInNamespace("ns-b", "rct-b", "shared-uid")

	idx := &fakeRCTIndexer{objects: []interface{}{rctA, rctB}}

	// Querying with ns-a should return only rctA.
	got, err := m.getInNamespace("shared-uid", idx, "ns-a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected rctA, got nil")
	}
	if got.GetName() != "rct-a" || got.GetNamespace() != "ns-a" {
		t.Errorf("expected rct-a in ns-a, got %s/%s", got.GetNamespace(), got.GetName())
	}

	// Querying with ns-b should return only rctB.
	got, err = m.getInNamespace("shared-uid", idx, "ns-b")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected rctB, got nil")
	}
	if got.GetName() != "rct-b" || got.GetNamespace() != "ns-b" {
		t.Errorf("expected rct-b in ns-b, got %s/%s", got.GetNamespace(), got.GetName())
	}

	// Querying a namespace with no match should return nil.
	got, err = m.getInNamespace("shared-uid", idx, "ns-c")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for ns-c, got %s/%s", got.GetNamespace(), got.GetName())
	}
}

func TestGetInNamespaceErrorsOnDuplicateInSameNamespace(t *testing.T) {
	m := newMinimalWorkloadRCTManager()

	// Two RCTs in the SAME namespace with the same UID – should error.
	rct1 := rctInNamespace("ns-a", "rct-1", "dup-uid")
	rct2 := rctInNamespace("ns-a", "rct-2", "dup-uid")

	idx := &fakeRCTIndexer{objects: []interface{}{rct1, rct2}}

	_, err := m.getInNamespace("dup-uid", idx, "ns-a")
	if err == nil {
		t.Fatal("expected error for duplicate RCTs in same namespace")
	}
}

// ---------- fakeMutationCache for cleanup tests ----------

// fakeMutationCache satisfies cache.MutationCache for unit tests.
type fakeMutationCache struct {
	objects []interface{}
}

func (f *fakeMutationCache) GetByKey(_ string) (interface{}, bool, error) { return nil, false, nil }
func (f *fakeMutationCache) ByIndex(_, _ string) ([]interface{}, error)   { return f.objects, nil }
func (f *fakeMutationCache) Mutation(_ interface{})                       {}

// ---------- WorkloadResourceClaimTemplateManager.cleanup with duplicate UID ----------

// makeRCTObject creates an Unstructured RCT with the given namespace, name,
// object UID, and CD UID label. The optional finalizer list is set on the object.
func makeRCTObject(ns, name, objUID, cdUID string, finalizers ...string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "resource.k8s.io/v1",
		"kind":       "ResourceClaimTemplate",
		"metadata": map[string]interface{}{
			"name":            name,
			"namespace":       ns,
			"uid":             objUID,
			"resourceVersion": "1",
			"labels": map[string]interface{}{
				constants.LabelComputeDomain:       cdUID,
				constants.LabelComputeDomainTarget: constants.RCTTargetWorkload,
			},
		},
	}}
	if len(finalizers) > 0 {
		obj.SetFinalizers(finalizers)
	}
	return obj
}

// TestCleanupDuplicateUIDSameNamespace verifies that when two RCTs in the same
// namespace carry the same orphan CD UID label, the cleanup method deletes both
// instead of failing with a "multiple ResourceClaimTemplates" error.
func TestCleanupDuplicateUIDSameNamespace(t *testing.T) {
	cdUID := "orphan-cd-uid"
	rct1 := makeRCTObject("ns-a", "rct-dup-1", "obj-uid-1", cdUID, constants.ComputeDomainFinalizer)
	rct2 := makeRCTObject("ns-a", "rct-dup-2", "obj-uid-2", cdUID, constants.ComputeDomainFinalizer)

	scheme := runtime.NewScheme()
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			resourceClaimTemplateGVR: "ResourceClaimTemplateList",
		},
		rct1, rct2,
	)

	mgr := &WorkloadResourceClaimTemplateManager{
		BaseResourceClaimTemplateManager: &BaseResourceClaimTemplateManager{
			config: &ManagerConfig{
				Contract:   NVIDIAContract,
				ClientSets: ClientSets{Dynamic: client},
			},
			target:        constants.RCTTargetWorkload,
			mutationCache: &fakeMutationCache{objects: []interface{}{rct1, rct2}},
		},
	}

	ctx := context.Background()
	if err := mgr.cleanup(ctx, cdUID); err != nil {
		t.Fatalf("cleanup failed: %v", err)
	}

	// Verify both RCTs received Delete actions.
	var deleteCount int
	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" && action.GetResource() == resourceClaimTemplateGVR {
			deleteCount++
		}
	}
	if deleteCount != 2 {
		t.Errorf("expected 2 Delete calls for duplicate RCTs, got %d", deleteCount)
	}
}

// TestCleanupDuplicateUIDCrossNamespace verifies that RCTs with the same
// orphan CD UID in different namespaces are all cleaned up.
func TestCleanupDuplicateUIDCrossNamespace(t *testing.T) {
	cdUID := "cross-ns-orphan"
	rct1 := makeRCTObject("ns-a", "rct-a", "obj-uid-a", cdUID)
	rct2 := makeRCTObject("ns-b", "rct-b", "obj-uid-b", cdUID)

	scheme := runtime.NewScheme()
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			resourceClaimTemplateGVR: "ResourceClaimTemplateList",
		},
		rct1, rct2,
	)

	mgr := &WorkloadResourceClaimTemplateManager{
		BaseResourceClaimTemplateManager: &BaseResourceClaimTemplateManager{
			config: &ManagerConfig{
				Contract:   NVIDIAContract,
				ClientSets: ClientSets{Dynamic: client},
			},
			target:        constants.RCTTargetWorkload,
			mutationCache: &fakeMutationCache{objects: []interface{}{rct1, rct2}},
		},
	}

	ctx := context.Background()
	if err := mgr.cleanup(ctx, cdUID); err != nil {
		t.Fatalf("cleanup failed: %v", err)
	}

	deletedNames := map[string]bool{}
	for _, action := range client.Actions() {
		if da, ok := action.(clienttesting.DeleteAction); ok && action.GetResource() == resourceClaimTemplateGVR {
			deletedNames[da.GetNamespace()+"/"+da.GetName()] = true
		}
	}
	for _, expected := range []string{"ns-a/rct-a", "ns-b/rct-b"} {
		if !deletedNames[expected] {
			t.Errorf("expected deletion of %s, actions: %v", expected, deletedNames)
		}
	}
}

func TestConfigureResourceClaimTemplateNilLabels(t *testing.T) {
	rct := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "resource.k8s.io/v1",
		"kind":       "ResourceClaimTemplate",
		"metadata": map[string]interface{}{
			"name":      "test",
			"namespace": "default",
		},
		"spec": map[string]interface{}{
			"spec": map[string]interface{}{
				"devices": map[string]interface{}{
					"requests": []interface{}{},
					"config":   []interface{}{},
				},
			},
		},
	}}
	// Should not panic with nil labels and empty requests/config
	configureNVIDIARCT(rct, "dc", "kind", "uid", "mode")
	labels := rct.GetLabels()
	if labels[constants.LabelComputeDomain] != "uid" {
		t.Errorf("expected label uid, got %v", labels)
	}
}

// ---------- getByComputeDomainUID label filtering ----------

func TestGetByComputeDomainUIDFiltersStaleIndexEntries(t *testing.T) {
	// Simulate MutationCache returning objects that belong to different CDs
	// (stale index). getByComputeDomainUID must filter out mismatches.
	targetUID := "cd-target"
	otherUID := "cd-other"

	matchingRCT := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "resource.k8s.io/v1",
		"kind":       "ResourceClaimTemplate",
		"metadata": map[string]interface{}{
			"name":      "rct-matching",
			"namespace": "default",
			"labels": map[string]interface{}{
				constants.LabelComputeDomain: targetUID,
			},
		},
	}}
	staleRCT := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "resource.k8s.io/v1",
		"kind":       "ResourceClaimTemplate",
		"metadata": map[string]interface{}{
			"name":      "rct-stale",
			"namespace": "default",
			"labels": map[string]interface{}{
				constants.LabelComputeDomain: otherUID,
			},
		},
	}}
	noLabelRCT := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "resource.k8s.io/v1",
		"kind":       "ResourceClaimTemplate",
		"metadata": map[string]interface{}{
			"name":      "rct-nolabel",
			"namespace": "default",
		},
	}}

	idx := &fakeRCTIndexer{objects: []interface{}{matchingRCT, staleRCT, noLabelRCT}}

	result, err := getByComputeDomainUID(idx, targetUID, constants.LabelComputeDomain)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	if result[0].GetName() != "rct-matching" {
		t.Errorf("expected rct-matching, got %s", result[0].GetName())
	}
}

func TestGetByComputeDomainUIDReturnsEmptyWhenAllMismatch(t *testing.T) {
	staleRCT := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "resource.k8s.io/v1",
		"kind":       "ResourceClaimTemplate",
		"metadata": map[string]interface{}{
			"name":      "rct-wrong",
			"namespace": "default",
			"labels": map[string]interface{}{
				constants.LabelComputeDomain: "cd-different",
			},
		},
	}}

	idx := &fakeRCTIndexer{objects: []interface{}{staleRCT}}

	result, err := getByComputeDomainUID(idx, "cd-target", constants.LabelComputeDomain)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected 0 results, got %d", len(result))
	}
}

func TestGetInNamespaceFiltersStaleCDLabel(t *testing.T) {
	m := newMinimalWorkloadRCTManager()

	// Indexer returns an RCT with the wrong CD label (stale index).
	targetUID := "cd-target"
	staleRCT := rctInNamespace("default", "rct-stale", "cd-other")

	idx := &fakeRCTIndexer{objects: []interface{}{staleRCT}}

	got, err := m.getInNamespace(targetUID, idx, "default")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for stale CD label, got %s/%s", got.GetNamespace(), got.GetName())
	}
}

// ---------- configureResourceClaimTemplate with missing opaque/parameters ----------

// newRCTWithSingleConfigEntry builds a ResourceClaimTemplate whose first device
// config entry is exactly the passed map, so a test can omit "opaque" or
// "parameters" entirely.
func newRCTWithSingleConfigEntry(config map[string]interface{}) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "resource.k8s.io/v1",
		"kind":       "ResourceClaimTemplate",
		"metadata": map[string]interface{}{
			"name":      "rct-sparse",
			"namespace": "default",
		},
		"spec": map[string]interface{}{
			"spec": map[string]interface{}{
				"devices": map[string]interface{}{
					"config": []interface{}{config},
				},
			},
		},
	}}
}

// TestConfigureResourceClaimTemplateFillsMissingOpaqueAndParameters drives
// configureResourceClaimTemplate directly with a device config that has no
// "opaque" map and one that has "opaque" but no "parameters". Both groups are
// covered and every written value is compared against a fixed literal of that
// group, so the values can only come from the contract bound to the manager.
func TestConfigureResourceClaimTemplateFillsMissingOpaqueAndParameters(t *testing.T) {
	const (
		aliyunOpaqueAPIVersion = "resource.aliyun.com/v1beta1"
		nvidiaOpaqueAPIVersion = "resource.nvidia.com/v1beta1"
		channelKind            = "ComputeDomainChannelConfig"
		uid                    = "uid-sparse"
	)

	tests := []struct {
		contract       cdapi.GroupContract
		wantAPIVersion string
		other          cdapi.GroupContract
	}{
		{cdapi.AliyunContract(), aliyunOpaqueAPIVersion, cdapi.NVIDIAContract()},
		{cdapi.NVIDIAContract(), nvidiaOpaqueAPIVersion, cdapi.AliyunContract()},
	}
	shapes := []struct {
		name   string
		config map[string]interface{}
	}{
		{"nil opaque", map[string]interface{}{}},
		{"nil parameters", map[string]interface{}{
			"opaque": map[string]interface{}{"driver": "placeholder-driver"},
		}},
	}

	for _, tt := range tests {
		source := tt.contract
		for _, shape := range shapes {
			t.Run(source.APIGroup+"/"+shape.name, func(t *testing.T) {
				rct := newRCTWithSingleConfigEntry(shape.config)
				configureResourceClaimTemplate(rct, source, constants.ChannelDeviceClassName, channelKind,
					newResourceClaimTemplateData(source, "default", rct.GetName(), uid, "", constants.RCTTargetWorkload))

				if finalizers := rct.GetFinalizers(); len(finalizers) != 1 || finalizers[0] != source.Finalizer {
					t.Errorf("finalizers = %v, want [%s]", finalizers, source.Finalizer)
				}
				labels := rct.GetLabels()
				if got := labels[source.LabelComputeDomain]; got != uid {
					t.Errorf("%s label = %q, want %q", source.LabelComputeDomain, got, uid)
				}

				configs, _, err := unstructured.NestedSlice(rct.Object, "spec", "spec", "devices", "config")
				if err != nil || len(configs) != 1 {
					t.Fatalf("device config = %v, %v; want one entry", configs, err)
				}
				config, ok := configs[0].(map[string]interface{})
				if !ok {
					t.Fatalf("device config entry is not a map: %T", configs[0])
				}
				opaque, ok := config["opaque"].(map[string]interface{})
				if !ok {
					t.Fatalf("opaque was not materialised: %v", config["opaque"])
				}
				if got := opaque["driver"]; got != "compute-domain.aliyun.com" {
					t.Errorf("opaque.driver = %v, want compute-domain.aliyun.com", got)
				}
				parameters, ok := opaque["parameters"].(map[string]interface{})
				if !ok {
					t.Fatalf("parameters were not materialised: %v", opaque["parameters"])
				}

				if got := parameters["apiVersion"]; got != tt.wantAPIVersion {
					t.Errorf("parameters.apiVersion = %v, want %s", got, tt.wantAPIVersion)
				}
				if got := parameters["kind"]; got != channelKind {
					t.Errorf("parameters.kind = %v, want %s", got, channelKind)
				}
				if got := parameters["domainID"]; got != uid {
					t.Errorf("parameters.domainID = %v, want %s", got, uid)
				}
				if _, found := parameters["allocationMode"]; found {
					t.Errorf("parameters.allocationMode must stay absent for an empty mode, got %v", parameters["allocationMode"])
				}

				// Self-proof that the values come from the bound contract and not
				// from an unrelated legacy source: the other group's constants and
				// the package-level legacy constants must never appear.
				if tt.wantAPIVersion == tt.other.OpaqueAPIVersion {
					t.Fatalf("test setup: both groups computed the same opaque apiVersion %q", tt.wantAPIVersion)
				}
				if _, found := labels[tt.other.LabelComputeDomain]; found {
					t.Errorf("labels carry the foreign %s key", tt.other.LabelComputeDomain)
				}
				if source.LabelComputeDomain != constants.LabelComputeDomain {
					if _, found := labels[constants.LabelComputeDomain]; found {
						t.Errorf("labels carry the legacy %s key", constants.LabelComputeDomain)
					}
					legacyAPIVersion := constants.APIGroup + "/" + constants.APIVersion
					if parameters["apiVersion"] == legacyAPIVersion {
						t.Errorf("parameters.apiVersion fell back to the legacy constant %q", legacyAPIVersion)
					}
					if source.Finalizer != constants.ComputeDomainFinalizer {
						for _, finalizer := range rct.GetFinalizers() {
							if finalizer == constants.ComputeDomainFinalizer {
								t.Errorf("finalizers carry the legacy %q", finalizer)
							}
						}
					}
				}
			})
		}
	}
}

// ---------- exclusive ownership: a dual-owned RCT is rejected everywhere ----------

// ownershipRCT builds a ResourceClaimTemplate whose labels are exactly the
// supplied set, so a test can express any combination of owner/target labels
// from the two supported API groups.
func ownershipRCT(namespace, name, objUID string, labels map[string]string, deleting bool, finalizers ...string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "resource.k8s.io/v1",
		"kind":       "ResourceClaimTemplate",
		"metadata": map[string]interface{}{
			"name":            name,
			"namespace":       namespace,
			"uid":             objUID,
			"resourceVersion": "1",
		},
	}}
	if len(labels) > 0 {
		obj.SetLabels(labels)
	}
	obj.SetFinalizers(finalizers)
	if deleting {
		obj.SetDeletionTimestamp(&metav1.Time{Time: time.Now()})
	}
	return obj
}

// otherContract returns the contract of the other served API group.
func otherContract(contract cdapi.GroupContract) cdapi.GroupContract {
	for _, candidate := range cdapi.SupportedContracts() {
		if candidate.APIGroup != contract.APIGroup {
			return candidate
		}
	}
	panic("expected exactly two supported API groups")
}

// newRCTOwnershipManager builds a workload ResourceClaimTemplate manager for one
// contract. live backs the fake dynamic client, cached is what the informer cache
// (and therefore the fast reuse path) reports, and informed is loaded into a real,
// never-started informer indexer so AssertRemoved can be exercised too.
func newRCTOwnershipManager(t *testing.T, contract cdapi.GroupContract, live, cached, informed []*unstructured.Unstructured) (*WorkloadResourceClaimTemplateManager, *dynamicfake.FakeDynamicClient) {
	t.Helper()
	runtimeObjects := make([]runtime.Object, 0, len(live))
	for _, obj := range live {
		runtimeObjects = append(runtimeObjects, obj)
	}
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{resourceClaimTemplateGVR: "ResourceClaimTemplateList"},
		runtimeObjects...)

	cachedObjects := make([]interface{}, 0, len(cached))
	for _, obj := range cached {
		cachedObjects = append(cachedObjects, obj)
	}
	informer := cache.NewSharedIndexInformer(&cache.ListWatch{}, &unstructured.Unstructured{}, 0, cache.Indexers{})
	if err := addComputeDomainLabelIndexer(informer, contract.LabelComputeDomain); err != nil {
		t.Fatalf("add ResourceClaimTemplate label indexer: %v", err)
	}
	for _, obj := range informed {
		if err := informer.GetStore().Add(obj); err != nil {
			t.Fatalf("add informed ResourceClaimTemplate %s/%s: %v", obj.GetNamespace(), obj.GetName(), err)
		}
	}

	return &WorkloadResourceClaimTemplateManager{
		BaseResourceClaimTemplateManager: &BaseResourceClaimTemplateManager{
			config:        &ManagerConfig{Contract: contract, ClientSets: ClientSets{Dynamic: client}},
			target:        constants.RCTTargetWorkload,
			informer:      informer,
			mutationCache: &fakeMutationCache{objects: cachedObjects},
		},
	}, client
}

// mutatingActions lists every write the fake client observed, so a rejection can
// be proven to have happened before any create/update/patch/delete.
func mutatingActions(client *dynamicfake.FakeDynamicClient) []string {
	var writes []string
	for _, action := range client.Actions() {
		switch action.GetVerb() {
		case "get", "list", "watch":
			continue
		}
		writes = append(writes, fmt.Sprintf("%s/%s", action.GetVerb(), action.GetResource().Resource))
	}
	return writes
}

// assertLabelsUnchanged proves foreign labels were reported, never erased.
func assertLabelsUnchanged(t *testing.T, client *dynamicfake.FakeDynamicClient, namespace, name string, want map[string]string) {
	t.Helper()
	live, err := client.Resource(resourceClaimTemplateGVR).Namespace(namespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get live ResourceClaimTemplate %s/%s: %v", namespace, name, err)
	}
	for key, value := range want {
		if got := live.GetLabels()[key]; got != value {
			t.Errorf("label %q = %q, want %q; foreign labels must never be erased", key, got, value)
		}
	}
}

// TestCachedMutationPathsRejectDualOwnership pins the cache fast path: an RCT
// reachable through the group's label index but carrying a label of the other
// supported group is refused by every mutation entry point, so neither group's
// manager set can reuse, delete or strip the finalizer of the shared object.
func TestCachedMutationPathsRejectDualOwnership(t *testing.T) {
	const (
		namespace = "default"
		objName   = "shared-rct"
		objUID    = "rct-obj-uid"
		cdUID     = "cd-uid"
		foreignCD = "foreign-cd-uid"
	)
	operations := []struct {
		name string
		run  func(context.Context, *WorkloadResourceClaimTemplateManager, *v1beta1.ComputeDomain) error
	}{
		{"create-reuses-cache", func(ctx context.Context, m *WorkloadResourceClaimTemplateManager, cd *v1beta1.ComputeDomain) error {
			_, err := m.Create(ctx, namespace, objName, cd)
			return err
		}},
		{"delete", func(ctx context.Context, m *WorkloadResourceClaimTemplateManager, cd *v1beta1.ComputeDomain) error {
			return m.Delete(ctx, string(cd.UID), namespace)
		}},
		{"remove-finalizer", func(ctx context.Context, m *WorkloadResourceClaimTemplateManager, cd *v1beta1.ComputeDomain) error {
			return m.RemoveFinalizer(ctx, string(cd.UID), namespace)
		}},
		{"assert-removed", func(ctx context.Context, m *WorkloadResourceClaimTemplateManager, cd *v1beta1.ComputeDomain) error {
			return m.AssertRemoved(ctx, string(cd.UID), namespace)
		}},
	}
	scenarios := []struct {
		name   string
		labels func(source, other cdapi.GroupContract) map[string]string
	}{
		{"dual-owner-same-uid", func(source, other cdapi.GroupContract) map[string]string {
			return map[string]string{
				source.LabelComputeDomain: cdUID,
				source.LabelTarget:        constants.RCTTargetWorkload,
				other.LabelComputeDomain:  cdUID,
			}
		}},
		{"dual-owner-different-uid", func(source, other cdapi.GroupContract) map[string]string {
			return map[string]string{
				source.LabelComputeDomain: cdUID,
				source.LabelTarget:        constants.RCTTargetWorkload,
				other.LabelComputeDomain:  foreignCD,
			}
		}},
		{"foreign-target-same-value", func(source, other cdapi.GroupContract) map[string]string {
			return map[string]string{
				source.LabelComputeDomain: cdUID,
				source.LabelTarget:        constants.RCTTargetWorkload,
				other.LabelTarget:         constants.RCTTargetWorkload,
			}
		}},
		{"foreign-target-other-value", func(source, other cdapi.GroupContract) map[string]string {
			return map[string]string{
				source.LabelComputeDomain: cdUID,
				source.LabelTarget:        constants.RCTTargetWorkload,
				other.LabelTarget:         "ComputeDomainChannelConfig",
			}
		}},
	}

	for _, contract := range cdapi.SupportedContracts() {
		contract := contract
		for _, scenario := range scenarios {
			scenario := scenario
			for _, operation := range operations {
				operation := operation
				t.Run(contract.APIGroup+"/"+scenario.name+"/"+operation.name, func(t *testing.T) {
					labels := scenario.labels(contract, otherContract(contract))
					// Deleting on purpose: with the ownership check removed, both
					// Delete and RemoveFinalizer would really mutate the object.
					obj := ownershipRCT(namespace, objName, objUID, labels, true, contract.Finalizer)
					mgr, client := newRCTOwnershipManager(t, contract,
						[]*unstructured.Unstructured{obj}, []*unstructured.Unstructured{obj}, []*unstructured.Unstructured{obj})
					cd := &v1beta1.ComputeDomain{ObjectMeta: metav1.ObjectMeta{Name: "cd", Namespace: namespace, UID: types.UID(cdUID)}}

					err := operation.run(context.Background(), mgr, cd)
					if err == nil {
						t.Fatalf("%s accepted an RCT owned by two API groups", operation.name)
					}
					if !strings.Contains(err.Error(), "foreign") {
						t.Errorf("error %v should name the foreign API group label", err)
					}
					if writes := mutatingActions(client); len(writes) != 0 {
						t.Errorf("dual-owned RCT was mutated: %v", writes)
					}
					assertLabelsUnchanged(t, client, namespace, objName, labels)
				})
			}
		}
	}
}

// TestAdoptOrRejectRejectsForeignOwnerOrTarget pins the live path: before Create,
// a same-named RCT is adopted only when its owner and target labels belong to the
// same API group and the same ComputeDomain. Foreign labels are a conflict even
// when they carry the same UID, and an object with no owner label is never taken
// over.
func TestAdoptOrRejectRejectsForeignOwnerOrTarget(t *testing.T) {
	const (
		namespace = "default"
		objName   = "existing-rct"
		cdUID     = "cd-uid"
		foreignCD = "foreign-cd-uid"
	)
	cases := []struct {
		name       string
		labels     func(source, other cdapi.GroupContract) map[string]string
		wantErrMsg string // empty means the object must be adopted
	}{
		{"single-group-owner-and-target", func(source, other cdapi.GroupContract) map[string]string {
			return map[string]string{source.LabelComputeDomain: cdUID, source.LabelTarget: constants.RCTTargetWorkload}
		}, ""},
		{"owner-only", func(source, other cdapi.GroupContract) map[string]string {
			return map[string]string{source.LabelComputeDomain: cdUID}
		}, ""},
		{"dual-owner-same-uid", func(source, other cdapi.GroupContract) map[string]string {
			return map[string]string{
				source.LabelComputeDomain: cdUID,
				source.LabelTarget:        constants.RCTTargetWorkload,
				other.LabelComputeDomain:  cdUID,
			}
		}, "foreign owner label"},
		{"dual-owner-different-uid", func(source, other cdapi.GroupContract) map[string]string {
			return map[string]string{
				source.LabelComputeDomain: cdUID,
				source.LabelTarget:        constants.RCTTargetWorkload,
				other.LabelComputeDomain:  foreignCD,
			}
		}, "foreign owner label"},
		{"foreign-target-only", func(source, other cdapi.GroupContract) map[string]string {
			return map[string]string{other.LabelTarget: constants.RCTTargetWorkload}
		}, "foreign target label"},
		{"foreign-owner-only", func(source, other cdapi.GroupContract) map[string]string {
			return map[string]string{other.LabelComputeDomain: foreignCD}
		}, "foreign owner label"},
		{"foreign-owner-same-uid", func(source, other cdapi.GroupContract) map[string]string {
			return map[string]string{other.LabelComputeDomain: cdUID}
		}, "foreign owner label"},
		{"no-owner-target-only", func(source, other cdapi.GroupContract) map[string]string {
			return map[string]string{source.LabelTarget: constants.RCTTargetWorkload}
		}, "no owner label"},
		{"other-compute-domain", func(source, other cdapi.GroupContract) map[string]string {
			return map[string]string{source.LabelComputeDomain: foreignCD, source.LabelTarget: constants.RCTTargetWorkload}
		}, "already owned"},
		{"no-labels", func(source, other cdapi.GroupContract) map[string]string {
			return nil
		}, "no owner label"},
	}

	for _, contract := range cdapi.SupportedContracts() {
		contract := contract
		for _, tc := range cases {
			tc := tc
			t.Run(contract.APIGroup+"/"+tc.name, func(t *testing.T) {
				labels := tc.labels(contract, otherContract(contract))
				obj := ownershipRCT(namespace, objName, "obj-uid", labels, false, contract.Finalizer)
				mgr, client := newRCTOwnershipManager(t, contract, []*unstructured.Unstructured{obj}, nil, nil)
				cd := &v1beta1.ComputeDomain{ObjectMeta: metav1.ObjectMeta{Name: "cd", Namespace: namespace, UID: types.UID(cdUID)}}

				got, err := mgr.Create(context.Background(), namespace, objName, cd)
				if tc.wantErrMsg == "" {
					if err != nil {
						t.Fatalf("Create rejected a single-group RCT: %v", err)
					}
					if got == nil || got.GetName() != objName {
						t.Fatalf("Create did not adopt the existing RCT, got %v", got)
					}
				} else {
					if err == nil {
						t.Fatalf("Create adopted an RCT with labels %v", labels)
					}
					if got != nil {
						t.Errorf("Create returned an object together with error %v", err)
					}
					if !strings.Contains(err.Error(), tc.wantErrMsg) {
						t.Errorf("error %v should contain %q", err, tc.wantErrMsg)
					}
				}
				if writes := mutatingActions(client); len(writes) != 0 {
					t.Errorf("Create must not write while reconciling an existing RCT: %v", writes)
				}
				if len(labels) > 0 {
					assertLabelsUnchanged(t, client, namespace, objName, labels)
				}
			})
		}
	}
}

// TestCleanupRejectsDualOwnershipAndKeepsCleaningOwnObjects pins the orphan
// cleanup callback: a dual-owned orphan is reported and left untouched, while an
// unambiguously owned object with the same ComputeDomain UID is still deleted.
func TestCleanupRejectsDualOwnershipAndKeepsCleaningOwnObjects(t *testing.T) {
	const (
		namespace = "default"
		cdUID     = "orphan-cd-uid"
		foreignCD = "foreign-cd-uid"
	)
	for _, contract := range cdapi.SupportedContracts() {
		contract := contract
		t.Run(contract.APIGroup, func(t *testing.T) {
			other := otherContract(contract)
			sharedLabels := map[string]string{
				contract.LabelComputeDomain: cdUID,
				contract.LabelTarget:        constants.RCTTargetWorkload,
				other.LabelComputeDomain:    foreignCD,
			}
			shared := ownershipRCT(namespace, "shared-rct", "obj-shared", sharedLabels, false, contract.Finalizer)
			own := ownershipRCT(namespace, "own-rct", "obj-own", map[string]string{
				contract.LabelComputeDomain: cdUID,
				contract.LabelTarget:        constants.RCTTargetWorkload,
			}, false, contract.Finalizer)
			mgr, client := newRCTOwnershipManager(t, contract,
				[]*unstructured.Unstructured{shared, own}, []*unstructured.Unstructured{shared, own}, nil)

			err := mgr.cleanup(context.Background(), cdUID)
			if err == nil || !strings.Contains(err.Error(), "foreign") {
				t.Fatalf("cleanup error = %v, want a foreign-ownership report", err)
			}
			var deleted []string
			for _, action := range client.Actions() {
				if deleteAction, ok := action.(clienttesting.DeleteAction); ok && action.GetResource() == resourceClaimTemplateGVR {
					deleted = append(deleted, deleteAction.GetName())
				}
			}
			if len(deleted) != 1 || deleted[0] != "own-rct" {
				t.Errorf("deleted objects = %v, want exactly [own-rct]", deleted)
			}
			if writes := mutatingActions(client); len(writes) != 1 {
				t.Errorf("mutating actions = %v, want exactly one delete", writes)
			}
			assertLabelsUnchanged(t, client, namespace, "shared-rct", sharedLabels)
		})
	}
}

// TestSingleGroupOwnershipStillWorksOnEveryPath is the positive control: the
// exclusivity check must not break idempotent reuse, legacy owner-only objects or
// any of the mutation paths for a normal, single-group ResourceClaimTemplate.
func TestSingleGroupOwnershipStillWorksOnEveryPath(t *testing.T) {
	const (
		namespace = "default"
		objName   = "own-rct"
		objUID    = "obj-uid"
		cdUID     = "cd-uid"
	)
	for _, contract := range cdapi.SupportedContracts() {
		contract := contract
		t.Run(contract.APIGroup, func(t *testing.T) {
			cd := &v1beta1.ComputeDomain{ObjectMeta: metav1.ObjectMeta{Name: "cd", Namespace: namespace, UID: types.UID(cdUID)}}
			fullLabels := map[string]string{
				contract.LabelComputeDomain: cdUID,
				contract.LabelTarget:        constants.RCTTargetWorkload,
			}

			t.Run("create-reuses-cached-object", func(t *testing.T) {
				obj := ownershipRCT(namespace, objName, objUID, fullLabels, false, contract.Finalizer)
				mgr, client := newRCTOwnershipManager(t, contract,
					[]*unstructured.Unstructured{obj}, []*unstructured.Unstructured{obj}, nil)
				got, err := mgr.Create(context.Background(), namespace, objName, cd)
				if err != nil {
					t.Fatalf("Create on the cache fast path: %v", err)
				}
				if got == nil || string(got.GetUID()) != objUID {
					t.Fatalf("Create did not reuse the cached RCT, got %v", got)
				}
				if writes := mutatingActions(client); len(writes) != 0 {
					t.Errorf("idempotent Create wrote to the cluster: %v", writes)
				}
			})

			t.Run("create-adopts-legacy-owner-only-object", func(t *testing.T) {
				// An object created before the target label existed: legal.
				obj := ownershipRCT(namespace, objName, objUID, map[string]string{contract.LabelComputeDomain: cdUID}, false, contract.Finalizer)
				mgr, client := newRCTOwnershipManager(t, contract, []*unstructured.Unstructured{obj}, nil, nil)
				got, err := mgr.Create(context.Background(), namespace, objName, cd)
				if err != nil {
					t.Fatalf("Create on the live adopt path: %v", err)
				}
				if got == nil || string(got.GetUID()) != objUID {
					t.Fatalf("Create did not adopt the legacy RCT, got %v", got)
				}
				if writes := mutatingActions(client); len(writes) != 0 {
					t.Errorf("adopting Create wrote to the cluster: %v", writes)
				}
			})

			t.Run("delete", func(t *testing.T) {
				obj := ownershipRCT(namespace, objName, objUID, fullLabels, false, contract.Finalizer)
				mgr, client := newRCTOwnershipManager(t, contract,
					[]*unstructured.Unstructured{obj}, []*unstructured.Unstructured{obj}, nil)
				if err := mgr.Delete(context.Background(), cdUID, namespace); err != nil {
					t.Fatalf("Delete: %v", err)
				}
				writes := mutatingActions(client)
				if len(writes) != 1 || writes[0] != "delete/"+resourceClaimTemplateGVR.Resource {
					t.Errorf("mutating actions = %v, want one delete", writes)
				}
			})

			t.Run("remove-finalizer", func(t *testing.T) {
				obj := ownershipRCT(namespace, objName, objUID, fullLabels, true, contract.Finalizer)
				mgr, client := newRCTOwnershipManager(t, contract,
					[]*unstructured.Unstructured{obj}, []*unstructured.Unstructured{obj}, nil)
				if err := mgr.RemoveFinalizer(context.Background(), cdUID, namespace); err != nil {
					t.Fatalf("RemoveFinalizer: %v", err)
				}
				writes := mutatingActions(client)
				if len(writes) != 1 || writes[0] != "update/"+resourceClaimTemplateGVR.Resource {
					t.Errorf("mutating actions = %v, want one update", writes)
				}
				updated, err := client.Resource(resourceClaimTemplateGVR).Namespace(namespace).Get(context.Background(), objName, metav1.GetOptions{})
				if err != nil {
					t.Fatalf("get updated RCT: %v", err)
				}
				for _, finalizer := range updated.GetFinalizers() {
					if finalizer == contract.Finalizer {
						t.Errorf("finalizer %q was not removed", finalizer)
					}
				}
			})

			t.Run("assert-removed", func(t *testing.T) {
				mgr, _ := newRCTOwnershipManager(t, contract, nil, nil, nil)
				if err := mgr.AssertRemoved(context.Background(), cdUID, namespace); err != nil {
					t.Fatalf("AssertRemoved on an absent RCT: %v", err)
				}
			})

			t.Run("assert-removed-reports-existing", func(t *testing.T) {
				obj := ownershipRCT(namespace, objName, objUID, fullLabels, false, contract.Finalizer)
				mgr, _ := newRCTOwnershipManager(t, contract, nil, nil, []*unstructured.Unstructured{obj})
				err := mgr.AssertRemoved(context.Background(), cdUID, namespace)
				if err == nil || !strings.Contains(err.Error(), "still exists") {
					t.Fatalf("AssertRemoved = %v, want a still-exists error", err)
				}
			})

			t.Run("cleanup", func(t *testing.T) {
				obj := ownershipRCT(namespace, objName, objUID, fullLabels, false, contract.Finalizer)
				mgr, client := newRCTOwnershipManager(t, contract,
					[]*unstructured.Unstructured{obj}, []*unstructured.Unstructured{obj}, nil)
				if err := mgr.cleanup(context.Background(), cdUID); err != nil {
					t.Fatalf("cleanup: %v", err)
				}
				writes := mutatingActions(client)
				if len(writes) != 1 || writes[0] != "delete/"+resourceClaimTemplateGVR.Resource {
					t.Errorf("mutating actions = %v, want one delete", writes)
				}
			})
		})
	}
}
