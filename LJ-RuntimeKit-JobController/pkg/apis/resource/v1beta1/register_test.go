package v1beta1

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/lj-runtimekit/job-controller/pkg/computedomain"
)

// TestSchemeRegistersBothAPIGroups verifies that a single scheme can decode and
// create objects from both supported ComputeDomain API groups.
func TestSchemeRegistersBothAPIGroups(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	for _, group := range computedomain.SupportedAPIGroups() {
		gv := schema.GroupVersion{Group: group, Version: computedomain.APIVersion}
		for _, kind := range []string{"ComputeDomain", "ComputeDomainList", "ComputeDomainClique", "ComputeDomainCliqueList"} {
			if !scheme.Recognizes(gv.WithKind(kind)) {
				t.Errorf("scheme does not recognise %s in group %s", kind, group)
			}
			if _, err := scheme.New(gv.WithKind(kind)); err != nil {
				t.Errorf("scheme.New(%s in %s): %v", kind, group, err)
			}
		}
	}
}

// TestSchemeGroupVersionsCoverBothGroups pins the canonical order of the
// registered group versions.
func TestSchemeGroupVersionsCoverBothGroups(t *testing.T) {
	versions := SchemeGroupVersions()
	if len(versions) != 2 {
		t.Fatalf("SchemeGroupVersions() has %d entries, want 2: %v", len(versions), versions)
	}
	wantFirst := schema.GroupVersion{Group: computedomain.GroupAliyun, Version: computedomain.APIVersion}
	wantSecond := schema.GroupVersion{Group: computedomain.GroupNVIDIA, Version: computedomain.APIVersion}
	if versions[0] != wantFirst || versions[1] != wantSecond {
		t.Fatalf("SchemeGroupVersions() = %v, want [%v %v]", versions, wantFirst, wantSecond)
	}
}
