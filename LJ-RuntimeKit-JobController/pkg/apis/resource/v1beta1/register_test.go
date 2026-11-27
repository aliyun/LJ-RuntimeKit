/*
Copyright (c) 2024-2026 Alibaba Cloud.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
