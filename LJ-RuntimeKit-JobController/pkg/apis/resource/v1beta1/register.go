package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/lj-runtimekit/job-controller/pkg/computedomain"
)

// SchemeGroupVersion is the legacy single-group version of these objects.
//
// Deprecated: the controller serves both resource.aliyun.com and
// resource.nvidia.com concurrently. Use SchemeGroupVersions() (or a
// computedomain.GroupContract) instead of this constant.
var SchemeGroupVersion = schema.GroupVersion{Group: computedomain.GroupNVIDIA, Version: computedomain.APIVersion}

var (
	// SchemeBuilder is used to add go types to the GroupVersionResource scheme.
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

	// AddToScheme applies all the stored functions to the scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

// SchemeGroupVersions returns every GroupVersion under which the
// ComputeDomain types are registered, in canonical order.
func SchemeGroupVersions() []schema.GroupVersion {
	contracts := computedomain.SupportedContracts()
	versions := make([]schema.GroupVersion, 0, len(contracts))
	for _, contract := range contracts {
		versions = append(versions, contract.GroupVersion)
	}
	return versions
}

// Resource takes an unqualified resource and returns a Group qualified GroupResource.
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

// addKnownTypes registers the ComputeDomain types under every supported API
// group so that a single scheme can decode objects from both groups.
func addKnownTypes(scheme *runtime.Scheme) error {
	for _, groupVersion := range SchemeGroupVersions() {
		scheme.AddKnownTypes(groupVersion,
			&ComputeDomain{},
			&ComputeDomainList{},
			&ComputeDomainClique{},
			&ComputeDomainCliqueList{},
		)
		metav1.AddToGroupVersion(scheme, groupVersion)
	}
	return nil
}
