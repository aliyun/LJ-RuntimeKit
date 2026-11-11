package computedomain

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupContract is the immutable, per-API-group contract that every
// group-dependent value in the ComputeDomain controller is derived from.
//
// It is the single source of truth for:
//   - the GroupVersion / GVRs used by the dynamic client,
//   - the group-scoped labels and finalizer written on ComputeDomains, Nodes
//     and ResourceClaimTemplates,
//   - the opaque apiVersion written into ResourceClaimTemplate device configs.
//
// Contracts are value types built once at package initialisation and kept in
// unexported package variables. Callers can only obtain a copy through
// AliyunContract(), NVIDIAContract(), SupportedContracts() or
// ContractForAPIGroup(), so no caller can mutate the shared value and change
// what the running controller serves. A controller instance runs one manager set
// per contract, and runtime code must never switch a global API group: both
// groups are served simultaneously by isolated manager sets, each carrying its
// own contract.
type GroupContract struct {
	// APIGroup is the API group, e.g. "resource.aliyun.com".
	APIGroup string

	// APIVersion is the served and storage version. Both supported groups
	// currently serve "v1beta1".
	APIVersion string

	// GroupVersion is APIGroup + APIVersion.
	GroupVersion schema.GroupVersion

	// OpaqueAPIVersion is the "group/version" string injected into the opaque
	// device parameters of generated ResourceClaimTemplates. It tells the
	// ComputeDomain driver which ComputeDomain API group the domainID belongs to.
	OpaqueAPIVersion string

	// ComputeDomainGVR is the ComputeDomain resource GVR of this group.
	ComputeDomainGVR schema.GroupVersionResource

	// ComputeDomainCliqueGVR is the ComputeDomainClique resource GVR of this
	// group. Both groups share the on-site aliyun.com clique labels, so every
	// query is scoped by this GVR to stay inside the source group.
	ComputeDomainCliqueGVR schema.GroupVersionResource

	// LabelComputeDomain marks ComputeDomain-owned Nodes and
	// ResourceClaimTemplates with the owning ComputeDomain UID.
	LabelComputeDomain string

	// LabelTarget marks a ResourceClaimTemplate with its target type.
	LabelTarget string

	// Finalizer is the group-scoped finalizer used on ComputeDomains and
	// workload ResourceClaimTemplates.
	Finalizer string

	// LabelCliqueDomain associates a ComputeDomainClique with its ComputeDomain
	// metadata.uid. Shared by both groups (on-site contract).
	LabelCliqueDomain string

	// LabelClique identifies a ComputeDomainClique's clique ID. Shared by both
	// groups (on-site contract).
	LabelClique string
}

// The two contracts are immutable values. They are deliberately unexported: an
// exported mutable package variable would let any importer rewrite the labels,
// finalizer or GVRs the controller uses at runtime.
var (
	aliyunContract = newGroupContract(GroupAliyun)
	nvidiaContract = newGroupContract(GroupNVIDIA)
)

// AliyunContract returns the resource.aliyun.com/v1beta1 contract. The returned
// value is a copy and contains no references, so callers cannot mutate the
// contract owned by this package.
func AliyunContract() GroupContract { return aliyunContract }

// NVIDIAContract returns the resource.nvidia.com/v1beta1 contract. The returned
// value is a copy and contains no references, so callers cannot mutate the
// contract owned by this package.
func NVIDIAContract() GroupContract { return nvidiaContract }

// newGroupContract derives every group-dependent value from the API group name
// so that no literal can drift between the two contracts.
func newGroupContract(group string) GroupContract {
	groupVersion := schema.GroupVersion{Group: group, Version: APIVersion}
	labelComputeDomain := group + "/computeDomain"
	return GroupContract{
		APIGroup:               group,
		APIVersion:             APIVersion,
		GroupVersion:           groupVersion,
		OpaqueAPIVersion:       groupVersion.String(),
		ComputeDomainGVR:       groupVersion.WithResource(ComputeDomainResource),
		ComputeDomainCliqueGVR: groupVersion.WithResource(ComputeDomainCliqueResource),
		LabelComputeDomain:     labelComputeDomain,
		LabelTarget:            group + "/computeDomainTarget",
		Finalizer:              labelComputeDomain,
		LabelCliqueDomain:      LabelComputeDomainCliqueDomain,
		LabelClique:            LabelComputeDomainClique,
	}
}

// SupportedAPIGroups returns every API group the controller serves. Both groups
// are always served at the same time, in this stable order.
func SupportedAPIGroups() []string {
	return []string{GroupAliyun, GroupNVIDIA}
}

// SupportedContracts returns one contract per served API group, in the stable
// order of SupportedAPIGroups. The controller starts one isolated manager set
// per returned contract; there is no way to serve a subset.
//
// Every call allocates a fresh slice holding copies of the package contracts, so
// a caller that mutates the result (or the slice itself) cannot influence later
// calls or the values already handed to a running controller.
func SupportedContracts() []GroupContract {
	return []GroupContract{aliyunContract, nvidiaContract}
}

// ContractForAPIGroup resolves a contract by API group name.
func ContractForAPIGroup(group string) (GroupContract, bool) {
	for _, contract := range SupportedContracts() {
		if contract.APIGroup == group {
			return contract, true
		}
	}
	return GroupContract{}, false
}

// OwnedComputeDomainUID returns the ComputeDomain UID recorded on the given
// object labels by this contract.
func (c GroupContract) OwnedComputeDomainUID(labels map[string]string) (string, bool) {
	uid := labels[c.LabelComputeDomain]
	return uid, uid != ""
}

// VerifyExclusiveOwnership reports whether labels designate exactly one owner:
// the ComputeDomain uid recorded under this contract's owner label.
//
// The check fails closed. Any owner or target label of another supported API
// group makes the object ambiguous, even when it carries the same uid: each
// group runs its own manager set and informer over the shared object, so either
// set could otherwise adopt, mutate, strip the finalizer of or delete it on
// behalf of the other. Foreign labels are a conflict, never something to erase.
//
// uid, when non-empty, must equal the recorded owner. target, when non-empty,
// must equal the target label value when that label is present; a missing target
// label is accepted so objects created before the label existed (or observed
// while they are being created) stay valid. An object with no owner label at all
// is rejected.
func (c GroupContract) VerifyExclusiveOwnership(labels map[string]string, uid, target string) error {
	for _, other := range SupportedContracts() {
		if other.APIGroup == c.APIGroup {
			continue
		}
		if value, ok := labels[other.LabelComputeDomain]; ok {
			return fmt.Errorf("object carries foreign owner label %q=%q of API group %s; refusing to adopt, mutate or delete it from API group %s",
				other.LabelComputeDomain, value, other.APIGroup, c.APIGroup)
		}
		if value, ok := labels[other.LabelTarget]; ok {
			return fmt.Errorf("object carries foreign target label %q=%q of API group %s; refusing to adopt, mutate or delete it from API group %s",
				other.LabelTarget, value, other.APIGroup, c.APIGroup)
		}
	}
	owner := labels[c.LabelComputeDomain]
	if owner == "" {
		return fmt.Errorf("object carries no owner label %q of API group %s", c.LabelComputeDomain, c.APIGroup)
	}
	if uid != "" && owner != uid {
		return fmt.Errorf("object is owned by ComputeDomain %s, not %s (API group %s)", owner, uid, c.APIGroup)
	}
	if value, ok := labels[c.LabelTarget]; ok && target != "" && value != target {
		return fmt.Errorf("object target label %q=%q does not match %q (API group %s)",
			c.LabelTarget, value, target, c.APIGroup)
	}
	return nil
}
