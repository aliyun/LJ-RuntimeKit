package computedomain

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// ---------- both groups are always served ----------

// TestBothAPIGroupsAreAlwaysServed pins that the contract registry exposes both
// API groups unconditionally: no default, no enable/disable list, no subset.
func TestBothAPIGroupsAreAlwaysServed(t *testing.T) {
	groups := SupportedAPIGroups()
	want := []string{GroupAliyun, GroupNVIDIA}
	if strings.Join(groups, ",") != strings.Join(want, ",") {
		t.Fatalf("SupportedAPIGroups() = %v, want %v", groups, want)
	}

	contracts := SupportedContracts()
	if len(contracts) != len(want) {
		t.Fatalf("SupportedContracts() has %d entries, want %d", len(contracts), len(want))
	}
	for i, contract := range contracts {
		if contract.APIGroup != want[i] {
			t.Fatalf("contract %d = %q, want %q", i, contract.APIGroup, want[i])
		}
		// Values returned by the registry are copies of the single source of truth,
		// so they must still be equal to what ContractForAPIGroup resolves.
		if got, ok := ContractForAPIGroup(contract.APIGroup); !ok || got != contract {
			t.Fatalf("ContractForAPIGroup(%q) = %+v,%v want %+v", contract.APIGroup, got, ok, contract)
		}
	}

	if _, ok := ContractForAPIGroup("resource.example.com"); ok {
		t.Fatal("ContractForAPIGroup must not resolve an unknown group")
	}
	if _, ok := ContractForAPIGroup(""); ok {
		t.Fatal("ContractForAPIGroup must not resolve an empty group")
	}
}

// ---------- contract derivation ----------

func TestGroupContractDerivation(t *testing.T) {
	tests := []struct {
		contract GroupContract
		group    string
	}{
		{AliyunContract(), GroupAliyun},
		{NVIDIAContract(), GroupNVIDIA},
	}
	for _, tt := range tests {
		contract := tt.contract
		group := tt.group
		t.Run(group, func(t *testing.T) {
			groupVersion := schema.GroupVersion{Group: group, Version: APIVersion}
			if contract.APIGroup != group {
				t.Errorf("APIGroup = %q, want %q", contract.APIGroup, group)
			}
			if contract.APIVersion != APIVersion {
				t.Errorf("APIVersion = %q, want %q", contract.APIVersion, APIVersion)
			}
			if contract.GroupVersion != groupVersion {
				t.Errorf("GroupVersion = %v, want %v", contract.GroupVersion, groupVersion)
			}
			if contract.OpaqueAPIVersion != group+"/"+APIVersion {
				t.Errorf("OpaqueAPIVersion = %q, want %q", contract.OpaqueAPIVersion, group+"/"+APIVersion)
			}
			if want := groupVersion.WithResource(ComputeDomainResource); contract.ComputeDomainGVR != want {
				t.Errorf("ComputeDomainGVR = %v, want %v", contract.ComputeDomainGVR, want)
			}
			if want := groupVersion.WithResource(ComputeDomainCliqueResource); contract.ComputeDomainCliqueGVR != want {
				t.Errorf("ComputeDomainCliqueGVR = %v, want %v", contract.ComputeDomainCliqueGVR, want)
			}
			if want := group + "/computeDomain"; contract.LabelComputeDomain != want {
				t.Errorf("LabelComputeDomain = %q, want %q", contract.LabelComputeDomain, want)
			}
			if want := group + "/computeDomainTarget"; contract.LabelTarget != want {
				t.Errorf("LabelTarget = %q, want %q", contract.LabelTarget, want)
			}
			if contract.Finalizer != contract.LabelComputeDomain {
				t.Errorf("Finalizer = %q, want %q", contract.Finalizer, contract.LabelComputeDomain)
			}
			if contract.LabelCliqueDomain != LabelComputeDomainCliqueDomain {
				t.Errorf("LabelCliqueDomain = %q, want shared %q", contract.LabelCliqueDomain, LabelComputeDomainCliqueDomain)
			}
			if contract.LabelClique != LabelComputeDomainClique {
				t.Errorf("LabelClique = %q, want shared %q", contract.LabelClique, LabelComputeDomainClique)
			}
		})
	}
}

// TestGroupContractsNeverShareComputeDomainKeys pins the "no double write" rule:
// the two contracts must not agree on the group-scoped keys, while they must
// agree on the shared on-site clique labels.
func TestGroupContractsNeverShareComputeDomainKeys(t *testing.T) {
	if AliyunContract().LabelComputeDomain == NVIDIAContract().LabelComputeDomain {
		t.Fatalf("LabelComputeDomain collides: %q", AliyunContract().LabelComputeDomain)
	}
	if AliyunContract().LabelTarget == NVIDIAContract().LabelTarget {
		t.Fatalf("LabelTarget collides: %q", AliyunContract().LabelTarget)
	}
	if AliyunContract().Finalizer == NVIDIAContract().Finalizer {
		t.Fatalf("Finalizer collides: %q", AliyunContract().Finalizer)
	}
	if AliyunContract().OpaqueAPIVersion == NVIDIAContract().OpaqueAPIVersion {
		t.Fatalf("OpaqueAPIVersion collides: %q", AliyunContract().OpaqueAPIVersion)
	}
	if AliyunContract().ComputeDomainGVR == NVIDIAContract().ComputeDomainGVR {
		t.Fatalf("ComputeDomainGVR collides: %v", AliyunContract().ComputeDomainGVR)
	}
	if AliyunContract().LabelCliqueDomain != NVIDIAContract().LabelCliqueDomain ||
		AliyunContract().LabelClique != NVIDIAContract().LabelClique {
		t.Fatal("the on-site clique labels must be shared by both groups")
	}
}

func TestContractOwnedComputeDomainUID(t *testing.T) {
	labels := map[string]string{
		AliyunContract().LabelComputeDomain: "aliyun-uid",
	}
	uid, ok := AliyunContract().OwnedComputeDomainUID(labels)
	if !ok || uid != "aliyun-uid" {
		t.Fatalf("AliyunContract().OwnedComputeDomainUID() = %q,%v want aliyun-uid,true", uid, ok)
	}
	if uid, ok := NVIDIAContract().OwnedComputeDomainUID(labels); ok {
		t.Fatalf("NVIDIAContract() must not read the Aliyun label, got %q", uid)
	}
	if _, ok := AliyunContract().OwnedComputeDomainUID(nil); ok {
		t.Fatal("nil labels must not report an owner")
	}
}

// ---------- the exported surface cannot mutate the served contracts ----------

// TestContractAccessorsReturnDetachedCopies pins that the contract values are
// private: every accessor hands out a copy, so a caller that rewrites what it
// received (labels, finalizer, GVR, opaque apiVersion) can neither change the
// value the controller already holds nor what later calls return.
func TestContractAccessorsReturnDetachedCopies(t *testing.T) {
	original := map[string]GroupContract{
		GroupAliyun: AliyunContract(),
		GroupNVIDIA: NVIDIAContract(),
	}

	// Mutate everything a caller can reach through the accessors.
	mutatedAliyun := AliyunContract()
	mutatedAliyun.APIGroup = "resource.tampered.example.com"
	mutatedAliyun.APIVersion = "v9"
	mutatedAliyun.GroupVersion = schema.GroupVersion{Group: "tampered", Version: "v9"}
	mutatedAliyun.OpaqueAPIVersion = "tampered/v9"
	mutatedAliyun.ComputeDomainGVR = schema.GroupVersionResource{Group: "tampered", Version: "v9", Resource: "tampered"}
	mutatedAliyun.ComputeDomainCliqueGVR = schema.GroupVersionResource{Group: "tampered", Version: "v9", Resource: "tampered"}
	mutatedAliyun.LabelComputeDomain = "tampered/computeDomain"
	mutatedAliyun.LabelTarget = "tampered/computeDomainTarget"
	mutatedAliyun.Finalizer = "tampered/finalizer"
	mutatedAliyun.LabelCliqueDomain = "tampered/cliqueDomain"
	mutatedAliyun.LabelClique = "tampered/clique"

	mutatedNVIDIA := NVIDIAContract()
	mutatedNVIDIA.LabelComputeDomain = "tampered-nvidia/computeDomain"
	mutatedNVIDIA.Finalizer = "tampered-nvidia/finalizer"

	// A mutation of the slice returned by SupportedContracts must also stay local.
	contracts := SupportedContracts()
	contracts[0] = mutatedAliyun
	contracts[1] = mutatedNVIDIA

	if got := AliyunContract(); got != original[GroupAliyun] {
		t.Errorf("AliyunContract() changed after the caller mutated its copy: %+v", got)
	}
	if got := NVIDIAContract(); got != original[GroupNVIDIA] {
		t.Errorf("NVIDIAContract() changed after the caller mutated its copy: %+v", got)
	}

	fresh := SupportedContracts()
	if len(fresh) != len(original) {
		t.Fatalf("SupportedContracts() has %d entries, want %d", len(fresh), len(original))
	}
	for _, contract := range fresh {
		want, ok := original[contract.APIGroup]
		if !ok {
			t.Fatalf("SupportedContracts() returned an unknown API group %q after a caller mutation", contract.APIGroup)
		}
		if contract != want {
			t.Errorf("SupportedContracts() entry %q changed after a caller mutation: %+v", contract.APIGroup, contract)
		}
	}

	// The backing array must be new on every call: writing through one result
	// must not be visible through another.
	second := SupportedContracts()
	fresh[0] = mutatedAliyun
	if second[0] != original[second[0].APIGroup] {
		t.Errorf("SupportedContracts() results share one backing array: %+v", second[0])
	}

	for _, group := range SupportedAPIGroups() {
		resolved, ok := ContractForAPIGroup(group)
		if !ok || resolved != original[group] {
			t.Errorf("ContractForAPIGroup(%q) = %+v,%v want %+v", group, resolved, ok, original[group])
		}
	}
}

// ---------- exclusive ownership ----------

// TestVerifyExclusiveOwnership pins the single ownership decision every
// ResourceClaimTemplate path shares. It fails closed on any label of another
// supported API group - including one carrying the same ComputeDomain uid, which
// the label values alone cannot tell apart - accepts an owner-only object so
// legacy objects and the creation window stay valid, and never treats a missing
// owner label as adoptable.
func TestVerifyExclusiveOwnership(t *testing.T) {
	const (
		uid         = "cd-uid"
		otherUID    = "other-cd-uid"
		target      = "Workload"
		otherTarget = "ComputeDomainChannelConfig"
	)
	for _, contract := range SupportedContracts() {
		contract := contract
		var other GroupContract
		for _, candidate := range SupportedContracts() {
			if candidate.APIGroup != contract.APIGroup {
				other = candidate
			}
		}
		tests := []struct {
			name       string
			labels     map[string]string
			uid        string
			target     string
			wantErrMsg string
		}{
			{"own owner and target", map[string]string{contract.LabelComputeDomain: uid, contract.LabelTarget: target}, uid, target, ""},
			{"own owner only", map[string]string{contract.LabelComputeDomain: uid}, uid, target, ""},
			{"own owner, no expectation", map[string]string{contract.LabelComputeDomain: uid}, "", "", ""},
			{"dual owner, same uid", map[string]string{contract.LabelComputeDomain: uid, other.LabelComputeDomain: uid}, uid, "", "foreign owner label"},
			{"dual owner, other uid", map[string]string{contract.LabelComputeDomain: uid, other.LabelComputeDomain: otherUID}, uid, "", "foreign owner label"},
			{"foreign target only", map[string]string{other.LabelTarget: target}, uid, target, "foreign target label"},
			{"foreign target only, other value", map[string]string{other.LabelTarget: otherTarget}, uid, target, "foreign target label"},
			{"own owner plus foreign target", map[string]string{contract.LabelComputeDomain: uid, other.LabelTarget: target}, uid, target, "foreign target label"},
			{"no owner label", map[string]string{contract.LabelTarget: target}, uid, target, "no owner label"},
			{"no labels", nil, uid, target, "no owner label"},
			{"other compute domain", map[string]string{contract.LabelComputeDomain: otherUID}, uid, target, "not " + uid},
			{"target mismatch", map[string]string{contract.LabelComputeDomain: uid, contract.LabelTarget: otherTarget}, uid, target, "does not match"},
		}
		for _, tc := range tests {
			tc := tc
			t.Run(contract.APIGroup+"/"+tc.name, func(t *testing.T) {
				err := contract.VerifyExclusiveOwnership(tc.labels, tc.uid, tc.target)
				if tc.wantErrMsg == "" {
					if err != nil {
						t.Fatalf("VerifyExclusiveOwnership(%v) = %v, want nil", tc.labels, err)
					}
					return
				}
				if err == nil {
					t.Fatalf("VerifyExclusiveOwnership(%v) = nil, want an error containing %q", tc.labels, tc.wantErrMsg)
				}
				if !strings.Contains(err.Error(), tc.wantErrMsg) {
					t.Errorf("error %v should contain %q", err, tc.wantErrMsg)
				}
			})
		}
	}
}
