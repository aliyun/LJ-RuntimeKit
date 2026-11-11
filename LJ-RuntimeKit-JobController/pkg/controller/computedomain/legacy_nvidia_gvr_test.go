package computedomain

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"

	cdapi "github.com/lj-runtimekit/job-controller/pkg/computedomain"
)

// Test-local shorthands for the two immutable group contracts. Production code
// has no exported contract variables at all: the package hands out copies
// through cdapi.AliyunContract()/cdapi.NVIDIAContract(), and every manager binds
// exactly one GroupContract at construction time.
var (
	NVIDIAContract = cdapi.NVIDIAContract()
	AliyunContract = cdapi.AliyunContract()
)

// Legacy NVIDIA-contract GVRs used by the pre-dual-group tests in this package.
// Production code has no such globals: every manager reads its GVRs from the
// bound GroupContract. New tests must reference the contract under test
// (AliyunContract / NVIDIAContract) explicitly instead of these aliases.
var (
	computeDomainGVR       = NVIDIAContract.ComputeDomainGVR
	computeDomainCliqueGVR = NVIDIAContract.ComputeDomainCliqueGVR
)

// mustNewController builds a Controller from cfg.
//
// NewController constructs the shared client-go rate limiting queue, whose
// waitingLoop goroutine starts immediately. The controller is registered for
// teardown so a test that never calls Run or Stop cannot leak that goroutine into
// the next test's goroutine baseline.
func mustNewController(t *testing.T, cfg Config) *Controller {
	t.Helper()
	ctrl := NewController(cfg)
	t.Cleanup(ctrl.Stop)
	return ctrl
}

// listKindsFor returns the dynamic-fake list-kind mapping for the given
// contract's ComputeDomain and ComputeDomainClique GVRs.
func listKindsFor(contract cdapi.GroupContract) map[schema.GroupVersionResource]string {
	return map[schema.GroupVersionResource]string{
		contract.ComputeDomainGVR:       "ComputeDomainList",
		contract.ComputeDomainCliqueGVR: "ComputeDomainCliqueList",
	}
}
