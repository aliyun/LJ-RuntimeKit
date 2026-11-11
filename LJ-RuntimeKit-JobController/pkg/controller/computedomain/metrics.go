package computedomain

import (
	"sync"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

var (
	computeDomainMetricsOnce sync.Once
	computeDomains           *metrics.GaugeVec
	computeDomainStatusMu    sync.Mutex
	computeDomainLastStatus  map[string]string
)

// RegisterComputeDomainMetrics registers the status-partitioned cluster gauge.
func RegisterComputeDomainMetrics() {
	computeDomainMetricsOnce.Do(func() {
		computeDomains = metrics.NewGaugeVec(&metrics.GaugeOpts{
			Namespace: "ppu_dra",
			Name:      "compute_domain_info",
			Help:      "Current number of ComputeDomain resources partitioned by global status.",
		}, []string{"status"})
		legacyregistry.MustRegister(computeDomains)
	})
}

// ObserveComputeDomainStatus moves one existing ComputeDomain between status
// gauges. exists must consult the ComputeDomain informer-backed store.
func ObserveComputeDomainStatus(uid, status string, exists func(string) bool) {
	if uid == "" || exists == nil {
		return
	}
	RegisterComputeDomainMetrics()
	computeDomainStatusMu.Lock()
	defer computeDomainStatusMu.Unlock()
	// Check existence while holding the same lock used by Forget. If deletion
	// happens after this check, Forget runs after this update and removes it;
	// if the informer store was already updated, the late Observe is rejected.
	if !exists(uid) {
		return
	}
	if computeDomainLastStatus == nil {
		computeDomainLastStatus = make(map[string]string)
	}
	previous, observed := computeDomainLastStatus[uid]
	if observed && previous == status {
		return
	}
	if observed {
		computeDomains.WithLabelValues(previous).Add(-1)
	}
	computeDomains.WithLabelValues(status).Add(1)
	computeDomainLastStatus[uid] = status
}

// ForgetComputeDomain removes a deleted ComputeDomain from the gauge.
func ForgetComputeDomain(uid string) {
	if uid == "" {
		return
	}
	RegisterComputeDomainMetrics()
	computeDomainStatusMu.Lock()
	defer computeDomainStatusMu.Unlock()
	previous, exists := computeDomainLastStatus[uid]
	if !exists {
		return
	}
	computeDomains.WithLabelValues(previous).Add(-1)
	delete(computeDomainLastStatus, uid)
}
