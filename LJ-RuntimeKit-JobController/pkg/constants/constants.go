package constants

import (
	"time"

	// Canonical ComputeDomain constants live in pkg/computedomain.
	// They are re-exported here unchanged for backward compatibility with
	// existing in-tree code and external consumers (e.g. the scheduler plugin).
	cd "github.com/lj-runtimekit/job-controller/pkg/computedomain"
)

// ==========================================================================
// ComputeDomain constants (re-exported from pkg/computedomain).
// New code should import pkg/computedomain directly.
// ==========================================================================

const (
	DriverName                       = cd.DriverName
	APIGroup                         = cd.APIGroup
	APIVersion                       = cd.APIVersion
	ComputeDomainResource            = cd.ComputeDomainResource
	ComputeDomainCliqueResource      = cd.ComputeDomainCliqueResource
	LabelComputeDomain               = cd.LabelComputeDomain
	LabelComputeDomainCliqueDomain   = cd.LabelComputeDomainCliqueDomain
	LabelComputeDomainClique         = cd.LabelComputeDomainClique
	LabelComputeDomainTarget         = cd.LabelComputeDomainTarget
	Finalizer                        = cd.Finalizer
	ComputeDomainFinalizer           = cd.ComputeDomainFinalizer
	ChannelDeviceClassName           = cd.ChannelDeviceClassName
	ChannelNonDefaultDeviceClassName = cd.ChannelNonDefaultDeviceClassName
	RCTTargetWorkload                = cd.RCTTargetWorkload
	StatusReady                      = cd.StatusReady
	StatusNotReady                   = cd.StatusNotReady
	NodeStatusReady                  = cd.NodeStatusReady
	NodeStatusNotReady               = cd.NodeStatusNotReady
	DefaultMaxNodesPerIMEXDomain     = cd.DefaultMaxNodesPerIMEXDomain
	DefaultIMEXIsolation             = cd.DefaultIMEXIsolation
	DefaultDevicePluginPodSelector   = cd.DefaultDevicePluginPodSelector
)

// ==========================================================================
// General controller timing constants (cross-controller, kept here).
// ==========================================================================

const (
	// InformerResyncPeriod defines how often the informer will resync its cache
	// with the API server. Ensures eventual consistency. Community default: 10m.
	InformerResyncPeriod = 10 * time.Minute

	// MutationCacheTTL defines how long mutation cache entries remain valid.
	// Should be long enough for the informer cache to catch up but not so long
	// that stale entries cause issues. Community default: 1h.
	MutationCacheTTL = time.Hour

	// StatusSyncInterval is the interval between periodic status synchronization
	// cycles for ComputeDomain CRs. Community default: 2s.
	StatusSyncInterval = 2 * time.Second

	// CleanupInterval is the interval between periodic orphan resource cleanup
	// cycles (workload RCTs and Node labels). Community default: 10m.
	CleanupInterval = 10 * time.Minute

	// CacheSyncTimeout bounds the initial informer cache sync. Both ComputeDomain
	// API groups are hard startup dependencies, so a CRD that is missing (or a
	// list/watch permission that is denied) must fail startup instead of leaving
	// the reflector retrying forever. Community default: 1m.
	CacheSyncTimeout = time.Minute
)
