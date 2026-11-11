// Package computedomain defines canonical ComputeDomain-specific constants
// shared by the controller and scheduler plugin. This is a leaf package with
// zero in-module dependencies, making it safe for re-export from
// pkg/constants without import cycles.
package computedomain

const (
	// ==========================================================================
	// Driver Identity
	// ==========================================================================

	// DriverName is the DRA driver name used in opaque device configurations.
	// PPU customization: uses aliyun.com domain instead of community nvidia.com.
	DriverName = "compute-domain.aliyun.com"

	// ==========================================================================
	// API Groups & Version
	// ==========================================================================

	// GroupAliyun is the Aliyun ComputeDomain API group. The controller always
	// serves it together with GroupNVIDIA, through one isolated manager set each.
	GroupAliyun = "resource.aliyun.com"

	// GroupNVIDIA is the community NVIDIA ComputeDomain API group.
	GroupNVIDIA = "resource.nvidia.com"

	// APIGroup is the legacy single-group default.
	//
	// Deprecated: the controller is parameterised per API group through
	// GroupContract (see contract.go); runtime code must never switch this
	// global. It is retained only for backward compatibility of the constants
	// re-exported through pkg/constants.
	APIGroup = GroupNVIDIA

	// APIVersion is the API version for ComputeDomain CRDs. Both supported
	// groups serve the same version.
	APIVersion = "v1beta1"

	// ==========================================================================
	// Resource Names (plural forms for GVR construction)
	// ==========================================================================

	// ComputeDomainResource is the plural resource name for ComputeDomain.
	ComputeDomainResource = "computedomains"

	// ComputeDomainCliqueResource is the plural resource name for ComputeDomainClique.
	ComputeDomainCliqueResource = "computedomaincliques"

	// ==========================================================================
	// Labels
	// ==========================================================================

	// LabelComputeDomain is the NVIDIA-scoped label key used to associate
	// resources with a ComputeDomain UID. Matches community
	// computeDomainLabelKey.
	//
	// Deprecated: use GroupContract.LabelComputeDomain, which is the same value
	// for the NVIDIA contract and resource.aliyun.com/computeDomain for the
	// Aliyun contract.
	LabelComputeDomain = GroupNVIDIA + "/computeDomain"

	// LabelComputeDomainCliqueDomain associates a ComputeDomainClique with its
	// ComputeDomain metadata.uid. This is the on-site CDClique contract.
	LabelComputeDomainCliqueDomain = "aliyun.com/clique-domain"

	// LabelComputeDomainClique identifies a ComputeDomainClique's clique ID.
	LabelComputeDomainClique = "aliyun.com/clique"

	// LabelComputeDomainTarget is the NVIDIA-scoped label key on
	// ResourceClaimTemplates indicating the target type (Daemon or Workload).
	// Matches community computeDomainResourceClaimTemplateTargetLabelKey.
	//
	// Deprecated: use GroupContract.LabelTarget.
	LabelComputeDomainTarget = GroupNVIDIA + "/computeDomainTarget"

	// ==========================================================================
	// Finalizers
	// ==========================================================================

	// Finalizer is the NVIDIA-scoped finalizer added to ComputeDomain and
	// workload ResourceClaimTemplate resources to ensure proper cleanup before
	// deletion.
	//
	// Deprecated: use GroupContract.Finalizer.
	Finalizer = LabelComputeDomain

	// ComputeDomainFinalizer is an alias for Finalizer, matching community naming.
	//
	// Deprecated: use GroupContract.Finalizer.
	ComputeDomainFinalizer = Finalizer

	// ==========================================================================
	// DeviceClass Names
	// ==========================================================================

	// ChannelDeviceClassName is the DeviceClass name used for default-channel
	// workload ResourceClaimTemplates.
	// PPU customization: uses aliyun.com domain instead of community nvidia.com.
	// Community value: "compute-domain-default-channel.nvidia.com"
	ChannelDeviceClassName = "compute-domain-default-channel.aliyun.com"

	// ChannelNonDefaultDeviceClassName is the DeviceClass name for non-default
	// channel workload ResourceClaimTemplates.
	// PPU customization: uses aliyun.com domain instead of community nvidia.com.
	// Community value: "compute-domain-channel.nvidia.com"
	ChannelNonDefaultDeviceClassName = "compute-domain-channel.aliyun.com"

	// ==========================================================================
	// RCT Target Types
	// ==========================================================================

	// RCTTargetWorkload is the label value for workload ResourceClaimTemplates.
	// Matches community computeDomainResourceClaimTemplateTargetWorkload.
	RCTTargetWorkload = "Workload"

	// ==========================================================================
	// Status Constants
	// ==========================================================================

	// StatusReady indicates a ComputeDomain is ready.
	StatusReady = "Ready"

	// StatusNotReady indicates a ComputeDomain is not ready.
	StatusNotReady = "NotReady"

	// NodeStatusReady indicates a node within a ComputeDomain is ready.
	NodeStatusReady = "Ready"

	// NodeStatusNotReady indicates a node within a ComputeDomain is not ready.
	NodeStatusNotReady = "NotReady"

	// ==========================================================================
	// Default Configuration Values
	// ==========================================================================

	// DefaultMaxNodesPerIMEXDomain is the default maximum number of nodes per
	// IMEX domain. Community default is 18.
	DefaultMaxNodesPerIMEXDomain = 18

	// DefaultIMEXIsolation is the default IMEX isolation mode.
	// Community default is "domain".
	DefaultIMEXIsolation = "domain"

	// DefaultDevicePluginPodSelector identifies the permanent Device Plugin pods.
	// Deployments with different stable labels must override the controller flag.
	DefaultDevicePluginPodSelector = "app.kubernetes.io/name=lj-runtimekit-device-plugin"
)
