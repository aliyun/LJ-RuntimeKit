package computedomain

import "testing"

// ---------------------------------------------------------------------------
// NormalizeNamespaces unit tests (Config-level, defence-in-depth)
// ---------------------------------------------------------------------------

func TestNormalizeNamespaces_BothEmpty(t *testing.T) {
	cfg := &Config{}
	NormalizeNamespaces(cfg)
	if cfg.Namespace != DefaultDriverNamespace {
		t.Errorf("empty Namespace: got %q, want %q", cfg.Namespace, DefaultDriverNamespace)
	}
	if cfg.ComputeDomainCliqueNamespace != DefaultCliqueNamespace {
		t.Errorf("empty ComputeDomainCliqueNamespace: got %q, want %q", cfg.ComputeDomainCliqueNamespace, DefaultCliqueNamespace)
	}
}

func TestNormalizeNamespaces_ExplicitValuesPreserved(t *testing.T) {
	cfg := &Config{Namespace: "custom-ns", ComputeDomainCliqueNamespace: "custom-clique"}
	NormalizeNamespaces(cfg)
	if cfg.Namespace != "custom-ns" {
		t.Errorf("Namespace overridden: got %q, want %q", cfg.Namespace, "custom-ns")
	}
	if cfg.ComputeDomainCliqueNamespace != "custom-clique" {
		t.Errorf("ComputeDomainCliqueNamespace overridden: got %q, want %q", cfg.ComputeDomainCliqueNamespace, "custom-clique")
	}
}

func TestNormalizeNamespaces_OnlyNamespaceEmpty(t *testing.T) {
	cfg := &Config{ComputeDomainCliqueNamespace: "my-ns"}
	NormalizeNamespaces(cfg)
	if cfg.Namespace != DefaultDriverNamespace {
		t.Errorf("empty Namespace: got %q, want %q", cfg.Namespace, DefaultDriverNamespace)
	}
	if cfg.ComputeDomainCliqueNamespace != "my-ns" {
		t.Errorf("ComputeDomainCliqueNamespace changed: got %q", cfg.ComputeDomainCliqueNamespace)
	}
}

func TestNormalizeNamespaces_OnlyCliqueEmpty(t *testing.T) {
	cfg := &Config{Namespace: "my-ns"}
	NormalizeNamespaces(cfg)
	if cfg.Namespace != "my-ns" {
		t.Errorf("Namespace changed: got %q", cfg.Namespace)
	}
	if cfg.ComputeDomainCliqueNamespace != DefaultCliqueNamespace {
		t.Errorf("empty ComputeDomainCliqueNamespace: got %q, want %q", cfg.ComputeDomainCliqueNamespace, DefaultCliqueNamespace)
	}
}

// ---------------------------------------------------------------------------
// NewController namespace propagation: ManagerConfig must never carry empty ns
// ---------------------------------------------------------------------------

func TestNewController_EmptyNamespace_DefaultsToKubeSystem(t *testing.T) {
	cfg := Config{Namespace: "", ComputeDomainCliqueNamespace: ""}
	ctrl := mustNewController(t, cfg)
	if ctrl.config.Namespace != DefaultDriverNamespace {
		t.Errorf("Config.Namespace: got %q, want %q", ctrl.config.Namespace, DefaultDriverNamespace)
	}
	mc := ctrl.managers[0].config
	if mc.ComputeDomainCliqueNamespace != DefaultCliqueNamespace {
		t.Errorf("ManagerConfig.ComputeDomainCliqueNamespace: got %q, want %q", mc.ComputeDomainCliqueNamespace, DefaultCliqueNamespace)
	}
}

func TestNewController_ExplicitNamespacesPreserved(t *testing.T) {
	cfg := Config{Namespace: "prod", ComputeDomainCliqueNamespace: "clique-ns"}
	ctrl := mustNewController(t, cfg)
	if ctrl.config.Namespace != "prod" {
		t.Errorf("Config.Namespace: got %q, want %q", ctrl.config.Namespace, "prod")
	}
	mc := ctrl.managers[0].config
	if mc.ComputeDomainCliqueNamespace != "clique-ns" {
		t.Errorf("ManagerConfig.ComputeDomainCliqueNamespace: got %q, want %q", mc.ComputeDomainCliqueNamespace, "clique-ns")
	}
}

func TestNewController_ManagerConfigNeverEmptyNamespace(t *testing.T) {
	// Simulate the worst case: both empty (like explicit --namespace= --compute-domain-clique-namespace=)
	cfg := Config{}
	ctrl := mustNewController(t, cfg)
	if ctrl.config.Namespace == "" {
		t.Fatal("Config.Namespace must never be empty (leader election Lease namespace)")
	}
	if ctrl.managers[0].config.ComputeDomainCliqueNamespace == "" {
		t.Fatal("ManagerConfig.ComputeDomainCliqueNamespace must never be empty")
	}
}
