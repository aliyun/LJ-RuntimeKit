package main

import "testing"

// ---------------------------------------------------------------------------
// normalizeNamespaces — CLI-level namespace safety net
// ---------------------------------------------------------------------------

func TestNormalizeNamespaces_BothEmpty(t *testing.T) {
	ns, cliqueNS := normalizeNamespaces("", "")
	if ns != "kube-system" {
		t.Errorf("empty ns: got %q, want %q", ns, "kube-system")
	}
	if cliqueNS != "default" {
		t.Errorf("empty cliqueNS: got %q, want %q", cliqueNS, "default")
	}
}

func TestNormalizeNamespaces_ExplicitEmptyString(t *testing.T) {
	// Simulates --namespace="" --compute-domain-clique-namespace=""
	ns, cliqueNS := normalizeNamespaces("", "")
	if ns == "" {
		t.Fatal("normalizeNamespaces must not return empty ns")
	}
	if cliqueNS == "" {
		t.Fatal("normalizeNamespaces must not return empty cliqueNS")
	}
}

func TestNormalizeNamespaces_ExplicitValuesPreserved(t *testing.T) {
	ns, cliqueNS := normalizeNamespaces("prod", "clique-prod")
	if ns != "prod" {
		t.Errorf("ns: got %q, want %q", ns, "prod")
	}
	if cliqueNS != "clique-prod" {
		t.Errorf("cliqueNS: got %q, want %q", cliqueNS, "clique-prod")
	}
}

func TestNormalizeNamespaces_OnlyNamespaceEmpty(t *testing.T) {
	ns, cliqueNS := normalizeNamespaces("", "my-clique")
	if ns != "kube-system" {
		t.Errorf("ns: got %q, want %q", ns, "kube-system")
	}
	if cliqueNS != "my-clique" {
		t.Errorf("cliqueNS: got %q, want %q", cliqueNS, "my-clique")
	}
}

func TestNormalizeNamespaces_OnlyCliqueEmpty(t *testing.T) {
	ns, cliqueNS := normalizeNamespaces("custom-ns", "")
	if ns != "custom-ns" {
		t.Errorf("ns: got %q, want %q", ns, "custom-ns")
	}
	if cliqueNS != "default" {
		t.Errorf("cliqueNS: got %q, want %q", cliqueNS, "default")
	}
}

func TestNormalizeNamespaces_DefaultValues(t *testing.T) {
	// Defaults match the documented Deployment layout:
	// Controller/SA/Lease/DP Pod → kube-system; CDClique → default.
	ns, cliqueNS := normalizeNamespaces("kube-system", "default")
	if ns != "kube-system" {
		t.Errorf("ns: got %q, want %q", ns, "kube-system")
	}
	if cliqueNS != "default" {
		t.Errorf("cliqueNS: got %q, want %q", cliqueNS, "default")
	}
}
