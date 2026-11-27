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

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

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

func TestControllerNamespaceCLI(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name string
		env  string
		args []string
		want string
	}{
		{name: "default", want: `namespace="kube-system"`},
		{name: "environment", env: "controller-system", want: `namespace="controller-system"`},
		{name: "explicit", env: "controller-system", args: []string{"--namespace=custom"}, want: `namespace="custom"`},
		{name: "explicit empty", env: "controller-system", args: []string{"--namespace="}, want: `namespace="kube-system"`},
		{name: "removed collector flag", args: []string{"--collector-namespace=other"}, want: "unknown flag: --collector-namespace"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			// 三个 gate 默认关闭，需显式启用一个功能才能走到 kubeconfig 校验。
			args := []string{"-test.run=^TestControllerCLIProcess$", "--", "--kubeconfig=" + filepath.Join(t.TempDir(), "missing-kubeconfig"), "--feature-gates=ActiveMonitoring=true"}
			cmd := exec.Command(executable, append(args, tt.args...)...)
			cmd.Env = append(os.Environ(), "RUNTIMEKIT_TEST_MAIN=1", "NAMESPACE="+tt.env, "POD_NAMESPACE=ignored")
			output, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatal("expected CLI to exit before connecting to Kubernetes")
			}
			if !strings.Contains(string(output), tt.want) {
				t.Fatalf("output does not contain %q: %s", tt.want, output)
			}
			if tt.name != "removed collector flag" && !strings.Contains(string(output), "Failed to build kubernetes config") {
				t.Fatalf("namespace configuration was not accepted: %s", output)
			}
		})
	}
}

func TestControllerFeatureGateCLI(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name string
		args []string
	}{
		{name: "default"},
		{name: "all disabled", args: []string{"--feature-gates=ComputeDomain=false,Profiling=false,ActiveMonitoring=false"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			// 三个 gate 默认关闭且未显式启用时，进程必须在构造 Kubernetes 客户端前退出。
			args := append([]string{"-test.run=^TestControllerCLIProcess$", "--", "--kubeconfig=" + filepath.Join(t.TempDir(), "missing-kubeconfig")}, tt.args...)
			cmd := exec.Command(executable, args...)
			cmd.Env = append(os.Environ(), "RUNTIMEKIT_TEST_MAIN=1")
			output, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatal("expected CLI to exit when no controller is enabled")
			}
			if !strings.Contains(string(output), "at least one controller must be enabled via --feature-gates") {
				t.Fatalf("output does not contain feature gate requirement: %s", output)
			}
			if strings.Contains(string(output), "Failed to build kubernetes config") {
				t.Fatalf("controller must fail before building kubernetes config: %s", output)
			}
		})
	}
}

func TestControllerCLIProcess(t *testing.T) {
	if os.Getenv("RUNTIMEKIT_TEST_MAIN") != "1" {
		return
	}
	os.Args = append(os.Args[:1], os.Args[3:]...)
	main()
}
