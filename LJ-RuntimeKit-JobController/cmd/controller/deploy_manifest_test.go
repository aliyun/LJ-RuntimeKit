package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"

	cdapi "github.com/lj-runtimekit/job-controller/pkg/computedomain"
)

// deployDir is the shipped manifest directory, relative to cmd/controller.
const deployDir = "../../deploy"

// readDeployFile loads one manifest from the deploy directory.
func readDeployFile(t *testing.T, name string) []byte {
	t.Helper()
	path := filepath.Join(deployDir, name)
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read manifest %s: %v", path, err)
	}
	return content
}

// decodeDeployFile decodes every YAML document in a manifest file.
func decodeDeployFile(t *testing.T, name string) []*unstructured.Unstructured {
	t.Helper()
	content := readDeployFile(t, name)
	decoder := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(content), 4096)
	var docs []*unstructured.Unstructured
	for {
		doc := &unstructured.Unstructured{}
		err := decoder.Decode(doc)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("decode %s: %v", name, err)
		}
		if len(doc.Object) == 0 {
			continue // empty document between '---' separators
		}
		docs = append(docs, doc)
	}
	if len(docs) == 0 {
		t.Fatalf("%s contains no documents", name)
	}
	return docs
}

// findDeployDoc returns the single document matching kind and metadata.name.
func findDeployDoc(t *testing.T, docs []*unstructured.Unstructured, kind, name string) *unstructured.Unstructured {
	t.Helper()
	var matches []*unstructured.Unstructured
	for _, doc := range docs {
		if doc.GetKind() != kind {
			continue
		}
		if name != "" && doc.GetName() != name {
			continue
		}
		matches = append(matches, doc)
	}
	if len(matches) != 1 {
		t.Fatalf("expected exactly one %s named %q, found %d", kind, name, len(matches))
	}
	return matches[0]
}

// ---------------------------------------------------------------------------
// CRDs: both API groups, identical schemas
// ---------------------------------------------------------------------------

// crdTwin describes the two CRD files that must serve the same schema from
// different API groups.
type crdTwin struct {
	kind     string
	listKind string
	plural   string
	files    map[string]string // APIGroup -> deploy file name
}

func crdTwins() []crdTwin {
	return []crdTwin{
		{
			kind:     "ComputeDomain",
			listKind: "ComputeDomainList",
			plural:   "computedomains",
			files: map[string]string{
				cdapi.GroupNVIDIA: "computedomains-crd.yaml",
				cdapi.GroupAliyun: "computedomains-aliyun-crd.yaml",
			},
		},
		{
			kind:     "ComputeDomainClique",
			listKind: "ComputeDomainCliqueList",
			plural:   "computedomaincliques",
			files: map[string]string{
				cdapi.GroupNVIDIA: "computedomaincliques-crd.yaml",
				cdapi.GroupAliyun: "computedomaincliques-aliyun-crd.yaml",
			},
		},
	}
}

// TestDeployCRDsServeBothAPIGroups pins that every supported API group ships
// its own CRD file, named after the group it serves.
func TestDeployCRDsServeBothAPIGroups(t *testing.T) {
	for _, twin := range crdTwins() {
		for _, group := range cdapi.SupportedAPIGroups() {
			group, file := group, twin.files[group]
			if file == "" {
				t.Fatalf("no CRD file declared for %s/%s", twin.kind, group)
			}
			t.Run(group+"/"+twin.kind, func(t *testing.T) {
				docs := decodeDeployFile(t, file)
				if len(docs) != 1 {
					t.Fatalf("%s should hold exactly one document, got %d", file, len(docs))
				}
				crd := docs[0]
				if got, want := crd.GetAPIVersion(), "apiextensions.k8s.io/v1"; got != want {
					t.Errorf("apiVersion = %q, want %q", got, want)
				}
				if got := crd.GetKind(); got != "CustomResourceDefinition" {
					t.Errorf("kind = %q, want CustomResourceDefinition", got)
				}
				if got, want := crd.GetName(), twin.plural+"."+group; got != want {
					t.Errorf("metadata.name = %q, want %q", got, want)
				}
				if got, _, _ := unstructured.NestedString(crd.Object, "spec", "group"); got != group {
					t.Errorf("spec.group = %q, want %q", got, group)
				}
				if got, _, _ := unstructured.NestedString(crd.Object, "spec", "scope"); got != "Namespaced" {
					t.Errorf("spec.scope = %q, want Namespaced", got)
				}
				if got, _, _ := unstructured.NestedString(crd.Object, "spec", "names", "kind"); got != twin.kind {
					t.Errorf("spec.names.kind = %q, want %q", got, twin.kind)
				}
				if got, _, _ := unstructured.NestedString(crd.Object, "spec", "names", "listKind"); got != twin.listKind {
					t.Errorf("spec.names.listKind = %q, want %q", got, twin.listKind)
				}
				if got, _, _ := unstructured.NestedString(crd.Object, "spec", "names", "plural"); got != twin.plural {
					t.Errorf("spec.names.plural = %q, want %q", got, twin.plural)
				}
				version := servedVersion(t, crd)
				if got, _ := version["name"].(string); got != cdapi.APIVersion {
					t.Errorf("spec.versions[0].name = %q, want %q", got, cdapi.APIVersion)
				}
			})
		}
	}
}

// servedVersion returns the single served+storage version of a CRD.
func servedVersion(t *testing.T, crd *unstructured.Unstructured) map[string]interface{} {
	t.Helper()
	versions, found, err := unstructured.NestedSlice(crd.Object, "spec", "versions")
	if err != nil || !found {
		t.Fatalf("CRD %s has no spec.versions: %v", crd.GetName(), err)
	}
	if len(versions) != 1 {
		t.Fatalf("CRD %s serves %d versions, want 1", crd.GetName(), len(versions))
	}
	version, ok := versions[0].(map[string]interface{})
	if !ok {
		t.Fatalf("CRD %s version entry is not an object: %T", crd.GetName(), versions[0])
	}
	if served, _ := version["served"].(bool); !served {
		t.Errorf("CRD %s version %v is not served", crd.GetName(), version["name"])
	}
	if storage, _ := version["storage"].(bool); !storage {
		t.Errorf("CRD %s version %v is not the storage version", crd.GetName(), version["name"])
	}
	return version
}

// TestDeployCRDTwinsShareIdenticalSchema pins that the Aliyun and NVIDIA CRDs
// expose exactly the same version block, so an object written through one group
// is representable through the other.
func TestDeployCRDTwinsShareIdenticalSchema(t *testing.T) {
	for _, twin := range crdTwins() {
		t.Run(twin.kind, func(t *testing.T) {
			versions := map[string][]interface{}{}
			for group, file := range twin.files {
				crd := decodeDeployFile(t, file)[0]
				list, _, err := unstructured.NestedSlice(crd.Object, "spec", "versions")
				if err != nil {
					t.Fatalf("%s: read spec.versions: %v", file, err)
				}
				versions[group] = list
			}
			if !reflect.DeepEqual(versions[cdapi.GroupAliyun], versions[cdapi.GroupNVIDIA]) {
				t.Fatalf("%s schema drifted between API groups:\naliyun=%#v\nnvidia=%#v",
					twin.kind, versions[cdapi.GroupAliyun], versions[cdapi.GroupNVIDIA])
			}
		})
	}
}

// TestDeployCliqueCRDKeepsTopLevelDaemons pins that daemons stays a top-level
// field of ComputeDomainClique (not nested under spec) in both API groups.
func TestDeployCliqueCRDKeepsTopLevelDaemons(t *testing.T) {
	for _, twin := range crdTwins() {
		if twin.kind != "ComputeDomainClique" {
			continue
		}
		for group, file := range twin.files {
			group, file := group, file
			t.Run(group, func(t *testing.T) {
				crd := decodeDeployFile(t, file)[0]
				version := servedVersion(t, crd)
				path := []string{"schema", "openAPIV3Schema", "properties"}
				properties, found, err := unstructured.NestedMap(version, path...)
				if err != nil || !found {
					t.Fatalf("%s: resolve schema properties: %v", file, err)
				}
				if _, ok := properties["daemons"]; !ok {
					t.Fatalf("%s: daemons must be a top-level property, got %v", file, sortedKeys(properties))
				}
				// daemons is NOT part of spec: the only top-level properties are the
				// api machinery envelope plus daemons itself.
				for _, envelope := range []string{"apiVersion", "kind", "metadata"} {
					if _, ok := properties[envelope]; !ok {
						t.Errorf("%s: missing top-level %q property", file, envelope)
					}
				}
				if extra := sortedKeys(properties); !reflect.DeepEqual(extra,
					[]string{"apiVersion", "daemons", "kind", "metadata"}) {
					t.Errorf("%s: top-level properties = %v, want only apiVersion/daemons/kind/metadata", file, extra)
				}
			})
		}
	}
}

func sortedKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// ---------------------------------------------------------------------------
// RBAC: both groups, CDClique read-mostly
// ---------------------------------------------------------------------------

func apiGroupsOfRule(t *testing.T, rule map[string]interface{}, file string) []string {
	t.Helper()
	raw, ok := rule["apiGroups"].([]interface{})
	if !ok {
		t.Fatalf("%s: rule has no apiGroups: %v", file, rule)
	}
	groups := make([]string, 0, len(raw))
	for _, item := range raw {
		group, _ := item.(string)
		groups = append(groups, group)
	}
	sort.Strings(groups)
	return groups
}

func resourcesOfRule(t *testing.T, rule map[string]interface{}, file string) []string {
	t.Helper()
	raw, ok := rule["resources"].([]interface{})
	if !ok {
		t.Fatalf("%s: rule has no resources: %v", file, rule)
	}
	resources := make([]string, 0, len(raw))
	for _, item := range raw {
		resource, _ := item.(string)
		resources = append(resources, resource)
	}
	sort.Strings(resources)
	return resources
}

func verbsOfRule(t *testing.T, rule map[string]interface{}, file string) []string {
	t.Helper()
	raw, ok := rule["verbs"].([]interface{})
	if !ok {
		t.Fatalf("%s: rule has no verbs: %v", file, rule)
	}
	verbs := make([]string, 0, len(raw))
	for _, item := range raw {
		verb, _ := item.(string)
		verbs = append(verbs, verb)
	}
	sort.Strings(verbs)
	return verbs
}

func rulesOf(t *testing.T, doc *unstructured.Unstructured) []map[string]interface{} {
	t.Helper()
	raw, found, err := unstructured.NestedSlice(doc.Object, "rules")
	if err != nil || !found {
		t.Fatalf("%s %s has no rules: %v", doc.GetKind(), doc.GetName(), err)
	}
	rules := make([]map[string]interface{}, 0, len(raw))
	for _, item := range raw {
		rule, ok := item.(map[string]interface{})
		if !ok {
			t.Fatalf("%s %s rule is not an object: %T", doc.GetKind(), doc.GetName(), item)
		}
		rules = append(rules, rule)
	}
	return rules
}

// TestDeployRBACGrantsBothAPIGroups pins that the shipped RBAC covers both
// ComputeDomain API groups and that ComputeDomainClique stays read-mostly.
func TestDeployRBACGrantsBothAPIGroups(t *testing.T) {
	const file = "rbac.yaml"
	docs := decodeDeployFile(t, file)
	wantGroups := append([]string(nil), cdapi.SupportedAPIGroups()...)
	sort.Strings(wantGroups)

	clusterRole := findDeployDoc(t, docs, "ClusterRole", "job-controller-clusterrole")
	clusterDomainRules, clusterStatusRules := 0, 0
	for _, rule := range rulesOf(t, clusterRole) {
		resources := resourcesOfRule(t, rule, file)
		groups := apiGroupsOfRule(t, rule, file)
		for _, resource := range resources {
			switch resource {
			case "computedomains":
				clusterDomainRules++
				if !reflect.DeepEqual(groups, wantGroups) {
					t.Errorf("ClusterRole computedomains apiGroups = %v, want %v", groups, wantGroups)
				}
			case "computedomains/status":
				clusterStatusRules++
				if !reflect.DeepEqual(groups, wantGroups) {
					t.Errorf("ClusterRole computedomains/status apiGroups = %v, want %v", groups, wantGroups)
				}
			}
		}
	}
	if clusterDomainRules != 1 || clusterStatusRules != 1 {
		t.Fatalf("ClusterRole declared %d computedomains and %d status rules, want 1 each",
			clusterDomainRules, clusterStatusRules)
	}

	cliqueRole := findDeployDoc(t, docs, "Role", "job-controller-cdclique-role")
	cliqueRules := rulesOf(t, cliqueRole)
	if len(cliqueRules) != 1 {
		t.Fatalf("CDClique Role has %d rules, want 1", len(cliqueRules))
	}
	rule := cliqueRules[0]
	if got := apiGroupsOfRule(t, rule, file); !reflect.DeepEqual(got, wantGroups) {
		t.Errorf("CDClique Role apiGroups = %v, want %v", got, wantGroups)
	}
	if got, want := resourcesOfRule(t, rule, file), []string{"computedomaincliques"}; !reflect.DeepEqual(got, want) {
		t.Errorf("CDClique Role resources = %v, want %v", got, want)
	}
	if got, want := verbsOfRule(t, rule, file), []string{"delete", "get", "list", "watch"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("CDClique Role verbs = %v, want %v (the controller must never write daemons)", got, want)
	}
	for _, verb := range verbsOfRule(t, rule, file) {
		switch verb {
		case "create", "update", "patch":
			t.Errorf("CDClique Role must not grant %q", verb)
		}
	}
}

// ---------------------------------------------------------------------------
// Deployment: both groups are unconditional, image tag unchanged
// ---------------------------------------------------------------------------

// containerArgs returns the args of the single controller container.
func controllerContainer(t *testing.T, doc *unstructured.Unstructured) map[string]interface{} {
	t.Helper()
	containers, found, err := unstructured.NestedSlice(doc.Object, "spec", "template", "spec", "containers")
	if err != nil || !found || len(containers) == 0 {
		t.Fatalf("Deployment has no containers: %v", err)
	}
	container, ok := containers[0].(map[string]interface{})
	if !ok {
		t.Fatalf("container entry is not an object: %T", containers[0])
	}
	return container
}

// TestDeployControllerPassesNoAPIGroupFlag pins that the Deployment does not
// configure ComputeDomain API groups at all: both groups are always served, so
// no flag may reappear (and the image tag must stay unchanged).
func TestDeployControllerPassesNoAPIGroupFlag(t *testing.T) {
	const file = "controller-deployment.yaml"
	docs := decodeDeployFile(t, file)
	deployment := findDeployDoc(t, docs, "Deployment", "lj-runtimekit-job-controller")
	container := controllerContainer(t, deployment)

	args, found, err := unstructured.NestedStringSlice(container, "args")
	if err != nil || !found {
		t.Fatalf("container has no args: %v", err)
	}

	for _, arg := range args {
		if strings.Contains(arg, "compute-domain-api-groups") {
			t.Fatalf("Deployment must not configure ComputeDomain API groups, found %q", arg)
		}
	}

	expected := []string{
		"--namespace=kube-system",
		"--compute-domain-clique-namespace=default",
		"--leader-election=true",
		"--http-endpoint=:6060",
	}
	for _, want := range expected {
		if !containsString(args, want) {
			t.Errorf("args missing %q: %v", want, args)
		}
	}
	if len(args) != len(expected) {
		t.Errorf("Deployment args drifted: %v, want %v", args, expected)
	}

	image, _ := container["image"].(string)
	if !strings.HasSuffix(image, ":1.13.1604") {
		t.Errorf("image = %q, must keep the 1.13.1604 tag", image)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Examples: one per API group, distinct ResourceClaimTemplate names
// ---------------------------------------------------------------------------

// TestDeployExamplesCoverBothAPIGroups pins that the two samples address
// different API groups and never reuse a ResourceClaimTemplate name, which is
// the cross-group conflict the controller refuses at runtime.
func TestDeployExamplesCoverBothAPIGroups(t *testing.T) {
	examples := map[string]string{
		cdapi.GroupNVIDIA: "example-computedomain.yaml",
		cdapi.GroupAliyun: "example-computedomain-aliyun.yaml",
	}
	templates := map[string]string{}
	for _, group := range cdapi.SupportedAPIGroups() {
		file := examples[group]
		if file == "" {
			t.Fatalf("no example declared for %s", group)
		}
		t.Run(group, func(t *testing.T) {
			docs := decodeDeployFile(t, file)
			if len(docs) != 1 {
				t.Fatalf("%s should hold exactly one document, got %d", file, len(docs))
			}
			cd := docs[0]
			if got, want := cd.GetAPIVersion(), group+"/"+cdapi.APIVersion; got != want {
				t.Errorf("apiVersion = %q, want %q", got, want)
			}
			if got := cd.GetKind(); got != "ComputeDomain" {
				t.Errorf("kind = %q, want ComputeDomain", got)
			}
			name, found, err := unstructured.NestedString(
				cd.Object, "spec", "channel", "resourceClaimTemplate", "name")
			if err != nil || !found || name == "" {
				t.Fatalf("%s: spec.channel.resourceClaimTemplate.name missing: %v", file, err)
			}
			templates[group] = name
		})
	}
	if len(templates) != 2 {
		t.Fatalf("expected two example templates, got %v", templates)
	}
	if templates[cdapi.GroupAliyun] == templates[cdapi.GroupNVIDIA] {
		t.Fatalf("both examples point at ResourceClaimTemplate %q; each API group must use its own name",
			templates[cdapi.GroupAliyun])
	}
}

// exampleOwnership pins the documented ownership of the resources an example
// mentions. The examples used to claim that the user pre-creates the
// ResourceClaimTemplate and that JobController creates the Pods; both are wrong
// and mislead anyone following the sample. The controller creates the
// ResourceClaimTemplate from spec.channel.resourceClaimTemplate.name (refusing to
// adopt a foreign or unowned object of the same name), while the workload Pod is
// created by the user or an upper-layer controller. Only comments are pinned
// here: the manifest bodies are asserted by TestDeployExamplesCoverBothAPIGroups.
type exampleOwnership struct {
	file           string
	mustMention    []string
	mustNotMention []string
}

func exampleOwnershipDoc() []exampleOwnership {
	return []exampleOwnership{
		{
			file: "example-computedomain.yaml",
			mustMention: []string{
				"JobController 按该名称在同一个 namespace 中创建 ResourceClaimTemplate",
				"用户（或上层控制器）创建工作负载 Pod",
			},
			mustNotMention: []string{
				"用户先创建一个 ResourceClaimTemplate",
				"用户预先创建的 ResourceClaimTemplate",
				"JobController 根据 ComputeDomain 创建工作负载 Pod",
			},
		},
		{
			file: "example-computedomain-aliyun.yaml",
			mustMention: []string{
				"控制器会以该名称创建",
			},
			mustNotMention: []string{
				"已存在本文件引用的 ResourceClaimTemplate",
				"用户预先创建",
			},
		},
	}
}

// TestDeployExamplesDocumentResourceOwnership pins that the samples document the
// real ownership split: the ResourceClaimTemplate is created by the controller
// from the ComputeDomain spec name (never pre-created by the user), and the Pod
// is created by the user or an upper-layer controller (never by JobController).
func TestDeployExamplesDocumentResourceOwnership(t *testing.T) {
	for _, example := range exampleOwnershipDoc() {
		example := example
		t.Run(example.file, func(t *testing.T) {
			text := string(readDeployFile(t, example.file))
			for _, want := range example.mustMention {
				if !strings.Contains(text, want) {
					t.Errorf("%s must document %q", example.file, want)
				}
			}
			for _, unwanted := range example.mustNotMention {
				if strings.Contains(text, unwanted) {
					t.Errorf("%s must not claim %q: it contradicts the controller behaviour", example.file, unwanted)
				}
			}
		})
	}
}
