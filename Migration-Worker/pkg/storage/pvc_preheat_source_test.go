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

package storage

import (
	"testing"
)

// TestPreheatSourceServiceName locks the derivation of the long-running preheat
// Service name: sha256("ns/name/pvcPath")[:16], **byte-identical** to
// GeneratePreheatSourceServiceName on the controller side (the hash values are
// hard-coded in this test - the two sides are implemented independently and
// must be pinned to the same output for the same known inputs; otherwise the
// URL restore builds differs from the preheat URL and task_id silently misses).
func TestPreheatSourceServiceName(t *testing.T) {
	tests := []struct {
		namespace string
		pvcName   string
		pvcPath   string
		want      string
	}{
		{
			namespace: "default",
			pvcName:   "oss-migration-pvc",
			pvcPath:   "2a5b82a6-0000-0000-0000-000000000000/v1/c0/checkpoint",
			want:      "preheat-source-fdab78b12c13d573",
		},
		{
			// Leading/trailing slashes must be normalized away: the value returned by
			// validRelPath on the controller side has no leading/trailing slashes either;
			// a checkpointDir with slashes and one without must hash to the same name.
			namespace: "default",
			pvcName:   "oss-migration-pvc",
			pvcPath:   "/2a5b82a6-0000-0000-0000-000000000000/v1/c0/checkpoint/",
			want:      "preheat-source-fdab78b12c13d573",
		},
		{
			// Path segments containing spaces are legal (checkpoint data may be moved to a user-made dir).
			namespace: "ns-a",
			pvcName:   "pvc-b",
			pvcPath:   "my dir/checkpoint",
			want:      "preheat-source-e3dfca63d78569ab",
		},
		{
			namespace: "ns-a",
			pvcName:   "pvc-b",
			pvcPath:   "checkpoint",
			want:      "preheat-source-48e11494ab4cff68",
		},
	}
	for _, tt := range tests {
		if got := preheatSourceServiceName(tt.namespace, tt.pvcName, tt.pvcPath); got != tt.want {
			t.Errorf("preheatSourceServiceName(%q, %q, %q) = %q, want %q",
				tt.namespace, tt.pvcName, tt.pvcPath, got, tt.want)
		}
	}
}

// TestBuildPreheatSourceURL locks the construction of the preheat dir URL:
// ClusterIP:50090 + escaped path. Consistent with the url.URL construction in
// handlePending on the controller side - task_id is computed from the shard
// URL, and a single byte difference makes the preheat cache silently miss.
func TestBuildPreheatSourceURL(t *testing.T) {
	tests := []struct {
		name      string
		clusterIP string
		pvcPath   string
		want      string
	}{
		{
			name:      "plain path",
			clusterIP: "10.96.0.10",
			pvcPath:   "pod-uid/123/c0/checkpoint",
			want:      "http://10.96.0.10:50090/pod-uid/123/c0/checkpoint",
		},
		{
			name:      "path with leading and trailing slashes is normalized",
			clusterIP: "10.96.0.10",
			pvcPath:   "/pod-uid/123/c0/checkpoint/",
			want:      "http://10.96.0.10:50090/pod-uid/123/c0/checkpoint",
		},
		{
			name:      "space is percent-escaped by url.URL",
			clusterIP: "10.96.0.10",
			pvcPath:   "my dir/checkpoint",
			want:      "http://10.96.0.10:50090/my%20dir/checkpoint",
		},
		{
			name:      "question mark is escaped so it stays in the path",
			clusterIP: "10.96.0.10",
			pvcPath:   "my?dir/checkpoint",
			want:      "http://10.96.0.10:50090/my%3Fdir/checkpoint",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := buildPreheatSourceURL(tt.clusterIP, tt.pvcPath); got != tt.want {
				t.Fatalf("buildPreheatSourceURL(%q, %q) = %q, want %q",
					tt.clusterIP, tt.pvcPath, got, tt.want)
			}
		})
	}
}

// TestEndpointSliceResourceMatches locks the core semantics of the
// create-or-update idempotency check: a port drift (the kernel reassigns a
// random port after worker restart) must be recognized as "not matching" and
// trigger update; skip the update when the rest of the fields are identical.
func TestEndpointSliceResourceMatches(t *testing.T) {
	ready := true
	name := "http"
	proto := "TCP"
	port := int32(33221)

	slice := &endpointSliceResource{}
	slice.Endpoints = []struct {
		Addresses  []string `json:"addresses"`
		Conditions struct {
			Ready *bool `json:"ready,omitempty"`
		} `json:"conditions,omitempty"`
	}{
		{
			Addresses: []string{"10.0.0.2"},
			Conditions: struct {
				Ready *bool `json:"ready,omitempty"`
			}{Ready: &ready},
		},
	}
	slice.Ports = []struct {
		Name     *string `json:"name,omitempty"`
		Protocol *string `json:"protocol,omitempty"`
		Port     *int32  `json:"port,omitempty"`
	}{
		{Name: &name, Protocol: &proto, Port: &port},
	}

	if !slice.endpointSliceMatches("10.0.0.2", 33221) {
		t.Fatal("endpointSliceMatches = false for identical port/address, want true (idempotent skip)")
	}
	if slice.endpointSliceMatches("10.0.0.2", 40999) {
		t.Fatal("endpointSliceMatches = true for drifted port, want false (must trigger update)")
	}
	if slice.endpointSliceMatches("10.0.0.3", 33221) {
		t.Fatal("endpointSliceMatches = true for drifted address, want false (must trigger update)")
	}
}

// TestBuildEndpointSliceResource locks the slice's association keys: name
// <serviceName>-<podName>, the kubernetes.io/service-name label pointing to the
// long-running Service, ownerReference to its own pod with controller=true (the
// GC reclaim anchor), endpoint ready=true (kube-proxy only forwards ready
// backends), and the port name matching the Service port name ("http").
func TestBuildEndpointSliceResource(t *testing.T) {
	slice := buildEndpointSliceResource("preheat-source-abc", "migration-worker-node1-x", "uid-123", "10.0.0.2", 33221)

	if slice.APIVersion != "discovery.k8s.io/v1" || slice.Kind != "EndpointSlice" {
		t.Fatalf("unexpected apiVersion/kind: %s/%s", slice.APIVersion, slice.Kind)
	}
	if slice.Metadata.Name != "preheat-source-abc-migration-worker-node1-x" {
		t.Fatalf("slice name = %q, want <serviceName>-<podName>", slice.Metadata.Name)
	}
	if slice.Metadata.Labels["kubernetes.io/service-name"] != "preheat-source-abc" {
		t.Fatalf("slice labels = %v, want kubernetes.io/service-name=preheat-source-abc", slice.Metadata.Labels)
	}
	if len(slice.Metadata.OwnerReferences) != 1 {
		t.Fatalf("ownerReferences len = %d, want 1 (GC anchor to own pod)", len(slice.Metadata.OwnerReferences))
	}
	owner := slice.Metadata.OwnerReferences[0]
	if owner.Kind != "Pod" || owner.Name != "migration-worker-node1-x" || owner.UID != "uid-123" || owner.Controller == nil || !*owner.Controller {
		t.Fatalf("unexpected ownerReference %+v", owner)
	}
	if slice.AddressType != "IPv4" {
		t.Fatalf("addressType = %q, want IPv4", slice.AddressType)
	}
	if len(slice.Endpoints) != 1 || len(slice.Endpoints[0].Addresses) != 1 || slice.Endpoints[0].Addresses[0] != "10.0.0.2" {
		t.Fatalf("unexpected endpoints %+v", slice.Endpoints)
	}
	if slice.Endpoints[0].Conditions.Ready == nil || !*slice.Endpoints[0].Conditions.Ready {
		t.Fatal("endpoint ready must be explicitly true, otherwise kube-proxy drops it")
	}
	if len(slice.Ports) != 1 || slice.Ports[0].Name == nil || *slice.Ports[0].Name != "http" ||
		slice.Ports[0].Port == nil || int(*slice.Ports[0].Port) != 33221 {
		t.Fatalf("unexpected ports %+v", slice.Ports)
	}
}
