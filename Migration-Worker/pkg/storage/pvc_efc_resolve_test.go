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
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"k8s.io/client-go/rest"
)

func TestEfcRelPath(t *testing.T) {
	tests := []struct {
		name          string
		prefix        string
		checkpointDir string
		want          string
	}{
		{name: "with prefix", prefix: "demo/migration-worker", checkpointDir: "pod-1/123/c0/checkpoint", want: "demo/migration-worker/pod-1/123/c0/checkpoint"},
		{name: "empty prefix", prefix: "", checkpointDir: "pod-1/123/c0/checkpoint", want: "pod-1/123/c0/checkpoint"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := efcRelPath(tt.prefix, tt.checkpointDir); got != tt.want {
				t.Fatalf("efcRelPath(%q, %q) = %q, want %q", tt.prefix, tt.checkpointDir, got, tt.want)
			}
		})
	}
}

// TestResolvePVCEFCPrefix simulates the PVC/PV REST responses of the apiserver
// with httptest and verifies the field-probing matrix: csi.volumeAttributes.path
// preferred, nfs.path fallback, empty when neither is available, and paths
// escaping /mnt/efc treated as "not available".
func TestResolvePVCEFCPrefix(t *testing.T) {
	const pvcNS = "default"
	// pvcVolumes: PVC name -> volumeName; empty = PVC created but unbound; missing key = PVC does not exist.
	pvcVolumes := map[string]string{
		"test-pvc":          "pv-oss",
		"nas-pvc":           "pv-nas",
		"nfs-pvc":           "pv-nfs",
		"nfsroot-pvc":       "pv-nfsroot",
		"hostpath-pvc":      "pv-hostpath",
		"nopath-pvc":        "pv-nopath",
		"unbound-pvc":       "",
		"missing-pv-pvc":    "pv-none",
		"escape-pvc":        "pv-escape",
		"escape-nested-pvc": "pv-escape-nested",
		"escape-nfs-pvc":    "pv-nfs-escape",
	}
	pvResponses := map[string]string{
		"pv-oss":      `{"spec":{"csi":{"volumeAttributes":{"path":"/demo/migration-worker","bucket":"b1"}}}}`,
		"pv-nas":      `{"spec":{"csi":{"volumeAttributes":{"path":"/data","server":"nas.example.com"}}}}`,
		"pv-nfs":      `{"spec":{"nfs":{"path":"/export/data","server":"nfs.example.com"}}}`,
		"pv-nfsroot":  `{"spec":{"nfs":{"path":"/","server":"nfs.example.com"}}}`,
		"pv-hostpath": `{"spec":{"hostPath":{"path":"/data/migration"}}}`,
		"pv-nopath":   `{"spec":{"csi":{"volumeAttributes":{"bucket":"b1"}}}}`,
		// The following three are escape cases: PV is a cluster-scoped object whose
		// path is written by static yaml or a provisioner; "../" must be blocked so
		// filepath.Join cannot point reads outside the mount root.
		"pv-escape":        `{"spec":{"csi":{"volumeAttributes":{"path":"/../../etc","bucket":"b1"}}}}`,
		"pv-escape-nested": `{"spec":{"csi":{"volumeAttributes":{"path":"/demo/migration-worker/../../..","bucket":"b1"}}}}`,
		"pv-nfs-escape":    `{"spec":{"nfs":{"path":"/../etc","server":"nfs.example.com"}}}`,
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if pvcPrefix := "/api/v1/namespaces/" + pvcNS + "/persistentvolumeclaims/"; strings.HasPrefix(r.URL.Path, pvcPrefix) {
			name := strings.TrimPrefix(r.URL.Path, pvcPrefix)
			vol, ok := pvcVolumes[name]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if vol == "" {
				fmt.Fprint(w, `{"spec":{}}`)
				return
			}
			fmt.Fprintf(w, `{"spec":{"volumeName":%q}}`, vol)
			return
		}
		if pvPrefix := "/api/v1/persistentvolumes/"; strings.HasPrefix(r.URL.Path, pvPrefix) {
			name := strings.TrimPrefix(r.URL.Path, pvPrefix)
			body, ok := pvResponses[name]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			fmt.Fprint(w, body)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	if err := SetKubeRESTConfig(&rest.Config{Host: server.URL}); err != nil {
		t.Fatalf("SetKubeRESTConfig failed: %v", err)
	}

	ctx := context.Background()
	tests := []struct {
		name string
		pvc  string
		want string
	}{
		{name: "oss csi path", pvc: "test-pvc", want: "demo/migration-worker"},
		{name: "nas csi path", pvc: "nas-pvc", want: "data"},
		{name: "nfs path", pvc: "nfs-pvc", want: "export/data"},
		{name: "nfs root path", pvc: "nfsroot-pvc", want: ""},
		{name: "hostpath pv no shared prefix", pvc: "hostpath-pvc", want: ""},
		{name: "csi without path key", pvc: "nopath-pvc", want: ""},
		{name: "unbound pvc", pvc: "unbound-pvc", want: ""},
		{name: "pv not found", pvc: "missing-pv-pvc", want: ""},
		{name: "pvc not found", pvc: "ghost-pvc", want: ""},
		{name: "empty pvc name", pvc: "", want: ""},
		{name: "csi path escaping mount root", pvc: "escape-pvc", want: ""},
		{name: "csi path nested escape", pvc: "escape-nested-pvc", want: ""},
		{name: "nfs path escaping mount root", pvc: "escape-nfs-pvc", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolvePVCEFCPrefix(ctx, pvcNS, tt.pvc)
			if got != tt.want {
				t.Fatalf("resolvePVCEFCPrefix(%q) = %q, want %q", tt.pvc, got, tt.want)
			}
		})
	}
}
