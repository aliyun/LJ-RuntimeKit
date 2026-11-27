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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"k8s.io/klog/v2"
)

// Co-hosted source: the restore worker mounts the same PVC, runs its own http
// file server and registers its own EndpointSlice, so after the preheat source
// pod (deleted once phase=Ready) exits, it keeps serving as the Dragonfly
// origin.
//
// Differences from the preheat source pod:
//   - the kernel allocates the port from 0.0.0.0:0 (per-node multi-task
//     workers naturally never conflict, no port allocator); after manual
//     EndpointSlice registration, kube-proxy forwards from the long-running
//     Service's 50090;
//   - the EndpointSlice is named <serviceName>-<podName> with ownerReference to
//     its own pod, reclaimed by K8s GC after pod deletion; the worker does no
//     active cleanup (converges even on kill / crash).

// preheatSourceHTTPPort matches controllers.PreheatSourceHTTPPort on the
// controller side: the long-running Service's internal port is fixed at 50090;
// URLs must use this value or task_id misses the preheat cache.
const preheatSourceHTTPPort = 50090

// preheatSourceServiceName derives the long-running preheat Service name:
// preheat-source-<first 16 hex of sha256("ns/name/pvcPath")>.
//
// **Byte-identical** to GeneratePreheatSourceServiceName on the controller side
// (same (pvcNamespace, pvcName, pvcPath) triple, both sides trim leading and
// trailing slashes from pvcPath): only if both sides compute the same Service
// name can restore get the ClusterIP used at preheat time, build the same URL,
// and make task_id hit the P2P cache.
func preheatSourceServiceName(pvcNamespace, pvcName, pvcPath string) string {
	key := pvcNamespace + "/" + pvcName + "/" + strings.Trim(pvcPath, "/")
	sum := sha256.Sum256([]byte(key))
	return "preheat-source-" + hex.EncodeToString(sum[:])[:16]
}

// preheatSourceServer is the co-hosted source's http file server: serves
// /mnt/pv (same root as the preheat source pod). Range is natively supported by
// net/http, which dfdaemon's ranged origin fetch depends on.
type preheatSourceServer struct {
	srv  *http.Server
	port int // the actual listening port allocated by the kernel (written into its own EndpointSlice)
}

// startPreheatSourceServer starts the co-hosted http server and returns the
// actual port. The server lives and dies with the worker process (pod deletion
// terminates the process), no explicit shutdown needed.
func startPreheatSourceServer() (*preheatSourceServer, error) {
	mux := http.NewServeMux()
	// An isolated mux only, to avoid polluting /metrics on the global DefaultServeMux.
	mux.Handle("/", http.FileServer(http.Dir(pvcMountPoint)))

	ln, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		return nil, fmt.Errorf("listen for preheat source: %w", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	srv := &http.Server{Handler: mux}
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			klog.Warningf("preheat source http server failed: %v", err)
		}
	}()
	return &preheatSourceServer{srv: srv, port: port}, nil
}

// derivePreheatSourceURL rebuilds the preheat dir URL from (pvcNamespace,
// pvcName, checkpointDir): derive the long-running Service name -> take the
// ClusterIP -> construct via url.URL. Byte-identical to the URL construction in
// handlePending on the controller side (checkpointDir maps to pvcPath, also
// trimmed of leading/trailing slashes; url.URL's String() escapes special chars
// per encodePath, and http.FileServer receives the decoded original value - a
// lossless round trip). Returns an error when the Service does not exist
// (preheat never ran on this path); the caller degrades to direct PVC read.
func derivePreheatSourceURL(ctx context.Context, pvcNamespace, pvcName, checkpointDir string) (string, error) {
	serviceName := preheatSourceServiceName(pvcNamespace, pvcName, checkpointDir)
	clusterIP, err := getServiceClusterIP(ctx, pvcNamespace, serviceName)
	if err != nil {
		return "", err
	}
	return buildPreheatSourceURL(clusterIP, checkpointDir), nil
}

// buildPreheatSourceURL builds the preheat dir URL from ClusterIP and pvcPath
// (pure function, so unit tests can lock the byte-identical consistency with
// handlePending on the controller side: a single byte difference in the URL
// breaks task_id and silently misses the preheat cache).
func buildPreheatSourceURL(clusterIP, pvcPath string) string {
	return (&url.URL{
		Scheme: "http",
		Host:   net.JoinHostPort(clusterIP, strconv.Itoa(preheatSourceHTTPPort)),
		Path:   "/" + strings.Trim(pvcPath, "/"),
	}).String()
}

// serviceResource is the minimal view of a core/v1 Service: the co-hosted source only needs ClusterIP.
type serviceResource struct {
	Spec struct {
		ClusterIP string `json:"clusterIP"`
	} `json:"spec"`
}

// getServiceClusterIP reads the ClusterIP of the long-running preheat Service.
func getServiceClusterIP(ctx context.Context, namespace, name string) (string, error) {
	svc := &serviceResource{}
	if err := kubeDoJSON(ctx, http.MethodGet,
		fmt.Sprintf("/api/v1/namespaces/%s/services/%s", namespace, name), nil, svc); err != nil {
		return "", fmt.Errorf("get preheat source service %s/%s: %w", namespace, name, err)
	}
	if svc.Spec.ClusterIP == "" {
		return "", fmt.Errorf("preheat source service %s/%s has no clusterIP", namespace, name)
	}
	return svc.Spec.ClusterIP, nil
}

// endpointSliceResource is the minimal view of a discovery.k8s.io/v1
// EndpointSlice: the worker only writes its own entry, and the fields exactly
// cover everything the apiserver validation requires.
type endpointSliceResource struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		Name            string            `json:"name"`
		Namespace       string            `json:"namespace"`
		Labels          map[string]string `json:"labels,omitempty"`
		OwnerReferences []struct {
			APIVersion string `json:"apiVersion"`
			Kind       string `json:"kind"`
			Name       string `json:"name"`
			UID        string `json:"uid"`
			Controller *bool  `json:"controller,omitempty"`
		} `json:"ownerReferences,omitempty"`
		ResourceVersion string `json:"resourceVersion,omitempty"`
	} `json:"metadata"`
	AddressType string `json:"addressType"`
	Endpoints   []struct {
		Addresses  []string `json:"addresses"`
		Conditions struct {
			Ready *bool `json:"ready,omitempty"`
		} `json:"conditions,omitempty"`
	} `json:"endpoints"`
	Ports []struct {
		Name     *string `json:"name,omitempty"`
		Protocol *string `json:"protocol,omitempty"`
		Port     *int32  `json:"port,omitempty"`
	} `json:"ports,omitempty"`
}

// endpointSliceMatches checks whether the existing slice is already in the
// desired shape (ports and addresses match and ready). Used for idempotent
// create-or-update: only a port drift (random port changes after worker process
// restart) needs an update.
func (e *endpointSliceResource) endpointSliceMatches(nodeIP string, port int) bool {
	if len(e.Endpoints) != 1 || len(e.Endpoints[0].Addresses) != 1 {
		return false
	}
	if e.Endpoints[0].Addresses[0] != nodeIP {
		return false
	}
	if e.Endpoints[0].Conditions.Ready == nil || !*e.Endpoints[0].Conditions.Ready {
		return false
	}
	if len(e.Ports) != 1 || e.Ports[0].Port == nil || int(*e.Ports[0].Port) != port {
		return false
	}
	return true
}

// buildEndpointSliceResource constructs the desired EndpointSlice object for this worker.
func buildEndpointSliceResource(serviceName, podName, podUID, nodeIP string, port int) *endpointSliceResource {
	controllerTrue := true
	ready := true
	portName := "http"
	protocol := "TCP"
	port32 := int32(port)
	slice := &endpointSliceResource{
		APIVersion: "discovery.k8s.io/v1",
		Kind:       "EndpointSlice",
	}
	slice.Metadata.Name = serviceName + "-" + podName
	slice.Metadata.Namespace = "" // POST determines it from the request path; PUT needs no backfill
	slice.Metadata.Labels = map[string]string{
		"kubernetes.io/service-name": serviceName,
	}
	slice.Metadata.OwnerReferences = []struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		Name       string `json:"name"`
		UID        string `json:"uid"`
		Controller *bool  `json:"controller,omitempty"`
	}{
		{APIVersion: "v1", Kind: "Pod", Name: podName, UID: podUID, Controller: &controllerTrue},
	}
	slice.AddressType = "IPv4"
	slice.Endpoints = []struct {
		Addresses  []string `json:"addresses"`
		Conditions struct {
			Ready *bool `json:"ready,omitempty"`
		} `json:"conditions,omitempty"`
	}{
		{
			Addresses: []string{nodeIP},
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
		{Name: &portName, Protocol: &protocol, Port: &port32},
	}
	return slice
}

// registerPreheatSourceEndpointSlice registers this worker as a backend of the
// long-running preheat Service (idempotent create-or-update): name
// <serviceName>-<podName>, endpoint = nodeIP:actual listening port,
// ownerReference to its own pod (reclaimed by GC). After a worker process
// restart the random port changes; use update to overwrite its own slice, no
// stale port entries left behind.
//
// podName/podUID/nodeIP come from downward API env injection (POD_NAME /
// POD_UID / NODE_IP, template change); missing or unavailable kube config
// returns an error and the caller only warns - the co-hosted source is an
// accelerator; failed registration only loses origin reachability (per-part
// direct PVC read fallback on miss), never blocks restore.
func registerPreheatSourceEndpointSlice(ctx context.Context, namespace, serviceName string, port int) error {
	podName := os.Getenv("POD_NAME")
	podUID := os.Getenv("POD_UID")
	nodeIP := os.Getenv("NODE_IP")
	if podName == "" || podUID == "" || nodeIP == "" {
		return fmt.Errorf("POD_NAME/POD_UID/NODE_IP env not injected (worker template must add downward API env)")
	}

	sliceName := serviceName + "-" + podName
	path := fmt.Sprintf("/apis/discovery.k8s.io/v1/namespaces/%s/endpointslices/%s", namespace, sliceName)

	existing := &endpointSliceResource{}
	err := kubeDoJSON(ctx, http.MethodGet, path, nil, existing)
	if err == nil {
		if existing.endpointSliceMatches(nodeIP, port) {
			return nil // already in the desired shape, idempotent skip
		}
		desired := buildEndpointSliceResource(serviceName, podName, podUID, nodeIP, port)
		desired.Metadata.ResourceVersion = existing.Metadata.ResourceVersion
		if err := kubeDoJSON(ctx, http.MethodPut, path, desired, nil); err != nil {
			return fmt.Errorf("update preheat source endpointslice %s: %w", sliceName, err)
		}
		klog.V(2).Infof("updated preheat source endpointslice %s -> %s:%d", sliceName, nodeIP, port)
		return nil
	}
	// GET 404 is treated as not-exists; other errors (no permission / network) propagate.
	if !isKubeNotFoundError(err) {
		return fmt.Errorf("get preheat source endpointslice %s: %w", sliceName, err)
	}

	desired := buildEndpointSliceResource(serviceName, podName, podUID, nodeIP, port)
	if err := kubeDoJSON(ctx, http.MethodPost,
		fmt.Sprintf("/apis/discovery.k8s.io/v1/namespaces/%s/endpointslices", namespace), desired, nil); err != nil {
		if isKubeAlreadyExistsError(err) {
			return nil // concurrent create race: another goroutine already created it, count as success
		}
		return fmt.Errorf("create preheat source endpointslice %s: %w", sliceName, err)
	}
	klog.V(2).Infof("created preheat source endpointslice %s -> %s:%d", sliceName, nodeIP, port)
	return nil
}

// kubeDoJSON sends a JSON request to the apiserver and decodes the response
// body into out (out may be nil, meaning the body is not needed). Reuses the
// REST config and HTTP client injected by SetKubeRESTConfig in preheatjob.go -
// the worker deliberately avoids typed clientsets and uses plain REST with
// minimal views.
func kubeDoJSON(ctx context.Context, method, path string, body interface{}, out interface{}) error {
	kubeMu.RLock()
	cfg, client := kubeRESTConfig, kubeHTTPClient
	kubeMu.RUnlock()

	if cfg == nil || client == nil {
		return fmt.Errorf("kubernetes client is not initialized")
	}

	var reader io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal kube request body: %w", err)
		}
		reader = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(cfg.Host, "/")+path, reader)
	if err != nil {
		return fmt.Errorf("build kube request %s %s: %w", method, path, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("kube request %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read kube response %s %s: %w", method, path, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return &kubeStatusError{status: resp.StatusCode, body: string(respBody)}
	}
	if out != nil {
		if err := json.Unmarshal(respBody, out); err != nil {
			return fmt.Errorf("unmarshal kube response %s %s: %w", method, path, err)
		}
	}
	return nil
}

// kubeStatusError is a kube request error carrying the HTTP status code, for 404/409 decision reuse.
type kubeStatusError struct {
	status int
	body   string
}

func (e *kubeStatusError) Error() string {
	return fmt.Sprintf("status=%d body=%s", e.status, e.body)
}

func isKubeNotFoundError(err error) bool {
	if se, ok := err.(*kubeStatusError); ok {
		return se.status == http.StatusNotFound
	}
	return false
}

func isKubeAlreadyExistsError(err error) bool {
	if se, ok := err.(*kubeStatusError); ok {
		return se.status == http.StatusConflict
	}
	return false
}
