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

package preheatjob

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/container-migration/controller/api/v1beta1"
	"github.com/container-migration/controller/pkg/preheat"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// A real example from the user environment: EFC only needs the path part of the URL (with the leading "/" stripped).
const (
	efcTestRemoteURL = "https://example-bucket.oss-cn-hangzhou.aliyuncs.com/migration/560642b3-7c93-4486-8615-d2a1e2ed3bde/1787626660105229171/demo-gpu-test"
	efcTestDir       = "migration/560642b3-7c93-4486-8615-d2a1e2ed3bde/1787626660105229171/demo-gpu-test"
)

// nodeListClient is a minimal stub implementing only NodeList's List (filtered
// by label selector); controller-runtime's fake client is not vendored, so the
// other methods are unreachable by the tests.
type nodeListClient struct {
	client.Client
	nodes []corev1.Node
	err   error
}

func (c nodeListClient) List(_ context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if c.err != nil {
		return c.err
	}
	nodeList, ok := list.(*corev1.NodeList)
	if !ok {
		return fmt.Errorf("unexpected list type %T", list)
	}
	lo := &client.ListOptions{}
	for _, o := range opts {
		o.ApplyToList(lo)
	}
	nodeList.Items = nil
	for i := range c.nodes {
		if lo.LabelSelector != nil && !lo.LabelSelector.Matches(labels.Set(c.nodes[i].Labels)) {
			continue
		}
		nodeList.Items = append(nodeList.Items, c.nodes[i])
	}
	return nil
}

func efcTestNode(name, ip string, labeled bool) corev1.Node {
	node := corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{
			Addresses:  []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: ip}},
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
		},
	}
	if labeled {
		node.Labels = map[string]string{efcNodeLabelKey: efcNodeLabelValue}
	}
	return node
}

// newEFCTestBackend starts an httptest server masquerading as the warmup
// service on a node and points efcBackend's port at it; the test node's
// InternalIP is fixed at 127.0.0.1.
func newEFCTestBackend(t *testing.T, nodes []corev1.Node, handler http.HandlerFunc) efcBackend {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	u, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse test server URL: %v", err)
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatalf("parse test server port: %v", err)
	}
	return efcBackend{
		kubeClient: nodeListClient{nodes: nodes},
		httpClient: server.Client(),
		port:       port,
	}
}

func efcTestJob() *v1beta1.PreheatJob {
	return &v1beta1.PreheatJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "job-1"},
		Spec:       v1beta1.PreheatJobSpec{RemoteURL: efcTestRemoteURL},
	}
}

// TestEFCTargetDir pins the URL -> target_path conversion: only the path part
// is taken with the leading "/" stripped, signature parameters do not affect
// the result, and a missing object path errors out.
func TestEFCTargetDir(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{name: "plain url", in: efcTestRemoteURL, want: efcTestDir},
		{name: "presigned url", in: efcTestRemoteURL + "?Expires=1&OSSAccessKeyId=ak&Signature=sig", want: efcTestDir},
		{name: "root path", in: "https://bucket.oss-cn-hongkong-internal.aliyuncs.com/", wantErr: true},
		{name: "no path", in: "https://bucket.oss-cn-hongkong-internal.aliyuncs.com", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := efcTargetDir(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("efcTargetDir(%q) = %q, expected an error", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("efcTargetDir(%q) returned an unexpected error: %v", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("efcTargetDir(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// TestEFCGetPreheatTask: GetPreheatTask queries the stat API and maps Status
// to task state: completed -> Success, running -> Running, failed -> Failure
// (with ErrorMessage).
func TestEFCGetPreheatTask(t *testing.T) {
	tests := []struct {
		status    string
		errMsg    string
		wantState preheat.TaskState
	}{
		{status: "completed", wantState: preheat.TaskStateSuccess},
		{status: "running", wantState: preheat.TaskStateRunning},
		{status: "failed", errMsg: "oss 403 forbidden", wantState: preheat.TaskStateFailure},
	}
	for _, tt := range tests {
		t.Run(tt.status, func(t *testing.T) {
			var gotPath, gotTarget string
			b := newEFCTestBackend(t,
				[]corev1.Node{efcTestNode("node-1", "127.0.0.1", true)},
				func(w http.ResponseWriter, r *http.Request) {
					gotPath = r.URL.Path
					gotTarget = r.URL.Query().Get(efcTargetPathParam)
					_ = json.NewEncoder(w).Encode(efcStatResult{
						ErrorCode: 0,
						Results: []efcResult{{
							ErrorCode: 0,
							TaskInfos: []efcTaskInfo{{ErrorCode: 0, ErrorMessage: tt.errMsg, Status: tt.status}},
						}},
					})
				})

			task, err := b.GetPreheatTask(context.Background(), efcTestDir)
			if err != nil {
				t.Fatalf("GetPreheatTask returned an unexpected error: %v", err)
			}
			if gotPath != "/v1/warmup/stat" {
				t.Errorf("request path = %q, want %q", gotPath, "/v1/warmup/stat")
			}
			if gotTarget != efcTestDir {
				t.Errorf("target_path = %q, want %q", gotTarget, efcTestDir)
			}
			if task.ID != efcTestDir {
				t.Errorf("task.ID = %q, want %q", task.ID, efcTestDir)
			}
			if task.State != tt.wantState {
				t.Errorf("task.State = %q, want %q", task.State, tt.wantState)
			}
			if tt.wantState == preheat.TaskStateFailure && task.Message != tt.errMsg {
				t.Errorf("task.Message = %q, want %q", task.Message, tt.errMsg)
			}
		})
	}
}

// TestEFCGetPreheatTaskPending: keep Running when stat cannot find the task
// yet (empty TaskInfos), avoiding misjudging the brief window where load was
// accepted but stat has not registered the task.
func TestEFCGetPreheatTaskPending(t *testing.T) {
	b := newEFCTestBackend(t,
		[]corev1.Node{efcTestNode("node-1", "127.0.0.1", true)},
		func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(efcStatResult{ErrorCode: 0})
		})

	task, err := b.GetPreheatTask(context.Background(), efcTestDir)
	if err != nil {
		t.Fatalf("GetPreheatTask returned an unexpected error: %v", err)
	}
	if task.State != preheat.TaskStateRunning {
		t.Errorf("task.State = %q, want %q", task.State, preheat.TaskStateRunning)
	}
}

// TestEFCGetPreheatTaskNoNodes: GetPreheatTask errors out when no labeled
// node exists, handed to handleRunning's retryOrFail to retry, and no request
// is sent out.
func TestEFCGetPreheatTaskNoNodes(t *testing.T) {
	b := newEFCTestBackend(t, nil, func(w http.ResponseWriter, r *http.Request) {
		t.Error("warmup API must not be called when no labeled node exists")
	})

	_, err := b.GetPreheatTask(context.Background(), efcTestDir)
	if !errors.Is(err, errNoEFCNodes) {
		t.Errorf("error = %v, want errNoEFCNodes", err)
	}
}

// TestEFCStartPreheat walks the full load path: the request hits
// /v1/warmup/load, target_path is the directory path with the leading "/"
// stripped, and taskIDs contains only this one dir.
func TestEFCStartPreheat(t *testing.T) {
	var gotPath, gotTarget string
	b := newEFCTestBackend(t,
		[]corev1.Node{efcTestNode("node-1", "127.0.0.1", true)},
		func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.Path
			gotTarget = r.URL.Query().Get(efcTargetPathParam)
			_ = json.NewEncoder(w).Encode(efcWarmupResult{ErrorCode: 0, ErrorMessage: ""})
		})

	job := efcTestJob()
	taskIDs, err := b.StartPreheat(context.Background(), preheat.StartPreheatRequest{
		Job:       job,
		RemoteURL: job.Spec.RemoteURL,
	})
	if err != nil {
		t.Fatalf("StartPreheat returned an unexpected error: %v", err)
	}
	if gotPath != "/v1/warmup/load" {
		t.Errorf("request path = %q, want %q", gotPath, "/v1/warmup/load")
	}
	if gotTarget != efcTestDir {
		t.Errorf("target_path = %q, want %q", gotTarget, efcTestDir)
	}
	if len(taskIDs) != 1 || taskIDs[0] != efcTestDir {
		t.Errorf("taskIDs = %v, want [%s]", taskIDs, efcTestDir)
	}
}

// TestEFCStartPreheatErrorCode: load must error out on a non-zero ErrorCode,
// and the error carries the server's ErrorMessage for retryOrFail to write
// into status.message.
func TestEFCStartPreheatErrorCode(t *testing.T) {
	b := newEFCTestBackend(t,
		[]corev1.Node{efcTestNode("node-1", "127.0.0.1", true)},
		func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(efcWarmupResult{ErrorCode: 7, ErrorMessage: "disk quota exceeded"})
		})

	_, err := b.StartPreheat(context.Background(), preheat.StartPreheatRequest{
		Job:       efcTestJob(),
		RemoteURL: efcTestRemoteURL,
	})
	if err == nil {
		t.Fatal("StartPreheat: expected an error, got nil")
	}
	if !strings.Contains(err.Error(), "error code 7") || !strings.Contains(err.Error(), "disk quota exceeded") {
		t.Errorf("error %q should carry the server error code and message", err)
	}
}

// TestEFCStartPreheatNoNodes: StartPreheat errors out directly when no
// labeled node exists (retried via retryOrFail), and no request is sent out.
func TestEFCStartPreheatNoNodes(t *testing.T) {
	b := newEFCTestBackend(t,
		[]corev1.Node{efcTestNode("node-1", "127.0.0.1", false)},
		func(w http.ResponseWriter, r *http.Request) {
			t.Error("warmup API must not be called when no labeled node exists")
		})

	_, err := b.StartPreheat(context.Background(), preheat.StartPreheatRequest{
		Job:       efcTestJob(),
		RemoteURL: efcTestRemoteURL,
	})
	if !errors.Is(err, errNoEFCNodes) {
		t.Errorf("error = %v, want errNoEFCNodes", err)
	}
}

// TestEFCStartPreheatNoSource: error out when both remoteURL and checkpointUID are missing.
func TestEFCStartPreheatNoSource(t *testing.T) {
	b := newEFCTestBackend(t,
		[]corev1.Node{efcTestNode("node-1", "127.0.0.1", true)},
		func(w http.ResponseWriter, r *http.Request) {})

	_, err := b.StartPreheat(context.Background(), preheat.StartPreheatRequest{
		Job: &v1beta1.PreheatJob{},
	})
	if err == nil || !strings.Contains(err.Error(), "neither remoteURL nor checkpointUID") {
		t.Errorf("error = %v, want a missing-source error", err)
	}
}

// TestEFCProbeCache walks the full stat path and pins the Status -> CacheState mapping:
// completed -> Cached、running -> Running、failed -> Failed。
func TestEFCProbeCache(t *testing.T) {
	tests := []struct {
		status     string
		wantState  preheat.CacheState
		wantCached bool
	}{
		{status: "completed", wantState: preheat.CacheStateCached, wantCached: true},
		{status: "running", wantState: preheat.CacheStateRunning, wantCached: false},
		{status: "failed", wantState: preheat.CacheStateFailed, wantCached: false},
	}
	for _, tt := range tests {
		t.Run(tt.status, func(t *testing.T) {
			var gotPath, gotTarget string
			b := newEFCTestBackend(t,
				[]corev1.Node{efcTestNode("node-1", "127.0.0.1", true)},
				func(w http.ResponseWriter, r *http.Request) {
					gotPath = r.URL.Path
					gotTarget = r.URL.Query().Get(efcTargetPathParam)
					_ = json.NewEncoder(w).Encode(efcStatResult{
						ErrorCode: 0,
						Results: []efcResult{{
							ErrorCode: 0,
							TaskInfos: []efcTaskInfo{{ErrorCode: 0, Status: tt.status}},
						}},
					})
				})

			peers, states, err := b.ProbeCache(context.Background(), efcTestJob())
			if err != nil {
				t.Fatalf("ProbeCache returned an unexpected error: %v", err)
			}
			if gotPath != "/v1/warmup/stat" {
				t.Errorf("request path = %q, want %q", gotPath, "/v1/warmup/stat")
			}
			if gotTarget != efcTestDir {
				t.Errorf("target_path = %q, want %q", gotTarget, efcTestDir)
			}
			if len(peers) != 1 {
				t.Fatalf("peers = %v, want exactly one entry", peers)
			}
			if peers[0].LastSeen == nil {
				t.Error("peer.LastSeen = nil, want a timestamp")
			}
			if peers[0].Cached != tt.wantCached {
				t.Errorf("peer.Cached = %v, want %v", peers[0].Cached, tt.wantCached)
			}
			if len(states) != 1 || states[0] != tt.wantState {
				t.Errorf("states = %v, want [%s]", states, tt.wantState)
			}
		})
	}
}

// TestEFCProbeCacheEmptyTaskInfos: treat as Running when stat returns empty
// TaskInfos, waiting for the next probe round to confirm whether the task is
// registered, while not misjudging it as cached.
func TestEFCProbeCacheEmptyTaskInfos(t *testing.T) {
	b := newEFCTestBackend(t,
		[]corev1.Node{efcTestNode("node-1", "127.0.0.1", true)},
		func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(efcStatResult{ErrorCode: 0})
		})

	peers, states, err := b.ProbeCache(context.Background(), efcTestJob())
	if err != nil {
		t.Fatalf("ProbeCache returned an unexpected error: %v", err)
	}
	if len(states) != 1 || states[0] != preheat.CacheStateRunning {
		t.Errorf("states = %v, want [%s]", states, preheat.CacheStateRunning)
	}
	if peers[0].Cached {
		t.Error("peer.Cached = true, want false")
	}
}

// TestEFCProbeCacheResultErrorCode: treat as Failed on a non-zero
// directory-level ErrorCode: the whole request succeeded (outer ErrorCode is
// 0), only this directory cannot be queried; it must not error out and
// retry.
func TestEFCProbeCacheResultErrorCode(t *testing.T) {
	b := newEFCTestBackend(t,
		[]corev1.Node{efcTestNode("node-1", "127.0.0.1", true)},
		func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(efcStatResult{
				ErrorCode: 0,
				Results:   []efcResult{{ErrorCode: 5, ErrorMessage: "target path not mounted"}},
			})
		})

	peers, states, err := b.ProbeCache(context.Background(), efcTestJob())
	if err != nil {
		t.Fatalf("ProbeCache returned an unexpected error: %v", err)
	}
	if len(states) != 1 || states[0] != preheat.CacheStateFailed {
		t.Errorf("states = %v, want [%s]", states, preheat.CacheStateFailed)
	}
	if peers[0].Cached {
		t.Error("peer.Cached = true, want false")
	}
}

// TestEFCGetPreheatTaskResultErrorCode: judge the task failed on a non-zero
// directory-level ErrorCode and pass the server's error reason up, avoiding
// infinite polling by handleRunning.
func TestEFCGetPreheatTaskResultErrorCode(t *testing.T) {
	b := newEFCTestBackend(t,
		[]corev1.Node{efcTestNode("node-1", "127.0.0.1", true)},
		func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(efcStatResult{
				ErrorCode: 0,
				Results:   []efcResult{{ErrorCode: 5, ErrorMessage: "target path not mounted"}},
			})
		})

	task, err := b.GetPreheatTask(context.Background(), efcTestDir)
	if err != nil {
		t.Fatalf("GetPreheatTask returned an unexpected error: %v", err)
	}
	if task.State != preheat.TaskStateFailure {
		t.Errorf("task.State = %s, want %s", task.State, preheat.TaskStateFailure)
	}
	if task.Message != "target path not mounted" {
		t.Errorf("task.Message = %q, want the server error message", task.Message)
	}
}

// TestEFCProbeCacheStatErrorCode: a non-zero outer ErrorCode of stat returns
// an error, logged by probeJob and retried next round instead of being
// misjudged Stale.
func TestEFCProbeCacheStatErrorCode(t *testing.T) {
	b := newEFCTestBackend(t,
		[]corev1.Node{efcTestNode("node-1", "127.0.0.1", true)},
		func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(efcStatResult{ErrorCode: 3, ErrorMessage: "warmup service busy"})
		})

	_, _, err := b.ProbeCache(context.Background(), efcTestJob())
	if err == nil {
		t.Fatal("ProbeCache: expected an error, got nil")
	}
	if !strings.Contains(err.Error(), "warmup service busy") {
		t.Errorf("error %q should carry the server error message", err)
	}
}

// TestEFCProbeCacheNoTargets: return preheat.ErrNoProbeTargets when no
// labeled node exists; the caller skips this probe round without updating
// status.
func TestEFCProbeCacheNoTargets(t *testing.T) {
	b := newEFCTestBackend(t, nil, func(w http.ResponseWriter, r *http.Request) {
		t.Error("warmup API must not be called when no labeled node exists")
	})

	_, _, err := b.ProbeCache(context.Background(), efcTestJob())
	if !errors.Is(err, preheat.ErrNoProbeTargets) {
		t.Errorf("error = %v, want preheat.ErrNoProbeTargets", err)
	}
}

// pvcPVGetClient is a minimal stub implementing only Get (PVC/PV):
// controller-runtime's fake client is not vendored, and resolvePVCTargetDir
// only uses Get.
type pvcPVGetClient struct {
	client.Client
	pvcs map[string]*corev1.PersistentVolumeClaim // key: namespace/name
	pvs  map[string]*corev1.PersistentVolume      // key: name
}

func (c pvcPVGetClient) Get(_ context.Context, key client.ObjectKey, obj client.Object,
	_ ...client.GetOption) error {
	switch o := obj.(type) {
	case *corev1.PersistentVolumeClaim:
		v, ok := c.pvcs[key.Namespace+"/"+key.Name]
		if !ok {
			return k8serrors.NewNotFound(corev1.Resource("persistentvolumeclaims"), key.Name)
		}
		*o = *v
		return nil
	case *corev1.PersistentVolume:
		v, ok := c.pvs[key.Name]
		if !ok {
			return k8serrors.NewNotFound(corev1.Resource("persistentvolumes"), key.Name)
		}
		*o = *v
		return nil
	}
	return fmt.Errorf("unexpected get type %T", obj)
}

func efcTestCSIPathPV(path string) *corev1.PersistentVolume {
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-under-test"},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:           "ossplugin.csi.alibabacloud.com",
					VolumeAttributes: map[string]string{"path": path, "bucket": "b1"},
				},
			},
		},
	}
}

func efcTestNFSPathPV(path string) *corev1.PersistentVolume {
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-under-test"},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				NFS: &corev1.NFSVolumeSource{Server: "nfs.example.com", Path: path},
			},
		},
	}
}

func efcTestHostPathPV() *corev1.PersistentVolume {
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-under-test"},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				HostPath: &corev1.HostPathVolumeSource{Path: "/data/migration"},
			},
		},
	}
}

// TestResolvePVCTargetDir pins the PVC source -> target_path conversion: the
// PV prefix prefers CSI, falls back to nfs.path (aligned with the field
// priority of the worker-side resolvePVCEFCPrefix); an illegal prefix degrades
// to pvcPath only under "unavailable = empty"; an illegal pvcPath itself
// ("."/".." segments) errors out.
func TestResolvePVCTargetDir(t *testing.T) {
	const (
		pvcNS   = "default"
		pvcName = "test-pvc"
		pvName  = "pv-under-test"
		ckpt    = "pod-1/123/c0/checkpoint"
	)
	pvcs := map[string]*corev1.PersistentVolumeClaim{
		pvcNS + "/" + pvcName: {
			ObjectMeta: metav1.ObjectMeta{Namespace: pvcNS, Name: pvcName},
			Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: pvName},
		},
	}

	tests := []struct {
		name    string
		pv      *corev1.PersistentVolume
		pvcPath string
		want    string
		wantErr bool
	}{
		{name: "csi prefix", pv: efcTestCSIPathPV("/migration"), pvcPath: ckpt, want: "migration/" + ckpt},
		{name: "nfs prefix fallback", pv: efcTestNFSPathPV("/export/data"), pvcPath: ckpt, want: "export/data/" + ckpt},
		{name: "csi empty path", pv: efcTestCSIPathPV(""), pvcPath: ckpt, want: ckpt},
		{name: "nfs root path", pv: efcTestNFSPathPV("/"), pvcPath: ckpt, want: ckpt},
		{name: "hostpath pv has no prefix", pv: efcTestHostPathPV(), pvcPath: ckpt, want: ckpt},
		{name: "csi escaping prefix degrades to empty", pv: efcTestCSIPathPV("/../../etc"), pvcPath: ckpt, want: ckpt},
		{name: "nfs escaping prefix degrades to empty", pv: efcTestNFSPathPV("/../etc"), pvcPath: ckpt, want: ckpt},
		{name: "user dir pvcPath passes through", pv: efcTestCSIPathPV("/data"), pvcPath: "my dir/a;b$c/checkpoint", want: "data/my dir/a;b$c/checkpoint"},
		{name: "pvcPath traversal rejected", pv: efcTestCSIPathPV("/data"), pvcPath: "../../etc", wantErr: true},
		{name: "pvcPath empty rejected", pv: efcTestCSIPathPV("/data"), pvcPath: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pvs := map[string]*corev1.PersistentVolume{}
			if tt.pv != nil {
				pvs[tt.pv.Name] = tt.pv
			}
			b := efcBackend{kubeClient: pvcPVGetClient{pvcs: pvcs, pvs: pvs}}
			got, err := b.resolvePVCTargetDir(context.Background(), &v1beta1.PreheatJobPVC{
				PvcName:      pvcName,
				PvcNamespace: pvcNS,
				PvcPath:      tt.pvcPath,
			})
			if tt.wantErr {
				if err == nil {
					t.Fatalf("resolvePVCTargetDir(pvcPath=%q) = %q, want an error", tt.pvcPath, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolvePVCTargetDir(pvcPath=%q) returned error: %v", tt.pvcPath, err)
			}
			if got != tt.want {
				t.Fatalf("resolvePVCTargetDir(pvcPath=%q) = %q, want %q", tt.pvcPath, got, tt.want)
			}
		})
	}
}
