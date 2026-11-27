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
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/container-migration/controller/api/v1beta1"
	"github.com/container-migration/controller/pkg/preheat"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// DefaultEFCWarmupPort is the fixed listening port of the EFC warmup service (runtimekit) on nodes.
	DefaultEFCWarmupPort = 17871

	// efcNodeLabelKey / efcNodeLabelValue: only nodes labeled
	// lj_runtimekit_enable=true deploy the EFC warmup service and are candidate
	// targets of preheat and probing.
	efcNodeLabelKey   = "lj_runtimekit_enable"
	efcNodeLabelValue = "true"

	// efcLoadAPI / efcStatAPI are the two HTTP endpoint paths of the EFC warmup service.
	efcLoadAPI = "load"
	efcStatAPI = "stat"

	// efcTargetPathParam is the shared query parameter name of both endpoints;
	// its value is the object path of the checkpoint directory inside the bucket
	// (the path part of the URL, leading "/" removed).
	efcTargetPathParam = "target_path"

	// efcLoadTimeout caps the wait for the load endpoint preheating the whole
	// directory; whether the preheat really completed is confirmed by
	// GetPreheatTask / ProbeCache via the stat endpoint. The stat probe reuses
	efcLoadTimeout = 30 * time.Second
)

// The Status enum values of a single task in the stat endpoint.
const (
	efcStatusRunning   = "running"
	efcStatusFailed    = "failed"
	efcStatusCompleted = "completed"
)

// efcWarmupResult is the JSON body of the load endpoint.
type efcWarmupResult struct {
	ErrorCode    int    `json:"ErrorCode"`
	ErrorMessage string `json:"ErrorMessage"`
}

// efcStatResult is the JSON body of the stat endpoint. The outer ErrorCode is
// the result of the whole request, Results are the per-target_path query
// results.
type efcStatResult struct {
	ErrorCode    int         `json:"ErrorCode"`
	ErrorMessage string      `json:"ErrorMessage"`
	Results      []efcResult `json:"Results"`
}

// efcResult is the query result of a single target_path in the stat body.
// Only one directory is queried each time, Results has exactly one element
// in the normal case.
type efcResult struct {
	ErrorCode    int           `json:"ErrorCode"`
	ErrorMessage string        `json:"ErrorMessage"`
	TaskInfos    []efcTaskInfo `json:"TaskInfos"`
}

// efcTaskInfo is the info of a single task in the stat body.
type efcTaskInfo struct {
	ErrorCode    int    `json:"ErrorCode"`
	ErrorMessage string `json:"ErrorMessage"`
	Status       string `json:"Status"`
	// TotalSize is the total bytes of the target path on OSS: produced after the
	// background task scans, the final value after completed (may be unstable
	// during running, no size comparison then).
	TotalSize int64 `json:"TotalSize"`
}

// errNoEFCNodes: no node labeled lj_runtimekit_enable=true (with an
// InternalIP) is available in the cluster. StartPreheat treats it as a
// normal failure going through retries; ProbeCache converts it into
// preheat.ErrNoProbeTargets, skipping this round of probing instead of
// misjudging Stale.
var errNoEFCNodes = errors.New("no node labeled " + efcNodeLabelKey + "=" + efcNodeLabelValue + " is available for EFC warmup")

// efcBackend is the EFC preheat backend: it calls the warmup HTTP endpoint of
// the node-side runtimekit and preheats the whole checkpoint directory
// directly.
//
// All differences from dragonfly are carried by the interface: a single task
// (the taskID is the directory path itself), no part discovery, no BIO
// idempotency key, no AK/SK origin credentials. The EFC cache view is shared
// cluster-wide; load / stat just pick a random candidate node per call.
type efcBackend struct {
	// kubeClient lists nodes by label and resolves checkpointUID -> remoteURL.
	kubeClient client.Client
	// httpClient calls the warmup endpoints; timeouts are controlled by the
	// per-request context uniformly.
	httpClient *http.Client
	// port is the warmup service port, default DefaultEFCWarmupPort, overridable in tests.
	port int
}

// newEFCBackend constructs the EFC backend wired to the real path.
func newEFCBackend(kubeClient client.Client) efcBackend {
	return efcBackend{
		kubeClient: kubeClient,
		httpClient: &http.Client{},
		port:       DefaultEFCWarmupPort,
	}
}

var _ preheat.PreheatBackend = efcBackend{}

func (efcBackend) Name() string { return preheat.BackendEFC }

// RequiresStorageCredentials: EFC preheats the whole directory without OSS
// presigned origin fetching, no AK/SK needed.
func (efcBackend) RequiresStorageCredentials() bool { return false }

// TaskLabel: EFC has one task for the whole directory, no part concept.
func (efcBackend) TaskLabel(int) string { return "the whole checkpoint directory" }

// StartPreheat calls the load endpoint to preheat the whole checkpoint
// directory.
//
// The taskID list has one element holding the directory path itself: the whole
// directory is one EFC task; this dir is both the taskID polled by
// GetPreheatTask and the target_path probed by ProbeCache. After load returns
// successfully, whether the preheat really completed is confirmed by
// GetPreheatTask querying the stat endpoint (Probing is only entered after
// completed).
func (b efcBackend) StartPreheat(ctx context.Context, req preheat.StartPreheatRequest) ([]string, error) {
	ctxLog := log.FromContext(ctx)

	dir, err := b.resolveTargetDir(ctx, req.Job, req.RemoteURL)
	if err != nil {
		return nil, err
	}
	ctxLog.V(1).Info(fmt.Sprintf("PreheatJob %s/%s EFC warmup load starting for %s", req.Job.Namespace, req.Job.Name, dir), "backend", b.Name())
	node, nodeIP, err := b.pickWarmupNode(ctx)
	if err != nil {
		return nil, err
	}

	loadCtx, cancel := context.WithTimeout(ctx, efcLoadTimeout)
	defer cancel()

	var result efcWarmupResult
	if err := b.callWarmupAPI(loadCtx, efcLoadAPI, nodeIP, dir, &result); err != nil {
		return nil, fmt.Errorf("failed to call EFC warmup %s on node %s for %s: %v", efcLoadAPI, node.Name, dir, err)
	}
	if result.ErrorCode != 0 {
		return nil, fmt.Errorf("EFC warmup %s on node %s for %s returned error code %d: %s",
			efcLoadAPI, node.Name, dir, result.ErrorCode, result.ErrorMessage)
	}

	ctxLog.Info(fmt.Sprintf("PreheatJob %s/%s EFC warmup load completed on node %s for %s",
		req.Job.Namespace, req.Job.Name, node.Name, dir), "backend", b.Name())
	return []string{dir}, nil
}

// GetPreheatTask and ProbeCache query the same stat endpoint; the only
// difference is that the input is the taskID directly (the EFC taskID is the
// directory path itself), and the stat Status is mapped into task lifecycle
// states returned to handleRunning to advance the state machine.
func (b efcBackend) GetPreheatTask(ctx context.Context, taskID string) (preheat.Task, error) {
	logger := log.FromContext(ctx)

	node, nodeIP, err := b.pickWarmupNode(ctx)
	if err != nil {
		return preheat.Task{}, err
	}

	statCtx, cancel := context.WithTimeout(ctx, DefaultProbeTimeout)
	defer cancel()

	var stat efcStatResult
	if err := b.callWarmupAPI(statCtx, efcStatAPI, nodeIP, taskID, &stat); err != nil {
		return preheat.Task{}, fmt.Errorf("failed to call EFC warmup %s on node %s for %s: %v", efcStatAPI, node.Name, taskID, err)
	}
	if stat.ErrorCode != 0 {
		return preheat.Task{}, fmt.Errorf("EFC warmup %s on node %s for %s returned error code %d: %s",
			efcStatAPI, node.Name, taskID, stat.ErrorCode, stat.ErrorMessage)
	}

	return efcTaskState(taskID, efcCacheState(taskID, stat.Results, logger), stat.Results), nil
}

// ProbeCache calls the stat endpoint to query the cache state of the whole
// directory.
//
// A random candidate node is picked per query (the EFC cache view is shared
// cluster-wide); the returned PeerList only has this one node: Cached means
// this node confirmed the directory cache completed.
func (b efcBackend) ProbeCache(ctx context.Context, job *v1beta1.PreheatJob) ([]v1beta1.PreheatPeerInfo, []preheat.CacheState, error) {
	logger := log.FromContext(ctx)
	now := metav1.Now()

	dir, err := b.resolveTargetDir(ctx, job, job.Spec.RemoteURL)
	if err != nil {
		return nil, nil, err
	}

	node, nodeIP, err := b.pickWarmupNode(ctx)
	if errors.Is(err, errNoEFCNodes) {
		// No probeable node for now: skip this round and probe later, no misjudged Stale.
		return nil, nil, preheat.ErrNoProbeTargets
	}
	if err != nil {
		return nil, nil, err
	}

	statCtx, cancel := context.WithTimeout(ctx, DefaultProbeTimeout)
	defer cancel()

	var stat efcStatResult
	if err := b.callWarmupAPI(statCtx, efcStatAPI, nodeIP, dir, &stat); err != nil {
		return nil, nil, fmt.Errorf("failed to call EFC warmup %s on node %s for %s: %v", efcStatAPI, node.Name, dir, err)
	}
	if stat.ErrorCode != 0 {
		return nil, nil, fmt.Errorf("EFC warmup %s on node %s for %s returned error code %d: %s",
			efcStatAPI, node.Name, dir, stat.ErrorCode, stat.ErrorMessage)
	}

	state := efcCacheState(dir, stat.Results, logger)
	peer := v1beta1.PreheatPeerInfo{
		Cached:   state == preheat.CacheStateCached,
		LastSeen: &now,
	}
	return []v1beta1.PreheatPeerInfo{peer}, []preheat.CacheState{state}, nil
}

// efcFirstTaskInfo takes the only task info of interest from the stat body.
//
// The stat result has two layers: Results[i] corresponds to one target_path,
// inside it TaskInfos is the task itself. Only one directory is queried each
// time, normally both layers have one element, take the first. A non-zero
// directory-level error code is carried out via efcTaskInfo so the caller goes
// down the same failure branch; ok=false means this directory has no task info
// to judge.
func efcFirstTaskInfo(results []efcResult) (efcTaskInfo, bool) {
	if len(results) == 0 {
		return efcTaskInfo{}, false
	}
	result := results[0]
	if result.ErrorCode != 0 {
		return efcTaskInfo{ErrorCode: result.ErrorCode, ErrorMessage: result.ErrorMessage}, true
	}
	if len(result.TaskInfos) == 0 {
		return efcTaskInfo{}, false
	}
	return result.TaskInfos[0], true
}

// efcCacheState maps the single task info from stat into the abstract cache
// state: completed -> Cached, running -> Running, failed -> Failed; a non-zero
// directory-level / task-level error code counts as Failed. When no task can
// be found yet, keep polling as Running to avoid misjudging the brief window
// where load is accepted but stat has not registered the task; an
// unrecognizable Status counts as NotFound.
func efcCacheState(dir string, results []efcResult, logger logr.Logger) preheat.CacheState {
	info, ok := efcFirstTaskInfo(results)
	if !ok {
		return preheat.CacheStateRunning
	}
	if info.ErrorCode != 0 {
		logger.Info("EFC warmup stat reported a task-level error",
			"targetPath", dir, "errorCode", info.ErrorCode, "errorMessage", info.ErrorMessage)
		return preheat.CacheStateFailed
	}
	switch {
	case strings.EqualFold(info.Status, efcStatusCompleted):
		return preheat.CacheStateCached
	case strings.EqualFold(info.Status, efcStatusRunning):
		return preheat.CacheStateRunning
	case strings.EqualFold(info.Status, efcStatusFailed):
		return preheat.CacheStateFailed
	default:
		logger.Info("EFC warmup stat returned an unknown status",
			"targetPath", dir, "status", info.Status)
		return preheat.CacheStateNotFound
	}
}

// efcTaskState maps the cache state from stat into the task lifecycle state
// for handleRunning to advance the state machine: Cached -> Success; Running
// -> Running (not fully preheated yet, keep polling); Failed / NotFound ->
// Failure (NotFound means the directory was never registered after load or was
// evicted; polling on would wait forever, fail directly with the reason).
func efcTaskState(taskID string, state preheat.CacheState, results []efcResult) preheat.Task {
	info, ok := efcFirstTaskInfo(results)
	switch state {
	case preheat.CacheStateCached:
		// Carry TotalSize out after completed: the data-integrity comparison
		// happens in handleRunning (against the total size recorded by the
		// checkpoint CR), not judged inside the backend.
		total := int64(0)
		if ok {
			total = info.TotalSize
		}
		return preheat.Task{ID: taskID, State: preheat.TaskStateSuccess, TotalSize: total}
	case preheat.CacheStateRunning:
		return preheat.Task{ID: taskID, State: preheat.TaskStateRunning}
	default:
		message := fmt.Sprintf("EFC warmup task for %s is not cached", taskID)
		if ok && info.ErrorMessage != "" {
			message = info.ErrorMessage
		}
		return preheat.Task{ID: taskID, State: preheat.TaskStateFailure, Message: message}
	}
}

// resolveTargetDir computes the target_path parameter of the EFC warmup
// endpoint.
//
// PVC source (spec.pvc or pvc resolved from checkpointUID): EFC relative path
// = the path prefix on the PV (csi.volumeAttributes["path"] first, nfs.path
// fallback; empty when unavailable or illegal) + pvcPath; the PVC type is not
// judged and no bucket comparison, whether the path exists in the EFC mount is
// guarded by the warmup error.
//
// OSS source, same as the probing path: when checkpointUID is non-empty
// prefer computing remoteURL from the checkpoint CR, otherwise use the caller's
// fallbackURL directly; then take the path part of the URL and strip the
// leading "/" - EFC locates the checkpoint directory by the object path inside
// the bucket, indifferent to host and signature parameters. E.g.
//
//	https://bucket.oss-cn-hongkong-internal.aliyuncs.com/chanke/migration-worker/xxx/123/pod0
//	-> chanke/migration-worker/xxx/123/pod0
func (b efcBackend) resolveTargetDir(ctx context.Context, job *v1beta1.PreheatJob, fallbackURL string) (string, error) {
	var pvcSrc *v1beta1.PreheatJobPVC
	if job.Spec.Pvc != nil {
		pvcSrc = job.Spec.Pvc
	}
	remoteURL := fallbackURL
	if job.Spec.CheckpointUID != "" {
		src, err := resolveSourceFromCheckpoint(ctx, b.kubeClient, job)
		if err != nil {
			return "", fmt.Errorf("failed to resolve preheat source from checkpoint %s: %v", job.Spec.CheckpointUID, err)
		}
		if src.IsPVC() {
			pvcSrc = src.PVC()
		} else {
			remoteURL = src.remoteURL
		}
	}
	if pvcSrc != nil {
		return b.resolvePVCTargetDir(ctx, pvcSrc)
	}
	if remoteURL == "" {
		return "", fmt.Errorf("PreheatJob %s/%s has neither remoteURL nor checkpointUID to resolve from",
			job.Namespace, job.Name)
	}
	return efcTargetDir(remoteURL)
}

// resolvePVCTargetDir resolves a PVC source into the relative path under the
// EFC mount: the path prefix on the PV (taken when available, CSI path first,
// nfs.path fallback) + pvcPath.
func (b efcBackend) resolvePVCTargetDir(ctx context.Context, pvcSrc *v1beta1.PreheatJobPVC) (string, error) {
	if pvcSrc.PvcName == "" || pvcSrc.PvcNamespace == "" {
		return "", fmt.Errorf("pvc.pvcName and pvc.pvcNamespace are required")
	}
	// pvcPath structural validation (same as the handlePending entry): the
	// probing path of ProbeController also re-resolves the pvc source via
	// resolveTargetDir and is not on the mandatory path of handlePending;
	// validate again here so both entries carry the same constraint.
	pvcPath, err := validRelPath(pvcSrc.PvcPath)
	if err != nil {
		return "", fmt.Errorf("invalid pvc.pvcPath %q: %v", pvcSrc.PvcPath, err)
	}
	if pvcPath == "" {
		return "", fmt.Errorf("pvc.pvcPath is required")
	}
	// Get PVC/PV goes through informer lazy loading: with missing permissions
	// the cache never syncs, and a bare reconcile ctx would block the whole
	// preheat path forever. A short timeout turns such failures into
	// retryable errors (retryOrFail counts and fails).
	resolveCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	pvc := &corev1.PersistentVolumeClaim{}
	if err := b.kubeClient.Get(resolveCtx, client.ObjectKey{
		Namespace: pvcSrc.PvcNamespace,
		Name:      pvcSrc.PvcName,
	}, pvc); err != nil {
		return "", fmt.Errorf("failed to get PVC %s/%s: %v", pvcSrc.PvcNamespace, pvcSrc.PvcName, err)
	}
	// The PV may carry a path prefix (the PVC mount root is a sub-path inside
	// the bucket/export directory, e.g. chanke/migration-worker); prepend it
	// to pvcPath when available. When unavailable (hostPath/local with
	// neither field) or illegal, the prefix is empty.
	pvPrefix := ""
	if pvc.Spec.VolumeName != "" {
		pv := &corev1.PersistentVolume{}
		if err := b.kubeClient.Get(resolveCtx, client.ObjectKey{Name: pvc.Spec.VolumeName}, pv); err != nil {
			return "", fmt.Errorf("failed to get PV %s for PVC %s/%s: %v",
				pvc.Spec.VolumeName, pvcSrc.PvcNamespace, pvcSrc.PvcName, err)
		}
		pvPrefix = pvPathPrefix(ctx, pvc.Spec.VolumeName, pv)
	}
	if pvPrefix == "" {
		return pvcPath, nil
	}
	return pvPrefix + "/" + pvcPath, nil
}

// pvPathPrefix takes the field of the PV expressing "the PVC mount root is a
// sub-path inside the shared storage" and validates it structurally, keeping
// the same field priority as pvEFCPrefix in the worker-side pkg/storage:
// csi.volumeAttributes["path"] first, otherwise nfs.path; hostPath/local with
// neither field yields empty. With both sides aligned, NAS / traditional NFS
// PVCs compose the same prefix; otherwise the preheat side misses the prefix
// while the restore side has it and the two read different locations.
//
// Illegal values (containing "."/".." segments or backslashes) are logged and
// return empty instead of an error - the PV is a cluster-scoped object, its
// path is written by static yaml or the CSI provisioner, and K8s performs no
// validation on volumeAttributes values; treating "unavailable = empty" lets
// target_path degrade to pvcPath itself, the missing path is guarded by the
// warmup load "get file meta error: -2" fallback, the preheat task is not
// failed here (isomorphic with the worker-side "no error, degrade only"
// contract).
func pvPathPrefix(ctx context.Context, pvName string, pv *corev1.PersistentVolume) string {
	logger := log.FromContext(ctx)
	if pv.Spec.CSI != nil {
		raw := pv.Spec.CSI.VolumeAttributes["path"]
		p, err := validRelPath(raw)
		if err == nil {
			return p
		}
		logger.Info("Ignoring invalid csi.volumeAttributes[path] of PV as preheat prefix, will use pvcPath only",
			"pv", pvName, "path", raw, "error", err)
	}
	if pv.Spec.NFS != nil {
		p, err := validRelPath(pv.Spec.NFS.Path)
		if err == nil {
			return p
		}
		logger.Info("Ignoring invalid nfs.path of PV as preheat prefix, will use pvcPath only",
			"pv", pvName, "path", pv.Spec.NFS.Path, "error", err)
	}
	return ""
}

// efcTargetDir takes the object path EFC needs from a full URL (leading "/" removed).
func efcTargetDir(remoteURL string) (string, error) {
	u, err := url.Parse(remoteURL)
	if err != nil {
		return "", fmt.Errorf("invalid remoteURL %q: %v", remoteURL, err)
	}
	dir := strings.TrimPrefix(u.Path, "/")
	if dir == "" {
		return "", fmt.Errorf("remoteURL %q carries no object path", remoteURL)
	}
	return dir, nil
}

// pickWarmupNode lists nodes labeled lj_runtimekit_enable=true and randomly
// picks one with an InternalIP as the call target of this warmup round.
func (b efcBackend) pickWarmupNode(ctx context.Context) (*corev1.Node, string, error) {
	ctxLog := log.FromContext(ctx)
	nodes := &corev1.NodeList{}
	if err := b.kubeClient.List(ctx, nodes,
		client.MatchingLabels{efcNodeLabelKey: efcNodeLabelValue}); err != nil {
		return nil, "", fmt.Errorf("failed to list EFC warmup nodes: %v", err)
	}
	ctxLog.V(1).Info(fmt.Sprintf("Found %d EFC warmup nodes", len(nodes.Items)), "backend", b.Name())

	type candidate struct {
		node *corev1.Node
		ip   string
	}
	candidates := make([]candidate, 0, len(nodes.Items))
	for i := range nodes.Items {
		ctxLog.V(1).Info(fmt.Sprintf("Considering node %s for EFC warmup", nodes.Items[i].Name), "backend", b.Name())
		if ip := nodeInternalIP(&nodes.Items[i]); ip != "" {
			candidates = append(candidates, candidate{node: &nodes.Items[i], ip: ip})
		}
	}
	if len(candidates) == 0 {
		return nil, "", errNoEFCNodes
	}

	picked := candidates[rand.Intn(len(candidates))]
	return picked.node, picked.ip, nil
}

// nodeInternalIP returns the InternalIP of a node, empty string when absent.
func nodeInternalIP(node *corev1.Node) string {
	for _, addr := range node.Status.Addresses {
		if addr.Type == corev1.NodeInternalIP && addr.Address != "" {
			return addr.Address
		}
	}
	return ""
}

// callWarmupAPI GET http://{ip}:{port}/v1/warmup/{api}?target_path={dir}
// and decodes the JSON body into out.
func (b efcBackend) callWarmupAPI(ctx context.Context, api, nodeIP, dir string, out any) error {
	query := url.Values{}
	query.Set(efcTargetPathParam, dir)
	endpoint := fmt.Sprintf("http://%s/v1/warmup/%s?%s",
		net.JoinHostPort(nodeIP, strconv.Itoa(b.port)), api, query.Encode())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	resp, err := b.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("read response from %s: %v", endpoint, err)
	}
	ctxLog := log.FromContext(ctx)
	ctxLog.V(1).Info(fmt.Sprintf("call efc warmup API  get response %v", string(body)))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %s from %s: %s", resp.Status, endpoint, strings.TrimSpace(string(body)))
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("decode response from %s: %v", endpoint, err)
	}
	return nil
}
