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

package controllers

import (
	"bytes"
	"context"
	"fmt"
	"hash/fnv"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/container-migration/controller/pkg/config"
	"github.com/container-migration/controller/pkg/metrics"
	"github.com/container-migration/controller/pkg/migrationworker"
	"github.com/container-migration/controller/pkg/util"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	kubernetesscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/utils/keymutex"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// workerConfig is a self-consistent snapshot of the worker configuration. The
// three fields must be updated and read together: a torn read across fields
// ("new template + old port") would build pods from the old template but label
// them with the new hash, after which drift can never be detected again. The
// whole triple is replaced via atomic.Pointer so readers always get a
// consistent set.
type workerConfig struct {
	// template: worker pod template YAML.
	template string
	// port: worker gRPC port.
	port int
	// hash: fingerprint of template + port, used for drift detection.
	hash string
	// source: ConfigMap coordinates and version that produced this config, for troubleshooting only.
	source string
}

// workerClientEntry caches a worker gRPC client together with the port it
// dialed. The port comes from the ConfigMap; once changed, the old connection
// must be dropped and rebuilt - the gRPC connection bakes the port into the
// dial address, updating only the cached port value does not make an
// established connection switch to the new port.
type workerClientEntry struct {
	client migrationworker.Client
	port   int
}

// SharedContext provides shared state and utilities for all controllers.
type SharedContext struct {
	Config *config.Config

	Client        client.Client
	EventRecorder record.EventRecorder
	RestConfig    *rest.Config
	Clientset     kubernetes.Interface

	// APIReader reads the apiserver directly, bypassing the informer cache.
	// Used only to scan whether a node still has in-flight tasks before a
	// drift replacement - the verdict must be based on the latest state; a
	// stale read would misjudge "not busy" and kill tasks.
	APIReader client.Reader

	// workerCfg: current worker config snapshot, replaced atomically.
	workerCfg atomic.Pointer[workerConfig]

	// workerClients: worker gRPC clients cached by node IP.
	// Must be protected by the "global" workerClientsLock: workerPodLock is
	// keyed by IP and only serializes the same IP; different IPs fall on
	// different key locks yet concurrently read/write the same Go map,
	// triggering a runtime "concurrent map read and map write" - a fatal error
	// that neither recover() nor controller-runtime's RecoverPanic can catch,
	// terminating the whole process.
	//
	// The trigger is not just concurrent reconciles: restore_xpu.go starts
	// async goroutines that outlive the current reconcile, and when one of
	// them drops node B's connection via RemoveWorkerClientWithCallback on the
	// transient error branch, the reconcile goroutine may be dialing node A at
	// the same time. A single-node cluster never hits this because nodeIP is
	// always the same value (same key lock), multi-node does.
	workerClients     map[string]workerClientEntry
	workerClientsLock sync.Mutex

	// workerUsers: per-node in-flight task registry, workerUsers[nodeIP][jobUID].
	// Registered before an RPC is dispatched, covering the "just dispatched,
	// status not yet persisted" window that APIReader cannot see either.
	workerUsers     map[string]map[string]struct{}
	workerUsersLock sync.Mutex

	// taskWorkerPorts: in-memory registry of "jobUID|nodeIP -> allocated
	// port", used by handlers to get the port when connecting to task-level
	// workers. Restored from existing pods by EnsureTaskWorker after a
	// controller restart.
	taskWorkerPorts     map[string]int
	taskWorkerPortsLock sync.Mutex

	// taskWorkerPortCooldowns records per node the ports of just-deleted
	// task-level workers and their cooldown deadlines: hostNetwork ports are
	// per-node, so the cooldown must carry the node dimension (a global
	// per-port ban would wrongly block the same port that is fully idle on
	// other nodes). HostNetwork ports are only released when the container
	// process actually exits (they may stay held after the pod object is
	// gone); during cooldown the allocator skips these ports, reducing the
	// chance of a bind conflict for the new worker.
	taskWorkerPortCooldowns map[string]map[int]time.Time

	// drainVerdict: per-node busy/idle verdict cache, throttling direct-read scans by WorkerDrainPollInterval.
	drainVerdict     map[string]drainVerdictEntry
	drainVerdictLock sync.Mutex

	configLock    sync.Mutex
	workerPodLock keymutex.KeyMutex
	versionLock   sync.Mutex
}

// drainVerdictEntry is the cached result of one busy/idle verdict for a node.
type drainVerdictEntry struct {
	blocked        bool
	users          []string
	checkedAt      time.Time
	firstBlockedAt time.Time
}

// NewContext creates a new SharedContext instance.
//
// A clientset build failure is raised directly: expose the problem at startup
// (main exits non-zero) instead of running with a nil Clientset and only
// panicking at ExecInPod - the latter happens inside a reconcile goroutine
// with no recover and would drag down the whole controller process.
func NewContext(migrationConfig *config.Config, client client.Client, eventRecorder record.EventRecorder, restConfig *rest.Config) (*SharedContext, error) {
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes clientset: %w", err)
	}
	return &SharedContext{
		Config:                  migrationConfig,
		Client:                  client,
		EventRecorder:           eventRecorder,
		RestConfig:              restConfig,
		Clientset:               clientset,
		workerClients:           map[string]workerClientEntry{},
		workerUsers:             map[string]map[string]struct{}{},
		taskWorkerPorts:         map[string]int{},
		taskWorkerPortCooldowns: map[string]map[int]time.Time{},
		drainVerdict:            map[string]drainVerdictEntry{},
		workerPodLock:           keymutex.NewHashed(10000),
	}, nil
}

// workerClientKey computes the client cache key of a task-level worker
// (nodeIP:allocated port). The shared worker key stays a bare nodeIP (port
// changes are handled by the port comparison inside GetOrCreateWorkerClient);
// the two cannot collide: task-level ports are allocated from grpcPort+1 and
// never equal the shared port.
func workerClientKey(ip string, port int) string {
	return fmt.Sprintf("%s:%d", ip, port)
}

// GetOrCreateWorkerClient returns an existing worker client for the given IP, or creates a new one.
//
// The two locks each guard one concern and cannot substitute each other:
//   - workerPodLock (keyed by IP) serializes the "cache lookup -> dial -> cache
//     write" sequence of the same IP, avoiding duplicate connections to the
//     same worker;
//   - workerClientsLock (global) protects the map itself, covering concurrent
//     access across IPs.
//
// Neither dialing nor getWorkerConfig (which may read the ConfigMap
// synchronously) holds workerClientsLock, otherwise connection setup across
// all nodes would be serialized into one queue.
//
// The shared worker cache key is a bare nodeIP: when the ConfigMap changes grpcPort, the old connection is dropped by port comparison.
func (c *SharedContext) GetOrCreateWorkerClient(ctx context.Context, ip string) (result migrationworker.Client, err error) {
	c.workerPodLock.LockKey(ip)
	defer c.workerPodLock.UnlockKey(ip)

	if c.Config.Env == config.EnvTest {
		if entry, ok := c.lookupWorkerClient(ip); ok {
			return entry.client, nil
		}
		serviceClient, err := migrationworker.NewMockClient(c.Client)
		if err != nil {
			return nil, err
		}
		c.storeWorkerClient(ip, workerClientEntry{client: serviceClient})
		return serviceClient, nil
	}

	cfg, err := c.getWorkerConfig(ctx, false)
	if err != nil {
		return nil, err
	}
	if cfg.port == 0 {
		return nil, fmt.Errorf("invalid grpc port %d", cfg.port)
	}

	// Reuse the cached connection only when the port matches; when the port
	// changed (grpcPort changed in the ConfigMap), the old connection must be
	// closed and rebuilt, otherwise it keeps dialing the old port.
	if entry, ok := c.lookupWorkerClient(ip); ok {
		if entry.port == cfg.port {
			return entry.client, nil
		}
		c.removeWorkerClientLocked(ip)
	}

	serviceClient, err := migrationworker.NewDefaultClient(ip, cfg.port, c.Config.RPCTimeout)
	if err != nil {
		return nil, err
	}
	c.storeWorkerClient(ip, workerClientEntry{client: serviceClient, port: cfg.port})
	return serviceClient, nil
}

// GetOrCreateTaskWorkerClient gets the gRPC client of a task-level worker
// (PVC mode only). Cache key is nodeIP:port, which does not interfere with the
// shared worker's nodeIP:grpcPort key.
func (c *SharedContext) GetOrCreateTaskWorkerClient(ctx context.Context, ip string, port int) (migrationworker.Client, error) {
	c.workerPodLock.LockKey(ip)
	defer c.workerPodLock.UnlockKey(ip)

	if port <= 0 {
		return nil, fmt.Errorf("invalid task worker port %d", port)
	}
	key := workerClientKey(ip, port)
	if entry, ok := c.lookupWorkerClient(key); ok {
		return entry.client, nil
	}
	serviceClient, err := migrationworker.NewDefaultClient(ip, port, c.Config.RPCTimeout)
	if err != nil {
		return nil, err
	}
	c.storeWorkerClient(key, workerClientEntry{client: serviceClient, port: port})
	return serviceClient, nil
}

// WorkerClientFor returns the worker client of a task on a node: PVC mode
// goes to the task-level worker (port taken from EnsureTaskWorker's registry),
// other modes go to the shared worker.
func (c *SharedContext) WorkerClientFor(ctx context.Context, jobUID, ip string) (migrationworker.Client, error) {
	if port := c.TaskWorkerPort(jobUID, ip); port > 0 {
		return c.GetOrCreateTaskWorkerClient(ctx, ip, port)
	}
	return c.GetOrCreateWorkerClient(ctx, ip)
}

// RemoveWorkerClientForWithCallback removes the worker client cache entry of
// a task on a node and then runs the callback: PVC mode drops the task-level
// worker (nodeIP:port), other modes drop the shared worker (bare nodeIP key).
func (c *SharedContext) RemoveWorkerClientForWithCallback(jobUID, ip string, callback func() error) error {
	if port := c.TaskWorkerPort(jobUID, ip); port > 0 {
		c.workerPodLock.LockKey(ip)
		defer c.workerPodLock.UnlockKey(ip)
		c.removeWorkerClientLocked(workerClientKey(ip, port))
		return callback()
	}
	return c.RemoveWorkerClientWithCallback(ip, callback)
}

// RemoveWorkerClientWithCallback removes the worker client for the given IP and executes the callback.
//
// The shared worker cache key is a bare nodeIP: delete directly by ip.
func (c *SharedContext) RemoveWorkerClientWithCallback(ip string, callback func() error) error {
	c.workerPodLock.LockKey(ip)
	defer c.workerPodLock.UnlockKey(ip)

	c.removeWorkerClientLocked(ip)
	return callback()
}

// removeWorkerClientLocked closes and removes the cached connection of an IP;
// the caller must already hold that IP's workerPodLock (for reuse inside the
// drift state machine's locked flow, avoiding a second lock acquisition that
// would self-deadlock). The Locked in the name refers to workerPodLock; the
// map itself is protected by the lock taken inside takeWorkerClient.
func (c *SharedContext) removeWorkerClientLocked(ip string) {
	// Close is done outside the lock: it touches gRPC connection state and should not stretch the map lock hold time onto the network stack.
	if entry, ok := c.takeWorkerClient(ip); ok && entry.client != nil {
		entry.client.Close()
	}
}

// lookupWorkerClient reads a cache entry.
func (c *SharedContext) lookupWorkerClient(ip string) (workerClientEntry, bool) {
	c.workerClientsLock.Lock()
	defer c.workerClientsLock.Unlock()

	entry, ok := c.workerClients[ip]
	return entry, ok
}

// storeWorkerClient writes a cache entry.
func (c *SharedContext) storeWorkerClient(ip string, entry workerClientEntry) {
	c.workerClientsLock.Lock()
	defer c.workerClientsLock.Unlock()

	c.workerClients[ip] = entry
}

// takeWorkerClient removes and returns a cache entry; the caller closes it outside the lock.
func (c *SharedContext) takeWorkerClient(ip string) (workerClientEntry, bool) {
	c.workerClientsLock.Lock()
	defer c.workerClientsLock.Unlock()

	entry, ok := c.workerClients[ip]
	if ok {
		delete(c.workerClients, ip)
	}
	return entry, ok
}

// getWorkerConfig returns the current worker config snapshot, syncing once first when the cache is empty or a refresh is forced.
func (c *SharedContext) getWorkerConfig(ctx context.Context, forceRefresh bool) (*workerConfig, error) {
	cfg := c.workerCfg.Load()
	if cfg == nil || forceRefresh {
		if err := c.SyncConfigMap(ctx); err != nil {
			return nil, err
		}
		cfg = c.workerCfg.Load()
	}
	if cfg == nil {
		return nil, fmt.Errorf("worker config is not loaded yet")
	}
	return cfg, nil
}

// SyncConfigMap synchronizes configuration from the ConfigMap.
//
// The snapshot is atomically replaced only after every field is resolved and
// validated locally: any illegal field keeps the previous usable config -
// neither polluting the cache nor drifting the hash, avoiding "broken template
// -> delete-then-recreate -> node without worker".
func (c *SharedContext) SyncConfigMap(ctx context.Context) error {
	c.configLock.Lock()
	defer c.configLock.Unlock()

	cm := &corev1.ConfigMap{}
	err := c.Client.Get(ctx, client.ObjectKey{
		Namespace: metav1.NamespaceSystem,
		Name:      c.Config.ConfigMapName,
	}, cm)
	if err != nil {
		return err
	}

	template, ok := cm.Data[config.WorkerPodTemplateKey]
	if !ok {
		metrics.RecordConfigMapError(c.Config.ConfigMapName, config.WorkerPodTemplateKey)
		return fmt.Errorf("worker pod template key `%s` not found", config.WorkerPodTemplateKey)
	}
	// Resolve and validate: the template must parse as a PodTemplateSpec, otherwise it is an illegal config and never enters the cache.
	if _, err := util.BuildMigrationWorker("__validate__", metav1.NamespaceSystem, template, "", ""); err != nil {
		metrics.RecordConfigMapError(c.Config.ConfigMapName, config.WorkerPodTemplateKey)
		return fmt.Errorf("worker pod template is invalid: %w", err)
	}

	portStr, ok := cm.Data[config.GrpcPortKey]
	if !ok {
		metrics.RecordConfigMapError(c.Config.ConfigMapName, config.GrpcPortKey)
		return fmt.Errorf("worker pod template key `%s` not found", config.GrpcPortKey)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		metrics.RecordConfigMapError(c.Config.ConfigMapName, config.GrpcPortKey)
		return err
	}
	if port <= 0 || port > 65535 {
		metrics.RecordConfigMapError(c.Config.ConfigMapName, config.GrpcPortKey)
		return fmt.Errorf("invalid grpc port %d", port)
	}

	c.workerCfg.Store(&workerConfig{
		template: template,
		port:     port,
		hash:     workerConfigHash(template, port),
		source:   fmt.Sprintf("%s/%s@%s", cm.Namespace, cm.Name, cm.ResourceVersion),
	})
	return nil
}

// workerConfigHash fingerprints template + grpcPort (covering only these two:
// changing unrelated keys or comments in the CM must not trigger a
// cluster-wide worker replacement). Returns 16 hex digits, satisfying the
// label value length limit.
func workerConfigHash(template string, port int) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(template))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(strconv.Itoa(port)))
	return fmt.Sprintf("%016x", h.Sum64())
}

// MarkWorkerInUse registers, before dispatching an RPC, that a CR is using the worker on a node.
func (c *SharedContext) MarkWorkerInUse(nodeIP, jobUID string) {
	if nodeIP == "" || jobUID == "" {
		return
	}
	c.workerUsersLock.Lock()
	defer c.workerUsersLock.Unlock()
	if c.workerUsers[nodeIP] == nil {
		c.workerUsers[nodeIP] = map[string]struct{}{}
	}
	c.workerUsers[nodeIP][jobUID] = struct{}{}
}

// ReleaseWorkerUse releases a CR's worker-usage registration when the task reaches a terminal phase or the CR is deleted.
func (c *SharedContext) ReleaseWorkerUse(nodeIP, jobUID string) {
	if nodeIP == "" || jobUID == "" {
		return
	}
	c.workerUsersLock.Lock()
	defer c.workerUsersLock.Unlock()
	if users := c.workerUsers[nodeIP]; users != nil {
		delete(users, jobUID)
		if len(users) == 0 {
			delete(c.workerUsers, nodeIP)
		}
	}
}

// registeredWorkerUsers returns the snapshot of jobUIDs currently registered for a node in the in-memory registry.
func (c *SharedContext) registeredWorkerUsers(nodeIP string) map[string]struct{} {
	c.workerUsersLock.Lock()
	defer c.workerUsersLock.Unlock()
	src := c.workerUsers[nodeIP]
	if len(src) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(src))
	for k := range src {
		out[k] = struct{}{}
	}
	return out
}

// dropRegisteredWorkerUser cleans up registry entries confirmed stale (CR
// terminal / gone), preventing reverse leaks.
func (c *SharedContext) dropRegisteredWorkerUser(nodeIP, jobUID string) {
	c.ReleaseWorkerUse(nodeIP, jobUID)
}

// GenerateUnixTimeVersion generates a version string using the current Unix timestamp in nanoseconds.
func (c *SharedContext) GenerateUnixTimeVersion() string {
	c.versionLock.Lock()
	defer c.versionLock.Unlock()

	now := time.Now().UnixNano()
	return fmt.Sprintf("%d", now)
}

// buildWorkerPod builds the worker pod object from the current config
// snapshot (with the hash label / source annotation), for the drift state
// machine to validate and construct before deleting the old pod.
func (c *SharedContext) buildWorkerPod(ctx context.Context, nodeName string) (*corev1.Pod, *workerConfig, error) {
	cfg, err := c.getWorkerConfig(ctx, false)
	if err != nil {
		return nil, nil, err
	}
	pod, err := util.BuildMigrationWorker(nodeName, c.Config.WorkerNamespace, cfg.template, cfg.hash, cfg.source)
	if err != nil {
		return nil, nil, err
	}
	return pod, cfg, nil
}

// createWorkerPodIfAbsent creates the worker pod; already exists counts as success (idempotent).
func (c *SharedContext) createWorkerPodIfAbsent(ctx context.Context, pod *corev1.Pod) error {
	err := c.Client.Create(ctx, pod)
	if k8serrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

// ExecInPod executes a command in the specified container of a pod.
// Used to signal init containers by writing a file via exec.
func (c *SharedContext) ExecInPod(ctx context.Context, namespace, podName, containerName string, command []string) error {
	req := c.Clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: containerName,
			Command:   command,
			Stdout:    true,
			Stderr:    true,
		}, runtime.NewParameterCodec(kubernetesscheme.Scheme))

	executor, err := remotecommand.NewSPDYExecutor(c.RestConfig, "POST", req.URL())
	if err != nil {
		return fmt.Errorf("failed to create SPDY executor: %w", err)
	}

	var stdout, stderr bytes.Buffer
	err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if err != nil {
		return fmt.Errorf("exec failed (stderr: %s): %w", stderr.String(), err)
	}
	return nil
}
