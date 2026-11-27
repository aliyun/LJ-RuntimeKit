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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/container-migration/controller/api/v1beta1"
	"github.com/container-migration/controller/pkg/labels"
	"github.com/container-migration/controller/pkg/metrics"
	"github.com/container-migration/controller/pkg/util"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// WorkerEnsureState is the three-state conclusion of EnsureWorkerUpToDate.
type WorkerEnsureState int

const (
	// WorkerReady: the target node already runs a worker matching the current
	// config and Running+Ready, tasks can be dispatched directly.
	WorkerReady WorkerEnsureState = iota
	// WorkerWaiting: not dispatchable yet (creating / waiting for ready /
	// waiting for the old worker to drain or terminate), the caller should retry.
	WorkerWaiting
	// WorkerFailed: a deterministic error (invalid config, delete failure etc.),
	// the caller should fail the task or return the error.
	WorkerFailed
)

// WorkerEnsureResult is the result of EnsureWorkerUpToDate.
type WorkerEnsureResult struct {
	State WorkerEnsureState
	// Blocked=true means the Waiting reason is "config drift + node still busy",
	// i.e. blocked by in-flight tasks; it is distinguished from the ordinary
	// "creating / waiting for ready" waits to decide whether to write the
	// WorkerBlocked condition.
	Blocked bool
	// Users are the blocking task identifiers (namespace/name, sorted and
	// deduplicated), only meaningful when Blocked.
	Users []string
	// Reason is a stable human-readable reason (no timestamps / counters, to
	// avoid a write storm from per-round changes).
	Reason string
	// Err is the error when State==WorkerFailed.
	Err error
}

// destBusyPhases is the set of destination phases that mark "the node still has
// in-flight tasks" on the restore side. SnapshotPrepared / InitReleased data
// is already on hostPath (restarting the worker does not lose it), but the
// worker's in-memory task table would be lost, breaking WaitRestore validation
// and CleanTask, so they are conservatively counted as busy.
var destBusyPhases = []v1beta1.JobPhase{
	v1beta1.DestPhaseScheduled,
	v1beta1.DestPhaseSnapshotPrepared,
	v1beta1.DestPhaseInitReleased,
	v1beta1.DestPhaseXPURestoring,
}

// EnsureWorkerUpToDate makes sure the target node runs a worker matching the
// current worker config; it is the only entry before dispatching a task.
// allowReplace controls whether replacement is allowed on config drift:
//   - true (task not yet dispatched to the node): drift + idle node means delete
//     the old worker and rebuild; a busy node returns Blocked and waits;
//   - false (task already dispatched, inspection only): never replace, only
//     guarantee the worker exists and is usable.
//
// The whole check->delete->create flow holds a per-nodeIP lock to serialize
// concurrent reconciles for the same node.
func (c *SharedContext) EnsureWorkerUpToDate(ctx context.Context, nodeName, nodeIP, jobUID string,
	allowReplace bool) WorkerEnsureResult {
	ctxLog := log.FromContext(ctx)

	c.workerPodLock.LockKey(nodeIP)
	defer c.workerPodLock.UnlockKey(nodeIP)

	// Build and validate the desired pod from the current config snapshot
	// first: invalid config fails before anything is deleted.
	desired, cfg, err := c.buildWorkerPod(ctx, nodeName)
	if err != nil {
		return WorkerEnsureResult{State: WorkerFailed, Err: err, Reason: "worker config invalid"}
	}

	existing := &corev1.Pod{}
	getErr := c.Client.Get(ctx, client.ObjectKey{
		Namespace: c.Config.WorkerNamespace,
		Name:      util.GenerateMigrationWorkerName(nodeName),
	}, existing)
	if k8serrors.IsNotFound(getErr) {
		if err := c.createWorkerPodIfAbsent(ctx, desired); err != nil {
			return WorkerEnsureResult{State: WorkerFailed, Err: err, Reason: "create worker pod failed"}
		}
		return WorkerEnsureResult{State: WorkerWaiting, Reason: "creating worker pod"}
	}
	if getErr != nil {
		// A read failure is treated as a transient condition, wait and retry
		// (do not fail the task on a single blip).
		return WorkerEnsureResult{State: WorkerWaiting, Reason: "cannot read worker pod, will retry"}
	}

	// Old pod terminating: only wait, never force delete (after a force delete
	// the node-side process still holds the port and the new pod cannot start).
	if existing.DeletionTimestamp != nil {
		return WorkerEnsureResult{State: WorkerWaiting, Reason: "waiting for old worker to terminate"}
	}

	currentHash := existing.Labels[labels.LabelWorkerConfigHash]
	if currentHash == cfg.hash {
		// Config matches: only check readiness.
		if podRunningReady(existing) {
			return WorkerEnsureResult{State: WorkerReady, Reason: "worker ready"}
		}
		return WorkerEnsureResult{State: WorkerWaiting, Reason: "waiting for worker to be ready"}
	}

	// Config drift (including legacy pods without the hash label:
	// currentHash=="" != cfg.hash).
	if !allowReplace {
		// Inspection path: no replacement, just guarantee usability.
		if podRunningReady(existing) {
			return WorkerEnsureResult{State: WorkerReady, Reason: "worker present (stale config, replace deferred)"}
		}
		return WorkerEnsureResult{State: WorkerWaiting, Reason: "worker present (stale config), waiting for ready"}
	}

	// Replacement allowed: first decide whether the node still has in-flight
	// tasks.
	users, err := c.WorkerBusyUsers(ctx, nodeIP)
	if err != nil {
		// Conservatively wait when busy state cannot be determined; never
		// delete a worker while tasks may exist.
		return WorkerEnsureResult{State: WorkerWaiting, Reason: "cannot determine node busy state, will retry"}
	}
	if len(users) > 0 {
		metrics.SetWorkerReplaceBlocked(nodeIP, true)
		return WorkerEnsureResult{
			State:   WorkerWaiting,
			Blocked: true,
			Users:   users,
			Reason:  fmt.Sprintf("node=%s stale worker draining", nodeIP),
		}
	}

	// Node idle: drop the old connection + delete the old pod, the next round
	// rebuilds.
	metrics.SetWorkerReplaceBlocked(nodeIP, false)
	c.removeWorkerClientLocked(nodeIP)
	if err := c.Client.Delete(ctx, existing); err != nil && !k8serrors.IsNotFound(err) {
		metrics.RecordWorkerReplace(nodeIP, "delete_error")
		return WorkerEnsureResult{State: WorkerFailed, Err: err, Reason: "delete stale worker failed"}
	}
	c.invalidateDrainVerdict(nodeIP)
	metrics.RecordWorkerReplace(nodeIP, "replaced")
	ctxLog.Info("Deleted stale worker for config drift, will recreate",
		"node", nodeName, "nodeIP", nodeIP, "oldHash", currentHash, "newHash", cfg.hash)
	return WorkerEnsureResult{State: WorkerWaiting, Reason: "replacing stale worker pod"}
}

// podRunningReady reports whether the pod is Running and all containers are
// ready.
func podRunningReady(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == corev1.PodReady {
			return pod.Status.Conditions[i].Status == corev1.ConditionTrue
		}
	}
	return false
}

// WorkerBusyUsers returns the identifiers (namespace/name, sorted and
// deduplicated) of CRs that still have in-flight tasks on a node. The verdict
// comes from the union of "CR list read directly via APIReader" and "the
// in-memory in-flight registry", and is cached for WorkerDrainPollInterval so
// the 1s-per-round restore requeue does not turn the direct LIST into one per
// second.
//
// The result is caller-agnostic (does not exclude self): at this control point
// of the replacement check, the self task has not been dispatched to the node,
// so it appears neither in the busy set of the CR scan nor in the registry, and
// can never misjudge itself as a blocker.
func (c *SharedContext) WorkerBusyUsers(ctx context.Context, nodeIP string) ([]string, error) {
	c.drainVerdictLock.Lock()
	if entry, ok := c.drainVerdict[nodeIP]; ok && time.Since(entry.checkedAt) < c.Config.WorkerDrainPollInterval {
		users := entry.users
		c.drainVerdictLock.Unlock()
		return users, nil
	}
	c.drainVerdictLock.Unlock()

	users, err := c.scanWorkerBusyUsers(ctx, nodeIP)
	if err != nil {
		return nil, err
	}

	c.drainVerdictLock.Lock()
	firstBlockedAt := c.drainVerdict[nodeIP].firstBlockedAt
	switch {
	case len(users) > 0 && firstBlockedAt.IsZero():
		firstBlockedAt = time.Now()
	case len(users) == 0:
		firstBlockedAt = time.Time{}
	}
	c.drainVerdict[nodeIP] = drainVerdictEntry{
		blocked:        len(users) > 0,
		users:          users,
		checkedAt:      time.Now(),
		firstBlockedAt: firstBlockedAt,
	}
	c.drainVerdictLock.Unlock()
	return users, nil
}

// scanWorkerBusyUsers computes the in-flight task set of a node by directly
// reading the CR list and merging the in-memory registry; it also cleans up
// reverse-leak registry entries confirmed stale (CR terminal / gone).
func (c *SharedContext) scanWorkerBusyUsers(ctx context.Context, nodeIP string) ([]string, error) {
	list := &v1beta1.ContainerStateMigrationJobRequestList{}
	if err := c.APIReader.List(ctx, list); err != nil {
		return nil, err
	}

	byUID := make(map[string]*v1beta1.ContainerStateMigrationJobRequest, len(list.Items))
	busy := map[string]string{} // uid -> namespace/name
	for i := range list.Items {
		cr := &list.Items[i]
		byUID[string(cr.UID)] = cr
		if isTerminalJobPhase(cr.Status.Phase) {
			continue
		}
		if crUsesNode(cr, nodeIP) {
			busy[string(cr.UID)] = cr.Namespace + "/" + cr.Name
		}
	}

	// Merge the in-memory registry: covers the "just dispatched, status not yet
	// landed" window; also cleans up reverse leaks.
	for uid := range c.registeredWorkerUsers(nodeIP) {
		if _, ok := busy[uid]; ok {
			continue
		}
		cr, found := byUID[uid]
		if !found || isTerminalJobPhase(cr.Status.Phase) {
			// CR deleted or terminal: the registry entry is stale, drop it.
			c.dropRegisteredWorkerUser(nodeIP, uid)
			continue
		}
		// CR still non-terminal: in-flight window, conservatively count busy
		// (better to wait longer).
		busy[uid] = cr.Namespace + "/" + cr.Name
	}

	out := make([]string, 0, len(busy))
	for _, disp := range busy {
		out = append(out, disp)
	}
	sort.Strings(out)
	return out, nil
}

// crUsesNode reports whether a CR currently uses the worker on the given node.
func crUsesNode(cr *v1beta1.ContainerStateMigrationJobRequest, nodeIP string) bool {
	if cs := cr.Status.CheckpointStatus; cs != nil && cs.SourceNodeIP == nodeIP {
		// Only an ongoing checkpoint (Phase==Checkpointing) really occupies the
		// node worker. CheckpointInvoked=True alone is not enough: the condition
		// stays True after completion, and a restore-only / full-migration CR
		// carries a Succeeded checkpointStatus (restore-only inherits it from
		// the referenced checkpoint) whose SourceNodeIP happens to equal the
		// restore target node in single-node scenarios - judging by
		// CheckpointInvoked alone would misjudge the CR as checkpointing on
		// that node, blocking its own restore into a self-deadlock.
		if cs.Phase == v1beta1.JobPhaseCheckpointing &&
			util.AssertConditionInStatus(cs.Conditions, v1beta1.RequestConditionCheckpointInvoked, metav1.ConditionTrue) {
			return true
		}
	}
	if rs := cr.Status.RestoreStatus; rs != nil {
		for i := range rs.Destinations {
			d := &rs.Destinations[i]
			if d.NodeIP == nodeIP && util.AssertJobPhaseIn(d.Phase, destBusyPhases...) {
				return true
			}
		}
	}
	return false
}

// isTerminalJobPhase reports whether the job is in a terminal phase (terminal
// jobs occupy no worker).
func isTerminalJobPhase(phase v1beta1.JobPhase) bool {
	return util.AssertJobPhaseIn(phase, v1beta1.JobPhaseSucceeded, v1beta1.JobPhasePartiallySucceeded,
		v1beta1.JobPhaseFailed, v1beta1.JobPhaseTerminated)
}

// invalidateDrainVerdict clears the busy-state verdict cache of a node, forcing
// a rescan on the next round after a replacement action.
func (c *SharedContext) invalidateDrainVerdict(nodeIP string) {
	c.drainVerdictLock.Lock()
	delete(c.drainVerdict, nodeIP)
	c.drainVerdictLock.Unlock()
}

// FormatWorkerBlockedMessage generates the stable text of the WorkerBlocked
// condition / status: blockers are sorted, at most the first 3 are listed with
// the total appended, so the same blocked state produces the exact same string
// every round and LastTransitionTime does not refresh per round into an etcd
// write storm.
func FormatWorkerBlockedMessage(nodeIP string, users []string) string {
	const maxShown = 3
	shown := users
	suffix := ""
	if len(users) > maxShown {
		shown = users[:maxShown]
		suffix = fmt.Sprintf(" (+%d more)", len(users)-maxShown)
	}
	return fmt.Sprintf("node=%s blockedBy=%s%s", nodeIP, strings.Join(shown, ","), suffix)
}

// ===================== Task-level worker (PVC direct-attach mode) =====================
//
// A PVC-mode task starts its own dedicated worker (task-level worker) on every
// node it involves. Key differences from the shared worker
// (EnsureWorkerUpToDate):
//   - the pod is created in the PVC namespace (K8s constraint: a pod can only
//     mount PVCs from its own namespace);
//   - the name carries a task suffix lj-runtimekit-migration-worker-<nodeName>-<jobUID8>,
//     multiple tasks coexist on the same node;
//   - the port is dynamically allocated from grpcPort+1 (grpcPort is always
//     reserved for the shared worker);
//   - mounts the task PVC at the fixed path /mnt/pv.
//
// The task-level worker lifecycle follows the task phase (see DeleteTaskWorker
// callers): checkpoint completion deletes the source-node worker, restore
// completion deletes the target-node worker, CR deletion cleans up leftovers.

const (
	// pvStorageVolumeName is the volume name mounting the task PVC in the
	// task-level worker.
	pvStorageVolumeName = "pv-storage"
	// PVCMountPoint is the fixed mount point of the task PVC in the task-level
	// worker and the preheat source pod, kept in sync with pvcMountPoint
	// (/mnt/pv) in the worker-side storage package. Exported for the preheatjob
	// package pre-read.
	PVCMountPoint = "/mnt/pv"
	// taskWorkerPortCooldown is the port cooldown period after a task-level
	// worker is deleted: a hostNetwork port is only released when the container
	// process actually exits; within the cooldown the allocator skips the port
	// (combined with the worker-side bind retry self-healing, a double guard
	// against port collision when a new worker starts).
	taskWorkerPortCooldown = 60 * time.Second
)

// EnsureTaskWorker makes sure the target node runs the task-dedicated PVC-mode
// worker; it is the only entry for worker readiness before a PVC task is
// dispatched. The whole check->allocate->create flow holds a per-nodeIP lock,
// serialized with the shared worker drift replacement under the same lock so
// port scans never trample each other.
func (c *SharedContext) EnsureTaskWorker(ctx context.Context, nodeName, nodeIP, jobUID string,
	pvc *v1beta1.JobPVC) WorkerEnsureResult {
	ctxLog := log.FromContext(ctx)

	c.workerPodLock.LockKey(nodeIP)
	defer c.workerPodLock.UnlockKey(nodeIP)

	cfg, err := c.getWorkerConfig(ctx, false)
	if err != nil {
		return WorkerEnsureResult{State: WorkerFailed, Err: err, Reason: "worker config invalid"}
	}

	name := util.GenerateTaskMigrationWorkerName(nodeName, jobUID)
	namespace := pvc.PvcNamespace

	existing := &corev1.Pod{}
	getErr := c.Client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, existing)
	if k8serrors.IsNotFound(getErr) {
		// First creation: allocate a port and inject the port arg and PVC mount.
		port, err := c.allocateTaskWorkerPort(ctx, nodeIP, cfg.port)
		if err != nil {
			return WorkerEnsureResult{State: WorkerFailed, Err: err, Reason: "allocate worker port failed"}
		}
		desired, err := c.buildTaskWorkerPod(ctx, nodeName, jobUID, pvc, port, cfg)
		if err != nil {
			return WorkerEnsureResult{State: WorkerFailed, Err: err, Reason: "worker config invalid"}
		}
		if err := c.createWorkerPodIfAbsent(ctx, desired); err != nil {
			return WorkerEnsureResult{State: WorkerFailed, Err: err, Reason: "create task worker pod failed"}
		}
		c.storeTaskWorkerPort(jobUID, nodeIP, port)
		ctxLog.Info("Created task worker pod", "node", nodeName, "nodeIP", nodeIP,
			"namespace", namespace, "port", port)
		return WorkerEnsureResult{State: WorkerWaiting, Reason: "creating task worker pod"}
	}
	if getErr != nil {
		// A read failure is treated as a transient condition, wait and retry
		// (do not fail the task on a single blip).
		return WorkerEnsureResult{State: WorkerWaiting, Reason: "cannot read task worker pod, will retry"}
	}

	// Pod exists: the port is taken from the existing pod's actual args (after
	// a controller restart the registry is restored from it).
	if port, ok := parseWorkerGRPCPort(&existing.Spec); ok {
		c.storeTaskWorkerPort(jobUID, nodeIP, port)
	}

	// Old pod terminating: only wait, never force delete (after a force delete
	// the node-side process still holds the port and the new pod cannot start).
	if existing.DeletionTimestamp != nil {
		return WorkerEnsureResult{State: WorkerWaiting, Reason: "waiting for old task worker to terminate"}
	}

	// A task-level worker is always newly created with the latest config and
	// has a short lifecycle; no drift replacement, only check readiness.
	if podRunningReady(existing) {
		return WorkerEnsureResult{State: WorkerReady, Reason: "task worker ready"}
	}
	return WorkerEnsureResult{State: WorkerWaiting, Reason: "waiting for task worker to be ready"}
}

// buildTaskWorkerPod builds the task-level worker pod from the current config
// snapshot: renames it, switches the namespace, injects the port arg and PVC
// mount. The shared worker template (image/mounts/privileges) is fully reused.
func (c *SharedContext) buildTaskWorkerPod(ctx context.Context, nodeName, jobUID string,
	pvc *v1beta1.JobPVC, port int, cfg *workerConfig) (*corev1.Pod, error) {
	pod, err := util.BuildMigrationWorker(nodeName, pvc.PvcNamespace, cfg.template, cfg.hash, cfg.source)
	if err != nil {
		return nil, err
	}
	// Task-level naming: multiple tasks coexist on the same node without clash.
	pod.Name = util.GenerateTaskMigrationWorkerName(nodeName, jobUID)
	// K8s constraint: a pod can only mount PVCs from its own namespace, so the
	// task-level worker is created in the PVC namespace; during PVC preheat the
	// worker also reads the PreheatJob CR, requiring an SA with migration.io
	// permissions - every namespace has an SA named
	// lj-runtimekit-migration-worker (ClusterRoleBinding to
	// lj-runtimekit-migration-worker), the permission-less default must not be
	// used.
	pod.Spec.ServiceAccountName = "lj-runtimekit-migration-worker"
	if len(pod.Spec.Containers) == 0 {
		return nil, fmt.Errorf("worker pod template has no containers")
	}
	// Port arg + PVC mount injection.
	pod.Spec.Containers[0].Args = append(pod.Spec.Containers[0].Args, fmt.Sprintf("--grpc-port=%d", port))
	pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
		Name:      pvStorageVolumeName,
		MountPath: PVCMountPoint,
	})
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
		Name: pvStorageVolumeName,
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.PvcName},
		},
	})
	return pod, nil
}

// allocateTaskWorkerPort allocates a port for a task-level worker: grpcPort
// (default 50051) is always reserved for the shared worker, the task-level
// port pool starts at grpcPort+1; ports scanned from the actual --grpc-port
// args of all existing worker pods (shared + task-level) on the node count as
// used, plus the cooldown ports of just-deleted workers (a hostNetwork port is
// only released when the container process exits; it may still be held after
// the pod object is gone), returning the first free port. The caller must
// already hold the workerPodLock of this nodeIP.
func (c *SharedContext) allocateTaskWorkerPort(ctx context.Context, nodeIP string, grpcPort int) (int, error) {
	used := map[int]bool{grpcPort: true} // grpcPort reserved for the shared worker

	// Cooldown ports count as used (per-node only: hostNetwork ports are
	// per-node, a global ban would wrongly block the same port number that is
	// free on other nodes); stale entries are cleaned up along the way.
	c.taskWorkerPortsLock.Lock()
	for p, until := range c.taskWorkerPortCooldowns[nodeIP] {
		if time.Now().Before(until) {
			used[p] = true
		} else {
			delete(c.taskWorkerPortCooldowns[nodeIP], p)
		}
	}
	// Ports already allocated on this node in the registry also count as used:
	// a just-created pod not yet scheduled (Status.HostIP empty) is invisible to
	// the pods scan; without merging the registry the same port would be
	// allocated twice causing a double-pod bind conflict. Key format is
	// jobUID|nodeIP, match this node by suffix.
	for key, p := range c.taskWorkerPorts {
		if strings.HasSuffix(key, "|"+nodeIP) {
			used[p] = true
		}
	}
	c.taskWorkerPortsLock.Unlock()

	sel, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{{
			Key:      labels.LabelComponent,
			Operator: metav1.LabelSelectorOpIn,
			Values:   []string{labels.MigrationWorkerComponentName, labels.MigrationWorkerComponentNameAlias},
		}},
	})
	if err != nil {
		return 0, fmt.Errorf("build worker label selector: %w", err)
	}
	pods := &corev1.PodList{}
	if err := c.Client.List(ctx, pods, client.MatchingLabelsSelector{Selector: sel}); err != nil {
		return 0, fmt.Errorf("list worker pods: %w", err)
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Status.HostIP != nodeIP {
			continue
		}
		if p, ok := parseWorkerGRPCPort(&pod.Spec); ok {
			used[p] = true
		}
	}

	for p := grpcPort + 1; p < 65535; p++ {
		if !used[p] {
			return p, nil
		}
	}
	return 0, fmt.Errorf("no free port available on node %s", nodeIP)
}

// parseWorkerGRPCPort parses --grpc-port=N from the container args of a worker
// pod (single-token form).
func parseWorkerGRPCPort(spec *corev1.PodSpec) (int, bool) {
	if len(spec.Containers) == 0 {
		return 0, false
	}
	for _, arg := range spec.Containers[0].Args {
		var p int
		if n, err := fmt.Sscanf(arg, "--grpc-port=%d", &p); err == nil && n == 1 {
			return p, true
		}
	}
	return 0, false
}

// DeleteTaskWorker deletes the task-level worker of a task on a node
// (idempotent), releasing the hostNetwork port and the PVC mount. The pod is
// located by "this node + name containing the task suffix"; the caller does
// not need to know the nodeName.
func (c *SharedContext) DeleteTaskWorker(ctx context.Context, nodeIP, jobUID string) error {
	c.workerPodLock.LockKey(nodeIP)
	defer c.workerPodLock.UnlockKey(nodeIP)

	pod, err := c.findTaskWorkerPod(ctx, nodeIP, jobUID)
	if k8serrors.IsNotFound(err) {
		// Pod already gone (e.g. deleted externally): the port is long released;
		// sync-drop the registry residue - port allocation merges the registry
		// (allocateTaskWorkerPort), a leftover entry would keep the port skipped
		// forever within this controller's lifetime. The pod object no longer
		// exists so no args to parse the port from; the client-cache entry is
		// left to be cleaned up per jobUID later.
		c.taskWorkerPortsLock.Lock()
		delete(c.taskWorkerPorts, jobUID+"|"+nodeIP)
		c.taskWorkerPortsLock.Unlock()
		return nil
	}
	if err != nil {
		return err
	}
	if pod.DeletionTimestamp != nil {
		// Terminating: wait and confirm on the next round.
		return nil
	}
	// Close the cached connection of this task-level worker (port parsed from
	// args, skip when unparseable).
	if port, ok := parseWorkerGRPCPort(&pod.Spec); ok {
		c.removeWorkerClientLocked(workerClientKey(nodeIP, port))
		// Port enters the cooldown table (per-node): the pod object is about to
		// disappear, but the container process still needs an async window
		// (delete grace / orphan process) to release the port; within the
		// cooldown this node's allocator will not reuse it.
		c.taskWorkerPortsLock.Lock()
		if c.taskWorkerPortCooldowns[nodeIP] == nil {
			c.taskWorkerPortCooldowns[nodeIP] = map[int]time.Time{}
		}
		c.taskWorkerPortCooldowns[nodeIP][port] = time.Now().Add(taskWorkerPortCooldown)
		c.taskWorkerPortsLock.Unlock()
	}
	// Sync-drop the port registry: the worker is deleted, a leftover port entry
	// would keep later WorkerClientFor dialing a non-existent worker via the
	// task-level port (data cleanup on CR deletion would fail with connection
	// refused). cleanTaskEnsuringWorker re-registers the new port when it
	// rebuilds the worker.
	c.taskWorkerPortsLock.Lock()
	delete(c.taskWorkerPorts, jobUID+"|"+nodeIP)
	c.taskWorkerPortsLock.Unlock()
	return c.Client.Delete(ctx, pod)
}

// findTaskWorkerPod locates the task-level worker pod of a task by "node + name
// containing the task suffix".
func (c *SharedContext) findTaskWorkerPod(ctx context.Context, nodeIP, jobUID string) (*corev1.Pod, error) {
	suffix := jobUID
	if len(suffix) > 8 {
		suffix = suffix[:8]
	}
	pods := &corev1.PodList{}
	if err := c.Client.List(ctx, pods, client.MatchingLabels{
		labels.LabelComponent: labels.MigrationWorkerComponentNameAlias,
	}); err != nil {
		return nil, err
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Status.HostIP != nodeIP {
			continue
		}
		if strings.HasPrefix(pod.Name, "lj-runtimekit-migration-worker-") && strings.HasSuffix(pod.Name, "-"+suffix) {
			return pod, nil
		}
	}
	return nil, k8serrors.NewNotFound(corev1.Resource("pods"), "lj-runtimekit-migration-worker-*-"+suffix)
}

// storeTaskWorkerPort / TaskWorkerPort maintain the in-memory registry of
// "jobUID+nodeIP -> allocated port", used by handlers to fetch the port when
// connecting a task-level worker via GetOrCreateTaskWorkerClient. After a
// controller restart EnsureTaskWorker restores it from the actual args of the
// existing pod, so no stale port is ever read.
func (c *SharedContext) storeTaskWorkerPort(jobUID, nodeIP string, port int) {
	c.taskWorkerPortsLock.Lock()
	defer c.taskWorkerPortsLock.Unlock()
	c.taskWorkerPorts[jobUID+"|"+nodeIP] = port
}

// TaskWorkerPort returns the port of the task-level worker of a task on a
// node, 0 when unknown.
func (c *SharedContext) TaskWorkerPort(jobUID, nodeIP string) int {
	c.taskWorkerPortsLock.Lock()
	defer c.taskWorkerPortsLock.Unlock()
	return c.taskWorkerPorts[jobUID+"|"+nodeIP]
}

// ============================================================================
// PVC preheat source pod
//
// The http origin of PVC preheat is served by a dedicated preheat source pod:
// it mounts the target PVC at /mnt/pv and exposes the directory as an http
// file service in --serve-http mode. After preheat Ready the pod is **deleted
// immediately**: source reachability during restore is provided by the restore
// workers double-hosting the source (same PVC mount, self-written
// EndpointSlice, see the worker-side storage package). It is also deleted on
// preheat failure (markFailed) and PreheatJob CR deletion (finalizer). Fully
// decoupled from the task-level worker lifecycle.
// ============================================================================

const (
	// PreheatSourceHTTPPort is the fixed http port of the preheat source pod
	// file service. The pod uses pod networking (not hostNetwork): each pod has
	// an independent network namespace, all listening on the same port never
	// conflict, so the number of concurrent PreheatJobs is no longer bounded by
	// a port pool. The remoteURL uses the Service ClusterIP (created with the
	// pod, stable), pod recreation does not affect the URL.
	PreheatSourceHTTPPort = 50090
)

// GeneratePreheatSourcePodName generates the preheat source pod name:
// preheat-source-<first 8 chars of jobUID>.
func GeneratePreheatSourcePodName(jobUID string) string {
	suffix := jobUID
	if len(suffix) > 8 {
		suffix = suffix[:8]
	}
	return fmt.Sprintf("preheat-source-%s", suffix)
}

// GeneratePreheatSourceServiceName generates the preheat source Service name:
// preheat-source-<first 16 hex of sha256("ns/name/pvcPath")>.
//
// The preheat side (controller) and the restore side (worker) compute the same
// name for the same PVC+path; the Service is persistent (never deleted) with a
// constant ClusterIP -> constant preheat URL -> constant task_id, the preheat
// cache is reused across rounds. Both sides must trim leading/trailing slashes
// of pvcPath before hashing (consistent with the normalization of validRelPath
// / the worker-side checkpointDir).
func GeneratePreheatSourceServiceName(pvcNamespace, pvcName, pvcPath string) string {
	key := pvcNamespace + "/" + pvcName + "/" + strings.Trim(pvcPath, "/")
	sum := sha256.Sum256([]byte(key))
	return "preheat-source-" + hex.EncodeToString(sum[:])[:16]
}

// EnsurePreheatSourcePod makes sure the PVC preheat source pod and its
// persistent ClusterIP Service exist, returning the Service ClusterIP and the
// pod readiness state.
//
// The preheat source pod uses pod networking + a fixed port
// (PreheatSourceHTTPPort): each pod has an independent network namespace, any
// number of concurrent PreheatJobs never conflict on ports, no port allocation
// needed. The Service is named by the hash of (pvcNamespace, pvcName, pvcPath),
// selector-less, persistent (constant ClusterIP -> constant URL/task_id, the
// preheat cache is reused across rounds); backends are declared by their
// respective EndpointSlices (the preheat source pod's slice is maintained by
// this function with ownerReference to the pod, GC'd by K8s after pod
// deletion).
// ready=false means the pod is not ready yet (scheduling/starting), the caller
// should wait and retry; a non-nil err means creation failed or the pod is in
// a non-self-healing failure state (scheduling failure, container stuck etc.),
// the message already carries the PVC hint.
func (c *SharedContext) EnsurePreheatSourcePod(ctx context.Context, jobUID, pvcName, pvcNamespace, pvcPath string) (string, bool, error) {
	ctxLog := log.FromContext(ctx)

	name := GeneratePreheatSourcePodName(jobUID)
	namespace := pvcNamespace
	serviceName := GeneratePreheatSourceServiceName(pvcNamespace, pvcName, pvcPath)

	existing := &corev1.Pod{}
	getErr := c.Client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, existing)
	if k8serrors.IsNotFound(getErr) {
		cfg, err := c.getWorkerConfig(ctx, false)
		if err != nil {
			return "", false, fmt.Errorf("worker config invalid: %w", err)
		}
		desired, err := c.buildPreheatSourcePod(jobUID, pvcName, namespace, cfg)
		if err != nil {
			return "", false, err
		}
		if err := c.createWorkerPodIfAbsent(ctx, desired); err != nil {
			return "", false, fmt.Errorf("create preheat source pod failed: %w", err)
		}
		if err := c.ensurePreheatSourceService(ctx, namespace, serviceName); err != nil {
			return "", false, err
		}
		ctxLog.Info("Created preheat source pod and service", "namespace", namespace, "name", name, "pvc", pvcName, "service", serviceName)
		return "", false, nil
	}
	if getErr != nil {
		return "", false, fmt.Errorf("get preheat source pod: %w", getErr)
	}

	// Old pod terminating: only wait, build the new one on a later round.
	if existing.DeletionTimestamp != nil {
		return "", false, nil
	}

	// Pod exists: first check non-self-healing failure states (scheduling
	// failure / container stuck), then ensure the Service and the preheat
	// source EndpointSlice, finally report the readiness state.
	if err := preheatSourcePodFailure(existing, pvcName); err != nil {
		return "", false, err
	}
	if err := c.ensurePreheatSourceService(ctx, namespace, serviceName); err != nil {
		return "", false, err
	}
	if podRunningReady(existing) {
		if err := c.ensurePreheatSourceEndpointSlice(ctx, existing, namespace, serviceName); err != nil {
			return "", false, err
		}
	}
	serviceIP, err := c.preheatSourceServiceIP(ctx, namespace, serviceName)
	if err != nil {
		return "", false, err
	}
	if podRunningReady(existing) {
		return serviceIP, true, nil
	}
	return serviceIP, false, nil
}

// ensurePreheatSourceService makes sure the preheat source Service exists
// (idempotent): no selector, backends declared by each registrant's
// EndpointSlice; the port is fixed at PreheatSourceHTTPPort. The Service is
// persistent: it survives preheat source pod deletion and PreheatJob CR
// deletion (the finalizer does not touch it); a constant ClusterIP is the
// precondition of stable preheat URL/task_id and cross-round cache reuse.
func (c *SharedContext) ensurePreheatSourceService(ctx context.Context, namespace, name string) error {
	existing := &corev1.Service{}
	err := c.Client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, existing)
	if err == nil {
		return nil
	}
	if !k8serrors.IsNotFound(err) {
		return fmt.Errorf("get preheat source service: %w", err)
	}
	desired := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{
				{
					Name:     "http",
					Protocol: corev1.ProtocolTCP,
					Port:     PreheatSourceHTTPPort,
				},
			},
		},
	}
	if err := c.Client.Create(ctx, desired); err != nil {
		if k8serrors.IsAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("create preheat source service failed: %w", err)
	}
	return nil
}

// ensurePreheatSourceEndpointSlice makes sure the preheat source pod's
// EndpointSlice exists (idempotent): endpoint is podIP:50090, the port name
// matches the Service ("http"), ownerReference points to the preheat source pod
// - the slice is GC'd by K8s after pod deletion, the controller never deletes
// it actively.
func (c *SharedContext) ensurePreheatSourceEndpointSlice(ctx context.Context, pod *corev1.Pod, namespace, serviceName string) error {
	name := serviceName + "-" + pod.Name
	existing := &discoveryv1.EndpointSlice{}
	err := c.Client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, existing)
	if err == nil {
		return nil
	}
	if !k8serrors.IsNotFound(err) {
		return fmt.Errorf("get preheat source endpointslice: %w", err)
	}
	if pod.Status.PodIP == "" {
		return fmt.Errorf("preheat source pod %s/%s has no pod IP", namespace, pod.Name)
	}

	controllerTrue := true
	portName := "http"
	port := int32(PreheatSourceHTTPPort)
	proto := corev1.ProtocolTCP
	ready := true
	desired := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				discoveryv1.LabelServiceName: serviceName,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "v1",
					Kind:       "Pod",
					Name:       pod.Name,
					UID:        pod.UID,
					Controller: &controllerTrue,
				},
			},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Ports: []discoveryv1.EndpointPort{
			{
				Name:     &portName,
				Port:     &port,
				Protocol: &proto,
			},
		},
		Endpoints: []discoveryv1.Endpoint{
			{
				Addresses:  []string{pod.Status.PodIP},
				Conditions: discoveryv1.EndpointConditions{Ready: &ready},
			},
		},
	}
	if err := c.Client.Create(ctx, desired); err != nil {
		if k8serrors.IsAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("create preheat source endpointslice: %w", err)
	}
	return nil
}

// preheatSourceServiceIP returns the ClusterIP of the preheat source Service;
// empty string when not yet allocated.
func (c *SharedContext) preheatSourceServiceIP(ctx context.Context, namespace, serviceName string) (string, error) {
	svc := &corev1.Service{}
	err := c.Client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: serviceName}, svc)
	if err != nil {
		return "", fmt.Errorf("get preheat source service: %w", err)
	}
	return svc.Spec.ClusterIP, nil
}

// buildPreheatSourcePod builds the preheat source pod from the shared worker
// template: renamed to preheat-source-<jobUID8>, node affinity cleared
// (scheduling left to the scheduler), SA set to default, hostNetwork off (pod
// networking + fixed port, any number of concurrent preheat source pods
// without conflict), LabelPreheatSourceJobUID stamped for Service selector
// matching, PVC mount and --serve-http args injected. The preheat source pod
// needs no gRPC port (the worker does not listen on gRPC in serve-http mode)
// and does not participate in the task-level worker port pool.
func (c *SharedContext) buildPreheatSourcePod(jobUID, pvcName, namespace string, cfg *workerConfig) (*corev1.Pod, error) {
	// nodeName is only used to generate the placeholder name and node affinity,
	// both are overwritten/cleared right after.
	pod, err := util.BuildMigrationWorker("preheat-source", namespace, cfg.template, cfg.hash, cfg.source)
	if err != nil {
		return nil, err
	}
	pod.Name = GeneratePreheatSourcePodName(jobUID)
	// The preheat source pod pins no node: leave it to the scheduler (a
	// PreheatJob may not fill checkpointUID, the controller has no source node
	// info to pin with).
	pod.Spec.Affinity = nil
	// Pod networking: each pod has an independent network namespace, the fixed
	// port never conflicts, concurrent preheat is unbounded.
	pod.Spec.HostNetwork = false
	pod.Spec.ServiceAccountName = "default"
	if pod.Labels == nil {
		pod.Labels = map[string]string{}
	}
	pod.Labels[labels.LabelPreheatSourceJobUID] = jobUID
	if len(pod.Spec.Containers) == 0 {
		return nil, fmt.Errorf("worker pod template has no containers")
	}
	pod.Spec.Containers[0].Args = append(pod.Spec.Containers[0].Args,
		"--serve-http",
		fmt.Sprintf("--http-port=%d", PreheatSourceHTTPPort))
	pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
		Name:      pvStorageVolumeName,
		MountPath: PVCMountPoint,
	})
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
		Name: pvStorageVolumeName,
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvcName},
		},
	})
	return pod, nil
}

// preheatSourcePodFailure returns the non-self-healing failure reason of the
// preheat source pod; nil for still-waitable states (Pending starting up /
// container starting).
func preheatSourcePodFailure(pod *corev1.Pod, pvcName string) error {
	// Scheduling failure: insufficient node resources / PVC mount constraints
	// are typical causes.
	for i := range pod.Status.Conditions {
		cond := &pod.Status.Conditions[i]
		if cond.Type == corev1.PodScheduled && cond.Status == corev1.ConditionFalse &&
			cond.Reason == corev1.PodReasonUnschedulable {
			return fmt.Errorf("preheat source pod cannot be scheduled: %s "+
				"(check PVC %s mountability and node resources)",
				cond.Message, pvcName)
		}
	}
	// Persistent container-wait errors (CreateContainerConfigError /
	// CrashLoopBackOff etc.; a mount failure is retried by the kubelet and
	// eventually surfaces as one of these states).
	for i := range pod.Status.ContainerStatuses {
		cs := &pod.Status.ContainerStatuses[i]
		if cs.State.Waiting != nil && cs.State.Waiting.Reason != "" &&
			cs.State.Waiting.Reason != "ContainerCreating" && cs.State.Waiting.Reason != "PodInitializing" {
			return fmt.Errorf("preheat source pod container is stuck in %s: %s "+
				"(check PVC %s mountability)",
				cs.State.Waiting.Reason, cs.State.Waiting.Message, pvcName)
		}
	}
	return nil
}

// DeletePreheatSourcePod deletes the PVC preheat source pod (idempotent).
// Called on preheat failure (markFailed), preheat Ready and PreheatJob CR
// deletion fallback, releasing the PVC mount; its EndpointSlice is GC'd by K8s
// together with the owner pod. **The Service is never deleted**: the
// persistent Service ClusterIP is part of the preheat URL/task_id, deleting it
// would destroy the preheated cache.
func (c *SharedContext) DeletePreheatSourcePod(ctx context.Context, jobUID, pvcNamespace string) error {
	pod := &corev1.Pod{}
	err := c.Client.Get(ctx, client.ObjectKey{
		Namespace: pvcNamespace,
		Name:      GeneratePreheatSourcePodName(jobUID),
	}, pod)
	if k8serrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if pod.DeletionTimestamp != nil {
		// Terminating: wait and confirm on the next round.
		return nil
	}
	if err := c.Client.Delete(ctx, pod); err != nil && !k8serrors.IsNotFound(err) {
		return err
	}
	return nil
}

// ============================================================================
// PVC copy-preheat pod
//
// Copy preheat (PreheatJob options filled with target-pvc-name /
// target-pvc-namespace) is carried out by a one-shot copy pod: mounts the
// source PVC at /mnt/src and the target PVC at /mnt/dst, copies the pvcPath
// subtree of the source PVC into a sibling .tmp directory on the target,
// verifies file count and sizes of both sides inside the pod, and only after
// the verification passes deletes the old pvcPath directory on the target
// (only this one level) and atomically renames the temp into place. Copy mode
// deliberately builds no preheat source pod and no Service - the persistent
// Service that restore derives by hashing the target coordinates must not
// exist, so restore automatically falls back to reading the mounted target
// PVC directly. The pod result is polled by phase in the preheatjob package
// (taskID = namespace/podName); deleted on Ready / Failed / PreheatJob CR
// deletion. Decoupled from the preheat source pod lifecycle.
// ============================================================================

const (
	// preheatCopySrcMountPoint / preheatCopyDstMountPoint are the mount points
	// of the source and target PVCs inside the copy pod.
	preheatCopySrcMountPoint = "/mnt/src"
	preheatCopyDstMountPoint = "/mnt/dst"
	// preheatCopySrcVolumeName / preheatCopyDstVolumeName are the volume names
	// of the source and target PVCs inside the copy pod.
	preheatCopySrcVolumeName = "pv-storage-src"
	preheatCopyDstVolumeName = "pv-storage-dst"
)

// preheatCopyScript is the copy script: first copies the whole pvcPath of the
// source PVC into a sibling <pvcPath>.tmp temp directory on the target, then
// generates "size+path" manifests from find+stat on both sides and diffs them -
// any mismatch in file count or size (or a copy producing 0 files) exits
// non-zero (pod Failed -> PreheatJob Failed directly, no auto retry); the
// half-finished tmp is cleaned up by an EXIT trap, the old data on the target
// stays untouched; only after verification passes does it delete the old
// pvcPath directory on the target (only this one level, never touching other
// paths on the target) and atomically renames into place (mv within one
// filesystem is rename), preventing stale parts of the previous round from
// mixing into restore. pvcPath is passed as the positional argument "$1" (never
// interpolated into the script text, same injection guard as
// warmupPreheatSourcePod); the entry already passed validRelPath structural
// validation; the script adds another defensive layer against empty values /
// absolute paths / ".." segments; variable expansion does not re-interpret
// quotes, special characters inside segments are safe.
const preheatCopyScript = `set -eu
p="$1"
# Defense: p already passed validRelPath validation at the entry; this is an
# extra layer ensuring the delete/rename target is strictly limited to the
# pvcPath level inside the target PVC - any empty value / root path / ".."
# segment fails directly.
case "$p" in
  ""|"."|".."|/*|*"/.."|"../"*|*"/../"*)
    echo "invalid pvcPath: $p"; exit 1 ;;
esac
[ -d "/mnt/src/$p" ] || { echo "source path /mnt/src/$p not found"; exit 1; }
tmp="/mnt/dst/$p.tmp"
rm -rf "$tmp"
mkdir -p "$tmp"
# Any failure (including cp exiting directly under set -e) cleans the
# half-finished tmp via the EXIT trap: the old target data stays untouched and
# the half-finished copy takes no space; the trap is only cancelled after a
# successful rename.
cleanup() { rm -rf "$tmp"; }
trap cleanup EXIT
cp -a "/mnt/src/$p/." "$tmp/"
(cd "/mnt/src/$p" && find . -type f -print0 | xargs -0 stat -c '%s %n' | sort) > /tmp/src.lst
(cd "$tmp" && find . -type f -print0 | xargs -0 stat -c '%s %n' | sort) > /tmp/dst.lst
if ! diff /tmp/src.lst /tmp/dst.lst; then
  echo "copy verification failed, keeping the existing target"
  exit 1
fi
count=$(wc -l < /tmp/dst.lst)
if [ "$count" -le 0 ]; then
  echo "copy produced 0 files, keeping the existing target"
  exit 1
fi
# Only touch old data after verification passes: delete only the pvcPath level
# (never other paths on the target), then atomically rename into place; mv
# within one filesystem is rename, there is no half-finished visible window.
rm -rf "/mnt/dst/$p"
if ! mv "$tmp" "/mnt/dst/$p"; then
  # On rename failure keep the tmp (the full copy is not lost), cancel the
  # trap before exiting.
  trap - EXIT
  echo "rename failed, temp copy kept at $tmp"
  exit 1
fi
trap - EXIT
echo "copy ok: $count files"
`

// GeneratePreheatCopyPodName generates the copy pod name:
// preheat-copy-<first 8 chars of jobUID>.
func GeneratePreheatCopyPodName(jobUID string) string {
	suffix := jobUID
	if len(suffix) > 8 {
		suffix = suffix[:8]
	}
	return fmt.Sprintf("preheat-copy-%s", suffix)
}

// CreatePreheatCopyPod creates the one-shot copy pod (idempotent: the pod name
// is determined by jobUID, an existing one is treated as success and reused).
// Success or failure is judged by the caller polling the phase; this function
// only creates. Source and target must be in the same namespace (K8s
// constraint: a single pod can only mount PVCs from its own namespace), the
// constraint is guaranteed by the entry validation in the preheatjob package.
func (c *SharedContext) CreatePreheatCopyPod(ctx context.Context, jobUID, srcPvcName, dstPvcName, dstPvcNamespace, pvcPath string) (string, error) {
	cfg, err := c.getWorkerConfig(ctx, false)
	if err != nil {
		return "", fmt.Errorf("worker config invalid: %w", err)
	}
	desired, err := c.buildPreheatCopyPod(jobUID, srcPvcName, dstPvcName, dstPvcNamespace, pvcPath, cfg)
	if err != nil {
		return "", err
	}
	if err := c.createWorkerPodIfAbsent(ctx, desired); err != nil {
		return "", fmt.Errorf("create preheat copy pod failed: %w", err)
	}
	return desired.Name, nil
}

// buildPreheatCopyPod builds the copy pod from the shared worker template:
// renamed to preheat-copy-<jobUID8>, node affinity cleared (scheduling left to
// the scheduler), SA set to default (pure file copy, no API permission
// needed), hostNetwork off (no port needed), command overwritten with the copy
// script (pvcPath passed as a positional argument), source/target PVC mounts
// injected. The image reuses the worker image (ubuntu 20.04 base with built-in
// cp/find/stat/diff).
func (c *SharedContext) buildPreheatCopyPod(jobUID, srcPvcName, dstPvcName, dstPvcNamespace, pvcPath string, cfg *workerConfig) (*corev1.Pod, error) {
	// nodeName is only used to generate the placeholder name and node affinity,
	// both are overwritten/cleared right after.
	pod, err := util.BuildMigrationWorker("preheat-copy", dstPvcNamespace, cfg.template, cfg.hash, cfg.source)
	if err != nil {
		return nil, err
	}
	pod.Name = GeneratePreheatCopyPodName(jobUID)
	pod.Spec.Affinity = nil
	// Pod networking: copy needs no port, turn off the template's
	// hostNetwork: true as well.
	pod.Spec.HostNetwork = false
	pod.Spec.ServiceAccountName = "default"
	// One-shot task: the worker template defaults to Always (long-running
	// service); after the copy script exits 0 the pod must stop at Succeeded,
	// otherwise the kubelet restarts the container forever, the pod never
	// reaches a terminal state, and the copyBackend polling never sees Success.
	pod.Spec.RestartPolicy = corev1.RestartPolicyNever
	if len(pod.Spec.Containers) == 0 {
		return nil, fmt.Errorf("worker pod template has no containers")
	}
	container := &pod.Spec.Containers[0]
	container.Command = []string{"sh", "-c", preheatCopyScript}
	container.Args = []string{"sh", pvcPath}
	container.VolumeMounts = append(container.VolumeMounts,
		corev1.VolumeMount{Name: preheatCopySrcVolumeName, MountPath: preheatCopySrcMountPoint},
		corev1.VolumeMount{Name: preheatCopyDstVolumeName, MountPath: preheatCopyDstMountPoint})
	pod.Spec.Volumes = append(pod.Spec.Volumes,
		corev1.Volume{
			Name: preheatCopySrcVolumeName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: srcPvcName},
			},
		},
		corev1.Volume{
			Name: preheatCopyDstVolumeName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: dstPvcName},
			},
		})
	return pod, nil
}

// DeletePreheatCopyPod deletes the copy pod (idempotent). Called on copy
// preheat Ready, failure and PreheatJob CR deletion fallback, releasing the
// mount of both the source and target PVCs.
func (c *SharedContext) DeletePreheatCopyPod(ctx context.Context, jobUID, namespace string) error {
	pod := &corev1.Pod{}
	err := c.Client.Get(ctx, client.ObjectKey{
		Namespace: namespace,
		Name:      GeneratePreheatCopyPodName(jobUID),
	}, pod)
	if k8serrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if pod.DeletionTimestamp != nil {
		// Terminating: wait and confirm on the next round.
		return nil
	}
	if err := c.Client.Delete(ctx, pod); err != nil && !k8serrors.IsNotFound(err) {
		return err
	}
	return nil
}
