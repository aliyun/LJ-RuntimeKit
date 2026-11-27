[简体中文](README.md) | **English**

# Container Migration Controller

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Container Migration Controller is a Kubernetes controller that migrates container state (memory,
file system, and device state) between nodes to achieve interruption-free live container migration.

The currently supported storage path is **direct PVC**:

```
checkpoint to PVC  ->  (optional) preheat: copy from one PVC to another  ->  restore reads from PVC
```

Other storage backends (local / nfs / oss / dragonfly / efc) may be added in the future and are not
covered here.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [How It Works (PVC Path)](#how-it-works-pvc-path)
- [Project Layout](#project-layout)
- [Quick Start](#quick-start)
  - [Prerequisites](#prerequisites)
  - [Building](#building)
  - [Deployment](#deployment)
  - [Uninstall](#uninstall)
- [Usage Examples (PVC Path)](#usage-examples-pvc-path)
  - [Example 1: Full checkpoint + restore](#example-1-full-checkpoint--restore)
  - [Example 2: Restore only (reference an existing checkpoint)](#example-2-restore-only-reference-an-existing-checkpoint)
  - [Example 3: Optional PVC-to-PVC preheat](#example-3-optional-pvc-to-pvc-preheat)
  - [Automatic checkpoint: Pod annotations](#automatic-checkpoint-pod-annotations)
- [Configuration](#configuration)
  - [Controller flags](#controller-flags)
  - [Worker configuration (ConfigMap)](#worker-configuration-configmap)
- [RBAC](#rbac)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

## Overview

Container Migration Controller manages two CRDs:

1. **ContainerStateMigrationJobRequest** (abbreviated `csmjr`): describes one migration job, with an
   optional checkpoint stage and an optional restore stage.
2. **PreheatJob** (abbreviated `phj`, optional): describes a preheat job. Preheat is a pure
   acceleration measure and does not affect data correctness; restore can read directly from the
   source PVC and skip preheat.

The controller does not touch containers directly: the actual checkpoint/restore execution is done by
the **Migration-Worker** (a per-job Pod) on the node, and the controller dispatches tasks over gRPC and
polls their progress.

## Architecture

```
+---------------------------------------------------------------------+
|                        Kubernetes Cluster                            |
+---------------------------------------------------------------------+
|                                                                       |
|  +--------------------+        +------------------------------+       |
|  | Migration          |        |      Source node (source Pod) |       |
|  | Controller         |        |  +----------+  +-----------+  |       |
|  |                    |        |  | Source   |  | per-job   |  |       |
|  |  - MigrationJob    | gRPC   |  | Pod      |  | Worker    |  |       |
|  |    Controller      |------->|  +----------+  +-----+-----+  |       |
|  |  - Pod Controller  |        |                      | mount   |       |
|  |  - PreheatJob      |        |              +-------v-------+ |       |
|  |    Controller      |        |              | Source PVC    | |       |
|  |  - ConfigMap       |        |              | (/mnt/pv)     | |       |
|  |    Controller      |        |              | checkpoint    | |       |
|  +--------------------+        |              | data          | |       |
|                                |              +---------------+ |       |
|                                +------------------------------+       |
|                                                                       |
|  +-------------+ create/poll  +------------------------------+        |
|  |  PreheatJob |------------->|  Copy preheat Pod (one-shot) |        |
|  |  Controller |              |  source PVC -> target PVC    |        |
|  +-------------+              |  same-path copy              |        |
|                               +------------------------------+        |
|                                                                       |
|  +----------------------------------------------+                    |
|  |            Target node (replica Pod)          |                    |
|  |  +--------------+      +-------------------+ |                    |
|  |  | per-job      |<-----|  Replica Pod      | |                    |
|  |  | Worker       |      |  (blocked init;   | |                    |
|  |  | (mounts PVC) |      |   released later) | |                    |
|  |  +--------------+      +-------------------+ |                    |
|  +----------------------------------------------+                    |
+---------------------------------------------------------------------+
```

## How It Works (PVC Path)

A full migration proceeds as follows (each stage is handled by one handler in the handler chain):

1. **Init**: validate CR fields. In PVC mode this requires:
   - the referenced PVC must exist;
   - jobs that include a restore stage require an RWX PVC (restore replicas may land on multiple nodes);
   - the checkpoint container list must contain exactly one container.
2. **Checkpoint** (optional): the controller creates a **per-job Worker** on the source node
   (mounting the PVC at the fixed mount point `/mnt/pv`), and the Worker writes the source
   container's rootfs / memory / xpu data directly into the PVC. The relative directory written is
   `{sourcePodUID}/{version}/{container}/checkpoint`, recorded in the CR's `status.checkpointStatus`.
3. **Preheat** (optional, pure acceleration): after the checkpoint completes, a PreheatJob CR may be
   created to preheat the checkpoint directory from the source PVC to a target PVC (copy mode) or a
   cluster cache. When preheat is skipped, restore reads directly from the source PVC. Preheat does
   not affect data correctness.
4. **Restore** (optional):
   - **CreatePods**: rebuild replica Pods from the source Pod template (replicas inherit the source
     Pod's `nodeSelector`; only `nodeName` is cleared). A blocking init container is injected into the
     Pod to hold back the main container until the data is ready.
   - **PrepareData**: the per-job Worker on the target node reads the checkpoint data from the PVC and
     unpacks it into a local directory on the target node (data preparation failures are retried
     `restore.retryCount` times, and fall back to a cold start after exhaustion).
   - **ReleaseInit**: the controller writes a signal file through `pods/exec` to release the init
     container, and the main container starts immediately with a runc restore from the checkpoint.
   - **WaitRestore**: confirm the main container is Running and that the restore actually took effect
     (query the Worker for the `restoreAttempted` proof, preventing the silent failure of "container
     Running but actually cold-started").
   - **RestoreXPU**: the Worker forwards to the node's XDM cr-tool to restore the container's XPU
     device state.
5. **Clean**: after the job reaches a terminal state, clean up the Worker-side job record; the per-job
   Worker is reclaimed when the stage completes or the CR is deleted, releasing the PVC mount and port.

> Note: checkpoint and restore can be declared in the same CR (full migration), or only one of them:
> declaring only `checkpoint` is a pure checkpoint job (the artifact stays in the PVC); declaring only
> `restore` references a historical checkpoint CR through `restore.checkpointUID`.

## Project Layout

```
Migration-Controller/
├── api/
│   └── v1beta1/                        # CRD type definitions (csmjr / preheatjob)
├── cmd/
│   ├── main.go                         # controller main program
│   └── leader.go                       # leader election wrapper (one Lease drives the whole process)
├── pkg/
│   ├── config/                         # runtime configuration and defaults
│   ├── controllers/
│   │   ├── migrationjob/               # MigrationJob main controller
│   │   │   └── handler/                # handler chain (12 stages)
│   │   ├── pod/                        # Pod watching, auto-creates checkpoint CRs
│   │   ├── preheatjob/                 # PreheatJob controller and three backends
│   │   │                              # (dragonfly / efc / copy)
│   │   ├── configmap/                  # hot reload of the Worker configuration ConfigMap
│   │   └── context.go                  # Worker client cache and drift detection
│   ├── migrationworker/                # Worker gRPC client (including generated proto code)
│   ├── dragonfly/                      # Dragonfly Manager/Scheduler client
│   ├── preheat/                        # preheat backend abstraction
│   ├── labels/                         # annotation / label constants
│   ├── metrics/                        # Prometheus metrics
│   └── util/                           # common utilities
├── proto/
│   └── container_state_migration.proto # Worker gRPC protocol definition
├── deploy/                             # CRD / RBAC / ConfigMap / Deployment (one-click deploy.sh)
├── helm/                               # Dragonfly deployment reference (optional)
├── Dockerfile                          # multi-stage build
├── Makefile
└── PROJECT                             # Kubebuilder project file
```

## Quick Start

### Prerequisites

- Go 1.24+
- Docker
- kubectl (configured with cluster access)
- Kubernetes cluster 1.23+ (the target cluster version; both the controller and the CRDs are designed for 1.23)
- Migration runtime deployed on nodes (the Migration-Worker image is pullable, and XDM plus
  xpu-container-runtime are available)

### Building

```bash
# Build host binary
make build

# Build Docker image (defaults to the lj-runtimekit private registry, overridable)
make docker
make docker IMG=<your-registry>/lj-runtimekit-migration-controller:<tag>
```

### Deployment

All controller resources live under `deploy/`; deploy with a single command
(CRD -> RBAC -> ConfigMap -> Deployment):

```bash
# 1. Build the image (the image name in the manifests defaults to
#    lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-migration-controller:v1.14.0-k8s1.34;
#    tag your image the same, or override it during deployment)
make docker IMG=lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-migration-controller:v1.14.0-k8s1.34

# 2. Deploy (default namespace=kube-system, image names taken from the manifests)
./deploy.sh deploy

# The same command supports overriding namespace and images (they can also be passed through the
# NAMESPACE / CONTROLLER_IMG / WORKER_IMG environment variables; command-line flags take precedence)
./deploy.sh --namespace my-ns \
            --controller-img <registry>/lj-runtimekit-migration-controller:v1.1.0 \
            --worker-img <registry>/lj-runtimekit-migration-worker:v1.1.0 \
            deploy

# Optional: deploy the Dragonfly P2P preheat system as well
./deploy.sh deploy --with-dragonfly

# Show full usage
./deploy.sh --help
```

After deployment, verify the controller is running:

```bash
kubectl -n kube-system get deployment lj-runtimekit-migration-controller
kubectl -n kube-system get pod -l component=lj-runtimekit-migration-controller
kubectl -n kube-system logs -f -l component=lj-runtimekit-migration-controller
```

### Uninstall

```bash
./deploy.sh uninstall

# If you customized namespace / images, uninstall must use the same flags so the script can find and delete the resources
./deploy.sh --namespace my-ns uninstall

# Uninstall Dragonfly as well
./deploy.sh uninstall --with-dragonfly
```

## Usage Examples (PVC Path)

First prepare an RWX PVC (for example `oss-migration-pvc`). All examples below use the direct PVC
path.

### Example 1: Full checkpoint + restore

Checkpoint to a PVC, then restore 2 replicas from the same PVC:

```yaml
apiVersion: migration.io/v1beta1
kind: ContainerStateMigrationJobRequest
metadata:
  name: sample-migration
  namespace: default
spec:
  # Storage backend: pvc (this document only covers the PVC path)
  storageType: pvc
  storage:
    pvc:
      pvcName: oss-migration-pvc
      pvcNamespace: default

  # Checkpoint stage: write the source container state to the PVC
  checkpoint:
    sourcePodName: my-source-pod
    sourcePodNamespace: default
    sourcePodUID: "REPLACE-WITH-REAL-POD-UID"
    containers:            # currently exactly one container is required
      - main
    timeoutInMinute: 30    # if set, passed to cr-tool --timeout (minutes); if unset, no --timeout; 0 is also passed through (cr-tool wait-then-abort semantics); negative values are rejected
    enableCheckpointAutoMode: true  # default true: add --enable-auto-mode to cr-tool to auto-detect a steady state; if false, timeoutInMinute is required
    # Optional: fine-grained exclusion policy
    containerCheckpointPolicy:
      main:
        excludeFilepath:
          - /tmp
          - /var/run

  # Restore stage: restore replica Pods from the PVC
  restore:
    replicas: 2
    retryCount: 3          # download / runc restore / XPU restore are counted independently;
                           # after exhaustion the replica falls back to a cold start
```

Apply it and watch progress:

```bash
kubectl apply -f migration.yaml
kubectl get csmjr sample-migration -o yaml   # check status.phase and per-stage status
kubectl get csmjr -w
```

### Example 2: Restore only (reference an existing checkpoint)

```yaml
apiVersion: migration.io/v1beta1
kind: ContainerStateMigrationJobRequest
metadata:
  name: sample-restore-only
  namespace: default
spec:
  storageType: pvc
  storage:
    pvc:
      pvcName: oss-migration-pvc
      pvcNamespace: default
  restore:
    replicas: 1
    retryCount: 3
    # reference metadata.uid of the historical checkpoint CR (obtained after example 1 succeeded)
    checkpointUID: "3f8c1a54-0b7d-4c62-9a1e-7d5f2b6c8e90"
```

`checkpointUID` can be obtained with:

```bash
kubectl get csmjr sample-migration -o jsonpath='{.metadata.uid}'
```

### Example 3: Optional PVC-to-PVC preheat

Preheat is a pure acceleration measure and can be omitted. Copy mode copies the checkpoint directory
of the source PVC to the target PVC at the **same path** (for example when the target PVC is mounted
on a node with faster data reads):

```yaml
apiVersion: migration.io/v1beta1
kind: PreheatJob
metadata:
  name: sample-preheat
  namespace: default
spec:
  # Recommended: reference metadata.uid of the checkpoint CR; the controller resolves the source PVC
  # coordinates and the checkpoint directory path inside the PVC automatically (no manual version
  # timestamp assembly)
  checkpointUID: "3f8c1a54-0b7d-4c62-9a1e-7d5f2b6c8e90"
  options:
    # copy mode: target PVC coordinates (filling both selects copy preheat)
    target-pvc-name: fast-local-pvc
    target-pvc-namespace: default
```

You can also bypass the checkpoint CR and specify the source PVC and relative directory directly:

```yaml
spec:
  # preheat source: source PVC and the relative checkpoint directory inside it
  pvc:
    pvcName: oss-migration-pvc
    pvcNamespace: default
    pvcPath: <sourcePodUID>/<version>/<container>/checkpoint
  options:
    target-pvc-name: fast-local-pvc
    target-pvc-namespace: default
```

Here `<version>` is the checkpoint start timestamp, readable from the checkpoint CR at
`status.checkpointStatus.containerMigrationStatuses[].checkpointAttributes.path`; when
`checkpointUID` is given, the controller reads it for you.

After preheat completes (`status.phase=Ready`), the restore stage reads the same-path data from the
target PVC. Without a PreheatJob, restore reads directly from the source PVC.

### Automatic checkpoint: Pod annotations

When the source Pod carries the following annotations, the Pod Controller automatically creates a CR
that runs the checkpoint stage once all containers to be checkpointed are Running and Ready (it also
runs the restore stage when `restore-replicas` is a positive integer):

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: my-source-pod
  namespace: default
  annotations:
    # automatic checkpoint switch
    migration.io/container-state-checkpoint-enable: "true"
    # storage type: pvc
    migration.io/storage-type: "pvc"
    # PVC coordinates
    migration.io/storage-pvc-name: "oss-migration-pvc"
    migration.io/storage-pvc-namespace: "default"
    # containers to checkpoint
    migration.io/checkpoint-containers: "main"
    # Optional: checkpoint timeout (minutes); if set, passed to cr-tool --timeout;
    # if unset, no --timeout is added (works with automode auto-detection); an explicit 0 blocks
    # checkpoint and reports a warning on the CR
    migration.io/checkpoint-timeout-minutes: "30"
    # Optional: automode switch (true/false), default true: add --enable-auto-mode to cr-tool to
    # auto-detect a steady state of the model service; if false, checkpoint-timeout-minutes is required
    migration.io/checkpoint-automode-enable: "true"
spec:
  containers:
    - name: main
      image: nginx:latest
```

The automatically created CR is named `checkpoint-<podUID>` and lives in the same namespace as the
source Pod. When annotations are missing or invalid the CR is still created, and the error is written
back to the CR's `status.message` by the InitHandler (instead of leaving only an Event that may be
garbage collected).

## Configuration

### Controller flags

| Flag | Default | Description |
| --- | --- | --- |
| `--leader-election` | true | Enable leader election (one Lease drives the whole process; with multiple replicas only one runs the controllers) |
| `--pod-name` | POD_NAME environment variable (falls back to hostname) | Leader election identity |
| `--worker-namespace` | namespace of the controller | Where Worker Pods are created |
| `--enable-preheat-job` | false | Enable the PreheatJob subsystem (requires Dragonfly deployed in the cluster) |
| `--rpc-timeout` | 2m | Fallback timeout for Worker RPCs without their own deadline |

### Leader election

The controller is deployed as a Deployment (`deploy/controller-deployment.yaml`) with a default of one
replica. With multiple replicas, election uses the Lease named `lj-runtimekit-migration-controller` in
`kube-system`: only the replica that acquires the Lease starts the controllers, and the others stay in
standby; when the leader loses the Lease the process exits, and the Deployment starts a new replica to
run the election again. The Lease keeps being renewed during graceful shutdown so leadership is not
released prematurely while draining.

### Worker configuration (ConfigMap)

The Worker Pod template and gRPC port come from the ConfigMap
`kube-system/lj-runtimekit-migration-controller-config`:

- `grpcPort`: Worker gRPC port (per-job Workers are allocated ports starting at grpcPort+1).
- `template`: Worker Pod template (YAML). After modification the controller replaces the Workers on a
  node when it is idle (configuration drift detection) without interrupting running migration jobs.

## RBAC

All permissions required by the controller and the Worker are maintained in `deploy/rbac.yaml` and
`deploy/worker-rbac.yaml` (hand-assembled, not generated by controller-gen). Per-job Workers are
created in the PVC's namespace, which needs a ServiceAccount of the same name bound to the same
ClusterRole.

## Troubleshooting

| Symptom | What to check |
| --- | --- |
| CR stays Pending | `kubectl describe csmjr <name>` to inspect status.message and Events; check whether the PVC exists and whether restore requires RWX |
| checkpoint stuck in Checkpointing | Check whether the Worker on the source node is Running (`kubectl -n <ns> get pod \| grep lj-runtimekit-migration-worker`) and whether the source Pod containers are Running |
| restore replica fell back to a cold start | `status.restoreStatus.destinations[].phaseRetries` records the retry history per stage; cold start information is in the `coldStart` field |
| Worker keeps being recreated | Check whether the worker ConfigMap was modified (drift replacement is expected) or the `worker config` records in the controller logs |
| preheat job Failed | `kubectl describe phj <name>` to inspect status.message; confirm pvcPath contains no `..` segments and the target PVC is writable |

The controller's structured logs report each handler-chain round; every stage's duration and result
(CONTINUE / SKIP / ABORT / DONE) appears in the logs.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

This project is licensed under the [Apache License 2.0](LICENSE).
