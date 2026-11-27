[简体中文](README.md) | **English**

# Migration Worker

Migration Worker is the node-side daemon of the container live migration project
(container-migration), driven over gRPC by
[container-migration-controller](https://github.com/container-migration/controller).
It does exactly one thing: **move data**.

- **Checkpoint side**: request the container's checkpoint data stream from the node-local
  X-Device-Manager (XDM) and write it straight into the storage backend.
- **Restore side**: pull the data back on the target node, unpack it into a local directory,
  and let the container runtime restore the container via runc restore.

The Worker never creates, starts, or restores containers — bringing the process up is the
job of `xpu-container-runtime`, which intercepts `runc create` on the target node and turns
it into a restore. This separation keeps the Worker small and stateless.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [How It Works (PVC Path)](#how-it-works-pvc-path)
- [gRPC API](#grpc-api)
- [Project Layout](#project-layout)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Metrics](#metrics)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

## Overview

At this stage the Worker supports one complete migration path — the **PVC path**:

1. **Checkpoint into a PVC**: the controller creates a per-job Worker on the source node,
   mounts the source PVC at the fixed path `/mnt/pv`, and the Worker writes the source
   container's rootfs / memory / xpu data into that PVC as chunked tar parts.
2. **Preheat (optional, pure acceleration)**: after the checkpoint completes, the data can
   optionally be preheated into another PVC or a cluster P2P cache (Dragonfly / EFC), or
   preheat can be skipped entirely and restore can read the PVC directly. Preheat does not
   participate in data correctness; it only affects speed.
3. **Restore from a PVC**: the per-job Worker on the target node mounts the PVC, reads the
   checkpoint data, and unpacks it concurrently into a node-local directory.

Other storage paths (direct OSS / EFC, etc.) may be added in the future and are not covered
here.

## Architecture

```
+-------------------------------------------------------------------+
|                        Kubernetes Cluster                         |
+-------------------------------------------------------------------+
|                                                                     |
|  +------------------+     gRPC     +----------------------------+   |
|  | Migration        |------------->| per-job Worker (1 per job)  |   |
|  | Controller       |              |  +- /mnt/pv  source/target  |   |
|  |                  |  create/     |  |           PVC mount      |   |
|  |  - job           |  delete      |  +- XDM unix socket (ckpt)  |   |
|  |    orchestration |------------->|  +- dfdaemon (P2P pull)     |   |
|  |  - preheat       |              |  +- gRPC :50051 (to ctrl)   |   |
|  |    orchestration |              +-------------+--------------+   |
|  +------------------+                            |                   |
|                                                  |                   |
|                       checkpoint write / read    |                   |
|                       +--------------------------v-------------+    |
|                       |           PVC (RWX)                    |    |
|                       |  {podUID}/{version}/{container}/       |    |
|                       |  checkpoint/{part-00.tar, ...}         |    |
|                       +----------------------------------------+    |
|                                                                     |
|  Preheat source Pod (optional, one-shot): serve /mnt/pv (http :50090) |
|     -> Dragonfly seed peer origin pull -> restore Worker pulls        |
|        over dfdaemon P2P                                              |
+-------------------------------------------------------------------+
```

Two modes in which the Worker runs:

- **Per-job Worker**: the controller creates one Pod per migration job (mounting the
  corresponding PVC); it is reclaimed as the stage advances or when the CR is deleted. Both
  checkpoint and restore run in this mode.
- **Preheat source Pod** (optional): in `--serve-http` mode it only serves `/mnt/pv` over
  HTTP as the origin for Dragonfly seed peers; it does not start gRPC or Prometheus. It is
  deleted once preheat is Ready, after which the restore Worker **doubles as the origin**
  (see below).

## How It Works (PVC Path)

### 1. Checkpoint: writing into the PVC

The controller creates a per-job Worker on the source node and mounts the source PVC at
`/mnt/pv`. Through the ContainerRuntime plugin in `pkg/runtime` (the only implementation is
XDM) the Worker obtains the container's checkpoint tar stream, packs rootfs / memory / xpu
data into separate **chunked tars** (`part-00.tar`, `part-01.tar`, …, each part being a tar
that carries the full directory skeleton and can be unpacked independently), and writes them
to `{podUID}/{version}/{container}/checkpoint/` in the PVC.

The chunked layout is what enables N-way concurrent unpacking on the restore side: each
stream independently downloads and unpacks its own part, so total time is roughly
`max(each stream)` rather than `sum`. Files split across parts (XDM's `XDM.split.*` PAX
records mark the slice ranges) have their metadata (setuid / xattr / mtime) fixed up after
all parts are unpacked.

### 2. Preheat (optional, pure acceleration)

After the checkpoint completes, restore can read through one of two preheat paths, selected
by the Worker flag `--pvc-preheat-backend` (global; invalid values are rejected at startup):

- **dragonfly (default)**: the restore Worker derives the name of the long-running preheat
  Service from `(pvcNamespace, pvcName, pvcPath)`, resolves its ClusterIP, and rebuilds a URL
  that is **byte-for-byte identical** to the one used during preheat (Dragonfly computes the
  task_id from the URL, so a single differing byte causes a silent cache miss), then pulls
  over the node-local dfdaemon P2P. Once the preheat source Pod is deleted, the restore
  Worker **doubles as the origin**: it mounts the same PVC and writes an EndpointSlice that
  registers itself with the long-running preheat Service. When P2P downloads of a part
  exhaust their retries, it falls back to reading `/mnt/pv` directly at part granularity.
- **efc**: the Worker derives the relative path prefix under the EFC mount from the PVC/PV,
  and restore prefers `/mnt/efc/<prefix>/<checkpointDir>`; on failure or hang it always falls
  back to reading the mounted PVC directly. EFC reads use a progress-aware timeout: it only
  declares a hang after consecutive zero-progress intervals, so slow reads are never killed.

When preheat is skipped, restore reads directly from the mounted PVC. Preheat is an
acceleration measure: any preheat anomaly (PreheatJob not ready, Service missing, P2P
failure) degrades to direct reads and does not affect correctness.

### 3. Restore: reading from the PVC

The per-job Worker on the target node mounts the PVC at `/mnt/pv` and unpacks the N parts in
the checkpoint directory concurrently into the target directory:

- The target directory is first unpacked into `<dstDir>-tmp` and atomically renamed only
  after everything succeeds; on failure only the tmp directory is cleaned up, leaving the
  target directory untouched.
- A `memory/` sentinel directory is created before unpacking: XDM's rootfs reaper decides
  whether a checkpoint may still be in use by checking for a `memory/` sibling of `rootfs/`,
  so with N-way out-of-order unpacking the sentinel must exist first — otherwise the reaper
  could delete an already written `rootfs/` while the download is still in progress.
- After all parts are unpacked, the Worker verifies that the union of slice ranges covers
  the original file length (catching missing or truncated parts) and then fixes up the
  metadata of the sliced files.
- Hang protection: the pure OSS fallback direct read uses a progress-aware timeout (the
  counter must show consecutive zero progress before cancellation), so slow reads are not
  killed by a hard timeout.

### 4. RestoreXPU

Once the main container is Running, the controller issues a RestoreXPU request; the Worker
forwards each container to the node-local XDM cr-tool to restore the XPU device state
(CPU-only containers are detected by XDM and return success immediately).

## gRPC API

The protocol is defined in `pkg/apis/container_state_migration.proto` (regenerate with
`make grpc-proto`). `ContainerStateMigrationService` has five RPCs:

| RPC | Purpose |
|-----|---------|
| `CheckpointToStorage` | Start the dump + upload pipeline, returning an ack immediately |
| `QueryCheckpointToStorageStatus` | Query per-container checkpoint attributes (path/size/duration/extras) |
| `RestoreFromStorage` | Start the download + unpack pipeline, returning an ack immediately |
| `QueryRestoreFromStorageStatus` | Query per-container restore attributes (including the restoreAttempted proof) |
| `CleanTask` | Clean up in-memory job records, optionally deleting written data |

`XpuDeviceManagerService` has a single **server-streaming** RPC, `Checkpoint`: the Worker acts
as the client and pulls checkpoint tar data from XDM chunk by chunk.

## Project Layout

```
Migration-Worker/
├── cmd/migration-worker/               # main program (cobra + klog + prometheus)
│   └── options/                        # all runtime flags and defaults
├── pkg/
│   ├── apis/                           # generated code for the controller and XDM protocols
│   ├── controller/                     # gRPC handlers, job scheduling, checkpoint/restore items
│   ├── runtime/                        # ContainerRuntime plugin layer (only implementation: XDM)
│   ├── storage/                        # storage backend plugins + PreheatJob parsing
│   │   ├── localfs.go                  # local plugin (direct PVC read/write goes here)
│   │   ├── pvc_preheat.go              # PVC composite storage for Dragonfly preheat
│   │   ├── pvc_preheat_source.go       # origin role (EndpointSlice registration)
│   │   ├── dragonfly.go / dragonfly_client.go  # Dragonfly P2P backend
│   │   └── oss.go / nfs.go             # OSS / NFS backends
│   └── util/
│       ├── archive/                    # chunked tar unpacking (UnpackPart concurrent unpack)
│       └── types.go                    # shared types such as CountingReader
├── Dockerfile                          # multi-stage build (no vendor, image runs as root)
├── Makefile
└── LICENSE / NOTICE
```

Storage backends are registered into a factory as plugins (`RegisterStoragePlugin`) and
selected by the controller through `storageType`. The PVC path is implemented by the
controller translating `storageType=pvc` into the local plugin (with the fixed mount point
`/mnt/pv`); `dragonfly` / `efc` are only acceleration paths for restore reads. Other backends
(`oss`, `nfs`) remain registered; their documentation will be added later.

## Quick Start

### Prerequisites

- Go 1.23+ (the build image is golang:1.24)
- Docker (to build the image)
- A Kubernetes cluster with XDM and (optionally) Dragonfly dfdaemon deployed on the nodes
- The controller deployed for the PVC path (it creates and reclaims per-job Workers)

### Building

```bash
# Host binary (Linux, statically linked)
make build

# Container image (multi-stage build; the image name defaults to the private
# lj-runtimekit registry and can be overridden)
make docker IMG=<registry>/<repo>:<tag>
```

| Target | Purpose |
|--------|---------|
| `make build` | Build the Linux binary into `bin/` |
| `make docker` | Build the container image (`IMG=...` overrides the image name) |
| `make test` | `go test ./...` (with coverage) |
| `make lint` | golangci-lint (`lint-fix` fixes automatically) |
| `make grpc-proto` | Regenerate the gRPC stubs |
| `make clean` | Clean `bin/`, `output/`, `cover.out` |

> The repository does not commit a `vendor/` directory; dependencies are resolved directly
> through the module proxy. Some tests under `pkg/util` rely on `lchown` and only pass when
> run as root on Linux; failures on macOS are expected.

### Running (local debugging)

The Worker is normally created as a per-job Pod by the controller's manifests, but it can be
started by hand for local debugging:

```bash
./bin/migration-worker \
  --grpc-port=50051 \
  --save-dir=/data/migration \
  --xdm-socket=/run/amp/eKitC/xdm/sockets/dp2mgr.sock \
  --pvc-preheat-backend=dragonfly
```

Runtime requirements:

- `KUBE_NODE_NAME` (injected via the downward API); startup fails if empty;
- the node-local XDM unix socket is reachable (`--xdm-socket`);
- `--save-dir` points to a host path visible to the target container runtime;
- PVC mode needs in-cluster configuration to look up the long-running preheat Service (a
  failure only logs a warning, and restore degrades to reading `/mnt/pv` directly);
- Dragonfly preheat requires the ServiceAccount to have `get` permission on `preheatjobs`.

## Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `--grpc-port` | `50051` | gRPC server listen port |
| `--save-dir` | `/home/admin/t4/migration` | Root directory for migration data |
| `--xdm-socket` | `/run/amp/eKitC/xdm/sockets/dp2mgr.sock` | XDM gRPC unix socket |
| `--checkpoint-parallelism` | `2` | Number of concurrent checkpoint jobs |
| `--restore-parallelism` | `6` | Number of concurrent restore jobs |
| `--http-client-timeout` | `2m` | Timeout for HTTP-style transfers |
| `--prometheus-port` | `9416` | `/metrics` port |
| `--serve-http` | `false` | Preheat source mode: only serve `/mnt/pv` over HTTP |
| `--http-port` | `50090` | File server port in serve-http mode (must match the controller-side constant) |
| `--pvc-preheat-backend` | `dragonfly` | Preheat path for PVC restore: `dragonfly` or `efc` |
| `--oss-part-size` | `30` | OSS multipart upload part size (MB) |
| `--oss-upload-threads` | `10` | OSS multipart upload thread count |
| `--dfdaemon-endpoint` | `/var/run/dragonfly/dfdaemon.sock` | dfdaemon address (unix path or host:port) |
| `--dragonfly-piece-size` | `0` | Dragonfly upload/download piece size (bytes); `0` lets dfdaemon decide (required for preheat cache hits) |
| `--dragonfly-reorder-buffer` | `512` | In-memory reorder buffer limit (MB); `0` is unlimited |
| `--dragonfly-piece-stall-timeout` | `90s` | Wait before hedging a missing piece; `0` disables |
| `--dragonfly-spill-limit` | `2048` | Hard limit for spilling unordered data to disk (MB); `0` is unlimited |

`migration-worker --help` prints the authoritative list.

## Metrics

Prometheus metrics are exposed on `/metrics` and all carry a node-name label: per-RPC
latency and status recorded by the gRPC interceptor, checkpoint latency by container and
payload size, restore latency by backend / file count / size, and checkpoint / restore queue
load.

## Troubleshooting

- **restore reports "no parts in directory"**: the checkpoint directory is not ready or the
  parts cannot be probed; confirm that the controller-side
  `CheckpointAttributes.FileCount` validation passed and that the PVC mount is healthy.
- **all P2P lookups miss and fall back to origin pull**: the URL differs from the one used
  during preheat, so the task_id does not match; check the derivation of the long-running
  preheat Service name (whether `pvcNamespace/pvcName/pvcPath` agree on both sides).
- **origin pull returns 403**: some dfdaemon versions use the URL with filtered signature
  query parameters for the origin pull; see the notes on `defaultFilteredQueryParams` in
  `pkg/storage/dragonfly_client.go`.
- **EFC reads hang indefinitely**: the progress-aware timeout only detects zero progress; a
  permanently blocked FUSE read falls back to the PVC after `efcNoProgressTimeout`; check the
  Worker logs to confirm whether it triggered.

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Apache 2.0, see [LICENSE](LICENSE). Third-party code attribution is in [NOTICE](NOTICE).
