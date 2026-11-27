[简体中文](README.md) | **English**

# LJ-RuntimeKit-Controller

LJ-RuntimeKit-Controller is a Kubernetes-based controller that watches node label changes and
automatically toggles features on nodes through a Unix socket.

## Overview

LJ-RuntimeKit-Controller watches the label changes of every Node in the cluster:

- When a Node carries the `lj_runtimekit_enable: "true"` label, features on that node are enabled automatically.
- When the label is absent or its value is not `"true"`, features on that node are disabled automatically.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Kubernetes Cluster                       │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌─────────────────┐     ┌─────────────────────────────┐     │
│  │   Controller    │     │         Node A               │     │
│  │                 │     │  ┌─────────┐                 │     │
│  │  Watches Node   │────▶│  │ Job Pod │──▶ socket       │     │
│  │  Label Changes  │     │  └─────────┘  (enable/       │     │
│  │                 │     │               disable)       │     │
│  │  Creates Jobs   │     │                              │     │
│  └─────────────────┘     └─────────────────────────────┘     │
│                                                               │
│                         ┌─────────────────────────────┐       │
│                         │         Node B               │       │
│                         │  ┌─────────┐                 │       │
│                         │  │ Job Pod │──▶ socket       │       │
│                         │  └─────────┘                 │       │
│                         └─────────────────────────────┘       │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

## How It Works

1. The **Controller** runs in the cluster and continuously watches label changes of all Nodes.
2. When the `lj_runtimekit_enable: "true"` label appears, it creates a **Job** (action=enable) on that node.
3. When the label disappears or becomes something other than `"true"`, it creates a **Job** (action=disable) on that node.
4. The **Job** is scheduled onto the target node and mounts `/run/amp/eKitC/xdm/sockets/`.
5. The `job-agent` inside the Job sends enable/disable requests through `config-daemon.sock`.
6. Once the Job succeeds, the controller deletes it immediately; failed Jobs are retried automatically by the `OnFailure` restart policy.

## Project Layout

```
LJ-RuntimeKit-Controller/
├── api/
│   └── proto/           # protobuf definitions
│       └── config_service.proto
├── cmd/
│   ├── controller/      # controller main program (kubebuilder framework)
│   │   └── main.go
│   └── job-agent/       # agent program running inside Jobs
│       └── main.go
├── internal/
│   └── controller/      # controller logic
│       ├── node_controller.go   # Node label watching and Job creation
│       └── job_controller.go    # automatic cleanup of successful Jobs
├── pkg/
│   └── configpb/        # Go code generated from protobuf
│       ├── config_service.pb.go
│       └── config_service_grpc.pb.go
├── config/
│   ├── default/         # Kustomize default configuration
│   ├── manager/         # Deployment configuration
│   └── rbac/            # RBAC configuration
├── deploy/              # deployment scripts
│   └── deploy.sh        # deploy/undeploy/status/logs
├── scripts/             # build scripts
│   └── build.sh         # build script
├── Dockerfile           # controller image
├── Dockerfile.job-agent # job agent image
├── Makefile
├── PROJECT              # Kubebuilder project file
└── go.mod
```

## Quick Start

### Prerequisites

- Go 1.22.7+
- Docker
- kubectl (configured with cluster access)
- Kubernetes cluster 1.31+ (against `k8s.io/*` v0.31.4)

### Building

#### Build binaries only

```bash
./scripts/build.sh --binary-only
```

or

```bash
make build
```

#### Build Docker images

```bash
./scripts/build.sh
```

or

```bash
make docker-build
```

#### Build and push images

```bash
./scripts/build.sh --push
```

or with custom image names:

```bash
CONTROLLER_IMG=myregistry/lj-runtimekit-controller:v1.0.0 \
JOB_AGENT_IMG=myregistry/lj-runtimekit-controller-job-agent:v1.0.0 \
./scripts/build.sh --push
```

### Deployment

```bash
./deploy/deploy.sh deploy
```

or with a custom namespace and images:

```bash
./deploy/deploy.sh --namespace lj-runtimekit-system \
    --controller-img myregistry/lj-runtimekit-controller:latest \
    --job-agent-img myregistry/lj-runtimekit-controller-job-agent:latest \
    deploy
```

### Uninstall

```bash
./deploy/deploy.sh undeploy
```

### Check status

```bash
./deploy/deploy.sh status
```

### View logs

```bash
./deploy/deploy.sh logs
```

## Usage Examples

### 1. Deploy the controller

```bash
# Build and push the images to a registry
./scripts/build.sh --push

# Deploy to the cluster
./deploy/deploy.sh deploy
```

### 2. Test the feature

Label a node to trigger feature enabling:

```bash
kubectl label node <node-name> lj_runtimekit_enable=true
```

Check the controller logs:

```bash
kubectl logs -n lj-runtimekit-system -l control-plane=controller-manager
```

Check the created Jobs:

```bash
kubectl get jobs -n lj-runtimekit-system
```

Remove the label to trigger feature disabling:

```bash
kubectl label node <node-name> lj_runtimekit_enable-
```

## Configuration Options

### Controller Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--metrics-bind-address` | `:8080` | Metrics server listen address |
| `--health-probe-bind-address` | `:8081` | Health probe listen address |
| `--leader-elect` | `false` | Enable leader election |
| `--job-namespace` | `lj-runtimekit-system` | Namespace where Jobs are created |
| `--job-image` | `lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-controller-job-agent:v1.14.0-k8s1.34` | Job agent image |

### Job Agent Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--action` | (required) | Operation type: `enable` or `disable` |
| `--socket` | `/run/amp/eKitC/xdm/sockets/config-daemon.sock` | Socket file path |
| `--timeout` | `30s` | Connection timeout |

## gRPC Protocol

The Job Agent establishes a gRPC connection with the node-side config-daemon over a Unix socket. The
proto definition lives in [api/proto/config_service.proto], and the generated Go code lives in
[pkg/configpb].

### Service definition

```protobuf
service AmpConfigService {
  rpc Enable(EnableRequest) returns (EnableResponse);
  rpc Disable(DisableRequest) returns (DisableResponse);
  rpc QueryStatus(QueryStatusRequest) returns (QueryStatusResponse);
}
```

### Enable/Disable response

| Field | Type | Description |
|-------|------|-------------|
| `status` | int32 | `0` means success; any other value means failure |
| `message` | string | Result description |
| `stdout` | string | Standard output |
| `stderr` | string | Standard error |
| `exit_code` | int32 | Process exit code |
| `duration_ms` | int64 | Execution duration in milliseconds |

### Regenerating protobuf code

```bash
make proto
```

or run directly:

```bash
protoc --go_out=. --go_opt=module=github.com/lj-runtimekit/controller \
  --go-grpc_out=. --go-grpc_opt=module=github.com/lj-runtimekit/controller \
  api/proto/*.proto
```

## RBAC Permissions

The controller requires the following permissions:

- **Nodes**: get, list, watch
- **Jobs**: get, list, watch, create, delete
- **Events**: create, patch
- **Leases**: get, list, watch, create, update, patch, delete (for leader election)

## Troubleshooting

### Job creation fails

Check the controller logs:
```bash
kubectl logs -n lj-runtimekit-system -l control-plane=controller-manager
```

Check whether permissions are sufficient:
```bash
kubectl auth can-i create jobs.batch -n lj-runtimekit-system --as=system:serviceaccount:lj-runtimekit-system:lj-runtimekit-controller
```

### Socket connection fails

Check whether the socket file exists:
```bash
# Run on the target node
ls -la /run/amp/eKitC/xdm/sockets/config-daemon.sock
```

Check the Job Pod status:
```bash
kubectl get pods -n lj-runtimekit-system -l app.kubernetes.io/managed-by=lj-runtimekit-controller
kubectl describe pod -n lj-runtimekit-system <pod-name>
kubectl logs -n lj-runtimekit-system <pod-name>
```

### Job stuck in Pending

Check whether the target node is available:
```bash
kubectl get nodes -l lj_runtimekit_enable=true
kubectl describe job -n lj-runtimekit-system <job-name>
```

## Development

### Run the controller locally

```bash
make run
```

### Run tests

```bash
make test
```

### Format code

```bash
make fmt
```

### Static checks

```bash
make vet
```

## License

Apache License 2.0
