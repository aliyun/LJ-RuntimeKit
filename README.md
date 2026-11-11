# LJ-RuntimeKit

LJ-RuntimeKit is a set of open-source Kubernetes components for managing heterogeneous accelerator resources (GPU, PPU, etc.) in cloud-native environments. It works alongside the LJ Runtime Kit daemon to provide device discovery, resource scheduling, monitoring, and lifecycle management.

## Documentation

See the [Official Documentation](https://help.aliyun.com/zh/intelligent-computing-lingjun/product-introduction-2) for product overview, architecture, and deployment guides.

## Components

| Component | Description |
|:--|:--|
| **LJ-RuntimeKit-Device-Plugin** | Kubernetes Device Plugin that registers accelerator devices with kubelet via gRPC, handling device discovery, health monitoring, and allocation. It also implements the ComputeDomain (DRA) plugin that allocates IMEX channels for multi-node memory sharing. |
| **LJ-RuntimeKit-Scheduler-Framework** | Out-of-tree Scheduler Framework plugin that provides topology-aware scoring, time-sharing scheduling, and MIG-aware placement for heterogeneous resources. It ships the Standard, Mig and Gang (all-or-nothing) plugins plus the unified deployment manifests, including `deploy/job-controller`. |
| **LJ-RuntimeKit-Exporter** | Prometheus exporter that collects accelerator metrics (utilization, memory, temperature, etc.) via gRPC and exposes them as standard Prometheus endpoints. |
| **LJ-RuntimeKit-Controller** | Kubernetes controller that watches node label changes and manages the enable/disable lifecycle of runtime components on target nodes via Job-based agents. |
| **LJ-RuntimeKit-JobController** | Controller for ComputeDomain / ComputeDomainClique (DRA). It installs the CRDs and DeviceClass, reconciles Clique membership and daemon endpoints, and generates the workload claim templates used by the PPU multi-node memory sharing (IMEX) path. |

## Quick Start

### Using Pre-built Images

Pre-built container images are available for each component. Deploy using the unified deployment script:

```bash
cd LJ-RuntimeKit-Scheduler-Framework

# Deploy all components (job-controller → device-plugin → scheduler → exporter → controller)
bash deploy/deploy-all.sh deploy

# Deploy specific components
bash deploy/deploy-all.sh deploy --components=device-plugin,scheduler,exporter,controller

# Skip the post-deploy readiness check (default timeout 180s, tunable via VERIFY_TIMEOUT)
bash deploy/deploy-all.sh deploy --skip-verify
```

The deployment script is located in the `LJ-RuntimeKit-Scheduler-Framework/deploy/` directory. `job-controller`
ships the ComputeDomain/ComputeDomainClique CRDs and DeviceClass that Device Plugin depends on, so it is applied
first and removed last. Its manifest references `lj-runtimekit-job-controller:1.13.20-k8s1.34` without a registry
prefix — push it to your registry or pre-load it on the nodes before deploying.

### Building from Source

Each component can be built independently:

```bash
cd LJ-RuntimeKit-<Component>
make build     # Build binary
make docker    # Build container image
```

See each component's README for detailed build instructions.

## Configuration Customization

Device Plugin, Scheduler Framework, and Exporter support lightweight configuration customization via ConfigMap without rebuilding images. Protocol constants such as extended resource names and node label keys are externalized in `protocol.yaml`, mounted into containers at `/etc/aliyun-gpu-protocol/protocol.yaml`.

Example — customize resource names:

```yaml
# protocol.yaml (ConfigMap data)
resources:
  gpu: "alibabacloud.com/gpu"
  gpu-core: "alibabacloud.com/gpu-core.percentage"
  gpu-memory: "alibabacloud.com/gpu-memory.percentage"
```

The ConfigMap is set to `optional: true` — components fall back to compiled defaults when it is absent. Changes must be applied consistently across all three components.

## Kubernetes Version Compatibility

The Scheduler Framework is compiled as an out-of-tree plugin against `k8s.io/kubernetes`. Use the provided script to switch target versions:

```bash
cd LJ-RuntimeKit-Scheduler-Framework
./hack/download-deps.sh v1.28.0
go mod tidy && go mod vendor
make build
```

Pre-built images are currently available for: **v1.28, v1.30, v1.32, v1.34, v1.35**.

`job-controller` and the ComputeDomain (DRA) path additionally require Kubernetes 1.34+ with the `resource.k8s.io`
API available.

## License

See [LICENSE](LICENSE) for details.
