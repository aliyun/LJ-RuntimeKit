# LJ-RuntimeKit

LJ-RuntimeKit is a set of open-source Kubernetes components for managing heterogeneous accelerator resources (GPU, PPU, etc.) in cloud-native environments. It works alongside the LJ Runtime Kit daemon to provide device discovery, resource scheduling, monitoring, and lifecycle management.

## Documentation

Product documentation for Lingjun Intelligent Computing:
<https://help.aliyun.com/zh/intelligent-computing-lingjun/product-introduction-2>

## Components

| Component | Description |
|:--|:--|
| **LJ-RuntimeKit-Device-Plugin** | Kubernetes Device Plugin that registers accelerator devices with kubelet via gRPC, handling device discovery, health monitoring, and allocation. |
| **LJ-RuntimeKit-Scheduler-Framework** | Out-of-tree Scheduler Framework plugin that provides topology-aware scoring, time-sharing scheduling, and MIG-aware placement for heterogeneous resources. |
| **LJ-RuntimeKit-Exporter** | Prometheus exporter that collects accelerator metrics (utilization, memory, temperature, etc.) via gRPC and exposes them as standard Prometheus endpoints. |
| **LJ-RuntimeKit-Controller** | Kubernetes controller that watches node label changes and manages the enable/disable lifecycle of runtime components on target nodes via Job-based agents. |
| **LJ-RuntimeKit-JobController** | Controller for profiling and ComputeDomain (DRA) jobs, including the node agent that runs them. |
| **Migration-Controller** | Control plane for container live migration, providing the `migration.io` CRDs and their controllers. |
| **Migration-Worker** | Data-plane worker pods that perform checkpoint and restore during container live migration. |

## Quick Start

### Using Pre-built Images

Pre-built container images are available for each component. Deploy using the unified deployment script:

```bash
# Deploy all components
./deploy/deploy-all.sh deploy

# Deploy specific components
./deploy/deploy-all.sh deploy --components=device-plugin,scheduler,exporter,controller
```

The deployment script and manifests are located in the `deploy/` directory at the repository root.

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
./hack/download-deps.sh v1.34.0
go mod tidy && go mod vendor
make build
```

Pre-built images are currently available for: **v1.34, v1.35, v1.37**.

## License

See [LICENSE](LICENSE) for details.
