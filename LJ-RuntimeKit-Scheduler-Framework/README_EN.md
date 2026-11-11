# LJ RuntimeKit Scheduler Framework User Manual

## 1. Project Overview

LJ RuntimeKit Scheduler Framework is a GPU scheduling extension based on the Kubernetes Scheduler Framework. It extends kube-scheduler with custom plugins to provide fine-grained GPU resource management for whole-card GPUs, virtual GPUs, NVIDIA MIG, and heterogeneous GPU devices.

Key capabilities:

- **Standard Plugin**: Supports NVIDIA whole-card GPUs, AMD GPUs, PPU devices, and virtual GPU scheduling through vCore/vMemory resources.
- **Mig Plugin**: Supports NVIDIA MIG (Multi-Instance GPU) scheduling for GI and CI resource modes.
- **Topology-aware scheduling**: For 2/4/8-card whole-GPU requests or virtual-GPU multi-replica requests, the scheduler prefers GPU groups that satisfy topology connectivity requirements.
- **Placement strategies**: Supports `binpack` and `spread` device selection strategies through node labels.
- **Deployment components**: Provides job-controller, device-plugin, scheduler, exporter, and controller deployment manifests.

## 2. Technology Stack and Dependencies

| Item | Description |
|------|-------------|
| Go version | `go 1.17` |
| Kubernetes dependencies | `k8s.io/* v0.21.2`, `k8s.io/kubernetes v1.21.2` |
| Scheduling framework | Kubernetes Scheduler Framework |
| Default image | `registry.cn-hangzhou.aliyuncs.com/eflops-amp/lj-runtimekit-scheduler-framework:v1.0.0` |
| Default namespace | `kube-system` |
| Scheduler name | `lj-runtimekit-scheduler` |

The scheduler entry point is `cmd/scheduler/main.go`. The currently registered plugins are:

- `Standard`
- `Mig`

## 3. Scheduling Architecture

### 3.1 Scheduling Flow

```text
Pod submission
  -> PreFilter(Standard): build cluster GPU cache and parse Pod resource requests
  -> Filter(Standard/Mig): filter nodes by available GPU/MIG resources
  -> Score(Standard): boost nodes that satisfy 2/4/8-card topology requirements
  -> Reserve(Standard/Mig): select concrete GPU/MIG resources and patch Pod annotations
  -> kube-scheduler binds the Pod
```

### 3.2 Standard Plugin

The `Standard` plugin implements `PreFilter`, `Filter`, `Score`, and `Reserve`:

- **PreFilter**: Iterates over scheduler snapshot nodes and builds GPU resource cache from node `Allocatable` resources and annotations.
- **Filter**: Handles whole-card GPU tasks, virtual GPU tasks, CPU-only Pods, and MIG handoff logic.
- **Score**: For whole-card or virtual-card 2/4/8-card requests, checks whether the node has a usable topology group and returns the highest score when matched.
- **Reserve**: Selects concrete devices based on `node.gpu.placement` and patches allocation results into Pod annotations.

### 3.3 Mig Plugin

The `Mig` plugin implements `Filter` and `Reserve`:

- **GI mode**: The Pod requests `aliyun.com/gpu-slice` without `aliyun.com/gpu-memory`.
- **CI mode**: The Pod requests both `aliyun.com/gpu-slice` and `aliyun.com/gpu-memory`.
- During Reserve, the plugin assigns a MIG mask, topology start, topology size, and device ID, then patches them into Pod annotations.

## 4. Build and Package

### 4.1 Makefile

```bash
make build         # Compile kube-scheduler binary
make docker        # Build scheduler image
make helm-package  # Package Helm chart into PACK
make clean         # Remove local build artifacts
```

Common variables:

```bash
make docker REGISTRY=registry.cn-hangzhou.aliyuncs.com/eflops-amp VERSION=v1.0.0
make build GOOS=linux GOARCH=amd64
```

### 4.2 build.sh

`build.sh` builds the image, exports the image tarball, and packages the Helm chart:

```bash
bash build.sh [REGISTRY_NAME] [VERSION]
```

Default values:

```bash
REGISTRY_NAME=registry.cn-hangzhou.aliyuncs.com/eflops-amp
VERSION=v1.0.0
```

Artifacts are generated under `PACK/`:

- `lj-runtimekit-scheduler-framework-${VERSION}.tar`: scheduler image tarball.
- `lj-runtimekit-scheduler-framework-helm-${VERSION}.tar.gz`: Helm chart package.

During packaging, the script first copies `helm/` into `PACK/helm`, modifies only the copied `values.yaml`, and never mutates the source `helm/values.yaml`.

### 4.3 Dockerfile

The project uses a multi-stage Docker build:

- Build stage: `golang:1.17-stretch`
- Runtime stage: `debian:bullseye-slim`
- Runtime binary: `/bin/kube-scheduler`

## 5. Deployment

### 5.1 One-Click Deployment Script

Use `deploy/deploy-all.sh` to deploy or remove components:

```bash
# Deploy the default components in order:
# job-controller, device-plugin, scheduler, exporter, controller
bash deploy/deploy-all.sh deploy

# Deploy selected components
bash deploy/deploy-all.sh deploy --components=job-controller
bash deploy/deploy-all.sh deploy --components=device-plugin,exporter

# Remove the default components (reverse order)
bash deploy/deploy-all.sh undeploy

# Remove selected components
bash deploy/deploy-all.sh undeploy --components=controller

# Show help (kubectl not required)
bash deploy/deploy-all.sh help
```

Available components:

| Component | Directory | Deployment method | Description |
|-----------|-----------|-------------------|-------------|
| job-controller | `deploy/job-controller` | `kubectl apply -f` | IMEX DRA Job Controller: ComputeDomain CRDs, DeviceClass, RBAC, and Deployment |
| device-plugin | `deploy/deviceplugin` | `kubectl apply -f` | GPU device publishing component |
| scheduler | `deploy/scheduler` | `kubectl apply -f` | Custom kube-scheduler with Standard/Mig/Gang plugins |
| exporter | `deploy/exporter` | `kubectl apply -f` | GPU metrics exporter, exposes port `9501` |
| controller | `deploy/controller` | `kubectl apply -f` | AMP Controller for GPU task lifecycle management |

Notes:

- Each component applies the top-level `*.yaml`/`*.yml` files of its own directory in numbered order and deletes them in reverse order; subdirectories are not scanned, so the examples under `deploy/job-controller/examples/` are never applied automatically and must be applied by hand once the CRDs exist:

  ```bash
  kubectl apply -f deploy/job-controller/examples/example-computedomain.yaml
  kubectl apply -f deploy/job-controller/examples/example-computedomain-aliyun.yaml
  ```

- The job-controller image is `lj-runtimekit-job-controller:1.13.20-k8s1.34` with `imagePullPolicy: IfNotPresent` and must be prepared in advance: `docker load` it for offline environments, or push it to a private registry and update the image reference in the YAML.

### 5.2 Scheduler YAML Deployment

To deploy only the scheduler, apply the numbered manifests in order:

```bash
kubectl apply -f deploy/scheduler/01-rbac.yaml
kubectl apply -f deploy/scheduler/02-configmap.yaml
kubectl apply -f deploy/scheduler/03-deployment.yaml
```

The scheduler configuration is defined in `deploy/scheduler/02-configmap.yaml` and enables the following plugins:

```yaml
profiles:
  - schedulerName: lj-runtimekit-scheduler
    plugins:
      queueSort:
        enabled:
        - name: Gang
        disabled:
        - name: PrioritySort
      preFilter:
        enabled:
        - name: Standard
        - name: Gang
      filter:
        enabled:
        - name: Standard
        - name: Mig
        - name: Gang
      score:
        enabled:
        - name: Standard
          weight: 100
        - name: Gang
          weight: 100
      reserve:
        enabled:
        - name: Standard
        - name: Mig
        - name: Gang
      preBind:
        enabled:
        - name: Standard
      permit:
        enabled:
        - name: Gang
```

### 5.3 Helm Deployment

```bash
helm install lj-runtimekit-scheduler ./helm --namespace kube-system
```

Key settings in `helm/values.yaml`:

```yaml
images:
   scheduler:
      image: "registry.cn-hangzhou.aliyuncs.com/eflops-amp/lj-runtimekit-scheduler-framework"
      tag: v1.0.0
      pullPolicy: IfNotPresent
```

## 6. Prerequisites

- Kubernetes cluster compatible with the scheduler dependency version; Kubernetes 1.21.x is recommended.
- Deploying `job-controller` requires Kubernetes 1.34 with DRA (Dynamic Resource Allocation).
- GPU drivers and runtime dependencies are installed on GPU nodes.
- Nodes that run device-plugin and exporter must be labeled:

```bash
kubectl label node <node-name> lj_runtimekit_enable=true
```

- For MIG scheduling, nodes must provide a valid `aliyun.com/device` annotation describing MIG capability and topology information.
- Pods scheduled by this scheduler must set:

```yaml
spec:
  schedulerName: lj-runtimekit-scheduler
```

## 7. Resources, Labels, and Annotations

### 7.1 Pod Resource Requests

| Resource key | Description | Example |
|--------------|-------------|---------|
| `nvidia.com/gpu` | NVIDIA whole-card GPU count | `"1"` |
| `aliyun.com/amd` | AMD whole-card GPU count | `"1"` |
| `aliyun.com/ppu` | PPU whole-card GPU count | `"1"` |
| `aliyun.com/gpu-memory` | Virtual GPU memory in GB | `"40"` |
| `aliyun.com/gpu-core` | Virtual GPU compute, where `100` means 100% | `"50"` |
| `aliyun.com/gpu-slice` | MIG slice count | `"1"` |

### 7.2 Pod Labels

| Label | Description | Example |
|-------|-------------|---------|
| `replicas` | Number of virtual GPU replicas; defaults to 1 when omitted | `"2"` |

### 7.3 Node Labels

| Label | Description | Values |
|-------|-------------|--------|
| `node.gpu.placement` | GPU device placement strategy | `binpack`, `spread` |
| `lj_runtimekit_enable` | Node selector label for device-plugin/exporter | `true` |

### 7.4 Node Annotations

| Annotation | Description |
|------------|-------------|
| `aliyun.com/device` | GPU/MIG device and topology information in JSON format |

### 7.5 Scheduler-Injected Pod Annotations

| Annotation | Description |
|------------|-------------|
| `ALIYUN_COM_GPU_IDX_LIST` | Allocated GPU device ID list |
| `ALIYUN_COM_GPU_COMPUTE_POD` | Pod requested compute value |
| `ALIYUN_COM_GPU_MEM_POD` | Pod requested memory value |
| `ALIYUN_COM_GPU_ASSIGNED` | Whole-card GPU assignment flag |
| `ALIYUN_COM_GPU_CORE_ASSIGNED` | Compute assignment flag |
| `ALIYUN_COM_GPU_MEM_ASSIGNED` | Memory assignment flag |
| `ALIYUN_COM_GPU_SLICE_ASSIGNED` | MIG slice assignment flag |
| `ALIYUN_COM_GPU_MEM_ASSUME_TIME` | Scheduling assignment timestamp |
| `aliyun.com/device-enable` | MIG resource enable flag |
| `aliyun.com/mig-type` | MIG type, `gi` or `ci` |
| `aliyun.com/mig-mask` | MIG resource mask |
| `aliyun.com/mig-topology-start` | MIG topology start bit |
| `aliyun.com/mig-topology-size` | MIG topology size |
| `aliyun.com/mig-mutil-device` | MIG multi-device information |

## 8. Usage Examples

### 8.1 Whole-Card GPU Pod

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: whole-gpu-example
spec:
  schedulerName: lj-runtimekit-scheduler
  containers:
  - name: app
    image: nvidia/cuda:11.0-base
    resources:
      limits:
        nvidia.com/gpu: "1"
```

### 8.2 Virtual GPU Pod

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: virtual-gpu-example
  labels:
    replicas: "2"
spec:
  schedulerName: lj-runtimekit-scheduler
  containers:
  - name: app
    image: nvidia/cuda:11.0-base
    resources:
      limits:
        aliyun.com/gpu-memory: "40"
        aliyun.com/gpu-core: "50"
```

`gpu-core=50` means 50% compute. `replicas=2` means the Pod needs two virtual GPU replicas that satisfy the requested resource shape.

### 8.3 MIG GI Pod

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: mig-gi-example
spec:
  schedulerName: lj-runtimekit-scheduler
  containers:
  - name: app
    image: nvidia/cuda:11.0-base
    resources:
      limits:
        aliyun.com/gpu-slice: "1"
```

### 8.4 MIG CI Pod

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: mig-ci-example
spec:
  schedulerName: lj-runtimekit-scheduler
  containers:
  - name: app
    image: nvidia/cuda:11.0-base
    resources:
      limits:
        aliyun.com/gpu-slice: "1"
        aliyun.com/gpu-memory: "20"
```

## 9. Verification and Troubleshooting

```bash
# Check scheduler Pod
kubectl get pods -n kube-system -l component=scheduler

# View scheduler logs
kubectl logs -n kube-system -l component=scheduler --tail=100

# Check device-plugin
kubectl get ds -n kube-system lj-runtimekit-device-plugin-ds
kubectl logs -n kube-system -l name=lj-runtimekit-device-plugin-ds --tail=100

# Check exporter
kubectl get ds,svc -n kube-system -l app.kubernetes.io/name=lj-runtimekit-exporter
kubectl logs -n kube-system -l app.kubernetes.io/name=lj-runtimekit-exporter --tail=100

# Check controller
kubectl get deploy,pods -n kube-system -l app.kubernetes.io/name=amp-controller
```

Common checks:

- Whether the Pod sets `schedulerName: lj-runtimekit-scheduler`.
- Whether GPU nodes expose expected resources in `Allocatable`.
- Whether device-plugin/exporter nodes have the `lj_runtimekit_enable=true` label.
- Whether MIG nodes have a valid `aliyun.com/device` annotation.
- Whether multi-card topology requests have a matching 2/4/8-card topology group.
- The exporter uses `hostNetwork: true` and listens on node port `:9501`. If the exporter Pod fails to start and the logs show `bind: address already in use`, port 9501 is occupied by another process. Check on the node with:

  ```bash
  ss -ltnp | grep 9501
  # or
  netstat -tlnp | grep 9501
  ```

## 10. License

This project is licensed under Apache License 2.0. See the `LICENSE` file for details.
