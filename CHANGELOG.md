# Changelog

## v1.14.0

Building on v1.13.0, this release introduces five major capabilities and updates the open-source Kubernetes components and deployment assets. Resource-layer Runtime services and prebuilt container images are not included in the source release archive.

### Major capabilities

- **DRA + IMEX: multi-node GPU memory sharing (IMEX)**. Integrates IMEX resource orchestration and allocation through Kubernetes Dynamic Resource Allocation (DRA).
- **Logical spatial multiplexing (LDM)**.
- **Profiling & Tracing**.
- **Active Monitoring**.
- **Fast model startup (Checkpoint/Restore)**.

These capabilities require compatible node Runtime services, drivers, and Kubernetes components. Deploying the open-source components alone does not enable the corresponding node-side capabilities.

### Component additions and updates

- Adds JobController source code to manage ComputeDomain (IMEX), ProfilingJob (Profiling & Tracing), and ActiveMonitoring through a single service.
- Updates Device Plugin and Scheduler Framework with ComputeDomain / DRA integration, GPU topology handling, active monitoring, and RDMA topology-aware scheduling implementations.
- Updates Exporter with a profiling Collector and Node Problem Detector (NPD) health-check adapters.
- Adds Migration Controller and Migration Worker source code for fast model startup (Checkpoint/Restore), including CRDs for snapshot, restore, and preheat tasks, Worker lifecycle management, and storage backend integrations.
- Updates the node feature-switch Controller and Job Agent, component build entry points, and Chinese and English READMEs.

### Deployment changes

- Uses `deploy/deploy-all.sh` at the repository root as the unified deployment entry point. Manifests are applied in the order of their numeric filename prefixes.
- Deploys seven components by default: `job-controller`, `device-plugin`, `scheduler`, `exporter`, `controller`, `migration-worker`, and `migration-controller`.
- Automatically installs the Worker template and RBAC first when only `migration-controller` is selected. Worker Pods are created per task rather than running as a persistent DaemonSet.
- Runs Migration Controller as a Deployment and waits for migration and preheat CRDs to become `Established` after installation.
- Checks for existing task CRs and Worker Pods before migration component removal. Migration CRDs are retained, and finalizers are not forcibly removed. The unified deployment script does not install Dragonfly or authentication Secrets.
- Includes ComputeDomain / ComputeDomainClique CRDs for both `resource.aliyun.com` and `resource.nvidia.com`, plus ProfilingJob and ActiveMonitoring CRDs and related deployment assets in JobController.

### Compatibility and upgrade notes

- The README lists prebuilt images targeting Kubernetes v1.34, v1.35, and v1.37. Publishing the source code does not confirm that these images have been pulled or validated in the target environment.
- Deployment manifests use the `v1.14.0-k8s1.34` image tag by default. For other Kubernetes versions, update the manifests according to the image release naming convention. Pre-pulling images or setting shell variables does not automatically rewrite the manifests.
- When upgrading Device Plugin, update both the init container and main container images. Also update Controller's `--job-image`, and check JobController's Collector / monitoring images and the Migration Worker template image individually.
- Feature gates are disabled by default in the source code. The unified deployment manifests at the repository root explicitly enable `ComputeDomain` for Device Plugin and `ComputeDomain`, `Profiling`, and `ActiveMonitoring` for JobController. Component-specific `make deploy` targets use different feature-gate settings; confirm the deployment entry point before proceeding.
- Install the CRDs, RBAC, and ConfigMaps required by the selected capabilities when upgrading; replacing workload images alone is insufficient. The unified deployment script does not automatically apply `deploy/job-controller/examples/`.
- Keep protocol configuration consistent across Device Plugin, Scheduler, and Exporter. After updating ConfigMaps, restart workloads as required by each component. Running `kubectl apply` alone does not guarantee that running processes have loaded the new configuration.
- The unified deployment script requires Bash 4 or later. Prepare node labels, RuntimeClass, node Runtime services, and optional feature dependencies according to the component documentation.
- Component readiness checks do not replace end-to-end validation of DRA + IMEX, LDM, Profiling & Tracing, Active Monitoring, or Checkpoint/Restore. Default credentials in the Dragonfly reference manifests are examples only; replace them and restrict access in production.

Full source diff: [v1.13.0...v1.14.0](https://github.com/aliyun/LJ-RuntimeKit/compare/v1.13.0...v1.14.0).
