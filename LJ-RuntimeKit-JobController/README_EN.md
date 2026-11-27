[简体中文](README.md) | **English**

# LJ-RuntimeKit Job Controller

JobController is a single service: entry point `cmd/controller`, image entrypoint `/job-controller`,
one Deployment, shared RBAC, and a leader-election Lease. Profiling, IMEX (controlled by the
`ComputeDomain` gate), and ActiveMonitoring (controlled by the `ActiveMonitoring` gate) are features
inside one process and cannot be deployed as several independent controllers running in parallel.
The collector is built separately in the sibling `LJ-RuntimeKit-Exporter` repository, created per
job by the Profiling feature; it is not a second controller.

## Unified Configuration and Feature Gates

- Defaults: source code, Makefile, and Deployment all use `ComputeDomain=false,Profiling=false,ActiveMonitoring=false`. Without an explicit `--feature-gates`, the process refuses to start because at least one feature must be enabled; this is expected. `make deploy` prints a WARNING and continues when no feature is enabled.
- At least one feature must be enabled explicitly before deployment, for example `make deploy FEATURE_GATES=ComputeDomain=true,Profiling=true,ActiveMonitoring=true`.
- Controller image: `lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-job-controller:v1.14.0-k8s1.34`. When releasing, always set `CONTROLLER_IMG` to the image that was actually built and can be pulled; do not reuse an old tag.
- Collector image: `lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-profiling-collector:v1.14.0-k8s1.34`.
- Deployment: `kube-system/lj-runtimekit-job-controller`; leader-election Lease: `kube-system/lj-runtimekit-job-controller`.
- Controller configuration ConfigMap: `kube-system/lj-runtimekit-jobcontroller-config`, template `deploy/lj-runtimekit-jobcontroller-configmap.yaml`. `COLLECTOR_CONFIGMAP` overrides the name and is used both for the rendered ConfigMap and the `--controllerConfig` startup argument.
- `NAMESPACE` defaults to `kube-system` and is used to render the Deployment, ServiceAccount, Lease RBAC, and collector ConfigMap; collector Pods are created in the namespace of the job PVC, while the ComputeDomainClique namespace remains `default`.
- HTTP port 6060: readiness `/healthz`, general metrics `/metrics`; when Profiling is enabled, `/metrics/profiling` is also served.

| Configuration | Features running |
|---|---|
| `ComputeDomain=false,Profiling=false,ActiveMonitoring=false` | Default: no feature enabled, the process refuses to start |
| `ComputeDomain=false,Profiling=true,ActiveMonitoring=true` | Profiling and ActiveMonitoring |
| `ComputeDomain=true,Profiling=true,ActiveMonitoring=true` | Profiling, IMEX, and ActiveMonitoring |
| `ComputeDomain=false,Profiling=true,ActiveMonitoring=false` | Profiling only |
| `ComputeDomain=true,Profiling=false,ActiveMonitoring=false` | IMEX only |
| `ComputeDomain=false,Profiling=false,ActiveMonitoring=true` | ActiveMonitoring only |

All combinations use the same build, deployment, and verification steps below; only `FEATURE_GATES`
changes. Disabling a feature does not delete CRDs, configuration, jobs, or data, nor does it create
that feature's informers or validate its dedicated runtime configuration. When every feature is
disabled the process refuses to start. Complete running jobs and their cleanup before disabling a
feature, otherwise finalizers may be left unprocessed.

## Build and Release

Go 1.24 or later, Docker, and registry push permission are required. The current controller Dockerfile
builds a static Linux/amd64 binary and uses a non-root runtime image.

Common Makefile targets:

| Command | Purpose |
|---|---|
| `make` / `make build` | Build `bin/job-controller`, Linux/amd64 by default; `BINARY`, `GOOS`, `GOARCH` are overridable |
| `make clean` | Remove only the artifact named by `BINARY`, keeping controller-gen and the Go cache |
| `make test` | Run the full race test suite |
| `make vet` | Run static checks |
| `make docker` | Build the controller image |
| `make deploy` | Install or update the controller in one flow, regardless of feature gates |
| `make undeploy` | Remove the controller, shared RBAC, and collector ConfigMap; keep CRDs, business resources, and data |
| `make manifests generate` | Generate the Profiling CRD and deepcopy code |

Running locally on macOS requires the host platform, for example `make build GOOS=darwin GOARCH=arm64`;
the image build is done by the Dockerfile itself and does not depend on `make build` output.

```sh
set -eu
VERSION=v1.14.0-k8s1.34 # example: use a new version for each release
REGISTRY=lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit
export CONTROLLER_IMG="$REGISTRY/lj-runtimekit-job-controller:$VERSION"
export COLLECTOR_IMG="$REGISTRY/lj-runtimekit-profiling-collector:v1.14.0-k8s1.34"
export NAMESPACE=kube-system
export FEATURE_GATES=ComputeDomain=true,Profiling=true,ActiveMonitoring=true # all disabled by default; enable explicitly before deployment
make test vet
DOCKER_DEFAULT_PLATFORM=linux/amd64 make docker
docker image inspect "$CONTROLLER_IMG" --format '{{.Os}}/{{.Architecture}} {{.Config.Entrypoint}} {{.Config.User}}'
docker push "$CONTROLLER_IMG"
```

The collector must be built from a confirmed Exporter source version. Do not simply point the
ConfigMap at a tag that does not exist, and do not retag an old image to pass it off as the latest
implementation:

```sh
set -eu
make -C ../LJ-RuntimeKit-Exporter test-collector
make -C ../LJ-RuntimeKit-Exporter docker-collector DOCKERCMD='docker build' \
  VERSION=v1.14.0-k8s1.34 \
  COLLECTOR_IMG=lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-profiling-collector:v1.14.0-k8s1.34
docker push lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-profiling-collector:v1.14.0-k8s1.34
```

## Unified Deployment and Update

First installation, routine updates, and feature switching all run the same `make deploy` with the
images, namespace, and gates exported above. Re-export these variables in a new terminal. Before
deploying, confirm the target context, that the namespace exists, and that the released image can be
pulled; check the Runtime Kit socket path and GPU resource names against
`deploy/lj-runtimekit-jobcontroller-configmap.yaml`. IMEX depends on the node Device Plugin, the IMEX
daemon, and DRA support; this repository only deploys the controller and its API/configuration and
does not replace node component installation.

```sh
set -eu
kubectl config current-context
make render-deployment render-rbac render-collector-config
make deploy
kubectl -n "$NAMESPACE" rollout status deployment/lj-runtimekit-job-controller --timeout=180s
kubectl -n "$NAMESPACE" get pods -l app=job-controller -o wide
kubectl -n "$NAMESPACE" logs deployment/lj-runtimekit-job-controller --tail=200
kubectl -n "$NAMESPACE" get lease lj-runtimekit-job-controller
```

`make deploy` does not branch by feature; it always performs:

1. Apply the six CRDs under `deploy/` and wait until Established: two ComputeDomain, two ComputeDomainClique, ProfilingJob, and ActiveMonitoring.
2. Apply the cluster-scoped PriorityClass `runtimekit-activemonitoring` (used to preempt for detection Pods; retained on uninstall).
3. Apply the collector ConfigMap.
4. Apply the full shared RBAC (both IMEX API groups, Profiling, ActiveMonitoring, and leader election).
5. Apply the single JobController Deployment; its `--feature-gates` decides which features run.

`make install` performs only the CRD application and wait described above, with `CRD_WAIT_TIMEOUT`
defaulting to `60s`. The installed resource set is the same even when only one feature runs. With
IMEX enabled, both `resource.aliyun.com` and `resource.nvidia.com` are served; a missing CRD in either
group causes startup failure. If existing CRDs are managed by another component, back them up and
confirm schema compatibility, and coordinate the update with the owner before applying; this flow
does not delete existing CRs, PVCs, or data.

All deployment resources live in `deploy/`: `profilingjobs-crd.yaml` is the generated Profiling CRD,
and `lj-runtimekit-jobcontroller-configmap.yaml` is the only collector configuration template, whose
name is rendered from `COLLECTOR_CONFIGMAP` at deployment time. `make manifests` uses controller-gen
`v0.16.5` to generate `config/crd/bases/profiling.runtimekit.io_profilingjobs.yaml` and syncs it to
the deployment CRD; tests verify the two stay identical. It does not generate or overwrite the IMEX
CRDs or the shared RBAC. Do not run `kubectl apply -f deploy/` directly: the directory also contains
user job examples and a DeviceClass example, and a normal deployment neither submits jobs nor creates
example resources.

To use a custom namespace, create it first, set `NAMESPACE` to the target name, and run the same
commands; ComputeDomainClique still uses `default`. Configuration-only updates also run the same
`make deploy`.

Verify readiness together with the feature configuration in the default logs, leader election, and
`All controllers ready`; check that there are no permission or informer errors. Non-leader replicas
in standby also pass the readiness check, so validate collection features against a real target
Pod/PVC after confirming the leader is ready; do not submit unmodified example jobs.

Before upgrading, check the Lease used by the old process: if it is also `lj-runtimekit-job-controller`,
the unified flow above performs a rolling update. An old `computedomain-controller` or a separate
Profiling Lease does not mutually exclude with the new Lease; you must complete running jobs, back up
the old Deployment, scale it to 0, and wait for the old Pods to exit completely before running the
same deployment command. The old `--collector-namespace` is removed; do not carry over incompatible
arguments. The current source, Makefile, and Deployment disable every feature by default; after an
upgrade you must explicitly set the features to enable in `FEATURE_GATES` (passing only some gates
does not restore the old defaults of the others). The deployment procedure is unchanged.

## Unified Uninstall

`make undeploy` performs the same operations for every feature combination: first delete the single
Deployment in the foreground and wait for its Pods to exit completely, then delete the shared RBAC and
the collector ConfigMap. Any failure or timeout stops the process; `UNDEPLOY_TIMEOUT` defaults to
`180s`. It does not delete CRDs, the PriorityClass, ProfilingJobs, ComputeDomains,
ComputeDomainCliques, PVCs, business Pods, or the leader-election Lease, and it does not perform any
bulk deletion of Profiling jobs or CRDs.

Before uninstalling, stop submitting new jobs, finish running Profiling jobs and the finalizer cleanup
of resources pending deletion, and back up the collector configuration. CRs retained after uninstall
are no longer reconciled; you cannot rely on the stopped controller to clean up collectors or
finalizers. To recover, use the same `make deploy`. The shared ClusterRole/Binding names are fixed; if
another deployment reuses this permission set, do not delete it.

```sh
kubectl config current-context
make -n undeploy NAMESPACE=kube-system # preview; custom deployments must reuse the original NAMESPACE and COLLECTOR_CONFIGMAP
make undeploy NAMESPACE=kube-system
```

## Legacy Deployment Migration Boundary

The old standalone controller used the Lease name `profilingjob.profiling.runtimekit.io`, while the
current unified controller uses `lj-runtimekit-job-controller` (`v1.0.0.1103.1` used
`computedomain-controller`). The two do not mutually exclude, so you cannot simply roll the old
Deployment's image or let both deployments manage ProfilingJobs at the same time; the old command-line
arguments are also incompatible with the new entry point. The current configuration name is set via
`--controllerConfig`; the old `--collector-configmap` argument is not carried over.

Before migrating, back up the old deployment, configuration, CRDs, and job records, complete running
jobs and finalizer cleanup, stop the old controller, and then perform the unified deployment. If the
old schema is incompatible, define a separate migration plan; the regular deployment and uninstall
flows in this project provide no bulk job or CRD cleanup entry point.
