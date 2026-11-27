**简体中文** | [English](README_EN.md)

# LJ-RuntimeKit Job Controller

JobController 是一个完整服务：统一入口 `cmd/controller`、镜像入口 `/job-controller`、一套 Deployment、共享 RBAC 和选主 Lease。Profiling、IMEX（对应 `ComputeDomain` 开关）与 ActiveMonitoring（对应 `ActiveMonitoring` 开关）是进程内的功能，不能作为多套独立 Controller 并行部署。Collector 在同级 `LJ-RuntimeKit-Exporter` 仓库独立构建，由 Profiling 功能按任务创建，不是第二个 Controller。

## 统一配置与功能开关

- 默认开关：源码、Makefile 与 Deployment 三处均为 `ComputeDomain=false,Profiling=false,ActiveMonitoring=false`；未显式传 `--feature-gates` 时进程因“至少启用一个功能”拒绝启动，属预期行为；`make deploy` 在未启用任何功能时会先打印 WARNING 再继续。
- 部署前必须显式启用至少一个功能，例如 `make deploy FEATURE_GATES=ComputeDomain=true,Profiling=true,ActiveMonitoring=true`。
- Controller 镜像：`lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-job-controller:v1.14.0-k8s1.34`；发布时统一通过 `CONTROLLER_IMG` 指定实际已构建、可拉取的新版本镜像，不复用旧 tag。
- Collector 镜像：`lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-profiling-collector:v1.14.0-k8s1.34`。
- Deployment：`kube-system/lj-runtimekit-job-controller`；选主 Lease：`kube-system/lj-runtimekit-job-controller`。
- Controller 配置 ConfigMap：`kube-system/lj-runtimekit-jobcontroller-config`，模板为 `deploy/lj-runtimekit-jobcontroller-configmap.yaml`。`COLLECTOR_CONFIGMAP` 可覆盖名称，同时渲染 ConfigMap 和启动参数 `--controllerConfig`。
- `NAMESPACE` 默认 `kube-system`，渲染 Deployment、ServiceAccount、Lease RBAC 和 Collector ConfigMap；Collector Pod 创建在任务 PVC 所在命名空间，ComputeDomainClique 命名空间仍为 `default`。
- HTTP 端口 6060：就绪 `/healthz`、通用指标 `/metrics`；启用 Profiling 时另有 `/metrics/profiling`。

| 配置 | 运行功能 |
|---|---|
| `ComputeDomain=false,Profiling=false,ActiveMonitoring=false` | 默认值：不启用任何功能，进程拒绝启动 |
| `ComputeDomain=false,Profiling=true,ActiveMonitoring=true` | Profiling 与 ActiveMonitoring |
| `ComputeDomain=true,Profiling=true,ActiveMonitoring=true` | Profiling、IMEX 与 ActiveMonitoring |
| `ComputeDomain=false,Profiling=true,ActiveMonitoring=false` | 仅 Profiling |
| `ComputeDomain=true,Profiling=false,ActiveMonitoring=false` | 仅 IMEX |
| `ComputeDomain=false,Profiling=false,ActiveMonitoring=true` | 仅 ActiveMonitoring |

所有组合都使用下文同一套构建、部署和验证操作，只改变 `FEATURE_GATES`。关闭功能不删除 CRD、配置、任务或数据，也不创建该功能的 informer、校验其专属运行配置。所有功能全部关闭时进程拒绝启动。关闭前先完成活动任务及清理，否则 finalizer 可能无人处理。

## 构建与发布

需要 Go 1.24 或更高版本、Docker、镜像仓库推送权限。当前 Controller Dockerfile 构建 Linux/amd64 静态二进制，使用非 root 运行时镜像。

常用 Makefile 操作：

| 命令 | 作用 |
|---|---|
| `make` / `make build` | 构建 `bin/job-controller`，默认 Linux/amd64；可覆盖 `BINARY`、`GOOS`、`GOARCH` |
| `make clean` | 仅删除 `BINARY` 指定的构建产物，保留 controller-gen 工具及 Go 缓存 |
| `make test` | 执行全量 race 测试 |
| `make vet` | 执行静态检查 |
| `make docker` | 构建 Controller 镜像 |
| `make deploy` | 统一安装、更新 Controller，不随功能开关拆分流程 |
| `make undeploy` | 卸载 Controller、共享 RBAC 和 Collector ConfigMap，保留 CRD、业务资源及数据 |
| `make manifests generate` | 生成 Profiling CRD 和 deepcopy 代码 |

macOS 本地运行需要指定本机平台，例如 `make build GOOS=darwin GOARCH=arm64`；镜像构建由 Dockerfile 独立完成，不依赖 `make build` 的产物。

```sh
set -eu
VERSION=v1.14.0-k8s1.34 # 示例：每次发布使用新的版本号
REGISTRY=lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit
export CONTROLLER_IMG="$REGISTRY/lj-runtimekit-job-controller:$VERSION"
export COLLECTOR_IMG="$REGISTRY/lj-runtimekit-profiling-collector:v1.14.0-k8s1.34"
export NAMESPACE=kube-system
export FEATURE_GATES=ComputeDomain=true,Profiling=true,ActiveMonitoring=true # 默认全关，部署前必须显式启用
make test vet
DOCKER_DEFAULT_PLATFORM=linux/amd64 make docker
docker image inspect "$CONTROLLER_IMG" --format '{{.Os}}/{{.Architecture}} {{.Config.Entrypoint}} {{.Config.User}}'
docker push "$CONTROLLER_IMG"
```

Collector 必须从已确认的 Exporter 源码版本构建，不要仅修改 ConfigMap 指向不存在的 tag，也不要将旧镜像重打 tag 冒充最新实现：

```sh
set -eu
make -C ../LJ-RuntimeKit-Exporter test-collector
make -C ../LJ-RuntimeKit-Exporter docker-collector DOCKERCMD='docker build' \
  VERSION=v1.14.0-k8s1.34 \
  COLLECTOR_IMG=lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-profiling-collector:v1.14.0-k8s1.34
docker push lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-profiling-collector:v1.14.0-k8s1.34
```

## 统一部署与更新

首次安装、日常更新和切换功能都执行同一条 `make deploy`，沿用上节导出的镜像、命名空间和开关参数。在新终端操作时重新设置这些变量。部署前确认目标 context、命名空间存在、实际发布的镜像可拉取；按 `deploy/lj-runtimekit-jobcontroller-configmap.yaml` 检查 Runtime Kit socket 路径和 GPU 资源名。IMEX 依赖节点 Device Plugin、IMEX daemon 和 DRA 支持，本仓库只部署控制器及其 API/配置，不代替节点组件安装。

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

`make deploy` 不按功能分支部署，固定执行：
1. 应用 `deploy/` 中 6 个 CRD，并等待 Established：两组 ComputeDomain、两组 ComputeDomainClique、ProfilingJob、ActiveMonitoring。
2. 应用集群级 PriorityClass `runtimekit-activemonitoring`（检测 Pod 让位用，卸载时保留）。
3. 应用 Collector ConfigMap。
4. 应用完整共享 RBAC（包含 IMEX 双 API group、Profiling、ActiveMonitoring、选主权限）。
5. 应用唯一的 JobController Deployment；其 `--feature-gates` 决定运行哪些功能。

`make install` 可单独执行上述 CRD 应用与等待，`CRD_WAIT_TIMEOUT` 默认 `60s`。即使只运行一个功能，安装资源集合也不变。IMEX 开启后同时服务 `resource.aliyun.com` 和 `resource.nvidia.com`，任何一组 CRD 缺失都会导致启动失败。现有 CRD 若由其他组件管理，应用前先备份并确认 schema 兼容、与管理方协调更新；此流程不删除已有 CR、PVC 或数据。

部署资源均在 `deploy/`：`profilingjobs-crd.yaml` 是生成的 Profiling CRD，`lj-runtimekit-jobcontroller-configmap.yaml` 是唯一的 Collector 配置模板，部署时其名称由 `COLLECTOR_CONFIGMAP` 渲染。`make manifests` 使用 controller-gen `v0.16.5` 生成 `config/crd/bases/profiling.runtimekit.io_profilingjobs.yaml`，同步到部署 CRD，测试校验两者一致；不生成或覆盖 IMEX CRD、共享 RBAC。不要直接执行 `kubectl apply -f deploy/`，目录还包含用户任务示例及 DeviceClass 示例，正常部署不自动提交任务或创建示例资源。

自定义命名空间时先创建命名空间，将 `NAMESPACE` 设置为目标名称后执行同样命令；ComputeDomainClique 仍使用 `default`。仅更新配置也执行同一条 `make deploy`。

验证就绪以及默认日志中的功能配置、选主、`All controllers ready`；检查没有权限或 informer 错误。非 leader 副本处于 standby 也能通过就绪检查，采集功能需在确认 leader 已就绪后用真实目标 Pod/PVC 验证，不能直接提交未修改的示例任务。

升级时先核对旧进程实际 Lease：同为 `lj-runtimekit-job-controller` 可使用上述统一流程滚动更新；旧 `computedomain-controller` 或独立 Profiling Lease 与新 Lease 不互斥，必须先完成活动任务、备份旧 Deployment、缩容到 0 并等待旧 Pod 完全退出，再执行同一部署命令。旧 `--collector-namespace` 已移除，不沿用不兼容参数。当前源码、Makefile 与 Deployment 默认关闭全部功能；升级后必须显式在 `FEATURE_GATES` 中设置要启用的功能（仅传入部分开关不会恢复其他功能的旧默认值），部署操作不变。

## 统一卸载

`make undeploy` 对所有功能开关组合执行相同操作：先前台删除唯一 Deployment 并等待其 Pod 完全退出，再删除共享 RBAC 和 Collector ConfigMap。任一步失败或超时都会停止；`UNDEPLOY_TIMEOUT` 默认 `180s`。不删除 CRD、PriorityClass、ProfilingJob、ComputeDomain、ComputeDomainClique、PVC、业务 Pod 或选主 Lease，不执行批量清空 Profiling 任务或 CRD 的操作。

卸载前停止提交新任务，完成活动 Profiling 任务及待删除资源的 finalizer 清理，并备份 Collector 配置。卸载后保留的 CR 不再调谐，不能依赖已停止的 Controller 清理 Collector 或 finalizer；需要恢复时使用同一套 `make deploy`。共享 ClusterRole/Binding 名称固定，其他部署若复用这套权限，不应删除。

```sh
kubectl config current-context
make -n undeploy NAMESPACE=kube-system # 预览；自定义部署需沿用原 NAMESPACE 和 COLLECTOR_CONFIGMAP
make undeploy NAMESPACE=kube-system
```

## 旧部署迁移边界

旧独立 Controller 的 Lease 名是 `profilingjob.profiling.runtimekit.io`，当前统一 Controller 使用 `lj-runtimekit-job-controller`（`v1.0.0.1103.1` 使用 `computedomain-controller`）。两者不能互相排斥，因此不能仅滚动替换旧 Deployment 镜像或让两套部署同时管理 ProfilingJob；旧命令行参数与新入口也不兼容。当前配置名称通过 `--controllerConfig` 指定，不沿用旧 `--collector-configmap` 参数。

迁移前备份旧部署、配置、CRD 和任务记录，完成活动任务及 finalizer 清理，停止旧 Controller 后再执行统一部署。旧 schema 不兼容时需要单独制定迁移方案，本项目的常规部署和卸载不提供批量清空任务或 CRD 的入口。
