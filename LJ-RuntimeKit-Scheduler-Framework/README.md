**简体中文** | [English](README_EN.md)

# LJ RuntimeKit Scheduler Framework 用户使用手册

## 1. 项目概述

LJ RuntimeKit Scheduler Framework 是一个基于 Kubernetes Scheduler Framework 的 GPU 调度器扩展项目。项目通过自定义 kube-scheduler 插件增强 Kubernetes 对 GPU 资源的精细化管理能力，适用于整卡 GPU、虚拟 GPU、NVIDIA MIG 以及多厂商 GPU 设备混合场景。

核心能力：

- **Standard Plugin**：支持 NVIDIA 整卡、AMD、PPU 资源，以及 vCore/vMemory 形式的虚拟 GPU 调度。
- **Mig Plugin**：支持 NVIDIA MIG（Multi-Instance GPU）GI/CI 两类切片资源调度。
- **ActiveMonitoring Plugin**：支持对主动监控 Pod 的定向 GPU 调度，并让业务 Pod 偏好无主动监控 Pod 的节点和 GPU。
- **拓扑感知调度**：针对 2/4/8 卡整卡任务或虚拟卡多副本任务，优先选择满足拓扑互联条件的 GPU 组。
- **调度策略**：通过节点标签支持 `binpack` 和 `spread` 两种 GPU 设备选择策略。
- **配套组件**：提供 job-controller、device-plugin、scheduler、exporter、controller 等部署组件。

## 2. 技术栈与依赖

| 项目 | 说明 |
|------|------|
| Go 版本 | `go 1.21` |
| Kubernetes 依赖 | `k8s.io/* v0.28.0`，`k8s.io/kubernetes v1.28.0` |
| 调度框架 | Kubernetes Scheduler Framework |
| 默认镜像 | `lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-scheduler-framework:v1.14.0-k8s1.34` |
| 默认命名空间 | `kube-system` |
| 调度器名称 | `lj-runtimekit-scheduler` |

调度器入口位于 `cmd/scheduler/main.go`，当前实际注册的插件为：

- `Standard`
- `Mig`
- `ActiveMonitoring`

## 3. 调度架构

### 3.1 调度流程

```text
Pod 提交
  -> PreFilter(Standard)：构建集群 GPU 设备缓存，解析 Pod 资源请求
  -> Filter(Standard/Mig)：判断候选节点资源是否满足请求
  -> Score(Standard)：对 2/4/8 卡拓扑满足情况进行加分
  -> Reserve(Standard/Mig)：选择具体 GPU/MIG 资源并 Patch Pod 注解
  -> kube-scheduler 绑定 Pod
```

### 3.2 Standard Plugin

`Standard` 插件实现了 `PreFilter`、`Filter`、`Score`、`Reserve` 阶段：

- **PreFilter**：遍历调度器快照中的节点，基于节点 `Allocatable` 与注解构建 GPU 资源缓存。
- **Filter**：处理整卡、虚拟 GPU、CPU Pod 以及 MIG 转派逻辑，过滤资源不足节点。
- **Score**：对整卡或虚拟卡的 2/4/8 卡请求，检查节点是否存在可用拓扑组，满足时返回最高分。
- **Reserve**：根据节点策略 `node.gpu.placement` 选择具体设备，并将分配结果写入 Pod 注解。

### 3.3 Mig Plugin

`Mig` 插件实现了 `Filter`、`Reserve` 阶段：

- **GI 模式**：Pod 请求 `aliyun.com/gpu-slice`，且未请求 `aliyun.com/gpu-memory`。
- **CI 模式**：Pod 同时请求 `aliyun.com/gpu-slice` 和 `aliyun.com/gpu-memory`。
- Reserve 阶段会分配 MIG 掩码、拓扑起始位、拓扑长度和设备 ID，并 Patch 到 Pod 注解中。

### 3.4 ActiveMonitoring Plugin

`ActiveMonitoring` 插件实现了 `Filter`、`Reserve`、`Score` 阶段：

- **Filter**：检测 Pod 仅放行存在空闲 GPU minor 的节点。
- **Reserve**：按 `rt-diag-target-minor` 注解把检测 Pod 记账到对应 GPU minor，并 Patch `ALIYUN_COM_GPU_IDX_LIST` 等分配注解（语义与 Standard 整卡分支一致）。
- **Score**：业务 Pod 按节点卡级检测占用偏好打分，引导业务远离正在检测的卡。

## 4. 构建与打包

### 4.1 Makefile

```bash
make build         # 编译 kube-scheduler 二进制
make docker        # 构建调度器镜像
make helm-package  # 打包 helm chart 到 PACK 目录
make clean         # 清理本地构建产物
```

常用变量：

```bash
make docker REGISTRY=lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit VERSION=v1.14.0-k8s1.34
make build GOOS=linux GOARCH=amd64
```

### 4.2 build.sh

`build.sh` 会完成镜像构建、镜像导出和 Helm 包打包：

```bash
bash build.sh [REGISTRY_NAME] [VERSION]
```

默认值：

```bash
REGISTRY_NAME=lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit
VERSION=v1.14.0-k8s1.34
```

产物位于 `PACK/`：

- `lj-runtimekit-scheduler-framework-${VERSION}.tar`：调度器镜像 tar 包。
- `lj-runtimekit-scheduler-framework-helm-${VERSION}.tar.gz`：Helm chart 包。

打包时脚本会先复制 `helm/` 到 `PACK/helm`，只修改副本中的 `values.yaml`，不会修改源码目录下的 `helm/values.yaml`。

### 4.3 Dockerfile

项目使用多阶段构建：

- 构建阶段：`golang:1.17-stretch`
- 运行阶段：`debian:bullseye-slim`
- 输出二进制：`/bin/kube-scheduler`

## 5. 部署方式

### 5.1 一键部署脚本

部署脚本已从本组件目录移到仓库根目录的 `deploy/`，以下命令均需在仓库根目录执行（需要 Bash 4+ 和 kubectl）：

```bash
cd <仓库根目录>

# 部署默认组件（顺序：job-controller、device-plugin、scheduler、exporter、controller、migration-worker、migration-controller）
bash deploy/deploy-all.sh deploy

# 部署指定组件
bash deploy/deploy-all.sh deploy --components=job-controller
bash deploy/deploy-all.sh deploy --components=device-plugin,exporter
# 部署迁移组件：自动先安装 Worker 模板及 RBAC，再启动 Controller
bash deploy/deploy-all.sh deploy --components=migration-controller
# 仅更新 Worker 模板及 RBAC（不会创建常驻 Worker Pod）
bash deploy/deploy-all.sh deploy --components=migration-worker

# 卸载默认组件（按反序执行）
bash deploy/deploy-all.sh undeploy

# 卸载指定组件
bash deploy/deploy-all.sh undeploy --components=controller
bash deploy/deploy-all.sh undeploy --components=migration-controller,migration-worker

# 查看帮助（无需 kubectl）
bash deploy/deploy-all.sh help
```

可用组件：

| 组件 | 目录 | 部署方式 | 说明 |
|------|------|----------|------|
| job-controller | `deploy/job-controller` | `kubectl apply -f` | IMEX DRA、Profiling 与 ActiveMonitoring Job Controller，含 ComputeDomain/ProfilingJob/ActiveMonitoring CRD、主动监控 Pod PriorityClass、DeviceClass、RBAC、profiling collector ConfigMap 与 Deployment |
| device-plugin | `deploy/deviceplugin` | `kubectl apply -f` | GPU 设备发布组件 |
| scheduler | `deploy/scheduler` | `kubectl apply -f` | 自定义 kube-scheduler，启用 Standard/Mig/Gang 插件 |
| exporter | `deploy/exporter` | `kubectl apply -f` | GPU 指标采集组件，暴露 `9501` 端口 |
| controller | `deploy/controller` | `kubectl apply -f` | AMP Controller，管理 GPU 任务相关生命周期 |
| migration-worker | `deploy/migration-worker` | `kubectl apply -f` | Migration Worker 的 RBAC 与 Pod 模板 ConfigMap，Worker Pod 由 Migration Controller 按需创建 |
| migration-controller | `deploy/migration-controller` | `kubectl apply -f` | 迁移/预热 CRD、Controller RBAC 与 Controller Deployment |

注意事项：

- 指定组件列表会去重并按默认顺序执行；卸载时反序执行。仅在部署 `migration-controller` 时自动补入 `migration-worker`，卸载不会自动扩大选择范围。
- Migration 清单位于仓库根目录的 `deploy/migration-controller/` 与 `deploy/migration-worker/`；Migration-Worker 仓库没有独立常驻部署清单。两个镜像沿用上游 `lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-migration-{controller,worker}:v1.14.0-k8s1.34`，发布前仍须按实际发布版本核对或替换 `migration-controller/04-deployment.yaml` 与 `migration-worker/02-configmap.yaml` 中的镜像，不代表已验证镜像可拉取或包含最新源码。
- Migration Controller 启动前等待两个 `migration.io` CRD Established；部署后检查 Controller Deployment 就绪和 Worker 配置资产，不等待 Worker 常驻 Pod，不代表 checkpoint/restore/预热端到端验收通过。Controller 为单副本 Deployment（`--leader-election=true`），更新镜像走 Deployment 滚动更新。
- Worker 保留特权、hostNetwork 及 XDM/迁移目录/Dragonfly/EFC 的 hostPath 配置，需在目标节点预先准备对应运行环境。RBAC 默认覆盖 `kube-system`、`default` 两个 namespace；PVC 任务的 Worker 位于 PVC 所在 namespace，其他 namespace 需额外创建同名 `lj-runtimekit-migration-worker` ServiceAccount，并绑定同名 ClusterRole。
- Controller 沿用上游 `--enable-preheat-job` 参数，但脚本不部署 Dragonfly 或认证 Secret。使用 Dragonfly 后端前须另行准备其服务及 `dragonfly/dragonfly-preheat-pat` Secret；EFC、PVC 等后端也需单独满足存储前置条件。
- 卸载 Migration 会保留两个 CRD，不自动删除业务 CR、Worker Pod 或摘除 finalizer。操作前须停止提交新任务，在 Controller 仍运行时正常清理迁移/预热 CR 及 Worker；存在任意上述资源或查询失败都会阻止卸载。Controller 仍存在时，不允许单独卸载 Worker 配置；卸载两者请同时选择两个组件。
- 每个组件按目录内编号顺序 apply 顶层 `*.yaml`/`*.yml`，卸载时反序 delete；不扫描子目录，因此 `deploy/job-controller/examples/` 下的示例不会被自动 apply，需在 CRD 就绪后手工执行：

  ```bash
  kubectl apply -f deploy/job-controller/examples/example-computedomain.yaml
  kubectl apply -f deploy/job-controller/examples/example-computedomain-aliyun.yaml
  kubectl apply -f deploy/job-controller/examples/example-activemonitoring.yaml
  ```

- job-controller 镜像为 `lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-job-controller:v1.14.0-k8s1.34`，`imagePullPolicy` 为 `IfNotPresent`，需提前准备：离线环境先 `docker load`，私有仓库先推送镜像并修改 YAML 中的镜像地址。
- job-controller 通过 `--feature-gates=ComputeDomain=true,Profiling=true,ActiveMonitoring=true` 启用 IMEX、Profiling 与 ActiveMonitoring，镜像需包含对应能力；collector 镜像、socket 路径等在 `deploy/job-controller/10-controller-configmap.yaml` 中修改。
- 卸载 job-controller 会删除其 CRD，集群中已有的 ComputeDomain、ProfilingJob 自定义资源会随之删除。

### 5.2 Scheduler YAML 部署

如果只部署 scheduler，可按编号文件顺序执行：

```bash
kubectl apply -f deploy/scheduler/01-rbac.yaml
kubectl apply -f deploy/scheduler/02-configmap.yaml
kubectl apply -f deploy/scheduler/03-deployment.yaml
```

当前调度器配置位于 `deploy/scheduler/02-configmap.yaml`，启用插件：

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
        - name: ActiveMonitoring
      score:
        enabled:
        - name: Standard
          weight: 100
        - name: Gang
          weight: 100
        - name: ActiveMonitoring
          weight: 20
      reserve:
        enabled:
        - name: Standard
        - name: Mig
        - name: Gang
        - name: ActiveMonitoring
      preBind:
        enabled:
        - name: Standard
      permit:
        enabled:
        - name: Gang
```

### 5.3 Helm 部署

```bash
helm install lj-runtimekit-scheduler ./helm --namespace kube-system
```

`helm/values.yaml` 的关键配置：

```yaml
images:
   scheduler:
      image: "lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-scheduler-framework"
      tag: v1.14.0-k8s1.34
      pullPolicy: IfNotPresent
```

## 6. 部署前置条件

- Kubernetes 集群与调度器依赖版本兼容，推荐 Kubernetes 1.28.x。
- 部署 `job-controller` 需要 Kubernetes 1.34 及 DRA（Dynamic Resource Allocation）能力。
- GPU 节点已安装驱动与运行时依赖。
- device-plugin 和 exporter 节点需要打标签：

```bash
kubectl label node <node-name> lj_runtimekit_enable=true
```

- 如使用 MIG 调度，节点需提供 `aliyun.com/device` 注解描述 MIG 能力和拓扑信息。
- 使用本调度器的 Pod 需设置：

```yaml
spec:
  schedulerName: lj-runtimekit-scheduler
```

## 7. 资源、标签与注解参考

### 7.1 Pod 资源请求

| 资源键 | 说明 | 示例 |
|--------|------|------|
| `nvidia.com/gpu` | NVIDIA 整卡数量 | `"1"` |
| `aliyun.com/amd` | AMD GPU 整卡数量 | `"1"` |
| `aliyun.com/ppu` | PPU 整卡数量 | `"1"` |
| `aliyun.com/gpu-memory` | 虚拟 GPU 显存，单位 GB | `"40"` |
| `aliyun.com/gpu-core` | 虚拟 GPU 算力，`100` 表示 100% | `"50"` |
| `aliyun.com/gpu-slice` | MIG 切片数量 | `"1"` |

### 7.2 Pod 标签

| 标签 | 说明 | 示例 |
|------|------|------|
| `replicas` | 虚拟 GPU 副本数，未设置时默认为 1 | `"2"` |

### 7.3 Node 标签

| 标签 | 说明 | 可选值 |
|------|------|--------|
| `node.gpu.placement` | GPU 设备选择策略 | `binpack`、`spread` |
| `lj_runtimekit_enable` | device-plugin/exporter 部署节点选择标签 | `true` |

### 7.4 Node 注解

| 注解 | 说明 |
|------|------|
| `aliyun.com/device` | GPU/MIG 设备与拓扑信息 JSON |

### 7.5 调度器自动注入的 Pod 注解

| 注解 | 说明 |
|------|------|
| `ALIYUN_COM_GPU_IDX_LIST` | 分配到的 GPU 设备 ID 列表 |
| `ALIYUN_COM_GPU_COMPUTE_POD` | Pod 请求的算力值 |
| `ALIYUN_COM_GPU_MEM_POD` | Pod 请求的显存值 |
| `ALIYUN_COM_GPU_ASSIGNED` | 整卡资源分配标记 |
| `ALIYUN_COM_GPU_CORE_ASSIGNED` | 算力资源分配标记 |
| `ALIYUN_COM_GPU_MEM_ASSIGNED` | 显存资源分配标记 |
| `ALIYUN_COM_GPU_SLICE_ASSIGNED` | MIG 切片分配标记 |
| `ALIYUN_COM_GPU_MEM_ASSUME_TIME` | 调度分配时间戳 |
| `aliyun.com/device-enable` | MIG 资源启用标记 |
| `aliyun.com/mig-type` | MIG 类型，`gi` 或 `ci` |
| `aliyun.com/mig-mask` | MIG 资源掩码 |
| `aliyun.com/mig-topology-start` | MIG 拓扑起始位 |
| `aliyun.com/mig-topology-size` | MIG 拓扑长度 |
| `aliyun.com/mig-mutil-device` | MIG 多设备信息 |

## 8. 使用示例

### 8.1 整卡任务

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

### 8.2 虚拟 GPU 任务

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

说明：`gpu-core=50` 表示 50% 算力，`replicas=2` 表示需要 2 个满足该资源规格的虚拟 GPU 副本。

### 8.3 MIG GI 任务

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

### 8.4 MIG CI 任务

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

## 9. 验证与排查

```bash
# 查看调度器 Pod
kubectl get pods -n kube-system -l component=scheduler

# 查看调度器日志
kubectl logs -n kube-system -l component=scheduler --tail=100

# 查看 device-plugin
kubectl get ds -n kube-system lj-runtimekit-device-plugin-ds
kubectl logs -n kube-system -l name=lj-runtimekit-device-plugin-ds --tail=100

# 查看 exporter
kubectl get ds,svc -n kube-system -l app.kubernetes.io/name=lj-runtimekit-exporter
kubectl logs -n kube-system -l app.kubernetes.io/name=lj-runtimekit-exporter --tail=100

# 查看 controller
kubectl get deploy,pods -n kube-system -l app.kubernetes.io/name=amp-controller
```

常见检查项：

- Pod 是否设置 `schedulerName: lj-runtimekit-scheduler`。
- GPU 节点是否具备对应资源的 `Allocatable`。
- device-plugin/exporter 节点是否带有 `lj_runtimekit_enable=true` 标签。
- MIG 节点是否存在合法的 `aliyun.com/device` 注解。
- 多卡拓扑任务是否存在满足 2/4/8 卡要求的拓扑组。
- exporter 使用 `hostNetwork: true` 监听节点 `:9501`。若 exporter Pod 启动失败且日志出现 `bind: address already in use`，说明 9501 端口被其他进程占用，可在节点上执行以下命令排查：

  ```bash
  ss -ltnp | grep 9501
  # 或
  netstat -tlnp | grep 9501
  ```

## 10. 开源许可

本项目采用 Apache License 2.0，详见 `LICENSE` 文件。
