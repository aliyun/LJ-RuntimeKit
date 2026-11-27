**简体中文** | [English](README_EN.md)

# Container Migration Controller

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Container Migration Controller 是一个 Kubernetes 控制器，用于在节点之间迁移容器状态（内存、文件系统与设备状态），实现无中断的容器热迁移。

当前支持的存储链路是 **PVC 直挂**：

```
checkpoint 到 PVC  →（可选）预热：从一个 PVC 复制到另一个 PVC  → restore 从 PVC 读取
```

其他存储后端（local / nfs / oss / dragonfly / efc）未来可能加入，本文档暂不展开。

## 目录

- [概述](#概述)
- [架构](#架构)
- [工作原理（PVC 链路）](#工作原理pvc-链路)
- [项目结构](#项目结构)
- [快速开始](#快速开始)
  - [前提条件](#前提条件)
  - [构建](#构建)
  - [部署](#部署)
  - [卸载](#卸载)
- [使用示例（PVC 链路）](#使用示例pvc-链路)
  - [示例一：完整 checkpoint + restore](#示例一完整-checkpoint--restore)
  - [示例二：仅 restore（引用已有 checkpoint）](#示例二仅-restore引用已有-checkpoint)
  - [示例三：可选的 PVC 到 PVC 预热](#示例三可选的-pvc-到-pvc-预热)
  - [自动 checkpoint：Pod 注解](#自动-checkpointpod-注解)
- [配置说明](#配置说明)
  - [Controller 参数](#controller-参数)
  - [Worker 配置（ConfigMap）](#worker-配置configmap)
- [RBAC](#rbac)
- [故障排查](#故障排查)
- [Contributing](#contributing)
- [License](#license)

## 概述

Container Migration Controller 管理两类 CRD：

1. **ContainerStateMigrationJobRequest**（简称 `csmjr`）：描述一次迁移任务，包含可选的
   checkpoint 阶段与可选的 restore 阶段。
2. **PreheatJob**（简称 `phj`，可选）：描述一次预热任务。预热是纯加速手段，
   不参与数据正确性，restore 可直接从源 PVC 读取，跳过预热。

控制器本身不直接操作容器：checkpoint / restore 的实际执行由节点上的
**Migration-Worker**（任务级 Pod）完成，控制器通过 gRPC 下发任务并轮询进度。

## 架构

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Kubernetes Cluster                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌────────────────────┐        ┌──────────────────────────────┐       │
│  │ Migration          │        │      源节点（源 Pod 所在）       │       │
│  │ Controller         │        │  ┌──────────┐  ┌───────────┐ │       │
│  │                    │        │  │  源 Pod   │  │ 任务级 Worker│ │       │
│  │  - MigrationJob    │ gRPC   │  └──────────┘  └─────┬─────┘ │       │
│  │    Controller      │───────▶│                       │ 挂载   │       │
│  │  - Pod Controller  │        │              ┌────────▼──────┐ │       │
│  │  - PreheatJob      │        │              │  源 PVC(/mnt/pv)│ │       │
│  │    Controller      │        │              │ checkpoint 数据│ │       │
│  │  - ConfigMap       │        │              └───────────────┘ │       │
│  │    Controller      │        │                                │       │
│  └────────────────────┘        └────────────────────────────────┘       │
│                                                                       │
│  ┌─────────────┐ 创建/轮询   ┌──────────────────────────────┐       │
│  │  PreheatJob  │───────────▶│  复制预热 Pod（一次性）          │       │
│  │   Controller │            │  源 PVC → 目标 PVC 同路径复制     │       │
│  └─────────────┘            └──────────────────────────────┘       │
│                                                                       │
│  ┌──────────────────────────────────────────────┐                    │
│  │            目标节点（副本 Pod 所在）             │                    │
│  │  ┌────────────┐        ┌─────────────────┐   │                    │
│  │  │ 任务级 Worker │◀─────│  副本 Pod         │   │                    │
│  │  │ (挂载 PVC)   │       │  (阻塞 init 放行)  │   │                    │
│  │  └────────────┘        └─────────────────┘   │                    │
│  └──────────────────────────────────────────────┘                    │
└─────────────────────────────────────────────────────────────────────┘
```

## 工作原理（PVC 链路）

一次完整的迁移按以下流程推进（每个阶段由 handler 链中的一个 handler 负责）：

1. **Init**：校验 CR 字段。PVC 模式要求：
   - 引用的 PVC 必须真实存在；
   - 带 restore 阶段的任务，PVC 必须为 RWX（restore 多副本可能分布在多个节点）；
   - checkpoint 容器列表必须恰好一个容器。
2. **Checkpoint**（可选）：控制器在源节点创建**任务级 Worker**（挂载 PVC 到固定
   挂载点 `/mnt/pv`），Worker 把源容器的 rootfs / memory / xpu 三份数据直接写入
   PVC。写入的目录相对路径形如 `{sourcePodUID}/{version}/{container}/checkpoint`，
   固化在 CR 的 `status.checkpointStatus` 中。
3. **预热**（可选，纯加速）：checkpoint 完成后，可创建 PreheatJob CR 把源 PVC 的
   checkpoint 目录预热到目标 PVC（copy 模式）或集群缓存。跳过预热时，restore
   直接从源 PVC 读取。预热不影响数据正确性。
4. **Restore**（可选）：
   - **CreatePods**：按源 Pod 模板重建副本 Pod（副本继承源 Pod 的
     `nodeSelector`，仅清除 `nodeName`）。Pod 中注入一个阻塞 init 容器，
     挡住主容器创建，直到数据就绪。
   - **PrepareData**：目标节点上的任务级 Worker 读取 PVC 中的 checkpoint 数据，
     解压到目标节点本地目录（数据准备失败按 `restore.retryCount` 重试，
     耗尽后回退冷启动）。
   - **ReleaseInit**：控制器通过 `pods/exec` 写信号文件放行 init 容器，
     主容器随即以 runc restore 方式从 checkpoint 恢复。
   - **WaitRestore**：确认主容器 Running 且恢复真实生效（向 Worker 查询
     `restoreAttempted` 凭证，防止"容器 Running 但实际是冷启动"的静默失败）。
   - **RestoreXPU**：Worker 转发本节点 XDM 的 cr-tool 恢复容器 XPU 设备状态。
5. **Clean**：任务进入终态后清理 Worker 侧任务记录；任务级 Worker 随阶段完成
   或 CR 删除被回收，释放 PVC 挂载与端口。

> 说明：checkpoint 与 restore 可在同一 CR 中声明（完整迁移），也可以只声明其一：
> 只填 `checkpoint` 是纯 checkpoint 任务（产物留在 PVC 中）；只填 `restore` 时
> 通过 `restore.checkpointUID` 引用历史 checkpoint CR。

## 项目结构

```
Migration-Controller/
├── api/
│   └── v1beta1/                        # CRD 类型定义（csmjr / preheatjob）
├── cmd/
│   ├── main.go                         # 控制器主程序
│   └── leader.go                       # Leader 选举包装（单 Lease 驱动全进程）
├── pkg/
│   ├── config/                         # 运行配置与默认值
│   ├── controllers/
│   │   ├── migrationjob/               # MigrationJob 主控制器
│   │   │   └── handler/                # handler 链（12 个阶段）
│   │   ├── pod/                        # Pod 监听，自动创建 checkpoint CR
│   │   ├── preheatjob/                 # PreheatJob 控制器与三种后端
│   │   │                              # （dragonfly / efc / copy）
│   │   ├── configmap/                  # Worker 配置 ConfigMap 热加载
│   │   └── context.go                  # Worker 客户端缓存与漂移检测
│   ├── migrationworker/                # Worker gRPC 客户端（含 proto 生成代码）
│   ├── dragonfly/                      # Dragonfly Manager/Scheduler 客户端
│   ├── preheat/                        # 预热后端抽象模型
│   ├── labels/                         # annotation / label 常量
│   ├── metrics/                        # Prometheus 指标
│   └── util/                           # 通用工具
├── proto/
│   └── container_state_migration.proto # Worker gRPC 协议定义
├── deploy/                             # CRD / RBAC / ConfigMap / Deployment（deploy.sh 一键部署）
├── helm/                               # Dragonfly 部署参考（可选）
├── Dockerfile                          # 多阶段构建
├── Makefile
└── PROJECT                             # Kubebuilder 项目文件
```

## 快速开始

### 前提条件

- Go 1.24+
- Docker
- kubectl（已配置集群访问）
- Kubernetes 集群 1.23+（目标集群版本，控制器与 CRD 均按 1.23 设计）
- 节点侧已部署迁移运行环境（Migration-Worker 镜像可拉取、XDM 与
  xpu-container-runtime 可用）

### 构建

```bash
# 构建宿主机二进制
make build

# 构建 Docker 镜像（默认指向 lj-runtimekit 私有仓库，可覆盖）
make docker
make docker IMG=<your-registry>/lj-runtimekit-migration-controller:<tag>
```

### 部署

控制器资源都在 `deploy/` 下，一条命令部署（CRD -> RBAC -> ConfigMap -> Deployment）：

```bash
# 1. 构建镜像（部署清单中的镜像名默认为
#    lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-migration-controller:v1.14.0-k8s1.34，
#    自行打同名 tag，或部署时用参数覆盖）
make docker IMG=lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-migration-controller:v1.14.0-k8s1.34

# 2. 部署（默认 namespace=kube-system，镜像名取清单默认值）
./deploy.sh deploy

# 同一命令支持覆盖 namespace 与镜像（也可以经环境变量 NAMESPACE /
# CONTROLLER_IMG / WORKER_IMG 传入，命令行参数优先）
./deploy.sh --namespace my-ns \
            --controller-img <registry>/lj-runtimekit-migration-controller:v1.1.0 \
            --worker-img <registry>/lj-runtimekit-migration-worker:v1.1.0 \
            deploy

# 可选：一并部署 Dragonfly P2P 预热系统
./deploy.sh deploy --with-dragonfly

# 查看完整用法
./deploy.sh --help
```

部署完成后确认控制器运行：

```bash
kubectl -n kube-system get deployment lj-runtimekit-migration-controller
kubectl -n kube-system get pod -l component=lj-runtimekit-migration-controller
kubectl -n kube-system logs -f -l component=lj-runtimekit-migration-controller
```

### 卸载

```bash
./deploy.sh uninstall

# 自定义过 namespace / 镜像时，卸载必须传同样的参数，脚本才能找到并删除资源
./deploy.sh --namespace my-ns uninstall

# 一并卸载 Dragonfly
./deploy.sh uninstall --with-dragonfly
```

## 使用示例（PVC 链路）

先准备一个 RWX PVC（例如 `oss-migration-pvc`）。以下示例全部使用 PVC 直挂链路。

### 示例一：完整 checkpoint + restore

checkpoint 到 PVC，再从同一 PVC restore 出 2 个副本：

```yaml
apiVersion: migration.io/v1beta1
kind: ContainerStateMigrationJobRequest
metadata:
  name: sample-migration
  namespace: default
spec:
  # 存储后端：pvc（当前文档只覆盖 PVC 链路）
  storageType: pvc
  storage:
    pvc:
      pvcName: oss-migration-pvc
      pvcNamespace: default

  # checkpoint 阶段：把源容器的状态写入 PVC
  checkpoint:
    sourcePodName: my-source-pod
    sourcePodNamespace: default
    sourcePodUID: "REPLACE-WITH-REAL-POD-UID"
    containers:            # 当前必须恰好一个容器
      - main
    timeoutInMinute: 30    # 填了才透传给 cr-tool 的 --timeout（分钟）；不填则不加 --timeout；填 0 也透传（cr-tool 语义 wait-then-abort）；负数报错
    enableCheckpointAutoMode: true  # 默认 true：cr-tool 加 --enable-auto-mode 自动探测稳态点；false 则不传且必须填 timeoutInMinute
    # 可选：细粒度排除策略
    containerCheckpointPolicy:
      main:
        excludeFilepath:
          - /tmp
          - /var/run

  # restore 阶段：从 PVC 恢复出副本 Pod
  restore:
    replicas: 2
    retryCount: 3          # 下载 / runc restore / XPU 恢复各自独立计数，
                           # 耗尽后该副本回退冷启动
```

应用后查看进度：

```bash
kubectl apply -f migration.yaml
kubectl get csmjr sample-migration -o yaml   # 查看 status.phase 与各阶段状态
kubectl get csmjr -w
```

### 示例二：仅 restore（引用已有 checkpoint）

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
    # 引用历史 checkpoint CR 的 metadata.uid（示例一中任务成功后拿到的 UID）
    checkpointUID: "3f8c1a54-0b7d-4c62-9a1e-7d5f2b6c8e90"
```

checkpointUID 可用以下命令获取：

```bash
kubectl get csmjr sample-migration -o jsonpath='{.metadata.uid}'
```

### 示例三：可选的 PVC 到 PVC 预热

预热是纯加速手段，可省略。copy 模式把源 PVC 的 checkpoint 目录整体复制到
目标 PVC 的**同路径**（例如目标 PVC 挂载在数据读取更快的节点上）：

```yaml
apiVersion: migration.io/v1beta1
kind: PreheatJob
metadata:
  name: sample-preheat
  namespace: default
spec:
  # 推荐：引用 checkpoint CR 的 metadata.uid，控制器自动解析源 PVC 坐标
  # 与 PVC 内 checkpoint 目录路径（无需手工拼接 version 时间戳）
  checkpointUID: "3f8c1a54-0b7d-4c62-9a1e-7d5f2b6c8e90"
  options:
    # copy 模式：目标 PVC 坐标（同时填写即选择复制预热）
    target-pvc-name: fast-local-pvc
    target-pvc-namespace: default
```

也可以不经 checkpoint CR、直接指定源 PVC 与目录相对路径：

```yaml
spec:
  # 预热源：源 PVC 与 PVC 内的 checkpoint 目录相对路径
  pvc:
    pvcName: oss-migration-pvc
    pvcNamespace: default
    pvcPath: <sourcePodUID>/<version>/<container>/checkpoint
  options:
    target-pvc-name: fast-local-pvc
    target-pvc-namespace: default
```

其中 `<version>` 是 checkpoint 发起时间戳，可从 checkpoint CR 的
`status.checkpointStatus.containerMigrationStatuses[].checkpointAttributes.path`
读取；填 `checkpointUID` 时控制器会代读。

预热完成后（`status.phase=Ready`），restore 阶段读取目标 PVC 同路径数据。
不创建 PreheatJob 时，restore 直接读源 PVC。

### 自动 checkpoint：Pod 注解

源 Pod 携带以下注解时，Pod Controller 会在其所有待 checkpoint 容器
Running 且 Ready 后，自动创建一条执行 checkpoint 阶段的 CR（声明
`restore-replicas` 正整数时同时执行 restore 阶段）：

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: my-source-pod
  namespace: default
  annotations:
    # 自动 checkpoint 开关
    migration.io/container-state-checkpoint-enable: "true"
    # 存储类型：pvc
    migration.io/storage-type: "pvc"
    # PVC 坐标
    migration.io/storage-pvc-name: "oss-migration-pvc"
    migration.io/storage-pvc-namespace: "default"
    # 待 checkpoint 容器
    migration.io/checkpoint-containers: "main"
    # 可选：checkpoint 超时（分钟），填了才透传给 cr-tool 的 --timeout；
    # 不填不加 --timeout（配合 automode 自动探测）；显式填 0 会拦截 checkpoint 并在 CR 上报 warning
    migration.io/checkpoint-timeout-minutes: "30"
    # 可选：自动模式开关（true/false），默认 true：cr-tool 加 --enable-auto-mode
    # 自动探测模型服务稳态点；false 时必须显式填 checkpoint-timeout-minutes
    migration.io/checkpoint-automode-enable: "true"
spec:
  containers:
    - name: main
      image: nginx:latest
```

自动创建的 CR 命名为 `checkpoint-<podUID>`，与源 Pod 同 namespace。
annotation 缺失或非法时 CR 仍会创建，错误由 InitHandler 写回 CR 的
`status.message`（而不是只留一条会被 GC 的 Event）。

## 配置说明

### Controller 参数

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `--leader-election` | true | 开启 Leader 选举（单 Lease 驱动全进程，多副本时只有一个副本运行控制器） |
| `--pod-name` | POD_NAME 环境变量（回退 hostname） | Leader 选举身份标识 |
| `--worker-namespace` | 控制器所在 namespace | Worker Pod 创建位置 |
| `--enable-preheat-job` | false | 打开 PreheatJob 子系统（需要集群已部署 Dragonfly） |
| `--rpc-timeout` | 2m | 未自带 deadline 的 Worker RPC 兜底超时 |

### Leader 选举

控制器以 Deployment 部署（`deploy/controller-deployment.yaml`），副本数默认 1。
多副本时通过 kube-system 下名为 `lj-runtimekit-migration-controller` 的 Lease 选举：
只有抢到 Lease 的副本会启动控制器，其余副本保持 standby；leader 失锁后进程
退出，由 Deployment 拉起新副本重新竞选。优雅停机期间持续续约 Lease，避免
排空过程中提前释放领导权。

### Worker 配置（ConfigMap）

Worker Pod 模板与 gRPC 端口来自 ConfigMap `kube-system/lj-runtimekit-migration-controller-config`：

- `grpcPort`：Worker gRPC 端口（任务级 Worker 从 grpcPort+1 起分配端口）。
- `template`：Worker Pod 模板（YAML）。修改后控制器会在节点空闲时滚动替换
  节点上的 Worker（配置漂移检测），不打断正在执行的迁移任务。

## RBAC

控制器与 Worker 所需权限全部在 `deploy/rbac.yaml` 与 `deploy/worker-rbac.yaml`
中维护（手工合订，不通过 controller-gen 生成）。任务级 Worker 创建在 PVC 所在
namespace，该 namespace 下需要同名的 ServiceAccount 并绑定同一个 ClusterRole。

## 故障排查

| 现象 | 排查方向 |
| --- | --- |
| CR 一直 Pending | `kubectl describe csmjr <name>` 查看 status.message 与 Events；检查 PVC 是否存在、restore 是否要求 RWX |
| checkpoint 卡在 Checkpointing | 检查源节点 Worker 是否 Running（`kubectl -n <ns> get pod \| grep lj-runtimekit-migration-worker`）、源 Pod 容器是否 Running |
| restore 副本回退冷启动 | `status.restoreStatus.destinations[].phaseRetries` 记录各阶段重试历史；冷启动信息在 `coldStart` 字段 |
| Worker 频繁重建 | 检查 worker ConfigMap 是否被改动（漂移替换是预期行为），或控制器日志中的 `worker config` 记录 |
| 预热任务 Failed | `kubectl describe phj <name>` 查看 status.message；确认 pvcPath 不含 `..` 段、目标 PVC 可写 |

控制器结构化日志按 handler 链输出每轮执行结果；每个阶段的耗时与结果
（CONTINUE / SKIP / ABORT / DONE）都会出现在日志中。

## Contributing

贡献指南见 [CONTRIBUTING.md](CONTRIBUTING.md)。

## License

本项目采用 [Apache License 2.0](LICENSE)。
