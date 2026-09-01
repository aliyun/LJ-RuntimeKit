# LJ RuntimeKit Scheduler Framework 用户使用手册

## 1. 项目概述

LJ RuntimeKit Scheduler Framework 是一个基于 Kubernetes Scheduler Framework 的 GPU 调度器扩展项目。项目通过自定义 kube-scheduler 插件增强 Kubernetes 对 GPU 资源的精细化管理能力，适用于整卡 GPU、虚拟 GPU、NVIDIA MIG 以及多厂商 GPU 设备混合场景。

核心能力：

- **Standard Plugin**：支持 NVIDIA 整卡、AMD、PPU 资源，以及 vCore/vMemory 形式的虚拟 GPU 调度。
- **Mig Plugin**：支持 NVIDIA MIG（Multi-Instance GPU）GI/CI 两类切片资源调度。
- **拓扑感知调度**：针对 2/4/8 卡整卡任务或虚拟卡多副本任务，优先选择满足拓扑互联条件的 GPU 组。
- **调度策略**：通过节点标签支持 `binpack` 和 `spread` 两种 GPU 设备选择策略。
- **配套组件**：提供 scheduler、device-plugin、exporter、controller 等部署组件。

## 2. 技术栈与依赖

| 项目 | 说明 |
|------|------|
| Go 版本 | `go 1.17` |
| Kubernetes 依赖 | `k8s.io/* v0.21.2`，`k8s.io/kubernetes v1.21.2` |
| 调度框架 | Kubernetes Scheduler Framework |
| 默认镜像 | `registry.cn-hangzhou.aliyuncs.com/eflops-amp/lj-runtimekit-scheduler-framework:v1.0.0` |
| 默认命名空间 | `kube-system` |
| 调度器名称 | `lj-runtimekit-scheduler` |

调度器入口位于 `cmd/scheduler/main.go`，当前实际注册的插件为：

- `Standard`
- `Mig`

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
make docker REGISTRY=registry.cn-hangzhou.aliyuncs.com/eflops-amp VERSION=v1.0.0
make build GOOS=linux GOARCH=amd64
```

### 4.2 build.sh

`build.sh` 会完成镜像构建、镜像导出和 Helm 包打包：

```bash
bash build.sh [REGISTRY_NAME] [VERSION]
```

默认值：

```bash
REGISTRY_NAME=registry.cn-hangzhou.aliyuncs.com/eflops-amp
VERSION=v1.0.0
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

推荐使用 `deploy/deploy-all.sh` 部署或卸载组件：

```bash
# 部署默认组件：scheduler、device-plugin、exporter、controller
bash deploy/deploy-all.sh deploy

# 部署指定组件
bash deploy/deploy-all.sh deploy --components=scheduler
bash deploy/deploy-all.sh deploy --components=device-plugin,exporter

# 卸载默认组件
bash deploy/deploy-all.sh undeploy

# 卸载指定组件
bash deploy/deploy-all.sh undeploy --components=controller
```

可用组件：

| 组件 | 目录 | 部署方式 | 说明 |
|------|------|----------|------|
| scheduler | `deploy/scheduler` | `kubectl apply -f` | 自定义 kube-scheduler，启用 Standard/Mig/Gang 插件 |
| device-plugin | `deploy/deviceplugin` | `kubectl apply -f` | GPU 设备发布组件 |
| exporter | `deploy/exporter` | `kubectl apply -f` | GPU 指标采集组件，暴露 `9501` 端口 |
| controller | `deploy/controller` | `kubectl apply -f` | AMP Controller，管理 GPU 任务相关生命周期 |

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

### 5.3 Helm 部署

```bash
helm install lj-runtimekit-scheduler ./helm --namespace kube-system
```

`helm/values.yaml` 的关键配置：

```yaml
images:
   scheduler:
      image: "registry.cn-hangzhou.aliyuncs.com/eflops-amp/lj-runtimekit-scheduler-framework"
      tag: v1.0.0
      pullPolicy: IfNotPresent
```

## 6. 部署前置条件

- Kubernetes 集群与调度器依赖版本兼容，推荐 Kubernetes 1.21.x。
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
