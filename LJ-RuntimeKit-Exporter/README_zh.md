[English](README.md) | **简体中文**

# LJ RuntimeKit Exporter

LJ RuntimeKit Exporter 是部署在单台服务器节点上、用于在线采集异构硬件相关信息与指标数据的软件，
专为数据中心场景设计。在企业内部，AMPerf 被认为是 GPU 性能指标最丰富、采集维度最全面、GPU 健康
诊断最准确且最迅速的在线方案。

## 功能特性

- 提供整卡指标，涵盖 GPU 运行状态、利用率、性能、基础健康等多个维度。
- 除提供容器内存与算力资源占用的标准指标外，配合 eGPU 还可输出容器粒度 GPU 利用率与性能指标。
- 在提供无感知整卡健康指标的基础上，AMPerf exporter 进一步提供更精细的健康监控能力，可在线检测
  GPU 软硬件故障，且对业务影响极小。

## 快速开始

LJ RuntimeKit Exporter 支持 RPM 与云原生两种部署方式，可通过启动参数或环境变量适配不同场景。

### 云原生部署配置

#### 前置条件

- 镜像地址：`lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-exporter`，
  镜像版本按实际需要选择。
- Helm 部署文件：离线提供 `deployment/` 目录。

#### Helm 配置项说明

- 端口配置：values.yaml 中的 `service:port` 用于配置容器对外暴露的端口，默认 9400；HTTP 地址同
  样可通过 `service:address` 配置。
- 镜像配置：values.yaml 中的 `image:repository` 与 `image:tag` 定义镜像仓库与版本。内网用户需先将
  镜像手动上传至私有仓库，并把 repository 和 tag 改为对应配置。
- 采集频率配置：values.yaml 中 extraEnv 的 `AMPERF_EXPORTER_INTERVAL` 环境变量，取值为采集频率
  （单位毫秒），默认 10000ms。
- 输出格式：values.yaml 中 extraEnv 的 `AMPERF_OUTPUT_FORMAT` 环境变量，取值为采集数据的输出
  格式，目前可选：**ack、egpu、etrace、prom、sgpu、walle**。
- 输出类型：values.yaml 中 extraEnv 的 `AMPERF_OUTPUT_TYPE` 环境变量，取值为 exporter 的输出
  类型，目前支持：**file、http pull、http push**。
- Pod 信息采集方式：values.yaml 中 extraEnv 的 `AMPERF_POD_COLLECTOR_TYPE` 环境变量，取值指定
  exporter 采集 Pod 信息的方式，目前可选：**none、kubelet uds、kubelet http**。当取值为
  kubelet uds 类型时，需要额外挂载 `/var/lib/kubelet/pods` 目录。
- 采集配置文件：values.yaml 中 extraEnv 的 `AMPERF_COLLECTORS_FILE` 环境变量，取值指定 exporter
  的采集配置文件路径，其中主要包含需要采集的信息条目。
- 运行模式：由部署侧环境变量控制，主要有 `all-components` 与 `exporter-only` 两种。前者会运行
  exporter、collector、detector 等全部组件；后者是默认环境，除 exporter 外的组件均已运行，只需
  运行 exporter。此处需用户自行判断，并配置如下挂载：

  ```yaml
  - name: amperf-grpc
    hostPath: "/tmp/amp/sockets/"
  ```

## 从源码构建

要从源码构建 LJ RuntimeKit Exporter，请克隆仓库并构建二进制：

```
git clone https://github.com/aliyun/LJ-RuntimeKit.git
cd LJ-RuntimeKit-Exporter
make build
```

### Profiling collector

独立的 profiling collector 位于 `cmd/collector`，实现位于 `pkg/collector`，安全文件操作位于
`pkg/safefs`，Runtime Kit RPC 定义位于 `api/rtkit/profiling`。它针对单个目标 Pod 的容器执行
profiling，并将原始文件写入 PVC。它与常驻运行的指标 exporter 相互独立，不依赖 JobController
或 Kubernetes 客户端。JobController 仍然负责 CRD、调度、collector Pod 模板与 collector ConfigMap。

需要 Go 1.21 或更高版本。collector 以 `CGO_ENABLED=0` 构建，构建机无需 GPU 库；exporter 仍需要
CGO/DCGM 支持。

```sh
make build-collector                       # cmd/collector/profiling-collector
./cmd/collector/profiling-collector --version
./cmd/collector/profiling-collector collect --help
make test-collector
make package-collector GOOS=linux GOARCH=amd64 VERSION=v1.0.0
make package-collector GOOS=linux GOARCH=arm64 VERSION=v1.0.0
make docker-collector DOCKERCMD='docker build' VERSION=v1.0.0
```

`dist/` 下的归档包含 `profiling-collector` 与 `LICENSE`，文件名中包含版本、操作系统与架构。
`make build-exporter` 仅构建现有 exporter，`make docker-exporter` 构建其镜像，`make docker`
（或其历史别名 `make docker-all`）构建两个镜像但不推送。collector 镜像默认使用
`lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-profiling-collector:$(VERSION)`；
可通过覆盖 `COLLECTOR_IMG` 指定其他仓库或 tag。该镜像保留
`/bin/profiling-collector collect`、访问 Runtime Kit socket 所需的 root 权限，以及带 shell 工具的
Debian 运行时。需把 JobController 的 collector ConfigMap `image`（或 `make deploy
COLLECTOR_IMG=...` 参数）设置为同一发布镜像。exporter 的 Helm chart 不会部署这个一次性
collector。

当两个仓库以同级目录检出时，可以在没有集群的情况下运行本地协议测试：

```sh
make build-collector
make -C ../LJ-RuntimeKit-JobController test-e2e \
  E2E_COLLECTOR_BIN="$(pwd)/cmd/collector/profiling-collector"
```

Runtime Kit RPC 线格式以及 collector 的终止 JSON、文件命名是与 JobController 之间的契约。修改
这些契约时需同步维护 fixture 协议与 `internal/collectorprotocol` 中的定义；两个仓库有意不互相
import 对方的 Go module。profiling 与 metrics 的 RPC 包描述的是同一个服务名，不能链接进同一
个二进制。

#### 安装

首先执行 `make docker` 完成镜像构建并生成 yaml 文件，镜像构建的相关参数可在 Makefile 中修改：

```
make docker
```

然后在 Kubernetes 集群中执行以下命令部署：

```
helm install lj-runtimekit-exporter -n <namespace> ./deployment -f ./deployment/values-exporter.yaml # namespace 自行定义
```

需要卸载时执行以下命令：

```
helm uninstall lj-runtimekit-exporter -n <namespace>
```

## 使用

LJ RuntimeKit Exporter 支持四种不同的指标采集方式以适配各类使用场景，分别是**文件接口**、
**HTTP PULL**、**HTTP PUSH** 与 **AMPerfd**。下面简要说明。

### HTTP PULL

在云原生环境中部署时，LJ RuntimeKit Exporter 默认通过 HTTP PULL 方式暴露指标，通常由 Prometheus
抓取。

## 贡献

欢迎贡献！请阅读 [CONTRIBUTING.md](CONTRIBUTING.md) 了解行为准则与提交 Pull Request 的流程。

## 许可证

本项目基于 Apache License 2.0 授权，详见 [LICENSE](LICENSE)。

## 支持

如有问题或疑问，请在 GitHub issue tracker 中提交 issue。

## 致谢

感谢 Kubernetes 社区的支持与协作，以及本项目所使用的所有外部库与工具的贡献者。
