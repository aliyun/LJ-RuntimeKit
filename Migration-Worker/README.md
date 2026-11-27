**简体中文** | [English](README_EN.md)

# Migration Worker

Migration Worker 是容器在线迁移项目（container-migration）的节点侧守护进程，
由 [container-migration-controller](https://github.com/container-migration/controller)
通过 gRPC 驱动。它只负责一件事：**搬运数据**。

- **Checkpoint 侧**：向本节点的 X-Device-Manager（XDM）请求容器的 checkpoint
  数据流，直接写进存储后端。
- **Restore 侧**：在目标节点把数据拉回，解压到本地目录，供容器运行时以
  runc restore 方式恢复。

Worker 从不创建、启动或恢复容器——把进程拉起来是 `xpu-container-runtime`
的职责，它在目标节点拦截 `runc create` 并转为 restore。这一分工让 Worker
保持小而无状态。

## 目录

- [概述](#概述)
- [架构](#架构)
- [工作原理（PVC 链路）](#工作原理pvc-链路)
- [gRPC 接口](#grpc-接口)
- [项目结构](#项目结构)
- [快速开始](#快速开始)
- [配置参数](#配置参数)
- [指标](#指标)
- [故障排查](#故障排查)
- [Contributing](#contributing)
- [License](#license)

## 概述

现阶段 Worker 支持一条完整的迁移链路——**PVC 链路**：

1. **checkpoint 到一个 PVC**：控制器在源节点创建任务级 Worker，把源 PVC 挂载
   到固定路径 `/mnt/pv`，Worker 把源容器的 rootfs / memory / xpu 三份数据以
   分片 tar 的形式直接写入该 PVC。
2. **预热（可选，纯加速）**：checkpoint 完成后，可选择把数据预热到另一个 PVC
   或集群 P2P 缓存（Dragonfly / EFC），也可以完全跳过预热，restore 直读 PVC。
   预热不参与数据正确性，只影响速度。
3. **restore 从一个 PVC**：目标节点上的任务级 Worker 挂载 PVC，读取 checkpoint
   数据、并发解压到节点本地目录。

其他存储链路（OSS / EFC 直连等）未来可能加入，本文不展开。

## 架构

```
┌───────────────────────────────────────────────────────────────────┐
│                        Kubernetes Cluster                         │
├───────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────────────┐     gRPC     ┌────────────────────────────┐   │
│  │ Migration        │─────────────▶│ 任务级 Worker（每任务一个）    │   │
│  │ Controller       │              │  ├─ /mnt/pv  源/目标 PVC 挂载│   │
│  │                  │  创建/删除    │  ├─ XDM unix socket（checkpoint）│
│  │  - 任务编排        │─────────────▶│  ├─ dfdaemon（P2P 拉取）      │   │
│  │  - 预热编排        │              │  └─ gRPC :50051（对 controller）│   │
│  └──────────────────┘              └────────────┬───────────────┘   │
│                                                  │                   │
│                          checkpoint 写 / 读        │                   │
│                          ┌───────────────────────▼──────────────┐    │
│                          │           PVC（RWX）                 │    │
│                          │  {podUID}/{version}/{container}/     │    │
│                          │  checkpoint/{part-00.tar, ...}       │    │
│                          └──────────────────────────────────────┘    │
│                                                                     │
│  预热源 Pod（可选，一次性）：serve /mnt/pv（http :50090）               │
│     → Dragonfly seed peer 回源 → restore Worker 经 dfdaemon P2P 拉取   │
└───────────────────────────────────────────────────────────────────┘
```

Worker 的两种运行形态：

- **任务级 Worker**：控制器为每个迁移任务创建一个 Pod（挂载对应 PVC），
  任务结束后随阶段推进或 CR 删除被回收。checkpoint / restore 均以此形态运行。
- **预热源 Pod**（可选）：`--serve-http` 模式下只提供 `/mnt/pv` 的 http 文件
  服务，作为 Dragonfly seed peer 的回源源站；不起 gRPC / prometheus。
  预热 Ready 后即被删除，此后由 restore Worker **兼任源**继续提供回源
  （详见下文）。

## 工作原理（PVC 链路）

### 1. Checkpoint：写入 PVC

控制器在源节点创建任务级 Worker，把源 PVC 挂到 `/mnt/pv`。Worker 通过
`pkg/runtime` 的 ContainerRuntime 插件（唯一实现是 XDM）取得容器的
checkpoint tar 流，rootfs / memory / xpu 三份数据分别打成**分片 tar**
（`part-00.tar`、`part-01.tar` …，每个分片都是携带完整目录骨架、可独立
解压的 tar），写入 PVC 的 `{podUID}/{version}/{container}/checkpoint/` 目录。

分片布局是 restore 侧 N 路并发解压的基础：每一路独立下载并解压自己的分片，
总耗时约等于 `max(各路耗时)` 而不是 `sum`。被拆散到多个分片的大文件（XDM
的 `XDM.split.*` PAX 记录标记切片区间）在各分片解完后统一修正元数据
（setuid / xattr / mtime）。

### 2. 预热（可选，纯加速）

checkpoint 完成后，restore 读取可以走两条预热链路，由 Worker 命令行参数
`--pvc-preheat-backend` 决定（全局生效，非法值启动即拒绝）：

- **dragonfly（默认）**：restore Worker 按 `(pvcNamespace, pvcName, pvcPath)`
  推导常驻预热 Service 名，取其 ClusterIP 重建与预热时**逐字节一致**的 URL
  （Dragonfly 的 task_id 由 URL 算出，任何一字节不同都会静默 miss 缓存），
  经本节点 dfdaemon P2P 拉取。预热源 Pod 删除后，restore Worker **兼任回源
  源站**：挂同一 PVC、自写 EndpointSlice 注册到常驻预热 Service。某个分片
  P2P 下载重试穷尽后，按 part 粒度直读 `/mnt/pv` 兜底。
- **efc**：Worker 从 PVC/PV 推导 EFC 挂载下的相对路径前缀，restore 优先读
  `/mnt/efc/<prefix>/<checkpointDir>`，失败/挂死一律兜底直读挂载的 PVC。
  EFC 读带进度感知超时：连续零增长才判定挂死，慢读永不误杀。

跳过预热时，restore 直接从挂载的 PVC 直读。预热是加速手段，任何预热异常
（PreheatJob 未就绪、Service 不存在、P2P 失败）都降级为直读，不影响正确性。

### 3. Restore：从 PVC 读取

目标节点上的任务级 Worker 挂载 PVC 到 `/mnt/pv`，把 checkpoint 目录下的
N 个分片并发解压到目标目录：

- 目标目录先解压到 `<dstDir>-tmp`，全部成功后原子 rename；失败只清理 tmp，
  不污染目标目录。
- 解压前预建 `memory/` 哨兵目录：XDM 的 rootfs reaper 以"rootfs/ 同级有没有
  memory/"判断 checkpoint 是否可能仍被使用，N 路乱序解压后必须先有哨兵，
  否则 reaper 可能在下载进行中删掉已落盘的 rootfs/。
- 分片解完后统一校验切片区间并集覆盖原文件长度（抓分片缺失/截断），再修正
  被切片文件的元数据。
- 挂死防护：纯 OSS 兜底直读用进度感知超时（监视计数器连续零增长才取消），
  慢读不被硬超时误杀。

### 4. RestoreXPU

主容器 Running 后，控制器下发 RestoreXPU 请求；Worker 把每个容器转发给本
节点 XDM 的 cr-tool 恢复 XPU 设备状态（CPU-only 容器由 XDM 判定后直接返回
成功）。

## gRPC 接口

协议定义见 `pkg/apis/container_state_migration.proto`（生成代码用
`make grpc-proto` 重新生成）。`ContainerStateMigrationService` 共五个 RPC：

| RPC | 作用 |
|-----|------|
| `CheckpointToStorage` | 启动 dump + 上传流水线，立即返回 ack |
| `QueryCheckpointToStorageStatus` | 查询单容器 checkpoint 属性（path/size/duration/extras） |
| `RestoreFromStorage` | 启动下载 + 解压流水线，立即返回 ack |
| `QueryRestoreFromStorageStatus` | 查询单容器 restore 属性（含 restoreAttempted 凭证） |
| `CleanTask` | 清理内存任务记录，可选删除已落盘数据 |

`XpuDeviceManagerService` 只有 `Checkpoint` 一个**服务端流式** RPC：Worker
作为客户端向 XDM 逐块取回 checkpoint tar 数据。

## 项目结构

```
Migration-Worker/
├── cmd/migration-worker/               # 主程序（cobra + klog + prometheus）
│   └── options/                        # 全部运行时参数与默认值
├── pkg/
│   ├── apis/                           # controller 协议与 XDM 协议的生成代码
│   ├── controller/                     # gRPC handler、任务调度、checkpoint/restore item
│   ├── runtime/                        # ContainerRuntime 插件层（唯一实现：XDM）
│   ├── storage/                        # 存储后端插件 + PreheatJob 解析
│   │   ├── localfs.go                  # local 插件（PVC 直挂读写走这里）
│   │   ├── pvc_preheat.go              # Dragonfly 预热的 PVC 组合存储
│   │   ├── pvc_preheat_source.go       # 兼任回源源站（EndpointSlice 注册）
│   │   ├── dragonfly.go / dragonfly_client.go  # Dragonfly P2P 后端
│   │   └── oss.go / nfs.go             # OSS / NFS 后端
│   └── util/
│       ├── archive/                    # 分片 tar 解压（UnpackPart 并发解包）
│       └── types.go                    # CountingReader 等通用类型
├── Dockerfile                          # 多阶段构建（无 vendor，镜像以 root 运行）
├── Makefile
└── LICENSE / NOTICE
```

存储后端以插件形式注册进工厂（`RegisterStoragePlugin`），控制器经
`storageType` 选择。PVC 链路由控制器把 `storageType=pvc` 转成 local 插件
（固定挂载点 `/mnt/pv`）实现；`dragonfly` / `efc` 只是 restore 读取的
加速链路。其余后端（`oss`、`nfs`）保留注册，链路文档未来补充。

## 快速开始

### 前提条件

- Go 1.23+（构建镜像为 golang:1.24）
- Docker（构建镜像）
- 一个 Kubernetes 集群，节点上已部署 XDM 与（可选的）Dragonfly dfdaemon
- 控制器已按 PVC 链路部署（负责创建/回收任务级 Worker）

### 构建

```bash
# 宿主机二进制（Linux，静态链接）
make build

# 容器镜像（多阶段构建，镜像名默认指向 lj-runtimekit 私有仓库，可覆盖）
make docker IMG=<registry>/<repo>:<tag>
```

| Target | 用途 |
|--------|------|
| `make build` | 构建 Linux 二进制到 `bin/` |
| `make docker` | 构建容器镜像（`IMG=...` 覆盖镜像名） |
| `make test` | `go test ./...`（含覆盖率） |
| `make lint` | golangci-lint（`lint-fix` 自动修复） |
| `make grpc-proto` | 重新生成 gRPC 桩代码 |
| `make clean` | 清理 `bin/`、`output/`、`cover.out` |

> 仓库不提交 `vendor/` 目录，依赖直接经模块代理解析。`pkg/util` 下部分测试
> 依赖 `lchown`，只在 Linux 上以 root 运行才能通过，macOS 上失败属预期。

### 运行（本地调试）

Worker 通常由控制器的部署清单以任务级 Pod 形式创建，本地调试可手工启动：

```bash
./bin/migration-worker \
  --grpc-port=50051 \
  --save-dir=/data/migration \
  --xdm-socket=/run/amp/eKitC/xdm/sockets/dp2mgr.sock \
  --pvc-preheat-backend=dragonfly
```

运行环境要求：

- `KUBE_NODE_NAME`（downward API 注入），为空则启动失败；
- 同节点 XDM unix socket 可达（`--xdm-socket`）；
- `--save-dir` 指向目标容器运行时可见的宿主路径；
- PVC 模式需要 in-cluster 配置读取常驻预热 Service（拿不到只告警，restore
  退化为直读 `/mnt/pv`）；
- Dragonfly 预热需要 ServiceAccount 具备 `preheatjobs` 的 get 权限。

## 配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--grpc-port` | `50051` | gRPC 服务监听端口 |
| `--save-dir` | `/home/admin/t4/migration` | 迁移数据落盘根目录 |
| `--xdm-socket` | `/run/amp/eKitC/xdm/sockets/dp2mgr.sock` | XDM gRPC unix socket |
| `--checkpoint-parallelism` | `2` | 并发 checkpoint 任务数 |
| `--restore-parallelism` | `6` | 并发 restore 任务数 |
| `--http-client-timeout` | `2m` | HTTP 类传输超时 |
| `--prometheus-port` | `9416` | `/metrics` 端口 |
| `--serve-http` | `false` | 预热源模式：只提供 `/mnt/pv` 的 http 文件服务 |
| `--http-port` | `50090` | serve-http 模式的文件服务端口（与控制器侧固定值一致） |
| `--pvc-preheat-backend` | `dragonfly` | PVC restore 的预热链路：`dragonfly` 或 `efc` |
| `--oss-part-size` | `30` | OSS 分片上传单片大小（MB） |
| `--oss-upload-threads` | `10` | OSS 分片上传线程数 |
| `--dfdaemon-endpoint` | `/var/run/dragonfly/dfdaemon.sock` | dfdaemon 地址（unix 路径或 host:port） |
| `--dragonfly-piece-size` | `0` | Dragonfly 上传/下载分片大小（字节），`0` 交给 dfdaemon 决定（预热缓存命中必需） |
| `--dragonfly-reorder-buffer` | `512` | 内存重排缓冲上限（MB），`0` 不限 |
| `--dragonfly-piece-stall-timeout` | `90s` | 缺失分片 hedge 重取等待，`0` 关闭 |
| `--dragonfly-spill-limit` | `2048` | 未重排数据落盘硬上限（MB），`0` 不限 |

`migration-worker --help` 输出权威列表。

## 指标

Prometheus 指标挂在 `/metrics`，全部带节点名标签：gRPC 拦截器记录的
per-RPC 延迟与状态、按容器与载荷大小分类的 checkpoint 延迟、按后端/文件数/
大小分类的 restore 延迟、checkpoint / restore 队列负载。

## 故障排查

- **restore 报"目录里没有分片"**：checkpoint 目录未就绪或分片探测不到；
  确认控制器侧 `CheckpointAttributes.FileCount` 校验已通过、PVC 挂载正常。
- **P2P 全部 miss、退化为回源**：URL 与预热时不一致导致 task_id 对不上；
  检查常驻预热 Service 名推导（`pvcNamespace/pvcName/pvcPath` 两侧是否一致）。
- **回源 403**：部分 dfdaemon 版本把过滤签名参数后的 URL 直接拿去回源；
  参见 `pkg/storage/dragonfly_client.go` 中 `defaultFilteredQueryParams` 的说明。
- **EFC 读无限挂死**：进度感知超时只判"零增长"；FUSE 读永久阻塞会在
  `efcNoProgressTimeout` 后兜底 PVC，看 Worker 日志确认是否触发。

## Contributing

请阅读 [CONTRIBUTING.md](CONTRIBUTING.md)。

## License

Apache 2.0，详见 [LICENSE](LICENSE)。第三方代码归属见 [NOTICE](NOTICE)。
