# LJ-RuntimeKit-Controller

LJ-RuntimeKit-Controller 是一个基于 Kubernetes 的控制器，用于监控节点标签变化，并通过 Unix Socket 自动配置节点上的功能开关。

## 概述

LJ-RuntimeKit-Controller 监听集群中每个 Node 的 label 变化：

- 当 Node 拥有 `lj_runtimekit_enable: "true"` 标签时，自动在该节点上启用功能
- 当 Node 没有该标签或标签值不为 `"true"` 时，自动在该节点上禁用功能

## 架构

```
┌─────────────────────────────────────────────────────────────┐
│                      Kubernetes Cluster                       │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌─────────────────┐     ┌─────────────────────────────┐     │
│  │   Controller    │     │         Node A               │     │
│  │                 │     │  ┌─────────┐                 │     │
│  │  Watches Node   │────▶│  │ Job Pod │──▶ socket       │     │
│  │  Label Changes  │     │  └─────────┘  (enable/       │     │
│  │                 │     │               disable)       │     │
│  │  Creates Jobs   │     │                              │     │
│  └─────────────────┘     └─────────────────────────────┘     │
│                                                               │
│                         ┌─────────────────────────────┐       │
│                         │         Node B               │       │
│                         │  ┌─────────┐                 │       │
│                         │  │ Job Pod │──▶ socket       │       │
│                         │  └─────────┘                 │       │
│                         └─────────────────────────────┘       │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

## 工作原理

1. **Controller** 运行在集群中，持续监听所有 Node 的 label 变化
2. 当检测到 `lj_runtimekit_enable: "true"` 标签出现时，在对应节点上创建一个 **Job**（action=enable）
3. 当标签消失或变为非 `"true"` 时，在对应节点上创建一个 **Job**（action=disable）
4. **Job** 被调度到目标节点上运行，并挂载 `/run/amp/eKitC/xdm/sockets/`
5. Job 内的 `job-agent` 通过 `config-daemon.sock` 发送启用/禁用请求
6. Job 成功完成后，控制器会立即自动删除该 Job；失败的 Job 会由 `OnFailure` 重启策略自动重试

## 项目结构

```
LJ-RuntimeKit-Controller/
├── api/
│   └── proto/           # protobuf 定义
│       └── config_service.proto
├── cmd/
│   ├── controller/      # 控制器主程序（kubebuilder 框架）
│   │   └── main.go
│   └── job-agent/       # Job 中运行的代理程序
│       └── main.go
├── internal/
│   └── controller/      # 控制器逻辑
│       ├── node_controller.go   # Node label 监听与 Job 创建
│       └── job_controller.go    # 成功 Job 自动清理
├── pkg/
│   └── configpb/        # protobuf 生成的 Go 代码
│       ├── config_service.pb.go
│       └── config_service_grpc.pb.go
├── config/
│   ├── default/         # Kustomize 默认配置
│   ├── manager/         # Deployment 配置
│   └── rbac/            # RBAC 配置
├── deploy/              # 部署脚本
│   └── deploy.sh        # 部署/卸载/状态/日志
├── scripts/             # 编译脚本
│   └── build.sh         # 构建脚本
├── Dockerfile           # Controller 镜像
├── Dockerfile.job-agent # Job Agent 镜像
├── Makefile
├── PROJECT              # Kubebuilder 项目文件
└── go.mod
```

## 快速开始

### 前提条件

- Go 1.25+
- Docker
- kubectl（已配置集群访问）
- Kubernetes 集群 1.20+

### 构建

#### 仅构建二进制文件

```bash
./scripts/build.sh --binary-only
```

或

```bash
make build
```

#### 构建 Docker 镜像

```bash
./scripts/build.sh
```

或

```bash
make docker-build
```

#### 构建并推送镜像

```bash
./scripts/build.sh --push
```

或指定自定义镜像名称：

```bash
CONTROLLER_IMG=myregistry/lj-runtimekit-controller:v1.0.0 \
JOB_AGENT_IMG=myregistry/lj-runtimekit-controller-job-agent:v1.0.0 \
./scripts/build.sh --push
```

### 部署

```bash
./deploy/deploy.sh deploy
```

或指定自定义命名空间和镜像：

```bash
./deploy/deploy.sh --namespace lj-runtimekit-system \
    --controller-img myregistry/lj-runtimekit-controller:latest \
    --job-agent-img myregistry/lj-runtimekit-controller-job-agent:latest \
    deploy
```

### 卸载

```bash
./deploy/deploy.sh undeploy
```

### 查看状态

```bash
./deploy/deploy.sh status
```

### 查看日志

```bash
./deploy/deploy.sh logs
```

## 使用示例

### 1. 部署控制器

```bash
# 构建并推送镜像到镜像仓库
./scripts/build.sh --push

# 部署到集群
./deploy/deploy.sh deploy
```

### 2. 测试功能

为节点添加标签以触发功能启用：

```bash
kubectl label node <node-name> lj_runtimekit_enable=true
```

检查控制器日志：

```bash
kubectl logs -n lj-runtimekit-system -l control-plane=controller-manager
```

检查创建的 Job：

```bash
kubectl get jobs -n lj-runtimekit-system
```

移除标签以触发功能禁用：

```bash
kubectl label node <node-name> lj_runtimekit_enable-
```

## 配置选项

### Controller 参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--metrics-bind-address` | `:8080` | Metrics 服务监听地址 |
| `--health-probe-bind-address` | `:8081` | 健康检查监听地址 |
| `--leader-elect` | `false` | 启用 Leader 选举 |
| `--job-namespace` | `lj-runtimekit-system` | Job 创建的命名空间 |
| `--job-image` | `lj-runtimekit-controller-job-agent:latest` | Job Agent 镜像 |

### Job Agent 参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--action` | (必填) | 操作类型：`enable` 或 `disable` |
| `--socket` | `/run/amp/eKitC/xdm/sockets/config-daemon.sock` | Socket 文件路径 |
| `--timeout` | `30s` | 连接超时时间 |

## gRPC 通信协议

Job Agent 通过 Unix Socket 与节点上的 config-daemon 建立 gRPC 连接，proto 定义位于 [api/proto/config_service.proto]。

生成的 Go 代码位于 [pkg/configpb]。

### 服务定义

```protobuf
service AmpConfigService {
  rpc Enable(EnableRequest) returns (EnableResponse);
  rpc Disable(DisableRequest) returns (DisableResponse);
  rpc QueryStatus(QueryStatusRequest) returns (QueryStatusResponse);
}
```

### Enable/Disable 响应

| 字段 | 类型 | 说明 |
|------|------|------|
| `status` | int32 | `0` 表示成功，非 `0` 表示失败 |
| `message` | string | 结果描述 |
| `stdout` | string | 标准输出 |
| `stderr` | string | 标准错误 |
| `exit_code` | int32 | 进程退出码 |
| `duration_ms` | int64 | 执行耗时（毫秒） |

### 重新生成 protobuf 代码

```bash
make proto
```

或直接运行：

```bash
protoc --go_out=. --go_opt=module=github.com/lj-runtimekit/controller \
  --go-grpc_out=. --go-grpc_opt=module=github.com/lj-runtimekit/controller \
  api/proto/*.proto
```

## RBAC 权限

Controller 需要以下权限：

- **Nodes**: get, list, watch
- **Jobs**: get, list, watch, create, delete
- **Events**: create, patch
- **Leases**: get, list, watch, create, update, patch, delete（用于 Leader 选举）

## 故障排查

### Job 创建失败

检查 Controller 日志：
```bash
kubectl logs -n lj-runtimekit-system -l control-plane=controller-manager
```

检查是否有足够的权限：
```bash
kubectl auth can-i create jobs.batch -n lj-runtimekit-system --as=system:serviceaccount:lj-runtimekit-system:lj-runtimekit-controller
```

### Socket 连接失败

检查 Socket 文件是否存在：
```bash
# 在目标节点上执行
ls -la /run/amp/eKitC/xdm/sockets/config-daemon.sock
```

检查 Job Pod 状态：
```bash
kubectl get pods -n lj-runtimekit-system -l app.kubernetes.io/managed-by=lj-runtimekit-controller
kubectl describe pod -n lj-runtimekit-system <pod-name>
kubectl logs -n lj-runtimekit-system <pod-name>
```

### Job 卡在 Pending 状态

检查目标节点是否可用：
```bash
kubectl get nodes -l lj_runtimekit_enable=true
kubectl describe job -n lj-runtimekit-system <job-name>
```

## 开发

### 本地运行 Controller

```bash
make run
```

### 运行测试

```bash
make test
```

### 代码格式化

```bash
make fmt
```

### 代码检查

```bash
make vet
```

## License

Apache License 2.0
