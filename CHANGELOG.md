# 版本变更记录

## v1.14.0

本版本在 v1.13.0 的基础上提供以下五项主要能力，并更新 Kubernetes 开源组件与部署资产。资源层 Runtime 服务及预编译容器镜像不包含在源码发布包中。

### 主要能力

- **DRA + IMEX：多节点显存共享（IMEX）**。通过 Kubernetes Dynamic Resource Allocation（DRA）接入 IMEX 资源编排与分配。
- **逻辑空分复用（LDM）**。
- **性能剖析与追踪（Profiling & Tracing）**。
- **主动监测（Active Monitoring）**。
- **模型快速拉起（Checkpoint/Restore）**。

这些能力需要相应版本的节点 Runtime 服务、驱动与 Kubernetes 组件协同支持；部署开源源码本身不代表节点侧能力已启用。

### 组件新增与更新

- 新增 JobController 源码：通过同一服务管理 ComputeDomain（IMEX）、ProfilingJob（性能剖析与追踪）与 ActiveMonitoring（主动监测）功能。
- 更新 Device Plugin 与 Scheduler Framework，包含 ComputeDomain / DRA 集成、GPU 拓扑处理、主动监测及 RDMA 拓扑感知调度相关实现。
- 更新 Exporter，包含性能剖析 Collector 与 Node Problem Detector（NPD）健康检测适配组件。
- 新增 Migration Controller 与 Migration Worker 源码，支撑模型快速拉起（Checkpoint/Restore），包含快照、恢复与预热任务 CRD、Worker 生命周期管理及存储后端集成。
- 更新节点开关 Controller 与 Job Agent，以及各组件构建入口和中英文 README。

### 部署变化

- 统一部署入口为仓库根目录的 `deploy/deploy-all.sh`，配套清单按文件名前缀编号顺序应用。
- 默认部署七个组件：`job-controller`、`device-plugin`、`scheduler`、`exporter`、`controller`、`migration-worker`、`migration-controller`。
- 仅选择 `migration-controller` 部署时，会自动先安装 Worker 模板与 RBAC；Worker Pod 按任务创建，非常驻 DaemonSet。
- Migration Controller 使用 Deployment；迁移与预热 CRD 安装后等待 `Established`。
- Migration 卸载前检查业务 CR 与 Worker Pod，保留迁移 CRD，不强制清理 finalizer。统一部署脚本不安装 Dragonfly 或认证 Secret。
- JobController 包含 `resource.aliyun.com` 与 `resource.nvidia.com` 两组 ComputeDomain / ComputeDomainClique CRD，以及 ProfilingJob、ActiveMonitoring CRD 和相关部署资产。

### 兼容性与升级注意事项

- 本版本 README 列出的预编译镜像适配版本为 Kubernetes v1.34、v1.35、v1.37；源码发布不等于这些镜像已在目标环境完成拉取或验证。
- 配套部署清单默认镜像 Tag 为 `v1.14.0-k8s1.34`。其他 Kubernetes 版本须按镜像发布约定修改清单；预拉取镜像或设置 shell 变量不会自动改写清单。
- Device Plugin 升级须同步更新 init 容器与主容器镜像；Controller 须同步更新 `--job-image`；JobController 的 Collector / 监测镜像与 Migration Worker 模板镜像也须逐项核对。
- 源码功能开关默认关闭；根目录的统一部署清单显式启用 Device Plugin 的 `ComputeDomain`，以及 JobController 的 `ComputeDomain`、`Profiling`、`ActiveMonitoring`。组件自身 `make deploy` 的开关配置与统一清单不同，部署前应确认实际使用的入口。
- 升级需安装所选功能对应的 CRD、RBAC 与 ConfigMap，不能只替换工作负载的镜像。`deploy/job-controller/examples/` 不会被一键脚本自动应用。
- Device Plugin、Scheduler、Exporter 的协议配置应保持一致；更新 ConfigMap 后须按组件要求重启工作负载，单独执行 `kubectl apply` 不保证运行中的进程已加载新配置。
- 统一部署脚本需要 Bash 4 或更新版本；节点标签、RuntimeClass、节点 Runtime 服务及可选功能依赖须按组件文档准备。
- 组件就绪检查不能代替 DRA + IMEX、LDM、Profiling & Tracing、Active Monitoring 或 Checkpoint/Restore 的端到端验证。Dragonfly 参考清单中的默认账号口令仅用于示例，生产环境须更换并限制访问。

完整源码差异：[v1.13.0...v1.14.0](https://github.com/aliyun/LJ-RuntimeKit/compare/v1.13.0...v1.14.0)。
