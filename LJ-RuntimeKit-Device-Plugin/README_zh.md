[English](README.md) | **简体中文**

# LJ RuntimeKit Device Plugin

欢迎使用 LJ RuntimeKit Device Plugin！这是一个开源设备插件，旨在为 Kubernetes 集群中管理和集成
多种硬件资源提供统一接口。如果你希望在 Kubernetes Pod 中使用 GPU、DCU、PPU、AMD 等特殊硬件或
其他加速器，本设备插件可以帮助你轻松实现。

---

## 功能特性

#### 无缝硬件集成

自动识别并管理集群中的硬件资源。

#### 广泛的硬件支持

兼容多种硬件设备。

#### 资源分配

与 Kubernetes 调度器集成，确保资源被正确分配给 Pod。当前支持的资源类型如下：

- `aliyun.com/gpu-memory`
- `aliyun.com/gpu-core`
- `aliyun.com/gpu-slice`

#### 自定义资源名称

支持定义自定义资源名称，便于更好地管理资源。当前支持的资源类型如下：

- `nvidia.com/gpu`
- `aliyun.com/ppu`
- `aliyun.com/amd`

#### 健康监控

实现健康检查，用于监控并上报硬件设备状态。

---

## 前置条件

开始使用 LJ RuntimeKit Device Plugin 之前，请确保具备以下条件：

- 运行 1.37 及以上版本的 Kubernetes 集群。
- 如需从源码构建，系统中需安装 Go 1.26 或更高版本。

## 快速开始

按以下步骤将 LJ RuntimeKit Device Plugin 快速部署到集群：

## 从源码构建

要从源码构建 LJ RuntimeKit Device Plugin，请先克隆代码并构建镜像：

```
git clone https://github.com/aliyun/LJ-RuntimeKit.git
cd LJ-RuntimeKit-Device-Plugin
docker build -t <image_name> -f Dockerfile .
```

也可以使用 Makefile 构建镜像：

```
make docker   # 构建新镜像
```

### 安装

镜像构建完成后，使用以下命令部署设备插件：

```
kubectl apply -f yaml/lj-runtimekit-device-plugin-rbac.yaml
kubectl apply -f yaml/lj-runtimekit-device-plugin-ds.yaml
```

需要卸载时执行以下命令：

```
kubectl delete -f yaml/lj-runtimekit-device-plugin-rbac.yaml
kubectl delete -f yaml/lj-runtimekit-device-plugin-ds.yaml
```

也可以使用 Makefile 完成 LJ RuntimeKit Device Plugin 的构建与部署：

```
make deploy # 应用 yaml 配置
```

### 验证

使用以下命令验证设备插件是否正常运行：

```
kubectl get pods -n <namespace> | grep lj-runtimekit-device-plugin
```

## 配置

部署前可以修改 `lj-runtimekit-device-plugin-ds.yaml` 中的镜像名称及其他配置。

## 故障排查

如遇问题，可以查看设备插件 Pod 的日志进行诊断：

```
kubectl logs <pod-name> -n <namespace>
kubectl describe pods <pod-name> -n <namespace>
```

## 贡献

开源社区之所以如此精彩，正是因为有了大家的贡献。我们非常感谢你做出的任何贡献。

贡献指南请参阅 [CONTRIBUTING.md](CONTRIBUTING.md)。

## 支持

如有任何问题或遇到任何故障，欢迎在 issue tracker 中提交 issue。

## 许可证

LJ RuntimeKit Device Plugin 基于 [Apache License 2.0](LICENSE) 开源。

## 致谢

感谢 Kubernetes 社区的指导与支持，以及所有帮助本设备插件变得更好的贡献者。

欢迎反馈与贡献！感谢你支持并使用 LJ RuntimeKit Device Plugin。
