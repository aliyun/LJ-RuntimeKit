# Kubernetes 集群 JobSet 支持配置操作手册

> 适用集群：Kubernetes v1.30.x（containerd 运行时）
> JobSet 版本：v0.7.2
> 已验证环境：192.168.0.8 集群（3 节点，经跳板机 8.160.167.12 访问）

---

## 一、架构概览

JobSet 是 Kubernetes 上的作业编排扩展（SIG-Scheduling 子项目），用于批量创建多组 Job（典型场景：分布式 AI 训练）。安装包含以下组件：

| 组件 | 说明 |
|------|------|
| CRD `jobsets.jobset.x-k8s.io` | JobSet 自定义资源定义（apiVersion: `jobset.x-k8s.io/v1alpha2`） |
| Deployment `jobset-controller-manager` | 控制器（manager 容器）+ kube-rbac-proxy（metrics 代理）双容器 |
| RBAC | ServiceAccount + ClusterRole/Binding（manager、proxy、metrics）+ namespace 内 leader-election Role |
| Webhook | Mutating + Validating WebhookConfiguration（校验/默认值注入 JobSet spec） |
| Secret `jobset-webhook-server-cert` | Webhook 服务端 TLS 证书（控制器以 volume 挂载） |

依赖镜像：

| 镜像 | 用途 |
|------|------|
| `k8s.m.daocloud.io/jobset/jobset:v0.7.2` | 控制器（daocloud 镜像源，对应 `registry.k8s.io/jobset/jobset:v0.7.2`） |
| `quay.m.daocloud.io/brancz/kube-rbac-proxy:v0.13.1` | metrics 代理 sidecar |

---

## 二、前置条件检查

```bash
# 1. K8s 版本 >= 1.27
kubectl version

# 2. 节点容器运行时（本手册按 containerd 编写）
kubectl get nodes -o wide   # CONTAINER-RUNTIME 列为 containerd://x.x.x

# 3. 网络出口情况（决定走在线拉取还是离线导入）
curl -sI --connect-timeout 5 https://k8s.m.daocloud.io/v2/ | head -1   # 可达则返回 HTTP/2 401（正常）
curl -sI --connect-timeout 5 https://registry.k8s.io/v2/ | head -1     # 通常内网不可达
```

**本集群结论**：节点可访问 daocloud 镜像源与杭州 ACR，不可访问 Docker Hub / registry.k8s.io / quay.io 原始地址。因此镜像走 **daocloud 源 docker pull → ctr 导入 containerd** 路线。

---

## 三、步骤 1：镜像准备

### 3.1 拉取镜像（任一可执行 docker 且能访问 daocloud 的节点）

```bash
docker pull k8s.m.daocloud.io/jobset/jobset:v0.7.2
docker pull quay.m.daocloud.io/brancz/kube-rbac-proxy:v0.13.1
```

### 3.2 分发到全部节点的 containerd

kubelet（containerd）与 docker 使用**独立的镜像存储**，docker 拉到的镜像 kubelet 看不见，必须导出后导入 containerd 的 `k8s.io` 命名空间。控制器 Pod 可能调度到任意节点，**所有节点都要导入**：

```bash
# 打包
docker save k8s.m.daocloud.io/jobset/jobset:v0.7.2 \
            quay.m.daocloud.io/brancz/kube-rbac-proxy:v0.13.1 \
            -o /tmp/jobset-images.tar

# 本机导入
ctr -n k8s.io images import /tmp/jobset-images.tar

# 其他节点（在源节点上执行，逐台 ssh 导入）
for ip in <node2-ip> <node3-ip>; do
    ssh root@$ip "ctr -n k8s.io images import - < /dev/stdin" < /tmp/jobset-images.tar
done
```

### 3.3 验证

```bash
# 每个节点执行，两个镜像均应出现
crictl images | grep -E "jobset|rbac-proxy"
```

> **备选（完全离线）**：在可联网机器 `docker save` 后把 tar 文件 scp 进集群，再逐节点 `ctr -n k8s.io images import`，流程相同。

---

## 四、步骤 2：安装 CRD

```bash
kubectl apply -f jobset-crd.yaml
```

**已知问题**：JobSet CRD 约 9000 行，客户端 `kubectl apply` 会把全量对象写入 `last-applied-configuration` 注解，超过 256KB 限制报错：

```
The CustomResourceDefinition "jobsets.jobset.x-k8s.io" is invalid:
metadata.annotations: Too long: must have at most 262144 bytes
```

**解决**：改用 server-side apply（不落该注解）：

```bash
kubectl apply --server-side -f jobset-crd.yaml
```

验证：

```bash
kubectl get crd jobsets.jobset.x-k8s.io
```

---

## 五、步骤 3：安装控制器与 RBAC

应用清单（单一文件，按 `---` 分隔，共 12 个对象）：

```bash
kubectl create ns jobset-system
kubectl apply -f jobset-all.yaml
```

清单包含：

| 类型 | 名称 |
|------|------|
| ServiceAccount | jobset-controller-manager |
| ClusterRole | jobset-manager-role / jobset-metrics-reader / jobset-proxy-role |
| ClusterRoleBinding | jobset-manager-rolebinding / jobset-proxy-rolebinding |
| Role + RoleBinding | jobset-leader-election-role(+binding)，namespace=jobset-system |
| Deployment | jobset-controller-manager（2 容器：manager + kube-rbac-proxy） |
| Service | jobset-controller-manager-metrics-service / jobset-webhook-service |

控制器关键启动参数（manager 容器）：

```yaml
args:
- --zap-log-level=2
- --health-probe-bind-address=:8081
- --metrics-bind-address=127.0.0.1:8080
- --leader-elect
```

---

## 六、步骤 4：安装 Webhook 证书与配置

控制器 Deployment 挂载了 secret `jobset-webhook-server-cert`，**缺失时 Pod 卡 ContainerCreating**，事件为：

```
FailedMount: MountVolume.SetUp failed for volume "cert":
secret "jobset-webhook-server-cert" not found
```

三种获取方式，任选其一：

1. **复用已有环境的证书**（本次采用，最简单）：从已装 JobSet 的集群导出后应用
   ```bash
   # 在源集群导出
   kubectl get secret jobset-webhook-server-cert -n jobset-system -o yaml > cert.yaml
   kubectl get mutatingwebhookconfiguration jobset-mutating-webhook-configuration -o yaml >> cert.yaml
   kubectl get validatingwebhookconfiguration jobset-validating-webhook-configuration -o yaml >> cert.yaml
   # 清理 metadata 运行时字段（resourceVersion/uid/creationTimestamp/managedFields）后在目标集群应用
   kubectl apply -f cert.yaml
   ```
2. 使用官方 manifest 自带的自签证书生成流程（官方 manifests.yaml 中含 cert 生成机制，要求能访问官方镜像）。
3. 用 openssl 手工生成 CA + 服务端证书，并把 CA 写入两个 WebhookConfiguration 的 `clientConfig.caBundle`。

> 注意：从其他集群复用的 secret/webhook 需确认 `clientConfig.service` 指向 `jobset-system/jobset-webhook-service`（两集群同名同 namespace 即可直接复用）。

**重要顺序**：secret 必须先于（或随）Deployment 存在，否则 Pod 卡在 ContainerCreating；secret 后补时，kubelet 挂载重试有退避周期，可删除 Pod 立即重建。

---

## 七、步骤 5：验证

```bash
# 1. 控制器 2/2 Running
kubectl get pod -n jobset-system

# 2. CRD 与 webhook 就绪
kubectl get crd jobsets.jobset.x-k8s.io
kubectl get mutatingwebhookconfigurations,validatingwebhookconfigurations | grep jobset

# 3. 冒烟测试：创建 JobSet 并观察 Pod 派生与完成
cat > /tmp/jobset-smoke.yaml <<'EOF'
apiVersion: jobset.x-k8s.io/v1alpha2
kind: JobSet
metadata:
  name: smoke-test
spec:
  failurePolicy:
    maxRestarts: 1
  replicatedJobs:
    - name: worker
      template:
        spec:
          parallelism: 2
          completions: 2
          template:
            spec:
              restartPolicy: Never
              containers:
              - name: busy
                image: registry.cn-hangzhou.aliyuncs.com/eflops-amp/ubuntu:22.04
                command: ["sleep", "30"]
EOF
kubectl apply -f /tmp/jobset-smoke.yaml

# 预期：约 30s 后
kubectl get jobset        # TERMINALSTATE=Completed, COMPLETED=True
kubectl get pod | grep smoke   # worker Pod 全部 Completed

# 4. 清理
kubectl delete jobset smoke-test
```

**192.168.0.8 集群实测结果**：JobSet 创建 → 派生 2 Pod → 全部完成 → `TERMINALSTATE=Completed`，端到端通过。

---

## 八、常见问题排查

| 现象 | 原因 | 解决 |
|------|------|------|
| `ErrImagePull` / `ImagePullBackOff` | 节点 containerd 无镜像且外网不可达 | 按 3.2 向该节点 `ctr -n k8s.io images import`；并确认 deployment `imagePullPolicy: IfNotPresent` |
| `docker images` 有镜像但 kubelet 拉不到 | docker 与 containerd 镜像存储独立 | `docker save` → `ctr -n k8s.io images import` |
| CRD apply 报 `annotations: Too long` | 客户端 apply 注解超 256KB | `kubectl apply --server-side -f` |
| Pod 卡 `ContainerCreating`，FailedMount secret not found | webhook 证书 secret 缺失 | 按第六节补 secret 后删除 Pod 重建 |
| JobSet 创建报 webhook 拒绝/超时 | 控制器未就绪或 cert 与 caBundle 不匹配 | 确认 controller 2/2 Running；webhook `clientConfig.caBundle` 与 secret 中 CA 一致 |
| 从其他集群导出的清单 apply 异常 | 残留运行时字段（uid/resourceVersion/status/clusterIP 等）或多次 `kubectl get >> file` 拼接缺 `---` 分隔符 | 清理运行时字段；拼接文件按 `apiVersion:` 边界补 `---` |

---

## 九、本次部署清单（192.168.0.8 集群实际落盘文件）

位于 control-plane 节点 `/tmp/`：

| 文件 | 内容 |
|------|------|
| `/tmp/jobset-crd.yaml` | JobSet CRD（清理后，14268 行） |
| `/tmp/jobset-all.yaml` | SA/RBAC/Deployment/Service 共 12 对象（清理后） |
| `/tmp/jobset-webhook.yaml` | cert secret + Mutating/Validating WebhookConfiguration |
| `/tmp/jobset-images.tar` | 两个控制器镜像打包（114MB） |

镜像分发覆盖节点：192.168.0.8（control-plane）、192.168.0.15、192.168.0.9。
