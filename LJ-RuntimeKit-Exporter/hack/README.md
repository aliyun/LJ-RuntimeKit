# hack/ 工具链

## download-deps.sh — K8s 依赖版本切换

### 用途
将 Exporter 项目的 `k8s.io/*` staging 依赖一键切换到指定 Kubernetes 版本。

当前 Exporter 依赖的 staging 模块：
- `k8s.io/api`
- `k8s.io/apimachinery`
- `k8s.io/client-go`
- `k8s.io/kubelet`

### 使用方式
```bash
# 切换到 K8s 1.27.3
./hack/download-deps.sh v1.27.3

# 切换到 K8s 1.21.2
./hack/download-deps.sh v1.21.2

# 切换完成后更新 vendor
./hack/update-vendor.sh
```

### 工作原理
1. 从 GitHub 拉取目标版本 `kubernetes/kubernetes` 的 go.mod
2. 解析出所有 `k8s.io/* => ./staging/src/k8s.io/*` 的 replace 行
3. 仅对本地 go.mod 中已存在的 `k8s.io/*` 模块进行替换（不会引入多余模块）
4. 对每个匹配模块调用 `go mod download -json` 获取确切版本
5. 在本地 go.mod 中写入 `replace k8s.io/xxx => k8s.io/xxx@版本`
6. 执行 `go mod download` 拉取依赖

### 与 SF 仓库脚本的差异
- **不依赖 `k8s.io/kubernetes` 主模块**：Exporter 只使用轻量的 staging 模块，脚本中不含 `go get k8s.io/kubernetes` 步骤
- **按需替换**：只替换本地 go.mod 中已有的模块，不会添加未使用的 replace 指令

## update-vendor.sh — Vendor 目录更新

### 用途
在 `download-deps.sh` 修改 go.mod 之后，执行 `go mod tidy` + `go mod vendor` 同步 vendor 目录。

### 使用方式
```bash
./hack/update-vendor.sh
```

## 注意事项

- **需要网络访问 GitHub**：`download-deps.sh` 通过 `curl` 拉取上游 go.mod，无法在离线环境使用
- **执行前确认 Go 版本**：目标 K8s 版本可能要求特定 Go 版本（如 K8s 1.27 需要 Go 1.20+），先确认本地 Go 版本
- **可能需要额外手动 replace**：部分非 staging 的依赖（如 `sigs.k8s.io/structured-merge-diff`、`k8s.io/utils`、`k8s.io/klog/v2`）可能也需要升级，脚本不覆盖这些，需手动处理
- **编译适配**：升级后 API 接口可能有 breaking change，需逐个修复编译错误

## 典型工作流

```bash
# 1. 切换依赖版本
./hack/download-deps.sh v1.27.3

# 2. 更新 vendor
./hack/update-vendor.sh

# 3. 验证编译
go build ./...

# 4. 修复可能的编译错误后提交
git add go.mod go.sum vendor/
git commit -m "chore: bump k8s dependencies to v1.27.3"
```
