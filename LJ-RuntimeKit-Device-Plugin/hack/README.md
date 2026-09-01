# hack/ 工具链

Device-Plugin 仓库的开发辅助脚本。当前主要用于一键切换 `k8s.io/*` staging 依赖版本。

## download-deps.sh — K8s 依赖版本切换

### 用途
将 Device-Plugin 的 `k8s.io/*` staging 依赖一键切换到指定 Kubernetes 版本。

### 使用方式
```bash
# 切换到 K8s 1.21.2
./hack/download-deps.sh v1.21.2

# 切换到 K8s 1.36.1
./hack/download-deps.sh v1.36.1
```

### 工作原理
1. 从 GitHub 拉取目标版本 `kubernetes/kubernetes` 的 go.mod
2. 解析出所有 `k8s.io/* => ./staging/src/k8s.io/*` 的 replace 行（脚本动态解析，无需手动列出模块）
3. 对每个模块调用 `go mod download -json` 获取确切版本
4. 在本地 go.mod 中写入 `replace k8s.io/xxx => k8s.io/xxx@版本`
5. 执行 `go mod download`

> 说明：Device-Plugin 并不依赖 `k8s.io/kubernetes` 主模块，因此脚本中**没有** `go get k8s.io/kubernetes@目标版本` 这一步。脚本只会对 Device-Plugin 实际引用的 staging 模块（如 `k8s.io/api`、`k8s.io/apimachinery`、`k8s.io/client-go`、`k8s.io/component-base`、`k8s.io/kubelet` 等）生成 replace。由于版本清单从目标 go.mod 动态解析，切换到不同 K8s 版本时无需改动脚本。

### 注意事项
- **需要网络访问 GitHub**：脚本通过 `curl` 拉取上游 go.mod，无法在离线环境使用
- **执行前确认 Go 版本**：目标 K8s 版本可能要求特定 Go 版本（如 K8s 1.36 需要 Go 1.26+），先升级 Go
- **执行后需 vendor 更新**：脚本只修改 go.mod，执行后需手动 `go mod tidy && go mod vendor` 更新 vendor 目录
- **可能需要额外手动 replace**：部分非 staging 的依赖（如 `sigs.k8s.io/structured-merge-diff`）可能也需要升级，脚本不覆盖这些，需手动处理
- **编译适配**：升级后 API 接口可能有 breaking change，需逐个修复 Device-Plugin 的编译错误

## tools.go — 工具依赖声明

通过 build tag `tools` 声明 `k8s.io/code-generator` 依赖，确保 `go mod tidy` 不会自动移除它。
无需直接调用。

## boilerplate.go.txt — 版权声明模板

code-generator 生成代码时使用的文件头模板。如需生成代码，传入 `--go-header-file hack/boilerplate.go.txt`。
