SHELL := /bin/bash
.DEFAULT_GOAL := build

BINARY ?= bin/job-controller
CMD ?= cmd/controller
GO ?= go
GOOS ?= linux
GOARCH ?= amd64
CONTAINER_TOOL ?= docker
CONTROLLER_IMG ?= lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-job-controller:v1.14.0-k8s1.34
COLLECTOR_IMG ?= lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-profiling-collector:v1.14.0-k8s1.34
KUBECTL ?= kubectl

FEATURE_GATES ?= ComputeDomain=false,Profiling=false,ActiveMonitoring=false
NAMESPACE ?= kube-system
COLLECTOR_CONFIGMAP ?= lj-runtimekit-jobcontroller-config

LOCALBIN ?= bin
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
CONTROLLER_TOOLS_VERSION ?= v0.16.5
PROFILING_PATHS := ./api/v1alpha1;./pkg/controller/profiling
CRD_FILES := deploy/computedomains-crd.yaml deploy/computedomains-aliyun-crd.yaml \
	deploy/computedomaincliques-crd.yaml deploy/computedomaincliques-aliyun-crd.yaml \
	deploy/profilingjobs-crd.yaml deploy/activemonitorings-crd.yaml
CRD_FLAGS = $(foreach file,$(CRD_FILES),-f $(file))
CRD_WAIT_TIMEOUT ?= 60s
UNDEPLOY_TIMEOUT ?= 180s

RENDER_DEPLOYMENT = sed \
	-e 's|^  namespace: kube-system$$|  namespace: $(NAMESPACE)|' \
	-e 's|^        image: .*|        image: $(CONTROLLER_IMG)|' \
	-e 's|^        - --namespace=.*|        - --namespace=$(NAMESPACE)|' \
	-e 's|^        - --feature-gates=.*|        - --feature-gates=$(FEATURE_GATES)|' \
	-e 's|^        - --controllerConfig=.*|        - --controllerConfig=$(COLLECTOR_CONFIGMAP)|' \
	deploy/controller-deployment.yaml
RENDER_RBAC = sed -e 's|^  namespace: kube-system$$|  namespace: $(NAMESPACE)|' deploy/rbac.yaml
RENDER_COLLECTOR_CONFIG = sed \
	-e 's|^  namespace: kube-system$$|  namespace: $(NAMESPACE)|' \
	-e 's|^  name: .*$$|  name: $(COLLECTOR_CONFIGMAP)|' \
	-e 's|^    image: .*|    image: $(COLLECTOR_IMG)|' \
	deploy/lj-runtimekit-jobcontroller-configmap.yaml

# 按 Controller 实际解析语义判定是否至少启用一个功能：逗号分隔、裁剪空白、同名后者覆盖、
# 布尔值等价 strconv.ParseBool 的 true 形式；退出码 0 表示有启用，1 表示三个全关。
FEATURE_GATES_CHECK = echo "$(FEATURE_GATES)" | awk -F, '{ for (i = 1; i <= NF; i++) { s = $$i; gsub(/^[[:space:]]+|[[:space:]]+$$/, "", s); if (s == "") continue; p = index(s, "="); if (p == 0) continue; k = tolower(substr(s, 1, p - 1)); v = tolower(substr(s, p + 1)); gsub(/^[[:space:]]+|[[:space:]]+$$/, "", k); gsub(/^[[:space:]]+|[[:space:]]+$$/, "", v); if (k == "computedomain" || k == "profiling" || k == "activemonitoring") last[k] = v } on = 0; for (k in last) if (last[k] == "true" || last[k] == "1" || last[k] == "t") on = 1; exit !on }'

.PHONY: build clean docker test vet manifests generate controller-gen \
	install render-deployment render-rbac render-collector-config deploy undeploy

build:
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) $(GO) build -o "$(BINARY)" ./$(CMD)

# 仅清理构建产物，保留 controller-gen 工具及 Go 缓存。
clean:
	rm -f -- "$(BINARY)"

docker:
	$(CONTAINER_TOOL) build -t "$(CONTROLLER_IMG)" .

test:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

# 仅生成 Profiling CRD；兼容原 config 路径，部署副本同步生成，统一 RBAC 不参与生成。
manifests: controller-gen
	"$(CONTROLLER_GEN)" crd paths="$(PROFILING_PATHS)" output:crd:artifacts:config=config/crd/bases
	cp config/crd/bases/profiling.runtimekit.io_profilingjobs.yaml deploy/profilingjobs-crd.yaml

generate: controller-gen
	"$(CONTROLLER_GEN)" object:headerFile="hack/boilerplate.go.txt" paths="$(PROFILING_PATHS)"

controller-gen: $(CONTROLLER_GEN)

$(CONTROLLER_GEN):
	mkdir -p "$(dir $(CONTROLLER_GEN))"
	GOBIN="$(abspath $(dir $(CONTROLLER_GEN)))" $(GO) install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_TOOLS_VERSION)

# 固定安装完整 CRD 集合；开关仅控制运行时功能，不改变安装行为。
install:
	$(KUBECTL) apply $(CRD_FLAGS)
	$(KUBECTL) wait --for=condition=Established --timeout="$(CRD_WAIT_TIMEOUT)" $(CRD_FLAGS)

render-deployment:
	@$(RENDER_DEPLOYMENT)

render-rbac:
	@$(RENDER_RBAC)

render-collector-config:
	@$(RENDER_COLLECTOR_CONFIG)

# 所有开关组合使用相同资源集合和部署顺序，禁用功能不会删除资源。
# 三个功能默认关闭：未显式启用时先提示，避免部署后 Pod 因“至少启用一个功能”退出。
deploy: install
	@$(FEATURE_GATES_CHECK) || echo "WARNING: FEATURE_GATES=$(FEATURE_GATES) 未启用任何功能，Controller 启动后会立即退出；请显式设置要启用的功能（见 README 功能开关表）" >&2
	$(KUBECTL) apply -f deploy/activemonitoring-priorityclass.yaml
	set -o pipefail; $(RENDER_COLLECTOR_CONFIG) | $(KUBECTL) apply -f -
	set -o pipefail; $(RENDER_RBAC) | $(KUBECTL) apply -f -
	set -o pipefail; $(RENDER_DEPLOYMENT) | $(KUBECTL) apply -f -

# 先等待 Controller 完全退出，再删除权限和配置；保留 CRD、业务资源及数据。
undeploy:
	set -o pipefail; $(RENDER_DEPLOYMENT) | $(KUBECTL) delete --ignore-not-found --cascade=foreground --wait=true --timeout="$(UNDEPLOY_TIMEOUT)" -f -
	set -o pipefail; $(RENDER_RBAC) | $(KUBECTL) delete --ignore-not-found --wait=true --timeout="$(UNDEPLOY_TIMEOUT)" -f -
	set -o pipefail; $(RENDER_COLLECTOR_CONFIG) | $(KUBECTL) delete --ignore-not-found --wait=true --timeout="$(UNDEPLOY_TIMEOUT)" -f -
