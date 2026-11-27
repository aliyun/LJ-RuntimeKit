# =============================================================================
# Container Migration Controller — Makefile
#
# This Makefile provides standard open-source style targets:
#   - Development: fmt, vet, lint, test, generate, manifests, grpc-proto
#   - Build:       build (host binary), docker
#   - Tooling:     controller-gen, kustomize, golangci-lint, envtest
#
# The build artifact is a single binary `bin/migration-controller`. The
# release artifact is a container image whose binary lives at
# `/bin/migration-controller`. See the Dockerfile for the multi-stage build.
# =============================================================================

# -----------------------------------------------------------------------------
# Project metadata
# -----------------------------------------------------------------------------
BINARY_NAME    ?= migration-controller
VERSION        ?= v1.0.0
GIT_COMMIT     ?= $(shell git rev-parse HEAD 2>/dev/null || echo "unknown")
BUILD_TIME     ?= $(shell date -u '+%Y-%m-%d %H:%M:%S UTC')

# Container image tag. The default points to the lj-runtimekit private
# registry; override with: make docker IMG=<registry>/<repo>:<tag>
IMAGE_REPO     ?= lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-migration-controller
IMG            ?= $(IMAGE_REPO):$(VERSION)
CONTAINER_TOOL ?= docker

# Module proxy override for the image build. Deliberately NOT auto-detected:
# `sudo make docker` evaluates this Makefile as root, whose go env has no
# GOPROXY and therefore reports Go's built-in default (proxy.golang.org) —
# forwarding that would silently override the working default baked into the
# Dockerfile. Left empty to use the Dockerfile default; override with either
#   make docker GOPROXY=<proxy>      or      GOPROXY=<proxy> make docker
GOPROXY        ?=
IMG_BUILD_ARGS := $(if $(GOPROXY),--build-arg GOPROXY='$(GOPROXY)',)

# Kubebuilder envtest k8s assets version.
ENVTEST_K8S_VERSION = 1.24.2

# -----------------------------------------------------------------------------
# Tool locations / versions
# -----------------------------------------------------------------------------
LOCALBIN                 ?= $(shell pwd)/bin
KUSTOMIZE                ?= $(LOCALBIN)/kustomize
CONTROLLER_GEN           ?= $(LOCALBIN)/controller-gen
ENVTEST                  ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT            ?= $(LOCALBIN)/golangci-lint

KUSTOMIZE_VERSION        ?= v5.2.1
CONTROLLER_TOOLS_VERSION ?= v0.16.5
GOLANGCI_LINT_VERSION    ?= v1.54.2

KUBECTL                  ?= kubectl
SHELL                     = /usr/bin/env bash -o pipefail
.SHELLFLAGS               = -ec

# Version metadata injected into the binary via -ldflags -X so that
# `migration-controller --version` reports what is actually running.
LDFLAGS := -s -w \
	-X 'main.version=$(VERSION)' \
	-X 'main.gitCommit=$(GIT_COMMIT)' \
	-X 'main.buildTime=$(BUILD_TIME)'

ifeq (,$(shell go env GOBIN))
GOBIN = $(shell go env GOPATH)/bin
else
GOBIN = $(shell go env GOBIN)
endif

# -----------------------------------------------------------------------------
# Help
# -----------------------------------------------------------------------------
.PHONY: all
all: build ## Default target: build the host binary.

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: manifests
manifests: controller-gen ## Generate CRD manifests into deploy/.
# Only CRDs are generated. RBAC does not go through controller-gen:
# deploy/rbac.yaml is a hand-maintained combined document (leader-election
# Role/RoleBinding + ClusterRole + ClusterRoleBinding + ServiceAccount, five
# objects), while the rbac generator only emits the ClusterRole of it, and the
# code lacks markers such as pods/exec and nodes, so the generated result has
# fewer permissions than actually needed. Keeping rbac:roleName= would only
# write an incomplete role.yaml to the default path config/rbac/, misleading
# readers.
#
# If generation is adopted later, all kubebuilder:rbac markers must be filled
# in first and a permission audit must be done.
	$(CONTROLLER_GEN) crd paths="./..." output:crd:artifacts:config=deploy
	mv deploy/migration.io_containerstatemigrationjobrequests.yaml deploy/crd.yaml
	mv deploy/migration.io_preheatjobs.yaml deploy/preheatjob-crd.yaml

.PHONY: generate
generate: controller-gen ## Generate DeepCopy / DeepCopyInto / DeepCopyObject methods.
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

.PHONY: grpc-proto
grpc-proto: ## Regenerate gRPC stubs from proto/container_state_migration.proto.
	protoc --go_out=./pkg --go-grpc_out=./pkg proto/container_state_migration.proto

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: lint
lint: golangci-lint ## Run golangci-lint.
	$(GOLANGCI_LINT) run

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint with autofix.
	$(GOLANGCI_LINT) run --fix

.PHONY: test
test: manifests generate fmt vet envtest ## Run unit tests.
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(shell pwd)/bin -p path)" \
		go test $$(go list ./... | grep -v /e2e) -coverprofile cover.out

##@ Build

.PHONY: build
build: fmt vet ## Build the controller binary into bin/$(BINARY_NAME).
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
		go build -trimpath -ldflags="$(LDFLAGS)" \
		-o bin/$(BINARY_NAME) ./cmd/

.PHONY: run
run: fmt vet ## Run the controller out-of-cluster against the current kube-context.
	go run ./cmd/

.PHONY: docker
docker: ## Build the container image via multi-stage Dockerfile.
	$(CONTAINER_TOOL) build --platform linux/amd64 $(IMG_BUILD_ARGS) \
		--build-arg VERSION='$(VERSION)' \
		--build-arg GIT_COMMIT='$(GIT_COMMIT)' \
		--build-arg BUILD_TIME='$(BUILD_TIME)' \
		-t $(IMG) .

.PHONY: clean
clean: ## Remove build artifacts.
	rm -rf bin/ output/ cover.out

##@ Tooling

$(LOCALBIN):
	mkdir -p $(LOCALBIN)

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Install controller-gen locally.
$(CONTROLLER_GEN): $(LOCALBIN)
	test -s $(LOCALBIN)/controller-gen && $(LOCALBIN)/controller-gen --version | grep -q $(CONTROLLER_TOOLS_VERSION) || \
		GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_TOOLS_VERSION)

.PHONY: kustomize
kustomize: $(KUSTOMIZE) ## Install kustomize locally.
$(KUSTOMIZE): $(LOCALBIN)
	@if test -x $(LOCALBIN)/kustomize && ! $(LOCALBIN)/kustomize version | grep -q $(KUSTOMIZE_VERSION); then \
		echo "$(LOCALBIN)/kustomize version mismatch; reinstalling."; \
		rm -rf $(LOCALBIN)/kustomize; \
	fi
	test -s $(LOCALBIN)/kustomize || GOBIN=$(LOCALBIN) GO111MODULE=on go install sigs.k8s.io/kustomize/kustomize/v5@$(KUSTOMIZE_VERSION)

.PHONY: envtest
envtest: $(ENVTEST) ## Install setup-envtest locally.
$(ENVTEST): $(LOCALBIN)
	test -s $(LOCALBIN)/setup-envtest || GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Install golangci-lint locally.
$(GOLANGCI_LINT): $(LOCALBIN)
	@[ -f $(GOLANGCI_LINT) ] || { \
		set -e ;\
		curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(LOCALBIN) $(GOLANGCI_LINT_VERSION) ;\
	}
