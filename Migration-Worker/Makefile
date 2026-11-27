# =============================================================================
# Migration Worker — Makefile
#
# This Makefile provides standard open-source style targets:
#   - Development: fmt, vet, lint, test, grpc-proto
#   - Build:       build (host binary), docker
#
# The build artifact is a single binary `bin/migration-worker`. The
# release artifact is a container image whose binary lives at
# `/bin/migration-worker`. See the Dockerfile for the multi-stage build.
# =============================================================================

# -----------------------------------------------------------------------------
# Project metadata
# -----------------------------------------------------------------------------
BINARY_NAME    ?= migration-worker
VERSION        ?= v1.0.0
GIT_COMMIT     ?= $(shell git rev-parse HEAD 2>/dev/null || echo "unknown")
BUILD_TIME     ?= $(shell date -u '+%Y-%m-%d %H:%M:%S UTC')

# Container image tag. Override with: make docker IMG=<registry>/<repo>:<tag>
IMAGE_REPO     ?= lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-migration-worker
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

# -----------------------------------------------------------------------------
# Tool locations / versions
# -----------------------------------------------------------------------------
LOCALBIN              ?= $(shell pwd)/bin
GOLANGCI_LINT         ?= $(LOCALBIN)/golangci-lint
GOLANGCI_LINT_VERSION ?= v1.54.2

SHELL       = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

LDFLAGS := -s -w \
	-X 'main.version=$(VERSION)' \
	-X 'main.gitCommit=$(GIT_COMMIT)' \
	-X 'main.buildTime=$(BUILD_TIME)'

# -----------------------------------------------------------------------------
# Help
# -----------------------------------------------------------------------------
.PHONY: all
all: build ## Default target: build the host binary.

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

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
test: fmt vet ## Run unit tests.
	go test ./... -coverprofile cover.out

.PHONY: grpc-proto
grpc-proto: ## Regenerate gRPC stubs from proto files.
	protoc --go_out=./pkg --go-grpc_out=./pkg pkg/apis/container_state_migration.proto
	protoc --go_out=. --go-grpc_out=. --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative pkg/apis/xdm/xdm_checkpoint.proto

##@ Build

.PHONY: build
build: fmt vet ## Build the worker binary into bin/$(BINARY_NAME).
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
		go build -trimpath -ldflags="$(LDFLAGS)" \
		-o bin/$(BINARY_NAME) ./cmd/migration-worker/main.go

.PHONY: run
run: fmt vet ## Run the worker locally for debugging.
	go run ./cmd/migration-worker/main.go

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

.PHONY: show-version
show-version: ## Display version metadata that will be embedded.
	@echo "Version:    $(VERSION)"
	@echo "Git Commit: $(GIT_COMMIT)"
	@echo "Build Time: $(BUILD_TIME)"

##@ Tooling

$(LOCALBIN):
	mkdir -p $(LOCALBIN)

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Install golangci-lint locally.
$(GOLANGCI_LINT): $(LOCALBIN)
	@[ -f $(GOLANGCI_LINT) ] || { \
		set -e ;\
		curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(LOCALBIN) $(GOLANGCI_LINT_VERSION) ;\
	}
