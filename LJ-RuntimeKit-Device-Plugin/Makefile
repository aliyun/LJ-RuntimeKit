# Copyright (c) 2024-2026 Alibaba Cloud.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ==============================================================================
# LJ RuntimeKit Device Plugin Makefile
# ==============================================================================

# Project variables
MODULE_NAME     := lj-runtimekit-device-plugin
BINARY_NAME     := lj-runtimekit-device-plugin
CMD_DIR         := cmd
# Build the whole cmd package, not just main.go: runHealthCheck and friends live
# in cmd/healthcheck.go, so compiling a single file leaves them undefined.
MAIN_PKG        := ./$(CMD_DIR)/
OUTPUT_DIR      := bin

# Docker variables
IMAGE_REGISTRY  := registry.cn-hangzhou.aliyuncs.com
IMAGE_NAMESPACE := eflops-amp
IMAGE_NAME      := lj-runtimekit-device-plugin
IMAGE_TAG       ?= v1.12.207
IMAGE_FULL      := $(IMAGE_REGISTRY)/$(IMAGE_NAMESPACE)/$(IMAGE_NAME):$(IMAGE_TAG)

# Kubernetes variables
NAMESPACE       := kube-system
YAML_DIR        := yaml
RBAC_YAML       := $(YAML_DIR)/lj-runtimekit-device-plugin-rbac.yaml
DS_YAML         := $(YAML_DIR)/lj-runtimekit-device-plugin-ds.yaml

# Build variables
CGO_ENABLED     ?= 1
GOOS            ?= linux
GOARCH          ?= amd64
GOPROXY         ?= https://mirrors.aliyun.com/goproxy/,direct
LDFLAGS         := -s -w

# ==============================================================================
# Phony targets
# ==============================================================================
.PHONY: all build clean test lint docker deploy undeploy vendor help

# ==============================================================================
# Default target
# ==============================================================================
all: build

# ==============================================================================
# Build
# ==============================================================================

## build: Build the binary for Linux (default target)
build:
	@echo "==> Building $(BINARY_NAME) for $(GOOS)/$(GOARCH)..."
	@mkdir -p $(OUTPUT_DIR)
	CGO_ENABLED=$(CGO_ENABLED) GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -ldflags="$(LDFLAGS)" -o $(OUTPUT_DIR)/$(BINARY_NAME) $(MAIN_PKG)
	@echo "==> Binary output: $(OUTPUT_DIR)/$(BINARY_NAME)"

## build-local: Build the binary for the current OS
build-local:
	@echo "==> Building $(BINARY_NAME) for local environment..."
	@mkdir -p $(OUTPUT_DIR)
	go build -ldflags="$(LDFLAGS)" -o $(OUTPUT_DIR)/$(BINARY_NAME) $(MAIN_PKG)
	@echo "==> Binary output: $(OUTPUT_DIR)/$(BINARY_NAME)"

# ==============================================================================
# Test
# ==============================================================================

## vendor: Generate vendor directory for offline builds (not required for docker target)
vendor:
	@echo "==> Vendoring Go dependencies..."
	GOPROXY=$(GOPROXY) go mod vendor

# ==============================================================================
# Test
# ==============================================================================

## test: Run all unit tests
test:
	@echo "==> Running unit tests..."
	go test -v -count=1 -race ./internal/utils/... ./internal/plugin/...

## test-coverage: Run tests with coverage report
test-coverage:
	@echo "==> Running tests with coverage..."
	go test -v -count=1 -coverprofile=coverage.out ./internal/utils/... ./internal/plugin/...
	go tool cover -func=coverage.out
	@echo "==> Full HTML report: coverage.html"
	go tool cover -html=coverage.out -o coverage.html

# ==============================================================================
# Docker
# ==============================================================================

## docker: Build Docker image
docker:
	@echo "==> Building Docker image $(IMAGE_FULL)..."
	docker build --build-arg GOPROXY=$(GOPROXY) -t $(IMAGE_FULL) -f Dockerfile .
	@echo "==> Image built: $(IMAGE_FULL)"

# ==============================================================================
# Kubernetes Deployment
# ==============================================================================

## deploy: Deploy to Kubernetes cluster
deploy:
	@echo "==> Deploying to namespace $(NAMESPACE)..."
	kubectl apply -f $(RBAC_YAML)
	kubectl apply -f $(DS_YAML)
	@echo "==> Deployment complete."
	@echo "==> Check status: make status"

## undeploy: Remove deployment from Kubernetes cluster
undeploy:
	@echo "==> Removing deployment from namespace $(NAMESPACE)..."
	kubectl delete -f $(DS_YAML) --ignore-not-found=true
	kubectl delete -f $(RBAC_YAML) --ignore-not-found=true
	@echo "==> Undeployment complete."

# ==============================================================================
# Utilities
# ==============================================================================

## clean: Remove build artifacts
clean:
	@echo "==> Cleaning build artifacts..."
	rm -rf $(OUTPUT_DIR)
	rm -f coverage.out coverage.html
	@echo "==> Clean complete."

## help: Show this help message
help:
	@echo "LJ RuntimeKit Device Plugin - Available Targets:"
	@echo ""
	@echo "  Build:"
	@echo "    build          Build binary for Linux (default)"
	@echo "    build-local    Build binary for current OS"
	@echo "    clean          Remove build artifacts"
	@echo ""
	@echo "  Test:"
	@echo "    test           Run unit tests"
	@echo "    test-coverage  Run tests with coverage report"
	@echo ""
	@echo "  Docker:"
	@echo "    docker         Build Docker image"
	@echo ""
	@echo "  Kubernetes:"
	@echo "    deploy         Deploy to cluster"
	@echo "    undeploy       Remove from cluster"
	@echo ""
	@echo "  Variables (override with make VAR=value):"
	@echo "    IMAGE_TAG      Docker image tag (default: v1.0.0)"
	@echo "    NAMESPACE      K8s namespace (default: kube-system)"
	@echo "    CGO_ENABLED    CGO enabled (default: 1)"
	@echo "    GOOS           Target OS (default: linux)"
	@echo "    GOARCH         Target arch (default: amd64)"
	@echo ""
