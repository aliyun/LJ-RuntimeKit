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

# ============================================================
# LJ RuntimeKit Scheduler Framework — Makefile
# ============================================================

# ---------- Go toolchain ----------
GO        ?= go
GOOS      ?= $(shell $(GO) env GOOS)
GOARCH    ?= $(shell $(GO) env GOARCH)

# ---------- Project info ----------
MODULE    ?= lj-runtimekit-scheduler-framework
CMD_PATH   = ./cmd/scheduler
BIN_NAME   = kube-scheduler
BIN_OUTPUT ?= ./$(BIN_NAME)

# ---------- Docker image ----------
REGISTRY   ?= registry.cn-hangzhou.aliyuncs.com/eflops-amp
IMAGE_NAME ?= lj-runtimekit-scheduler-framework
VERSION    ?= v1.0.0
IMAGE      = $(REGISTRY)/$(IMAGE_NAME):$(VERSION)

# ---------- Go module proxy ----------
# This repo does not vendor dependencies; they are resolved through the proxy.
GOPROXY    ?= https://mirrors.aliyun.com/goproxy/,direct

# ---------- Helm ----------
HELM_CHART ?= ./helm
HELM_NS    ?= kube-system
HELM_NAME  ?= lj-runtimekit-scheduler

# ============================================================
# Phony targets
# ============================================================
.PHONY: all build vendor docker docker-save deploy deploy-helm undeploy clean help

# ============================================================
# Default target
# ============================================================
all: build docker
	@echo "=== build + docker done ==="

# ============================================================
# Build: compile the scheduler binary
# ============================================================
build:
	@echo ">>> Building $(BIN_NAME) for $(GOOS)/$(GOARCH) ..."
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) \
		$(GO) build -o $(BIN_OUTPUT) $(CMD_PATH)
	@echo ">>> Binary: $(BIN_OUTPUT)"

# ============================================================
# Vendor: generate vendor/ for offline builds (not required for docker)
# ============================================================
vendor:
	@echo ">>> Vendoring Go dependencies ..."
	GOPROXY=$(GOPROXY) $(GO) mod vendor
	@echo ">>> vendor/ generated"

# ============================================================
# Docker: build container image
# ============================================================
docker:
	@echo ">>> Building Docker image $(IMAGE) ..."
	docker build \
		--build-arg GOPROXY=$(GOPROXY) \
		-t $(IMAGE) \
		-f Dockerfile .
	@echo ">>> Image: $(IMAGE)"

# ============================================================
# Helm package: create helm chart tarball
# ============================================================
helm-package:
	@mkdir -p ./PACK
	@echo ">>> Packaging Helm chart ..."
	tar czvf ./PACK/$(IMAGE_NAME)-helm-$(VERSION).tar.gz $(HELM_CHART)
	@echo ">>> Helm chart: ./PACK/$(IMAGE_NAME)-helm-$(VERSION).tar.gz"

# ============================================================
# Clean: remove build artifacts
# ============================================================
clean:
	@echo ">>> Cleaning build artifacts ..."
	rm -f $(BIN_OUTPUT)
	rm -rf ./PACK
	@echo ">>> Clean done."

# ============================================================
# Help: show available targets
# ============================================================
help:
	@echo "LJ RuntimeKit Scheduler Framework — Makefile Targets"
	@echo ""
	@echo "  build          Compile kube-scheduler binary"
	@echo "  vendor         Generate vendor/ for offline builds"
	@echo "  docker         Build Docker image"
	@echo "  helm-package   Package Helm chart as tarball"
	@echo "  clean          Remove build artifacts"
	@echo "  all            Build binary + Docker image (default)"
	@echo ""
	@echo "Variables:"
	@echo "  REGISTRY   Docker registry  (default: $(REGISTRY))"
	@echo "  VERSION    Image tag        (default: $(VERSION))"
	@echo "  GOOS/ARCH  Cross-compile    (default: $(GOOS)/$(GOARCH))"
	@echo "  HELM_NS    Helm namespace   (default: $(HELM_NS))"
