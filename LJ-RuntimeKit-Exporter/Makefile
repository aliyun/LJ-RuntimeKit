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
# LJ-Runtimekit-Exporter - Makefile
# ==============================================================================

VERSION        ?= v1.14.0-k8s1.34
# Go version used by the Debian Bullseye builder for host ABI compatibility.
# Must be >= the `go` directive in go.mod, which dependencies pin to 1.21.
GOLANG_VERSION := 1.21.13
ARCH           := $(shell uname -m)
PROTOC         ?= protoc
GO             ?= go
GOOS           ?= $(shell $(GO) env GOOS)
GOARCH         ?= $(shell $(GO) env GOARCH)

REGISTRY       ?= lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-exporter
COLLECTOR_IMG  ?= lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-profiling-collector:$(VERSION)
# ?= so CI or a non-interactive shell can drop the sudo prefix:
#   make docker DOCKERCMD='DOCKER_BUILDKIT=1 docker build'
DOCKERCMD      ?= sudo DOCKER_BUILDKIT=1 docker build
COLLECTOR_OUTPUT ?= cmd/collector/profiling-collector
COLLECTOR_BUILD_DIR := build/collector/$(GOOS)-$(GOARCH)
DIST_DIR       ?= dist
TEST_PACKAGES  := ./cmd/... ./pkg/... ./api/...

# Module proxy used when resolving dependencies (this repo does not vendor).
# goproxy.cn is kept as a fallback because the Aliyun mirror is missing a few
# indirect modules (e.g. github.com/elazarl/goproxy), and `direct` is unreachable
# from the internal build network.
GOPROXY        ?= https://mirrors.aliyun.com/goproxy/,https://goproxy.cn/,direct
# Export it so every recipe resolves modules through the mirror instead of the
# host's default GOPROXY, which is unreachable from the internal build network.
export GOPROXY

.PHONY: help build build-exporter build-collector package-collector build-adapter generate-xdm vendor test test-collector test-verbose test-coverage docker docker-exporter docker-collector docker-all clean

# Default target: show help
help:
	@echo "LJ-Runtimekit-Exporter Build System"
	@echo ""
	@echo "Build:"
	@echo "  build             - Build exporter and profiling collector binaries"
	@echo "  build-exporter    - Build exporter binary"
	@echo "  build-adapter     - Build NPD adapter binary"
	@echo "  build-collector   - Build standalone collector (GOOS/GOARCH supported)"
	@echo "  package-collector - Package collector and license as a versioned tar.gz"
	@echo "  generate-xdm      - Regenerate XDM protobuf bindings (requires protoc and Go plugins)"
	@echo "  vendor            - Generate vendor directory for offline builds"
	@echo ""
	@echo "Test:"
	@echo "  test              - Run all unit tests"
	@echo "  test-collector    - Run standalone collector tests without GPU libraries"
	@echo "  test-verbose      - Run unit tests with verbose output"
	@echo "  test-coverage     - Run unit tests with coverage report"
	@echo ""
	@echo "Clean:"
	@echo "  clean             - Remove build artifacts"
	@echo ""
	@echo "Docker:"
	@echo "  docker            - Build both images"
	@echo "  docker-exporter   - Build GPU exporter image including the host NPD adapter"
	@echo "  docker-collector  - Build standalone profiling collector image"
	@echo "  docker-all        - Build both images (alias of docker)"

# ==============================================================================
# Build
# ==============================================================================

build: build-exporter build-collector

build-exporter:
	$(GO) build -o cmd/exporter/exporter \
		-ldflags "-X 'main.BuildVersion=${VERSION}'" -buildvcs=false ./cmd/exporter

build-collector:
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) $(GO) build -trimpath \
		-ldflags "-s -w -X 'main.BuildVersion=$(VERSION)'" -buildvcs=false \
		-o "$(COLLECTOR_OUTPUT)" ./cmd/collector

package-collector:
	mkdir -p "$(COLLECTOR_BUILD_DIR)" "$(DIST_DIR)"
	$(MAKE) build-collector COLLECTOR_OUTPUT="$(COLLECTOR_BUILD_DIR)/profiling-collector"
	tar -czf "$(DIST_DIR)/profiling-collector-$(VERSION)-$(GOOS)-$(GOARCH).tar.gz" \
		-C "$(COLLECTOR_BUILD_DIR)" profiling-collector -C "$(CURDIR)" LICENSE

build-adapter:
	cd cmd/rt_npd_adapter; $(GO) build -o rt_npd_adapter \
		-ldflags "-s -w" -buildvcs=false

# Use the existing generator versions: protoc-gen-go v1.28.0 and protoc-gen-go-grpc v1.2.0.
generate-xdm:
	$(PROTOC) -I=pkg/grpc/xdm --go_out=pkg/grpc/xdm --go_opt=paths=source_relative \
		--go-grpc_out=pkg/grpc/xdm --go-grpc_opt=paths=source_relative pkg/grpc/xdm/api_outer.proto

vendor:
	GOPROXY=$(GOPROXY) $(GO) mod vendor

# ==============================================================================
# Testing
# ==============================================================================

test:
	@echo "Running unit tests..."
	$(GO) test $(TEST_PACKAGES) -count=1

test-collector:
	CGO_ENABLED=0 $(GO) test ./cmd/collector/... ./pkg/collector/... ./pkg/safefs/... ./api/... -count=1

test-verbose:
	@echo "Running unit tests (verbose)..."
	$(GO) test $(TEST_PACKAGES) -v -count=1

test-coverage:
	@echo "Running unit tests with coverage..."
	$(GO) test $(TEST_PACKAGES) -coverprofile=coverage.out -count=1
	$(GO) tool cover -func=coverage.out
	@echo ""
	@echo "HTML coverage report: coverage.html"
	$(GO) tool cover -html=coverage.out -o coverage.html

# ==============================================================================
# Docker Images
# ==============================================================================

docker-exporter:
	$(DOCKERCMD) \
		--build-arg "AMPERF_VERSION=$(VERSION)" \
		--build-arg "GOLANG_VERSION=$(GOLANG_VERSION)" \
		--build-arg "GOPROXY=$(GOPROXY)" \
		--tag "$(REGISTRY):$(VERSION)" \
		--file docker/Dockerfile .

docker-collector:
	$(DOCKERCMD) \
		--build-arg "VERSION=$(VERSION)" \
		--build-arg "GOLANG_VERSION=$(GOLANG_VERSION)" \
		--build-arg "GOPROXY=$(GOPROXY)" \
		--tag "$(COLLECTOR_IMG)" \
		--file docker/Dockerfile.collector .

docker: docker-exporter docker-collector

# Kept as the historical name; `docker` is the primary entry point.
docker-all: docker

# ==============================================================================
# Clean
# ==============================================================================

clean:
	rm -rf cmd/exporter/exporter cmd/rt_npd_adapter/rt_npd_adapter
	rm -f "$(COLLECTOR_OUTPUT)"
	rm -rf build/collector
	rm -f "$(DIST_DIR)"/profiling-collector-*.tar.gz
	rm -f coverage.out coverage.html
