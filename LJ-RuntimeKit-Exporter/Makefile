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

VERSION        ?= v1.0.0
# Builder image tag (registry.cn-hangzhou.aliyuncs.com/eflops-amp/golang).
# Must be >= the `go` directive in go.mod, which dependencies pin to 1.21.
GOLANG_VERSION := 1.21.13
ARCH           := $(shell uname -m)

REGISTRY       ?= registry.cn-hangzhou.aliyuncs.com/eflops-amp/lj-runtimekit-exporter
DOCKERCMD      := sudo DOCKER_BUILDKIT=1 docker build

# Module proxy used when resolving dependencies (this repo does not vendor).
GOPROXY        ?= https://mirrors.aliyun.com/goproxy/,direct

.PHONY: help build vendor test test-verbose test-coverage docker clean

# Default target: show help
help:
	@echo "LJ-Runtimekit-Exporter Build System"
	@echo ""
	@echo "Build:"
	@echo "  build           - Build exporter binary"
	@echo "  vendor          - Generate vendor directory for offline builds"
	@echo ""
	@echo "Test:"
	@echo "  test            - Run unit tests"
	@echo "  test-verbose    - Run unit tests with verbose output"
	@echo "  test-coverage   - Run unit tests with coverage report"
	@echo ""
	@echo "Clean:"
	@echo "  clean           - Remove build artifacts"
	@echo ""
	@echo "Docker:"
	@echo "  docker          - Build GPU exporter image"

# ==============================================================================
# Build
# ==============================================================================

build:
	cd cmd/exporter; go build -o exporter \
		-ldflags "-X 'main.BuildVersion=${VERSION}'" -buildvcs=false

vendor:
	GOPROXY=$(GOPROXY) go mod vendor

# ==============================================================================
# Testing
# ==============================================================================

test:
	@echo "Running unit tests..."
	go test ./pkg/common/... ./pkg/exporter/... -count=1

test-verbose:
	@echo "Running unit tests (verbose)..."
	go test ./pkg/common/... ./pkg/exporter/... -v -count=1

test-coverage:
	@echo "Running unit tests with coverage..."
	go test ./pkg/common/... ./pkg/exporter/... \
		-coverprofile=coverage.out -count=1
	go tool cover -func=coverage.out
	@echo ""
	@echo "HTML coverage report: coverage.html"
	go tool cover -html=coverage.out -o coverage.html

# ==============================================================================
# Docker Images
# ==============================================================================

docker:
	$(DOCKERCMD) \
		--build-arg "AMPERF_VERSION=$(VERSION)" \
		--build-arg "GOLANG_VERSION=$(GOLANG_VERSION)" \
		--build-arg "GOPROXY=$(GOPROXY)" \
		--tag "$(REGISTRY):$(VERSION)" \
		--file docker/Dockerfile .

# ==============================================================================
# Clean
# ==============================================================================

clean:
	rm -rf cmd/exporter/exporter
	rm -rf coverage.out coverage.html
