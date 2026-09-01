# Image URL to use all building/pushing image targets
IMG ?= lj-runtimekit-controller:latest
JOB_AGENT_IMG ?= lj-runtimekit-controller-job-agent:latest
BUILDER_IMAGE ?= public.ecr.aws/docker/library/golang:1.22.7
BASE_IMAGE ?= public.ecr.aws/docker/library/alpine:3.19
GOPROXY ?= https://mirrors.aliyun.com/goproxy/,direct

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# Setting SHELL to bash allows bash commands to be executed by recipes.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

##@ General

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: proto
proto: ## Generate Go code from protobuf definitions.
	protoc --go_out=. --go_opt=module=github.com/lj-runtimekit/controller \
		--go-grpc_out=. --go-grpc_opt=module=github.com/lj-runtimekit/controller \
		api/proto/*.proto

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: test
test: fmt vet ## Run tests.
	go test ./... -coverprofile cover.out

##@ Build

.PHONY: build
build: fmt vet ## Build manager, job-agent and kubectl-amp binaries.
	go build -o bin/manager cmd/controller/main.go
	go build -o bin/job-agent cmd/job-agent/main.go
	go build -o bin/kubectl-amp cmd/kubectl-amp/main.go

.PHONY: build-controller
build-controller: ## Build controller binary only.
	go build -o bin/manager cmd/controller/main.go

.PHONY: build-job-agent
build-job-agent: ## Build job-agent binary only.
	go build -o bin/job-agent cmd/job-agent/main.go

.PHONY: build-kubectl-amp
build-kubectl-amp: ## Build kubectl-amp binary only.
	go build -o bin/kubectl-amp cmd/kubectl-amp/main.go

.PHONY: run
run: fmt vet ## Run a controller from your host.
	go run ./cmd/controller/main.go

.PHONY: docker docker-build
docker docker-build: ## Build docker images for controller and job-agent.
	docker build -t ${IMG} -f Dockerfile \
		--build-arg BUILDER_IMAGE=${BUILDER_IMAGE} \
		--build-arg BASE_IMAGE=${BASE_IMAGE} \
		--build-arg GOPROXY=${GOPROXY} .
	docker build -t ${JOB_AGENT_IMG} -f Dockerfile.job-agent \
		--build-arg BUILDER_IMAGE=${BUILDER_IMAGE} \
		--build-arg BASE_IMAGE=${BASE_IMAGE} \
		--build-arg GOPROXY=${GOPROXY} .

.PHONY: vendor
vendor: ## Generate vendor directory for offline builds (not required for docker target).
	go mod vendor

.PHONY: docker-push
docker-push: ## Push docker images.
	docker push ${IMG}
	docker push ${JOB_AGENT_IMG}

##@ Deployment

.PHONY: install
install: ## Install CRDs into the K8s cluster specified in ~/.kube/config.
	kubectl apply -k config/default

.PHONY: uninstall
uninstall: ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config.
	kubectl delete -k config/default

.PHONY: deploy
deploy: ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	kubectl apply -k config/default

.PHONY: undeploy
undeploy: ## Undeploy controller from the K8s cluster specified in ~/.kube/config.
	kubectl delete -k config/default

.PHONY: clean
clean: ## Clean build artifacts.
	rm -rf bin/
