# Build the manager binary
ARG BUILDER_IMAGE=public.ecr.aws/docker/library/golang:1.22.7
ARG BASE_IMAGE=public.ecr.aws/docker/library/alpine:3.19

FROM ${BUILDER_IMAGE} AS builder
ARG TARGETOS
ARG TARGETARCH
ARG GOPROXY=https://mirrors.aliyun.com/goproxy/,direct

WORKDIR /workspace
# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum

# Copy the go source (required before go mod vendor can resolve imports)
COPY cmd/controller/main.go cmd/controller/main.go
COPY internal/controller/ internal/controller/

# Vendor dependencies and build
RUN GOPROXY=${GOPROXY} go mod vendor
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -mod=vendor -a -o manager cmd/controller/main.go

FROM ${BASE_IMAGE}
WORKDIR /
COPY --from=builder /workspace/manager .
# Base image defaults to root.

ENTRYPOINT ["/manager"]
