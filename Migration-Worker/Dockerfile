# Multi-stage build for migration-worker.
# The final image contains a single statically linked binary at /bin/migration-worker.
#
# Build:
#   docker build -t lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-migration-worker:v1.14.0-k8s1.34 .
#
# Run (debug only):
#   docker run --rm lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-migration-worker:v1.14.0-k8s1.34 --help

############################
# Stage 1: build the binary
############################
FROM golang:1.24 AS builder

ARG TARGETOS=linux
ARG TARGETARCH=amd64
ARG VERSION=dev
ARG GIT_COMMIT=unknown
ARG BUILD_TIME=unknown

# Module proxy for the in-container build. Go's built-in default
# (proxy.golang.org) is unreachable from CN networks, and the developer's
# ~/.config/go/env is not visible inside the build, so a working default is
# baked in here. Override with --build-arg GOPROXY=<proxy>, or equivalently
# `make docker GOPROXY=<proxy>`.
ARG GOPROXY=https://goproxy.cn,direct
ENV GOPROXY=${GOPROXY}

WORKDIR /workspace

# Copy module files first for better layer caching.
COPY go.mod go.sum ./

# Copy sources.
COPY cmd/ cmd/
COPY pkg/ pkg/

# Produce a stripped, static binary. The version metadata is injected
# via -ldflags so the resulting binary can answer `--version`.
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath \
        -ldflags="-s -w \
            -X 'main.version=${VERSION}' \
            -X 'main.gitCommit=${GIT_COMMIT}' \
            -X 'main.buildTime=${BUILD_TIME}'" \
        -o /out/migration-worker \
        ./cmd/migration-worker/main.go

############################
# Stage 2: runtime
############################
# NOTE: migration-worker intentionally runs as root in the container.
# It is a node-level daemon that needs to:
#   1. read/write hostPath directories owned by root (e.g. checkpoint save dir),
#   2. talk to the local container runtime / xdm UNIX socket on the host,
#   3. share the host network namespace (hostNetwork: true).
# Switching to a non-root UID is therefore not feasible without granting
# extra capabilities. If you need stricter isolation, drop unneeded Linux
# capabilities via securityContext at the Pod level instead.
FROM ubuntu:20.04

LABEL org.opencontainers.image.title="migration-worker" \
      org.opencontainers.image.description="Node-side daemon for live container state migration" \
      org.opencontainers.image.source="https://github.com/container-migration/worker" \
      org.opencontainers.image.licenses="Apache-2.0"

# ca-certificates: HTTPS calls to remote storage backends.
# nfs-common:      mount NFS-backed checkpoint stores when needed.
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates \
        nfs-common \
        tzdata \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

ENV TZ=Asia/Shanghai

COPY --from=builder /out/migration-worker /bin/migration-worker

ENTRYPOINT ["/bin/migration-worker"]
