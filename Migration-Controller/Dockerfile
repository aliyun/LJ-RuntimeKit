# Multi-stage build for lj-runtimekit-migration-controller.
# The final image contains a single statically linked binary at /bin/migration-controller.
#
# Build:
#   docker build -t lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-migration-controller:v1.14.0-k8s1.34 .
#
# Run (out of cluster, debug only):
#   docker run --rm lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-migration-controller:v1.14.0-k8s1.34 --help

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
COPY api/ api/
COPY pkg/ pkg/

# Produce a stripped, static binary. The version metadata is injected
# via -ldflags so the resulting binary can answer `--version`.
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath \
        -ldflags="-s -w \
            -X 'main.version=${VERSION}' \
            -X 'main.gitCommit=${GIT_COMMIT}' \
            -X 'main.buildTime=${BUILD_TIME}'" \
        -o /out/migration-controller \
        ./cmd/

############################
# Stage 2: runtime
############################
FROM ubuntu:20.04

LABEL org.opencontainers.image.title="lj-runtimekit-migration-controller" \
      org.opencontainers.image.description="Kubernetes controller for live container state migration" \
      org.opencontainers.image.source="https://github.com/container-migration/controller" \
      org.opencontainers.image.licenses="Apache-2.0"

# Install ca-certificates for HTTPS calls to the Kubernetes API server, and
# tzdata so the TZ environment variable below can resolve to a real timezone
# (Go's time package reads TZ + /usr/share/zoneinfo even with CGO_ENABLED=0).
# Also create a dedicated non-root user, and clean apt cache to keep the image small.
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates \
        tzdata \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd -g 65532 nonroot \
 && useradd -u 65532 -g nonroot -s /sbin/nologin -M nonroot

# Align log timestamps with Migration-Worker: klog uses time.Local, so without TZ
# the container defaults to UTC and logs lag CST(UTC+8) by 8 hours.
ENV TZ=Asia/Shanghai

# Place the binary at /bin/migration-controller as the project convention.
COPY --from=builder /out/migration-controller /bin/migration-controller

USER nonroot:nonroot

ENTRYPOINT ["/bin/migration-controller"]
