ARG GOLANG_VERSION
# CGO requires a C toolchain. Keep the glibc baseline compatible with host execution
# on Ubuntu 22.04 and 24.04. Custom builder images must preserve this compatibility.
# Override with --build-arg BUILDER_IMAGE=<your mirror> if needed.
ARG BUILDER_IMAGE=golang:${GOLANG_VERSION}-bullseye
FROM ${BUILDER_IMAGE} AS builder
WORKDIR /go/src/exporter

COPY . .

ARG GOPROXY="https://mirrors.aliyun.com/goproxy/,https://goproxy.cn/,direct"
ENV GOPROXY=${GOPROXY}
ENV GO111MODULE="on"

# The repo does not ship a vendor/ directory; materialise it from the module
# proxy so the build below resolves dependencies from vendor/.
RUN go mod vendor

ARG AMPERF_VERSION
# Mirrors the `build` target in the Makefile. Invoked directly rather than via
# `make` because the builder image does not ship make.
RUN cd cmd/exporter \
 && go build -mod=vendor -o exporter \
        -ldflags "-X 'main.BuildVersion=${AMPERF_VERSION}'" -buildvcs=false
RUN cd cmd/rt_npd_adapter \
 && go build -mod=vendor -o rt_npd_adapter \
        -ldflags "-s -w" -trimpath -buildvcs=false

FROM registry.cn-hangzhou.aliyuncs.com/eflops-amp/ubuntu:22.04
LABEL io.k8s.display-name="lj-runtimekit-exporter"

COPY --from=builder /go/src/exporter/cmd/exporter/exporter /usr/local/harp/amperf/
COPY --from=builder /go/src/exporter/cmd/rt_npd_adapter/rt_npd_adapter /usr/local/harp/amperf/
COPY etc /usr/local/harp/etc

RUN apt-get update && apt-get install -y --no-install-recommends dmidecode

WORKDIR /workspace
ENTRYPOINT ["/usr/local/harp/amperf/exporter"]
