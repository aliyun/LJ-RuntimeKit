FROM golang:1.21 as build

ARG GOPROXY=https://mirrors.aliyun.com/goproxy/,direct

WORKDIR /go/src/device-plugin
COPY . .

# The repo does not ship a vendor/ directory; materialise it from the module
# proxy so the build below is reproducible and offline from here on.
RUN GOPROXY=${GOPROXY} go mod vendor

RUN export CGO_LDFLAGS_ALLOW='-Wl,--unresolved-symbols=ignore-in-object-files' && \
    go build -mod=vendor -ldflags="-s -w" -o /go/bin/lj-runtimekit-device-plugin ./cmd/

FROM ubuntu:22.04

ENV XPU_ENABLE="1"
ENV XPU_VISIBLE_DEVICES="all"
ENV NVIDIA_VISIBLE_DEVICES="all"

COPY --from=build /go/bin/lj-runtimekit-device-plugin /usr/bin/lj-runtimekit-device-plugin

CMD ["lj-runtimekit-device-plugin","-logtostderr"]
