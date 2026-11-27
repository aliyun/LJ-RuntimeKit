# Stage 1: Build
FROM golang:1.24 AS builder

ARG GOPROXY=https://mirrors.aliyun.com/goproxy/,direct
ENV GOPROXY=${GOPROXY}

WORKDIR /workspace

COPY go.mod go.sum* ./
RUN go mod download

COPY cmd/ cmd/
COPY pkg/ pkg/
COPY api/ api/
COPY templates/ templates/

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /workspace/bin/job-controller ./cmd/controller

# Stage 2: Runtime
FROM gcr.io/distroless/static:nonroot

COPY --from=builder /workspace/bin/job-controller /job-controller
COPY --from=builder /workspace/templates/workload-claim-template.tmpl.yaml /templates/workload-claim-template.tmpl.yaml

USER 65534:65534

ENTRYPOINT ["/job-controller"]
