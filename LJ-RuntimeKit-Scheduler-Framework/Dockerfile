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

FROM golang:1.21-bullseye as build

ARG GOPROXY=https://mirrors.aliyun.com/goproxy/,direct

WORKDIR /go/src/Scheduler

COPY . .

# The repo does not ship a vendor/ directory; materialise it from the module
# proxy so the build below is reproducible and offline from here on.
RUN GOPROXY=${GOPROXY} go mod vendor

RUN CGO_ENABLED=0 go build -mod=vendor -o /go/bin/kube-scheduler ./cmd/scheduler/main.go

FROM debian:bullseye-slim

COPY --from=build /go/bin/kube-scheduler /bin/kube-scheduler

CMD ["/bin/kube-scheduler"]