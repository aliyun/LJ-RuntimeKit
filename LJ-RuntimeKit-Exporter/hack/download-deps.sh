#!/bin/bash
#
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

# download-deps.sh — 一键切换 Exporter 项目的 K8s staging 依赖版本
#
# 工作原理：
#   1. 从 GitHub 拉取目标 K8s 版本的 go.mod
#   2. 解析出所有 staging replace 行（k8s.io/* => ./staging/src/k8s.io/*）
#   3. 仅对本地 go.mod 中已存在的 k8s.io/* 模块执行版本替换
#   4. 通过 go mod download -json 获取精确版本号，写入 go mod edit -replace

VERSION=${1#"v"}
if [ -z "$VERSION" ]; then
  echo "Usage: ./hack/download-deps.sh <k8s-version>"
  echo "Example:"
  echo "  ./hack/download-deps.sh v1.21.0"
  echo "  ./hack/download-deps.sh v1.27.3"
  exit 1
fi

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

echo "==> Fetching K8s v${VERSION} go.mod from GitHub ..."

# Find out all the replaced imports (staging modules) from upstream K8s go.mod.
MODS=($(
  curl -sS "https://raw.githubusercontent.com/kubernetes/kubernetes/v${VERSION}/go.mod" |
    sed -n 's|.*k8s.io/\(.*\) => ./staging/src/k8s.io/.*|k8s.io/\1|p'
))

if [ ${#MODS[@]} -eq 0 ]; then
  echo "ERROR: Failed to parse staging modules from K8s v${VERSION} go.mod"
  echo "Please check the version number and network connectivity."
  exit 1
fi

echo "==> Found ${#MODS[@]} staging modules in upstream K8s go.mod"

# Only apply replace for modules that are already referenced in the local go.mod.
LOCAL_GOMOD="${REPO_ROOT}/go.mod"
REPLACED=0

for MOD in "${MODS[@]}"; do
  if ! grep -q "${MOD}" "${LOCAL_GOMOD}"; then
    continue
  fi

  echo "    Resolving ${MOD} ..."
  V=$(
    go mod download -json "${MOD}@kubernetes-${VERSION}" |
      sed -n 's|.*"Version": "\(.*\)".*|\1|p'
  )

  if [ -z "$V" ]; then
    echo "    WARNING: could not resolve version for ${MOD}, skipping"
    continue
  fi

  go mod edit "-replace=${MOD}=${MOD}@${V}"
  echo "    ${MOD} => ${V}"
  REPLACED=$((REPLACED + 1))
done

echo "==> Applied ${REPLACED} replace directive(s)"
echo "==> Running go mod download ..."
go mod download

echo ""
echo "Done! Next steps:"
echo "  1. Run './hack/update-vendor.sh' to update vendor directory"
echo "  2. Verify the build: go build ./..."
