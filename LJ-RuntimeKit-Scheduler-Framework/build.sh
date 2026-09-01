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

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REGISTRY_NAME=${1:-${REGISTRY_NAME:-"registry.cn-hangzhou.aliyuncs.com/eflops-amp"}}
AMP_PACK_VERSION=${2:-${AMP_PACK_VERSION:-"v1.0.0"}}
IMAGE_NAME="lj-runtimekit-scheduler-framework"
SOURCE_HELM_DIR="${SCRIPT_DIR}/helm"
PACK_HELM_DIR="${SCRIPT_DIR}/PACK/helm"
PACK_VALUES_FILE="${PACK_HELM_DIR}/values.yaml"

build()
{
    local REGISTRY_NAME="$1"
    local AMP_PACK_VERSION="$2"
    local values_tmp
    local old_path

    cd "$SCRIPT_DIR"

    rm -rf ./PACK
    mkdir -p ./PACK

    sudo docker build -t "${REGISTRY_NAME}/${IMAGE_NAME}:${AMP_PACK_VERSION}" -f Dockerfile .
    # sudo docker push "${REGISTRY_NAME}/${IMAGE_NAME}:${AMP_PACK_VERSION}"
    sudo docker save -o "./PACK/lj-runtimekit-scheduler-framework-${AMP_PACK_VERSION}.tar" "${REGISTRY_NAME}/${IMAGE_NAME}:${AMP_PACK_VERSION}"
    sudo chmod 666 "./PACK/lj-runtimekit-scheduler-framework-${AMP_PACK_VERSION}.tar"

    cp -r "$SOURCE_HELM_DIR" "$PACK_HELM_DIR"

    values_tmp="$(mktemp "${SCRIPT_DIR}/values.yaml.tmp.XXXXXX")"
    awk -v image="${REGISTRY_NAME}/${IMAGE_NAME}" -v tag="${AMP_PACK_VERSION}" '
        /^[[:space:]]*image:[[:space:]]*/ {
            match($0, /^[[:space:]]*/)
            indent = substr($0, 1, RLENGTH)
            print indent "image: \"" image "\""
            next
        }
        /^[[:space:]]*tag:[[:space:]]*/ {
            match($0, /^[[:space:]]*/)
            indent = substr($0, 1, RLENGTH)
            print indent "tag: " tag
            next
        }
        { print }
    ' "$PACK_VALUES_FILE" > "$values_tmp"
    mv "$values_tmp" "$PACK_VALUES_FILE"

    old_path=$(pwd)
    cd ./PACK
    tar czvf "lj-runtimekit-scheduler-framework-helm-${AMP_PACK_VERSION}.tar.gz" ./helm
    cd "$old_path"

    rm -rf "$PACK_HELM_DIR"
}

build "$REGISTRY_NAME" "$AMP_PACK_VERSION"