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

VERSION=${1#"v"}
if [ -z "$VERSION" ]; then
  echo "Please specify the Kubernetes version: e.g."
  echo "./download-deps.sh v1.21.0"
  exit 1
fi

set -euo pipefail

# Find out all the replaced imports, make a list of them.
MODS=($(
  curl -sS "https://raw.githubusercontent.com/kubernetes/kubernetes/v${VERSION}/go.mod" |
    sed -n 's|.*k8s.io/\(.*\) => ./staging/src/k8s.io/.*|k8s.io/\1|p'
))

# Now add those similar replace statements in the local go.mod file, but first find the version that
# the Kubernetes is using for them.
for MOD in "${MODS[@]}"; do
  V=$(
    go mod download -json "${MOD}@kubernetes-${VERSION}" |
      sed -n 's|.*"Version": "\(.*\)".*|\1|p'
  )

  go mod edit "-replace=${MOD}=${MOD}@${V}"
done

# Device-Plugin does not depend on the k8s.io/kubernetes main module, so there is no
# `go get k8s.io/kubernetes@...` step here (unlike some upstream projects). Only the
# staging modules that Device-Plugin actually imports are pinned via the replace list above.
go mod download
