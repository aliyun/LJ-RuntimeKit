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
#
#
# deploy.sh - One-click deploy/uninstall script for Migration-Controller
#
# Usage:
#   ./deploy.sh deploy                      Deploy Migration-Controller only
#   ./deploy.sh deploy --with-dragonfly     Deploy Migration-Controller + Dragonfly
#   ./deploy.sh uninstall                   Uninstall Migration-Controller only
#   ./deploy.sh uninstall --with-dragonfly  Uninstall Migration-Controller + Dragonfly
#
# The target namespace and the controller/worker images can be overridden via
# environment variables or flags (flags take precedence); defaults match the
# manifests under deploy/:
#   NAMESPACE       (default kube-system)
#   CONTROLLER_IMG  (default lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-migration-controller:v1.14.0-k8s1.34)
#   WORKER_IMG      (default lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-migration-worker:v1.14.0-k8s1.34)
#

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEPLOY_DIR="${SCRIPT_DIR}/deploy"
DRAGONFLY_MANIFESTS_DIR="${DEPLOY_DIR}/dragonfly"
HELM_DIR="${SCRIPT_DIR}/helm"
DRAGONFLY_NAMESPACE="dragonfly"
DRAGONFLY_RELEASE="dragonfly"
DRAGONFLY_VALUES="${HELM_DIR}/dragonfly-values.yaml"
DRAGONFLY_CHART="${HELM_DIR}/dragonfly-1.3.11.tgz"
# The controller reads this Secret (dragonfly-preheat-pat) at startup to log in
# to the Manager and request a PAT; if --enable-preheat-job is turned on without
# it, preheat fails outright. Therefore it is deployed together with Dragonfly.
DRAGONFLY_PREHEAT_SECRET="${DEPLOY_DIR}/dragonfly-preheat-secret.yaml"

# Deployment parameters (environment variables set the defaults, flags override).
NAMESPACE=${NAMESPACE:-"kube-system"}
CONTROLLER_IMG=${CONTROLLER_IMG:-"lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-migration-controller:v1.14.0-k8s1.34"}
WORKER_IMG=${WORKER_IMG:-"lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-migration-worker:v1.14.0-k8s1.34"}

# Pre-flight checks
command -v kubectl >/dev/null 2>&1 || { echo "[ERROR] kubectl not found in PATH"; exit 1; }

usage() {
    echo "Usage: $0 [OPTIONS] COMMAND"
    echo ""
    echo "Commands:"
    echo "  deploy                      Deploy Migration-Controller (optionally with Dragonfly)"
    echo "  uninstall                   Uninstall Migration-Controller (optionally with Dragonfly)"
    echo ""
    echo "Options:"
    echo "  --namespace NS              Target namespace (default: kube-system)"
    echo "  --controller-img IMAGE      Controller image (default: lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-migration-controller:v1.14.0-k8s1.34)"
    echo "  --worker-img IMAGE          Worker image (default: lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-migration-worker:v1.14.0-k8s1.34)"
    echo "  --with-dragonfly            Deploy/uninstall the Dragonfly P2P system as well"
    echo "  -h, --help                  Show this help message"
    echo ""
    echo "The same values can be supplied via the NAMESPACE / CONTROLLER_IMG / WORKER_IMG"
    echo "environment variables; flags take precedence."
    echo ""
    echo "Examples:"
    echo "  $0 deploy                                          # Controller only"
    echo "  $0 deploy --with-dragonfly                         # Controller + Dragonfly"
    echo "  $0 --namespace my-ns deploy"
    echo "  $0 --controller-img registry.example.com/controller:v1.1.0 deploy"
    echo "  $0 uninstall --with-dragonfly"
}

# render_manifests prints the kustomize output of deploy/ with the configured
# namespace and images substituted. The manifests in the repo keep their
# default values; overrides are applied at render time only.
render_manifests() {
    kubectl kustomize "${DEPLOY_DIR}" | sed \
        -e "s|namespace: kube-system|namespace: ${NAMESPACE}|g" \
        -e "s|image: lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-migration-controller:v1.14.0-k8s1.34|image: ${CONTROLLER_IMG}|" \
        -e "s|image: lj-runtimekit-registry.cn-hangzhou.cr.aliyuncs.com/lj-runtimekit/lj-runtimekit-migration-worker:v1.14.0-k8s1.34|image: ${WORKER_IMG}|"
}

# ==================== Migration-Controller deploy ====================
deploy_controller() {
    echo ">>> Deploying Migration-Controller ..."
    # Remove a legacy bare pod from the pre-Deployment era: a pod of the same
    # name would otherwise keep running alongside the Deployment (a bare pod
    # and a Deployment may share a name) and double-write cluster state.
    kubectl delete pod -n "${NAMESPACE}" lj-runtimekit-migration-controller --ignore-not-found --wait=true --timeout=60s || true
    render_manifests | kubectl apply -f -
    echo ">>> Migration-Controller deployed"
}

# ==================== Migration-Controller uninstall ====================
# CRD deletion requires all CR instances to be gone first; CRs carry the
# migration.io/cleanup finalizer which is removed by the controller. If the
# controller is deleted first and CRs remain, CRD deletion hangs forever.
# Therefore the uninstall order must be: stop the controller -> strip finalizers
# and delete CRs -> clean workers -> delete the remaining resources -> delete
# the CRDs last.
uninstall_controller() {
    echo ">>> Uninstalling Migration-Controller ..."

    echo "[uninstall] Step 1: stop the controller so no workers keep being created ..."
    kubectl delete deployment -n "${NAMESPACE}" lj-runtimekit-migration-controller --ignore-not-found --wait=true --timeout=60s || true
    # Also remove a legacy bare pod (pre-Deployment era) that may still exist
    # under the same name.
    kubectl delete pod -n "${NAMESPACE}" lj-runtimekit-migration-controller --ignore-not-found --wait=true --timeout=60s || true

    echo "[uninstall] Step 2: clean up leftover CRs (strip finalizers first, otherwise CRD deletion hangs) ..."
    for crd in containerstatemigrationjobrequests.migration.io preheatjobs.migration.io; do
        kubectl get crd "$crd" >/dev/null 2>&1 || continue
        kind=$(kubectl get crd "$crd" -o jsonpath='{.spec.names.kind}')
        # Strip finalizers from all instances (the controller is stopped, nobody will process them)
        kubectl get "$kind" -A -o jsonpath='{range .items[*]}{.metadata.namespace}{" "}{.metadata.name}{"\n"}{end}' 2>/dev/null | \
        while read -r ns name; do
            [ -z "$ns" ] && continue
            kubectl patch "$kind" "$name" -n "$ns" --type=merge -p '{"metadata":{"finalizers":[]}}' >/dev/null 2>&1 || true
            kubectl delete "$kind" "$name" -n "$ns" --ignore-not-found --wait=false >/dev/null 2>&1 || true
        done
    done

    echo "[uninstall] Step 3: clean up migration-worker pods created by the controller ..."
    kubectl delete pods -A -l component=lj-runtimekit-migration-worker --ignore-not-found --wait=false || true
    # Fallback: also scan by the lj-runtimekit-migration-worker-<node> naming
    # convention; the container-migration-era pods keep the legacy
    # migration-worker- prefix and are swept too.
    kubectl get pods -n "${NAMESPACE}" --no-headers 2>/dev/null | awk '/^(lj-runtimekit-)?migration-worker-/{print $1}' | \
    while read -r p; do
        kubectl delete pod -n "${NAMESPACE}" "$p" --ignore-not-found --wait=false || true
    done

    echo "[uninstall] Step 4: delete the remaining resources (RBAC / ConfigMap etc.) ..."
    render_manifests | kubectl delete -f - --ignore-not-found

    echo "[uninstall] Step 5: verify the CRDs are gone ..."
    for crd in containerstatemigrationjobrequests.migration.io preheatjobs.migration.io; do
        if kubectl get crd "$crd" >/dev/null 2>&1; then
            echo "[WARN] CRD $crd still present (a CR with a finalizer may remain); please inspect manually"
        fi
    done

    echo ">>> Migration-Controller uninstalled"
}

# ==================== Dragonfly deploy ====================
deploy_dragonfly() {
    echo ""
    echo "=========================================="
    echo ">>> Deploying the Dragonfly P2P system ..."
    echo "  Values: $DRAGONFLY_VALUES"
    echo "=========================================="

    command -v helm >/dev/null 2>&1 || { echo "[ERROR] helm not found in PATH"; exit 1; }
    [[ -f "$DRAGONFLY_CHART" ]] || { echo "[ERROR] Helm chart not found: $DRAGONFLY_CHART"; exit 1; }
    [[ -f "$DRAGONFLY_VALUES" ]] || { echo "[ERROR] Values file not found: $DRAGONFLY_VALUES"; exit 1; }

    echo ""
    echo "[Dragonfly] Step 1: create the namespace ..."
    kubectl apply -f "$DRAGONFLY_MANIFESTS_DIR/namespace.yaml"

    echo "[Dragonfly] Step 2: deploy MySQL ..."
    kubectl apply -f "$DRAGONFLY_MANIFESTS_DIR/mysql-ext-deployment.yaml"
    kubectl apply -f "$DRAGONFLY_MANIFESTS_DIR/mysql-ext-service.yaml"

    echo "[Dragonfly] Step 3: deploy Redis ..."
    kubectl apply -f "$DRAGONFLY_MANIFESTS_DIR/redis-ext-deployment.yaml"
    kubectl apply -f "$DRAGONFLY_MANIFESTS_DIR/redis-ext-service.yaml"

    echo "[Dragonfly] Step 4: wait for MySQL and Redis to become ready ..."
    kubectl rollout status deployment/dragonfly-mysql-ext -n $DRAGONFLY_NAMESPACE --timeout=120s
    kubectl rollout status deployment/dragonfly-redis-ext -n $DRAGONFLY_NAMESPACE --timeout=120s

    echo "[Dragonfly] Step 5: install Dragonfly via Helm ..."
    helm upgrade --install $DRAGONFLY_RELEASE "$DRAGONFLY_CHART" \
        --namespace $DRAGONFLY_NAMESPACE \
        --values "$DRAGONFLY_VALUES" \
        --wait --timeout 300s

    echo "[Dragonfly] Step 6: deploy the preheat PAT Secret ..."
    kubectl apply -f "$DRAGONFLY_PREHEAT_SECRET"

    echo ""
    echo "[Dragonfly] Step 7: verify ..."
    kubectl get pods -n $DRAGONFLY_NAMESPACE
    echo ""
    echo ">>> Dragonfly deployed"
}

# ==================== Dragonfly uninstall ====================
uninstall_dragonfly() {
    echo ""
    echo "=========================================="
    echo ">>> Uninstalling the Dragonfly P2P system ..."
    echo "=========================================="

    command -v helm >/dev/null 2>&1 || { echo "[ERROR] helm not found in PATH"; exit 1; }

    echo "[Dragonfly] Step 1: uninstall the Helm release ..."
    helm uninstall $DRAGONFLY_RELEASE --namespace $DRAGONFLY_NAMESPACE 2>/dev/null && \
        echo "  Helm release '$DRAGONFLY_RELEASE' uninstalled" || \
        echo "  Helm release '$DRAGONFLY_RELEASE' not found, skipping"

    echo "[Dragonfly] Step 2: delete the preheat PAT Secret ..."
    kubectl delete -f "$DRAGONFLY_PREHEAT_SECRET" --ignore-not-found

    echo "[Dragonfly] Step 3: delete MySQL ..."
    kubectl delete -f "$DRAGONFLY_MANIFESTS_DIR/mysql-ext-deployment.yaml" --ignore-not-found
    kubectl delete -f "$DRAGONFLY_MANIFESTS_DIR/mysql-ext-service.yaml" --ignore-not-found

    echo "[Dragonfly] Step 4: delete Redis ..."
    kubectl delete -f "$DRAGONFLY_MANIFESTS_DIR/redis-ext-deployment.yaml" --ignore-not-found
    kubectl delete -f "$DRAGONFLY_MANIFESTS_DIR/redis-ext-service.yaml" --ignore-not-found

    echo "[Dragonfly] Step 5: delete the ConfigMaps ..."
    kubectl delete -f "$DRAGONFLY_MANIFESTS_DIR/configmap-manager.yaml" --ignore-not-found
    kubectl delete -f "$DRAGONFLY_MANIFESTS_DIR/configmap-scheduler.yaml" --ignore-not-found
    kubectl delete -f "$DRAGONFLY_MANIFESTS_DIR/configmap-client.yaml" --ignore-not-found
    kubectl delete -f "$DRAGONFLY_MANIFESTS_DIR/configmap-seed-client.yaml" --ignore-not-found

    echo "[Dragonfly] Step 6: delete the namespace ..."
    kubectl delete namespace $DRAGONFLY_NAMESPACE --ignore-not-found --wait=true --timeout=60s

    echo ""
    echo ">>> Dragonfly uninstalled"
}

# ==================== Argument parsing ====================
ACTION=""
WITH_DRAGONFLY=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        deploy|uninstall)
            ACTION="$1"
            shift
            ;;
        --with-dragonfly)
            WITH_DRAGONFLY=true
            shift
            ;;
        --namespace)
            NAMESPACE="$2"
            shift 2
            ;;
        --controller-img)
            CONTROLLER_IMG="$2"
            shift 2
            ;;
        --worker-img)
            WORKER_IMG="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "ERROR: unknown argument '$1'"
            echo ""
            usage
            exit 1
            ;;
    esac
done

if [ -z "$ACTION" ]; then
    usage
    exit 1
fi

# ==================== Main ====================
case "$ACTION" in
    deploy)
        deploy_controller
        if [ "$WITH_DRAGONFLY" = "true" ]; then
            deploy_dragonfly
        fi
        echo ""
        echo "=========================================="
        echo "  Deployment completed"
        echo "=========================================="
        ;;
    uninstall)
        uninstall_controller
        if [ "$WITH_DRAGONFLY" = "true" ]; then
            uninstall_dragonfly
        fi
        echo ""
        echo "=========================================="
        echo "  Uninstall completed"
        echo "=========================================="
        ;;
esac
