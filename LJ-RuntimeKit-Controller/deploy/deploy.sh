#!/bin/bash
# Deploy script for AMP-Controller
set -e

# Default values
NAMESPACE=${NAMESPACE:-"lj-runtimekit-system"}
CONTROLLER_IMG=${CONTROLLER_IMG:-"lj-runtimekit-controller:latest"}
JOB_AGENT_IMG=${JOB_AGENT_IMG:-"lj-runtimekit-controller-job-agent:latest"}

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

usage() {
    echo "Usage: $0 [OPTIONS] COMMAND"
    echo ""
    echo "Commands:"
    echo "  deploy       Deploy the controller to the cluster"
    echo "  undeploy     Remove the controller from the cluster"
    echo "  status       Show the status of the controller"
    echo "  logs         Show the controller logs"
    echo ""
    echo "Options:"
    echo "  --namespace NS            Target namespace (default: lj-runtimekit-system)"
    echo "  --controller-img IMAGE    Controller docker image"
    echo "  --job-agent-img IMAGE     Job agent docker image"
    echo "  -h, --help                Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 deploy"
    echo "  $0 --namespace my-ns deploy"
    echo "  $0 undeploy"
    echo "  $0 logs"
}

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Parse arguments
COMMAND=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --namespace)
            NAMESPACE="$2"
            shift 2
            ;;
        --controller-img)
            CONTROLLER_IMG="$2"
            shift 2
            ;;
        --job-agent-img)
            JOB_AGENT_IMG="$2"
            shift 2
            ;;
        deploy|undeploy|status|logs)
            COMMAND="$1"
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Check dependencies
command -v kubectl >/dev/null 2>&1 || { log_error "kubectl is required but not installed."; exit 1; }

# Validate command
if [ -z "$COMMAND" ]; then
    log_error "Command is required"
    usage
    exit 1
fi

cd "$PROJECT_ROOT"

case $COMMAND in
    deploy)
        log_info "Deploying AMP-Controller to namespace: $NAMESPACE"
        log_info "Controller image: $CONTROLLER_IMG"
        log_info "Job agent image: $JOB_AGENT_IMG"

        # Create a temporary copy of config/ so we don't mutate the source files
        TMP_DIR=$(mktemp -d "$PROJECT_ROOT/.deploy-XXXXXX")
        cp -r "$PROJECT_ROOT/config" "$TMP_DIR/config"

        # Update image tags in the temporary manager.yaml
        # Replace job-agent first to avoid partial matches on controller name
        sed -E \
            -e "s|image: [^ ]*lj-runtimekit-controller-job-agent(:[^ ]*)?|image: $JOB_AGENT_IMG|g" \
            -e "s|image: [^ ]*lj-runtimekit-controller(:[^ ]*)?|image: $CONTROLLER_IMG|g" \
            -e "s|--job-image=[^ ]*lj-runtimekit-controller-job-agent(:[^ ]*)?|--job-image=$JOB_AGENT_IMG|g" \
            "$TMP_DIR/config/manager/manager.yaml" > "$TMP_DIR/config/manager/manager.yaml.tmp"
        mv "$TMP_DIR/config/manager/manager.yaml.tmp" "$TMP_DIR/config/manager/manager.yaml"

        # Update namespace in the temporary kustomization
        sed -E "s|^namespace: .*|namespace: $NAMESPACE|g" \
            "$TMP_DIR/config/default/kustomization.yaml" > "$TMP_DIR/config/default/kustomization.yaml.tmp"
        mv "$TMP_DIR/config/default/kustomization.yaml.tmp" "$TMP_DIR/config/default/kustomization.yaml"

        # Apply the configuration
        log_info "Applying Kubernetes manifests..."
        kubectl apply -k "$TMP_DIR/config/default"

        # Cleanup temporary directory
        rm -rf "$TMP_DIR"

        log_info "Waiting for deployment to be ready..."
        kubectl rollout status deployment/lj-runtimekit-controller-manager -n "$NAMESPACE" --timeout=120s

        log_info "Deployment complete!"
        log_info "Check status with: $0 status"
        ;;

    undeploy)
        log_info "Undeploying AMP-Controller from namespace: $NAMESPACE"

        log_warn "This will delete all AMP-Controller resources including running jobs."
        read -p "Are you sure? (y/N) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            kubectl delete -k config/default
            log_info "Undeployment complete!"
        else
            log_info "Undeployment cancelled."
        fi
        ;;

    status)
        log_info "AMP-Controller status in namespace: $NAMESPACE"
        echo ""
        echo "=== Deployment ==="
        kubectl get deployment -n "$NAMESPACE" -l app.kubernetes.io/name=lj-runtimekit-controller
        echo ""
        echo "=== Pods ==="
        kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/name=lj-runtimekit-controller
        echo ""
        echo "=== Jobs (recent) ==="
        kubectl get jobs -n "$NAMESPACE" -l app.kubernetes.io/managed-by=lj-runtimekit-controller 2>/dev/null || echo "No jobs found"
        ;;

    logs)
        log_info "Showing controller logs..."
        kubectl logs -n "$NAMESPACE" -l control-plane=controller-manager --tail=100 -f
        ;;

    *)
        log_error "Unknown command: $COMMAND"
        usage
        exit 1
        ;;
esac
