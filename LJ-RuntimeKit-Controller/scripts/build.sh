#!/bin/bash
# Build script for LJ-RuntimeKit-Controller
set -e

# Default values
CONTROLLER_IMG=${CONTROLLER_IMG:-"lj-runtimekit-controller:latest"}
JOB_AGENT_IMG=${JOB_AGENT_IMG:-"lj-runtimekit-controller-job-agent:latest"}
BUILDER_IMAGE=${BUILDER_IMAGE:-"public.ecr.aws/docker/library/golang:1.22.7"}
BASE_IMAGE=${BASE_IMAGE:-"public.ecr.aws/docker/library/alpine:3.19"}
PLATFORM=${PLATFORM:-"linux/amd64"}
PUSH=${PUSH:-"false"}
NO_BUILDKIT=${NO_BUILDKIT:-"false"}

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
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --controller-img IMAGE    Controller docker image (default: lj-runtimekit-controller:latest)"
    echo "  --job-agent-img IMAGE     Job agent docker image (default: lj-runtimekit-controller-job-agent:latest)"
    echo "  --builder-image IMAGE     Builder image (default: public.ecr.aws/docker/library/golang:1.22.7)"
    echo "  --base-image IMAGE        Base image (default: public.ecr.aws/docker/library/alpine:3.19)"
    echo "  --platform PLATFORM       Target platform (default: linux/amd64)"
    echo "  --push                    Push images after building"
    echo "  --no-buildkit             Disable Docker BuildKit (legacy builder)"
    echo "  --binary-only             Build only binaries, no docker images"
    echo "  -h, --help                Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                                  # Build all images"
    echo "  $0 --binary-only                    # Build only binaries"
    echo "  $0 --push                           # Build and push images"
    echo "  $0 --builder-image mirror/golang:1.25.5 --base-image mirror/alpine:3.19  # Use mirror"
    echo "  $0 --no-buildkit                    # Use legacy docker builder"
    echo "  CONTROLLER_IMG=myregistry/lj-runtimekit-controller:latest $0  # Use custom image"
}

# Parse arguments
BINARY_ONLY=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --controller-img)
            CONTROLLER_IMG="$2"
            shift 2
            ;;
        --job-agent-img)
            JOB_AGENT_IMG="$2"
            shift 2
            ;;
        --builder-image)
            BUILDER_IMAGE="$2"
            shift 2
            ;;
        --base-image)
            BASE_IMAGE="$2"
            shift 2
            ;;
        --platform)
            PLATFORM="$2"
            shift 2
            ;;
        --push)
            PUSH="true"
            shift
            ;;
        --no-buildkit)
            NO_BUILDKIT="true"
            shift
            ;;
        --binary-only)
            BINARY_ONLY=true
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
command -v go >/dev/null 2>&1 || { log_error "go is required but not installed."; exit 1; }

if [ "$BINARY_ONLY" = false ]; then
    command -v docker >/dev/null 2>&1 || { log_error "docker is required but not installed."; exit 1; }
fi

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

log_info "Building AMP-Controller..."
log_info "Project root: $PROJECT_ROOT"
log_info "Controller image: $CONTROLLER_IMG"
log_info "Job agent image: $JOB_AGENT_IMG"
log_info "Builder image: $BUILDER_IMAGE"
log_info "Base image: $BASE_IMAGE"
log_info "Platform: $PLATFORM"

# Download dependencies
log_info "Downloading Go dependencies..."
go mod download

# Vendor dependencies for offline Docker build
log_info "Vendoring Go dependencies for offline Docker build..."
go mod vendor

# Build binaries
log_info "Building controller binary..."
go build -o bin/manager cmd/controller/main.go
log_info "Controller binary built: bin/manager"

log_info "Building job-agent binary..."
go build -o bin/job-agent cmd/job-agent/main.go
log_info "Job-agent binary built: bin/job-agent"

log_info "Building kubectl-amp binary..."
go build -o bin/kubectl-amp cmd/kubectl-amp/main.go
log_info "Kubectl-amp binary built: bin/kubectl-amp"

if [ "$BINARY_ONLY" = true ]; then
    log_info "Binary-only build complete."
    exit 0
fi

# Build Docker images
log_info "Building Docker image: $CONTROLLER_IMG"
if [ "$NO_BUILDKIT" = "true" ]; then
    export DOCKER_BUILDKIT=0
    log_info "Using legacy Docker builder (BuildKit disabled)"
fi

docker build -t "$CONTROLLER_IMG" -f Dockerfile \
  --build-arg BUILDER_IMAGE="$BUILDER_IMAGE" \
  --build-arg BASE_IMAGE="$BASE_IMAGE" \
  --platform "$PLATFORM" .

log_info "Building Docker image: $JOB_AGENT_IMG"
docker build -t "$JOB_AGENT_IMG" -f Dockerfile.job-agent \
  --build-arg BUILDER_IMAGE="$BUILDER_IMAGE" \
  --build-arg BASE_IMAGE="$BASE_IMAGE" \
  --platform "$PLATFORM" .

# Push images if requested
if [ "$PUSH" = "true" ]; then
    log_info "Pushing Docker image: $CONTROLLER_IMG"
    docker push "$CONTROLLER_IMG"

    log_info "Pushing Docker image: $JOB_AGENT_IMG"
    docker push "$JOB_AGENT_IMG"
fi

log_info "Build complete!"
log_info "Controller image: $CONTROLLER_IMG"
log_info "Job agent image: $JOB_AGENT_IMG"
