# Release

This document describes the release process and versioning strategy for LJ RuntimeKit Exporter.

## Versioning

LJ RuntimeKit Exporter uses a single semantic version for source releases, binaries, Docker images, and Helm deployments.

- Version format: `vMAJOR.MINOR.PATCH`, for example `v1.0.0`.
- The default version is defined by `VERSION` in `Makefile`.
- The Docker image tag is the same as `VERSION`.
- The Helm chart `appVersion` should match the released application version without the leading `v`, for example `1.0.0` for `v1.0.0`.
- The Helm chart `version` should be updated when the chart package itself changes.

The project does not maintain multiple long-lived release branches. New versions are released on demand.

## Release Artifacts

A release may include the following artifacts:

- `exporter` binary, built from `cmd/exporter`.
- Docker image, published to `registry.cn-hangzhou.aliyuncs.com/eflops-amp/lj-runtimekit-exporter:<VERSION>` by default.
- Helm chart under `deployment/`.
- Git tag matching the release version.

## Prerequisites

Before releasing, ensure the environment has:

- Go installed and compatible with the project dependencies.
- Docker with BuildKit enabled.
- Helm 3.x, if packaging or validating the chart.
- Access to the target Docker registry.
- A clean Git working tree.

## Prepare a Release

Choose the release version and export it for the current shell:

```bash
export VERSION=v1.0.0
```

Update version references before building:

- `Makefile`: update `VERSION` if the default version should change.
- `deployment/Chart.yaml`: update `version` and `appVersion` when releasing a chart.
- `deployment/values.yaml`: update `image.tag` to the release image tag.

## Validate

Run unit tests before publishing:

```bash
make test
```

Optional coverage check:

```bash
make test-coverage
```

If Helm is available, validate the chart:

```bash
helm lint ./deployment
```

## Build

Build the local binary:

```bash
make build VERSION=${VERSION}
```

The binary is generated at:

```text
cmd/exporter/exporter
```

Build the Docker image:

```bash
make docker VERSION=${VERSION}
```

To publish to a different registry, override `REGISTRY`:

```bash
make docker VERSION=${VERSION} REGISTRY=<registry>/<namespace>/<image>
```

## Publish Docker Image

Push the image after a successful build:

```bash
docker push registry.cn-hangzhou.aliyuncs.com/eflops-amp/lj-runtimekit-exporter:${VERSION}
```

If a custom registry was used during build, push the matching image tag instead.

## Package Helm Chart

Package the Helm chart when a chart artifact is required:

```bash
helm package ./deployment \
  --version <chart-version> \
  --app-version ${VERSION#v}
```

Example for `v1.0.0`:

```bash
helm package ./deployment --version 1.0.0 --app-version 1.0.0
```

## Deploy Verification

Install or upgrade the release in a Kubernetes cluster:

```bash
helm upgrade --install lj-runtimekit-exporter ./deployment \
  -n <namespace> \
  -f ./deployment/values-exporter.yaml \
  --set image.tag=${VERSION}
```

Verify the DaemonSet and pods are running:

```bash
kubectl get daemonset,pod -n <namespace> -l app.kubernetes.io/name=lj-runtimekit-exporter
```

Uninstall if rollback or cleanup is needed:

```bash
helm uninstall lj-runtimekit-exporter -n <namespace>
```

## Git Tag

Create and push a Git tag after the release artifacts are verified:

```bash
git tag -a ${VERSION} -m "Release ${VERSION}"
git push origin ${VERSION}
```

## Release Checklist

- [ ] Version selected, for example `v1.0.0`.
- [ ] `Makefile`, `deployment/Chart.yaml`, and `deployment/values.yaml` version fields are consistent.
- [ ] `make test` passes.
- [ ] `helm lint ./deployment` passes, if Helm is available.
- [ ] `make build VERSION=${VERSION}` succeeds.
- [ ] `make docker VERSION=${VERSION}` succeeds.
- [ ] Docker image is pushed to the target registry.
- [ ] Helm chart is packaged or deployment manifests are verified.
- [ ] Git tag is created and pushed.
