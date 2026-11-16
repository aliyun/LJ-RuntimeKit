# LJ RuntimeKit Device Plugin

Welcome to the LJ RuntimeKit Device Plugin! This is an open-source device plugin
that aims to provide a unified interface for managing and integrating a variety of
hardware resources with Kubernetes clusters. If you're looking to utilize special
hardware like GPU、DCU、PPU、AMD, or any other accelerators in your Kubernetes pods,
this device plugin is designed to help you do just that with ease.

---
## Features
#### Seamless Hardware Integration
Automatically recognizes and manages your cluster's hardware resources. 
#### Broad Hardware Support
Compatible with a wide range of hardware devices.
#### Resource Allocation
Integrates with the Kubernetes Scheduler to ensure resources are allocated properly to pods.
The currently supported resource types are as follows：
- aliyun.com/gpu-memory
- aliyun.com/gpu-core
- aliyun.com/gpu-slice

#### Custom Resource Definitions
Allows defining custom resource names for better resource management.
The currently supported resource types are as follows：
- nvidia.com/gpu
- aliyun.com/ppu
- aliyun.com/amd

#### Health Monitoring
Implements health checks to monitor and report the status of hardware devices.

---

## Prerequisites
Before you start using the LJ RuntimeKit Device Plugin, make sure that you have:
- A Kubernetes cluster running version 1.18 or higher.
- Go 1.17 or later installed on your system if you intend to build from source.

## Quick Start
Follow these steps to quickly deploy the LJ RuntimeKit Device Plugin to your cluster:

## Building from Source
To build the LJ RuntimeKit Device Plugin from source, clone the code and create an image with:

```
git clone https://github.com/aliyun/LJ-RuntimeKit.git
cd LJ-RuntimeKit/LJ-RuntimeKit-Device-Plugin
docker build -t <imgae_name> -f Dockerfile . 
```
Alternatively, you can Makefile to build the image.
```
make docker   # build new image
```
### Installation
After completing the creation of the image， deploy the device plugin using the following command:
```
kubectl apply -f yaml/lj-runtimekit-device-plugin-rbac.yaml
kubectl apply -f yaml/lj-runtimekit-device-plugin-ds.yaml
```
Execute the following command when uninstalling is required.
```
kubectl delete -f yaml/lj-runtimekit-device-plugin-rbac.yaml
kubectl delete -f yaml/lj-runtimekit-device-plugin-ds.yaml
```
You can also use Makefile to complete the build and deployment of LJ RuntimeKit Device Plugin:
```
make deploy # apply yaml configuration
```

### Verification
To verify that the device plugin is running correctly, use this command:
```
kubectl get pods -n <namespace> | grep lj-runtimekit-device-plugin
```

## Configuration
You can modify the image name and other configurations in the lj-runtimekit-device-plugin-ds.yaml file before you deploy it.

## Troubleshooting
If you face any issues, you can check the logs of the device plugin pod for diagnostics:
```
kubectl logs <pod-name> -n <namespace>
kubectl describe pods <pod-name> -n <namespace>
```

## Contributing
Contributions are what make the open-source community such an amazing place to learn, inspire, and create.
Any contributions you make are greatly appreciated.

Check out the [CONTRIBUTING.md](CONTRIBUTING.md) file for guidelines on how to contribute.

## Support
If you have any questions or encounter any issues, please don't hesitate to create an issue in the
issue tracker.

## License
The LJ RuntimeKit Device Plugin is open-sourced under the [Apache License 2.0](LICENSE).
## Acknowledgments
Thanks to the Kubernetes community for their guidance and support.

All contributors who have helped to make this device plugin better.

Your feedback and contributions are welcome! Thank you for supporting and using the LJ RuntimeKit
Device Plugin.