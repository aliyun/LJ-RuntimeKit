# LJ RuntimeKit Exporter

LJ RuntimeKit Exporter is a software for online collection of heterogeneous hardware-related information/metrics
deployed on a single server node, specifically designed for data center scenarios. Within the
enterprise, AMPerf is considered to be the solution that offers the richest GPU performance metrics,
the most comprehensive data collection dimensions, and the most accurate and rapid online GPU
health diagnostics.

## Features

- The provided full-card metrics include indicators of dimensions such as GPU operating status, utilization,
  performance, and basic health.
- In addition to providing standard metrics for container memory and compute resource usage, when combined
  with eGPU, it can also deliver advanced metrics for container GPU utilization and performance indicators.
- Building on the foundation of providing imperceptible full-card health metrics, AMPerf exporter further offers
  a more nuanced health monitoring function, allowing for online detection of GPU hardware and software failures
  with minimal impact on operations.

## Getting Started

LJ RuntimeKit Exporter supports deployment through RPM and cloud-native methods, adapting to different scenarios via startup
parameters or environment variables.

### Cloud-native Deployment Configuration
#### Prerequisites

- Image address: registry.cn-hangzhou.aliyuncs.com/eflops-amp/lj-runtimekit-exporter, with the image version selected
  according to actual needs.
- Helm deployment file: the 'deployment/' folder is provided offline.

#### Helm configuration item description:
- port configuration: The service:port tag in values.yaml allows the container to expose the port number externally,
  with a default of 9400. Similarly, HTTP address can also be configured through service: address, with a default of 9400.
- Mirroring Configuration: Image configuration values.yaml file image:repository and tag tags define the image
  repository and version. For intranet users, they need to manually upload the image to the private repository and change
  the repository and tag to the corresponding configuration.
- Collection frequency configuration: In the extraEnv file in the values.yaml file - name:AMPERF_EXPORTER_INTERVAL
  environment variable, the value is the exporter frequency (in ms), with a default of 10000ms.
- output format: In the extraEnv file in the values.yaml file - name: AMPERF-OUTPUT-FORMAT environment variable, the
  value is the output format for collecting data. Currently, there are several options available: **ack, egpu, etrace, prom, sgpu, and walle**.
- output type: In the extraEnv file in the values.yaml file - name: AMPERF-OUTPUT.TYPE environment variable, the value
  is the type of output from the exporter. Currently, there are several types: **file, http full, and http push**.
- collect pod info type: In the extraEnv file in the values.yaml file - name: AMPERF-POD-COLLECTOR-TYPE environment
  variable, where the value is specified by the exporter as the way to collect pod information. Currently, there are
  several options: **none, kubelet uds, and kubelet http**. When it is of kubelet uds type, it is necessary to add a
  mounting directory /var/lib/kubelet/pod resources
- collect file: In the extraEnv file in the values.yaml file - name: AMPERF.COLLECTORSFILE environment variable, the value
  specifies the path for collecting documents for the exporter, which mainly includes information entries that need to be collected.
- running mode: In the extraEnv file in the values.yaml file - name: AMPERF-DAEMON-MODE environment variable, the value
  is the running mode of the exporter, mainly including all-components and exporter-only. The former will run all components
  including exporter, collector, detector, etc., while the latter is the default environment where all components except the
  exporter have already been run, and only the exporter needs to be run. Users need to make their own judgments here,   - name: amperf-grpc
  hostPath: "/tmp/amp/sockets/"

## Building from Source
To build the LJ RuntimeKit Exporter from source, clone the repository and build the binary with:
```
git clone https://github.com/aliyun/LJ-RuntimeKit.git
cd LJ-RuntimeKit-Exporter
```

#### Installation
First, execute the amp_exporter script to complete image construction and generate yaml files. Modify the relevant parameters for building images through the amp_exporter script
```
make docker
```
Then, execute the following command to deploy in the k8s cluster.
```
helm install lj-runtimekit-exporter -n <namespace> ./deployment -f ./deployment/values-exporter.yaml #namespace self defined
```
Execute the following command when uninstalling is required.
```
helm uninstall lj-runtimekit-exporter -n <namespace>
```

## Usage

LJ RuntimeKit Exporter supports four different metric collection methods to adapt to various usage scenarios. These methods are
**file interface**, **HTTP PULL**, **HTTP PUSH**, and **AMPerfd**. Here is a brief overview of each method.

### HTTP PULL

When deployed in a cloud-native environment, LJ RuntimeKit Exporter by default exposes metrics through the HTTP PULL method, which is
typically scraped by Prometheus. 

## Contributing

We welcome contributions! Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the
process for submitting pull requests.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Support

If you have any issues or questions, please file an issue on the GitHub issue tracker.

## Acknowledgements

Thanks to the Kubernetes community for support and collaboration.

Any external libraries or tools used in this project are credited here.
