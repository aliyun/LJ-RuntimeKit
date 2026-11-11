/*
Copyright (c) 2026 Alibaba Cloud.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/urfave/cli"
	"k8s.io/klog/v2"

	"github.com/lj-runtimekit/device-plugin/internal/computedomain"
)

// computeDomainFlags 返回内嵌 DRA compute-domain 插件的命令行参数。
//
// 插件总开关走 feature gate(见 internal/feature):--feature-gates=ComputeDomain=true;
// 下面这些参数仅在开关开启时生效,全部带安全默认值,不开启时对原
// device plugin 的行为没有任何影响。
func computeDomainFlags(o *options) []cli.Flag {
	namespace := os.Getenv("POD_NAMESPACE")
	if namespace == "" {
		namespace = "default"
	}

	return []cli.Flag{
		&cli.StringFlag{
			Name:        "node-name",
			Usage:       "the name of the node the plugin runs on (required when the ComputeDomain feature gate is enabled; falls back to the NODE_NAME environment variable)",
			Destination: &o.nodeName,
		},
		&cli.StringFlag{
			Name:        "namespace",
			Value:       namespace,
			Usage:       "Namespace for ComputeDomainClique resources",
			Destination: &o.namespace,
		},
		&cli.StringFlag{
			Name:        "runtimekit-endpoint",
			Value:       computedomain.DefaultRuntimeKitEndpoint,
			Usage:       "the gRPC endpoint of the RuntimeKit daemon used to manage IMEX devices",
			Destination: &o.runtimekitEndpoint,
		},
		&cli.StringFlag{
			Name:        "kubelet-registrar-directory-path",
			Value:       computedomain.DefaultKubeletRegistryDir,
			Usage:       "absolute path to the directory where kubelet stores plugin registrations",
			Destination: &o.kubeletRegistrarDirectoryPath,
		},
		&cli.StringFlag{
			Name:        "kubelet-plugins-directory-path",
			Value:       computedomain.DefaultKubeletPluginsDir,
			Usage:       "absolute path to the directory where kubelet stores plugin data",
			Destination: &o.kubeletPluginsDirectoryPath,
		},
		&cli.StringFlag{
			Name:        "kubeconfig",
			Usage:       "absolute path to the kubeconfig file for the embedded DRA plugin (defaults to in-cluster config)",
			Destination: &o.kubeconfig,
		},
		&cli.IntFlag{
			Name:        "max-nodes-per-imex-domain",
			Value:       18,
			Usage:       "Maximum number of nodes per IMEX domain for DNS-mode index allocation",
			Destination: &o.maxNodesPerIMEXDomain,
		},
	}
}

// startComputeDomain 在当前进程内嵌启动 DRA compute-domain kubelet plugin,
// 与传统 device plugin 并行运行。
//
// DRA 插件在自己的 goroutine 中运行:启动失败只记录错误、不影响原
// device plugin 的正常运行;进程收到退出信号时一并优雅退出。
func startComputeDomain(o *options) error {
	nodeName := o.nodeName
	if nodeName == "" {
		nodeName = os.Getenv("NODE_NAME")
	}
	if nodeName == "" {
		return fmt.Errorf("node name is required to embed the compute-domain plugin: set --node-name or the NODE_NAME environment variable")
	}

	flags := &computedomain.Flags{
		NodeName:                      nodeName,
		Namespace:                     o.namespace,
		RuntimeKitEndpoint:            o.runtimekitEndpoint,
		KubeletRegistrarDirectoryPath: o.kubeletRegistrarDirectoryPath,
		KubeletPluginsDirectoryPath:   o.kubeletPluginsDirectoryPath,
		Kubeconfig:                    o.kubeconfig,
		MaxNodesPerIMEXDomain:         o.maxNodesPerIMEXDomain,
	}

	config, err := computedomain.NewConfig(flags)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	go func() {
		defer stop()
		klog.Infof("starting embedded compute-domain DRA plugin (driver: %s) on node %s", computedomain.DriverName, nodeName)
		if err := computedomain.RunPlugin(ctx, config); err != nil {
			klog.Errorf("embedded compute-domain DRA plugin exited: %v", err)
		}
	}()

	return nil
}
