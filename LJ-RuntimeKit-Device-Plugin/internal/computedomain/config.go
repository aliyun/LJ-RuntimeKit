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

package computedomain

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"k8s.io/client-go/dynamic"
	coreclientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"

	nvclientset "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/nvidia.com/clientset/versioned"
	nvscheme "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/nvidia.com/clientset/versioned/scheme"
)

// DefaultKubeletRegistryDir / DefaultKubeletPluginsDir 是 kubelet 存放插件
// 注册 socket 与插件数据的标准目录。
const (
	DefaultKubeletRegistryDir = "/var/lib/kubelet/plugins_registry"
	DefaultKubeletPluginsDir  = "/var/lib/kubelet/plugins"

	// DefaultRuntimeKitEndpoint 是 RuntimeKit 守护进程(x-device-manager)
	// 的默认 gRPC 端点,与 internal/utils 中 XDMGrpcSocket 保持一致。
	DefaultRuntimeKitEndpoint = "unix:/run/amp/eKitC/xdm/sockets/dp2mgr.sock"
)

// Flags 持有 compute-domain kubelet plugin 的全部命令行参数,
// 由 cmd 层填充后传入 NewConfig。
type Flags struct {
	NodeName                      string
	Namespace                     string
	RuntimeKitEndpoint            string
	CdiRoot                       string
	KubeletRegistrarDirectoryPath string
	KubeletPluginsDirectoryPath   string
	Kubeconfig                    string
	MaxNodesPerIMEXDomain         int
}

// Config 持有 Kubernetes 客户端与插件配置。
type Config struct {
	Flags      *Flags
	CoreClient coreclientset.Interface
	DynamicCli dynamic.Interface
	NvidiaCli  nvclientset.Interface
}

// DriverPluginPath 返回本插件在 kubelet plugins 目录下的数据目录。
func (c Config) DriverPluginPath() string {
	return filepath.Join(c.Flags.KubeletPluginsDirectoryPath, DriverName)
}

// NewConfig builds a Config with Kubernetes clients built from the in-cluster
// config or an explicit kubeconfig.
func NewConfig(flags *Flags) (*Config, error) {
	var restConfig *rest.Config
	var err error

	if flags.Kubeconfig != "" {
		restConfig, err = clientcmd.BuildConfigFromFlags("", flags.Kubeconfig)
	} else {
		// Fall back to in-cluster config, then to the default kubeconfig
		// lookup rules ($KUBECONFIG / ~/.kube/config).
		restConfig, err = rest.InClusterConfig()
		if err != nil {
			loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
			restConfig, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
				loadingRules, &clientcmd.ConfigOverrides{}).ClientConfig()
		}
	}
	if err != nil {
		return nil, fmt.Errorf("error building kube client config: %w", err)
	}

	coreClient, err := coreclientset.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("error creating core clientset: %w", err)
	}

	dynamicCli, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("error creating dynamic client: %w", err)
	}

	// 构建指向 resource.aliyun.com/v1beta1 的 REST client
	nvRestConfig := rest.CopyConfig(restConfig)
	nvRestConfig.GroupVersion = &SchemeGroupVersion
	nvRestConfig.APIPath = "/apis"
	nvRestConfig.NegotiatedSerializer = nvscheme.Codecs.WithoutConversion()
	restClient, err := rest.RESTClientFor(nvRestConfig)
	if err != nil {
		return nil, fmt.Errorf("error creating REST client for %s: %w", SchemeGroupVersion, err)
	}
	nvidiaCli := nvclientset.New(restClient)

	return &Config{
		Flags:      flags,
		CoreClient: coreClient,
		DynamicCli: dynamicCli,
		NvidiaCli:  nvidiaCli,
	}, nil
}

// RunPlugin initializes and runs the compute domain kubelet plugin.
//
// ctx 由调用方负责取消(如收到 SIGINT / SIGTERM),本函数阻塞直到 ctx
// 结束并完成清理。
func RunPlugin(ctx context.Context, config *Config) error {
	// Create the plugin directory
	err := os.MkdirAll(config.DriverPluginPath(), 0750)
	if err != nil {
		return err
	}

	// Create and start the driver
	driver, err := NewDriver(ctx, config)
	if err != nil {
		return fmt.Errorf("error creating driver: %w", err)
	}

	err = driver.Start(ctx)
	if err != nil {
		return fmt.Errorf("error starting driver: %w", err)
	}

	<-ctx.Done()
	if err := ctx.Err(); err != nil && !errors.Is(err, context.Canceled) {
		// A canceled context is the normal case here when the process receives
		// a signal. Only log the error for more interesting cases.
		klog.Errorf("error from context: %v", err)
	}

	err = driver.Shutdown()
	if err != nil {
		klog.Errorf("unable to cleanly shutdown driver: %v", err)
	}

	return nil
}
