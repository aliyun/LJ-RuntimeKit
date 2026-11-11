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
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"

	"google.golang.org/grpc"
	"k8s.io/klog/v2"
	drapbv1 "k8s.io/kubelet/pkg/apis/dra/v1"
	drapbv1beta1 "k8s.io/kubelet/pkg/apis/dra/v1beta1"
	registerapi "k8s.io/kubelet/pkg/apis/pluginregistration/v1"
)

// RegistrationService 实现 kubelet 插件注册的 gRPC 服务。
//
// 它在 kubelet 的 plugins_registry 目录下监听
// "<driverName>-reg.sock",kubelet 的 plugin watcher 发现该 socket 后会
// 回调 GetInfo / NotifyRegistrationStatus,从而把本插件的 DRA socket
// 注册给 kubelet。
//
// 它对应 k8s.io/dynamic-resource-allocation/kubeletplugin 中 Start() 的
// "registrationService" 部分(自实现,rollingUpdateUID 相关逻辑已去掉,
// 因此注册 socket 文件名固定为 <driverName>-reg.sock)。
type RegistrationService struct {
	config *Config

	// draEndpointPath 是 DRA gRPC 服务的 socket 完整路径,注册时上报给
	// kubelet(kubelet 会通过它访问 NodePrepareResources 等接口)。
	draEndpointPath string

	socketPath string
	server     *grpc.Server
	listener   net.Listener
	wg         sync.WaitGroup
}

// NewRegistrationService 创建 RegistrationService。
func NewRegistrationService(config *Config, draEndpointPath string) *RegistrationService {
	return &RegistrationService{
		config:          config,
		draEndpointPath: draEndpointPath,
		socketPath:      filepath.Join(config.Flags.KubeletRegistrarDirectoryPath, DriverName+RegistrationSocketFileSuffix),
	}
}

// Start 开始监听注册 socket 并提供 gRPC 服务。
func (s *RegistrationService) Start(ctx context.Context) error {
	listener, err := listenUnixSocket(s.socketPath)
	if err != nil {
		return fmt.Errorf("error listening on %q: %w", s.socketPath, err)
	}
	s.listener = listener

	s.server = grpc.NewServer()
	registerapi.RegisterRegistrationServer(s.server, &registrationServer{
		draEndpointPath: s.draEndpointPath,
	})

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.server.Serve(listener); err != nil {
			klog.Errorf("registration gRPC server failed: %v", err)
		}
	}()

	klog.Infof("Registration gRPC server started at %q (DRA endpoint: %q)", s.socketPath, s.draEndpointPath)
	return nil
}

// Stop 停止注册 gRPC 服务并清理 socket 文件。
func (s *RegistrationService) Stop() {
	if s.server != nil {
		s.server.Stop()
	}
	s.wg.Wait()
	if err := os.Remove(s.socketPath); err != nil && !os.IsNotExist(err) {
		klog.Errorf("error removing registration socket %q: %v", s.socketPath, err)
	}
}

// registrationServer 实现 kubelet plugin registration 的 gRPC 接口。
type registrationServer struct {
	draEndpointPath string

	registerapi.UnimplementedRegistrationServer
}

// GetInfo 由 kubelet plugin watcher 调用,返回插件信息。
func (e *registrationServer) GetInfo(ctx context.Context, req *registerapi.InfoRequest) (*registerapi.PluginInfo, error) {
	return &registerapi.PluginInfo{
		Type:     registerapi.DRAPlugin,
		Name:     DriverName,
		Endpoint: e.draEndpointPath,
		SupportedVersions: []string{
			drapbv1.DRAPluginService,
			drapbv1beta1.DRAPluginService,
		},
	}, nil
}

// NotifyRegistrationStatus 由 kubelet plugin watcher 调用,告知注册结果。
func (e *registrationServer) NotifyRegistrationStatus(ctx context.Context, status *registerapi.RegistrationStatus) (*registerapi.RegistrationStatusResponse, error) {
	if !status.PluginRegistered {
		return nil, fmt.Errorf("failed registration process: %+v", status.Error)
	}

	klog.Infof("Plugin registered with kubelet: %v", DriverName)
	return &registerapi.RegistrationStatusResponse{}, nil
}
