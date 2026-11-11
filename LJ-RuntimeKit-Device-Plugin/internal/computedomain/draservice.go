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
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	cgoresource "k8s.io/client-go/kubernetes/typed/resource/v1"
	"k8s.io/klog/v2"
	drapbv1 "k8s.io/kubelet/pkg/apis/dra/v1"
	drapbv1beta1 "k8s.io/kubelet/pkg/apis/dra/v1beta1"
)

// DRAService 实现 kubelet 的 DRA gRPC 服务(NodePrepareResources /
// NodeUnprepareResources),并负责发布 ResourceSlice。
//
// 它对应 k8s.io/dynamic-resource-allocation/kubeletplugin 中 Start() 的
// "draService" 部分(自实现以保持 driver 结构清晰)。
type DRAService struct {
	driver         *driver
	config         *Config
	resourceClient cgoresource.ResourceV1Interface

	socketPath string
	server     *grpc.Server
	listener   net.Listener
	wg         sync.WaitGroup
}

// NewDRAService 创建 DRAService。
func NewDRAService(d *driver, config *Config) *DRAService {
	return &DRAService{
		driver:         d,
		config:         config,
		resourceClient: config.CoreClient.ResourceV1(),
		socketPath:     filepath.Join(config.DriverPluginPath(), DRASocketFileName),
	}
}

// SocketPath 返回 DRA gRPC 服务的 socket 路径(注册时上报给 kubelet)。
func (s *DRAService) SocketPath() string {
	return s.socketPath
}

// Start 启动 DRA 服务:
//   - 监听 DRA socket 提供 gRPC 服务。
//
// ComputeDomainClique informer 由 DeviceState 在首次 Prepare 时懒初始化。
// ResourceSlice 的发布/调和由 driver.Start 里的 resourceSliceSyncer
// 负责(与 dra-driver-nvidia-gpu 一样是后台异步进行的)。
func (s *DRAService) Start(ctx context.Context) error {
	// 监听 DRA socket。
	listener, err := listenUnixSocket(s.socketPath)
	if err != nil {
		return fmt.Errorf("error listening on %q: %w", s.socketPath, err)
	}
	s.listener = listener

	s.server = grpc.NewServer()
	impl := &draPluginServer{service: s}
	drapbv1.RegisterDRAPluginServer(s.server, impl)
	drapbv1beta1.RegisterDRAPluginServer(s.server, drapbv1beta1.V1ServerWrapper{DRAPluginServer: impl})

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.server.Serve(listener); err != nil {
			klog.Errorf("DRA gRPC server failed: %v", err)
		}
	}()

	klog.Infof("DRA gRPC server started at %q", s.socketPath)
	return nil
}

// Stop 停止 DRA gRPC 服务并清理 socket 文件。
func (s *DRAService) Stop() {
	if s.server != nil {
		s.server.Stop()
	}
	s.wg.Wait()
	if err := os.Remove(s.socketPath); err != nil && !os.IsNotExist(err) {
		klog.Errorf("error removing DRA socket %q: %v", s.socketPath, err)
	}
}

// listenUnixSocket 在指定路径上监听 unix socket,先清理残留 socket 文件。
func listenUnixSocket(socketPath string) (net.Listener, error) {
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("error removing stale socket %q: %w", socketPath, err)
	}
	return net.Listen("unix", socketPath)
}

// draPluginServer 是 DRA gRPC 服务的实现,把 gRPC 请求转成对 driver 的
// nodePrepareResource / nodeUnprepareResource 调用。
type draPluginServer struct {
	service *DRAService
	drapbv1.UnimplementedDRAPluginServer
}

// NodePrepareResources implements [drapbv1.DRAPluginServer]。
//
// 请求中的多个 claim 通过 workqueue 并发处理(带 rate-limiting 重试):
// claim 的 Get 查询互不阻塞,单个 claim 的瞬态失败会被 workqueue 按退避
// 策略自动重试,而 permanent error 和成功都会立即结束该 claim 的处理。
// 实际的 Prepare 仍由 pulock 保证节点级互斥。
func (h *draPluginServer) NodePrepareResources(ctx context.Context, req *drapbv1.NodePrepareResourcesRequest) (*drapbv1.NodePrepareResourcesResponse, error) {
	klog.V(6).Infof("NodePrepareResources called with %d claim(s)", len(req.Claims))

	resp := &drapbv1.NodePrepareResourcesResponse{Claims: map[string]*drapbv1.NodePrepareResourceResponse{}}

	var wg sync.WaitGroup
	var mutex sync.Mutex
	ctx, cancel := context.WithTimeout(ctx, ErrorRetryMaxTimeout)
	workQueue := New(DefaultPrepUnprepRateLimiter())

	for _, claimReq := range req.Claims {
		// 先获取 claim 对象（同步,失败直接记录错误）。
		claim, err := h.service.resourceClient.ResourceClaims(claimReq.Namespace).Get(ctx, claimReq.Name, metav1.GetOptions{})
		if err != nil {
			resp.Claims[claimReq.Uid] = &drapbv1.NodePrepareResourceResponse{
				Error: fmt.Sprintf("retrieve claim %s/%s: %v", claimReq.Namespace, claimReq.Name, err),
			}
			continue
		}
		if claim.Status.Allocation == nil {
			resp.Claims[claimReq.Uid] = &drapbv1.NodePrepareResourceResponse{
				Error: fmt.Sprintf("claim %s/%s not allocated", claimReq.Namespace, claimReq.Name),
			}
			continue
		}
		if claim.UID != types.UID(claimReq.Uid) {
			resp.Claims[claimReq.Uid] = &drapbv1.NodePrepareResourceResponse{
				Error: fmt.Sprintf("claim %s/%s got replaced", claimReq.Namespace, claimReq.Name),
			}
			continue
		}

		wg.Add(1)
		key := claimReq.Uid
		workQueue.EnqueueRawWithKey(claim, key, func(ctx context.Context, obj any) error {
			c := obj.(*resourceapi.ResourceClaim)
			done, res := h.service.driver.nodePrepareResource(ctx, c)
			if done {
				mutex.Lock()
				prepResp := &drapbv1.NodePrepareResourceResponse{}
				if res.Err != nil {
					prepResp.Error = res.Err.Error()
				} else {
					for _, device := range res.Devices {
						prepResp.Devices = append(prepResp.Devices, &drapbv1.Device{
							RequestNames: device.Requests,
							PoolName:     device.PoolName,
							DeviceName:   device.DeviceName,
							CdiDeviceIds: device.CDIDeviceIDs,
						})
					}
				}
				resp.Claims[string(c.UID)] = prepResp
				mutex.Unlock()
				wg.Done()
				return nil
			}
			return fmt.Errorf("%w", res.Err)
		})
	}

	go func() {
		wg.Wait()
		cancel()
	}()

	workQueue.Run(ctx)
	return resp, nil
}

// NodeUnprepareResources implements [drapbv1.DRAPluginServer]。
//
// 与 NodePrepareResources 一样通过 workqueue 并发处理多个 claim(带
// rate-limiting 重试),实际的 Unprepare 仍由 pulock 保证节点级互斥。
func (h *draPluginServer) NodeUnprepareResources(ctx context.Context, req *drapbv1.NodeUnprepareResourcesRequest) (*drapbv1.NodeUnprepareResourcesResponse, error) {
	klog.V(6).Infof("NodeUnprepareResources called with %d claim(s)", len(req.Claims))

	resp := &drapbv1.NodeUnprepareResourcesResponse{Claims: map[string]*drapbv1.NodeUnprepareResourceResponse{}}

	var wg sync.WaitGroup
	var mutex sync.Mutex
	ctx, cancel := context.WithTimeout(ctx, ErrorRetryMaxTimeout)
	workQueue := New(DefaultPrepUnprepRateLimiter())

	for _, claimReq := range req.Claims {
		claimRef := NamespacedObject{UID: claimReq.Uid}
		claimRef.NamespacedName.Namespace = claimReq.Namespace
		claimRef.NamespacedName.Name = claimReq.Name

		wg.Add(1)
		key := claimReq.Uid
		workQueue.EnqueueRawWithKey(claimRef, key, func(ctx context.Context, obj any) error {
			ref := obj.(NamespacedObject)
			done, err := h.service.driver.nodeUnprepareResource(ctx, ref)
			if done {
				mutex.Lock()
				unprepResp := &drapbv1.NodeUnprepareResourceResponse{}
				if err != nil {
					unprepResp.Error = err.Error()
					klog.V(0).Infof("Permanent error unpreparing devices for claim %v: %v", ref.UID, err)
				}
				resp.Claims[ref.UID] = unprepResp
				mutex.Unlock()
				wg.Done()
				return nil
			}
			return fmt.Errorf("%w", err)
		})
	}

	go func() {
		wg.Wait()
		cancel()
	}()

	workQueue.Run(ctx)
	return resp, nil
}
