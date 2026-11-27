/*
Copyright (c) 2024-2026 Alibaba Cloud.

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

package plugin

import (
	"context"
	"fmt"
	"net"
	"os"
	"path"
	"sync"
	"time"

	xpugrpc "github.com/lj-runtimekit/device-plugin/internal/grpc"
	"github.com/lj-runtimekit/device-plugin/internal/utils"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

type DeviceBase struct {
	Minor uint   `json:"minor"`
	UUID  string `json:"uuid"`
}

var _ PluginInterface = &XpuDevicePlugin{}

type XpuDevicePlugin struct {
	pluginapi.UnimplementedDevicePluginServer

	ResourceName string
	Devs         map[string]*pluginapi.Device
	Socket       string
	server       *grpc.Server
	AssignedFlag string

	deviceMeta *utils.DeviceMeta

	K8sClient kubernetes.Interface
	stop      chan struct{}
	// health carries a wake up signal to the ListAndWatch goroutine, the
	// device list itself is read under devsMu
	health chan struct{}
	// devsMu guards Devs: it is written by initialize / Unhealthy / SetHealth
	// and read by the ListAndWatch goroutine
	devsMu sync.RWMutex
}

func DevicePluginForResource(resourceName, socketPath, assignedFlag string, deviceMeta *utils.DeviceMeta, client kubernetes.Interface) (PluginInterface, error) {
	return &XpuDevicePlugin{
		ResourceName: resourceName,
		Devs:         make(map[string]*pluginapi.Device),
		Socket:       socketPath,
		AssignedFlag: assignedFlag,
		deviceMeta:   deviceMeta,
		K8sClient:    client,

		stop:   make(chan struct{}),
		health: make(chan struct{}, 1),
	}, nil
}

// sliceDeviceIDs resolves the slice fake device ids of one card. gpu-slice is
// a dual-mode resource ("MPS-" ids when xdm advertises MPS compute slices, i.e.
// the card creates no MIG instance; "MIG-" ids otherwise); the resolution is
// shared with the health path so reporting and unhealth marking never drift
// apart.
func (dp *XpuDevicePlugin) sliceDeviceIDs(dev *xpugrpc.Device) []string {
	ids, err := utils.SliceFakeDeviceIDs(dev)
	if err != nil {
		klog.Errorf("dp %s failed to resolve slice devices: %v", dp.ResourceName, err)
		return nil
	}
	return ids
}

func (dp *XpuDevicePlugin) initialize() {
	devs := make(map[string]*pluginapi.Device)

	// RDMA token 插件不依赖 deviceMeta，直接生成 fake 设备
	if dp.ResourceName == utils.ResourceRDMA {
		for i := 0; i < utils.RDMATokenCount; i++ {
			id := fmt.Sprintf("rdma-token-%d", i)
			devs[id] = &pluginapi.Device{
				ID:     id,
				Health: pluginapi.Healthy,
			}
		}
		dp.setDevices(devs)
		return
	}

	deviceIDs := dp.getAllDeviceIDs()
	for _, id := range deviceIDs {
		dev, ok := dp.deviceMeta.Get(id)
		if !ok {
			klog.Errorf("device plugin %s set device id %d not found", dp.ResourceName, id)
			continue
		}
		switch dp.ResourceName {
		case utils.ResourceCore:
			for i := uint(0); i < uint(dev.TotalCore); i++ {
				fakeCoreID := utils.GenerateFakeCoreID(dev.Id, i)
				devs[fakeCoreID] = &pluginapi.Device{
					ID:     fakeCoreID,
					Health: dev.Health,
				}
			}
		case utils.ResourceMemory:
			utils.SetGPUMemory(uint(dev.TotalMemory))
			for i := uint(0); i < utils.GetGPUMemory(); i++ {
				fakeMemoryID := utils.GenerateFakeMemoryID(dev.Id, i)
				devs[fakeMemoryID] = &pluginapi.Device{
					ID:     fakeMemoryID,
					Health: dev.Health,
				}
			}
		case utils.ResourceSlice:
			for _, fakeSliceID := range dp.sliceDeviceIDs(dev) {
				devs[fakeSliceID] = &pluginapi.Device{
					ID:     fakeSliceID,
					Health: dev.Health,
				}
			}
		case utils.ResourcePossibility:
			for i := uint(0); i < uint(utils.DefaultResourcePossibility); i++ {
				fakePossibilityID := utils.GenerateFakePossibilityID(dev.Id, i)
				devs[fakePossibilityID] = &pluginapi.Device{
					ID:     fakePossibilityID,
					Health: dev.Health,
				}
			}
		default:
			devs[dev.Id] = &pluginapi.Device{
				ID:     dev.Id,
				Health: dev.Health,
			}
		}
	}
	dp.setDevices(devs)
}

// setDevices installs a freshly built device list.
func (dp *XpuDevicePlugin) setDevices(devs map[string]*pluginapi.Device) {
	dp.devsMu.Lock()
	defer dp.devsMu.Unlock()
	dp.Devs = devs
}

// SetHealth flips every device of this resource to the given health and wakes
// up ListAndWatch, so kubelet recomputes the allocatable amount. Devices of an
// unhealthy resource stay registered but no longer take part in scheduling,
// which is how the topology sync withdraws the rdma resource without tearing
// the plugin down.
func (dp *XpuDevicePlugin) SetHealth(healthy bool) {
	health := pluginapi.Unhealthy
	if healthy {
		health = pluginapi.Healthy
	}

	dp.devsMu.Lock()
	for _, dev := range dp.Devs {
		dev.Health = health
	}
	dp.devsMu.Unlock()

	dp.notifyDevicesChanged()
}

// notifyDevicesChanged wakes the ListAndWatch goroutine. It never blocks: one
// pending signal is enough because the goroutine sends the whole device list.
func (dp *XpuDevicePlugin) notifyDevicesChanged() {
	select {
	case <-dp.stop:
		return
	default:
	}

	select {
	case dp.health <- struct{}{}:
	default:
	}
}

func (dp *XpuDevicePlugin) Name() string {
	return dp.ResourceName
}

func (dp *XpuDevicePlugin) Stop() error {
	if dp.server == nil {
		return nil
	}

	dp.server.Stop()
	dp.server = nil
	// health stays open on purpose: notifyDevicesChanged may run from the
	// topology sync right after Stop and sending on a closed channel panics
	close(dp.stop)

	return dp.cleanup()
}

func (dp *XpuDevicePlugin) Start() error {
	dp.initialize()

	if err := dp.serve(); err != nil {
		klog.Errorf("Could not start device plugin %s due to %v", dp.Name(), err)
		return err
	}

	klog.Infoln("Starting to serve on", dp.Socket)

	if err := dp.register(); err != nil {
		klog.Errorf("Could not register device plugin %s due to %v", dp.Name(), err)
		dp.Stop()
		return err
	}
	klog.V(4).Infof("Registered device plugin %s with Kubelet", dp.Name())

	return nil
}

func (dp *XpuDevicePlugin) Unhealthy(device *DeviceBase) {
	// RDMA token 插件不需要处理健康状态变更
	if dp.ResourceName == utils.ResourceRDMA {
		return
	}

	dev, ok := dp.deviceMeta.Get(int(device.Minor))
	if !ok {
		klog.Errorf("device plugin %s unhealthy device %d %s not found", dp.ResourceName, device.Minor, device.UUID)
		return
	}

	// the ListAndWatch goroutine reads the list concurrently
	dp.devsMu.Lock()
	defer dp.devsMu.Unlock()

	switch dp.ResourceName {
	case utils.ResourceCore:
		klog.Infof("unhealthy %s dev %v", dp.ResourceName, dev)
		for i := uint(0); i < uint(dev.TotalCore); i++ {
			fakeCoreID := utils.GenerateFakeCoreID(dev.Id, i)
			fakeDev, ok := dp.Devs[fakeCoreID]
			if ok {
				fakeDev.Health = pluginapi.Unhealthy
			}
			if i == uint(dev.TotalCore)-1 {
				dp.notifyDevicesChanged()
			}
		}
	case utils.ResourceMemory:
		klog.Infof("unhealthy %s dev %v", dp.ResourceName, dev)
		for i := uint(0); i < utils.GetGPUMemory(); i++ {
			fakeMemoryID := utils.GenerateFakeMemoryID(dev.Id, i)
			fakeDev, ok := dp.Devs[fakeMemoryID]
			if ok {
				fakeDev.Health = pluginapi.Unhealthy
			}
			if i == utils.GetGPUMemory()-1 {
				dp.notifyDevicesChanged()
			}
		}
	case utils.ResourceSlice:
		klog.Infof("unhealthy %s dev %v", dp.ResourceName, dev)
		sliceIDs := dp.sliceDeviceIDs(dev)
		for i, fakeSliceID := range sliceIDs {
			fakeDev, ok := dp.Devs[fakeSliceID]
			if ok {
				fakeDev.Health = pluginapi.Unhealthy
			}
			if i == len(sliceIDs)-1 {
				dp.notifyDevicesChanged()
			}
		}
	case utils.ResourcePossibility:
		klog.Infof("unhealthy %s dev %v", dp.ResourceName, dev)
		for i := uint(0); i < uint(utils.DefaultResourcePossibility); i++ {
			fakePossibilityID := utils.GenerateFakePossibilityID(dev.Id, i)
			fakeDev, ok := dp.Devs[fakePossibilityID]
			if ok {
				fakeDev.Health = pluginapi.Unhealthy
			}
			if i == utils.DefaultResourcePossibility-1 {
				dp.notifyDevicesChanged()
			}
		}
	default:
		klog.Infof("unhealthy %s dev %v", dp.ResourceName, dev)
		fakeDev, ok := dp.Devs[dev.Id]
		if ok {
			fakeDev.Health = pluginapi.Unhealthy
			dp.notifyDevicesChanged()
		}
	}

	return
}

func (dp *XpuDevicePlugin) dial(unixSocketPath string, timeout time.Duration) (*grpc.ClientConn, error) {
	c, err := grpc.Dial(unixSocketPath, grpc.WithInsecure(), grpc.WithBlock(),
		grpc.WithTimeout(timeout),
		grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
			return net.DialTimeout("unix", addr, timeout)
		}))

	if err != nil {
		return nil, err
	}
	return c, nil
}

func (dp *XpuDevicePlugin) startGRPCServer(sock net.Listener) {
	go func() {
		klog.Infof("starting gRPC server for %s", dp.ResourceName)
		if err := dp.server.Serve(sock); err != nil {
			klog.Fatalf("gRPC server for %s crashed: %v", dp.ResourceName, err)
		}
	}()
}

func (dp *XpuDevicePlugin) waitForServerStart(socket string) error {
	conn, err := dp.dial(socket, 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	return nil
}

func (dp *XpuDevicePlugin) serve() error {
	if err := dp.cleanup(); err != nil {
		return err
	}
	sock, err := net.Listen("unix", dp.Socket)
	if err != nil {
		return err
	}

	dp.server = grpc.NewServer()
	pluginapi.RegisterDevicePluginServer(dp.server, dp)
	go dp.server.Serve(sock)

	// Wait for server to start by launching a blocking connection
	conn, err := dp.dial(dp.Socket, 5*time.Second)
	if err != nil {
		return err
	}
	conn.Close()

	return nil
}

// Register registers the device plugin with the kubelet.
func (dp *XpuDevicePlugin) register() error {
	conn, err := dp.dial(pluginapi.KubeletSocket, 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	klog.Infof("registering device plugin %s on socket %s", dp.ResourceName, dp.Socket)

	client := pluginapi.NewRegistrationClient(conn)

	registerReq := &pluginapi.RegisterRequest{
		Version:      pluginapi.Version,
		Endpoint:     path.Base(dp.Socket),
		ResourceName: dp.ResourceName,
		// PreStartRequired makes kubelet invoke PreStartContainer before each
		// container starts, where the pod identity resolution and the xdm
		// allocate call are performed.
		Options: &pluginapi.DevicePluginOptions{
			PreStartRequired: true,
		},
	}

	_, err = client.Register(context.TODO(), registerReq)
	if err != nil {
		return err
	}
	return nil
}

// cleanup removes the UNIX socket used by the gRPC server
func (dp *XpuDevicePlugin) cleanup() error {
	if err := os.Remove(dp.Socket); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// GetPluginDevices returns a snapshot of the device list. ListAndWatch serialises
// the returned slice outside of devsMu while SetHealth / Unhealthy rewrite the
// health of the shared devices, so every device is cloned under the lock: handing
// out the shared pointers would race with the health update and could send a list
// that mixes the old and the new health.
func (dp *XpuDevicePlugin) GetPluginDevices() []*pluginapi.Device {
	dp.devsMu.RLock()
	defer dp.devsMu.RUnlock()

	res := make([]*pluginapi.Device, 0, len(dp.Devs))
	for _, device := range dp.Devs {
		if device == nil {
			continue
		}
		res = append(res, proto.Clone(device).(*pluginapi.Device))
	}

	return res
}

// ListAndWatch lists devices and updates that list according to the health status.
func (dp *XpuDevicePlugin) ListAndWatch(e *pluginapi.Empty, s pluginapi.DevicePlugin_ListAndWatchServer) error {
	klog.Infof("starting ListAndWatch for %s", dp.ResourceName)

	if err := s.Send(&pluginapi.ListAndWatchResponse{Devices: dp.GetPluginDevices()}); err != nil {
		return err
	}

	for {
		select {
		case <-dp.stop:
			return nil
		case <-dp.health:
			// FIXME: there is no way to recover from the Unhealthy state.

			if err := s.Send(&pluginapi.ListAndWatchResponse{Devices: dp.GetPluginDevices()}); err != nil {
				klog.Errorf("ListAndWatch for %s failed to send devices: %v", dp.ResourceName, err)
				return err
			}
		}
	}
}

func (dp *XpuDevicePlugin) GetDevicePluginOptions(context.Context, *pluginapi.Empty) (*pluginapi.DevicePluginOptions, error) {
	return &pluginapi.DevicePluginOptions{
		PreStartRequired: true,
	}, nil
}

func (dp *XpuDevicePlugin) GetPreferredAllocation(context.Context, *pluginapi.PreferredAllocationRequest) (*pluginapi.PreferredAllocationResponse, error) {
	return &pluginapi.PreferredAllocationResponse{}, nil
}

func (dp *XpuDevicePlugin) getAllDeviceIDs() []int {
	dp.deviceMeta.RLock()
	defer dp.deviceMeta.RUnlock()

	ids := make([]int, 0, len(dp.deviceMeta.Cache))
	for id := range dp.deviceMeta.Cache {
		ids = append(ids, id)
	}

	return ids
}
