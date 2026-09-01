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
	"strconv"
	"time"

	"github.com/lj-runtimekit/device-plugin/internal/utils"

	"google.golang.org/grpc"
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
	ResourceName string
	Devs         map[string]*pluginapi.Device
	Socket       string
	server       *grpc.Server
	AssignedFlag string

	deviceMeta *utils.DeviceMeta

	K8sClient *kubernetes.Clientset
	stop      chan struct{}
	health    chan *pluginapi.Device
}

func DevicePluginForResource(resourceName, socketPath, assignedFlag string, deviceMeta *utils.DeviceMeta, client *kubernetes.Clientset) (PluginInterface, error) {
	return &XpuDevicePlugin{
		ResourceName: resourceName,
		Devs:         make(map[string]*pluginapi.Device),
		Socket:       socketPath,
		AssignedFlag: assignedFlag,
		deviceMeta:   deviceMeta,
		K8sClient:    client,

		stop:   make(chan struct{}),
		health: make(chan *pluginapi.Device),
	}, nil
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
		dp.Devs = devs
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
			migEnable, ok := dev.ExtenderResources[utils.MigEnableKey]
			if ok && migEnable == utils.MigEnableValue {
				slice, ok := dev.ExtenderResources[utils.MigSliceKey]
				if ok {
					sliceNum, err := strconv.Atoi(slice)
					if err != nil {
						klog.Errorf("dp %s failed to parse idx %d migEnable value %s", dp.ResourceName, id, slice)
						continue
					}

					for i := 0; i < sliceNum; i++ {
						fakeMigID := utils.GenerateFakeMigID(dev.Id, uint(sliceNum), uint(i))
						devs[fakeMigID] = &pluginapi.Device{
							ID:     fakeMigID,
							Health: dev.Health,
						}
					}
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
	dp.Devs = devs
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
	close(dp.stop)
	close(dp.health)

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
				dp.health <- fakeDev
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
				dp.health <- fakeDev
			}
		}
	case utils.ResourceSlice:
		klog.Infof("unhealthy %s dev %v", dp.ResourceName, dev)
		migEnable, ok := dev.ExtenderResources[utils.MigEnableKey]
		if ok && migEnable == utils.MigEnableValue {
			slice, ok := dev.ExtenderResources[utils.MigSliceKey]
			if ok {
				sliceNum, err := strconv.Atoi(slice)
				if err != nil {
					klog.Errorf("dp %s failed to parse idx %d migEnable value %s", dp.ResourceName, device.Minor, slice)
					return
				}

				for i := 0; i < sliceNum; i++ {
					fakeMigID := utils.GenerateFakeMigID(dev.Id, uint(sliceNum), uint(i))
					fakeDev, ok := dp.Devs[fakeMigID]
					if ok {
						fakeDev.Health = pluginapi.Unhealthy
					}
					if i == sliceNum-1 {
						dp.health <- fakeDev
					}
				}
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
				dp.health <- fakeDev
			}
		}
	default:
		klog.Infof("unhealthy %s dev %v", dp.ResourceName, dev)
		fakeDev, ok := dp.Devs[dev.Id]
		if ok {
			fakeDev.Health = pluginapi.Unhealthy
			dp.health <- fakeDev
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

func (dp *XpuDevicePlugin) GetPluginDevices() []*pluginapi.Device {
	var res []*pluginapi.Device
	for _, device := range dp.Devs {
		res = append(res, device)
	}
	return res
}

// ListAndWatch lists devices and updates that list according to the health status.
func (dp *XpuDevicePlugin) ListAndWatch(e *pluginapi.Empty, s pluginapi.DevicePlugin_ListAndWatchServer) error {
	klog.Infof("starting ListAndWatch for %s", dp.ResourceName)

	s.Send(&pluginapi.ListAndWatchResponse{Devices: dp.GetPluginDevices()})

	for {
		select {
		case <-dp.stop:
			return nil
		case <-dp.health:
			// FIXME: there is no way to recover from the Unhealthy state.

			s.Send(&pluginapi.ListAndWatchResponse{Devices: dp.GetPluginDevices()})
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
