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

package exporter

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	podresourcesapi "k8s.io/kubelet/pkg/apis/podresources/v1alpha1"
)

var (
	socketDir  = "/var/lib/kubelet/pod-resources"
	socketPath = socketDir + "/kubelet.sock"

	connectionTimeout = 10 * time.Second

	validResourceNames = map[string]bool{
		nvidiaResourceName: true,
		gpuResourceName:    true,
	}
)

type PodInfoKubeletResource struct {
	config *Config
}

func NewPodInfoKubeletResource(c *Config) (*PodInfoKubeletResource, error) {
	return &PodInfoKubeletResource{
		config: c,
	}, nil
}

func (p *PodInfoKubeletResource) Name() string {
	return "podMapperKubeletResource"
}

func (p *PodInfoKubeletResource) GetGpuPodInfo() ([]PodInfo, error) {
	_, err := os.Stat(socketPath)
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("No Kubelet socket")
	}

	c, cleanup, err := connectToServer(socketPath)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	pods, err := listPods(c)
	if err != nil {
		return nil, err
	}

	return toDeviceToPod(pods), nil
}

func connectToServer(socket string) (*grpc.ClientConn, func(), error) {
	ctx, cancel := context.WithTimeout(context.Background(), connectionTimeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, socket, grpc.WithInsecure(), grpc.WithBlock(),
		grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
			return net.DialTimeout("unix", addr, timeout)
		}),
	)

	if err != nil {
		return nil, func() {}, fmt.Errorf("failure connecting to %s: %v", socket, err)
	}

	return conn, func() { conn.Close() }, nil
}

func listPods(conn *grpc.ClientConn) (*podresourcesapi.ListPodResourcesResponse, error) {
	client := podresourcesapi.NewPodResourcesListerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), connectionTimeout)
	defer cancel()

	resp, err := client.List(ctx, &podresourcesapi.ListPodResourcesRequest{})
	if err != nil {
		return nil, fmt.Errorf("failure getting pod resources %v", err)
	}

	return resp, nil
}

func toDeviceToPod(devicePods *podresourcesapi.ListPodResourcesResponse) []PodInfo {
	deviceToPod := make([]PodInfo, 0)

	for _, pod := range devicePods.GetPodResources() {
		for _, container := range pod.GetContainers() {
			for _, device := range container.GetDevices() {

				resourceName := device.GetResourceName()
				if valid, exists := validResourceNames[resourceName]; exists && !valid {
					if !strings.HasPrefix(resourceName, nvidiaMigResourcePrefix) {
						continue
					}
				}

				podInfo := PodInfo{
					Full:          true,
					Name:          pod.GetName(),
					Namespace:     pod.GetNamespace(),
					ContainerName: container.GetName(),
				}

				for _, deviceid := range device.GetDeviceIds() {
					podInfo.DeviceId = deviceid
					podInfo.Phase = NanValue
					deviceToPod = append(deviceToPod, podInfo)
				}
			}
		}
	}

	return deviceToPod
}
