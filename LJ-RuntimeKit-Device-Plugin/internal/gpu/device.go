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

package gpu

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	xpugrpc "github.com/lj-runtimekit/device-plugin/internal/grpc"
	"github.com/lj-runtimekit/device-plugin/internal/utils"

	"k8s.io/klog/v2"
)

func GetDeviceType(client xpugrpc.XpuDeviceManagerServiceClient) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	req := &xpugrpc.XPUTypeRequest{}

	resp, err := client.ListSupportedXPUType(ctx, req)
	if err != nil {
		return "", fmt.Errorf("failed to list supported XPU types: %w", err)
	}

	if resp == nil {
		return "", fmt.Errorf("received nil response from XPU type service")
	}

	if resp.Status != StatusOK {
		return "", fmt.Errorf("non-OK status from XPU type service: %v", resp.Status)
	}
	if len(resp.SupportedType) <= 0 {
		return "", fmt.Errorf("XPU type array is empty")
	}

	var xpuType string
	switch resp.SupportedType[0] {
	case xpugrpc.XPUType_GPU:
		xpuType = utils.DeviceTypeGPU
	case xpugrpc.XPUType_PPU:
		xpuType = utils.DeviceTypePPU
	case xpugrpc.XPUType_AMD:
		xpuType = utils.DeviceTypeAMD
	default:
		return "", fmt.Errorf("unsupported device type")
	}
	return xpuType, nil
}

// listAndWatchRetryDelay is the first retry delay of GrpcListAndWatch. It is a
// variable so tests do not have to wait a second per retry.
var listAndWatchRetryDelay = time.Second

// GrpcListAndWatch waits for the first xdm snapshot and returns once the device
// cache is filled with it. Both establishing the stream and receiving from it are
// retried with a capped exponential backoff: without a snapshot the plugins
// started afterwards would have no device to serve, so the snapshot is the one
// thing this call cannot give up on. Every failed round is logged with the
// reason, which is the only signal that xdm is unreachable while the plugins are
// still not started.
func GrpcListAndWatch(ctx context.Context, client xpugrpc.XpuDeviceManagerServiceClient, deviceCache *utils.DeviceMeta) {
	klog.Infof("starting xdm listAndWatch gRPC")
	backoff := listAndWatchRetryDelay

	for {
		if ctx.Err() != nil {
			klog.Infof("xdm listAndWatch stopped before receiving a device snapshot")
			return
		}

		err := receiveFirstSnapshot(ctx, client, deviceCache)
		if err == nil {
			klog.Info("listAndWatch client exiting after receiving data")
			return
		}
		if ctx.Err() != nil {
			klog.Infof("xdm listAndWatch stopped before receiving a device snapshot")
			return
		}

		klog.Errorf("xdm listAndWatch failed to get a device snapshot: %v, retrying in %s", err, backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > maxWatchBackoff {
			backoff = maxWatchBackoff
		}
	}
}

// receiveFirstSnapshot consumes the stream until it yields a non empty snapshot
// or breaks. The caller owns the retry: a broken gRPC stream keeps returning its
// last error, so it has to be replaced instead of recv'd again.
func receiveFirstSnapshot(ctx context.Context, client xpugrpc.XpuDeviceManagerServiceClient, deviceCache *utils.DeviceMeta) error {
	stream, err := client.ListAndWatch(ctx, &xpugrpc.ListAndWatchRequest{})
	if err != nil {
		return fmt.Errorf("establish stream: %w", err)
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return fmt.Errorf("stream closed by server before any device snapshot")
		}
		if err != nil {
			return err
		}

		if resp == nil {
			klog.Errorf("Received nil response from stream")
			continue
		}

		if len(resp.Devices) == 0 {
			klog.Errorf("No devices in response")
			continue
		}

		for _, d := range resp.Devices {
			minorStr := d.ExtenderResources[utils.DeviceMinor]
			minor, err := strconv.Atoi(minorStr)
			if err != nil {
				klog.Errorf("failed to get device %s minor: %v", d.Id, err)
				continue
			}

			deviceCache.Put(minor, d)
			// print the raw abstract topology json as reported by xdm so it
			// can be verified against the protocol definition
			if topo, ok := d.ExtenderResources[utils.TopologyKey]; ok && topo != "" {
				klog.Infof("listAndWatch device %s (minor %d) reported topology: %s", d.Id, minor, topo)
			}
		}

		klog.V(4).Infof("listAndWatch received device data %v", deviceCache.Cache)
		return nil
	}
}

// maxWatchBackoff caps the reconnect delay of the long lived ListAndWatch
// stream, so a restarted xdm is picked up quickly without hot looping.
const maxWatchBackoff = 30 * time.Second

// snapshotOnlyReconnectInterval is the reconnect delay used when the server
// closes the stream right after serving the snapshot, which is how an xdm that
// predates the continuous watch behaves. It must stay long: reconnecting on the
// short error backoff would rebuild the stream once a second forever for a
// snapshot that did not change. It is a variable so tests do not have to wait
// for the real delay.
var snapshotOnlyReconnectInterval = 30 * time.Second

// WatchXdmDevices keeps the xdm ListAndWatch stream open instead of consuming
// a single snapshot: every pushed snapshot replaces the device cache (devices
// missing from it are dropped) and triggers onSnapshot, which is what lets the
// node topology follow xdm after startup. A broken stream is re-established
// with a capped exponential backoff; the reconnect delivers the current full
// snapshot again, so no history has to be replayed.
func WatchXdmDevices(ctx context.Context, client xpugrpc.XpuDeviceManagerServiceClient, deviceCache *utils.DeviceMeta, onSnapshot func()) {
	backoff := time.Second

	for {
		if ctx.Err() != nil {
			klog.Info("xdm listAndWatch stopped")
			return
		}

		// any successfully received snapshot proves the stream healthy, so the
		// next reconnect starts from the shortest delay again
		err := receiveXdmSnapshots(ctx, client, deviceCache, func() {
			backoff = time.Second
			onSnapshot()
		})
		if ctx.Err() != nil {
			klog.Info("xdm listAndWatch stopped")
			return
		}

		// A server that closes the stream after serving the snapshot is not
		// failing: reconnecting on the short error backoff would rebuild the
		// stream once a second forever for data that did not change. Treat it as a
		// slow poll instead, every reconnect delivers a full snapshot anyway.
		if errors.Is(err, io.EOF) {
			klog.V(2).Infof("xdm listAndWatch stream closed by the server after a snapshot, polling again in %s", snapshotOnlyReconnectInterval)
			select {
			case <-ctx.Done():
				return
			case <-time.After(snapshotOnlyReconnectInterval):
			}
			continue
		}

		klog.Errorf("xdm listAndWatch stream ended: %v, reconnecting in %s", err, backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > maxWatchBackoff {
			backoff = maxWatchBackoff
		}
	}
}

// receiveXdmSnapshots consumes the stream until it is closed or broken.
func receiveXdmSnapshots(ctx context.Context, client xpugrpc.XpuDeviceManagerServiceClient, deviceCache *utils.DeviceMeta, onSnapshot func()) error {
	stream, err := client.ListAndWatch(ctx, &xpugrpc.ListAndWatchRequest{})
	if err != nil {
		return fmt.Errorf("establish stream: %w", err)
	}

	for {
		resp, err := stream.Recv()
		if err != nil {
			return err
		}

		if resp == nil {
			klog.Errorf("received nil response from stream")
			continue
		}

		if len(resp.Devices) == 0 {
			// an empty list is a broken snapshot, not a withdrawal: the empty
			// topology is pushed as devices without the topology key
			klog.Errorf("no devices in response")
			continue
		}

		snapshot := make(map[int]*xpugrpc.Device, len(resp.Devices))
		for _, d := range resp.Devices {
			minorStr := d.ExtenderResources[utils.DeviceMinor]
			minor, err := strconv.Atoi(minorStr)
			if err != nil {
				klog.Errorf("failed to get device %s minor: %v", d.Id, err)
				continue
			}

			snapshot[minor] = d
			if topo, ok := d.ExtenderResources[utils.TopologyKey]; ok && topo != "" {
				klog.V(4).Infof("listAndWatch device %s (minor %d) reported topology: %s", d.Id, minor, topo)
			}
		}

		deviceCache.Replace(snapshot)
		klog.V(4).Infof("listAndWatch received %d device(s)", len(snapshot))
		onSnapshot()
	}
}
