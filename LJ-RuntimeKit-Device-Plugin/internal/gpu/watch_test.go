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
	"io"
	"sync/atomic"
	"testing"
	"time"

	xpugrpc "github.com/lj-runtimekit/device-plugin/internal/grpc"
	"github.com/lj-runtimekit/device-plugin/internal/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func snapshotOf(devices ...*xpugrpc.Device) func() (*xpugrpc.ListAndWatchResponse, error) {
	return func() (*xpugrpc.ListAndWatchResponse, error) {
		return &xpugrpc.ListAndWatchResponse{Devices: devices}, nil
	}
}

func minorDevice(id, minor string) *xpugrpc.Device {
	return &xpugrpc.Device{Id: id, ExtenderResources: map[string]string{utils.DeviceMinor: minor}}
}

// A long lived stream must replace the cache per snapshot: a device dropped by
// xdm cannot be left behind, and every snapshot wakes the consumer.
func TestReceiveXdmSnapshotsReplacesCache(t *testing.T) {
	client := new(MockXpuDeviceManagerServiceClient)
	stream := &MockStream{
		recvFuncs: []func() (*xpugrpc.ListAndWatchResponse, error){
			snapshotOf(minorDevice("dev1", "0"), minorDevice("dev2", "1")),
			snapshotOf(minorDevice("dev1", "0")),
		},
	}

	cache := utils.NewDeviceMeta()
	cache.Put(9, &xpugrpc.Device{Id: "stale"})

	client.On("ListAndWatch", mock.Anything, mock.Anything).Return(stream, nil)

	notified := 0
	err := receiveXdmSnapshots(context.Background(), client, cache, func() { notified++ })

	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, 2, notified)
	assert.Len(t, cache.Cache, 1)
	assert.Equal(t, "dev1", cache.Cache[0].Id)
}

// An empty device list is a broken snapshot, it must not wipe the cache.
func TestReceiveXdmSnapshotsKeepsCacheOnEmptyList(t *testing.T) {
	client := new(MockXpuDeviceManagerServiceClient)
	stream := &MockStream{
		recvFuncs: []func() (*xpugrpc.ListAndWatchResponse, error){
			func() (*xpugrpc.ListAndWatchResponse, error) {
				return &xpugrpc.ListAndWatchResponse{}, nil
			},
		},
	}

	cache := utils.NewDeviceMeta()
	cache.Put(0, minorDevice("dev1", "0"))

	client.On("ListAndWatch", mock.Anything, mock.Anything).Return(stream, nil)

	notified := 0
	err := receiveXdmSnapshots(context.Background(), client, cache, func() { notified++ })

	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, 0, notified)
	assert.Len(t, cache.Cache, 1)
}

// setSnapshotOnlyReconnectInterval shortens the poll delay for one test and
// restores it afterwards. The tests are not parallel, so the variable is safe to
// touch.
func setSnapshotOnlyReconnectInterval(t *testing.T, interval time.Duration) {
	t.Helper()
	prev := snapshotOnlyReconnectInterval
	snapshotOnlyReconnectInterval = interval
	t.Cleanup(func() { snapshotOnlyReconnectInterval = prev })
}

// A closed stream is re-established: the consumer sees the snapshot of every
// connection instead of exiting after the first one.
func TestWatchXdmDevicesReconnects(t *testing.T) {
	setSnapshotOnlyReconnectInterval(t, 10*time.Millisecond)

	client := new(MockXpuDeviceManagerServiceClient)

	first := &MockStream{
		recvFuncs: []func() (*xpugrpc.ListAndWatchResponse, error){
			snapshotOf(minorDevice("dev1", "0")),
		},
	}
	second := &MockStream{
		recvFuncs: []func() (*xpugrpc.ListAndWatchResponse, error){
			snapshotOf(minorDevice("dev1", "0"), minorDevice("dev2", "1")),
		},
	}

	client.On("ListAndWatch", mock.Anything, mock.Anything).Return(first, nil).Once()
	client.On("ListAndWatch", mock.Anything, mock.Anything).Return(second, nil).Once()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var notified int32
	done := make(chan struct{})
	go func() {
		WatchXdmDevices(ctx, client, utils.NewDeviceMeta(), func() {
			if atomic.AddInt32(&notified, 1) == 2 {
				cancel()
			}
		})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("watch did not stop after the context was cancelled")
	}

	assert.Equal(t, int32(2), atomic.LoadInt32(&notified))
}

// An xdm that closes the stream after serving the snapshot must not turn into a
// one second reconnect loop: the slow poll delay is what keeps it cheap.
func TestWatchXdmDevicesPollsWhenServerClosesStream(t *testing.T) {
	setSnapshotOnlyReconnectInterval(t, 200*time.Millisecond)

	client := new(MockXpuDeviceManagerServiceClient)
	stream := &MockStream{
		recvFuncs: []func() (*xpugrpc.ListAndWatchResponse, error){
			snapshotOf(minorDevice("dev1", "0")),
		},
	}

	var opened int32
	client.On("ListAndWatch", mock.Anything, mock.Anything).Return(stream, nil).Run(func(mock.Arguments) {
		atomic.AddInt32(&opened, 1)
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		WatchXdmDevices(ctx, client, utils.NewDeviceMeta(), func() {})
		close(done)
	}()

	// the error backoff would not open a second stream before one second, the
	// poll delay opens it at 200ms
	time.Sleep(600 * time.Millisecond)
	openedStreams := atomic.LoadInt32(&opened)

	// wait for the watcher before the cleanup restores the package variable,
	// otherwise the write races with its next read
	cancel()
	<-done

	assert.GreaterOrEqual(t, openedStreams, int32(2),
		"the stream must be reopened on the poll delay, not on the error backoff")
}
