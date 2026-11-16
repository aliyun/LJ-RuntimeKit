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
	"errors"
	"testing"

	xpugrpc "github.com/lj-runtimekit/device-plugin/internal/grpc"
	xpuplugin "github.com/lj-runtimekit/device-plugin/internal/plugin"
	"github.com/lj-runtimekit/device-plugin/internal/utils"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

const topoSyncTestNode = "node-topo-sync"

const topoSyncTestTopology = `{"levels":[` +
	`{"level":1,"name":"pcieSwitches","peers":[0],"topo1":[0]},` +
	`{"level":2,"name":"numaNodes","peers":[0],"topo2":[0]},` +
	`{"level":3,"name":"cpuSockets","peers":[0],"topo3":[0]},` +
	`{"level":11,"name":"hyperNode","topo11":"hn-1"},` +
	`{"level":21,"name":"leafSwitch","topo21":"ASW-001"},` +
	`{"level":22,"name":"pointOfDelivery","topo22":"POD-1"}]}`

// nodePatches returns the node patches the fake cluster received, in order.
func nodePatches(client *k8sfake.Clientset) []k8stesting.PatchAction {
	patches := make([]k8stesting.PatchAction, 0, len(client.Actions()))
	for _, action := range client.Actions() {
		if patch, ok := action.(k8stesting.PatchAction); ok && action.GetResource().Resource == "nodes" {
			patches = append(patches, patch)
		}
	}

	return patches
}

// fakeRdmaPlugin records the health the topology sync asks for.
type fakeRdmaPlugin struct {
	healthy bool
	calls   int
}

func (p *fakeRdmaPlugin) Name() string                    { return utils.ResourceRDMA }
func (p *fakeRdmaPlugin) Start() error                    { return nil }
func (p *fakeRdmaPlugin) Stop() error                     { return nil }
func (p *fakeRdmaPlugin) Unhealthy(*xpuplugin.DeviceBase) {}

func (p *fakeRdmaPlugin) SetHealth(healthy bool) {
	p.healthy = healthy
	p.calls++
}

func newTopoSyncTestManager(t *testing.T) (*DeviceManager, *k8sfake.Clientset, *fakeRdmaPlugin) {
	t.Helper()
	nodeName = topoSyncTestNode

	client := k8sfake.NewSimpleClientset(&corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: topoSyncTestNode},
	})
	rdma := &fakeRdmaPlugin{}
	dm := &DeviceManager{
		k8sClient:  client,
		deviceMeta: utils.NewDeviceMeta(),
		plugins:    make([]xpuplugin.PluginInterface, 0),
		rdmaPlugin: rdma,
	}

	return dm, client, rdma
}

func topoSyncTestDevice(id, minor, topology string) *xpugrpc.Device {
	resources := map[string]string{utils.DeviceMinor: minor}
	if topology != "" {
		resources[utils.TopologyKey] = topology
	}

	return &xpugrpc.Device{
		Id:                id,
		Health:            "Healthy",
		TotalCore:         100,
		TotalMemory:       1024,
		ExtenderResources: resources,
	}
}

// The sync must publish the topology, leave the node untouched for an unchanged
// snapshot, withdraw every owned field on the empty topology and restore it
// afterwards, including the rdma resource.
func TestTopoSyncPublishesWithdrawsAndRestores(t *testing.T) {
	dm, client, rdma := newTopoSyncTestManager(t)
	sync := NewTopoSync(dm)

	dm.deviceMeta.Replace(map[int]*xpugrpc.Device{
		0: topoSyncTestDevice("dev0", "0", topoSyncTestTopology),
	})

	assert.NoError(t, sync.Sync())
	patches := nodePatches(client)
	assert.Len(t, patches, 1)

	publish := string(patches[0].GetPatch())
	assert.Contains(t, publish, utils.AnnotationRdmaTopo)
	assert.Contains(t, publish, "ASW-001")
	assert.Contains(t, publish, utils.NodeLabelHyperNodeID)
	assert.True(t, rdma.healthy)

	// an unchanged snapshot must not patch the node again
	assert.NoError(t, sync.Sync())
	assert.Len(t, nodePatches(client), 1)

	// xdm withdraws the topology: devices stay, the topology key is gone
	dm.deviceMeta.Replace(map[int]*xpugrpc.Device{
		0: topoSyncTestDevice("dev0", "0", ""),
	})

	assert.NoError(t, sync.Sync())
	patches = nodePatches(client)
	assert.Len(t, patches, 2)

	withdraw := string(patches[1].GetPatch())
	assert.Equal(t, types.MergePatchType, patches[1].GetPatchType())
	// a null value is what makes the json merge patch delete the owned keys
	assert.Contains(t, withdraw, `"`+utils.AnnotationRdmaTopo+`":null`)
	assert.Contains(t, withdraw, `"`+utils.NodeLabelLeafSwitchID+`":null`)
	assert.Contains(t, withdraw, `"`+utils.NodeLabelNetworkPoD+`":null`)
	assert.Contains(t, withdraw, `"`+utils.NodeLabelHyperNodeID+`":null`)
	assert.NotContains(t, withdraw, "ASW-001")
	// the device annotation is still published, it carries no topology now
	assert.Contains(t, withdraw, utils.AnnotationDevice)
	assert.False(t, rdma.healthy)

	// the topology comes back, the restore must be pushed even though the
	// content equals the published one
	dm.deviceMeta.Replace(map[int]*xpugrpc.Device{
		0: topoSyncTestDevice("dev0", "0", topoSyncTestTopology),
	})

	assert.NoError(t, sync.Sync())
	patches = nodePatches(client)
	assert.Len(t, patches, 3)
	assert.Contains(t, string(patches[2].GetPatch()), "ASW-001")
	assert.True(t, rdma.healthy)
}

// topoSyncLabelOnlyTopology still reports topology, but no pcie switch level: no
// rdma device can be mounted behind a switch that does not exist.
const topoSyncLabelOnlyTopology = `{"levels":[{"level":21,"name":"leafSwitch","topo21":"ASW-002"}]}`

// A transition that failed halfway must not be remembered as applied: the
// withdrawal flips the rdma health before patching the node, so a skipped retry
// would leave the resource withdrawn forever once the topology comes back.
func TestTopoSyncRestoresAfterFailedWithdrawal(t *testing.T) {
	dm, client, rdma := newTopoSyncTestManager(t)
	sync := NewTopoSync(dm)

	dm.deviceMeta.Replace(map[int]*xpugrpc.Device{
		0: topoSyncTestDevice("dev0", "0", topoSyncTestTopology),
	})
	assert.NoError(t, sync.Sync())
	assert.True(t, rdma.healthy)

	// the node patch fails once, the withdrawal already marked the resource
	// unhealthy, so the recorded target must stay retryable
	var failed bool
	applied := make([]string, 0, 1)
	client.PrependReactor("patch", "nodes", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if !failed {
			failed = true
			return true, nil, errors.New("node patch failed")
		}

		applied = append(applied, string(action.(k8stesting.PatchAction).GetPatch()))
		return false, nil, nil
	})

	dm.deviceMeta.Replace(map[int]*xpugrpc.Device{
		0: topoSyncTestDevice("dev0", "0", ""),
	})
	assert.Error(t, sync.Sync())
	assert.False(t, rdma.healthy)
	assert.Empty(t, applied)

	// the topology comes back equal to the one applied before the failure: the
	// retry still has to publish it and restore the rdma resource
	dm.deviceMeta.Replace(map[int]*xpugrpc.Device{
		0: topoSyncTestDevice("dev0", "0", topoSyncTestTopology),
	})
	assert.NoError(t, sync.Sync())
	assert.True(t, rdma.healthy)
	assert.Len(t, applied, 1)
	assert.Contains(t, applied[0], "ASW-001")
}

// A snapshot that only keeps a node identifier withdraws the pcie tree and the
// labels that are gone, instead of leaving the stale rdma annotation behind.
func TestTopoSyncWithdrawsPartialTopology(t *testing.T) {
	dm, client, rdma := newTopoSyncTestManager(t)
	sync := NewTopoSync(dm)

	dm.deviceMeta.Replace(map[int]*xpugrpc.Device{
		0: topoSyncTestDevice("dev0", "0", topoSyncTestTopology),
	})
	assert.NoError(t, sync.Sync())
	assert.True(t, rdma.healthy)

	dm.deviceMeta.Replace(map[int]*xpugrpc.Device{
		0: topoSyncTestDevice("dev0", "0", topoSyncLabelOnlyTopology),
	})

	assert.NoError(t, sync.Sync())
	patches := nodePatches(client)
	assert.Len(t, patches, 2)

	partial := string(patches[1].GetPatch())
	// the pcie tree and the dropped labels must be deleted from the node
	assert.Contains(t, partial, `"`+utils.AnnotationRdmaTopo+`":null`)
	assert.Contains(t, partial, `"`+utils.NodeLabelHyperNodeID+`":null`)
	assert.Contains(t, partial, `"`+utils.NodeLabelNetworkPoD+`":null`)
	// the identifier xdm still reports is updated, not deleted
	assert.Contains(t, partial, `"`+utils.NodeLabelLeafSwitchID+`":"ASW-002"`)
	assert.NotContains(t, partial, "ASW-001")
	// no pcie switch means no rdma device can be mounted behind one
	assert.False(t, rdma.healthy)
}

// An NVML failure must survive the next annotation build: the xdm snapshot keeps
// reporting the device as healthy, so both views are merged when the node
// annotation is built.
func TestTopoSyncKeepsNvmlUnhealthy(t *testing.T) {
	dm, client, _ := newTopoSyncTestManager(t)
	sync := NewTopoSync(dm)

	dm.deviceMeta.Replace(map[int]*xpugrpc.Device{
		0: topoSyncTestDevice("dev0", "0", topoSyncTestTopology),
	})
	assert.NoError(t, sync.Sync())

	// NVML reports the device as failed, xdm still reports it as healthy
	dm.markUnhealthy(0)
	assert.NoError(t, sync.Sync())

	patches := nodePatches(client)
	// the device annotation is a json string nested in the patch, hence the
	// escaped quotes
	health := string(patches[len(patches)-1].GetPatch())
	assert.Contains(t, health, `\"health\":\"`+pluginapi.Unhealthy+`\"`)
	assert.NotContains(t, health, `\"health\":\"`+pluginapi.Healthy+`\"`)
}

// A health report says nothing about the topology: republishing it must touch
// the device annotation only, otherwise a transient translate failure would wipe
// the published pcie tree and the node labels through a health event.
func TestPatchDeviceAnnotationTouchesOnlyDevices(t *testing.T) {
	dm, client, _ := newTopoSyncTestManager(t)
	sync := NewTopoSync(dm)

	dm.deviceMeta.Replace(map[int]*xpugrpc.Device{
		0: topoSyncTestDevice("dev0", "0", topoSyncTestTopology),
	})
	assert.NoError(t, sync.Sync())
	assert.Len(t, nodePatches(client), 1)

	dm.markUnhealthy(0)
	assert.NoError(t, dm.patchDeviceAnnotation())

	patches := nodePatches(client)
	assert.Len(t, patches, 2)

	patch := string(patches[1].GetPatch())
	assert.Contains(t, patch, utils.AnnotationDevice)
	assert.Contains(t, patch, `\"health\":\"`+pluginapi.Unhealthy+`\"`)
	// the pcie tree and the node labels belong to the topology worker
	assert.NotContains(t, patch, utils.AnnotationRdmaTopo)
	assert.NotContains(t, patch, utils.NodeLabelLeafSwitchID)
	assert.NotContains(t, patch, utils.NodeLabelNetworkPoD)
	assert.NotContains(t, patch, utils.NodeLabelHyperNodeID)
}
