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

package mig

import (
	"context"
	"fmt"

	ampcache "github.com/lj-runtimekit/scheduler/pkg/cache"
	"github.com/lj-runtimekit/scheduler/pkg/mig/migDevice"
	"github.com/lj-runtimekit/scheduler/pkg/utils"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodename"
)

var _ framework.FilterPlugin = &MigPlugin{}
var _ framework.ReservePlugin = &MigPlugin{}

const (
	Name = "Mig"
)

// MigPlugin implements Filter and Reserve for MIG (Multi-Instance GPU) tasks.
// It handles both CI (Compute Instance) and GI (GPU Instance) modes.
type MigPlugin struct {
	fh     framework.Handle
	plugin migDevice.VirtualMode
}

func (p *MigPlugin) Name() string {
	return Name
}

func (p *MigPlugin) Filter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	klog.V(5).Infof("start mig plugin filter process for pod %s in node %s", pod.Name, nodeInfo.Node().Name)

	if !utils.IsMigPodTask(pod) {
		return framework.NewStatus(framework.Success, "pod is not mig task")
	}

	_, _, standardReq, errs := ampcache.GetPreFilterStateData(state)
	if errs != nil {
		klog.Errorf("pod %s/%s in Reserve stage getPreFilterStateData failed due to %v", pod.Namespace, pod.Name, errs)
		return framework.AsStatus(errs)
	}

	if standardReq.NormalGpuReqCnt > 0 {
		return framework.NewStatus(framework.Success, "pod is total card task")
	}

	node := nodeInfo.Node()
	if node == nil {
		return framework.NewStatus(framework.Error, "node not found")
	}
	var newFunc migDevice.NewFactory
	var err error
	switch {
	case utils.IsGiTask(pod):
		newFunc, err = migDevice.GetMigDeviceModePlugin(string(utils.TypeMigGi))
	case utils.IsCiTask(pod):
		newFunc, err = migDevice.GetMigDeviceModePlugin(string(utils.TypeMigCi))
	default:
		return framework.NewStatus(framework.Success, fmt.Sprintf("pod %s/%s is not gi mode or ci mode", pod.Namespace, pod.Name))
	}

	if err != nil {
		klog.Errorf("pod %s/%s in filter stage get mig device mode failed due to %v", pod.Namespace, pod.Name, err)
		return framework.NewStatus(framework.Error, err.Error())
	}

	clusterDevices, clusterNodeMigDevice, _, err := ampcache.GetPreFilterStateData(state)
	if err != nil {
		klog.Errorf("pod %s/%s in mig filter stage getPreFilterStateData failed due to %v", pod.Namespace, pod.Name, err)
		return framework.AsStatus(err)
	}

	migDevices, ok := clusterNodeMigDevice[node.Name]
	if !ok {
		klog.Errorf("pod %s/%s in mig filter stage get node %s mig virtual device failed due to %v", pod.Namespace, pod.Name, node.Name, err)
		migDevices = &ampcache.GpuDevice{
			Devices: make(map[int]*ampcache.Device),
		}
	}

	availableDevices, ok := clusterDevices[node.Name]
	if !ok {
		return framework.NewStatus(framework.Error, fmt.Sprintf("can not found node %s in normal cluster cache", node.Name))
	}

	p.plugin = newFunc(migDevices)
	if p.plugin.AssumePod(availableDevices, pod, nodeInfo) {
		klog.V(5).Infof("In mig filer process pod %s/%s assume on node %s success!", pod.Namespace, pod.Name, nodename.Name)
		return framework.NewStatus(framework.Success, "")
	}

	klog.V(5).Infof("In mig filer process pod %s/%s assume on node %s failed!", pod.Namespace, pod.Name, nodename.Name)
	return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("assume pod %s/%s failed", pod.Namespace, pod.Name))
}

func (p *MigPlugin) Reserve(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) *framework.Status {
	klog.V(5).Infof("start mig plugin Reserve process for pod %s/%s in node %s", pod.Namespace, pod.Name, nodeName)

	if !utils.IsMigPodTask(pod) {
		return framework.NewStatus(framework.Success, "pod is not mig task")
	}

	_, _, standardReq, errs := ampcache.GetPreFilterStateData(state)
	if errs != nil {
		klog.Errorf("pod %s/%s in Reserve stage getPreFilterStateData failed due to %v", pod.Namespace, pod.Name, errs)
		return framework.AsStatus(errs)
	}

	if standardReq.NormalGpuReqCnt > 0 {
		return framework.NewStatus(framework.Success, "pod is total card task")
	}

	clusterDevices, _, _, err := ampcache.GetPreFilterStateData(state)
	if err != nil {
		klog.Errorf("pod %s/%s in mig filter stage getPreFilterStateData failed due to %v", pod.Namespace, pod.Name, err)
		return framework.AsStatus(err)
	}

	availableDevices, ok := clusterDevices[nodeName]
	if !ok {
		return framework.NewStatus(framework.Error, fmt.Sprintf("can not found node %s in normal cluster cache", nodeName))
	}

	devId, mask, start, size, found := p.plugin.AllocatePod(availableDevices, pod)
	if !found {
		return framework.NewStatus(framework.Unschedulable, "cannot found the candidate dev")
	}
	klog.V(4).Infof("reserve for pod %s/%s with mode %s found devId %d mask %07b, start %d, size %d", pod.Namespace, pod.Name, p.plugin.Name(), devId, mask, start, size)
	patchObj, err := patchPodAssigned(pod, p.plugin.Name(), mask, start, size, devId)
	if err != nil {
		return framework.AsStatus(err)
	}
	var newPod *v1.Pod
	if err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		var err error
		newPod, err = p.fh.ClientSet().CoreV1().Pods(pod.Namespace).Patch(context.TODO(), pod.Name, types.StrategicMergePatchType, patchObj, metav1.PatchOptions{})
		return err
	}); err != nil {
		klog.Errorf("failed to patch pod %s/%s error %s", pod.Namespace, pod.Name, err.Error())
		return framework.NewStatus(framework.Error, fmt.Sprintf("patch pod %s/%s failed due to %v", pod.Namespace, pod.Name, err))
	}

	pod.Annotations = newPod.Annotations
	return framework.NewStatus(framework.Success, "")
}

// Unreserve is a no-op for the Mig plugin.
func (p *MigPlugin) Unreserve(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) {
}

// New initializes and returns a new MigPlugin.
func New(obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	klog.V(5).Infof("begin to new device plugin")

	migDevicePlugin := new(MigPlugin)
	migDevicePlugin.fh = handle

	return migDevicePlugin, nil
}
