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

package activemonitoring

import (
	"context"
	"fmt"

	ampcache "github.com/lj-runtimekit/scheduler/pkg/cache"
	"github.com/lj-runtimekit/scheduler/pkg/utils"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

const Name = "ActiveMonitoring"

// penaltyPerTestPod is the Score deduction per running detection Pod on a node,
// so business Pods prefer nodes with fewer detection Pods.
const penaltyPerTestPod = int64(10)

var _ framework.FilterPlugin = &Plugin{}
var _ framework.ScorePlugin = &Plugin{}

// Plugin pins detection Pods onto their target minor and scores nodes for
// business Pods by detection-Pod density.
type Plugin struct {
	fh framework.Handle
}

func New(_ runtime.Object, h framework.Handle) (framework.Plugin, error) {
	klog.V(6).Infof("begin to new activemonitoring plugin")
	return &Plugin{fh: h}, nil
}

func (p *Plugin) Name() string { return Name }

// Filter checks, for a detection Pod, that its target minor on the node is idle.
func (p *Plugin) Filter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	if !utils.IsActiveMonitoringPod(pod) {
		return framework.NewStatus(framework.Success, "not a activemonitoring pod")
	}
	node := nodeInfo.Node()
	if node == nil {
		return framework.NewStatus(framework.Error, "node not found")
	}

	minor, err := utils.GetActiveMonitoringTargetMinor(pod)
	if err != nil {
		return framework.NewStatus(framework.Unschedulable, "missing target minor annotation")
	}

	clusterDevices, _, _, err := ampcache.GetPreFilterStateData(state)
	if err != nil {
		klog.Errorf("activemonitoring pod %s/%s getPreFilterStateData failed: %v", pod.Namespace, pod.Name, err)
		return framework.AsStatus(err)
	}
	devs, ok := clusterDevices[node.Name]
	if !ok {
		return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("node %s not in cache", node.Name))
	}
	dev, ok := devs[minor]
	if !ok {
		return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("gpu %d not found on node %s", minor, node.Name))
	}
	if !dev.IdleDevice() {
		return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("target gpu %d is busy on node %s", minor, node.Name))
	}
	if dev.HasTestPod() {
		return framework.NewStatus(framework.Unschedulable, fmt.Sprintf("target gpu %d is occupied by another detection pod on node %s", minor, node.Name))
	}
	return framework.NewStatus(framework.Success, "")
}

// Score is the preference layer for business Pods: a node with no detection Pod
// scores MaxNodeScore, decreasing by penaltyPerTestPod per running detection Pod.
func (p *Plugin) Score(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	if utils.IsActiveMonitoringPod(pod) {
		return 0, nil
	}
	nodeInfo, err := p.fh.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil {
		klog.V(5).Infof("activemonitoring score: get nodeInfo %s failed: %v", nodeName, err)
		return 0, nil
	}
	testPods := 0
	for _, pi := range nodeInfo.Pods {
		if utils.IsActiveMonitoringPod(pi.Pod) && !utils.NotRunning(pi.Pod) {
			testPods++
		}
	}
	return scoreForTestPodCount(testPods), nil
}

func scoreForTestPodCount(testPods int) int64 {
	score := max(framework.MaxNodeScore-int64(testPods)*penaltyPerTestPod, 0)
	return score
}

func (p *Plugin) ScoreExtensions() framework.ScoreExtensions {
	return nil
}
