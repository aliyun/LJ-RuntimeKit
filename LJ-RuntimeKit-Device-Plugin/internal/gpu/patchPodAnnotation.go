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
	"encoding/json"
	"fmt"
	"strings"
	"time"

	xpugrpc "github.com/lj-runtimekit/device-plugin/internal/grpc"
	"github.com/lj-runtimekit/device-plugin/internal/utils"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
)

func (dm *DeviceManager) GetPodUsed(client xpugrpc.XpuDeviceManagerServiceClient) ([]*xpugrpc.Resource, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	req := &xpugrpc.ListInitialEnvRequest{}

	resp, err := client.ListInitialEnv(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to list initial env: %v", err)
	}

	if resp == nil {
		return nil, fmt.Errorf("received nil list init env response from xdm server")
	}

	if resp.Status != StatusOK {
		return nil, fmt.Errorf("list init env received non-OK status from xdm server %v %s", resp.Status, resp.Message)
	}

	return resp.Resources, nil
}

func (dm *DeviceManager) addPodAnnotation(podUsed *xpugrpc.Resource) error {

	klog.Infof("xdm patchPodAnnotation podUsed %s/%s anno %v", podUsed.NameSpace, podUsed.Name, podUsed.Annotations)
	patchData := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": podUsed.Annotations,
		},
	}

	patchBytes, err := json.Marshal(patchData)
	if err != nil {
		return fmt.Errorf("failed to marshal patch data for pod %s/%s: %v", podUsed.NameSpace, podUsed.Name, err)
	}

	if err := utils.RetryOnConflictOrTooManyRequests(func() error {
		var applyErr error
		_, applyErr = dm.k8sClient.CoreV1().Pods(podUsed.NameSpace).Patch(
			context.Background(),
			podUsed.Name,
			types.StrategicMergePatchType,
			patchBytes,
			metav1.PatchOptions{},
		)
		return applyErr
	}); err != nil {
		return fmt.Errorf("failed to patch pod %s/%s: %v", podUsed.NameSpace, podUsed.Name, err)
	}

	return nil
}

func (dm *DeviceManager) patchPodsAnnotation(usedResource []*xpugrpc.Resource) error {
	var errs []string
	for _, resource := range usedResource {
		if err := dm.addPodAnnotation(resource); err != nil {
			klog.Errorf("patchPodAnnotation resource %v failed: %v", resource, err)
			errs = append(errs, err.Error())
		}
	}

	if len(errs) != 0 {
		return fmt.Errorf("patchPodsAnnotation failed: %s", strings.Join(errs, ","))
	}

	return nil
}
