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
	"fmt"
	"strings"

	"github.com/lj-runtimekit/exporter/pkg/common"

	"github.com/sirupsen/logrus"
)

// BasicPodMapper enriches metrics with pod information from Kubernetes.
type BasicPodMapper struct {
	config    *Config
	gpuInfo   *SystemGpuInfo
	formatter FormatterInterface
	podClient PodInfoInterface
}

// NewPodMapper creates a new PodMapper based on the config.
func NewPodMapper(config *Config, formatter FormatterInterface, info *SystemGpuInfo) (PodMapper, error) {
	return NewBasicPodMapper(config, formatter, info)
}

func NewBasicPodMapper(config *Config, formatter FormatterInterface, info *SystemGpuInfo) (*BasicPodMapper, error) {
	var podCli PodInfoInterface

	if config.ExporterPodCollectorType == PodCollectTypeKubeletUds {
		podCollectorUds, err := NewPodInfoKubeletResource(config)
		if err != nil {
			return nil, err
		}
		podCli = podCollectorUds
	} else if config.ExporterPodCollectorType == PodCollectTypeKubeletHttp {
		podCollectorHttp, err := NewPodInfoKubeletApiServer(config, info)
		if err != nil {
			return nil, err
		}
		podCli = podCollectorHttp
	} else if config.ExporterPodCollectorType == PodCollectTypeNone {
		return nil, fmt.Errorf("no need to open podMapper")
	} else {
		return nil, fmt.Errorf("unknown pod collect type: %s", config.ExporterPodCollectorType)
	}

	return &BasicPodMapper{
		podClient: podCli,
		formatter: formatter,
		config:    config,
		gpuInfo:   info,
	}, nil
}

func (m *BasicPodMapper) Process(metrics []common.MetricItem) ([]common.MetricItem, error) {
	pods, err := m.podClient.GetGpuPodInfo()
	if err != nil {
		return nil, err
	}

	podMaps := make(map[string][]PodInfo)
	for _, pod := range pods {
		podMaps[pod.DeviceId] = append(podMaps[pod.DeviceId], pod)
	}

	for i, metric := range metrics {
		if !metric.Counter.PodMap {
			continue
		}

		var deviceId string
		if metric.MigUUID != "" {
			deviceId = metric.MigUUID
		} else if metric.UUID != "" {
			deviceId = metric.UUID
		} else {
			logrus.Debugf("device id not found in metrics %s", metric.Counter.MetricName)
			continue
		}

		devicePods, ok := podMaps[deviceId]
		if !ok || len(devicePods) == 0 {
			logrus.Debugf("pod not founds for device %s", deviceId)
			continue
		}

		if metrics[i].Labels == nil {
			metrics[i].Labels = make(map[string]string)
		}

		if metric.Egpu {
			m.handleEGPU(&metrics[i], devicePods)
		} else {
			m.handleNonEGPU(&metrics[i], devicePods)
		}
	}

	return metrics, nil
}

func (m *BasicPodMapper) handleEGPU(metric *common.MetricItem, pods []PodInfo) {
	for _, pod := range pods {
		if metric.Full || pod.Full || // Skip whole-card metrics for EGPU
			metric.Cid != pod.ContainerId { // EGPU metrics include container ID
			continue
		}

		for attr := PodAttrName; attr <= PodAttrMax; attr++ {
			if labels, ok := m.formatter.GetPodLabel(attr); ok {
				for _, label := range labels {
					metric.Labels[label] = pod.GetVal(attr)
				}
			}
		}

	}
}

func (m *BasicPodMapper) handleNonEGPU(metric *common.MetricItem, pods []PodInfo) {
	if len(pods) == 1 && // Only one pod per device supported
		(strings.HasPrefix(pods[0].DeviceId, "MIG") || pods[0].Full) {
		// Whole card or MIG: one metric per device
		for attr := PodAttrName; attr <= PodAttrMax; attr++ {
			if labels, ok := m.formatter.GetPodLabel(attr); ok {
				for _, label := range labels {
					metric.Labels[label] = pods[0].GetVal(attr)
				}
			}
		}
	}
}
