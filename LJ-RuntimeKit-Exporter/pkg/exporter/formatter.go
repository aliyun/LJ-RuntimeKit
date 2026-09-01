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
	"encoding/json"
	"fmt"
	"strings"

	"github.com/lj-runtimekit/exporter/pkg/common"

	"github.com/sirupsen/logrus"
)

// FormatterInterface defines how metric data is formatted for output.
type FormatterInterface interface {
	Format(dataType int32, data interface{}) string
	GetPodLabel(PodAttrType) ([]string, bool)
	GetLabelByID(LabelID) ([]string, bool)
}

type fmtConfig struct {
	Labels map[LabelID][]string `json:"labels"`
}

type LabelID int

const (
	// host
	LabelIdInvalid  LabelID = iota
	LabelIdHostName LabelID = 1
	// k8s node
	LabelIdNodeName LabelID = 4
	LabelIdNodeIP   LabelID = 5
	// k8s pod
	LabelIdPodName      LabelID = 6
	LabelIdPodNamespace LabelID = 7
	LabelIdPodPhase     LabelID = 8
	// k8s container
	LabelIdContainerName LabelID = 9
	LabelIdContainerID   LabelID = 10
	// GPU
	LabelIdGPUName            LabelID = 11
	LabelIdGPUIndex           LabelID = 12
	LabelIdGPUUUID            LabelID = 13
	LabelIdGPUMigGID          LabelID = 20
	LabelIdGPUMigCID          LabelID = 21
	LabelIdGPUMigProfile      LabelID = 22
	LabelIdGPUIndexWithVendor LabelID = 24
	// process
	LabelIdProcessID LabelID = 26
	// other
	LabelIdDetail LabelID = 27
)

type FormatItem struct {
	counter *common.Counter
	Labels  map[string]string
	Value   string
}

const (
	defaultLabelHostname      = "Hostname"
	defaultLabelNodeName      = "NodeName"
	defaultLabelNodeIP        = "NodeIP"
	defaultLabelPodName       = "pod"
	defaultLabelPodNamespace  = "namespace"
	defaultLabelPodPhase      = "phase"
	defaultLabelContainerName = "container"
	defaultLabelContainerId   = "containerId"
	defaultLabelGPUIndex      = "gpu"
	defaultLabelGPUName       = "modelName"
	defaultLabelGPUUUID       = "UUID"
	defaultLabelGPUMigProfile = "GPU_I_PROFILE"
	defaultLabelGPUMigGID     = "GPU_I_ID"
	defaultLabelGPUMigCID     = "GPU_CI_ID"
	defaultLabelGPUMigUUID    = "mig_uuid"
	defaultLabelGPUDevice     = "device"
	defaultLabelProcessID     = "pid"
	defaultLabelDetail        = "detail"
)

// TODO: one labelID corresponding multiple labels
// label white list
var (
	labelMapProm = map[LabelID][]string{
		LabelIdHostName:           {defaultLabelHostname},
		LabelIdNodeName:           {defaultLabelNodeName},
		LabelIdNodeIP:             {defaultLabelNodeIP},
		LabelIdPodName:            {defaultLabelPodName},
		LabelIdPodNamespace:       {defaultLabelPodNamespace},
		LabelIdContainerName:      {defaultLabelContainerName},
		LabelIdGPUIndex:           {defaultLabelGPUIndex},
		LabelIdGPUName:            {defaultLabelGPUName},
		LabelIdGPUUUID:            {defaultLabelGPUUUID},
		LabelIdGPUMigProfile:      {defaultLabelGPUMigProfile},
		LabelIdGPUMigGID:          {defaultLabelGPUMigGID},
		LabelIdGPUMigCID:          {defaultLabelGPUMigCID},
		LabelIdGPUIndexWithVendor: {defaultLabelGPUDevice},
		LabelIdProcessID:          {defaultLabelProcessID},
		LabelIdDetail:             {defaultLabelDetail},
	}

	labelMapEGPU = map[LabelID][]string{
		LabelIdHostName:           {defaultLabelHostname},
		LabelIdNodeName:           {defaultLabelNodeName},
		LabelIdNodeIP:             {defaultLabelNodeIP},
		LabelIdPodName:            {defaultLabelPodName},
		LabelIdPodNamespace:       {defaultLabelPodNamespace},
		LabelIdPodPhase:           {defaultLabelPodPhase},
		LabelIdContainerName:      {"container_name"},
		LabelIdContainerID:        {defaultLabelContainerId},
		LabelIdGPUIndex:           {defaultLabelGPUIndex},
		LabelIdGPUName:            {defaultLabelGPUName},
		LabelIdGPUUUID:            {defaultLabelGPUUUID},
		LabelIdGPUMigProfile:      {defaultLabelGPUMigProfile},
		LabelIdGPUMigGID:          {defaultLabelGPUMigGID},
		LabelIdGPUMigCID:          {defaultLabelGPUMigCID},
		LabelIdGPUIndexWithVendor: {defaultLabelGPUDevice},
		LabelIdProcessID:          {defaultLabelProcessID},
		LabelIdDetail:             {defaultLabelDetail},
	}
)

// FormatterFactory creates a new FormatterInterface based on the specified format type.
func FormatterFactory(fmtType string, fmtConfig string, config *Config, info *SystemInfo, counters []common.Counter) (FormatterInterface, error) {
	fmtCfg, err := parseFmtConfigJson(fmtConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to parse formatter config: %v", err)
	}
	formatters := map[string]func() FormatterInterface{
		OutputFmtProm: func() FormatterInterface {
			return newPrometheusFormatter(config, info, counters, fmtCfg, labelMapProm)
		},
		OutputFmtKubergpu: func() FormatterInterface {
			return newEgpuFormatter(config, info, counters, fmtCfg, labelMapEGPU)
		},
	}

	if formatterFunc, ok := formatters[fmtType]; ok {
		return formatterFunc(), nil
	}

	logrus.Warningf("unknown format type: %s", config.ExporterOutputFormat)
	return nil, fmt.Errorf("unknown format type: %s", config.ExporterOutputFormat)
}

func parseFmtConfigJson(configJson string) (fmtConfig, error) {
	// default config
	var cfg fmtConfig
	if configJson == "" || configJson == "{}" {
		return cfg, nil
	}
	err := json.Unmarshal([]byte(configJson), &cfg)
	if err != nil {
		return cfg, fmt.Errorf("failed to decode formatter config (%s):%v", configJson, err)
	}
	return cfg, nil
}

func setLabelIfNotEmpty(labels map[string]string, label string, value string) {
	if len(value) > 0 && value != NanValue {
		labels[label] = value
	}
}

func setLabelIfNonZero(labels map[string]string, label string, value int64) {
	if value != 0 {
		labels[label] = fmt.Sprintf("%d", value)
	}
}

// setLabelIfNA only writes the label when the value is N/A; empty values must not be written.
func setLabelIfNA(labels map[string]string, label string, value string) {
	if value == NanValue {
		labels[label] = NanValue
	}
}

// TODO: no transformation for reducing cpu & memory cost
func transformMetric2fmtItem(metric *common.MetricItem, info *SystemInfo, labelMap map[LabelID][]string) (FormatItem, error) {
	if metric == nil {
		return FormatItem{}, fmt.Errorf("invalid argument: metric is nil")
	}
	item := FormatItem{
		counter: metric.Counter,
		Labels:  make(map[string]string),
		Value:   metric.Value,
	}
	if item.counter != nil && item.counter.MetricName == "node_accelerator_total" {
		item.Value = "1"
	}
	for k, v := range metric.RcvLabels {
		item.Labels[k] = v
	}
	for labelID, labels := range labelMap { // e.g. labelMapArms
		for _, label := range labels { // e.g. LabelIdHyperNodeId:     {"hyper_node_id"},
			if labelInMetric, exist := metric.Labels[label]; exist { // use metric label if exist
				setLabelIfNotEmpty(item.Labels, label, labelInMetric)
				continue
			}
			switch labelID {
			case LabelIdProcessID:
				setLabelIfNonZero(item.Labels, label, int64(metric.Pid))
			case LabelIdGPUIndex:
				setLabelIfNotEmpty(item.Labels, label, metric.ID)
			case LabelIdGPUIndexWithVendor:
				if metric.ID == NanValue {
					break
				}
				if strings.Contains(metric.ModelName, "AMD") {
					setLabelIfNotEmpty(item.Labels, label, "amd"+metric.ID)
				} else {
					setLabelIfNotEmpty(item.Labels, label, "nvidia"+metric.ID)
				}
			case LabelIdGPUUUID:
				setLabelIfNotEmpty(item.Labels, label, metric.UUID)
			case LabelIdGPUName:
				setLabelIfNotEmpty(item.Labels, label, metric.ModelName)
			case LabelIdGPUMigProfile:
				setLabelIfNotEmpty(item.Labels, label, metric.MigProfile)
			case LabelIdGPUMigGID:
				setLabelIfNotEmpty(item.Labels, label, metric.GPUInstanceID)
			case LabelIdGPUMigCID:
				setLabelIfNotEmpty(item.Labels, label, metric.ComputeId)
			case LabelIdDetail:
				setLabelIfNotEmpty(item.Labels, label, metric.Message)
			case LabelIdHostName:
				setLabelIfNotEmpty(item.Labels, label, metric.HostName)
			case LabelIdNodeName:
				setLabelIfNotEmpty(item.Labels, label, metric.NodeName)
			case LabelIdNodeIP:
				setLabelIfNotEmpty(item.Labels, label, metric.NodeIP)
			case LabelIdContainerID:
				setLabelIfNotEmpty(item.Labels, label, metric.Cid)
			default:
				break
			}
		}
	}

	return item, nil
}

func podAttr2LabelID(attr PodAttrType) LabelID {
	switch attr {
	case PodAttrName:
		return LabelIdPodName
	case PodAttrNamespace:
		return LabelIdPodNamespace
	case PodAttrContainer:
		return LabelIdContainerName
	case PodAttrContainerID:
		return LabelIdContainerID
	case PodAttrPhase:
		return LabelIdPodPhase
	default:
		return LabelIdInvalid
	}
}
