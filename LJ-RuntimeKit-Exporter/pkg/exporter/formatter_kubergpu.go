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
	"bytes"
	"text/template"

	"github.com/lj-runtimekit/exporter/pkg/common"

	"github.com/NVIDIA/go-dcgm/pkg/dcgm"
	"github.com/sirupsen/logrus"
)

type egpuFormatter struct {
	*prometheusFormatter
	info        *SystemInfo
	counterMaps map[int]*common.Counter
}

func generateCounterMaps(counters []common.Counter) map[int]*common.Counter {
	cMaps := make(map[int]*common.Counter)
	for i, counter := range counters {
		if common.IsFieldIdInModule(common.AmperfModuEgpu, counter.FieldID) {
			cMaps[counter.FieldID] = &counters[i]
		}
	}

	return cMaps
}

func newEgpuFormatter(config *Config, info *SystemInfo, counters []common.Counter, fmtCfg fmtConfig, defaultLabels map[LabelID][]string) *egpuFormatter {
	labels := make(map[LabelID][]string)
	if len(fmtCfg.Labels) > 0 {
		labels = fmtCfg.Labels
	} else {
		logrus.Infof("using default egpu fommat labels")
		labels = defaultLabels
	}
	return &egpuFormatter{
		prometheusFormatter: &prometheusFormatter{
			c:        config,
			template: template.Must(template.New("collect").Parse(formatTemplateProm)),
			labelMap: labels,
		},
		info:        info,
		counterMaps: generateCounterMaps(counters),
	}
}

// TODO: formatter only perform format
func (e *egpuFormatter) copyEgpuWholeCardMetrics(metric common.MetricItem, fmtItemMap map[common.Counter][]FormatItem) {
	handleMetric := func(m common.MetricItem, toFieldID int) {
		if counter, ok := e.counterMaps[toFieldID]; ok {
			m.Counter = counter
			item, err := transformMetric2fmtItem(&m, e.info, e.labelMap)
			if err != nil {
				logrus.Warning("Transform metric to format item err :", err)
				return
			}
			fmtItemMap[*m.Counter] = append(fmtItemMap[*m.Counter], item)
		}
	}
	switch metric.Counter.FieldID {
	case common.FieldIdMemTotalByte:
		handleMetric(metric, common.FieldIdEgpuMemTotalByte)
	case common.FieldIdMemUsedByte:
		handleMetric(metric, common.FieldIdEgpuMemUsedByte)
	case dcgm.DCGM_FI_DEV_FB_USED:
		handleMetric(metric, common.FieldIdEgpuMemUsedMib)
	case dcgm.DCGM_FI_DEV_FB_FREE:
		handleMetric(metric, common.FieldIdEgpuMemFreeMib)
	case dcgm.DCGM_FI_DEV_FB_TOTAL:
		handleMetric(metric, common.FieldIdEgpuMemTotalMib)
	case dcgm.DCGM_FI_DEV_GPU_UTIL:
		handleMetric(metric, common.FieldIdEgpuDutyCycle)
		handleMetric(metric, common.FieldIdEgpuDutyCyclePct)
	}
}

// TODO: fix using map[*common.Counter][]FormatItem
func (e *egpuFormatter) formatMetrics(metrics []common.MetricItem) string {
	var res bytes.Buffer
	fmtItemMap := make(map[common.Counter][]FormatItem)
	for _, metric := range metrics {
		item, err := transformMetric2fmtItem(&metric, e.info, e.labelMap)
		if err != nil {
			logrus.Warning("Transform metric to format item err :", err)
			continue
		}
		fmtItemMap[*metric.Counter] = append(fmtItemMap[*metric.Counter], item)
		if !common.IsFieldIdInModule(common.AmperfModuEgpu, metric.Counter.FieldID) {
			// non-egpu metrics copy whole gpu pod
			podNameLabels, ok := e.GetPodLabel(PodAttrName)
			if !ok || metric.Labels == nil {
				continue
			}
			for _, podNameLabel := range podNameLabels {
				if len(metric.Labels[podNameLabel]) > 0 { // has pod name label
					e.copyEgpuWholeCardMetrics(metric, fmtItemMap)
					break
				}
			}
		}
	}

	if err := e.template.Execute(&res, fmtItemMap); err != nil {
		logrus.Warning("Format collect data err :", err)
		return ""
	}

	return res.String()
}

func (e *egpuFormatter) Format(dataType int32, data interface{}) string {
	switch dataType {
	case common.DetectDataType, common.CollectDataType:
		return e.formatMetrics(data.([]common.MetricItem))
	}
	logrus.Warning("Error data type :", dataType)
	return ""
}
