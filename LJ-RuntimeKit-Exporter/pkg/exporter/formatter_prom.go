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

	"github.com/sirupsen/logrus"
)

type prometheusFormatter struct {
	c    *Config
	info *SystemInfo
	//etraceFormatter FormatterInterface
	template *template.Template
	labelMap map[LabelID][]string
}

var formatTemplateProm = `
{{- range $counter, $metrics := . -}}
# HELP {{ $counter.MetricName }} {{ $counter.Help }}
# TYPE {{ $counter.MetricName }} {{ $counter.PromType }}
{{- range $metric := $metrics }}
{{ $counter.MetricName }}{
{{- $firstLabel := true -}}
{{- range $k, $v := $metric.Labels -}}
    {{- if not $firstLabel -}},{{ end }}
    {{- $k }}="{{ $v }}"
    {{- $firstLabel = false -}}
{{- end -}}
} {{ $metric.Value -}}
{{- end }}
{{ end }}`

func newPrometheusFormatter(config *Config, info *SystemInfo, counters []common.Counter, fmtCfg fmtConfig, defaultLabels map[LabelID][]string) *prometheusFormatter {
	labels := make(map[LabelID][]string)
	if len(fmtCfg.Labels) > 0 {
		labels = fmtCfg.Labels
	} else {
		logrus.Infof("using default prometheus fommat labels")
		labels = defaultLabels
	}
	return &prometheusFormatter{
		c:        config,
		info:     info,
		template: template.Must(template.New("collect").Parse(formatTemplateProm)),
		labelMap: labels,
	}
}

func (p *prometheusFormatter) formatMetrics(metrics []common.MetricItem) string {
	var resComm bytes.Buffer
	fmtItemMap := make(map[*common.Counter][]FormatItem)
	for _, metric := range metrics {
		item, err := transformMetric2fmtItem(&metric, p.info, p.labelMap)
		if err != nil {
			logrus.Warning("Format metric err :", err)
			continue
		}
		// Uses pointer address as key; same content with different counter addresses will be formatted multiple times
		fmtItemMap[metric.Counter] = append(fmtItemMap[metric.Counter], item)
	}

	if err := p.template.Execute(&resComm, fmtItemMap); err != nil {
		logrus.Warning("Format detect data err :", err)
	}

	return resComm.String()
}

func (p *prometheusFormatter) Format(dataType int32, data interface{}) string {
	switch dataType {
	case common.CollectDataType, common.DetectDataType:
		return p.formatMetrics(data.([]common.MetricItem))
	default:
		logrus.Warning("Unsupported data type: ", dataType)
	}

	logrus.Warning("Error data type :", dataType)
	return ""
}

func (p *prometheusFormatter) GetPodLabel(attr PodAttrType) ([]string, bool) {
	return p.GetLabelByID(podAttr2LabelID(attr))
}

func (p *prometheusFormatter) GetLabelByID(lId LabelID) ([]string, bool) {
	attr, ok := p.labelMap[lId]
	return attr, ok
}
