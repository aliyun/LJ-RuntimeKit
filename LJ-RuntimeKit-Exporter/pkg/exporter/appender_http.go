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
	"github.com/lj-runtimekit/exporter/pkg/common"

	"github.com/sirupsen/logrus"
)

// httpAppender delivers formatted metrics via an HTTP channel.
type httpAppender struct {
	config      *Config
	formatter   FormatterInterface
	metricsChan chan common.FormattedData
}

func newHttpAppender(formatter FormatterInterface, c *Config, httpChan chan common.FormattedData) (*httpAppender, error) {
	return &httpAppender{
		config:      c,
		formatter:   formatter,
		metricsChan: httpChan,
	}, nil
}

func (h *httpAppender) GetConfig() interface{} {
	return nil
}

func (h *httpAppender) doMetricReportCollect(data interface{}) {
	h.metricsChan <- common.FormattedData{
		Type: common.CollectDataType,
		Data: h.formatter.Format(common.CollectDataType, data),
	}
}

func (h *httpAppender) doMetricReportDetect(data interface{}) {
	h.metricsChan <- common.FormattedData{
		Type: common.DetectDataType,
		Data: h.formatter.Format(common.DetectDataType, data),
	}
}

func (h *httpAppender) DoMetricReport(dataType int32, data interface{}) {
	switch dataType {
	case common.CollectDataType:
		h.doMetricReportCollect(data)
	case common.DetectDataType:
		h.doMetricReportDetect(data)
	default:
		logrus.Warning("Unsupported data type: ", dataType)
	}
}

func (h *httpAppender) DoEventReport(int32, interface{}) {

}
