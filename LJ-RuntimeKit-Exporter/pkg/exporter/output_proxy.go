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
)

// OutputProxy coordinates formatters and appenders for metric output.
type OutputProxy struct {
	config     *OutputProxyConfig
	formatters []FormatterInterface
	appenders  []AppenderInterface
}

// NewOutputProxy creates a new OutputProxy with the given config, system info, and counters.
func NewOutputProxy(proxyCfg *OutputProxyConfig, info *SystemInfo, counters []common.Counter) (*OutputProxy, error) {
	proxy := &OutputProxy{
		config:     proxyCfg,
		formatters: make([]FormatterInterface, 0),
		appenders:  make([]AppenderInterface, 0),
	}

	formatter, err := FormatterFactory(proxyCfg.config.ExporterOutputFormat, "", proxyCfg.config, info, counters)
	if err != nil {
		return nil, err
	}

	appender, err := AppenderFactory(proxyCfg.config.ExporterOutputType, "", formatter, proxyCfg, info, counters)
	if err != nil {
		return nil, err
	}

	proxy.formatters = append(proxy.formatters, formatter)
	proxy.appenders = append(proxy.appenders, appender)

	return proxy, nil
}

func (o *OutputProxy) DoMetricReport(dataType int32, data interface{}) {
	for _, appender := range o.appenders {
		appender.DoMetricReport(dataType, data)
	}
}

func (o *OutputProxy) DoEventReport(dataType int32, data interface{}) {
	for _, appender := range o.appenders {
		appender.DoEventReport(dataType, data)
	}
}

func (o *OutputProxy) GetFormatters() FormatterInterface {
	return o.formatters[0]
}
