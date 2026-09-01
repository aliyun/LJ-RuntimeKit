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

	"github.com/lj-runtimekit/exporter/pkg/common"
)

// AppenderInterface defines how formatted metrics are delivered to the output.
type AppenderInterface interface {
	DoMetricReport(dataType int32, data interface{})
	DoEventReport(dataType int32, data interface{})
	GetConfig() interface{}
}

// AppenderFactory creates a new AppenderInterface based on the specified type.
func AppenderFactory(apdType string, apdConfig string, formatter FormatterInterface, proxyCfg *OutputProxyConfig, info *SystemInfo, counters []common.Counter) (AppenderInterface, error) {
	var (
		appender AppenderInterface
		err      error
	)

	switch apdType {
	case OutputTypeHttpPull, OutputTypeHttpPush:
		appender, err = newHttpAppender(formatter, proxyCfg.config, proxyCfg.httpChan)
	default:
		return nil, fmt.Errorf("unknown appender type: %s", apdType)
	}

	if err != nil {
		return nil, err
	}

	return appender, nil
}
