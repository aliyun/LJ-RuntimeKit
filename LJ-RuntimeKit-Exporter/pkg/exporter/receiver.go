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

// Receiver defines the interface for metric data receivers.
type Receiver interface {
	Start() error
	Stop() error
	Name() string
	GetGpuInfo() (*SystemGpuInfo, error)
}

// ReceiverFactory creates a new Receiver based on the specified type.
func ReceiverFactory(recvType string,
	uploadCallback func(metrics interface{}, dataType int32, dataSrc int32, reportType int32),
	config *Config, counters []common.Counter,
	hostInfo *HostInfo,
) (Receiver, error) {
	var receiver Receiver
	var err error
	switch recvType {
	case ReceiverTypeGrpc:
		receiver, err = NewGrpcReceiver(config, counters, hostInfo, uploadCallback)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown receiver type: %s", recvType)
	}

	return receiver, nil
}
