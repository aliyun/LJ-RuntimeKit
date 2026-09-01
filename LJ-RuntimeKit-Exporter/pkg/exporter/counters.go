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

// GenerateCounters parses the collectors CSV file and returns all counter definitions.
func GenerateCounters(config *Config, rmDuplicate bool) ([]common.Counter, error) {
	collectorsFile := "/usr/local/harp/etc/" + config.CollectorsFile

	counters, err := common.ExtractAllCountersNotVerify(collectorsFile, rmDuplicate)
	if err != nil {
		return nil, err
	}
	logrus.Infof("parse metric file %s", collectorsFile)

	return counters, nil
}

// NewCollectorCounters groups counters by field ID for collector-type metrics.
func NewCollectorCounters(counters []common.Counter) map[int32][]common.Counter {
	counterMaps := make(map[int32][]common.Counter, 0)
	for _, counter := range counters {
		if common.IsCountersThisType(common.CollectDataType, counter.FieldID) {
			if _, ok := counterMaps[int32(counter.FieldID)]; !ok {
				counterMaps[int32(counter.FieldID)] = make([]common.Counter, 0)
			}
			counterMaps[int32(counter.FieldID)] = append(counterMaps[int32(counter.FieldID)], counter)
		}
	}

	return counterMaps
}

// NewDetectorCounters groups counters by field ID for detector-type metrics.
func NewDetectorCounters(counters []common.Counter) map[int32][]common.Counter {
	counterMaps := make(map[int32][]common.Counter, 0)
	for _, counter := range counters {
		if common.IsCountersThisType(common.DetectDataType, counter.FieldID) {
			if _, ok := counterMaps[int32(counter.FieldID)]; !ok {
				counterMaps[int32(counter.FieldID)] = make([]common.Counter, 0)
			}
			counterMaps[int32(counter.FieldID)] = append(counterMaps[int32(counter.FieldID)], counter)
		}
	}

	return counterMaps
}
