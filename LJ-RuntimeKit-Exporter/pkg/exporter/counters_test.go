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
	"testing"
)

func TestNewCollectorCounters(t *testing.T) {
	// Build mixed counters: some CollectDataType, some DetectDataType
	counters := []common.Counter{
		// CollectDataType: FieldIdFp64Tflops (4000) — < FieldIdEtraceMax
		{FieldID: common.FieldIdFp64Tflops, MetricName: "fp64_tflops"},
		// CollectDataType: FieldIdEnergy (4006) — < FieldIdEtraceMax
		{FieldID: common.FieldIdEnergy, MetricName: "energy"},
		// CollectDataType: FieldIdEgpuDutyCycle (6000) — < FieldIdEtraceMax
		{FieldID: common.FieldIdEgpuDutyCycle, MetricName: "egpu_duty_cycle"},
		// CollectDataType: EpuCont module (10000)
		{FieldID: common.FieldIdContBase, MetricName: "cont_metric"},
		// CollectDataType: SystemEstimate module (20000)
		{FieldID: common.FieldIdSystemEstimateBase, MetricName: "sys_estimate"},

		// DetectDataType: FieldIdDetectLostCard (9000)
		{FieldID: common.FieldIdDetectLostCard, MetricName: "detect_lost_card"},
		// DetectDataType: FieldIdDetectXid (9006)
		{FieldID: common.FieldIdDetectXid, MetricName: "detect_xid"},
		// DetectDataType: RundGuestDiag module (11000)
		{FieldID: common.FieldIdRundGuestDiagBase, MetricName: "rund_guest_diag"},
		// DetectDataType: RundHostDiag module (12000)
		{FieldID: common.FieldIdRundHostDiagBase, MetricName: "rund_host_diag"},
	}

	result := NewCollectorCounters(counters)

	// Verify CollectDataType counters are included
	for _, fieldID := range []int{
		common.FieldIdFp64Tflops,
		common.FieldIdEnergy,
		common.FieldIdEgpuDutyCycle,
		common.FieldIdContBase,
		common.FieldIdSystemEstimateBase,
	} {
		key := int32(fieldID)
		list, ok := result[key]
		if !ok {
			t.Errorf("expected CollectDataType counter for FieldID %d to be present, but not found", fieldID)
			continue
		}
		if len(list) != 1 {
			t.Errorf("expected 1 counter for FieldID %d, got %d", fieldID, len(list))
		}
	}

	// Verify DetectDataType counters are NOT included
	for _, fieldID := range []int{
		common.FieldIdDetectLostCard,
		common.FieldIdDetectXid,
		common.FieldIdRundGuestDiagBase,
		common.FieldIdRundHostDiagBase,
	} {
		key := int32(fieldID)
		if _, ok := result[key]; ok {
			t.Errorf("expected DetectDataType counter for FieldID %d to NOT be present, but found", fieldID)
		}
	}

	// Verify total number of keys equals number of CollectDataType counters
	if len(result) != 5 {
		t.Errorf("expected 5 keys in result, got %d", len(result))
	}
}

func TestNewDetectorCounters(t *testing.T) {
	// Build mixed counters: some CollectDataType, some DetectDataType
	counters := []common.Counter{
		// CollectDataType: FieldIdFp64Tflops (4000)
		{FieldID: common.FieldIdFp64Tflops, MetricName: "fp64_tflops"},
		// CollectDataType: FieldIdEnergy (4006)
		{FieldID: common.FieldIdEnergy, MetricName: "energy"},
		// CollectDataType: FieldIdWatchOverall (5000)
		{FieldID: common.FieldIdWatchOverall, MetricName: "watch_overall"},

		// DetectDataType: FieldIdDetectLostCard (9000)
		{FieldID: common.FieldIdDetectLostCard, MetricName: "detect_lost_card"},
		// DetectDataType: FieldIdDetectEccError (9001)
		{FieldID: common.FieldIdDetectEccError, MetricName: "detect_ecc"},
		// DetectDataType: FieldIdDetectXid (9006)
		{FieldID: common.FieldIdDetectXid, MetricName: "detect_xid"},
		// DetectDataType: FieldIdDetectHang (9011)
		{FieldID: common.FieldIdDetectHang, MetricName: "detect_hang"},
		// DetectDataType: RundGuestDiag module (11000)
		{FieldID: common.FieldIdRundGuestDiagBase, MetricName: "rund_guest"},
		// DetectDataType: RundHostDiag module (12000)
		{FieldID: common.FieldIdRundHostDiagBase, MetricName: "rund_host"},
	}

	result := NewDetectorCounters(counters)

	// Verify DetectDataType counters are included
	for _, fieldID := range []int{
		common.FieldIdDetectLostCard,
		common.FieldIdDetectEccError,
		common.FieldIdDetectXid,
		common.FieldIdDetectHang,
		common.FieldIdRundGuestDiagBase,
		common.FieldIdRundHostDiagBase,
	} {
		key := int32(fieldID)
		list, ok := result[key]
		if !ok {
			t.Errorf("expected DetectDataType counter for FieldID %d to be present, but not found", fieldID)
			continue
		}
		if len(list) != 1 {
			t.Errorf("expected 1 counter for FieldID %d, got %d", fieldID, len(list))
		}
	}

	// Verify CollectDataType counters are NOT included
	for _, fieldID := range []int{
		common.FieldIdFp64Tflops,
		common.FieldIdEnergy,
		common.FieldIdWatchOverall,
	} {
		key := int32(fieldID)
		if _, ok := result[key]; ok {
			t.Errorf("expected CollectDataType counter for FieldID %d to NOT be present, but found", fieldID)
		}
	}

	// Verify correct count: 6 DetectDataType counters
	if len(result) != 6 {
		t.Errorf("expected 6 keys in result, got %d", len(result))
	}
}

func TestNewCollectorCounters_Empty(t *testing.T) {
	result := NewCollectorCounters([]common.Counter{})

	if len(result) != 0 {
		t.Errorf("expected empty result for empty input, got %d keys", len(result))
	}
}

func TestNewDetectorCounters_Empty(t *testing.T) {
	result := NewDetectorCounters([]common.Counter{})

	if len(result) != 0 {
		t.Errorf("expected empty result for empty input, got %d keys", len(result))
	}
}
