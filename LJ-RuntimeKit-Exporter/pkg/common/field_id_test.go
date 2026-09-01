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

package common

import (
	"testing"
)

func TestIsFieldIn(t *testing.T) {
	tests := []struct {
		name     string
		start    int
		end      int
		field    int
		expected bool
	}{
		{
			name:     "inside range - middle value",
			start:    4000,
			end:      4999,
			field:    4500,
			expected: true,
		},
		{
			name:     "at start boundary - inclusive",
			start:    4000,
			end:      4999,
			field:    4000,
			expected: true,
		},
		{
			name:     "at end boundary - exclusive",
			start:    4000,
			end:      4999,
			field:    4999,
			expected: false,
		},
		{
			name:     "just before end - one less",
			start:    4000,
			end:      4999,
			field:    4998,
			expected: true,
		},
		{
			name:     "just above start",
			start:    4000,
			end:      4999,
			field:    4001,
			expected: true,
		},
		{
			name:     "below start",
			start:    4000,
			end:      4999,
			field:    3999,
			expected: false,
		},
		{
			name:     "above end",
			start:    4000,
			end:      4999,
			field:    5000,
			expected: false,
		},
		{
			name:     "empty range - start equals end",
			start:    4000,
			end:      4000,
			field:    4000,
			expected: false,
		},
		{
			name:     "invalid range - start greater than end",
			start:    5000,
			end:      4000,
			field:    4500,
			expected: false,
		},
		{
			name:     "zero range",
			start:    0,
			end:      100,
			field:    50,
			expected: true,
		},
		{
			name:     "single element range - field at start",
			start:    4000,
			end:      4001,
			field:    4000,
			expected: true,
		},
		{
			name:     "single element range - field at end",
			start:    4000,
			end:      4001,
			field:    4001,
			expected: false,
		},
		{
			name:     "negative values - inside range",
			start:    -100,
			end:      -50,
			field:    -75,
			expected: true,
		},
		{
			name:     "negative values - below start",
			start:    -100,
			end:      -50,
			field:    -200,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsFieldIn(tt.start, tt.end, tt.field)
			if result != tt.expected {
				t.Errorf("IsFieldIn(%d, %d, %d) = %v; expected %v",
					tt.start, tt.end, tt.field, result, tt.expected)
			}
		})
	}
}

func TestIsFieldIdInModule(t *testing.T) {
	tests := []struct {
		name     string
		module   int32
		field    int
		expected bool
	}{
		// AmperfModuEstimate (base=4000, max=4999)
		{
			name:     "Estimate - at base",
			module:   AmperfModuEstimate,
			field:    FieldIdEstimateBase,
			expected: true,
		},
		{
			name:     "Estimate - valid field Fp32Tflops",
			module:   AmperfModuEstimate,
			field:    FieldIdFp32Tflops,
			expected: true,
		},
		{
			name:     "Estimate - valid field MemUsedByte",
			module:   AmperfModuEstimate,
			field:    FieldIdMemUsedByte,
			expected: true,
		},
		{
			name:     "Estimate - at max boundary (exclusive)",
			module:   AmperfModuEstimate,
			field:    FieldIdEstimateMax,
			expected: false,
		},
		{
			name:     "Estimate - one below max",
			module:   AmperfModuEstimate,
			field:    FieldIdEstimateMax - 1,
			expected: true,
		},
		{
			name:     "Estimate - below base",
			module:   AmperfModuEstimate,
			field:    FieldIdEstimateBase - 1,
			expected: false,
		},
		{
			name:     "Estimate - above max",
			module:   AmperfModuEstimate,
			field:    FieldIdEstimateMax + 1,
			expected: false,
		},

		// AmperfModuHealthWatch (base=5000, max=5999)
		{
			name:     "HealthWatch - at base",
			module:   AmperfModuHealthWatch,
			field:    FieldIdHealthWatchBase,
			expected: true,
		},
		{
			name:     "HealthWatch - WatchOverall field",
			module:   AmperfModuHealthWatch,
			field:    FieldIdWatchOverall,
			expected: true,
		},
		{
			name:     "HealthWatch - at max boundary",
			module:   AmperfModuHealthWatch,
			field:    FieldIdWatchMax,
			expected: false,
		},
		{
			name:     "HealthWatch - above max",
			module:   AmperfModuHealthWatch,
			field:    FieldIdWatchMax + 1,
			expected: false,
		},

		// AmperfModuHealthDetect (base=9000, max=9999)
		{
			name:     "HealthDetect - at base",
			module:   AmperfModuHealthDetect,
			field:    FieldIdHealthDetectBase,
			expected: true,
		},
		{
			name:     "HealthDetect - DetectLostCard",
			module:   AmperfModuHealthDetect,
			field:    FieldIdDetectLostCard,
			expected: true,
		},
		{
			name:     "HealthDetect - DetectXid",
			module:   AmperfModuHealthDetect,
			field:    FieldIdDetectXid,
			expected: true,
		},
		{
			name:     "HealthDetect - DetectAmperfStatus",
			module:   AmperfModuHealthDetect,
			field:    FieldIdDetectAmperfStatus,
			expected: true,
		},
		{
			name:     "HealthDetect - at max boundary",
			module:   AmperfModuHealthDetect,
			field:    FieldIdDetectMax,
			expected: false,
		},

		// AmperfModuEgpu (base=6000, max=6999)
		{
			name:     "Egpu - at base",
			module:   AmperfModuEgpu,
			field:    FieldIdEgpuBase,
			expected: true,
		},
		{
			name:     "Egpu - EgpuDutyCycle",
			module:   AmperfModuEgpu,
			field:    FieldIdEgpuDutyCycle,
			expected: true,
		},
		{
			name:     "Egpu - EgpuMemFreeMib",
			module:   AmperfModuEgpu,
			field:    FieldIdEgpuMemFreeMib,
			expected: true,
		},
		{
			name:     "Egpu - AoaQueue sub-module field",
			module:   AmperfModuEgpu,
			field:    FieldIdAoaQueue,
			expected: true,
		},
		{
			name:     "Egpu - at max boundary",
			module:   AmperfModuEgpu,
			field:    FieldIdEgpuMax,
			expected: false,
		},

		// AmperfModuEtrace (base=8000, max=8999)
		{
			name:     "Etrace - at base",
			module:   AmperfModuEtrace,
			field:    FieldIdEtraceBase,
			expected: true,
		},
		{
			name:     "Etrace - EtraceRtAvg",
			module:   AmperfModuEtrace,
			field:    FieldIdEtraceRtAvg,
			expected: true,
		},
		{
			name:     "Etrace - at max boundary",
			module:   AmperfModuEtrace,
			field:    FieldIdEtraceMax,
			expected: false,
		},

		// AmperfModuProcess (base=7000, max=7999)
		{
			name:     "Process - at base",
			module:   AmperfModuProcess,
			field:    FieldIdProcessBase,
			expected: true,
		},
		{
			name:     "Process - ProcEncUtil",
			module:   AmperfModuProcess,
			field:    FieldIdProcEncUtil,
			expected: true,
		},
		{
			name:     "Process - at max boundary",
			module:   AmperfModuProcess,
			field:    FieldIdProcMax,
			expected: false,
		},

		// AmperfModuEpuCont (base=10000, max=10999)
		{
			name:     "EpuCont - at base",
			module:   AmperfModuEpuCont,
			field:    FieldIdContBase,
			expected: true,
		},
		{
			name:     "EpuCont - at max boundary",
			module:   AmperfModuEpuCont,
			field:    FieldIdContMax,
			expected: false,
		},
		{
			name:     "EpuCont - one below max",
			module:   AmperfModuEpuCont,
			field:    FieldIdContMax - 1,
			expected: true,
		},

		// AmperfModuRundGuestDiag (base=11000, max=11999)
		{
			name:     "RundGuestDiag - at base",
			module:   AmperfModuRundGuestDiag,
			field:    FieldIdRundGuestDiagBase,
			expected: true,
		},
		{
			name:     "RundGuestDiag - at max boundary",
			module:   AmperfModuRundGuestDiag,
			field:    FieldIdRundGuestDiagMax,
			expected: false,
		},

		// AmperfModuRundHostDiag (base=12000, max=12999)
		{
			name:     "RundHostDiag - at base",
			module:   AmperfModuRundHostDiag,
			field:    FieldIdRundHostDiagBase,
			expected: true,
		},
		{
			name:     "RundHostDiag - at max boundary",
			module:   AmperfModuRundHostDiag,
			field:    FieldIdRundHostDiagMax,
			expected: false,
		},

		// AmperfModuHealthPredict (base=13000, max=13999)
		{
			name:     "HealthPredict - at base",
			module:   AmperfModuHealthPredict,
			field:    FieldIdHealthPredictBase,
			expected: true,
		},
		{
			name:     "HealthPredict - at max boundary",
			module:   AmperfModuHealthPredict,
			field:    FieldIdHealthPredictMax,
			expected: false,
		},

		// AmperfModuSystemEstimate (base=20000, max=20999)
		{
			name:     "SystemEstimate - at base",
			module:   AmperfModuSystemEstimate,
			field:    FieldIdSystemEstimateBase,
			expected: true,
		},
		{
			name:     "SystemEstimate - at max boundary",
			module:   AmperfModuSystemEstimate,
			field:    FieldIdSystemEstimateMax,
			expected: false,
		},
		{
			name:     "SystemEstimate - one below max",
			module:   AmperfModuSystemEstimate,
			field:    FieldIdSystemEstimateMax - 1,
			expected: true,
		},

		// Unknown modules
		{
			name:     "Unknown module - zero",
			module:   0,
			field:    4000,
			expected: false,
		},
		{
			name:     "Unknown module - negative",
			module:   -1,
			field:    4000,
			expected: false,
		},
		{
			name:     "Unknown module - arbitrary value",
			module:   99999,
			field:    4000,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsFieldIdInModule(tt.module, tt.field)
			if result != tt.expected {
				t.Errorf("IsFieldIdInModule(%d, %d) = %v; expected %v",
					tt.module, tt.field, result, tt.expected)
			}
		})
	}
}

func TestIsCountersThisType(t *testing.T) {
	tests := []struct {
		name     string
		dataType int32
		field    int
		expected bool
	}{
		// CollectDataType (value=0): field < FieldIdEtraceMax || EpuCont || SystemEstimate
		{
			name:     "Collect - Estimate field Fp64Tflops (below EtraceMax)",
			dataType: CollectDataType,
			field:    FieldIdFp64Tflops,
			expected: true,
		},
		{
			name:     "Collect - Estimate field MemUsedByte",
			dataType: CollectDataType,
			field:    FieldIdMemUsedByte,
			expected: true,
		},
		{
			name:     "Collect - Estimate field ThrottleHwSlow",
			dataType: CollectDataType,
			field:    FieldIdThrottleHwSlow,
			expected: true,
		},
		{
			name:     "Collect - HealthWatch field WatchOverall (below EtraceMax)",
			dataType: CollectDataType,
			field:    FieldIdWatchOverall,
			expected: true,
		},
		{
			name:     "Collect - Egpu field EgpuDutyCycle (below EtraceMax)",
			dataType: CollectDataType,
			field:    FieldIdEgpuDutyCycle,
			expected: true,
		},
		{
			name:     "Collect - Egpu AoaQueue sub-module field",
			dataType: CollectDataType,
			field:    FieldIdAoaQueue,
			expected: true,
		},
		{
			name:     "Collect - Process field ProcEncUtil (below EtraceMax)",
			dataType: CollectDataType,
			field:    FieldIdProcEncUtil,
			expected: true,
		},
		{
			name:     "Collect - Etrace field EtraceRtAvg (below EtraceMax)",
			dataType: CollectDataType,
			field:    FieldIdEtraceRtAvg,
			expected: true,
		},
		{
			name:     "Collect - EtraceMax boundary (exclusive via <)",
			dataType: CollectDataType,
			field:    FieldIdEtraceMax,
			expected: false,
		},
		{
			name:     "Collect - EpuCont field (above EtraceMax, in EpuCont module)",
			dataType: CollectDataType,
			field:    FieldIdContBase,
			expected: true,
		},
		{
			name:     "Collect - SystemEstimate field (above EtraceMax, in SystemEstimate module)",
			dataType: CollectDataType,
			field:    FieldIdSystemEstimateBase,
			expected: true,
		},
		{
			name:     "Collect - HealthDetect field (NOT collect type)",
			dataType: CollectDataType,
			field:    FieldIdDetectLostCard,
			expected: false,
		},
		{
			name:     "Collect - DetectEccError (NOT collect type)",
			dataType: CollectDataType,
			field:    FieldIdDetectEccError,
			expected: false,
		},
		{
			name:     "Collect - RundGuestDiag field (NOT collect type)",
			dataType: CollectDataType,
			field:    FieldIdRundGuestDiagBase,
			expected: false,
		},
		{
			name:     "Collect - RundHostDiag field (NOT collect type)",
			dataType: CollectDataType,
			field:    FieldIdRundHostDiagBase,
			expected: false,
		},
		{
			name:     "Collect - HealthPredict field (NOT collect type)",
			dataType: CollectDataType,
			field:    FieldIdHealthPredictBase,
			expected: false,
		},

		// DetectDataType (value=1): HealthDetect || RundGuestDiag || RundHostDiag
		{
			name:     "Detect - HealthDetect field DetectLostCard",
			dataType: DetectDataType,
			field:    FieldIdDetectLostCard,
			expected: true,
		},
		{
			name:     "Detect - HealthDetect field DetectXid",
			dataType: DetectDataType,
			field:    FieldIdDetectXid,
			expected: true,
		},
		{
			name:     "Detect - HealthDetect field DetectAmperfStatus",
			dataType: DetectDataType,
			field:    FieldIdDetectAmperfStatus,
			expected: true,
		},
		{
			name:     "Detect - HealthDetect field DetectKernelErrCnt",
			dataType: DetectDataType,
			field:    FieldIdDetectKernelErrCnt,
			expected: true,
		},
		{
			name:     "Detect - RundGuestDiag field",
			dataType: DetectDataType,
			field:    FieldIdRundGuestDiagBase,
			expected: true,
		},
		{
			name:     "Detect - RundHostDiag field",
			dataType: DetectDataType,
			field:    FieldIdRundHostDiagBase,
			expected: true,
		},
		{
			name:     "Detect - Estimate field (NOT detect type)",
			dataType: DetectDataType,
			field:    FieldIdFp64Tflops,
			expected: false,
		},
		{
			name:     "Detect - Egpu field (NOT detect type)",
			dataType: DetectDataType,
			field:    FieldIdEgpuDutyCycle,
			expected: false,
		},
		{
			name:     "Detect - HealthWatch field (NOT detect type)",
			dataType: DetectDataType,
			field:    FieldIdWatchOverall,
			expected: false,
		},
		{
			name:     "Detect - Etrace field (NOT detect type)",
			dataType: DetectDataType,
			field:    FieldIdEtraceRtAvg,
			expected: false,
		},
		{
			name:     "Detect - Process field (NOT detect type)",
			dataType: DetectDataType,
			field:    FieldIdProcEncUtil,
			expected: false,
		},
		{
			name:     "Detect - EpuCont field (NOT detect type)",
			dataType: DetectDataType,
			field:    FieldIdContBase,
			expected: false,
		},
		{
			name:     "Detect - SystemEstimate field (NOT detect type)",
			dataType: DetectDataType,
			field:    FieldIdSystemEstimateBase,
			expected: false,
		},
		{
			name:     "Detect - HealthPredict field (NOT detect type)",
			dataType: DetectDataType,
			field:    FieldIdHealthPredictBase,
			expected: false,
		},

		// Unknown data type
		{
			name:     "Unknown type - arbitrary value 2",
			dataType: 2,
			field:    FieldIdFp64Tflops,
			expected: false,
		},
		{
			name:     "Unknown type - negative value",
			dataType: -1,
			field:    FieldIdFp64Tflops,
			expected: false,
		},
		{
			name:     "Unknown type - large value",
			dataType: 100,
			field:    FieldIdDetectLostCard,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsCountersThisType(tt.dataType, tt.field)
			if result != tt.expected {
				t.Errorf("IsCountersThisType(%d, %d) = %v; expected %v",
					tt.dataType, tt.field, result, tt.expected)
			}
		})
	}
}

func TestAMPERF_FI_Map(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		expected int
	}{
		{
			name:     "FP64 TFlops used",
			key:      "AMPF_FI_FROF_FP64_TFPS_USED",
			expected: FieldIdFp64Tflops,
		},
		{
			name:     "FP32 TFlops used",
			key:      "AMPF_FI_FROF_FP32_TFPS_USED",
			expected: FieldIdFp32Tflops,
		},
		{
			name:     "FP16 TFlops used",
			key:      "AMPF_FI_FROF_FP16_TFPS_USED",
			expected: FieldIdFp16Tflops,
		},
		{
			name:     "Tensor TFlops used",
			key:      "AMPF_FI_FROF_TENS_TFPS_USED",
			expected: FieldIdTensorTflops,
		},
		{
			name:     "Memory bandwidth",
			key:      "AMPF_FI_FROF_MEM_BANDWIDTH",
			expected: FieldIdMemBandwidth,
		},
		{
			name:     "FP64 TFlops limit",
			key:      "AMPF_FI_FROF_FP64_TFPS_LIMIT",
			expected: FieldIdFp64TflopsLmt,
		},
		{
			name:     "Memory used byte",
			key:      "AMPF_FI_DEV_MEM_USED_BYTE",
			expected: FieldIdMemUsedByte,
		},
		{
			name:     "Memory total byte",
			key:      "AMPF_FI_DEV_MEM_TOTAL_BYTE",
			expected: FieldIdMemTotalByte,
		},
		{
			name:     "Power usage milli",
			key:      "AMPF_FI_DEV_POWER_USAGE_MILLI",
			expected: FieldIdPowerUsgMilli,
		},
		{
			name:     "Health watch overall",
			key:      "AMPF_FI_HEALTH_WATCH_OVERALL",
			expected: FieldIdWatchOverall,
		},
		{
			name:     "Health detect DBE ECC",
			key:      "AMPF_FI_HEALTH_DET_DBE_ECC",
			expected: FieldIdDetectEccError,
		},
		{
			name:     "Health detect XID",
			key:      "AMPF_FI_HEALTH_DET_XID",
			expected: FieldIdDetectXid,
		},
		{
			name:     "Health detect hang",
			key:      "AMPF_FI_HEALTH_DET_HANG",
			expected: FieldIdDetectHang,
		},
		{
			name:     "Health detect kernel error",
			key:      "AMPF_FI_HEALTH_DET_KERNEL_ERR",
			expected: FieldIdDetectKernelErr,
		},
		{
			name:     "Health detect Amperf status",
			key:      "AMPF_FI_HEALTH_DET_AMPERF_STATUS",
			expected: FieldIdDetectAmperfStatus,
		},
		{
			name:     "Health detect profile status",
			key:      "AMPF_FI_HEALTH_DET_PROFILE_STATUS",
			expected: FieldIdDetectAmperfProfStatus,
		},
		{
			name:     "EGPU duty cycle",
			key:      "AMPF_FI_EGPU_DUTY_CYCLE",
			expected: FieldIdEgpuDutyCycle,
		},
		{
			name:     "EGPU duty cycle percent",
			key:      "AMPF_FI_EGPU_DUTY_CYCLE_PERCENT",
			expected: FieldIdEgpuDutyCyclePct,
		},
		{
			name:     "EGPU memory total byte",
			key:      "AMPF_FI_EGPU_MEM_TOTAL_BYTE",
			expected: FieldIdEgpuMemTotalByte,
		},
		{
			name:     "EGPU memory used byte",
			key:      "AMPF_FI_EGPU_MEM_USED_BYTE",
			expected: FieldIdEgpuMemUsedByte,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, ok := AMPERF_FI[tt.key]
			if !ok {
				t.Errorf("AMPERF_FI missing key %q", tt.key)
				return
			}
			if val != tt.expected {
				t.Errorf("AMPERF_FI[%q] = %d; expected %d", tt.key, val, tt.expected)
			}
		})
	}
}

func TestAMPERF_FI_MapSize(t *testing.T) {
	// Verify the map has the expected number of entries
	expectedSize := 33
	actualSize := len(AMPERF_FI)
	if actualSize != expectedSize {
		t.Errorf("AMPERF_FI has %d entries; expected %d", actualSize, expectedSize)
	}
}

func TestModuleBaseConstants(t *testing.T) {
	// Verify that module bases and field id bases are consistent
	tests := []struct {
		name     string
		module   int32
		base     int
		expected int
	}{
		{
			name:     "AmperfModuEstimate matches FieldIdEstimateBase",
			module:   AmperfModuEstimate,
			base:     FieldIdEstimateBase,
			expected: int(AmperfModuEstimate),
		},
		{
			name:     "AmperfModuHealthWatch matches FieldIdHealthWatchBase",
			module:   AmperfModuHealthWatch,
			base:     FieldIdHealthWatchBase,
			expected: int(AmperfModuHealthWatch),
		},
		{
			name:     "AmperfModuHealthDetect matches FieldIdHealthDetectBase",
			module:   AmperfModuHealthDetect,
			base:     FieldIdHealthDetectBase,
			expected: int(AmperfModuHealthDetect),
		},
		{
			name:     "AmperfModuEgpu matches FieldIdEgpuBase",
			module:   AmperfModuEgpu,
			base:     FieldIdEgpuBase,
			expected: int(AmperfModuEgpu),
		},
		{
			name:     "AmperfModuEtrace matches FieldIdEtraceBase",
			module:   AmperfModuEtrace,
			base:     FieldIdEtraceBase,
			expected: int(AmperfModuEtrace),
		},
		{
			name:     "AmperfModuProcess matches FieldIdProcessBase",
			module:   AmperfModuProcess,
			base:     FieldIdProcessBase,
			expected: int(AmperfModuProcess),
		},
		{
			name:     "AmperfModuEpuCont matches FieldIdContBase",
			module:   AmperfModuEpuCont,
			base:     FieldIdContBase,
			expected: int(AmperfModuEpuCont),
		},
		{
			name:     "AmperfModuRundGuestDiag matches FieldIdRundGuestDiagBase",
			module:   AmperfModuRundGuestDiag,
			base:     FieldIdRundGuestDiagBase,
			expected: int(AmperfModuRundGuestDiag),
		},
		{
			name:     "AmperfModuRundHostDiag matches FieldIdRundHostDiagBase",
			module:   AmperfModuRundHostDiag,
			base:     FieldIdRundHostDiagBase,
			expected: int(AmperfModuRundHostDiag),
		},
		{
			name:     "AmperfModuHealthPredict matches FieldIdHealthPredictBase",
			module:   AmperfModuHealthPredict,
			base:     FieldIdHealthPredictBase,
			expected: int(AmperfModuHealthPredict),
		},
		{
			name:     "AmperfModuSystemEstimate matches FieldIdSystemEstimateBase",
			module:   AmperfModuSystemEstimate,
			base:     FieldIdSystemEstimateBase,
			expected: int(AmperfModuSystemEstimate),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if int(tt.module) != tt.base {
				t.Errorf("module %d base %d does not match expected %d",
					tt.module, tt.base, tt.expected)
			}
		})
	}
}

func TestFieldIdModuleOffset(t *testing.T) {
	// Verify that all module max values are correctly computed as base + offset
	tests := []struct {
		name string
		base int
		max  int
	}{
		{
			name: "Estimate max",
			base: FieldIdEstimateBase,
			max:  FieldIdEstimateMax,
		},
		{
			name: "HealthWatch max",
			base: FieldIdHealthWatchBase,
			max:  FieldIdWatchMax,
		},
		{
			name: "HealthDetect max",
			base: FieldIdHealthDetectBase,
			max:  FieldIdDetectMax,
		},
		{
			name: "Egpu max",
			base: FieldIdEgpuBase,
			max:  FieldIdEgpuMax,
		},
		{
			name: "Etrace max",
			base: FieldIdEtraceBase,
			max:  FieldIdEtraceMax,
		},
		{
			name: "Process max",
			base: FieldIdProcessBase,
			max:  FieldIdProcMax,
		},
		{
			name: "EpuCont max",
			base: FieldIdContBase,
			max:  FieldIdContMax,
		},
		{
			name: "RundGuestDiag max",
			base: FieldIdRundGuestDiagBase,
			max:  FieldIdRundGuestDiagMax,
		},
		{
			name: "RundHostDiag max",
			base: FieldIdRundHostDiagBase,
			max:  FieldIdRundHostDiagMax,
		},
		{
			name: "HealthPredict max",
			base: FieldIdHealthPredictBase,
			max:  FieldIdHealthPredictMax,
		},
		{
			name: "SystemEstimate max",
			base: FieldIdSystemEstimateBase,
			max:  FieldIdSystemEstimateMax,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expected := tt.base + FieldIdModuleOffset
			if tt.max != expected {
				t.Errorf("%s max = %d; expected base(%d) + offset(%d) = %d",
					tt.name, tt.max, tt.base, FieldIdModuleOffset, expected)
			}
		})
	}
}
