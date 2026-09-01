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

// DetectorConfig holds configuration parameters for a detection term.
type DetectorConfig struct {
	InvokeTerm    DetectTermType
	InvokeTimeout uint32
}

// DetectTermType represents the type identifier for a detection term.
type DetectTermType uint32

// Field name prefixes used to distinguish DCGM and AMP fields.
const (
	DCGM_FIELD_NAME_PREFIX string = "DCGM"
	AMPF_FIELD_NAME_PREFIX string = "AMPF"
)

// Module base identifiers define the starting field ID for each metric module.
const (
	AmperfModuEstimate       int32 = 4000
	AmperfModuHealthWatch    int32 = 5000
	AmperfModuEgpu           int32 = 6000
	AmperfModuProcess        int32 = 7000
	AmperfModuEtrace         int32 = 8000
	AmperfModuHealthDetect   int32 = 9000
	AmperfModuEpuCont        int32 = 10000
	AmperfModuRundGuestDiag  int32 = 11000
	AmperfModuRundHostDiag   int32 = 12000
	AmperfModuHealthPredict  int32 = 13000
	AmperfModuSystemEstimate int32 = 20000
)

const (
	FieldIdEstimateBase       = int(AmperfModuEstimate)
	FieldIdHealthWatchBase    = int(AmperfModuHealthWatch)
	FieldIdEgpuBase           = int(AmperfModuEgpu)
	FieldIdProcessBase        = int(AmperfModuProcess)
	FieldIdEtraceBase         = int(AmperfModuEtrace)
	FieldIdHealthDetectBase   = int(AmperfModuHealthDetect)
	FieldIdContBase           = int(AmperfModuEpuCont)
	FieldIdRundGuestDiagBase  = int(AmperfModuRundGuestDiag)
	FieldIdRundHostDiagBase   = int(AmperfModuRundHostDiag)
	FieldIdHealthPredictBase  = int(AmperfModuHealthPredict)
	FieldIdSystemEstimateBase = int(AmperfModuSystemEstimate)
	FieldIdModuleOffset       = int(999)
)

const (
	/* Module metrics estimated */
	FieldIdFp64Tflops      int = FieldIdEstimateBase
	FieldIdFp32Tflops      int = 4001
	FieldIdFp16Tflops      int = 4002
	FieldIdTensorTflops    int = 4003
	FieldIdAmpTflops       int = 4004
	FieldIdMemBandwidth    int = 4005
	FieldIdEnergy          int = 4006
	FieldIdFp64TflopsLmt   int = 4007
	FieldIdFp32TflopsLmt   int = 4008
	FieldIdFp16TflopsLmt   int = 4009
	FieldIdTensorTflopsLmt int = 4010
	FieldIdMemUsedByte     int = 4011
	FieldIdMemTotalByte    int = 4012
	FieldIdMemBandWidthLmt int = 4013
	FieldIdPowerUsgMilli   int = 4014
	FieldIdThrottlePwrBrk  int = 4015
	FieldIdThrottleHwSlow  int = 4016

	FieldIdEstimateMax int = FieldIdEstimateBase + FieldIdModuleOffset

	/* Module health check */
	FieldIdWatchOverall int = FieldIdHealthWatchBase
	FieldIdWatchMax     int = FieldIdHealthWatchBase + FieldIdModuleOffset

	/* Module egpu */
	FieldIdEgpuDutyCycle    int = FieldIdEgpuBase
	FieldIdEgpuDutyCyclePct int = 6001
	FieldIdEgpuMemTotalByte int = 6002
	FieldIdEgpuMemUsedByte  int = 6003
	FieldIdEgpuMemTotalMib  int = 6004
	FieldIdEgpuMemUsedMib   int = 6005
	FieldIdEgpuMemFreeMib   int = 6006

	/* egpu sub module AoA */
	FieldIdAoaBase  int = 6100
	FieldIdAoaQueue int = FieldIdAoaBase
	FieldIdEgpuMax  int = FieldIdEgpuBase + FieldIdModuleOffset

	/* Module process stat */
	FieldIdProcEncUtil int = FieldIdProcessBase
	FieldIdProcMax     int = FieldIdProcessBase + FieldIdModuleOffset

	/* Module etrace */
	FieldIdEtraceRtAvg int = FieldIdEtraceBase
	FieldIdEtraceMax   int = FieldIdEtraceBase + FieldIdModuleOffset

	/* Module health detect */
	FieldIdDetectLostCard     int = FieldIdHealthDetectBase
	FieldIdDetectEccError     int = 9001
	FieldIdDetectMountTimes   int = 9002
	FieldIdDetectCmptModes    int = 9003
	FieldIdDetectMemLeak      int = 9004
	FieldIdDetectCommStat     int = 9005
	FieldIdDetectXid          int = 9006
	FieldIdDetectP2PCap       int = 9009
	FieldIdDetectNvlinkSt     int = 9010
	FieldIdDetectHang         int = 9011
	FieldIdDetectFatalXid     int = 9012
	FieldIdDetectEvent        int = 9014
	FieldIdDetectXIdCnt       int = 9015
	FieldIdDetectLostCardVm   int = 9016
	FieldIdDetectKernelErr    int = 9017
	FieldIdDetectKernelErrCnt int = 9018

	/* Module Amperf health detect */
	FieldIdDetectAmperfStatus     int = 9600
	FieldIdDetectAmperfProfStatus int = 9601
	FieldIdDetectMax              int = FieldIdHealthDetectBase + FieldIdModuleOffset
	FieldIdContMax                int = FieldIdContBase + FieldIdModuleOffset

	/* Module RUND Guest health detect */
	FieldIdRundGuestDiagMax int = FieldIdRundGuestDiagBase + FieldIdModuleOffset

	/* Module RUND Host health detect */
	FieldIdRundHostDiagMax int = FieldIdRundHostDiagBase + FieldIdModuleOffset

	/* Module health prediction */
	FieldIdHealthPredictMax int = FieldIdHealthPredictBase + FieldIdModuleOffset

	/* Module system metrics estimated */
	FieldIdSystemEstimateMax int = FieldIdSystemEstimateBase + FieldIdModuleOffset
)

// AMPERF_FI maps AMP field names to their corresponding field IDs.
var (
	AMPERF_FI = map[string]int{
		"AMPF_FI_FROF_FP64_TFPS_USED":       FieldIdFp64Tflops,
		"AMPF_FI_FROF_FP32_TFPS_USED":       FieldIdFp32Tflops,
		"AMPF_FI_FROF_FP16_TFPS_USED":       FieldIdFp16Tflops,
		"AMPF_FI_FROF_TENS_TFPS_USED":       FieldIdTensorTflops,
		"AMPF_FI_FROF_MEM_BANDWIDTH":        FieldIdMemBandwidth,
		"AMPF_FI_FROF_FP64_TFPS_LIMIT":      FieldIdFp64TflopsLmt,
		"AMPF_FI_FROF_FP32_TFPS_LIMIT":      FieldIdFp32TflopsLmt,
		"AMPF_FI_FROF_FP16_TFPS_LIMIT":      FieldIdFp16TflopsLmt,
		"AMPF_FI_FROF_TENS_TFPS_LIMIT":      FieldIdTensorTflopsLmt,
		"AMPF_FI_DEV_MEM_USED_BYTE":         FieldIdMemUsedByte,
		"AMPF_FI_DEV_MEM_TOTAL_BYTE":        FieldIdMemTotalByte,
		"AMPF_FI_DEV_MEM_BDW_LIMIT_BYTE":    FieldIdMemBandWidthLmt,
		"AMPF_FI_DEV_POWER_USAGE_MILLI":     FieldIdPowerUsgMilli,
		"AMPF_FI_DEV_THROTTLE_POWER_BRAKE":  FieldIdThrottlePwrBrk,
		"AMPF_FI_DEV_THROTTLE_HW_SLOWDOWN":  FieldIdThrottleHwSlow,
		"AMPF_FI_HEALTH_WATCH_OVERALL":      FieldIdWatchOverall,
		"AMPF_FI_HEALTH_DET_DBE_ECC":        FieldIdDetectEccError,
		"AMPF_FI_HEALTH_DET_MOUNT_TIMES":    FieldIdDetectMountTimes,
		"AMPF_FI_HEALTH_DET_CMPT_MODE":      FieldIdDetectCmptModes,
		"AMPF_FI_HEALTH_DET_COMM_STAT":      FieldIdDetectCommStat,
		"AMPF_FI_HEALTH_DET_XID":            FieldIdDetectXid,
		"AMPF_FI_HEALTH_DET_P2P_CAP":        FieldIdDetectP2PCap,
		"AMPF_FI_HEALTH_DET_NVL_STAT":       FieldIdDetectNvlinkSt,
		"AMPF_FI_HEALTH_DET_HANG":           FieldIdDetectHang,
		"AMPF_FI_HEALTH_DET_XID_CNT":        FieldIdDetectXIdCnt,
		"AMPF_FI_HEALTH_DET_KERNEL_ERR":     FieldIdDetectKernelErr,
		"AMPF_FI_HEALTH_DET_KERNEL_ERR_CNT": FieldIdDetectKernelErrCnt,
		"AMPF_FI_HEALTH_DET_AMPERF_STATUS":  FieldIdDetectAmperfStatus,
		"AMPF_FI_HEALTH_DET_PROFILE_STATUS": FieldIdDetectAmperfProfStatus,
		"AMPF_FI_EGPU_DUTY_CYCLE":           FieldIdEgpuDutyCycle,
		"AMPF_FI_EGPU_DUTY_CYCLE_PERCENT":   FieldIdEgpuDutyCyclePct,
		"AMPF_FI_EGPU_MEM_TOTAL_BYTE":       FieldIdEgpuMemTotalByte,
		"AMPF_FI_EGPU_MEM_USED_BYTE":        FieldIdEgpuMemUsedByte,
	}
)

// IsFieldIn reports whether the given field ID falls within the [start, end) range.
func IsFieldIn(start int, end int, field int) bool {
	if field >= start && field < end {
		return true
	}

	return false
}

// IsFieldIdInModule reports whether the given field ID belongs to the specified module.
func IsFieldIdInModule(module int32, field int) bool {
	switch module {
	case AmperfModuEstimate:
		return IsFieldIn(FieldIdEstimateBase, FieldIdEstimateMax, field)
	case AmperfModuHealthWatch:
		return IsFieldIn(FieldIdHealthWatchBase, FieldIdWatchMax, field)
	case AmperfModuHealthDetect:
		return IsFieldIn(FieldIdHealthDetectBase, FieldIdDetectMax, field)
	case AmperfModuEgpu:
		return IsFieldIn(FieldIdEgpuBase, FieldIdEgpuMax, field)
	case AmperfModuEtrace:
		return IsFieldIn(FieldIdEtraceBase, FieldIdEtraceMax, field)
	case AmperfModuProcess:
		return IsFieldIn(FieldIdProcessBase, FieldIdProcMax, field)
	case AmperfModuEpuCont:
		return IsFieldIn(FieldIdContBase, FieldIdContMax, field)
	case AmperfModuRundGuestDiag:
		return IsFieldIn(FieldIdRundGuestDiagBase, FieldIdRundGuestDiagMax, field)
	case AmperfModuRundHostDiag:
		return IsFieldIn(FieldIdRundHostDiagBase, FieldIdRundHostDiagMax, field)
	case AmperfModuHealthPredict:
		return IsFieldIn(FieldIdHealthPredictBase, FieldIdHealthPredictMax, field)
	case AmperfModuSystemEstimate:
		return IsFieldIn(FieldIdSystemEstimateBase, FieldIdSystemEstimateMax, field)
	default:
		return false
	}
}

// IsCountersThisType reports whether the given field ID matches the specified data type.
func IsCountersThisType(dataType int32, field int) bool {
	switch dataType {
	case CollectDataType:
		return field < FieldIdEtraceMax ||
			IsFieldIdInModule(AmperfModuEpuCont, field) ||
			IsFieldIdInModule(AmperfModuSystemEstimate, field)
	case DetectDataType:
		return IsFieldIdInModule(AmperfModuHealthDetect, field) ||
			IsFieldIdInModule(AmperfModuRundGuestDiag, field) ||
			IsFieldIdInModule(AmperfModuRundHostDiag, field)
	default:
		return false
	}
}
