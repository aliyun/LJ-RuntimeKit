// Package adapter translates health verdicts without implementing detection rules or thresholds.
package adapter

import (
	xdmgrpc "github.com/lj-runtimekit/exporter/pkg/grpc/xdm"
	"github.com/lj-runtimekit/exporter/pkg/npdadapter/exitcode"
	"github.com/lj-runtimekit/exporter/pkg/npdadapter/output"
)

// Evaluate checks category presence and translates the reported status.
func Evaluate(snapshot *xdmgrpc.HealthSnapshot, category string, nowMs int64, maxOutputLength int) (int, string) {
	result := snapshot.Pick(category)
	code, message := exitcode.Unknown, ""
	switch {
	case result == nil:
		message = "category not reported"
	// Snapshot freshness validation is temporarily disabled.
	// case snapshot.Stale(nowMs):
	// 	message = fmt.Sprintf("stale data: age=%dms", snapshot.AgeMs(nowMs))
	default:
		message = result.GetReason()
		switch result.GetStatus() {
		case xdmgrpc.HealthStatus_HEALTHY:
			code, message = exitcode.Healthy, ""
		case xdmgrpc.HealthStatus_UNHEALTHY:
			code = exitcode.Unhealthy
		case xdmgrpc.HealthStatus_UNKNOWN:
			code = exitcode.Unknown
		default:
			if message == "" {
				message = "unknown health state"
			}
		}
	}
	return code, output.Truncate(message, maxOutputLength)
}
