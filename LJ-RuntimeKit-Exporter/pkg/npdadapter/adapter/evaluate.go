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
