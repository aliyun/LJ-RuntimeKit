package adapter

import (
	"testing"

	xdmgrpc "github.com/lj-runtimekit/exporter/pkg/grpc/xdm"
	"github.com/lj-runtimekit/exporter/pkg/npdadapter/exitcode"
)

func TestEvaluate(t *testing.T) {
	tests := []struct {
		name         string
		result       *xdmgrpc.CategoryHealth
		sampled, ttl int64
		code         int
		message      string
	}{
		{"missing", nil, 0, 0, exitcode.Unknown, "category not reported"},
		{"missing before stale", nil, 1000, 100, exitcode.Unknown, "category not reported"},
		{"healthy", &xdmgrpc.CategoryHealth{Status: xdmgrpc.HealthStatus_HEALTHY, Reason: "ignored"}, 1000, 500, exitcode.Healthy, ""},
		{"unhealthy", &xdmgrpc.CategoryHealth{Status: xdmgrpc.HealthStatus_UNHEALTHY, Reason: "device failure"}, 1000, 500, exitcode.Unhealthy, "device failure"},
		{"unknown", &xdmgrpc.CategoryHealth{Status: xdmgrpc.HealthStatus_UNKNOWN, Reason: "sampling failed"}, 0, 0, exitcode.Unknown, "sampling failed"},
		{"unspecified", &xdmgrpc.CategoryHealth{}, 0, 0, exitcode.Unknown, "unknown health state"},
		{"future enum", &xdmgrpc.CategoryHealth{Status: 123, Reason: "future state"}, 0, 0, exitcode.Unknown, "future state"},
		{"stale healthy", &xdmgrpc.CategoryHealth{Status: xdmgrpc.HealthStatus_HEALTHY}, 1000, 100, exitcode.Healthy, ""},
		{"stale unhealthy", &xdmgrpc.CategoryHealth{Status: xdmgrpc.HealthStatus_UNHEALTHY, Reason: "device failure"}, 1000, 100, exitcode.Unhealthy, "device failure"},
		{"stale unknown", &xdmgrpc.CategoryHealth{Status: xdmgrpc.HealthStatus_UNKNOWN, Reason: "sampling failed"}, 1000, 100, exitcode.Unknown, "sampling failed"},
		{"no timestamp", &xdmgrpc.CategoryHealth{Status: xdmgrpc.HealthStatus_HEALTHY}, 0, 100, exitcode.Healthy, ""},
		{"no ttl", &xdmgrpc.CategoryHealth{Status: xdmgrpc.HealthStatus_HEALTHY}, 1000, 0, exitcode.Healthy, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snapshot := &xdmgrpc.HealthSnapshot{SampledAtMs: tt.sampled, TtlMs: tt.ttl}
			if tt.result != nil {
				tt.result.Category = "dynamic-category"
				snapshot.Categories = []*xdmgrpc.CategoryHealth{tt.result}
			}
			code, message := Evaluate(snapshot, "dynamic-category", 1200, 160)
			if code != tt.code || message != tt.message {
				t.Fatalf("Evaluate = (%d, %q), want (%d, %q)", code, message, tt.code, tt.message)
			}
		})
	}
}

func TestEvaluateFiltersAndTruncates(t *testing.T) {
	snapshot := &xdmgrpc.HealthSnapshot{Categories: []*xdmgrpc.CategoryHealth{
		{Category: "other", Status: xdmgrpc.HealthStatus_HEALTHY},
		{Category: "target", Status: xdmgrpc.HealthStatus_UNHEALTHY, Reason: "€€€€"},
	}}
	code, message := Evaluate(snapshot, "target", 1200, 7)
	if code != exitcode.Unhealthy || message != "€€" {
		t.Fatalf("Evaluate = (%d, %q)", code, message)
	}
	code, message = Evaluate(nil, "missing", 1200, 8)
	if code != exitcode.Unknown || message != "category" {
		t.Fatalf("Evaluate(nil) = (%d, %q)", code, message)
	}
	snapshot.SampledAtMs, snapshot.TtlMs = 1000, 100
	for _, tt := range []struct {
		category string
		code     int
		message  string
	}{
		{"other", exitcode.Healthy, ""},
		{"target", exitcode.Unhealthy, "€€€€"},
		{"missing", exitcode.Unknown, "category not reported"},
	} {
		code, message = Evaluate(snapshot, tt.category, 1200, 160)
		if code != tt.code || message != tt.message {
			t.Fatalf("snapshot age must not override the reported status: %s = (%d, %q)", tt.category, code, message)
		}
	}
}
