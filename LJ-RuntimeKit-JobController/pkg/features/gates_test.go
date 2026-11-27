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

package features

import (
	"testing"

	"github.com/spf13/pflag"
)

func TestFeatureGates(t *testing.T) {
	for _, tt := range []struct {
		name          string
		value         string
		computeDomain bool
		profiling     bool
		wantErr       bool
	}{
		{name: "defaults"},
		{name: "disable profiling", value: "Profiling=false"},
		{name: "disable compute", value: "ComputeDomain=false,Profiling=true", profiling: true},
		{name: "enable compute", value: "ComputeDomain=true,Profiling=true", computeDomain: true, profiling: true},
		{name: "compute only", value: "ComputeDomain=true", computeDomain: true},
		{name: "profiling only", value: "Profiling=true", profiling: true},
		{name: "both", value: "ComputeDomain=true,Profiling=true", computeDomain: true, profiling: true},
		{name: "neither", value: "ComputeDomain=false,Profiling=false"},
		{name: "unknown", value: "Unknown=true", wantErr: true},
		{name: "invalid boolean", value: "Profiling=invalid", wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			gate := NewFeatureGate()
			flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
			gate.AddFlag(flags)
			var args []string
			if tt.value != "" {
				args = append(args, "--feature-gates="+tt.value)
			}
			err := flags.Parse(args)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if gate.Enabled(ComputeDomain) != tt.computeDomain || gate.Enabled(Profiling) != tt.profiling {
				t.Fatalf("unexpected gates: ComputeDomain=%v Profiling=%v", gate.Enabled(ComputeDomain), gate.Enabled(Profiling))
			}
		})
	}
}
