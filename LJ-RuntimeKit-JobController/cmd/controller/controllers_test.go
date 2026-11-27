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

package main

import (
	"reflect"
	"sort"
	"testing"

	controller "github.com/lj-runtimekit/job-controller/pkg/controller"
	"github.com/lj-runtimekit/job-controller/pkg/controller/activemonitoring"
	"github.com/lj-runtimekit/job-controller/pkg/controller/computedomain"
	"github.com/lj-runtimekit/job-controller/pkg/controller/profiling"
	"github.com/lj-runtimekit/job-controller/pkg/features"
)

func TestEnabledFactories(t *testing.T) {
	for _, tt := range []struct {
		name    string
		gates   string
		want    []string
		wantErr bool
	}{
		{name: "default", wantErr: true},
		{name: "all disabled", gates: "ComputeDomain=false,Profiling=false,ActiveMonitoring=false", wantErr: true},
		{name: "compute only", gates: "ComputeDomain=true", want: []string{"computedomain"}},
		{name: "profiling only", gates: "Profiling=true", want: []string{"profiling"}},
		{name: "activemonitoring only", gates: "ActiveMonitoring=true", want: []string{"activemonitoring"}},
		{name: "compute and profiling", gates: "ComputeDomain=true,Profiling=true", want: []string{"computedomain", "profiling"}},
		{name: "disable activemonitoring", gates: "ComputeDomain=true,Profiling=true,ActiveMonitoring=false", want: []string{"computedomain", "profiling"}},
		{name: "all enabled", gates: "ComputeDomain=true,Profiling=true,ActiveMonitoring=true", want: []string{"activemonitoring", "computedomain", "profiling"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			gate := features.NewFeatureGate()
			if tt.gates != "" {
				if err := gate.Set(tt.gates); err != nil {
					t.Fatal(err)
				}
			}
			factories, err := enabledFactories(gate, computedomain.Config{}, 2, profiling.Config{CollectorNamespace: "kube-system", ConfigMapName: "profiling-collector-config"}, activemonitoring.Config{Namespace: "kube-system"})
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			var names []string
			for name := range factories {
				names = append(names, name)
			}
			sort.Strings(names)
			if !reflect.DeepEqual(names, tt.want) {
				t.Fatalf("factories = %v, want %v", names, tt.want)
			}
		})
	}
}

func TestDisabledControllerConfigurationIsIgnored(t *testing.T) {
	gate := features.NewFeatureGate()
	if err := gate.Set("ComputeDomain=true,Profiling=false"); err != nil {
		t.Fatal(err)
	}
	factories, err := enabledFactories(gate, computedomain.Config{}, 2, profiling.Config{}, activemonitoring.Config{})
	if err != nil {
		t.Fatalf("disabled profiling config was validated: %v", err)
	}
	if _, err := factories[computedomain.ControllerName](controller.SharedClients{}); err != nil {
		t.Fatalf("compute-only factory depends on profiling REST config: %v", err)
	}
	if err := gate.Set("ComputeDomain=false,Profiling=true"); err != nil {
		t.Fatal(err)
	}
	factories, err = enabledFactories(gate, computedomain.Config{IMEXConfigOverrides: "unsupported"}, 2, profiling.Config{CollectorNamespace: "test", ConfigMapName: "config"}, activemonitoring.Config{})
	if err != nil {
		t.Fatalf("disabled ComputeDomain config was validated: %v", err)
	}
	if _, ok := factories[computedomain.ControllerName]; ok {
		t.Fatal("disabled ComputeDomain factory registered")
	}
}

func TestEnabledControllerConfigurationIsValidated(t *testing.T) {
	gate := features.NewFeatureGate()
	if err := gate.Set("ComputeDomain=true,Profiling=false"); err != nil {
		t.Fatal(err)
	}
	if _, err := enabledFactories(gate, computedomain.Config{IMEXConfigOverrides: "unsupported"}, 2, profiling.Config{}, activemonitoring.Config{}); err == nil {
		t.Fatal("enabled ComputeDomain accepted unsupported overrides")
	}
	if err := gate.Set("ComputeDomain=false,Profiling=true"); err != nil {
		t.Fatal(err)
	}
	if _, err := enabledFactories(gate, computedomain.Config{}, 2, profiling.Config{}, activemonitoring.Config{}); err == nil {
		t.Fatal("enabled profiling accepted empty config")
	}
}
