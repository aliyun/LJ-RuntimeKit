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
	"fmt"

	"k8s.io/component-base/featuregate"

	controller "github.com/lj-runtimekit/job-controller/pkg/controller"
	"github.com/lj-runtimekit/job-controller/pkg/controller/activemonitoring"
	"github.com/lj-runtimekit/job-controller/pkg/controller/computedomain"
	"github.com/lj-runtimekit/job-controller/pkg/controller/profiling"
	"github.com/lj-runtimekit/job-controller/pkg/features"
)

func enabledFactories(gate featuregate.FeatureGate, computeConfig computedomain.Config, workers int, profilingConfig profiling.Config, amConfig activemonitoring.Config) (map[string]controller.Factory, error) {
	factories := make(map[string]controller.Factory)
	if gate.Enabled(features.ComputeDomain) {
		if computeConfig.IMEXConfigOverrides != "" {
			return nil, fmt.Errorf("--imex-config-overrides is not implemented; non-empty values are unsupported")
		}
		factories[computedomain.ControllerName] = computedomain.NewFactory(computeConfig, workers)
	}
	if gate.Enabled(features.Profiling) {
		if err := profilingConfig.Validate(); err != nil {
			return nil, err
		}
		factories[profiling.ControllerName] = profiling.NewFactory(profilingConfig)
	}
	if gate.Enabled(features.ActiveMonitoring) {
		factories[activemonitoring.ControllerName] = activemonitoring.NewFactory(amConfig, workers)
	}
	if len(factories) == 0 {
		return nil, fmt.Errorf("at least one controller must be enabled via --feature-gates")
	}
	return factories, nil
}
