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

package activemonitoring

import (
	"context"
	"sync"

	controller "github.com/lj-runtimekit/job-controller/pkg/controller"
)

// controllerAdapter wraps Controller to satisfy controller.Interface.
type controllerAdapter struct {
	inner   *Controller
	workers int
	ready   chan struct{}
	once    sync.Once
}

func (a *controllerAdapter) Name() string                  { return ControllerName }
func (a *controllerAdapter) Run(ctx context.Context) error { return a.inner.Run(ctx, a.workers) }
func (a *controllerAdapter) Ready() <-chan struct{}        { return a.ready }

// NewFactory returns a controller.Factory that creates a ActiveMonitoring controller with
// the given configuration. Controller-specific flags are captured at
// factory-creation time; only SharedClients is injected at runtime by the Manager.
func NewFactory(config Config, workers int) controller.Factory {
	return func(clients controller.SharedClients) (controller.Interface, error) {
		cfg := config // local copy so the factory is safe to call more than once
		cfg.KubeClient = clients.KubeClient
		cfg.DynamicClient = clients.DynamicClient
		adapter := &controllerAdapter{
			inner:   NewController(cfg),
			workers: workers,
			ready:   make(chan struct{}),
		}
		// OnReady is invoked by Controller.Run after all informer caches are
		// synced. sync.Once guards against double-close.
		adapter.inner.OnReady = func() {
			adapter.once.Do(func() { close(adapter.ready) })
		}
		return adapter, nil
	}
}
