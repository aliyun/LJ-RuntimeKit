package computedomain

import (
	"context"
	"sync"

	controller "github.com/lj-runtimekit/job-controller/pkg/controller"
)

const (
	// ControllerName is the canonical name used in registration, logging,
	// and the --enable-controllers flag.
	ControllerName = "computedomain"
)

// controllerAdapter wraps the existing Controller to satisfy controller.Interface.
type controllerAdapter struct {
	inner   *Controller
	workers int
	ready   chan struct{}
	once    sync.Once
}

func (a *controllerAdapter) Name() string                  { return ControllerName }
func (a *controllerAdapter) Run(ctx context.Context) error { return a.inner.Run(ctx, a.workers) }
func (a *controllerAdapter) Ready() <-chan struct{}        { return a.ready }

// NewFactory returns a controller.Factory that creates a ComputeDomain
// controller with the given configuration. Controller-specific flags are
// captured at factory-creation time; only SharedClients is injected at
// runtime by the Manager.
func NewFactory(config Config, workers int) controller.Factory {
	return func(clients controller.SharedClients) (controller.Interface, error) {
		cfg := config // local copy so the factory is safe to call more than once
		cfg.KubeClient = clients.KubeClient
		cfg.DynamicClient = clients.DynamicClient
		// Both ComputeDomain API groups are always served; a missing CRD in
		// either group fails startup (see Controller.Run).
		inner := NewController(cfg)
		adapter := &controllerAdapter{
			inner:   inner,
			workers: workers,
			ready:   make(chan struct{}),
		}
		// OnReady is invoked by Controller.Run only when readiness is actually
		// published: both group caches synced, no shutdown started and the run
		// context still live. It is never called for a controller that failed to
		// start or was stopped, and never called twice. sync.Once guards against
		// double-close if Run is ever (erroneously) called twice.
		adapter.inner.OnReady = func() {
			adapter.once.Do(func() { close(adapter.ready) })
		}
		return adapter, nil
	}
}
