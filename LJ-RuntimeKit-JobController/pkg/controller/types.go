// Package controller defines the minimal abstraction for RuntimeKit controllers
// and provides a concurrent Manager that runs them within a single process.
package controller

import (
	"context"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

// Interface is the minimal contract every RuntimeKit controller must satisfy.
// Each controller receives a cancellable context and blocks until it finishes.
// Returning a non-nil error signals a fatal failure; the Manager will
// propagate it and terminate all sibling controllers.
type Interface interface {
	// Name returns a human-readable, unique identifier for logging and
	// diagnostics (e.g. "computedomain").
	Name() string

	// Run starts the controller and blocks until ctx is cancelled or a fatal
	// error occurs. The context is cancelled when the Manager shuts down or
	// a sibling controller fails.
	Run(ctx context.Context) error

	// Ready returns a channel that is closed once the controller has finished
	// critical initialisation (e.g. informer cache sync) and is ready to
	// process events. The Manager waits for every controller's Ready signal
	// before transitioning from PhaseStarting to PhaseRunning.
	//
	// Implementations that have no meaningful init phase may close the
	// channel at the beginning of Run.
	Ready() <-chan struct{}
}

// SharedClients holds process-level Kubernetes clients initialised once in
// main() and shared across all controllers. Controllers MUST NOT reconfigure
// these clients but MAY create their own informers and work queues.
type SharedClients struct {
	KubeClient    kubernetes.Interface
	DynamicClient dynamic.Interface
}
