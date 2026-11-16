package controller

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"k8s.io/klog/v2"
)

// ManagerPhase describes the lifecycle state of a Manager.
//
// Normal path:  Idle → Starting → Running → Stopping → Stopped
// Failure:      any non-Failed state  →  Failed  (absorbing)
//
// Once Failed is reached it is never overwritten.
type ManagerPhase int32

const (
	// PhaseIdle — Manager created but Run() has not been called.
	// A standby (non-leader) replica stays in this phase.
	PhaseIdle ManagerPhase = iota
	// PhaseStarting — Run() entered; controller goroutines launched but not
	// all have signalled Ready yet.
	PhaseStarting
	// PhaseRunning — Every controller has signalled Ready.
	PhaseRunning
	// PhaseStopping — Graceful context cancellation detected; waiting for
	// controller goroutines to drain.
	PhaseStopping
	// PhaseStopped — All goroutines finished after a graceful shutdown.
	PhaseStopped
	// PhaseFailed — A controller returned an error, exited unexpectedly, or
	// no controllers were provided. This is an absorbing terminal state:
	// once set, no other phase can overwrite it.
	PhaseFailed
)

// String implements fmt.Stringer for logging and health endpoints.
func (p ManagerPhase) String() string {
	switch p {
	case PhaseIdle:
		return "Idle"
	case PhaseStarting:
		return "Starting"
	case PhaseRunning:
		return "Running"
	case PhaseStopping:
		return "Stopping"
	case PhaseStopped:
		return "Stopped"
	case PhaseFailed:
		return "Failed"
	default:
		return fmt.Sprintf("Unknown(%d)", int32(p))
	}
}

// Manager runs a set of controllers concurrently within a single process.
// It propagates the first fatal error from any controller to the caller and
// exposes a phase-based health signal.
//
// Health / readiness semantics (used by the /healthz HTTP handler):
//
//	PhaseIdle     — process alive, standby (not yet leading)  → 200
//	PhaseStarting — leader initialising, caches syncing       → 503
//	PhaseRunning  — all controllers ready and processing      → 200
//	PhaseStopping — graceful shutdown in progress             → 503
//	PhaseStopped  — shutdown complete                         → 503
//	PhaseFailed   — controller error / unexpected exit        → 503
//
// Lifecycle: NewManager → Run (blocks) → returns.
type Manager struct {
	controllers []Interface
	phase       atomic.Int32 // stores ManagerPhase
}

// NewManager creates a Manager for the given controllers.
// The initial phase is PhaseIdle.
func NewManager(controllers []Interface) *Manager {
	// atomic.Int32 zero-value is 0 == PhaseIdle.
	return &Manager{controllers: controllers}
}

// Run starts every controller in its own goroutine and blocks until:
//   - ctx is cancelled (graceful shutdown), or
//   - any controller returns a non-nil error (fatal failure), or
//   - any controller returns nil while ctx is still active (unexpected exit).
//
// The phase moves to PhaseStarting immediately, then to PhaseRunning once
// every controller signals Ready.  On any failure the phase is set to
// PhaseFailed (absorbing) *before* sibling cancellation, eliminating the
// select race between errCh and ctx.Done().
func (m *Manager) Run(ctx context.Context) error {
	if len(m.controllers) == 0 {
		m.setFailed()
		return fmt.Errorf("no controllers to run")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	m.advancePhase(PhaseStarting)

	errCh := make(chan error, len(m.controllers))
	var wg sync.WaitGroup

	for _, c := range m.controllers {
		c := c
		wg.Add(1)
		go func() {
			defer wg.Done()
			klog.InfoS("Starting controller", "controller", c.Name())
			if err := c.Run(ctx); err != nil {
				klog.ErrorS(err, "Controller failed", "controller", c.Name())
				m.setFailed() // absorbing — before cancel, preventing race
				errCh <- fmt.Errorf("controller %s: %w", c.Name(), err)
				cancel()
			} else if ctx.Err() == nil {
				// Controller returned nil without the context being
				// cancelled — treat as unexpected early exit.
				klog.ErrorS(nil, "Controller exited unexpectedly",
					"controller", c.Name())
				m.setFailed()
				errCh <- fmt.Errorf("controller %s: unexpected early exit (returned nil without context cancellation)", c.Name())
				cancel()
			} else {
				klog.InfoS("Controller stopped", "controller", c.Name())
			}
		}()
	}

	// Wait for every controller to signal Ready, or ctx cancellation.
	if m.awaitReady(ctx) {
		m.advancePhase(PhaseRunning)
	}
	// If awaitReady returned false the context was cancelled — either by
	// a failing goroutine (phase already Failed) or by external shutdown
	// (phase still Starting, will move to Stopping below).

	// Block until a controller fails or the parent context is cancelled.
	var firstErr error
	select {
	case firstErr = <-errCh:
		m.setFailed()
		cancel()
	case <-ctx.Done():
		m.advancePhase(PhaseStopping)
	}

	wg.Wait()

	// Drain a remaining error if we exited via ctx.Done().
	if firstErr == nil {
		select {
		case firstErr = <-errCh:
			m.setFailed()
		default:
		}
	}

	// Terminal: Stopped only if not already Failed.
	m.advancePhase(PhaseStopped)

	return firstErr
}

// Phase returns the current lifecycle phase of the Manager.
func (m *Manager) Phase() ManagerPhase {
	return ManagerPhase(m.phase.Load())
}

// IsHealthy returns true only while all controllers are running and ready.
func (m *Manager) IsHealthy() bool {
	return m.Phase() == PhaseRunning
}

// ReadinessOK returns true for phases where the K8s readiness probe should
// return 200: PhaseIdle (standby) and PhaseRunning (healthy leader).
func (m *Manager) ReadinessOK() bool {
	p := m.Phase()
	return p == PhaseIdle || p == PhaseRunning
}

// -----------------------------------------------------------------------
// Internal helpers
// -----------------------------------------------------------------------

// advancePhase moves to target only if current < target and not Failed.
// PhaseFailed is the highest value, so it is never overwritten.
func (m *Manager) advancePhase(target ManagerPhase) bool {
	for {
		cur := m.phase.Load()
		if cur == int32(PhaseFailed) {
			return false // absorbing terminal
		}
		if int32(target) <= cur {
			return false // already at or past target
		}
		if m.phase.CompareAndSwap(cur, int32(target)) {
			return true
		}
	}
}

// setFailed unconditionally moves to PhaseFailed from any non-Failed state.
func (m *Manager) setFailed() {
	for {
		cur := m.phase.Load()
		if cur == int32(PhaseFailed) {
			return
		}
		if m.phase.CompareAndSwap(cur, int32(PhaseFailed)) {
			return
		}
	}
}

// awaitReady blocks until every controller's Ready channel is closed or ctx
// is cancelled. Returns true only if all controllers became ready.
func (m *Manager) awaitReady(ctx context.Context) bool {
	for _, c := range m.controllers {
		select {
		case <-c.Ready():
		case <-ctx.Done():
			return false
		}
	}
	return true
}
