package controller

import (
	"fmt"
	"sort"
	"sync"
)

// Factory creates a controller instance from shared process-level clients.
// Controller-specific configuration (CLI flags, etc.) is captured by the
// closure at registration time; SharedClients is the only runtime input
// provided by the Manager.
type Factory func(clients SharedClients) (Interface, error)

var (
	registryMu sync.RWMutex
	registry   = make(map[string]Factory)
)

// Register adds a named controller factory to the global registry.
// It panics if a factory with the same name is already registered,
// preventing silent conflicts between controller packages.
func Register(name string, factory Factory) {
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, exists := registry[name]; exists {
		panic(fmt.Sprintf("controller %q is already registered", name))
	}
	registry[name] = factory
}

// Get returns the factory for the named controller, or (nil, false) if the
// name is not registered.
func Get(name string) (Factory, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	f, ok := registry[name]
	return f, ok
}

// KnownControllers returns the sorted names of all registered controllers.
func KnownControllers() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// resetRegistry clears the global registry. Exported for tests only.
func resetRegistry() {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry = make(map[string]Factory)
}
