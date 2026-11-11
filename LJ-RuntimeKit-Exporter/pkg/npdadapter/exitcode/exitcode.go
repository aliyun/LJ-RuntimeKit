// Package exitcode defines exit codes for NPD custom plugin processes.
package exitcode

const (
	// Healthy maps to NPD condition=False.
	Healthy = 0
	// Unhealthy maps to NPD condition=True.
	Unhealthy = 1
	// Unknown indicates an unknown status, a missing category, or stale data.
	Unknown = 2
	// Error indicates an argument, connection, or RPC failure; NPD maps it to Unknown.
	Error = 3
)
