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
