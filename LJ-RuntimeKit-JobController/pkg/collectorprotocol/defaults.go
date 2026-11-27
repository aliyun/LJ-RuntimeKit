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

package collectorprotocol

import "time"

// Defaults must stay compatible with the external collector CLI defaults.
const (
	DefaultSocketPath        = "/run/amp/eKitC/xdm/sockets/dp2mgr.sock"
	DefaultPollInterval      = 2 * time.Second
	DefaultRPCTimeout        = 15 * time.Second
	DefaultStartTimeout      = 2 * time.Minute
	DefaultStreamIdleTimeout = 5 * time.Minute
	DefaultFinishTimeout     = time.Minute
	DefaultStopTimeout       = 15 * time.Second
	DefaultMaxRetries        = 3
	DefaultRetryBackoff      = 10 * time.Second
	DefaultMaxRetryBackoff   = 2 * time.Minute
	DefaultStartBusyRetries  = 5
	DefaultStartBusyBackoff  = 15 * time.Second
)
