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

package controller

// WorkState models the lifecycle of a queued task (checkpoint / restore)
// inside the scheduler.
//
// State transitions:
//
//	WaitingState -> RunningState -> {SucceedState | FailedState}
//
// CleanTask uses the state to decide whether to cancel an in-flight
// task and / or reclaim staging directories.
type WorkState string

var (
	UnknownState WorkState = "unknown"
	RunningState WorkState = "running"
	WaitingState WorkState = "waiting"
	SucceedState WorkState = "succeed"
	FailedState  WorkState = "failed"
)

// Worker-side gRPC response status codes. They mirror the codes used
// by the controller so the two sides agree on the meaning of each
// numeric value.
const (
	StatusOK            = 0
	StatusPending       = 2
	StatusNotFound      = 3
	StatusBadRequest    = 1003
	StatusPodNotFound   = 1004
	StatusInternalError = 1005
)
