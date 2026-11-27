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

import (
	"encoding/json"
	"unicode/utf8"
)

// Phase is the overall outcome of a collector run.
type Phase string

const (
	PhaseSucceeded Phase = "Succeeded"
	PhaseFailed    Phase = "Failed"
)

// Failure reasons reported to the controller.
const (
	ReasonInvalidArgs       = "InvalidArgs"
	ReasonSocketUnavailable = "SocketUnavailable"
	ReasonStartFailed       = "StartFailed"
	ReasonStartBusy         = "StartBusy"
	ReasonStartTimeout      = "StartTimeout"
	ReasonQueryFailed       = "QueryFailed"
	ReasonProfilingFailed   = "ProfilingFailed"
	ReasonProfilingCanceled = "ProfilingCanceled"
	ReasonStreamFailed      = "StreamFailed"
	ReasonOutputFailed      = "OutputFailed"
	ReasonFinishTimeout     = "FinishTimeout"
	ReasonInterrupted       = "Interrupted"
	ReasonStopFailed        = "StopFailed"
	ReasonUnexpectedStatus  = "UnexpectedStatus"
	ReasonCleanupFailed     = "CleanupFailed"
)

// TerminationMessageLimit is the kubelet cap on a container termination message.
const TerminationMessageLimit = 4096

// Result is written as JSON to the container termination message so the
// controller can read it from pod.status.containerStatuses[].state.terminated.message.
type Result struct {
	Phase             Phase             `json:"phase"`
	Reason            string            `json:"reason,omitempty"`
	Message           string            `json:"message,omitempty"`
	StopError         string            `json:"stopError,omitempty"`
	ElapsedMillis     int64             `json:"elapsedMillis,omitempty"`
	OmittedContainers int               `json:"omittedContainers,omitempty"`
	PodNamespace      string            `json:"podNamespace,omitempty"`
	PodName           string            `json:"podName,omitempty"`
	PodUID            string            `json:"podUID,omitempty"`
	Containers        []ContainerResult `json:"containers,omitempty"`
}

// ContainerResult is the outcome for a single target container.
type ContainerResult struct {
	ContainerID string `json:"containerID"`
	Phase       Phase  `json:"phase"`
	Reason      string `json:"reason,omitempty"`
	Message     string `json:"message,omitempty"`
	// ElapsedMillis is the collected duration last reported by Runtime Kit.
	ElapsedMillis int64    `json:"elapsedMillis,omitempty"`
	OutputFiles   []string `json:"outputFiles,omitempty"`
	Bytes         int64    `json:"bytes,omitempty"`
	StopError     string   `json:"stopError,omitempty"`
}

// Failed builds a failed Result that carries no per-container detail.
func Failed(reason, message string) Result {
	return Result{Phase: PhaseFailed, Reason: reason, Message: message}
}

// Marshal encodes r, shortening messages until it fits within limit bytes.
func (r Result) Marshal(limit int) []byte {
	for _, c := range r.Containers {
		r.ElapsedMillis = max(r.ElapsedMillis, c.ElapsedMillis)
	}
	for _, n := range []int{-1, 512, 128, 32} {
		b, _ := json.Marshal(r.truncated(n))
		if len(b) <= limit {
			return b
		}
	}
	slim := r.truncated(32)
	// Successful reports must not lose the stream's actual file names.
	if slim.Phase == PhaseSucceeded {
		slim.Phase = PhaseFailed
		slim.Reason = ReasonOutputFailed
		slim.Message = "output file report exceeds termination message limit; output files are retained on PVC"
	}
	slim.OmittedContainers += len(slim.Containers)
	slim.Containers = nil
	slim.PodName = truncate(slim.PodName, 253)
	slim.PodNamespace = truncate(slim.PodNamespace, 253)
	slim.PodUID = truncate(slim.PodUID, 253)
	b, _ := json.Marshal(slim)
	if len(b) <= limit {
		return b
	}
	b, _ = json.Marshal(Result{
		Phase: slim.Phase, Reason: truncate(slim.Reason, 32), Message: truncate(slim.Message, 32),
		StopError: truncate(slim.StopError, 32), ElapsedMillis: slim.ElapsedMillis, OmittedContainers: slim.OmittedContainers,
	})
	return b
}

func (r Result) truncated(n int) Result {
	if n < 0 {
		return r
	}
	out := r
	out.Message = truncate(r.Message, n)
	out.StopError = truncate(r.StopError, n)
	out.Containers = make([]ContainerResult, len(r.Containers))
	for i, c := range r.Containers {
		c.Message = truncate(c.Message, n)
		c.StopError = truncate(c.StopError, n)
		out.Containers[i] = c
	}
	return out
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n] + "..."
}

// ParseResult decodes a collector termination message.
func ParseResult(msg string) (Result, error) {
	var r Result
	err := json.Unmarshal([]byte(msg), &r)
	return r, err
}
