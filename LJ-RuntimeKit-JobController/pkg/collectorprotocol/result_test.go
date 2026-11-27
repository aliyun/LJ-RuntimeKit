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
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestResultJSONRoundTrip(t *testing.T) {
	r := Result{
		Phase: PhaseFailed, Reason: ReasonStreamFailed, Message: "stream broken", StopError: "stop failed",
		ElapsedMillis: 42, OmittedContainers: 1, PodNamespace: "ns", PodName: "pod", PodUID: "uid",
		Containers: []ContainerResult{{
			ContainerID: "c1", Phase: PhaseFailed, Reason: ReasonStreamFailed, Message: "partial stream",
			ElapsedMillis: 42, OutputFiles: []string{"ns_pod_c1_trace.json", "ns_pod_c1_empty.log.partial"}, Bytes: 5, StopError: "stop failed",
		}},
	}
	want := `{"phase":"Failed","reason":"StreamFailed","message":"stream broken","stopError":"stop failed","elapsedMillis":42,"omittedContainers":1,"podNamespace":"ns","podName":"pod","podUID":"uid","containers":[{"containerID":"c1","phase":"Failed","reason":"StreamFailed","message":"partial stream","elapsedMillis":42,"outputFiles":["ns_pod_c1_trace.json","ns_pod_c1_empty.log.partial"],"bytes":5,"stopError":"stop failed"}]}`
	data := r.Marshal(TerminationMessageLimit)
	if string(data) != want {
		t.Fatalf("wire JSON = %s, want %s", data, want)
	}
	got, err := ParseResult(string(data))
	if err != nil || !reflect.DeepEqual(got, r) {
		t.Fatalf("round trip = %+v, %v, want %+v", got, err, r)
	}
}

func TestFailedJSON(t *testing.T) {
	r := Failed(ReasonInvalidArgs, "invalid mode")
	if got := string(r.Marshal(TerminationMessageLimit)); got != `{"phase":"Failed","reason":"InvalidArgs","message":"invalid mode"}` {
		t.Fatalf("failed JSON = %s", got)
	}
	if got := string((Result{Phase: PhaseSucceeded}).Marshal(TerminationMessageLimit)); got != `{"phase":"Succeeded"}` {
		t.Fatalf("optional fields not omitted: %s", got)
	}
	for _, msg := range []string{"", "not JSON", "{", `{"elapsedMillis":"bad"}`} {
		if _, err := ParseResult(msg); err == nil {
			t.Errorf("malformed JSON accepted: %q", msg)
		}
	}
}

func TestResultFitsTerminationMessage(t *testing.T) {
	r := Result{Phase: PhaseFailed, Reason: ReasonStreamFailed, Message: strings.Repeat("x", 10000)}
	for i := 0; i < 16; i++ {
		r.Containers = append(r.Containers, ContainerResult{
			ContainerID: strings.Repeat("a", 64), Phase: PhaseFailed, Message: strings.Repeat("y", 2000),
			ElapsedMillis: int64(i + 1), OutputFiles: []string{"ns_pod_c1_trace.json.partial"},
		})
	}
	before, _ := json.Marshal(r)
	data := r.Marshal(TerminationMessageLimit)
	if len(data) > TerminationMessageLimit || !json.Valid(data) {
		t.Fatalf("invalid termination JSON (%d bytes)", len(data))
	}
	got, err := ParseResult(string(data))
	if err != nil || got.Phase != PhaseFailed || got.Reason != ReasonStreamFailed || got.ElapsedMillis != 16 || len(got.Containers) != 16 {
		t.Fatalf("parse = %+v, %v", got, err)
	}
	for _, c := range got.Containers {
		if !reflect.DeepEqual(c.OutputFiles, []string{"ns_pod_c1_trace.json.partial"}) {
			t.Fatalf("truncation changed reported files: %+v", c)
		}
	}
	after, _ := json.Marshal(r)
	if string(before) != string(after) {
		t.Fatal("Marshal mutated the original result")
	}
}

func TestUnicodeTerminationResult(t *testing.T) {
	r := Failed(ReasonStreamFailed, strings.Repeat("采集失败\"\n", 5000))
	// Escaped identity fields force the final compact representation.
	r.PodName, r.PodNamespace, r.PodUID = strings.Repeat("\x00", 5000), strings.Repeat("\x00", 5000), strings.Repeat("\x00", 5000)
	data := r.Marshal(TerminationMessageLimit)
	if len(data) > TerminationMessageLimit || !json.Valid(data) || !utf8.Valid(data) {
		t.Fatalf("invalid termination JSON (%d bytes)", len(data))
	}
	got, err := ParseResult(string(data))
	if err != nil || got.Phase != PhaseFailed || got.Reason != ReasonStreamFailed || got.PodName != "" {
		t.Fatalf("compact result = %+v, %v", got, err)
	}
	for _, tc := range []struct {
		input string
		limit int
		want  string
	}{
		{"采集失败", 4, "采..."}, {"采集", 0, "..."}, {"采集", 6, "采集"}, {"abc", 2, "ab..."},
	} {
		if got := truncate(tc.input, tc.limit); !utf8.ValidString(got) || got != tc.want {
			t.Errorf("truncate(%q, %d) = %q, want %q", tc.input, tc.limit, got, tc.want)
		}
	}
}

func TestCompactionPreservesCleanupError(t *testing.T) {
	r := Failed(ReasonStreamFailed, strings.Repeat("stream failed", 1000))
	r.StopError = strings.Repeat("StopProfiling failed", 1000)
	for i := 0; i < 32; i++ {
		r.Containers = append(r.Containers, ContainerResult{ContainerID: strings.Repeat("a", 64), Phase: PhaseFailed, OutputFiles: []string{strings.Repeat("p", 1000), strings.Repeat("q", 1000)}})
	}
	data := r.Marshal(TerminationMessageLimit)
	parsed, err := ParseResult(string(data))
	if err != nil || len(data) > TerminationMessageLimit || parsed.StopError == "" || parsed.OmittedContainers != 32 || parsed.Reason != ReasonStreamFailed {
		t.Fatalf("invalid compacted result: %+v, %v", parsed, err)
	}
}

func TestCompactionPreservesElapsedDuration(t *testing.T) {
	for _, phase := range []Phase{PhaseSucceeded, PhaseFailed} {
		t.Run(string(phase), func(t *testing.T) {
			r := Result{Phase: phase, OmittedContainers: 2}
			for i := 0; i < 32; i++ {
				r.Containers = append(r.Containers, ContainerResult{ContainerID: strings.Repeat("a", 64), Phase: phase, ElapsedMillis: int64(i + 1), OutputFiles: []string{strings.Repeat("p", 1000), strings.Repeat("q", 1000)}})
			}
			data := r.Marshal(TerminationMessageLimit)
			parsed, err := ParseResult(string(data))
			if err != nil || len(data) > TerminationMessageLimit || parsed.Phase != PhaseFailed || parsed.ElapsedMillis != 32 || parsed.OmittedContainers != 34 || len(parsed.Containers) != 0 {
				t.Fatalf("duration or omitted count lost in compacted result: %+v, %v", parsed, err)
			}
			if phase == PhaseSucceeded && (parsed.Reason != ReasonOutputFailed || !strings.Contains(parsed.Message, "output file report exceeds")) {
				t.Fatalf("oversized success silently lost file paths: %+v", parsed)
			}
		})
	}
}
