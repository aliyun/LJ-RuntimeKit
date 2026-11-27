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

package migrationjob

import (
	"strings"
	"testing"
	"time"

	"github.com/container-migration/controller/api/v1beta1"
	"github.com/container-migration/controller/pkg/controllers"
	"github.com/container-migration/controller/pkg/controllers/migrationjob/handler"
)

// TestSkipProducesNoLogLine pins the direct semantics of this change: the
// switch in the Reconcile loop takes the empty branch for Skip (no log line),
// while Continue/Done/Abort all fall into the default branch and print a line.
//
// IsSkip's classification result represents that switch's direction: only
// Skip produces no line, everything else produces a line.
func TestSkipProducesNoLogLine(t *testing.T) {
	cases := []struct {
		name     string
		result   controllers.Result
		wantLine bool
	}{
		{"Skip: no line", controllers.NewSkipResult("not applicable for phase Restoring"), false},
		{"Continue: line", controllers.NewContinueResult(), true},
		{"Done: line", controllers.NewDoneResult(), true},
		{"Abort: line", controllers.NewAbortWithRetryAfterResult(time.Second), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if gotLine := !c.result.IsSkip(); gotLine != c.wantLine {
				t.Fatalf("whether a log line is expected: got %v, want %v", gotLine, c.wantLine)
			}
		})
	}
}

// TestEveryRoundHasNonSkipHandler pins the precondition of the "no skip lines"
// safety: in any phase at least one handler returns non-Skip, otherwise the
// round between START/END would be empty and completely inexplicable.
//
// The guarantee comes from the two boundary handlers splitting the phase space
// into two complementary halves:
//   - non-terminal: RoundCheckHandler returns Continue/Abort (never Skip)
//   - terminal: CleanHandler returns Done (never Skip)
//
// The Skip branch conditions of these two handlers must always be mutually
// exclusive - the day someone adds a non-terminal Skip branch to RoundCheck or
// a terminal Skip branch to Clean, this test alarms.
// branch, or a terminal Skip branch to Clean, this test alarms.
func TestEveryRoundHasNonSkipHandler(t *testing.T) {
	terminal := []v1beta1.JobPhase{
		v1beta1.JobPhaseSucceeded, v1beta1.JobPhasePartiallySucceeded,
		v1beta1.JobPhaseFailed, v1beta1.JobPhaseTerminated,
	}
	nonTerminal := []v1beta1.JobPhase{
		v1beta1.JobPhasePending, v1beta1.JobPhaseCheckpointing,
		v1beta1.JobPhaseRestoring,
	}

	// Terminal: RoundCheck skips, so Clean must not skip.
	for _, phase := range terminal {
		if handler.RoundCheckSkipsPhase(phase) != true {
			t.Fatalf("phase=%s: RoundCheck should skip in terminal phases", phase)
		}
		if handler.CleanSkipsPhase(phase) {
			t.Fatalf("phase=%s: Clean must not skip in terminal phases, otherwise the round is all-Skip with no log line", phase)
		}
	}

	// Non-terminal: Clean skips, so RoundCheck must not skip.
	for _, phase := range nonTerminal {
		if handler.CleanSkipsPhase(phase) != true {
			t.Fatalf("phase=%s: Clean should skip in non-terminal phases", phase)
		}
		if handler.RoundCheckSkipsPhase(phase) {
			t.Fatalf("phase=%s: RoundCheck must not skip in non-terminal phases, otherwise the round is all-Skip with no log line", phase)
		}
	}
}

// TestHandlerOutcome_SkipCarriesReason pins "no skip lines does not mean
// losing information": after the per-handler SKIP lines are removed, the last
// handler's skip reason is the only clue of why this round ended the way it
// did; it must appear in the END line.
func TestHandlerOutcome_SkipCarriesReason(t *testing.T) {
	got := handlerOutcome(controllers.NewSkipResult("job not in terminal state (Restoring)"))

	if !strings.HasPrefix(got, "SKIP(") {
		t.Fatalf("expected SKIP(...) form, got %q", got)
	}
	if !strings.Contains(got, "job not in terminal state (Restoring)") {
		t.Fatalf("skip reason lost: %q", got)
	}
}

func TestHandlerOutcome_OtherKinds(t *testing.T) {
	if got := handlerOutcome(controllers.NewDoneResult()); got != "DONE" {
		t.Fatalf("Done -> %q", got)
	}
	if got := handlerOutcome(controllers.NewContinueResult()); got != "CONTINUE" {
		t.Fatalf("Continue -> %q", got)
	}
	got := handlerOutcome(controllers.NewAbortWithRetryAfterResult(3 * time.Second))
	if !strings.Contains(got, "ABORT") || !strings.Contains(got, "3s") {
		t.Fatalf("Abort should carry requeueAfter, got %q", got)
	}
}
