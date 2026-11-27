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

package preheatjob

import (
	"testing"

	"github.com/container-migration/controller/api/v1beta1"
	"github.com/container-migration/controller/pkg/dragonfly"
	"github.com/container-migration/controller/pkg/preheat"
)

// TestIsEFCEnabled pins the backend selection rule: only efc-enable
// explicitly true (tolerating case and surrounding whitespace) goes EFC,
// everything else falls back to the default Dragonfly.
func TestIsEFCEnabled(t *testing.T) {
	tests := []struct {
		name    string
		options map[string]string
		want    bool
	}{
		{name: "enabled", options: map[string]string{preheat.OptionEFCEnable: "true"}, want: true},
		{name: "enabled case-insensitive", options: map[string]string{preheat.OptionEFCEnable: "TRUE"}, want: true},
		{name: "enabled with spaces", options: map[string]string{preheat.OptionEFCEnable: " true "}, want: true},
		{name: "explicitly disabled", options: map[string]string{preheat.OptionEFCEnable: "false"}, want: false},
		{name: "empty value", options: map[string]string{preheat.OptionEFCEnable: ""}, want: false},
		{name: "arbitrary value", options: map[string]string{preheat.OptionEFCEnable: "yes"}, want: false},
		{name: "key absent", options: map[string]string{"accessKeyId": "ak"}, want: false},
		{name: "nil options", options: nil, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := &v1beta1.PreheatJob{Spec: v1beta1.PreheatJobSpec{Options: tt.options}}
			if got := preheat.IsEFCEnabled(job); got != tt.want {
				t.Errorf("IsEFCEnabled = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestBackendFor confirms the controller routes to the corresponding backend by target-pvc coordinates / efc-enable; the default must be Dragonfly.
func TestBackendFor(t *testing.T) {
	r := &PreheatJobReconciler{
		dragonfly: dragonflyBackend{},
		efc:       efcBackend{},
		copy:      copyBackend{},
	}

	t.Run("default is dragonfly", func(t *testing.T) {
		job := &v1beta1.PreheatJob{}
		if got := r.backendFor(job).Name(); got != preheat.BackendDragonfly {
			t.Errorf("backend = %q, want %q", got, preheat.BackendDragonfly)
		}
	})

	t.Run("efc-enable=true selects efc", func(t *testing.T) {
		job := &v1beta1.PreheatJob{
			Spec: v1beta1.PreheatJobSpec{Options: map[string]string{preheat.OptionEFCEnable: "true"}},
		}
		if got := r.backendFor(job).Name(); got != preheat.BackendEFC {
			t.Errorf("backend = %q, want %q", got, preheat.BackendEFC)
		}
	})

	t.Run("target-pvc options select copy", func(t *testing.T) {
		job := &v1beta1.PreheatJob{
			Spec: v1beta1.PreheatJobSpec{Options: map[string]string{
				preheat.OptionTargetPvcName:      "dst",
				preheat.OptionTargetPvcNamespace: "default",
			}},
		}
		if got := r.backendFor(job).Name(); got != preheat.BackendCopy {
			t.Errorf("backend = %q, want %q", got, preheat.BackendCopy)
		}
	})

	t.Run("probe controller uses the same routing", func(t *testing.T) {
		p := &ProbeController{dragonfly: dragonflyBackend{}, efc: efcBackend{}, copy: copyBackend{}}
		if got := p.backendFor(&v1beta1.PreheatJob{}).Name(); got != preheat.BackendDragonfly {
			t.Errorf("backend = %q, want %q", got, preheat.BackendDragonfly)
		}
		efcJob := &v1beta1.PreheatJob{
			Spec: v1beta1.PreheatJobSpec{Options: map[string]string{preheat.OptionEFCEnable: "true"}},
		}
		if got := p.backendFor(efcJob).Name(); got != preheat.BackendEFC {
			t.Errorf("backend = %q, want %q", got, preheat.BackendEFC)
		}
		copyJob := &v1beta1.PreheatJob{
			Spec: v1beta1.PreheatJobSpec{Options: map[string]string{preheat.OptionTargetPvcName: "dst"}},
		}
		if got := p.backendFor(copyJob).Name(); got != preheat.BackendCopy {
			t.Errorf("backend = %q, want %q", got, preheat.BackendCopy)
		}
	})
}

// TestIsCopyMode pins the copy mode selection rule: any target-pvc coordinate
// key present means copy mode (filling only one is rejected by the Pending
// phase validation instead of silently falling back to Dragonfly).
func TestIsCopyMode(t *testing.T) {
	tests := []struct {
		name    string
		options map[string]string
		want    bool
	}{
		{name: "both keys", options: map[string]string{
			preheat.OptionTargetPvcName:      "dst",
			preheat.OptionTargetPvcNamespace: "default",
		}, want: true},
		{name: "name only", options: map[string]string{preheat.OptionTargetPvcName: "dst"}, want: true},
		{name: "namespace only", options: map[string]string{preheat.OptionTargetPvcNamespace: "default"}, want: true},
		{name: "empty value", options: map[string]string{preheat.OptionTargetPvcName: " "}, want: false},
		{name: "key absent", options: map[string]string{preheat.OptionEFCEnable: "true"}, want: false},
		{name: "nil options", options: nil, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := &v1beta1.PreheatJob{Spec: v1beta1.PreheatJobSpec{Options: tt.options}}
			if got := preheat.IsCopyMode(job); got != tt.want {
				t.Errorf("IsCopyMode = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestRequiresStorageCredentials pins the origin credential requirements of
// the three backends: dragonfly fetches OSS origin via presigned URLs and must
// validate AK/SK; EFC preheats the whole directory and copy does file-level
// copying inside the pod - neither needs credentials and the controller must
// not force them.
func TestRequiresStorageCredentials(t *testing.T) {
	if !(dragonflyBackend{}).RequiresStorageCredentials() {
		t.Error("dragonfly backend must require storage credentials")
	}
	if (efcBackend{}).RequiresStorageCredentials() {
		t.Error("efc backend must not require storage credentials")
	}
	if (copyBackend{}).RequiresStorageCredentials() {
		t.Error("copy backend must not require storage credentials")
	}
}

// TestDragonflyBackendParity pins that dragonflyBackend delegates exactly like
// the original dragonfly package behavior: TaskLabel is the part name. This is
// the bottom line of "abstraction must not change the Dragonfly path": one
// byte off in the part name and dfdaemon's task_id mismatches, no preheat
// cache hit at all.
func TestDragonflyBackendParity(t *testing.T) {
	var b preheat.PreheatBackend = dragonflyBackend{}

	for _, idx := range []int{0, 1, 7, 63} {
		if got, want := b.TaskLabel(idx), dragonfly.PartName(idx); got != want {
			t.Errorf("TaskLabel(%d) = %q, want %q", idx, got, want)
		}
	}
}

// TestStateConstantValues pins the literal values of the state constants:
// these strings are both the task state values of the Dragonfly manager API
// and the mapping results of dfdaemon cache states; changing one byte drifts
// the state judgement. The dragonfly package's TaskState / TaskCacheState are
// already aliases of the preheat abstraction layer types and the constants are
// the same objects (dependency direction: dragonfly references the abstraction
// layer); compile-time assertions plus literal checks double the protection.
func TestStateConstantValues(t *testing.T) {
	// Compile-time assertion: dragonfly state constants can be assigned directly to the abstraction layer types.
	var (
		_ preheat.TaskState  = dragonfly.TaskStatePending
		_ preheat.TaskState  = dragonfly.TaskStateRunning
		_ preheat.TaskState  = dragonfly.TaskStateSuccess
		_ preheat.TaskState  = dragonfly.TaskStateFailure
		_ preheat.CacheState = dragonfly.CacheStateNotFound
		_ preheat.CacheState = dragonfly.CacheStateRunning
		_ preheat.CacheState = dragonfly.CacheStateFailed
		_ preheat.CacheState = dragonfly.CacheStateCached
	)

	taskStates := map[string]string{
		string(preheat.TaskStatePending): "Pending",
		string(preheat.TaskStateRunning): "Running",
		string(preheat.TaskStateSuccess): "Success",
		string(preheat.TaskStateFailure): "Failure",
	}
	for got, want := range taskStates {
		if got != want {
			t.Errorf("TaskState = %q, want %q", got, want)
		}
	}

	cacheStates := map[string]string{
		string(preheat.CacheStateNotFound): "NotFound",
		string(preheat.CacheStateRunning):  "Running",
		string(preheat.CacheStateFailed):   "Failed",
		string(preheat.CacheStateCached):   "Cached",
	}
	for got, want := range cacheStates {
		if got != want {
			t.Errorf("CacheState = %q, want %q", got, want)
		}
	}
}
