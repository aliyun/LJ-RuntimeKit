/*
Copyright (c) 2026 Alibaba Cloud.

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

package computedomain

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func TestClaimTaskStorePutGet(t *testing.T) {
	dir := t.TempDir()
	store := newClaimTaskStore(dir)
	want := ClaimTaskEntry{
		TaskUID:   "task-1",
		PodUID:    "pod-1",
		Name:      "claim-1",
		Namespace: "default",
	}

	if err := store.Put("claim-uid-1", want); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	got, err := store.Get("claim-uid-1")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got == nil || !reflect.DeepEqual(*got, want) {
		t.Fatalf("Get() = %#v, want %#v", got, want)
	}

	info, err := os.Stat(filepath.Join(dir, ClaimTaskFileName))
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if gotMode := info.Mode().Perm(); gotMode != 0600 {
		t.Fatalf("file mode = %o, want 600", gotMode)
	}
	if _, err := os.Stat(filepath.Join(dir, ".claim-tasks.json.tmp")); !os.IsNotExist(err) {
		t.Fatalf("temporary file remains after save: %v", err)
	}
}

func TestClaimTaskStoreMultipleEntriesAndDelete(t *testing.T) {
	store := newClaimTaskStore(t.TempDir())
	entries := map[string]ClaimTaskEntry{
		"claim-1": {TaskUID: "task-1", PodUID: "pod-1", Name: "one", Namespace: "ns"},
		"claim-2": {TaskUID: "task-2", PodUID: "pod-2", Name: "two", Namespace: "ns"},
	}
	for claimUID, entry := range entries {
		if err := store.Put(claimUID, entry); err != nil {
			t.Fatalf("Put(%q) error = %v", claimUID, err)
		}
	}

	if err := store.Delete("claim-1"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	got, err := store.ListAll()
	if err != nil {
		t.Fatalf("ListAll() error = %v", err)
	}
	want := map[string]ClaimTaskEntry{"claim-2": entries["claim-2"]}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ListAll() = %#v, want %#v", got, want)
	}
}

func TestClaimTaskStoreConcurrentPutDelete(t *testing.T) {
	store := newClaimTaskStore(t.TempDir())
	const count = 64
	for i := 0; i < count; i++ {
		claimUID := fmt.Sprintf("claim-%d", i)
		if err := store.Put(claimUID, ClaimTaskEntry{TaskUID: "initial"}); err != nil {
			t.Fatalf("Put(%q) error = %v", claimUID, err)
		}
	}

	errs := make(chan error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			claimUID := fmt.Sprintf("claim-%d", i)
			if i < count/2 {
				errs <- store.Delete(claimUID)
				return
			}
			errs <- store.Put(claimUID, ClaimTaskEntry{TaskUID: fmt.Sprintf("task-%d", i)})
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent operation error = %v", err)
		}
	}

	got, err := store.ListAll()
	if err != nil {
		t.Fatalf("ListAll() error = %v", err)
	}
	if len(got) != count/2 {
		t.Fatalf("ListAll() length = %d, want %d", len(got), count/2)
	}
	for i := count / 2; i < count; i++ {
		claimUID := fmt.Sprintf("claim-%d", i)
		if got[claimUID].TaskUID != fmt.Sprintf("task-%d", i) {
			t.Errorf("entry %q = %#v", claimUID, got[claimUID])
		}
	}
}

func TestClaimTaskStoreMissingFileReturnsEmptyMap(t *testing.T) {
	got, err := newClaimTaskStore(t.TempDir()).ListAll()
	if err != nil {
		t.Fatalf("ListAll() error = %v", err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("ListAll() = %#v, want non-nil empty map", got)
	}
}

func TestClaimTaskStoreCorruptJSONReturnsError(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, ClaimTaskFileName), []byte("{"), 0600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	if _, err := newClaimTaskStore(dir).ListAll(); err == nil {
		t.Fatal("ListAll() error = nil, want JSON decoding error")
	}
}

func TestClaimTaskStoreGetMissingReturnsNil(t *testing.T) {
	got, err := newClaimTaskStore(t.TempDir()).Get("missing")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got != nil {
		t.Fatalf("Get() = %#v, want nil", got)
	}
}

func TestClaimTaskStoreDeleteMissingSucceeds(t *testing.T) {
	if err := newClaimTaskStore(t.TempDir()).Delete("missing"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
}

func TestClaimTaskStoreListAllReturnsCopy(t *testing.T) {
	store := newClaimTaskStore(t.TempDir())
	want := ClaimTaskEntry{TaskUID: "task-1", PodUID: "pod-1"}
	if err := store.Put("claim-1", want); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	first, err := store.ListAll()
	if err != nil {
		t.Fatalf("ListAll() error = %v", err)
	}
	first["claim-1"] = ClaimTaskEntry{TaskUID: "changed"}
	first["claim-2"] = ClaimTaskEntry{TaskUID: "added"}

	second, err := store.ListAll()
	if err != nil {
		t.Fatalf("ListAll() error = %v", err)
	}
	if len(second) != 1 || !reflect.DeepEqual(second["claim-1"], want) {
		t.Fatalf("ListAll() after mutating result = %#v, want original entry", second)
	}
}
