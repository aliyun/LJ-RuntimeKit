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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"k8s.io/klog/v2"
)

type ClaimTaskEntry struct {
	TaskUID   string `json:"taskUID"`
	PodUID    string `json:"podUID"`
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

type claimTaskStore struct {
	dir   string
	mutex sync.Mutex
}

func newClaimTaskStore(dir string) *claimTaskStore {
	return &claimTaskStore{dir: dir}
}

func (s *claimTaskStore) load() (map[string]ClaimTaskEntry, error) {
	data, err := os.ReadFile(filepath.Join(s.dir, ClaimTaskFileName))
	if os.IsNotExist(err) {
		return make(map[string]ClaimTaskEntry), nil
	}
	if err != nil {
		return nil, err
	}

	entries := make(map[string]ClaimTaskEntry)
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("unmarshal claim task entries: %w", err)
	}
	if entries == nil {
		entries = make(map[string]ClaimTaskEntry)
	}
	return entries, nil
}

func (s *claimTaskStore) save(entries map[string]ClaimTaskEntry) error {
	data, err := json.Marshal(entries)
	if err != nil {
		return fmt.Errorf("marshal claim task entries: %w", err)
	}

	filePath := filepath.Join(s.dir, ClaimTaskFileName)
	tmpPath := filepath.Join(s.dir, ".claim-tasks.json.tmp")
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("create temporary claim task file: %w", err)
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("write temporary claim task file: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync temporary claim task file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close temporary claim task file: %w", err)
	}
	if err := os.Rename(tmpPath, filePath); err != nil {
		return fmt.Errorf("rename claim task file: %w", err)
	}
	cleanup = false

	// Sync parent directory to ensure the rename is durable across power loss.
	if dir, err := os.Open(s.dir); err != nil {
		klog.Warningf("claim task store: failed to open dir for sync: %v", err)
	} else {
		if err := dir.Sync(); err != nil {
			klog.Warningf("claim task store: failed to sync dir: %v", err)
		}
		_ = dir.Close()
	}
	return nil
}

func (s *claimTaskStore) Get(claimUID string) (*ClaimTaskEntry, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	entries, err := s.load()
	if err != nil {
		return nil, err
	}
	entry, ok := entries[claimUID]
	if !ok {
		return nil, nil
	}
	return &entry, nil
}

func (s *claimTaskStore) Put(claimUID string, entry ClaimTaskEntry) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	entries, err := s.load()
	if err != nil {
		return err
	}
	entries[claimUID] = entry
	return s.save(entries)
}

func (s *claimTaskStore) Delete(claimUID string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	entries, err := s.load()
	if err != nil {
		return err
	}
	if _, ok := entries[claimUID]; !ok {
		return nil
	}
	delete(entries, claimUID)
	return s.save(entries)
}

func (s *claimTaskStore) ListAll() (map[string]ClaimTaskEntry, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	entries, err := s.load()
	if err != nil {
		return nil, err
	}
	result := make(map[string]ClaimTaskEntry, len(entries))
	for claimUID, entry := range entries {
		result[claimUID] = entry
	}
	return result, nil
}
