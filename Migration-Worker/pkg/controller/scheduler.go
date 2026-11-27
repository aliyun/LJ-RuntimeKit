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

import (
	"os"
	"time"

	"github.com/container-migration/worker/cmd/migration-worker/options"
	migruntime "github.com/container-migration/worker/pkg/runtime"
	mgutil "github.com/container-migration/worker/pkg/util"
	"github.com/container-migration/worker/pkg/util/queue"

	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
)

// Scheduler owns the two task queues (checkpoint / restore) and the
// underlying ContainerRuntime instance. It hosts a configurable pool
// of worker goroutines that consume each queue.
type Scheduler struct {
	Opts    *options.Options
	SaveDir string
	Runtime migruntime.ContainerRuntime

	checkpointItemQueue *queue.Queue
	restoreItemQueue    *queue.Queue
}

// NewScheduler builds a Scheduler:
//   - ensures SaveDir exists;
//   - initializes the XDM runtime;
//   - prepares the task queues.
func NewScheduler(opts *options.Options) (*Scheduler, error) {
	s := &Scheduler{
		Opts:                opts,
		checkpointItemQueue: queue.NewQueue(),
		restoreItemQueue:    queue.NewQueue(),
	}
	klog.V(4).Infof("Checkpoint engine is XDM at %s, default homeDir is %s", opts.XDMSocket, opts.SaveDir)

	s.SaveDir = opts.SaveDir
	if _, err := os.Stat(s.SaveDir); err != nil {
		if !os.IsNotExist(err) {
			klog.Errorf("Failed to stat %s: %v", s.SaveDir, err)
			return nil, err
		}
		klog.V(4).Infof("Dir %s does not exist, creating it", s.SaveDir)
		if err := os.MkdirAll(s.SaveDir, 0755); err != nil {
			klog.Errorf("Failed to mkdir -p %s: %v", s.SaveDir, err)
			return nil, err
		}
	}

	s.Runtime = migruntime.NewXDMRuntime(opts.XDMSocket)
	if err := s.Runtime.Init(); err != nil {
		klog.Errorf("Failed to init checkpoint engine %s: %v", s.Runtime.Name(), err)
		return nil, err
	}
	return s, nil
}

// --- Checkpoint queue operations ---

func (s *Scheduler) AddCheckpointItemToQueue(request interface{}) error {
	migrationItem, err := NewCheckpointItem(s, request)
	if err != nil {
		klog.Errorf("Failed to new checkpoint item: %v", err)
		return err
	}
	// PushIfAbsent keeps the first item authoritative: a duplicate request
	// with the same requestID must never replace a waiting/running item,
	// otherwise the replacement is never scheduled and the request freezes
	// in WaitingState forever.
	inserted, err := s.checkpointItemQueue.PushIfAbsent(migrationItem.RequestID, migrationItem)
	if err != nil {
		klog.Errorf("Failed to push checkpoint item to queue: %v", err)
		return err
	}
	if !inserted {
		klog.V(4).Infof("Checkpoint item %s already exists, skip duplicate enqueue", migrationItem.RequestID)
	}
	return nil
}

func (s *Scheduler) PopCheckpointItemFromQueue() (*CheckpointItem, bool) {
	itemKey, quit := s.checkpointItemQueue.Pop()
	if quit {
		return nil, true
	}
	item := s.checkpointItemQueue.Get(itemKey)
	if item == nil {
		return nil, false
	}
	migrationItem, ok := item.(*CheckpointItem)
	if !ok {
		return nil, false
	}
	migrationItem.State = RunningState
	return migrationItem, false
}

func (s *Scheduler) GetCheckpointItemFromQueue(requestID string) (*CheckpointItem, bool) {
	item := s.checkpointItemQueue.Get(requestID)
	if item == nil {
		return nil, false
	}
	migrationItem, ok := item.(*CheckpointItem)
	if !ok {
		return nil, false
	}
	return migrationItem, true
}

func (s *Scheduler) GetWaitingCheckpointTaskNum(requestID string) (int, bool) {
	item, ok := s.GetCheckpointItemFromQueue(requestID)
	if !ok || item.State != WaitingState {
		return 0, false
	}
	for num, key := range s.checkpointItemQueue.GetItemKeys() {
		if key == requestID {
			return num, true
		}
	}
	return 0, false
}

// --- Restore queue operations ---

func (s *Scheduler) AddRestoreItemToQueue(request interface{}) error {
	migrationItem, err := NewRestoreItem(s, request)
	if err != nil {
		klog.Errorf("Failed to new restore item: %v", err)
		return err
	}
	// PushIfAbsent: same idempotency guarantee as the checkpoint queue —
	// the manager-level check is a non-atomic Get-then-Add, two concurrent
	// duplicate requests could both pass it, so the queue must arbitrate.
	inserted, err := s.restoreItemQueue.PushIfAbsent(migrationItem.RequestID, migrationItem)
	if err != nil {
		klog.Errorf("Failed to push restore item to queue: %v", err)
		return err
	}
	if !inserted {
		klog.V(4).Infof("Restore item %s already exists, skip duplicate enqueue", migrationItem.RequestID)
	}
	return nil
}

func (s *Scheduler) PopRestoreItemFromQueue() (*RestoreItem, bool) {
	itemKey, quit := s.restoreItemQueue.Pop()
	if quit {
		return nil, true
	}
	item := s.restoreItemQueue.Get(itemKey)
	if item == nil {
		return nil, false
	}
	migrationItem, ok := item.(*RestoreItem)
	if !ok {
		return nil, false
	}
	migrationItem.State = RunningState
	return migrationItem, false
}

func (s *Scheduler) GetRestoreItemFromQueue(requestID string) (*RestoreItem, bool) {
	item := s.restoreItemQueue.Get(requestID)
	if item == nil {
		return nil, false
	}
	migrationItem, ok := item.(*RestoreItem)
	if !ok {
		return nil, false
	}
	return migrationItem, true
}

func (s *Scheduler) GetWaitingRestoreTaskNum(requestID string) (int, bool) {
	item, ok := s.GetRestoreItemFromQueue(requestID)
	if !ok || item.State != WaitingState {
		return 0, false
	}
	for num, key := range s.restoreItemQueue.GetItemKeys() {
		if key == requestID {
			return num, true
		}
	}
	return 0, false
}

// --- Common ---

// RemoveItemFromQueue removes the item with the given requestID from
// both queues. Safe to call when the item is in only one (or neither).
func (s *Scheduler) RemoveItemFromQueue(requestID string) {
	s.checkpointItemQueue.Remove(requestID)
	s.restoreItemQueue.Remove(requestID)
}

// Run starts the worker goroutines for both queues and periodic load
// reporters. It blocks until stopCh is closed.
func (s *Scheduler) Run(stopCh <-chan struct{}) {
	defer runtime.HandleCrash()
	defer s.checkpointItemQueue.ShutDown()
	defer s.restoreItemQueue.ShutDown()

	for i := 0; i < s.Opts.CheckpointParallelism; i++ {
		go wait.Until(s.startCheckpointWorker, time.Second, stopCh)
	}
	go wait.Until(s.reportCheckpointLoad, 15*time.Second, stopCh)

	for i := 0; i < s.Opts.RestoreParallelism; i++ {
		go wait.Until(s.startRestoreWorker, time.Second, stopCh)
	}
	go wait.Until(s.reportRestoreLoad, 15*time.Second, stopCh)

	klog.V(4).Infof("Starting migration scheduler, checkpoint parallelism: %d, restore parallelism: %d",
		s.Opts.CheckpointParallelism, s.Opts.RestoreParallelism)
	<-stopCh
	klog.V(4).Info("Stopping migration scheduler...")
}

func (s *Scheduler) reportCheckpointLoad() {
	mgutil.RecordMigrationWorkerTaskLoad(migruntime.NodeName, "checkpoint", float64(s.checkpointItemQueue.Len()))
}

func (s *Scheduler) reportRestoreLoad() {
	mgutil.RecordMigrationWorkerTaskLoad(migruntime.NodeName, "restore", float64(s.restoreItemQueue.Len()))
}

func (s *Scheduler) startCheckpointWorker() {
	workFunc := func() bool {
		migrationItem, quit := s.PopCheckpointItemFromQueue()
		if quit {
			return true
		}
		if migrationItem == nil {
			klog.V(4).Info("Checkpoint item may have been cleaned")
			return false
		}
		migrationItem.LaunchCheckpointItem()
		return false
	}
	for {
		if quit := workFunc(); quit {
			return
		}
	}
}

func (s *Scheduler) startRestoreWorker() {
	workFunc := func() bool {
		migrationItem, quit := s.PopRestoreItemFromQueue()
		if quit {
			return true
		}
		if migrationItem == nil {
			klog.V(4).Info("Restore item may have been cleaned")
			return false
		}
		migrationItem.LaunchRestoreItem()
		return false
	}
	for {
		if quit := workFunc(); quit {
			return
		}
	}
}
