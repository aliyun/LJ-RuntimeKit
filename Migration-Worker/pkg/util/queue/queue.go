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

package queue

import (
	"errors"
	"sync"
)

type Queue struct {
	lock sync.Mutex
	cond *sync.Cond

	shuttingDown bool
	itemsKey     []string
	itemsMap     map[string]interface{}
}

func NewQueue() *Queue {
	q := &Queue{
		itemsKey:     make([]string, 0),
		itemsMap:     make(map[string]interface{}),
		shuttingDown: false,
	}
	q.cond = sync.NewCond(&q.lock)
	return q
}

func (q *Queue) Len() int {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	return len(q.itemsKey)
}

func (q *Queue) GetItemKeys() []string {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	keys := make([]string, len(q.itemsKey))
	copy(keys, q.itemsKey)
	return keys
}

func (q *Queue) Push(requestID string, requestItem interface{}) error {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	if q.shuttingDown {
		return errors.New("queue is shutting down")
	}
	if _, exists := q.itemsMap[requestID]; !exists {
		q.itemsKey = append(q.itemsKey, requestID)
	}
	q.itemsMap[requestID] = requestItem
	q.cond.Signal()
	return nil
}

// PushIfAbsent atomically inserts the item only when requestID is not
// already present (waiting, running or finished). It returns
// inserted=false when an item with the same requestID exists, so callers
// can treat duplicate triggers as idempotent no-ops instead of silently
// replacing an in-flight item (which would leave the replacement
// unexecuted and its status stuck at pending forever).
func (q *Queue) PushIfAbsent(requestID string, requestItem interface{}) (bool, error) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	if q.shuttingDown {
		return false, errors.New("queue is shutting down")
	}
	if _, exists := q.itemsMap[requestID]; exists {
		return false, nil
	}
	q.itemsKey = append(q.itemsKey, requestID)
	q.itemsMap[requestID] = requestItem
	q.cond.Signal()
	return true, nil
}

func (q *Queue) Pop() (string, bool) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	for len(q.itemsKey) == 0 && !q.shuttingDown {
		q.cond.Wait()
	}
	if len(q.itemsKey) == 0 {
		// We must be shutting down.
		return "", true
	}
	itemsKey := q.itemsKey[0]
	q.itemsKey = q.itemsKey[1:]
	return itemsKey, false
}

func (q *Queue) Get(requestID string) interface{} {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	state, ok := q.itemsMap[requestID]
	if !ok {
		return nil
	}
	return state
}

func (q *Queue) Remove(requestID string) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	delete(q.itemsMap, requestID)
	return
}

func (q *Queue) ShutDown() {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	q.shuttingDown = true
	q.cond.Broadcast()
}
