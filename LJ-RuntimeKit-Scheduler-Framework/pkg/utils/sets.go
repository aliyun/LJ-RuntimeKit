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

package utils

// Sets is a set of integers implemented as a map.
type Sets map[int]struct{}

// New creates a new empty Sets.
func New() *Sets {
	s := new(Sets)
	*s = make(map[int]struct{})
	return s
}

// Insert adds the given IDs to the set.
func (s Sets) Insert(ids ...int) {
	for _, id := range ids {
		s[id] = struct{}{}
	}
	return
}

// Has returns true if the given ID is in the set.
func (s Sets) Has(id int) bool {
	_, ok := s[id]
	return ok
}

// Clone returns a deep copy of the set.
func (s Sets) Clone() *Sets {
	out := New()
	for key := range s {
		out.Insert(key)
	}
	return out
}
