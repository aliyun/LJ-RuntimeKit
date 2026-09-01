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

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSets_New(t *testing.T) {
	s := New()
	assert.NotNil(t, s)
	assert.Empty(t, *s)
}

func TestSets_Insert(t *testing.T) {
	s := New()

	// Single insert
	s.Insert(1)
	assert.True(t, s.Has(1))
	assert.Equal(t, 1, len(*s))

	// Multiple insert
	s.Insert(2, 3, 4)
	assert.True(t, s.Has(2))
	assert.True(t, s.Has(3))
	assert.True(t, s.Has(4))
	assert.Equal(t, 4, len(*s))

	// Duplicate insert should not increase size
	s.Insert(2)
	assert.Equal(t, 4, len(*s))
}

func TestSets_Has(t *testing.T) {
	s := New()
	s.Insert(1, 2, 3)

	assert.True(t, s.Has(1))
	assert.True(t, s.Has(2))
	assert.True(t, s.Has(3))
	assert.False(t, s.Has(4))
	assert.False(t, s.Has(0))
	assert.False(t, s.Has(-1))
}

func TestSets_Clone(t *testing.T) {
	s := New()
	s.Insert(1, 2, 3)

	cloned := s.Clone()
	assert.Equal(t, len(*s), len(*cloned))
	assert.True(t, cloned.Has(1))
	assert.True(t, cloned.Has(2))
	assert.True(t, cloned.Has(3))

	// Deep copy check
	cloned.Insert(4)
	assert.True(t, cloned.Has(4))
	assert.False(t, s.Has(4))

	// Empty set clone
	empty := New()
	emptyCloned := empty.Clone()
	assert.Empty(t, *emptyCloned)
}
