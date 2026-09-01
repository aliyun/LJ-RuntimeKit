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
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetResourceFromNode(t *testing.T) {
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "test-node"},
		Status: v1.NodeStatus{
			Allocatable: v1.ResourceList{
				v1.ResourceName(ResourceNvidia): *resource.NewQuantity(8, resource.DecimalSI),
				v1.ResourceName(ResourceMemory): *resource.NewQuantity(16384, resource.DecimalSI),
				v1.ResourceName(ResourceCore):   *resource.NewQuantity(100, resource.DecimalSI),
			},
		},
	}

	assert.Equal(t, int64(8), GetResourceFromNode(node, ResourceNvidia))
	assert.Equal(t, int64(16384), GetResourceFromNode(node, ResourceMemory))
	assert.Equal(t, int64(100), GetResourceFromNode(node, ResourceCore))
	assert.Equal(t, int64(0), GetResourceFromNode(node, "unknown-resource"))
}

func TestGetLabelsFromNodeStr(t *testing.T) {
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				"node.gpu.placement": "binpack",
				"region":             "us-west",
			},
		},
	}

	tests := []struct {
		name     string
		key      string
		expected string
		wantErr  bool
	}{
		{"existing label", "node.gpu.placement", "binpack", false},
		{"another label", "region", "us-west", false},
		{"missing label", "nonexistent", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := GetLabelsFromNodeStr(node, tt.key)
			if tt.wantErr {
				assert.Error(t, err)
				assert.True(t, err == NotFoundErr)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, val)
			}
		})
	}

	// Nil labels
	nilNode := &v1.Node{}
	_, err := GetLabelsFromNodeStr(nilNode, "any")
	assert.Error(t, err)
}

func TestGetLabelsFromNodeInt(t *testing.T) {
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				"gpu-count": "4",
				"invalid":   "not-a-number",
			},
		},
	}

	tests := []struct {
		name     string
		key      string
		expected int
		wantErr  bool
	}{
		{"valid int", "gpu-count", 4, false},
		{"invalid int", "invalid", 0, true},
		{"missing key", "missing", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := GetLabelsFromNodeInt(node, tt.key)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, val)
			}
		})
	}

	// Nil labels
	nilNode := &v1.Node{}
	_, err := GetLabelsFromNodeInt(nilNode, "any")
	assert.Error(t, err)
}
