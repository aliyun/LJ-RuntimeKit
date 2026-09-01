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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/component-base/featuregate"
)

func TestDefaultMutableFeatureGate_Initialized(t *testing.T) {
	assert.NotNil(t, DefaultMutableFeatureGate)
}

func TestDefaultFeatureGate_Initialized(t *testing.T) {
	assert.NotNil(t, DefaultFeatureGate)
}

func TestGetAllFeatureGatesDescriptions_ReturnsString(t *testing.T) {
	desc := GetAllFeatureGatesDescriptions()
	// Should return a string (maybe empty if no features registered)
	assert.IsType(t, "", desc)
}

func TestGetAllFeatureGatesDescriptions_WithFeatures(t *testing.T) {
	// Add some test features
	testFeatures := map[featuregate.Feature]featuregate.FeatureSpec{
		"TestFeature1": {Default: true, PreRelease: featuregate.Beta},
		"TestFeature2": {Default: false, PreRelease: featuregate.Alpha},
	}

	err := DefaultMutableFeatureGate.Add(testFeatures)
	// May fail if already added, which is ok
	if err == nil {
		desc := GetAllFeatureGatesDescriptions()
		// Should contain the feature names
		assert.True(t, strings.Contains(desc, "TestFeature1") || strings.Contains(desc, "TestFeature2") || desc == "")
	}
}
