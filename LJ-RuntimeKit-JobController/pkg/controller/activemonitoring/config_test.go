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

package activemonitoring

import (
	"context"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/lj-runtimekit/job-controller/pkg/nodeagent"
)

const (
	testConfigMapNS   = "kube-system"
	testConfigMapName = "lj-runtimekit-jobcontroller-config"
)

// newConfigTestController builds a Controller with the ConfigMap informer
// wired (Namespace + ConfigMapName set), starts its informers and waits for
// cache sync, so the detection settings are seeded exactly like in Run.
func newConfigTestController(t *testing.T, objs ...runtime.Object) (*Controller, context.CancelFunc) {
	t.Helper()
	scheme := runtime.NewScheme()
	unstructuredObjs := make([]runtime.Object, 0, len(objs))
	for _, obj := range objs {
		unstructuredObjs = append(unstructuredObjs, mustUnstructured(t, obj))
	}
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			activemonitoringGVR: "ActiveMonitoringList",
			podGVR:              "PodList",
			nodeGVR:             "NodeList",
			configMapGVR:        "ConfigMapList",
		}, unstructuredObjs...)

	c := NewController(Config{
		DynamicClient: client,
		Namespace:     testConfigMapNS,
		ConfigMapName: testConfigMapName,
	})
	ctx, cancel := context.WithCancel(context.Background())
	if err := c.startInformers(ctx); err != nil {
		cancel()
		t.Fatalf("start informers: %v", err)
	}
	return c, cancel
}

// testConfigMap builds the shared controller ConfigMap carrying config.yaml data.
func testConfigMap(data string) *v1.ConfigMap {
	return &v1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{Namespace: testConfigMapNS, Name: testConfigMapName},
		Data:       map[string]string{nodeagent.ConfigKey: data},
	}
}

// testConfigMapEvent builds the unstructured ConfigMap object an informer
// Add/Update event would carry, with an explicit resourceVersion.
func testConfigMapEvent(t *testing.T, data, rv string) runtime.Object {
	t.Helper()
	cm := testConfigMap(data)
	cm.ResourceVersion = rv
	return mustUnstructured(t, cm)
}

func assertSettings(t *testing.T, got *detectionSettings, wantImage, wantSocket string) {
	t.Helper()
	if got.image != wantImage || got.socketPath != wantSocket {
		t.Fatalf("detectionConfig() = {image:%q socketPath:%q}, want {image:%q socketPath:%q}",
			got.image, got.socketPath, wantImage, wantSocket)
	}
}

func TestDetectionConfigDefaultsWhenConfigMapMissing(t *testing.T) {
	c, cancel := newConfigTestController(t)
	defer cancel()
	assertSettings(t, c.detectionConfig(), defaultTestImage, defaultRuntimeKitSocket)
}

func TestDetectionConfigDefaultsWhenWiringEmpty(t *testing.T) {
	// No ConfigMapName: the ConfigMap informer is not created at all and the
	// built-in defaults are used, matching the pre-ConfigMap behaviour.
	c, cancel := newTestController(t)
	defer cancel()
	if c.cmInformer != nil {
		t.Fatal("cmInformer must be nil when ConfigMapName is empty")
	}
	assertSettings(t, c.detectionConfig(), defaultTestImage, defaultRuntimeKitSocket)
}

func TestDetectionConfigSeededFromConfigMap(t *testing.T) {
	cm := testConfigMap(`image: collector:test
socketPath: /run/top/dp2mgr.sock
activeMonitoringImage: registry.example/diag:v2
`)
	c, cancel := newConfigTestController(t, cm)
	defer cancel()
	assertSettings(t, c.detectionConfig(), "registry.example/diag:v2", "/run/top/dp2mgr.sock")
}

func TestDetectionConfigImageFallback(t *testing.T) {
	// A ConfigMap without activeMonitoringImage (older clusters) keeps the
	// built-in default image but still provides the socketPath.
	cm := testConfigMap(`image: collector:test
socketPath: /run/top/dp2mgr.sock
`)
	c, cancel := newConfigTestController(t, cm)
	defer cancel()
	assertSettings(t, c.detectionConfig(), defaultTestImage, "/run/top/dp2mgr.sock")
}

func TestDetectionConfigSocketFallback(t *testing.T) {
	// Without an explicit socketPath, nodeagent.ParseConfig defaults it to the
	// dp2mgr socket, which equals the built-in defaultRuntimeKitSocket.
	cm := testConfigMap(`image: collector:test
activeMonitoringImage: registry.example/diag:v2
`)
	c, cancel := newConfigTestController(t, cm)
	defer cancel()
	assertSettings(t, c.detectionConfig(), "registry.example/diag:v2", defaultRuntimeKitSocket)
}

func TestDetectionConfigInvalidAtSeedUsesDefaults(t *testing.T) {
	cm := testConfigMap(`bogus-key: 1
image: collector:test
`)
	c, cancel := newConfigTestController(t, cm)
	defer cancel()
	assertSettings(t, c.detectionConfig(), defaultTestImage, defaultRuntimeKitSocket)
}

func TestStoreDetectionConfigLastGood(t *testing.T) {
	cm := testConfigMap(`image: collector:test
activeMonitoringImage: registry.example/diag:v1
`)
	c, cancel := newConfigTestController(t, cm)
	defer cancel()
	assertSettings(t, c.detectionConfig(), "registry.example/diag:v1", defaultRuntimeKitSocket)

	// An invalid update keeps the last good settings.
	c.onConfigMapEvent(testConfigMapEvent(t, "image: collector:test\nbogus-key: 1\n", "2"))
	assertSettings(t, c.detectionConfig(), "registry.example/diag:v1", defaultRuntimeKitSocket)

	// A missing config.yaml key also keeps the last good settings.
	empty := &v1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{Namespace: testConfigMapNS, Name: testConfigMapName, ResourceVersion: "3"},
	}
	c.onConfigMapEvent(mustUnstructured(t, empty))
	assertSettings(t, c.detectionConfig(), "registry.example/diag:v1", defaultRuntimeKitSocket)

	// A later valid update replaces the last good settings.
	c.onConfigMapEvent(testConfigMapEvent(t, `image: collector:test
socketPath: /run/top/dp2mgr.sock
activeMonitoringImage: registry.example/diag:v2
`, "4"))
	assertSettings(t, c.detectionConfig(), "registry.example/diag:v2", "/run/top/dp2mgr.sock")
}
