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

package controllers

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/container-migration/controller/api/v1beta1"
	"github.com/container-migration/controller/pkg/config"
	"github.com/container-migration/controller/pkg/labels"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/keymutex"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ============================ minimal in-memory fake client ============================
//
// controller-runtime's fake client is not vendored (unavailable offline), so
// a hand-written in-memory implementation supporting only the types this test
// needs (Pod / ConfigMap / CSMJR List) is used here.

type fakeClient struct {
	pods      map[string]*corev1.Pod
	cms       map[string]*corev1.ConfigMap
	crs       []*v1beta1.ContainerStateMigrationJobRequest
	listErr   error
	createErr error
	deleteErr error
}

func newFakeClient() *fakeClient {
	return &fakeClient{
		pods: map[string]*corev1.Pod{},
		cms:  map[string]*corev1.ConfigMap{},
	}
}

func nsName(ns, name string) string { return ns + "/" + name }

func (f *fakeClient) Get(_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	switch o := obj.(type) {
	case *corev1.Pod:
		p, ok := f.pods[nsName(key.Namespace, key.Name)]
		if !ok {
			return apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, key.Name)
		}
		*o = *p.DeepCopy()
		return nil
	case *corev1.ConfigMap:
		cm, ok := f.cms[nsName(key.Namespace, key.Name)]
		if !ok {
			return apierrors.NewNotFound(schema.GroupResource{Resource: "configmaps"}, key.Name)
		}
		*o = *cm.DeepCopy()
		return nil
	default:
		return fmt.Errorf("fakeClient.Get: unsupported type %T", obj)
	}
}

func (f *fakeClient) List(_ context.Context, list client.ObjectList, _ ...client.ListOption) error {
	if f.listErr != nil {
		return f.listErr
	}
	l, ok := list.(*v1beta1.ContainerStateMigrationJobRequestList)
	if !ok {
		return fmt.Errorf("fakeClient.List: unsupported type %T", list)
	}
	l.Items = l.Items[:0]
	for _, cr := range f.crs {
		l.Items = append(l.Items, *cr.DeepCopy())
	}
	return nil
}

func (f *fakeClient) Create(_ context.Context, obj client.Object, _ ...client.CreateOption) error {
	if f.createErr != nil {
		return f.createErr
	}
	p, ok := obj.(*corev1.Pod)
	if !ok {
		return fmt.Errorf("fakeClient.Create: unsupported type %T", obj)
	}
	key := nsName(p.Namespace, p.Name)
	if _, exists := f.pods[key]; exists {
		return apierrors.NewAlreadyExists(schema.GroupResource{Resource: "pods"}, p.Name)
	}
	f.pods[key] = p.DeepCopy()
	return nil
}

func (f *fakeClient) Delete(_ context.Context, obj client.Object, _ ...client.DeleteOption) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	p, ok := obj.(*corev1.Pod)
	if !ok {
		return fmt.Errorf("fakeClient.Delete: unsupported type %T", obj)
	}
	key := nsName(p.Namespace, p.Name)
	if _, exists := f.pods[key]; !exists {
		return apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, p.Name)
	}
	delete(f.pods, key)
	return nil
}

// The following methods only exist to satisfy the client.Client interface; not used by this test.
func (f *fakeClient) Update(context.Context, client.Object, ...client.UpdateOption) error { return nil }
func (f *fakeClient) Patch(context.Context, client.Object, client.Patch, ...client.PatchOption) error {
	return nil
}
func (f *fakeClient) DeleteAllOf(context.Context, client.Object, ...client.DeleteAllOfOption) error {
	return nil
}
func (f *fakeClient) Status() client.SubResourceWriter            { return nil }
func (f *fakeClient) SubResource(string) client.SubResourceClient { return nil }
func (f *fakeClient) Scheme() *runtime.Scheme                     { return runtime.NewScheme() }
func (f *fakeClient) RESTMapper() meta.RESTMapper                 { return nil }
func (f *fakeClient) GroupVersionKindFor(runtime.Object) (schema.GroupVersionKind, error) {
	return schema.GroupVersionKind{}, nil
}
func (f *fakeClient) IsObjectNamespaced(runtime.Object) (bool, error) { return true, nil }

// ============================ test scaffolding ============================

const testWorkerTemplate = `metadata:
  labels:
    component: lj-runtimekit-migration-worker
spec:
  containers:
  - name: migration-worker
    image: registry.example.com/worker:v1
`

func newTestContext(t *testing.T, fc *fakeClient) *SharedContext {
	t.Helper()
	cfg := config.NewConfig()
	cfg.WorkerNamespace = metav1.NamespaceSystem
	cfg.WorkerDrainPollInterval = 10 * time.Second
	return &SharedContext{
		Config:        cfg,
		Client:        fc,
		APIReader:     fc,
		workerClients: map[string]workerClientEntry{},
		workerUsers:   map[string]map[string]struct{}{},
		drainVerdict:  map[string]drainVerdictEntry{},
		workerPodLock: keymutex.NewHashed(10000),
	}
}

func putWorkerConfigMap(fc *fakeClient, port string) {
	fc.cms[nsName(metav1.NamespaceSystem, config.DefaultWorkerConfigMapName)] = &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       metav1.NamespaceSystem,
			Name:            config.DefaultWorkerConfigMapName,
			ResourceVersion: "1",
		},
		Data: map[string]string{
			config.WorkerPodTemplateKey: testWorkerTemplate,
			config.GrpcPortKey:          port,
		},
	}
}

func readyPod(name, hash string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: metav1.NamespaceSystem,
			Name:      name,
			Labels:    map[string]string{labels.LabelWorkerConfigHash: hash},
		},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
}
