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

package profiling

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/textlogger"
	"k8s.io/utils/ptr"
	runtimectrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	profilingv1alpha1 "github.com/lj-runtimekit/job-controller/api/v1alpha1"
	controller "github.com/lj-runtimekit/job-controller/pkg/controller"
	"github.com/lj-runtimekit/job-controller/pkg/nodeagent"
)

type adapterInformer struct {
	cache.Informer
	synced atomic.Bool
}

func (i *adapterInformer) HasSynced() bool { return i.synced.Load() }

type adapterCache struct {
	cache.Cache
	informers  []*adapterInformer
	objects    []string
	registered chan struct{}
	err        error
}

func (c *adapterCache) GetInformer(_ context.Context, obj client.Object, opts ...cache.InformerGetOption) (cache.Informer, error) {
	options := cache.InformerGetOptions{}
	for _, opt := range opts {
		opt(&options)
	}
	if options.BlockUntilSynced == nil || *options.BlockUntilSynced {
		return nil, fmt.Errorf("GetInformer must not wait before cache start")
	}
	if c.err != nil {
		return nil, c.err
	}
	c.objects = append(c.objects, reflect.TypeOf(obj).String())
	if len(c.objects) == len(c.informers) {
		close(c.registered)
	}
	return c.informers[len(c.objects)-1], nil
}

func testAdapter(run func(context.Context) error) (*controllerAdapter, *adapterCache) {
	c := &adapterCache{registered: make(chan struct{})}
	for i := 0; i < 5; i++ {
		c.informers = append(c.informers, &adapterInformer{})
	}
	started := make(chan struct{})
	close(started)
	return &controllerAdapter{cache: c, managerStarted: started, run: run, ready: make(chan struct{}), syncTimeout: 2 * time.Second}, c
}

func awaitAdapterExit(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("adapter did not stop")
		return nil
	}
}

func TestAdapterWaitsForEveryInformerAndDrains(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan struct{})
	drain := make(chan struct{})
	a, c := testAdapter(func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-drain
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	<-started
	<-c.registered
	want := []string{"*v1alpha1.ProfilingJob", "*v1.Pod", "*v1.PersistentVolumeClaim", "*v1.StorageClass", "*v1.ConfigMap"}
	if !reflect.DeepEqual(c.objects, want) {
		t.Fatalf("registered informers %v, want %v", c.objects, want)
	}
	for _, informer := range c.informers[:4] {
		informer.synced.Store(true)
	}
	select {
	case <-a.Ready():
		t.Fatal("ready before the ConfigMap informer synced")
	case <-time.After(150 * time.Millisecond):
	}
	c.informers[4].synced.Store(true)
	select {
	case <-a.Ready():
	case <-time.After(3 * time.Second):
		t.Fatal("adapter did not become ready")
	}
	cancel()
	<-canceled
	select {
	case <-done:
		t.Fatal("adapter returned before the manager drained")
	default:
	}
	close(drain)
	if err := awaitAdapterExit(t, done); err != nil {
		t.Fatal(err)
	}
	if err := a.Run(context.Background()); err == nil {
		t.Fatal("adapter accepted a second start")
	}
}

func TestAdapterInitializationFailures(t *testing.T) {
	failure := errors.New("manager failed")
	for _, tt := range []struct {
		name        string
		informerErr error
		runErr      error
		timeout     bool
	}{
		{name: "informer unavailable", informerErr: errors.New("no match for ProfilingJob")},
		{name: "manager failed", runErr: failure},
		{name: "unexpected manager exit"},
		{name: "cache sync timeout", timeout: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var called atomic.Bool
			a, c := testAdapter(func(ctx context.Context) error {
				called.Store(true)
				if tt.timeout || tt.informerErr != nil {
					<-ctx.Done()
				}
				return tt.runErr
			})
			c.err = tt.informerErr
			a.syncTimeout = 25 * time.Millisecond
			done := make(chan error, 1)
			go func() { done <- a.Run(context.Background()) }()
			err := awaitAdapterExit(t, done)
			if err == nil {
				t.Fatal("expected initialization failure")
			}
			if tt.runErr != nil && !errors.Is(err, tt.runErr) {
				t.Fatalf("lost manager error: %v", err)
			}
			if tt.informerErr != nil && (!errors.Is(err, tt.informerErr) || !called.Load()) {
				t.Fatalf("informer failure did not stop the started manager: %v", err)
			}
			select {
			case <-a.Ready():
				t.Fatal("failed initialization reported ready")
			default:
			}
		})
	}
}

func TestAdapterCancellationDuringSync(t *testing.T) {
	started := make(chan struct{})
	a, _ := testAdapter(func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	<-started
	cancel()
	if err := awaitAdapterExit(t, done); err != nil {
		t.Fatalf("graceful cancellation failed: %v", err)
	}
	select {
	case <-a.Ready():
		t.Fatal("canceled initialization reported ready")
	default:
	}
}

func TestAdapterPropagatesRuntimeFailure(t *testing.T) {
	failure := errors.New("runtime failure")
	fail := make(chan struct{})
	a, c := testAdapter(func(context.Context) error {
		<-fail
		return failure
	})
	for _, informer := range c.informers {
		informer.synced.Store(true)
	}
	done := make(chan error, 1)
	go func() { done <- a.Run(context.Background()) }()
	select {
	case <-a.Ready():
	case <-time.After(3 * time.Second):
		t.Fatal("adapter did not become ready")
	}
	close(fail)
	if err := awaitAdapterExit(t, done); !errors.Is(err, failure) {
		t.Fatalf("got %v, want runtime failure", err)
	}
}

func TestProfilingCacheFilters(t *testing.T) {
	config := Config{CollectorNamespace: "collector-system", ConfigMapName: "collector-config"}
	options := profilingCacheOptions(config)
	if len(options.ByObject) != 2 || len(options.DefaultNamespaces) != 0 {
		t.Fatal("unexpected global or per-object cache restrictions")
	}
	for object, filter := range options.ByObject {
		switch object.(type) {
		case *corev1.Pod:
			if !filter.Label.Matches(labels.Set{nodeagent.LabelComponent: nodeagent.ComponentCollector}) || filter.Label.Matches(labels.Set{"app": "target"}) {
				t.Fatal("Pod cache does not isolate collectors")
			}
			if len(filter.Namespaces) != 0 {
				t.Fatal("collector Pods must be watched across namespaces")
			}
		case *corev1.ConfigMap:
			if _, ok := filter.Namespaces[config.CollectorNamespace]; !ok || len(filter.Namespaces) != 1 {
				t.Fatal("ConfigMap namespace filter changed")
			}
			if !filter.Field.Matches(fields.Set{"metadata.name": config.ConfigMapName}) || filter.Field.Matches(fields.Set{"metadata.name": "other"}) {
				t.Fatal("ConfigMap name filter changed")
			}
		default:
			t.Fatalf("unexpected filtered type %T", object)
		}
	}
}

func TestProfilingFactoryRequiresConfig(t *testing.T) {
	for _, config := range []Config{{}, {CollectorNamespace: "kube-system"}, {ConfigMapName: "config"}} {
		if _, err := NewFactory(config)(controller.SharedClients{}); err == nil {
			t.Fatal("accepted incomplete profiling config")
		}
	}
	if _, err := NewFactory(Config{CollectorNamespace: "kube-system", ConfigMapName: "config"})(controller.SharedClients{}); err == nil {
		t.Fatal("accepted missing REST config")
	}
}

func TestAdapterExpiredStartupCannotReportReady(t *testing.T) {
	for i := 0; i < 100; i++ {
		a, c := testAdapter(func(ctx context.Context) error {
			<-ctx.Done()
			return nil
		})
		for _, informer := range c.informers {
			informer.synced.Store(true)
		}
		a.syncTimeout = -time.Second
		done := make(chan error, 1)
		go func() { done <- a.Run(context.Background()) }()
		if err := awaitAdapterExit(t, done); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expired startup lost its timeout: %v", err)
		}
		select {
		case <-a.Ready():
			t.Fatal("expired startup reported ready")
		default:
		}
	}
}

func TestAdapterPreservesFailureDuringCancellation(t *testing.T) {
	failure := errors.New("manager shutdown failed")
	a, c := testAdapter(func(ctx context.Context) error {
		<-ctx.Done()
		return failure
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	<-c.registered
	cancel()
	if err := awaitAdapterExit(t, done); !errors.Is(err, failure) {
		t.Fatalf("shutdown failure was lost: %v", err)
	}
}

func TestProfilingFactoryDoesNotStartBeforeRun(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		<-r.Context().Done()
	}))
	defer server.Close()
	restConfig := &rest.Config{Host: server.URL}
	original := rest.CopyConfig(restConfig)
	c, err := NewFactory(Config{CollectorNamespace: "kube-system", ConfigMapName: "config"})(controller.SharedClients{RESTConfig: restConfig})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(restConfig, original) {
		t.Fatal("factory mutated shared REST config")
	}
	select {
	case <-c.Ready():
		t.Fatal("factory reported ready before Run")
	default:
	}
	if requests.Load() != 0 {
		t.Fatal("factory contacted Kubernetes before Run")
	}
	a := c.(*controllerAdapter)
	a.syncTimeout = time.Second
	done := make(chan error, 1)
	go func() { done <- a.Run(context.Background()) }()
	if err := awaitAdapterExit(t, done); err == nil {
		t.Fatal("unavailable Kubernetes API did not fail startup")
	}
	select {
	case <-a.Ready():
		t.Fatal("failed factory startup reported ready")
	default:
	}
}

func TestProfilingFactoryLogsLifecycleAtDefaultVerbosity(t *testing.T) {
	if os.Getenv("PROFILING_TEST_LIFECYCLE_LOGS") != "1" {
		// Controller-runtime reserves controller names process-wide, including across managers.
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestProfilingFactoryLogsLifecycleAtDefaultVerbosity$", "-test.v")
		cmd.Env = append(os.Environ(), "PROFILING_TEST_LIFECYCLE_LOGS=1")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("lifecycle logging subprocess: %v\n%s", err, output)
		}
		return
	}
	pending := newJob()
	pending.Spec.PersistentVolumeClaimName = "missing"
	completed, failed := newJob(), newJob()
	completed.Name, completed.UID = "completed", "completed-uid"
	failed.Name, failed.UID = "failed", "failed-uid"
	for _, job := range []*profilingv1alpha1.ProfilingJob{completed, failed} {
		job.Status = profilingv1alpha1.ProfilingJobStatus{
			LastRunIndex: ptr.To(job.Spec.RunIndex),
			Phase:        profilingv1alpha1.PhaseRunning,
			RunConfig:    &profilingv1alpha1.RunConfiguration{Duration: job.Spec.Duration, ProfilingMode: job.Spec.ProfilingMode},
			Targets:      []profilingv1alpha1.TargetStatus{{TargetNamespace: ns, TargetName: "target", Stage: profilingv1alpha1.StageCompleted}},
		}
	}
	failed.Status.Targets[0].Stage = profilingv1alpha1.StageFailed
	failed.Status.Targets[0].Reason = "StreamFailed"
	failed.Status.Targets[0].Message = "collector stream failed"
	f := newFixture(t, pending, completed, failed, newPVC(), newConfigMap("collector:v1"))
	events := make(chan *corev1.Event, 20)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("watch") == "true" {
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			return
		}
		var obj runtime.Object
		var err error
		lists := map[string]client.ObjectList{
			"pods":                   &corev1.PodList{},
			"persistentvolumeclaims": &corev1.PersistentVolumeClaimList{},
			"configmaps":             &corev1.ConfigMapList{},
			"storageclasses":         &storagev1.StorageClassList{},
			"profilingjobs":          &profilingv1alpha1.ProfilingJobList{},
		}
		switch {
		case path.Base(r.URL.Path) == "events" && r.Method == http.MethodPost:
			event := &corev1.Event{}
			err = json.NewDecoder(r.Body).Decode(event)
			if err == nil {
				events <- event
			}
			obj = event
		case r.Method == http.MethodGet && lists[path.Base(r.URL.Path)] != nil:
			list := lists[path.Base(r.URL.Path)]
			err = f.client.List(r.Context(), list)
			list.SetResourceVersion("1")
			obj = list
		case strings.Contains(r.URL.Path, "/profilingjobs/"):
			job := &profilingv1alpha1.ProfilingJob{}
			if r.Method == http.MethodGet {
				err = f.client.Get(r.Context(), client.ObjectKey{Namespace: ns, Name: path.Base(r.URL.Path)}, job)
			} else if err = json.NewDecoder(r.Body).Decode(job); err == nil {
				if path.Base(r.URL.Path) == "status" {
					err = f.client.Status().Update(r.Context(), job)
				} else {
					err = f.client.Update(r.Context(), job)
				}
			}
			obj = job
		default:
			http.Error(w, "unexpected request: "+r.Method+" "+r.URL.Path, http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		kinds, _, err := f.r.Scheme.ObjectKinds(obj)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		obj.GetObjectKind().SetGroupVersionKind(kinds[0])
		if err := json.NewEncoder(w).Encode(obj); err != nil {
			t.Errorf("encode API response: %v", err)
		}
	}))
	defer server.Close()
	c, err := NewFactory(Config{CollectorNamespace: collectorNS, ConfigMapName: configName})(controller.SharedClients{
		RESTConfig: &rest.Config{Host: server.URL, ContentConfig: rest.ContentConfig{ContentType: runtime.ContentTypeJSON, AcceptContentTypes: runtime.ContentTypeJSON}},
	})
	if err != nil {
		t.Fatal(err)
	}
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	lines := make(chan string, 100)
	go func() {
		scanner := bufio.NewScanner(reader)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		close(lines)
	}()
	logger := textlogger.NewLogger(textlogger.NewConfig(textlogger.Verbosity(0), textlogger.Output(writer)))
	ctx, cancel := context.WithCancel(klog.NewContext(context.Background(), logger))
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	defer func() {
		cancel()
		if err := awaitAdapterExit(t, done); err != nil {
			t.Errorf("stop controller: %v", err)
		}
	}()
	want := map[string]string{
		"RoundCreated": `object="inference/job"`,
		"PVCNotReady":  `object="inference/job"`,
		"Completed":    `object="inference/completed"`,
		"Failed":       `object="inference/failed"`,
	}
	logged, recorded := map[string]bool{}, map[string]bool{}
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	for len(logged) < len(want) || len(recorded) < len(want) {
		select {
		case line := <-lines:
			for reason, object := range want {
				if !strings.Contains(line, `reason="`+reason+`"`) {
					continue
				}
				if !strings.Contains(line, object) || !strings.Contains(line, `kind="ProfilingJob"`) || !strings.Contains(line, `message="`) {
					t.Fatalf("lifecycle log lacks context: %s", line)
				}
				if reason == "Failed" && !strings.Contains(line, `type="Warning"`) {
					t.Fatalf("failure log lost Warning type: %s", line)
				}
				logged[reason] = true
			}
		case event := <-events:
			if _, ok := want[event.Reason]; ok {
				recorded[event.Reason] = true
			}
		case <-timeout.C:
			t.Fatalf("default verbosity logs=%v events=%v, want reasons=%v", logged, recorded, want)
		}
	}
}

func TestContextTransportCancelsBackgroundRequests(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		t.Run(fmt.Sprintf("streaming=%v", streaming), func(t *testing.T) {
			seen := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(seen)
				if streaming {
					fmt.Fprint(w, "ok\n")
					w.(http.Flusher).Flush()
				}
				<-r.Context().Done()
			}))
			defer server.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			httpClient := server.Client()
			httpClient.Transport = &contextTransport{RoundTripper: httpClient.Transport, ctx: ctx}
			streamRead := make(chan struct{})
			done := make(chan error, 1)
			go func() {
				response, err := httpClient.Get(server.URL)
				if err == nil {
					defer response.Body.Close()
					if streaming {
						data := make([]byte, 3)
						if _, err = io.ReadFull(response.Body, data); err == nil && string(data) == "ok\n" {
							close(streamRead)
						}
					}
					if err == nil {
						_, err = io.ReadAll(response.Body)
					}
				}
				done <- err
			}()
			select {
			case <-seen:
			case <-time.After(3 * time.Second):
				t.Fatal("request did not reach server")
			}
			if streaming {
				select {
				case <-streamRead:
				case <-time.After(3 * time.Second):
					t.Fatal("stream was canceled before its body could be read")
				}
			}
			cancel()
			if err := awaitAdapterExit(t, done); !errors.Is(err, context.Canceled) {
				t.Fatalf("background request did not observe lifecycle cancellation: %v", err)
			}
		})
	}
}

func TestAdapterRealCacheStartup(t *testing.T) {
	for _, mode := range []string{"forbidden timeout", "forbidden cancellation", "synced"} {
		t.Run(mode, func(t *testing.T) {
			requestSeen := make(chan struct{}, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case requestSeen <- struct{}{}:
				default:
				}
				if mode != "synced" {
					http.Error(w, "forbidden", http.StatusForbidden)
					return
				}
				if r.URL.Query().Get("watch") == "true" {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusOK)
					w.(http.Flusher).Flush()
					<-r.Context().Done()
					return
				}
				lists := map[string][2]string{
					"pods":                   {"v1", "PodList"},
					"persistentvolumeclaims": {"v1", "PersistentVolumeClaimList"},
					"configmaps":             {"v1", "ConfigMapList"},
					"storageclasses":         {"storage.k8s.io/v1", "StorageClassList"},
					"profilingjobs":          {"profiling.runtimekit.io/v1alpha1", "ProfilingJobList"},
				}
				list := lists[path.Base(r.URL.Path)]
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintf(w, `{"apiVersion":%q,"kind":%q,"metadata":{"resourceVersion":"1"},"items":[]}`, list[0], list[1])
			}))
			defer server.Close()
			scheme := runtime.NewScheme()
			if err := clientgoscheme.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			if err := profilingv1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{corev1.SchemeGroupVersion, profilingv1alpha1.GroupVersion, {Group: "storage.k8s.io", Version: "v1"}})
			for _, kind := range []string{"Pod", "PersistentVolumeClaim", "ConfigMap"} {
				mapper.Add(corev1.SchemeGroupVersion.WithKind(kind), meta.RESTScopeNamespace)
			}
			mapper.Add(profilingv1alpha1.GroupVersion.WithKind("ProfilingJob"), meta.RESTScopeNamespace)
			mapper.Add(schema.GroupVersionKind{Group: "storage.k8s.io", Version: "v1", Kind: "StorageClass"}, meta.RESTScopeRoot)
			shutdownTimeout := time.Duration(-1)
			mgr, err := runtimectrl.NewManager(&rest.Config{Host: server.URL}, runtimectrl.Options{
				Scheme: scheme,
				MapperProvider: func(*rest.Config, *http.Client) (meta.RESTMapper, error) {
					return mapper, nil
				},
				Cache:                   profilingCacheOptions(Config{CollectorNamespace: "kube-system", ConfigMapName: "config"}),
				Metrics:                 metricsserver.Options{BindAddress: "0"},
				HealthProbeBindAddress:  "0",
				GracefulShutdownTimeout: &shutdownTimeout,
			})
			if err != nil {
				t.Fatal(err)
			}
			a := &controllerAdapter{cache: mgr.GetCache(), managerStarted: mgr.Elected(), run: mgr.Start, ready: make(chan struct{}), syncTimeout: time.Second}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- a.Run(ctx) }()
			select {
			case <-requestSeen:
			case <-time.After(3 * time.Second):
				t.Fatal("real cache did not contact API server")
			}
			if mode == "synced" {
				select {
				case <-a.Ready():
				case <-time.After(3 * time.Second):
					t.Fatal("real cache did not become ready")
				}
				cancel()
			} else if mode == "forbidden cancellation" {
				cancel()
			}
			err = awaitAdapterExit(t, done)
			if (mode == "forbidden timeout") != (err != nil) {
				t.Fatalf("unexpected shutdown result: %v", err)
			}
			if mode != "synced" {
				select {
				case <-a.Ready():
					t.Fatal("unsynced real cache reported ready")
				default:
				}
			}
		})
	}
}
