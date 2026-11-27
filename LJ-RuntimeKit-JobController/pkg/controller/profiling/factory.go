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
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	toolscache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	runtimectrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	profilingv1alpha1 "github.com/lj-runtimekit/job-controller/api/v1alpha1"
	controller "github.com/lj-runtimekit/job-controller/pkg/controller"
	"github.com/lj-runtimekit/job-controller/pkg/nodeagent"
)

const ControllerName = "profiling"

const cacheSyncTimeout = 2 * time.Minute

type Config struct {
	CollectorNamespace string
	ConfigMapName      string
}

func (c Config) Validate() error {
	if c.CollectorNamespace == "" {
		return fmt.Errorf("collector namespace must not be empty")
	}
	if c.ConfigMapName == "" {
		return fmt.Errorf("collector ConfigMap name must not be empty")
	}
	return nil
}

func NewFactory(config Config) controller.Factory {
	return func(clients controller.SharedClients) (instance controller.Interface, err error) {
		if err := config.Validate(); err != nil {
			return nil, err
		}
		if clients.RESTConfig == nil {
			return nil, fmt.Errorf("profiling controller requires a Kubernetes REST config")
		}
		scheme := runtime.NewScheme()
		if err := clientgoscheme.AddToScheme(scheme); err != nil {
			return nil, err
		}
		if err := profilingv1alpha1.AddToScheme(scheme); err != nil {
			return nil, err
		}
		httpCtx, cancelHTTP := context.WithCancel(context.Background())
		defer func() {
			if err != nil {
				cancelHTTP()
			}
		}()
		runtimeConfig := rest.CopyConfig(clients.RESTConfig)
		runtimeConfig.Wrap(func(transport http.RoundTripper) http.RoundTripper {
			return &contextTransport{RoundTripper: transport, ctx: httpCtx}
		})
		shutdownTimeout := time.Duration(-1)
		mgr, err := runtimectrl.NewManager(runtimeConfig, runtimectrl.Options{
			MapperProvider: func(*rest.Config, *http.Client) (meta.RESTMapper, error) {
				return profilingRESTMapper(), nil
			},
			Scheme:                  scheme,
			Cache:                   profilingCacheOptions(config),
			LeaderElection:          false,
			Metrics:                 metricsserver.Options{BindAddress: "0"},
			HealthProbeBindAddress:  "0",
			PprofBindAddress:        "0",
			GracefulShutdownTimeout: &shutdownTimeout,
		})
		if err != nil {
			return nil, fmt.Errorf("create profiling manager: %w", err)
		}
		eventClient, err := corev1client.NewForConfigAndClient(mgr.GetConfig(), mgr.GetHTTPClient())
		if err != nil {
			return nil, fmt.Errorf("create profiling event client: %w", err)
		}
		reconciler := &ProfilingJobReconciler{
			Client:             mgr.GetClient(),
			APIReader:          mgr.GetAPIReader(),
			Scheme:             scheme,
			CollectorNamespace: config.CollectorNamespace,
			ConfigMapName:      config.ConfigMapName,
		}
		if err := reconciler.SetupWithManager(mgr); err != nil {
			return nil, fmt.Errorf("register profiling controller: %w", err)
		}
		return &controllerAdapter{
			cache:          mgr.GetCache(),
			managerStarted: mgr.Elected(),
			ready:          make(chan struct{}),
			syncTimeout:    cacheSyncTimeout,
			run: func(ctx context.Context) error {
				stop := context.AfterFunc(ctx, cancelHTTP)
				defer stop()
				defer cancelHTTP()
				broadcaster := record.NewBroadcaster(record.WithContext(ctx), record.WithCorrelatorOptions(record.CorrelatorOptions{BurstSize: 50}))
				defer broadcaster.Shutdown()
				broadcaster.StartRecordingToSink(&corev1client.EventSinkImpl{Interface: eventClient.Events("")})
				broadcaster.StartStructuredLogging(0)
				reconciler.Recorder = broadcaster.NewRecorder(scheme, corev1.EventSource{Component: "lj-runtimekit-profiling-controller"})
				return mgr.Start(ctx)
			},
		}, nil
	}
}

// Fixed API versions avoid discovery while constructing caches before leadership.
func profilingRESTMapper() meta.RESTMapper {
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{corev1.SchemeGroupVersion, storagev1.SchemeGroupVersion, profilingv1alpha1.GroupVersion})
	for _, kind := range []string{"Pod", "PersistentVolumeClaim", "ConfigMap"} {
		mapper.Add(corev1.SchemeGroupVersion.WithKind(kind), meta.RESTScopeNamespace)
	}
	mapper.Add(storagev1.SchemeGroupVersion.WithKind("StorageClass"), meta.RESTScopeRoot)
	mapper.Add(profilingv1alpha1.GroupVersion.WithKind("ProfilingJob"), meta.RESTScopeNamespace)
	return mapper
}

func profilingCacheOptions(config Config) cache.Options {
	return cache.Options{ByObject: map[client.Object]cache.ByObject{
		&corev1.Pod{}: {Label: labels.SelectorFromSet(labels.Set{nodeagent.LabelComponent: nodeagent.ComponentCollector})},
		&corev1.ConfigMap{}: {
			Namespaces: map[string]cache.Config{config.CollectorNamespace: {}},
			Field:      fields.OneTermEqualSelector("metadata.name", config.ConfigMapName),
		},
	}}
}

func MetricsHandler() http.Handler {
	return promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{})
}

// Legacy Event sinks otherwise keep background HTTP requests alive after shutdown.
type contextTransport struct {
	http.RoundTripper
	ctx context.Context
}

func (t *contextTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	ctx, cancel := context.WithCancel(request.Context())
	stop := context.AfterFunc(t.ctx, cancel)
	response, err := t.RoundTripper.RoundTrip(request.Clone(ctx))
	if err != nil {
		stop()
		cancel()
		return nil, err
	}
	response.Body = &contextResponseBody{ReadCloser: response.Body, cancel: cancel, stop: stop}
	return response, nil
}

type contextResponseBody struct {
	io.ReadCloser
	cancel context.CancelFunc
	stop   func() bool
}

func (b *contextResponseBody) Close() error {
	defer b.cancel()
	defer b.stop()
	return b.ReadCloser.Close()
}

type controllerAdapter struct {
	cache          cache.Cache
	managerStarted <-chan struct{}
	run            func(context.Context) error
	ready          chan struct{}
	syncTimeout    time.Duration
	started        atomic.Bool
}

func (a *controllerAdapter) Name() string           { return ControllerName }
func (a *controllerAdapter) Ready() <-chan struct{} { return a.ready }

func (a *controllerAdapter) Run(parent context.Context) error {
	if !a.started.CompareAndSwap(false, true) {
		return fmt.Errorf("profiling controller already started")
	}
	if parent.Err() != nil {
		return nil
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	log := runtimectrl.LoggerFrom(parent).WithName(ControllerName)
	start := time.Now()
	log.Info("Starting profiling controller-runtime manager", "cacheSyncTimeout", a.syncTimeout)
	syncCtx, cancelSync := context.WithTimeout(ctx, a.syncTimeout)
	defer cancelSync()
	stopStartup := context.AfterFunc(syncCtx, cancel)
	defer stopStartup()

	done := make(chan error, 1)
	go func() {
		err := a.run(ctx)
		cancel()
		done <- err
	}()
	stop := func(cause error) error {
		cancel()
		if err := <-done; err != nil {
			log.Error(err, "Profiling manager failed during startup", "cause", cause, "elapsed", time.Since(start).Round(time.Millisecond))
			return err
		}
		if parent.Err() != nil && errors.Is(cause, parent.Err()) {
			log.Info("Profiling startup canceled during shutdown", "elapsed", time.Since(start).Round(time.Millisecond))
			return nil
		}
		log.Error(cause, "Profiling controller startup failed", "elapsed", time.Since(start).Round(time.Millisecond))
		return cause
	}

	// Pre-registering informers can trap controller-runtime v0.22 cache startup on cancellation.
	select {
	case <-a.managerStarted:
	case <-syncCtx.Done():
		return stop(fmt.Errorf("profiling manager startup failed: %w", syncCtx.Err()))
	}
	objects := []client.Object{
		&profilingv1alpha1.ProfilingJob{}, &corev1.Pod{}, &corev1.PersistentVolumeClaim{},
		&storagev1.StorageClass{}, &corev1.ConfigMap{},
	}
	log.Info("Profiling manager started; waiting for informer caches to sync", "informers", len(objects), "elapsed", time.Since(start).Round(time.Millisecond))
	synced := make([]toolscache.InformerSynced, 0, len(objects))
	for _, obj := range objects {
		log.Info("Registering profiling informer", "resourceType", fmt.Sprintf("%T", obj))
		informer, err := a.cache.GetInformer(syncCtx, obj, cache.BlockUntilSynced(false))
		if err != nil {
			return stop(fmt.Errorf("get profiling informer for %T: %w", obj, err))
		}
		synced = append(synced, informer.HasSynced)
	}
	if !toolscache.WaitForCacheSync(syncCtx.Done(), synced...) {
		var pending []string
		for i, hasSynced := range synced {
			if !hasSynced() {
				pending = append(pending, fmt.Sprintf("%T", objects[i]))
			}
		}
		log.Info("Profiling informer sync interrupted", "pendingInformers", pending)
		return stop(fmt.Errorf("profiling informer cache sync failed: %w", syncCtx.Err()))
	}
	if !stopStartup() || syncCtx.Err() != nil {
		return stop(fmt.Errorf("profiling informer cache sync interrupted: %w", syncCtx.Err()))
	}
	cancelSync()
	if ctx.Err() == nil {
		log.Info("Profiling informer caches synced; controller ready", "elapsed", time.Since(start).Round(time.Millisecond))
		close(a.ready)
	}
	err := <-done
	if err != nil {
		log.Error(err, "Profiling controller-runtime manager failed", "uptime", time.Since(start).Round(time.Second))
	} else {
		log.Info("Profiling controller-runtime manager exited", "uptime", time.Since(start).Round(time.Second))
	}
	return err
}
