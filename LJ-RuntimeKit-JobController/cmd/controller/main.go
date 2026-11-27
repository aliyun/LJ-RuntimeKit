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

package main

import (
	"context"
	goflag "flag"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/klog/v2"
	runtimectrl "sigs.k8s.io/controller-runtime"

	cdapi "github.com/lj-runtimekit/job-controller/pkg/computedomain"
	"github.com/lj-runtimekit/job-controller/pkg/constants"
	ctrl "github.com/lj-runtimekit/job-controller/pkg/controller"
	"github.com/lj-runtimekit/job-controller/pkg/controller/activemonitoring"
	"github.com/lj-runtimekit/job-controller/pkg/controller/computedomain"
	"github.com/lj-runtimekit/job-controller/pkg/controller/profiling"
	"github.com/lj-runtimekit/job-controller/pkg/features"

	flag "github.com/spf13/pflag"
)

func main() {
	// CLI flags.
	var (
		podName                      string
		namespace                    string
		computeDomainCliqueNamespace string
		devicePluginPodSelector      string
		maxNodes                     int
		isolation                    string
		imexConfigOverrides          string
		httpEndpoint                 string
		leaderElect                  bool
		kubeconfig                   string
		workers                      int
		controllerConfig             string
	)
	gate := features.NewFeatureGate()
	gate.AddFlag(flag.CommandLine)

	flag.StringVar(&podName, "pod-name", envOrDefault("POD_NAME", ""), "Name of the controller pod")
	flag.StringVar(&namespace, "namespace", envOrDefault("NAMESPACE", "kube-system"), "Namespace for the leader election Lease (default kube-system)")
	flag.StringVar(&computeDomainCliqueNamespace, "compute-domain-clique-namespace", envOrDefault("COMPUTE_DOMAIN_CLIQUE_NAMESPACE", "default"), "Namespace for ComputeDomainClique informer, cache, live List and Delete (default \"default\")")
	// The ComputeDomain controller always serves both resource.aliyun.com and
	// resource.nvidia.com: one isolated manager set per group behind a single
	// Lease and a single callback queue. There is no flag to enable, disable or
	// default a group, and both CRDs are hard startup dependencies.
	// --device-plugin-pod-selector is retained as a deprecated no-op. The
	// controller no longer inspects Device Plugin pods: the Device Plugin is the
	// only writer of CDClique daemon entries. The flag is kept (rather than
	// removed) because on-site Deployment manifests may still pass it, and an
	// unknown flag would prevent the process from starting.
	flag.StringVar(&devicePluginPodSelector, "device-plugin-pod-selector", constants.DefaultDevicePluginPodSelector, "Deprecated no-op: the controller no longer inspects Device Plugin pods")
	if err := flag.CommandLine.MarkDeprecated("device-plugin-pod-selector", "no-op; the Device Plugin is the sole owner of CDClique daemon entries"); err != nil {
		klog.Fatalf("Failed to mark --device-plugin-pod-selector as deprecated: %v", err)
	}
	flag.IntVar(&maxNodes, "max-nodes-per-imex-domain", constants.DefaultMaxNodesPerIMEXDomain, "Maximum nodes per IMEX domain")
	flag.StringVar(&isolation, "imex-isolation", constants.DefaultIMEXIsolation, "IMEX isolation mode")
	flag.StringVar(&imexConfigOverrides, "imex-config-overrides", "", "JSON overrides for IMEX configuration")
	flag.StringVar(&httpEndpoint, "http-endpoint", ":6060", "HTTP endpoint for pprof and metrics (e.g. :6060)")
	flag.BoolVar(&leaderElect, "leader-election", true, "Enable leader election")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig file (optional, uses in-cluster config if empty)")
	flag.IntVar(&workers, "workers", 2, "Number of ComputeDomain reconcile workers")
	flag.StringVar(&controllerConfig, "controllerConfig", "lj-runtimekit-jobcontroller-config", "Name of the controller configuration ConfigMap (profiling collector settings and the activeMonitoring detection section)")

	klog.InitFlags(nil)
	flag.CommandLine.AddGoFlagSet(goflag.CommandLine)
	flag.Parse()
	runtimectrl.SetLogger(klog.Background())

	// Normalize namespace flags: explicit --namespace="" must not bypass the
	// safe default.  The normalization is extracted into normalizeNamespaces
	// (testable) and also backed by NewController's NormalizeNamespaces as a
	// defense-in-depth measure.
	namespace, computeDomainCliqueNamespace = normalizeNamespaces(namespace, computeDomainCliqueNamespace)

	if podName == "" {
		klog.Warning("--pod-name or POD_NAME not set, leader election identity may be unreliable")
		podName, _ = os.Hostname()
	}

	factories, err := enabledFactories(gate, computedomain.Config{
		Namespace:                    namespace,
		ComputeDomainCliqueNamespace: computeDomainCliqueNamespace,
		MaxNodes:                     maxNodes,
		IMEXIsolation:                isolation,
		IMEXConfigOverrides:          imexConfigOverrides,
	}, workers, profiling.Config{
		CollectorNamespace: namespace,
		ConfigMapName:      controllerConfig,
	}, activemonitoring.Config{
		Namespace:     namespace,
		ConfigMapName: controllerConfig,
	})
	if err != nil {
		klog.Fatal(err)
	}
	if gate.Enabled(features.ComputeDomain) {
		computedomain.RegisterComputeDomainMetrics()
	}
	for name, factory := range factories {
		ctrl.Register(name, factory)
	}
	enabled := ctrl.KnownControllers()

	klog.InfoS("Starting RuntimeKit Controller Manager",
		"podName", podName,
		"namespace", namespace,
		"computeDomainCliqueNamespace", computeDomainCliqueNamespace,
		"computeDomainAPIGroups", cdapi.SupportedAPIGroups(),
		"enabledControllers", enabled,
		"featureGates", map[string]bool{
			string(features.ComputeDomain):    gate.Enabled(features.ComputeDomain),
			string(features.Profiling):        gate.Enabled(features.Profiling),
			string(features.ActiveMonitoring): gate.Enabled(features.ActiveMonitoring),
		},
		"leaderElection", leaderElect,
		"httpEndpoint", httpEndpoint,
		"kubeconfig", kubeconfig,
	)
	if gate.Enabled(features.ComputeDomain) {
		klog.InfoS("ComputeDomain controller configuration",
			"devicePluginPodSelector", devicePluginPodSelector,
			"maxNodesPerIMEXDomain", maxNodes,
			"imexIsolation", isolation,
			"imexConfigOverridesSet", imexConfigOverrides != "",
			"workers", workers,
		)
	}
	if gate.Enabled(features.Profiling) {
		klog.InfoS("Profiling controller configuration",
			"collectorNamespace", namespace,
			"collectorConfigMap", controllerConfig,
		)
	}
	if gate.Enabled(features.ActiveMonitoring) {
		klog.InfoS("ActiveMonitoring controller configuration",
			"namespace", namespace,
			"configMapName", controllerConfig,
			"workers", workers,
		)
	}

	// Build K8s config (shared across all controllers).
	config, err := buildConfig(kubeconfig)
	if err != nil {
		klog.Fatalf("Failed to build kubernetes config: %v", err)
	}
	klog.InfoS("Kubernetes client configured", "apiServer", config.Host, "inCluster", kubeconfig == "", "qps", config.QPS, "burst", config.Burst)

	kubeClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Fatalf("Failed to create kubernetes client: %v", err)
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		klog.Fatalf("Failed to create dynamic client: %v", err)
	}

	clients := ctrl.SharedClients{
		KubeClient:    kubeClient,
		DynamicClient: dynamicClient,
		RESTConfig:    config,
	}

	// Instantiate enabled controllers via their registered factories.
	var controllers []ctrl.Interface
	for _, name := range enabled {
		factory, ok := ctrl.Get(name)
		if !ok {
			klog.Fatalf("Unknown controller %q; known controllers: %v", name, ctrl.KnownControllers())
		}
		started := time.Now()
		klog.InfoS("Creating controller", "controller", name)
		c, fErr := factory(clients)
		if fErr != nil {
			klog.Fatalf("Failed to create controller %q: %v", name, fErr)
		}
		klog.InfoS("Controller created", "controller", name, "elapsed", time.Since(started).Round(time.Millisecond))
		controllers = append(controllers, c)
	}

	mgr := ctrl.NewManager(controllers)

	// Set up signal handling.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		klog.InfoS("Received signal, shutting down", "signal", sig)
		cancel()
	}()

	// Start HTTP server for pprof, metrics, and health.
	if httpEndpoint != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", legacyregistry.Handler())
		if gate.Enabled(features.Profiling) {
			mux.Handle("/metrics/profiling", profiling.MetricsHandler())
		}
		// /healthz serves as the K8s readiness probe.
		//   PhaseIdle     — standby, not yet leading          → 200
		//   PhaseStarting — leader, caches syncing             → 503
		//   PhaseRunning  — all controllers ready               → 200
		//   PhaseStopping — graceful shutdown, draining         → 503
		//   PhaseStopped  — shutdown complete                   → 503
		//   PhaseFailed   — controller error / unexpected exit  → 503
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			phase := mgr.Phase()
			if mgr.ReadinessOK() {
				w.WriteHeader(http.StatusOK)
				_, _ = fmt.Fprintf(w, "ok (phase: %s)", phase)
			} else {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = fmt.Fprintf(w, "not ready (phase: %s)", phase)
			}
		})
		// pprof handlers are registered on DefaultServeMux via the blank import;
		// delegate any unmatched /debug/ path so /debug/pprof/* works.
		mux.Handle("/debug/", http.DefaultServeMux)
		paths := []string{"/metrics", "/healthz", "/debug/pprof/"}
		if gate.Enabled(features.Profiling) {
			paths = append(paths, "/metrics/profiling")
		}
		go func() {
			klog.InfoS("Starting HTTP server", "endpoint", httpEndpoint, "paths", paths)
			server := &http.Server{
				Addr:              httpEndpoint,
				Handler:           mux,
				ReadHeaderTimeout: 10 * time.Second,
			}
			listener, err := net.Listen("tcp", server.Addr)
			if err != nil {
				klog.ErrorS(err, "HTTP server listen failed", "endpoint", httpEndpoint)
				return
			}
			klog.InfoS("HTTP server listening", "endpoint", listener.Addr().String(), "paths", paths)
			if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
				klog.ErrorS(err, "HTTP server failed", "endpoint", httpEndpoint)
			}
		}()
	} else {
		klog.InfoS("HTTP server disabled; metrics, readiness and pprof endpoints are unavailable")
	}

	if !leaderElect {
		klog.InfoS("Leader election disabled; starting controllers directly")
		if err := mgr.Run(ctx); err != nil {
			klog.Fatalf("Controller manager exited with error: %v", err)
		}
		klog.InfoS("RuntimeKit Controller Manager exited", "phase", mgr.Phase())
		return
	}

	// Leader election.
	//
	// Design: a single Lease is used for the entire process rather than
	// per-controller leases. This ensures all controllers in the same
	// Deployment share identical leader/standby state, preventing
	// split-brain scenarios where some controllers lead on one replica and
	// others on another. This mirrors the kube-controller-manager pattern
	// and simplifies operational monitoring (one Lease object to observe).
	//
	// Trade-off: a single misbehaving controller that fails to renew the
	// lease affects all controllers. Since they share a process, this is
	// the desired behaviour — partial leadership would cause inconsistent
	// cluster state that is harder to diagnose than a full restart.
	// namespace is guaranteed non-empty after normalizeNamespaces above.
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      "lj-runtimekit-job-controller",
			Namespace: namespace, // normalized — never empty
		},
		Client: kubeClient.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: podName,
		},
	}

	const (
		leaseDuration = 15 * time.Second
		renewDeadline = 10 * time.Second
		retryPeriod   = 2 * time.Second
	)
	klog.InfoS("Starting leader election; controllers start after the Lease is acquired",
		"lease", klog.KRef(lock.LeaseMeta.Namespace, lock.LeaseMeta.Name),
		"identity", podName,
		"leaseDuration", leaseDuration,
		"renewDeadline", renewDeadline,
		"retryPeriod", retryPeriod,
	)
	err = runLeaderElection(ctx, leaderelection.LeaderElectionConfig{
		Lock:          lock,
		LeaseDuration: leaseDuration,
		RenewDeadline: renewDeadline,
		RetryPeriod:   retryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnNewLeader: func(identity string) {
				if identity != podName {
					klog.InfoS("New leader elected; this replica stays on standby", "leader", identity, "identity", podName)
				}
			},
		},
	}, func(ctx context.Context) error {
		klog.InfoS("Acquired leadership, starting controllers", "identity", podName)
		return mgr.Run(ctx)
	})
	if err != nil {
		klog.Fatalf("Controller manager exited with error: %v", err)
	}
	klog.InfoS("RuntimeKit Controller Manager exited", "phase", mgr.Phase())
}

// buildConfig creates a rest.Config from kubeconfig path or in-cluster config.
func buildConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}

// normalizeNamespaces ensures that both namespace parameters are never empty.
// Empty values (including explicit --namespace="") are replaced with the safe
// defaults defined in the computedomain package.
func normalizeNamespaces(ns, cliqueNS string) (string, string) {
	if ns == "" {
		ns = computedomain.DefaultDriverNamespace
	}
	if cliqueNS == "" {
		cliqueNS = computedomain.DefaultCliqueNamespace
	}
	return ns, cliqueNS
}

// envOrDefault returns the value of an environment variable or a default.
func envOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
