package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
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

	cdapi "github.com/lj-runtimekit/job-controller/pkg/computedomain"
	"github.com/lj-runtimekit/job-controller/pkg/constants"
	ctrl "github.com/lj-runtimekit/job-controller/pkg/controller"
	"github.com/lj-runtimekit/job-controller/pkg/controller/computedomain"

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
		enableControllers            string
	)

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
	flag.IntVar(&workers, "workers", 2, "Number of reconcile workers")
	flag.StringVar(&enableControllers, "enable-controllers", "*", `Comma-separated list of controllers to enable, or "*" for all registered controllers`)

	klog.InitFlags(nil)
	flag.Parse()

	// Normalize namespace flags: explicit --namespace="" must not bypass the
	// safe default.  The normalization is extracted into normalizeNamespaces
	// (testable) and also backed by NewController's NormalizeNamespaces as a
	// defense-in-depth measure.
	namespace, computeDomainCliqueNamespace = normalizeNamespaces(namespace, computeDomainCliqueNamespace)

	if podName == "" {
		klog.Warning("--pod-name or POD_NAME not set, leader election identity may be unreliable")
		podName, _ = os.Hostname()
	}

	if imexConfigOverrides != "" {
		klog.Fatal("--imex-config-overrides is not implemented; non-empty values are unsupported")
	}

	// ---------------------------------------------------------------
	// Register available controllers.
	//
	// To add a new controller:
	//   1. Implement controller.Interface in a new sub-package.
	//   2. Register its Factory here (capturing controller-specific flags).
	//   3. The manager handles concurrent start, stop, and error propagation.
	// ---------------------------------------------------------------
	computedomain.RegisterComputeDomainMetrics()
	ctrl.Register(computedomain.ControllerName, computedomain.NewFactory(computedomain.Config{
		Namespace:                    namespace,
		ComputeDomainCliqueNamespace: computeDomainCliqueNamespace,
		MaxNodes:                     maxNodes,
		IMEXIsolation:                isolation,
		IMEXConfigOverrides:          imexConfigOverrides,
	}, workers))

	// Resolve which controllers to enable.
	enabled := resolveEnabledControllers(enableControllers, ctrl.KnownControllers())

	klog.InfoS("Starting RuntimeKit Controller Manager",
		"podName", podName,
		"namespace", namespace,
		"computeDomainCliqueNamespace", computeDomainCliqueNamespace,
		"computeDomainAPIGroups", cdapi.SupportedAPIGroups(),
		"enabledControllers", enabled,
		"leaderElection", leaderElect,
	)

	// Build K8s config (shared across all controllers).
	config, err := buildConfig(kubeconfig)
	if err != nil {
		klog.Fatalf("Failed to build kubernetes config: %v", err)
	}

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
	}

	// Instantiate enabled controllers via their registered factories.
	var controllers []ctrl.Interface
	for _, name := range enabled {
		factory, ok := ctrl.Get(name)
		if !ok {
			klog.Fatalf("Unknown controller %q; known controllers: %v", name, ctrl.KnownControllers())
		}
		c, fErr := factory(clients)
		if fErr != nil {
			klog.Fatalf("Failed to create controller %q: %v", name, fErr)
		}
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
		go func() {
			klog.InfoS("Starting HTTP server", "endpoint", httpEndpoint)
			server := &http.Server{
				Addr:              httpEndpoint,
				Handler:           mux,
				ReadHeaderTimeout: 10 * time.Second,
			}
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				klog.ErrorS(err, "HTTP server failed")
			}
		}()
	}

	// run wraps Manager.Run; used by both the leader-election and standalone
	// code paths.
	run := func(ctx context.Context) {
		if err := mgr.Run(ctx); err != nil {
			klog.Fatalf("Controller manager exited with error: %v", err)
		}
	}

	if !leaderElect {
		run(ctx)
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
			Name:      "computedomain-controller",
			Namespace: namespace, // normalized — never empty
		},
		Client: kubeClient.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: podName,
		},
	}

	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		LeaseDuration:   15 * time.Second,
		RenewDeadline:   10 * time.Second,
		RetryPeriod:     2 * time.Second,
		ReleaseOnCancel: true,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				klog.InfoS("Acquired leadership, starting controllers")
				run(ctx)
			},
			OnStoppedLeading: func() {
				klog.InfoS("Lost leadership, shutting down")
				cancel()
			},
			OnNewLeader: func(identity string) {
				if identity != podName {
					klog.InfoS("New leader elected", "leader", identity)
				}
			},
		},
	})
}

// resolveEnabledControllers parses the --enable-controllers flag value.
// "*" (or empty) enables all known controllers; otherwise only the
// comma-separated names are returned.
func resolveEnabledControllers(flag string, known []string) []string {
	flag = strings.TrimSpace(flag)
	if flag == "*" || flag == "" {
		return known
	}
	var result []string
	for _, name := range strings.Split(flag, ",") {
		name = strings.TrimSpace(name)
		if name != "" {
			result = append(result, name)
		}
	}
	return result
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
