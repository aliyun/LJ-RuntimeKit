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
	"crypto/tls"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	migrationv1beta1 "github.com/container-migration/controller/api/v1beta1"
	"github.com/container-migration/controller/pkg/config"
	"github.com/container-migration/controller/pkg/controllers"
	"github.com/container-migration/controller/pkg/controllers/configmap"
	"github.com/container-migration/controller/pkg/controllers/migrationjob"
	podcontroller "github.com/container-migration/controller/pkg/controllers/pod"
	"github.com/container-migration/controller/pkg/controllers/preheatjob"
	"github.com/container-migration/controller/pkg/dragonfly"
	"github.com/container-migration/controller/pkg/metrics"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	//+kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

// Build-time metadata, injected via -ldflags -X by both `make build` and the
// image build, and surfaced by the --version flag.
var (
	version   = "unknown"
	gitCommit = "unknown"
	buildTime = "unknown"
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(migrationv1beta1.AddToScheme(scheme))
	//+kubebuilder:scaffold:scheme
}

func main() {
	// Checked before InitFlags/flag.Parse: --version is not a registered flag,
	// so letting flag.Parse see it would exit with "flag provided but not
	// defined". Returning here also keeps --version usable without a
	// kubeconfig, because ctrl.GetConfigOrDie() only runs further below.
	if hasVersionArg(os.Args) {
		fmt.Printf("Version:     %s\n", version)
		fmt.Printf("Git Commit:  %s\n", gitCommit)
		fmt.Printf("Build Time:  %s\n", buildTime)
		return
	}

	cfg := config.NewConfig()
	cfg.InitFlags(flag.CommandLine)
	klog.InitFlags(nil)

	// The pod name is the leader election identity; POD_NAME is injected via
	// the Deployment's downward API. Fallback to hostname matches the
	// LJ-RuntimeKit-JobController behaviour.
	podName := flag.String("pod-name", envOrDefault("POD_NAME", ""), "Name of the controller pod, used as the leader election identity")
	leaseNamespace := flag.String("namespace", envOrDefault("NAMESPACE", "kube-system"), "Namespace of the leader election Lease (default kube-system)")

	flag.Parse()
	flag.VisitAll(func(f *flag.Flag) {
		fmt.Println(fmt.Sprintf("FLAG: --%s=%q", f.Name, f.Value))
	})

	ctrl.SetLogger(klog.NewKlogr())

	// if the enable-http2 flag is false (the default), server/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling server/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancelation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("disabling server/2")
		c.NextProtos = []string{"server/1.1"}
	}

	tlsOpts := []func(*tls.Config){}
	if !cfg.EnableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	webhookServer := webhook.NewServer(webhook.Options{
		TLSOpts: tlsOpts,
	})

	restConfig := ctrl.GetConfigOrDie()
	mgr, err := ctrl.NewManager(restConfig, ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: "0", // Disable built-in metrics server; we start it async below
		},
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: "0", // Disabled: a standalone health server below answers probes on cfg.ProbeAddr
		// Leader election is intentionally left off (the controller-runtime
		// default) here: the outer runLeaderElection wrapper (cmd/leader.go)
		// drives a single Lease for the whole process, so all controllers
		// share one leader/standby state and split-brain is impossible.
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	sharedContext, err := controllers.NewContext(cfg,
		mgr.GetClient(),
		mgr.GetEventRecorderFor("lj-runtimekit-migration-controller"),
		restConfig)
	if err != nil {
		setupLog.Error(err, "unable to create shared context")
		os.Exit(1)
	}
	// APIReader reads the apiserver directly (bypassing the informer cache), only used to scan node busy state before a worker drift replacement.
	sharedContext.APIReader = mgr.GetAPIReader()

	if err = configmap.NewConfigMapReconciler(mgr.GetScheme(), sharedContext).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create configmap controller")
		os.Exit(1)
	}

	if err = migrationjob.NewMigrationJobReconciler(
		mgr.GetScheme(), sharedContext).
		SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create migration job controller")
		os.Exit(1)
	}

	if err = podcontroller.NewPodReconciler(mgr.GetScheme(), sharedContext).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create pod controller")
		os.Exit(1)
	}

	// The PreheatJob subsystem is off by default: it requires Dragonfly manager
	// and scheduler already deployed in the cluster. Once enabled, any client
	// init failure only degrades to skipping preheat and must not drag down the
	// whole controller - the migration main path does not depend on preheat.
	if cfg.EnablePreheatJob {
		if err := setupPreheatJob(mgr, sharedContext, cfg); err != nil {
			setupLog.Error(err, "PreheatJob subsystem disabled due to initialization failure")
		}
	}

	//+kubebuilder:scaffold:builder

	// Standalone health server answering liveness/readiness probes. It is
	// started before leader election so standby (non-leading) replicas also
	// answer probes instead of being killed by the liveness probe. The
	// manager's built-in health server is disabled above to avoid a port
	// conflict.
	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	healthMux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	go func() {
		if err := http.ListenAndServe(cfg.ProbeAddr, healthMux); err != nil {
			setupLog.Error(err, "Health server failed to start")
		}
	}()

	// Start Prometheus metrics server asynchronously.
	// If port binding fails (e.g. port conflict), the main process keeps running;
	// only metrics collection is unavailable.
	go func() {
		if cfg.MetricsAddr == "" || cfg.MetricsAddr == "0" {
			return
		}
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.HandlerFor(crmetrics.Registry, promhttp.HandlerOpts{}))
		setupLog.Info("starting async metrics server", "addr", cfg.MetricsAddr)
		if err := http.ListenAndServe(cfg.MetricsAddr, mux); err != nil {
			setupLog.Error(err, "Metrics server failed to start, metrics will be unavailable", "addr", cfg.MetricsAddr)
		}
	}()

	setupLog.Info("starting manager")

	metrics.SetHealthStatus("healthy")

	// Signal handling: SIGTERM/SIGINT cancel the root context, which drives
	// both leader election shutdown and manager drain.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		setupLog.Info("Received signal, shutting down", "signal", sig)
		cancel()
	}()

	if !cfg.EnableLeaderElection {
		setupLog.Info("Leader election disabled; starting manager directly")
		if err := mgr.Start(ctx); err != nil {
			setupLog.Error(err, "problem running manager")
			metrics.SetHealthStatus("unhealthy")
			os.Exit(1)
		}
		return
	}

	// Leader election.
	//
	// A single Lease drives the whole process (not per-controller leases),
	// so all controllers share one leader/standby state. This mirrors the
	// LJ-RuntimeKit-JobController pattern: the manager only starts after the
	// Lease is acquired, and it is torn down the moment leadership is lost.
	// The Lease lives in the controller's own namespace; RBAC for leases is
	// namespaced there (deploy/rbac.yaml).
	if *podName == "" {
		*podName = mustHostname()
		setupLog.Info("--pod-name or POD_NAME not set; falling back to hostname", "hostname", *podName)
	}

	kubeClient, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		setupLog.Error(err, "unable to create kubernetes client")
		os.Exit(1)
	}
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      "lj-runtimekit-migration-controller",
			Namespace: *leaseNamespace,
		},
		Client: kubeClient.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: *podName,
		},
	}

	const (
		leaseDuration = 15 * time.Second
		renewDeadline = 10 * time.Second
		retryPeriod   = 2 * time.Second
	)
	setupLog.Info("Starting leader election; manager starts after the Lease is acquired",
		"lease", lock.Describe(),
		"identity", *podName,
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
				if identity != *podName {
					setupLog.Info("New leader elected; this replica stays on standby", "leader", identity, "identity", *podName)
				}
			},
		},
	}, func(ctx context.Context) error {
		setupLog.Info("Acquired leadership, starting manager", "identity", *podName)
		return mgr.Start(ctx)
	})
	if err != nil {
		setupLog.Error(err, "problem running manager")
		metrics.SetHealthStatus("unhealthy")
		os.Exit(1)
	}
}

// setupPreheatJob wires up the PreheatJob controller and the cache probe loop.
//
// On error the caller only logs without exiting: preheat is an acceleration
// measure; Dragonfly not being deployed must not bring down the migration main
// path along with it.
func setupPreheatJob(mgr ctrl.Manager, sharedContext *controllers.SharedContext, cfg *config.Config) error {
	dfClient, err := dragonfly.NewClient(dragonfly.Config{
		ManagerURL:      cfg.DragonflyManagerURL,
		K8sClient:       mgr.GetClient(),
		SecretNamespace: cfg.DragonflySecretNamespace,
		SecretName:      cfg.DragonflySecretName,
		SecretKey:       cfg.DragonflyPATTokenKey,
	})
	if err != nil {
		return fmt.Errorf("create dragonfly manager client: %w", err)
	}

	seedPeerSource, err := dragonfly.NewSchedulerClient(cfg.DragonflySchedulerAddress)
	if err != nil {
		return fmt.Errorf("create dragonfly scheduler client: %w", err)
	}

	if err := preheatjob.NewPreheatJobReconciler(mgr.GetScheme(), sharedContext, dfClient).
		SetupWithManager(mgr); err != nil {
		return fmt.Errorf("create preheat job controller: %w", err)
	}

	probeController := preheatjob.NewProbeController(sharedContext, seedPeerSource,
		dragonfly.NewDfdaemonProber(), cfg.PreheatJobProbeInterval)
	if err := mgr.Add(probeController); err != nil {
		return fmt.Errorf("add preheat job probe controller: %w", err)
	}

	setupLog.Info("PreheatJob subsystem enabled",
		"managerURL", cfg.DragonflyManagerURL,
		"schedulerAddress", cfg.DragonflySchedulerAddress,
		"probeInterval", cfg.PreheatJobProbeInterval)
	return nil
}

// envOrDefault returns the value of an environment variable or a default.
func envOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

// mustHostname returns the machine hostname, falling back to a stable
// placeholder on error. It exists so the leader election identity always
// has a value even outside a Pod context.
func mustHostname() string {
	name, err := os.Hostname()
	if err != nil || name == "" {
		return "lj-runtimekit-migration-controller"
	}
	return name
}

// hasVersionArg reports whether build metadata was requested.
//
// Deliberately does not match "-v", unlike migration-worker: klog owns -v in
// this binary (klog.InitFlags registers it as the log verbosity level) and the
// deployed pod args are "-v=4". The space-separated form "-v 4" is equally
// legal for klog and would leave a bare "-v" token in os.Args, so matching it
// here would make the controller print its version and exit instead of
// starting.
func hasVersionArg(args []string) bool {
	for _, arg := range args {
		if arg == "--version" || arg == "-version" {
			return true
		}
	}
	return false
}
