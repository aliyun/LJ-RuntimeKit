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
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/container-migration/worker/cmd/migration-worker/options"
	pb "github.com/container-migration/worker/pkg/apis"
	"github.com/container-migration/worker/pkg/controller"
	"github.com/container-migration/worker/pkg/runtime"
	"github.com/container-migration/worker/pkg/storage"
	mgutil "github.com/container-migration/worker/pkg/util"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
)

// Build-time metadata. They are populated by -ldflags at build time
// and surfaced via the --version flag.
var (
	version   = "unknown"
	buildTime = "unknown"
	gitCommit = "unknown"
)

func main() {
	opts := options.Default()
	command := &cobra.Command{
		Use:  "migration-worker",
		Long: "migration-worker is a node-side daemon that performs container checkpoint / restore operations on behalf of the migration controller.",
		Run: func(cmd *cobra.Command, args []string) {
			if err := runCommand(cmd, opts); err != nil {
				fmt.Fprintf(os.Stderr, "%v\n", err)
				os.Exit(1)
			}
		},
		Args: func(cmd *cobra.Command, args []string) error {
			for _, arg := range args {
				if len(arg) > 0 {
					return fmt.Errorf("%q does not take any arguments, got %q", cmd.CommandPath(), args)
				}
			}
			return nil
		},
	}
	klog.InitFlags(nil)
	fs := opts.Flags()
	fs.AddGoFlagSet(flag.CommandLine)
	command.Flags().AddFlagSet(fs)

	if hasVersionArg(os.Args) {
		fmt.Printf("Version:     %s\n", version)
		fmt.Printf("Git Commit:  %s\n", gitCommit)
		fmt.Printf("Build Time:  %s\n", buildTime)
		return
	}

	runtime.NodeName = os.Getenv("KUBE_NODE_NAME")
	klog.V(4).Infof("NodeName: %s", runtime.NodeName)
	if runtime.NodeName == "" {
		klog.Fatalln("Please set env KUBE_NODE_NAME")
	}

	defer klog.Flush()
	if err := command.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

// runCommand wires up the controller manager, the metrics server and
// the gRPC server, then blocks until a SIGTERM-style signal arrives
// at which point it triggers a graceful shutdown.
func runCommand(cmd *cobra.Command, opts *options.Options) error {
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		klog.Infof("FLAG: --%s=%q", f.Name, f.Value)
	})

	// PVC restore preheat backend flag validation: invalid values fail at startup,
	// so a mistyped flag is caught before reaching the restore phase (the degraded
	// behavior is silent and hard to notice).
	if opts.PvcPreheatBackend != options.PvcPreheatBackendDragonfly && opts.PvcPreheatBackend != options.PvcPreheatBackendEFC {
		return fmt.Errorf("invalid --pvc-preheat-backend %q: must be %q or %q",
			opts.PvcPreheatBackend, options.PvcPreheatBackendDragonfly, options.PvcPreheatBackendEFC)
	}

	// Preheat source mode: only serves /mnt/pv over http (Dragonfly seed peer
	// origin fetch); gRPC/prometheus/controller scheduler are not started. Range is
	// natively supported by net/http (http.ServeContent), which dfdaemon's ranged
	// origin fetch depends on.
	if opts.ServeHTTP {
		return runServeHTTP(opts)
	}

	stopCh := make(chan struct{})

	// PVC mode (reads the ClusterIP of the long-running preheat Service and
	// self-registers the EndpointSlice co-hosted source) and the PreheatJob
	// resolution for storageType=dragonfly/efc both need the apiserver. Without
	// in-cluster config (e.g. running the worker directly on the host) only warn:
	// dragonfly can still use the oss_mount_point path, PVC mode degrades to direct
	// /mnt/pv reads, and other storage backends do not need it at all.
	// apiserver。
	if cfg, err := rest.InClusterConfig(); err != nil {
		klog.Warningf("no in-cluster kube config, PreheatJob resolution and PVC preheat source will be disabled: %v", err)
	} else if err := storage.SetKubeRESTConfig(cfg); err != nil {
		klog.Warningf("failed to init kube client, PreheatJob resolution and PVC preheat source will be disabled: %v", err)
	}

	controllerManager, err := controller.NewManager(opts)
	if err != nil {
		klog.Errorf("new migration worker manager failed: %v", err)
		return err
	}
	go controllerManager.ControllerScheduler.Run(stopCh)

	go func() {
		klog.V(4).Infof("Starting prometheus server on %v", opts.PrometheusPort)
		http.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe(fmt.Sprintf(":%v", opts.PrometheusPort), nil); err != nil {
			klog.Errorf("Prometheus monitoring failed: %v", err)
		}
	}()

	grpcAddr := fmt.Sprintf(":%d", opts.GrpcPort)
	lis, err := listenWithRetry(grpcAddr)
	if err != nil {
		klog.Errorf("failed to listen on %s: %v", grpcAddr, err)
		return err
	}
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(unaryServerInterceptor))
	pb.RegisterContainerStateMigrationServiceServer(grpcServer, controllerManager)
	klog.V(0).Infof("Server listening at %v", lis.Addr())

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		sig := <-stop
		klog.V(4).Infof("recv sig %v, shutting down gracefully...", sig)
		klog.V(0).Info("Gracefully stopping gRPC server...")
		grpcServer.GracefulStop()

		if err := lis.Close(); err != nil {
			klog.Errorf("close listener failed: %v", err)
		}
		close(stopCh)
		klog.Info("Server stopped.")
	}()

	if err := grpcServer.Serve(lis); err != nil {
		klog.Errorf("failed to serve: %v", err)
		return err
	}
	return nil
}

// runServeHTTP implements the preheat source pod's file service: exposes the
// PVC mount point /mnt/pv as the static file root, and exits immediately on
// SIGTERM/SIGINT.
//
// Deliberately Close instead of Shutdown (no draining in-flight requests): this
// pod only serves Dragonfly seed peer origin fetch; when it is deleted
// (markFailed, PreheatJob CR deletion, node eviction), in-flight large shard
// transfers get cut, and the worker-side PVCWithPreheat classifies the
// connection reset as retryable - after retries are exhausted it falls back to
// per-part direct reads of the mounted PVC; dragonfly also extracts into
// <dstDir>-tmp and only atomically renames on success, so falling back never
// pollutes the target dir. Interruption therefore only costs speed (direct-read
// fallback is much slower than a P2P hit), never restore correctness - not
// worth raising terminationGracePeriodSeconds to drain (the default 30s is far
// from enough for 8-way concurrent transfers of a 6GB shard anyway; raising it
// only postpones "cut immediately" to "cut after 30s").
func runServeHTTP(opts *options.Options) error {
	mux := http.NewServeMux()
	// An isolated mux only, to avoid polluting /metrics on the global DefaultServeMux.
	mux.Handle("/", http.FileServer(http.Dir("/mnt/pv")))
	addr := fmt.Sprintf(":%d", opts.HTTPPort)

	srv := &http.Server{Addr: addr, Handler: mux}
	klog.V(0).Infof("serve-http mode: serving /mnt/pv on %s", addr)

	// ListenAndServe runs in a goroutine and the main flow blocks on the signal:
	// otherwise the close would race with the main flow exiting - as soon as
	// ListenAndServe returns, main returns and the process exits, while the close
	// in the other goroutine has not finished; the stopped log would be printed
	// while connections are still alive, and the logs race with main's defer
	// klog.Flush() before process exit.
	serveErr := make(chan error, 1)
	go func() {
		err := srv.ListenAndServe()
		if err == http.ErrServerClosed {
			// Normal teardown triggered by srv.Close() below, not an error.
			err = nil
		}
		serveErr <- err
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	select {
	case err := <-serveErr:
		// The server stopped on its own without a signal: only a ListenAndServe start
		// failure reaches here. A nil err would mean ListenAndServe returned
		// ErrServerClosed with nobody calling Close - impossible on the current code
		// path.
		if err == nil {
			return nil
		}
		// No special-case for port conflicts on bind failure (which once exited with
		// code 98 for controller-side port-switch self-healing): the preheat source pod
		// now uses pod networking - each pod has its own netns, and in serve-http mode
		// only this one port binds (neither gRPC nor prometheus starts), so a conflict
		// is structurally unreachable; the controller-side exit-code self-healing was
		// also removed along with the port pool mechanism. Return the error uniformly
		// and let main exit with code 1 into CrashLoopBackOff.
		klog.Errorf("http file server failed: %v", err)
		return err
	case sig := <-stop:
		klog.V(0).Infof("recv sig %v, closing http server...", sig)
	}

	// Close immediately closes the listener and all active connections without waiting; returns in milliseconds.
	if err := srv.Close(); err != nil {
		klog.Warningf("close http file server: %v", err)
	}
	klog.Info("http file server stopped.")
	return nil
}

// unaryServerInterceptor records per-RPC latency and status into
// Prometheus and emits a structured access log.
func unaryServerInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	duration := time.Since(start)
	methodName := mgutil.ParseFullMethod(info.FullMethod)
	if methodName != "" {
		mgutil.RecordMigrationWorkerGrpcLatency(runtime.NodeName, methodName, duration.Seconds())
	}
	status := 0
	if err != nil {
		status = 1
	}
	mgutil.RecordMigrationWorkerGrpcStatus(runtime.NodeName, methodName, float64(status))
	klog.V(0).Infof("[grpc] method=%s, status=%d, took=%s",
		info.FullMethod, status, duration.Truncate(time.Millisecond))
	return resp, err
}

// hasVersionArg returns true when the user passed --version / -v on
// the command line. It is checked before cobra parsing so the worker
// can print version metadata and exit without setting up the runtime.
func hasVersionArg(args []string) bool {
	for _, arg := range args {
		if arg == "--version" || arg == "-v" {
			return true
		}
	}
	return false
}

// listenWithRetry listens on the gRPC port with retries.
//
// Task-level workers use hostNetwork and share the node port pool with other
// workers; the port is allocated by the controller. The controller's allocation
// view is based on pod objects in the apiserver, but the port is not really
// released until the old pod's container process exits (deletion grace, kubelet
// cleanup, orphaned processes - async windows) - a new worker may get the port
// of a just-deleted worker and fail to bind. On bind failure retry at a fixed
// interval, covering the 30s deletion grace plus redundancy, self-healing
// within the window without exiting; only after exhausting retries return an
// error (the upper layer exits into CrashLoop and kubelet restarts it).
func listenWithRetry(grpcAddr string) (net.Listener, error) {
	const (
		retryInterval = 2 * time.Second
		maxAttempts   = 90 // 3 minutes: covers the pod deletion grace (30s) plus redundancy
	)
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		lis, err := net.Listen("tcp", grpcAddr)
		if err == nil {
			if attempt > 1 {
				klog.Infof("gRPC port %s acquired after %d attempts", grpcAddr, attempt)
			}
			return lis, nil
		}
		if attempt == maxAttempts {
			return nil, err
		}
		klog.Warningf("failed to listen on %s (attempt %d/%d), retrying in %s: %v",
			grpcAddr, attempt, maxAttempts, retryInterval, err)
		time.Sleep(retryInterval)
	}
	return nil, fmt.Errorf("unreachable") // the loop necessarily returns at maxAttempts; defensive guard
}
