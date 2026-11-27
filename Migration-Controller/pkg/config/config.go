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

package config

import (
	"flag"
	"os"
	"strings"
	"time"
)

const (
	// defaultRPCTimeout is the fallback for all worker unary RPCs without their
	// own deadline (applied in the interceptor of migrationworker.NewDefaultClient).
	// 2min matches the existing deployment flag --rpc-timeout=2m, avoiding a
	// shorter implicit timeout when no parameter is passed. It does not affect
	// RestoreXPUOnNode: that call carries its own longer deadline computed from
	// the container count, which the interceptor lets through.
	defaultRPCTimeout = time.Minute * 2

	// DefaultWorkerNamespace is only the fallback when the controller's own
	// namespace cannot be detected (e.g. running bare outside a cluster for
	// debugging). Normal in-cluster runs always take the detection result, keeping
	// worker pods in the same namespace as the controller.
	DefaultWorkerNamespace = "kube-system"

	// serviceAccountNamespaceFile is the namespace file kubelet auto-mounts for
	// every pod, readable without the downward API.
	serviceAccountNamespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

	DefaultRetryWaitingTime = time.Second
	// DefaultWorkerSyncTime is the fallback refresh period of the worker config
	// ConfigMap cache. Not relied upon when watch events work normally; only
	// prevents the cache from staying stale after events are lost.
	DefaultWorkerSyncTime = time.Minute * 2
	DefaultHTTPServerPort = 9000

	// DefaultWorkerDrainPollInterval is the interval at which the controller
	// re-probes whether a node has drained when its worker config is stale but the
	// node still has in-flight tasks. The task main loop requeues at a shorter
	// period; this interval only throttles the direct-read scan of the node's busy
	// state.
	DefaultWorkerDrainPollInterval = time.Second * 10
	// DefaultWorkerReadyTimeout is the cap for waiting for the new pod to reach
	// Running+Ready after a worker replacement. Covers the worst case of pulling
	// the worker image once; beyond it the replacement is judged failed with an
	// alert instead of a silent wait.
	DefaultWorkerReadyTimeout = time.Minute * 2

	// DefaultInitContainerImage is empty: when unset, the blocking init container
	// uses the image of the source pod's first business container directly. The
	// source image can certainly be pulled on the target cluster (the source pod
	// ran on it), while a hardcoded external image like busybox would leave
	// replica pods stuck in ImagePullBackOff on offline clusters.
	DefaultInitContainerImage = ""

	// DefaultCheckpointDataRootDir is the on-disk root of checkpoint data on the
	// target node in the restore stage. It must match the migration hostPath mount
	// point in the worker pod template, otherwise the data lands in the worker
	// container's writable layer and is lost as soon as the worker pod is
	// reclaimed.
	DefaultCheckpointDataRootDir = "/home/admin/t4/migration"
)

const (
	DefaultWorkerConfigMapName = "lj-runtimekit-migration-controller-config"
	WorkerPodTemplateKey       = "template"
	GrpcPortKey                = "grpcPort"
)

const (
	EnvTest       = "test"
	EnvProduction = "production"
)

// Config holds all configuration for the migration controller.
type Config struct {
	MetricsAddr          string
	EnableLeaderElection bool
	ProbeAddr            string
	SecureMetrics        bool
	EnableHTTP2          bool

	Env                     string
	MaxConcurrentReconciles int
	ConfigMapName           string

	// WorkerNamespace is the namespace worker pods are created in. Defaults to
	// the controller's own namespace, keeping the controller and workers in the
	// same namespace and thus reusing the same RBAC set.
	// ServiceAccount / RBAC。
	WorkerNamespace string

	// EnablePreheatJob switches the PreheatJob subsystem. Preheat depends on
	// Dragonfly manager / scheduler being deployed in the cluster; enabling it
	// without a deployment only spams error logs, so it is off by default.
	EnablePreheatJob bool
	// DragonflyManagerURL is the Dragonfly Manager HTTP API address.
	DragonflyManagerURL string
	// DragonflySchedulerAddress is the Dragonfly Scheduler gRPC address, used with ListHosts to discover seed peers.
	DragonflySchedulerAddress string
	// PreheatJobProbeInterval is the interval for probing the cache state of ready PreheatJobs.
	PreheatJobProbeInterval time.Duration
	// DragonflySecretNamespace / DragonflySecretName / DragonflyPATTokenKey
	// Secret coordinates holding the Dragonfly Manager auth token.
	DragonflySecretNamespace string
	DragonflySecretName      string
	DragonflyPATTokenKey     string

	ControllerConfig
}

// NewConfig creates a new Config with default values.
func NewConfig() *Config {
	return &Config{
		Env:                     EnvProduction,
		MetricsAddr:             ":8080",
		ProbeAddr:               ":8081",
		EnableLeaderElection:    true,
		SecureMetrics:           false,
		EnableHTTP2:             false,
		MaxConcurrentReconciles: 4,
		ConfigMapName:           DefaultWorkerConfigMapName,
		WorkerNamespace:         CurrentNamespace(),

		EnablePreheatJob:          false,
		DragonflyManagerURL:       "http://dragonfly-manager.dragonfly.svc.cluster.local:8080",
		DragonflySchedulerAddress: "dragonfly-scheduler.dragonfly.svc.cluster.local:8002",
		PreheatJobProbeInterval:   10 * time.Second,
		DragonflySecretNamespace:  "dragonfly",
		DragonflySecretName:       "dragonfly-preheat-pat",
		DragonflyPATTokenKey:      "PAT_TOKEN",

		ControllerConfig: ControllerConfig{
			RPCTimeout:              defaultRPCTimeout,
			RetryWaitTime:           DefaultRetryWaitingTime,
			RestoreRetryWaitTime:    DefaultRetryWaitingTime,
			CheckpointRetryWaitTime: DefaultRetryWaitingTime,
			WorkerSyncTime:          DefaultWorkerSyncTime,
			WorkerDrainPollInterval: DefaultWorkerDrainPollInterval,
			WorkerReadyTimeout:      DefaultWorkerReadyTimeout,
			CheckpointDataRootDir:   DefaultCheckpointDataRootDir,
			InitContainerImage:      DefaultInitContainerImage,
		},
	}
}

func (c *Config) InitFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.Env, "env", c.Env, "The deploy env, one of `production` or `test`.")
	fs.StringVar(&c.MetricsAddr, "metrics-bind-address", c.MetricsAddr, "The address the metric endpoint binds to.")
	fs.StringVar(&c.ProbeAddr, "health-probe-bind-address", c.ProbeAddr, "The address the probe endpoint binds to.")
	fs.BoolVar(&c.EnableLeaderElection, "leader-election", c.EnableLeaderElection,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	fs.BoolVar(&c.SecureMetrics, "metrics-secure", c.SecureMetrics,
		"If set the metrics endpoint is served securely")
	fs.BoolVar(&c.EnableHTTP2, "enable-http2", c.EnableHTTP2,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")

	// The parallelism applies to "different CRs"; the same object is still
	// guaranteed by controller-runtime's workqueue to never be reconciled
	// concurrently. Default raised from 1 to 4: under serial processing any one
	// slow RPC could stall migration tasks cluster-wide, while concurrency
	// downgrades the impact to occupying only 1/N of the processing capacity.
	// The apiserver-side default rate limit is QPS=20/Burst=30 (what
	// controller-runtime fills in when QPS==0); 4-way concurrency is far from
	// the cap.
	fs.IntVar(&c.MaxConcurrentReconciles, "max-concurrent-reconciles", c.MaxConcurrentReconciles,
		"Set the maximum number of concurrent reconciles.")
	fs.StringVar(&c.ConfigMapName, "worker-config-map-name", c.ConfigMapName, "The ConfigMap name for worker.")
	fs.StringVar(&c.WorkerNamespace, "worker-namespace", c.WorkerNamespace,
		"The namespace where migration worker pods are created. "+
			"Defaults to the namespace the controller itself runs in.")

	fs.BoolVar(&c.EnablePreheatJob, "enable-preheat-job", c.EnablePreheatJob,
		"Enable the PreheatJob subsystem. Requires a Dragonfly manager and scheduler deployed in the cluster.")
	fs.StringVar(&c.DragonflyManagerURL, "dragonfly-manager-url", c.DragonflyManagerURL,
		"The base URL of the Dragonfly Manager HTTP API.")
	fs.StringVar(&c.DragonflySchedulerAddress, "dragonfly-scheduler-address", c.DragonflySchedulerAddress,
		"The gRPC address of the Dragonfly Scheduler, used to discover alive seed peers via ListHosts.")
	fs.DurationVar(&c.PreheatJobProbeInterval, "preheat-job-probe-interval", c.PreheatJobProbeInterval,
		"The interval at which the probe controller checks the cache status of PreheatJobs.")
	fs.StringVar(&c.DragonflySecretNamespace, "dragonfly-secret-namespace", c.DragonflySecretNamespace,
		"The namespace of the Secret holding Dragonfly authentication tokens.")
	fs.StringVar(&c.DragonflySecretName, "dragonfly-secret-name", c.DragonflySecretName,
		"The name of the Secret holding Dragonfly authentication tokens.")
	fs.StringVar(&c.DragonflyPATTokenKey, "dragonfly-pat-token-key", c.DragonflyPATTokenKey,
		"The data key in the Dragonfly Secret that holds the PAT token.")

	c.initControllerFlags(fs)
}

// CurrentNamespace returns the namespace the controller itself lives in:
// prefer the POD_NAMESPACE env var (for easy override when debugging outside
// the cluster), otherwise read the in-cluster auto-mounted namespace file.
func CurrentNamespace() string {
	if ns := strings.TrimSpace(os.Getenv("POD_NAMESPACE")); ns != "" {
		return ns
	}
	if data, err := os.ReadFile(serviceAccountNamespaceFile); err == nil {
		if ns := strings.TrimSpace(string(data)); ns != "" {
			return ns
		}
	}
	return DefaultWorkerNamespace
}

// ControllerConfig holds controller-level configuration.
type ControllerConfig struct {
	RPCTimeout              time.Duration
	WorkerSyncTime          time.Duration
	RetryWaitTime           time.Duration
	RestoreRetryWaitTime    time.Duration
	CheckpointRetryWaitTime time.Duration

	// WorkerDrainPollInterval is the throttled re-probe interval for busy state
	// when the worker config has drifted and the node is busy; also used as the
	// requeue interval of blocked tasks.
	WorkerDrainPollInterval time.Duration
	// WorkerReadyTimeout is the cap for waiting for the new pod to be Running+Ready after a worker replacement.
	WorkerReadyTimeout time.Duration

	// CheckpointDataRootDir is the restore on-disk root; PrepareDataHandler uses it to compose homeDir.
	CheckpointDataRootDir string
	// InitContainerImage is the image of the blocking init container. Only needs sh and sleep.
	InitContainerImage string
}

func (c *Config) initControllerFlags(fs *flag.FlagSet) {
	fs.DurationVar(&c.RPCTimeout, "rpc-timeout", c.RPCTimeout,
		"Set rpc timeout for client to migration worker.")
	fs.DurationVar(&c.RetryWaitTime, "retry-wait-time", c.RetryWaitTime,
		"Set retry wait time for objects handled by controller.")
	fs.DurationVar(&c.WorkerSyncTime, "worker-sync-time", c.WorkerSyncTime,
		"Set the fallback refresh interval of the worker config ConfigMap cache.")
	fs.DurationVar(&c.WorkerDrainPollInterval, "worker-drain-poll-interval", c.WorkerDrainPollInterval,
		"When a node's worker config is stale but the node still has in-flight tasks, "+
			"the interval at which the controller re-checks whether the node has drained.")
	fs.DurationVar(&c.WorkerReadyTimeout, "worker-ready-timeout", c.WorkerReadyTimeout,
		"The maximum time to wait for a newly created worker pod to become Running+Ready "+
			"after a config-driven replacement.")
	fs.StringVar(&c.CheckpointDataRootDir, "checkpoint-data-root-dir", c.CheckpointDataRootDir,
		"Root directory on target nodes where checkpoint data is extracted. "+
			"Must match the migration hostPath mount in the worker pod template.")
	fs.StringVar(&c.InitContainerImage, "init-container-image", c.InitContainerImage,
		"Image used by the blocking init container injected into restore target pods.")
}
