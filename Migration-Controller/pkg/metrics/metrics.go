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

package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

const (
	SubSystemMigrationController = "migration_controller"
)

var (
	configMapErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: SubSystemMigrationController,
		Name:      "config_map_parse_error",
		Help:      "count the error at configmap parsing.",
	}, []string{"name", "key"})

	rpcCallLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Subsystem: SubSystemMigrationController,
		Name:      "rpc_call_latency",
		Help:      "count the latency at rpc call.",
		Buckets: []float64{
			float64(time.Second),
			float64(time.Second * 5),
			float64(time.Second * 10),
			float64(time.Second * 15),
			float64(time.Second * 20),
		},
	}, []string{"api", "errCode"})

	healthStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: SubSystemMigrationController,
		Name:      "health_status",
		Help:      "health status of the migration controller.",
	}, []string{"status"})

	workerReplaceTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: SubSystemMigrationController,
		Name:      "worker_replace_total",
		Help:      "count of stale worker replacements triggered by config drift, by result.",
	}, []string{"node", "result"})

	workerReplaceBlocked = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: SubSystemMigrationController,
		Name:      "worker_replace_blocked",
		Help:      "1 when a stale worker on a node cannot be replaced because the node still has in-flight tasks.",
	}, []string{"node"})
)

func init() {
	metrics.Registry.MustRegister(configMapErrors, rpcCallLatency, healthStatus,
		workerReplaceTotal, workerReplaceBlocked)
}

func RecordRPCCallLatency(api, errCode string, latency float64) {
	rpcCallLatency.WithLabelValues(api, errCode).Observe(latency)
}

func RecordConfigMapError(name, key string) {
	configMapErrors.WithLabelValues(name, key).Inc()
}

func SetHealthStatus(status string) {
	healthStatus.WithLabelValues("healthy").Set(0)
	healthStatus.WithLabelValues("unhealthy").Set(0)
	healthStatus.WithLabelValues(status).Set(1)
}

// RecordWorkerReplace records the result of one worker replacement action (replaced / delete_error).
func RecordWorkerReplace(node, result string) {
	workerReplaceTotal.WithLabelValues(node, result).Inc()
}

// SetWorkerReplaceBlocked marks whether a node's stale worker cannot be replaced because the node is still busy.
func SetWorkerReplaceBlocked(node string, blocked bool) {
	v := 0.0
	if blocked {
		v = 1.0
	}
	workerReplaceBlocked.WithLabelValues(node).Set(v)
}
