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

package util

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestRecordMigrationWorkerGrpcLatency(t *testing.T) {
	reg := prometheus.NewRegistry()

	if err := reg.Register(MigrationWorkerGrpcLatency); err != nil {
		t.Fatalf("Failed to register MigrationWorkerGrpcLatency: %v", err)
	}

	nodeName := "node1"
	interfaceName := "interface1"
	latency := 0.123

	RecordMigrationWorkerGrpcLatency(nodeName, interfaceName, latency)

	expected := `# HELP migration_worker_grpc_latency latency of grpc interface for migration worker
# TYPE migration_worker_grpc_latency gauge
migration_worker_grpc_latency{grpc_interface_name="interface1",nodeName="node1",use_host_binary=""} 0.123
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(expected)); err != nil {
		t.Fatal(err)
	}
}

func TestRecordMigrationWorkerGrpcStatus(t *testing.T) {
	reg := prometheus.NewRegistry()

	if err := reg.Register(MigrationWorkerGrpcStatus); err != nil {
		t.Fatalf("Failed to register MigrationWorkerGrpcStatus: %v", err)
	}

	nodeName := "node2"
	interfaceName := "interface2"
	status := 1.0

	RecordMigrationWorkerGrpcStatus(nodeName, interfaceName, status)

	expected := `# HELP migration_worker_grpc_status status of grpc interface for migration worker
# TYPE migration_worker_grpc_status gauge
migration_worker_grpc_status{grpc_interface_name="interface2",nodeName="node2",use_host_binary=""} 1
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(expected)); err != nil {
		t.Fatal(err)
	}
}

func TestRecordMigrationWorkerCheckpointLatency(t *testing.T) {
	reg := prometheus.NewRegistry()

	if err := reg.Register(MigrationWorkerCheckpointLatency); err != nil {
		t.Fatalf("Failed to register MigrationWorkerCheckpointLatency: %v", err)
	}

	nodeName := "node6"
	containerID := "pouch://container6"
	fileSize := "6"
	status := "0"
	latency := 0.666

	RecordMigrationWorkerCheckpointLatency(nodeName, containerID, fileSize, status, latency)

	expected := `# HELP migration_worker_checkpoint_latency latency of checkpoint interface for migration worker
# TYPE migration_worker_checkpoint_latency gauge
migration_worker_checkpoint_latency{containerID="container6", file_size="6", nodeName="node6", status="0",use_host_binary=""} 0.666
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(expected)); err != nil {
		t.Fatal(err)
	}
}

func TestRecordMigrationWorkerRestoreLatency(t *testing.T) {
	reg := prometheus.NewRegistry()

	if err := reg.Register(MigrationWorkerRestoreLatency); err != nil {
		t.Fatalf("Failed to register MigrationWorkerRestoreLatency: %v", err)
	}

	nodeName := "node7"
	storage := "oss"
	status := "0"
	fileCount := int64(7)
	fileSize := int64(7)
	latency := 0.777

	RecordMigrationWorkerRestoreLatency(nodeName, storage, status, fileCount, fileSize, latency)

	expected := `# HELP migration_worker_untar_latency latency of untar interface for migration worker
# TYPE migration_worker_untar_latency gauge
migration_worker_untar_latency{file_count="7",file_size="(0GB,10GB]",nodeName="node7",status="0",storage_type="oss",use_host_binary=""} 0.777
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(expected)); err != nil {
		t.Fatal(err)
	}
}

func TestRecordRecordMigrationWorkerTaskLoad(t *testing.T) {
	reg := prometheus.NewRegistry()

	if err := reg.Register(MigrationWorkerTaskLoad); err != nil {
		t.Fatalf("Failed to register MigrationWorkerTaskLoad: %v", err)
	}

	nodeName := "node8"
	taskType := "type8"
	load := 8.0

	RecordMigrationWorkerTaskLoad(nodeName, taskType, load)

	expected := `# HELP migration_worker_task_load node task load of migration worker
# TYPE migration_worker_task_load gauge
migration_worker_task_load{nodeName="node8",task_type="type8",use_host_binary=""} 8
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(expected)); err != nil {
		t.Fatal(err)
	}
}
