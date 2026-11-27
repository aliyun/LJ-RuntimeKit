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
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	MigrationWorkerSubsystem = "migration_worker"
	NodeKey                  = "nodeName"
	GrpcInterfaceKey         = "grpc_interface_name"
	ContainerKey             = "containerID"
	FileCountKey             = "file_count"
	FileSizeKey              = "file_size"
	StorageKey               = "storage_type"
	StatusKey                = "status"
	TaskTypeKey              = "task_type"
	BinaryKey                = "use_host_binary"
)

var (
	MigrationWorkerGrpcLatency = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: MigrationWorkerSubsystem,
		Name:      "grpc_latency",
		Help:      "latency of grpc interface for migration worker",
	}, []string{NodeKey, GrpcInterfaceKey, BinaryKey})

	MigrationWorkerGrpcStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: MigrationWorkerSubsystem,
		Name:      "grpc_status",
		Help:      "status of grpc interface for migration worker",
	}, []string{NodeKey, GrpcInterfaceKey, BinaryKey})

	MigrationWorkerCheckpointLatency = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: MigrationWorkerSubsystem,
		Name:      "checkpoint_latency",
		Help:      "latency of checkpoint interface for migration worker",
	}, []string{NodeKey, ContainerKey, FileSizeKey, StatusKey, BinaryKey})

	MigrationWorkerRestoreLatency = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: MigrationWorkerSubsystem,
		Name:      "untar_latency",
		Help:      "latency of untar interface for migration worker",
	}, []string{NodeKey, FileCountKey, FileSizeKey, StatusKey, StorageKey, BinaryKey})

	MigrationWorkerTaskLoad = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: MigrationWorkerSubsystem,
		Name:      "task_load",
		Help:      "node task load of migration worker",
	}, []string{NodeKey, TaskTypeKey, BinaryKey})

	CommonCollectors = []prometheus.Collector{
		MigrationWorkerGrpcLatency,
		MigrationWorkerGrpcStatus,
		MigrationWorkerCheckpointLatency,
		MigrationWorkerRestoreLatency,
		MigrationWorkerTaskLoad,
	}
)

func RecordMigrationWorkerGrpcLatency(nodeName, interfaceName string, latency float64) {
	labels := map[string]string{}
	labels[NodeKey] = nodeName
	labels[GrpcInterfaceKey] = interfaceName
	labels[BinaryKey] = UseHostBinary
	MigrationWorkerGrpcLatency.With(labels).Set(latency)
}

func RecordMigrationWorkerGrpcStatus(nodeName, interfaceName string, status float64) {
	labels := map[string]string{}
	labels[NodeKey] = nodeName
	labels[GrpcInterfaceKey] = interfaceName
	labels[BinaryKey] = UseHostBinary
	MigrationWorkerGrpcStatus.With(labels).Set(status)
}

func RecordMigrationWorkerCheckpointLatency(nodeName, containerID, fileSize, status string, latency float64) {
	containerId := strings.TrimPrefix(containerID, "pouch://")
	labels := map[string]string{}
	labels[NodeKey] = nodeName
	labels[ContainerKey] = containerId
	labels[FileSizeKey] = fileSize
	labels[StatusKey] = status
	labels[BinaryKey] = UseHostBinary
	MigrationWorkerCheckpointLatency.With(labels).Set(latency)
}

func RecordMigrationWorkerRestoreLatency(nodeName, storage, status string, fileCount, fileSize int64, latency float64) {
	labels := map[string]string{}
	var fileSizeStr string

	fileSizeGB := float64(fileSize) / 1024 / 1024 / 1024
	switch {
	case fileSizeGB <= 10:
		fileSizeStr = "(0GB,10GB]"
	case fileSizeGB <= 32:
		fileSizeStr = "(10GB,32GB]"
	case fileSizeGB <= 64:
		fileSizeStr = "(32GB,64GB]"
	case fileSizeGB <= 128:
		fileSizeStr = "(64GB,128GB]"
	default:
		fileSizeStr = "(128GB,∞)"
	}
	labels[NodeKey] = nodeName
	labels[FileCountKey] = fmt.Sprintf("%d", fileCount)
	labels[FileSizeKey] = fileSizeStr
	labels[StorageKey] = storage
	labels[StatusKey] = status
	labels[BinaryKey] = UseHostBinary
	MigrationWorkerRestoreLatency.With(labels).Set(latency)
}

func RecordMigrationWorkerTaskLoad(nodeName, taskType string, load float64) {
	labels := map[string]string{}
	labels[NodeKey] = nodeName
	labels[TaskTypeKey] = taskType
	labels[BinaryKey] = UseHostBinary
	MigrationWorkerTaskLoad.With(labels).Set(load)
}

func init() {
	prometheus.MustRegister(CommonCollectors...)
}
