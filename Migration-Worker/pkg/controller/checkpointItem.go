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

package controller

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/container-migration/worker/cmd/migration-worker/options"
	migrationworker "github.com/container-migration/worker/pkg/apis"
	migruntime "github.com/container-migration/worker/pkg/runtime"
	"github.com/container-migration/worker/pkg/storage"
	mgutil "github.com/container-migration/worker/pkg/util"

	"google.golang.org/protobuf/proto"
	"k8s.io/klog/v2"
)

type CheckpointItem struct {
	RequestID string
	State     WorkState
	Ctx       context.Context
	Cancel    context.CancelFunc
	Done      chan struct{}

	CheckpointRequest *migrationworker.CheckpointToStorageRequest
	CheckpointStatus  *migrationworker.QueryCheckpointToStorageStatusResponse

	opts    *options.Options
	saveDir string

	storage storage.StorageInterface
	runtime migruntime.ContainerRuntime

	statusMux sync.Mutex
}

func NewCheckpointItem(controllerScheduler *Scheduler, request interface{}) (*CheckpointItem, error) {
	checkpointRequest, ok := request.(*migrationworker.CheckpointToStorageRequest)
	if !ok {
		return nil, fmt.Errorf("invalid item type: %v", request)
	}
	item := &CheckpointItem{}
	item.opts = controllerScheduler.Opts
	item.runtime = controllerScheduler.Runtime
	item.saveDir = controllerScheduler.SaveDir
	item.Ctx, item.Cancel = context.WithCancel(context.Background())
	item.Done = make(chan struct{})

	item.RequestID = checkpointRequest.RequestID
	item.State = WaitingState
	item.CheckpointRequest = checkpointRequest
	item.CheckpointStatus = &migrationworker.QueryCheckpointToStorageStatusResponse{}
	item.CheckpointStatus.RequestID = checkpointRequest.RequestID
	item.CheckpointStatus.Status = StatusPending
	item.CheckpointStatus.Message = "Waiting for checkpointing"
	item.CheckpointStatus.Data = make(map[string]*migrationworker.CheckpointAttributes)

	var err error
	item.storage, err = storage.NewStorageInterface(item.opts, checkpointRequest.StorageType, checkpointRequest.Storage)
	if err != nil {
		return nil, fmt.Errorf("failed to new storage manager, error: %v", err)
	}
	return item, nil
}

// SnapshotStatus returns a deep copy of the current status.
//
// CheckpointStatus is a protobuf message holding maps (Data and the nested
// Extras) that the per-second upload sampler keeps writing to for the whole
// duration of the checkpoint. Any reader that touches it without this lock —
// klog's %+v, or the gRPC layer marshalling the returned pointer after the
// handler has returned — races with those writes, and a concurrent map
// iteration is a *fatal* runtime error that no recover can catch. Callers
// outside this type must always go through here.
func (item *CheckpointItem) SnapshotStatus() *migrationworker.QueryCheckpointToStorageStatusResponse {
	item.statusMux.Lock()
	defer item.statusMux.Unlock()
	return proto.Clone(item.CheckpointStatus).(*migrationworker.QueryCheckpointToStorageStatusResponse)
}

func (item *CheckpointItem) UpdateExtrasOfStatus(containerName string, extras map[string]string, err error) {
	item.statusMux.Lock()
	defer item.statusMux.Unlock()
	item.updateExtrasLocked(containerName, extras, err)
}

// updateExtrasLocked requires item.statusMux to be held.
func (item *CheckpointItem) updateExtrasLocked(containerName string, extras map[string]string, err error) {
	if err != nil {
		item.CheckpointStatus.Message = fmt.Sprintf("failed to query upload info: %v;%s", err, item.CheckpointStatus.Message)
		return
	}
	if len(extras) == 0 {
		return
	}
	if item.CheckpointStatus.Data[containerName] == nil {
		item.CheckpointStatus.Data[containerName] = &migrationworker.CheckpointAttributes{}
	}
	if item.CheckpointStatus.Data[containerName].Extras == nil {
		item.CheckpointStatus.Data[containerName].Extras = make(map[string]string)
	}
	for k, v := range extras {
		item.CheckpointStatus.Data[containerName].Extras[k] = v
	}
}

func (item *CheckpointItem) UpdateStatus(data map[string]*migrationworker.CheckpointAttributes, errs []error) {
	defer close(item.Done)
	item.statusMux.Lock()
	defer item.statusMux.Unlock()
	if len(errs) != 0 {
		item.State = FailedState
		item.CheckpointStatus.Status = StatusInternalError
		item.CheckpointStatus.Message = fmt.Sprintf("%v", errs)
	} else {
		item.State = SucceedState
		item.CheckpointStatus.Status = StatusOK
		item.CheckpointStatus.Message = ""
	}
	if len(data) == 0 {
		return
	}
	dataBak := item.CheckpointStatus.Data
	item.CheckpointStatus.Data = data
	for name, checkpointAttributes := range dataBak {
		item.updateExtrasLocked(name, extrasMissingFrom(data[name], checkpointAttributes.Extras), nil)
	}
}

func (item *CheckpointItem) LaunchCheckpointItem() {
	checkpointRequest := item.CheckpointRequest
	requestStartedAt := time.Now()
	var errs []error
	var wg sync.WaitGroup
	var mu sync.Mutex
	var checkpointAttributes = make(map[string]*migrationworker.CheckpointAttributes)
	defer func() {
		item.UpdateStatus(checkpointAttributes, errs)
		klog.V(2).Infof("[checkpoint][request=%s] request completed, containers=%d, succeeded=%d, failed=%d, took=%s",
			item.RequestID, len(checkpointRequest.Policy), len(checkpointAttributes), len(errs),
			time.Since(requestStartedAt).Truncate(time.Millisecond))
	}()

	if len(checkpointRequest.Policy) == 0 {
		errs = append(errs, fmt.Errorf("checkpoint policy is nil"))
		klog.Errorf("[checkpoint][request=%s] request rejected: checkpoint policy is empty", checkpointRequest.RequestID)
		return
	}
	for _, containerCheckpointPolicy := range checkpointRequest.Policy {
		wg.Add(1)
		go func(checkpointPolicy *migrationworker.CheckpointPolicy) {
			defer wg.Done()
			attribute, err := item.CheckpointContainerToStorage(checkpointPolicy)
			if err != nil {
				klog.Errorf("[checkpoint][request=%s][container=%s] container checkpoint failed: %v",
					item.RequestID, checkpointPolicy.ContainerName, err)
				mu.Lock()
				errs = append(errs, fmt.Errorf("failed to checkpoint %s : %v", checkpointPolicy.ContainerName, err))
				mu.Unlock()
				return
			}
			mu.Lock()
			checkpointAttributes[checkpointPolicy.ContainerName] = attribute
			mu.Unlock()
			mgutil.RecordMigrationWorkerCheckpointLatency(migruntime.NodeName, checkpointPolicy.CheckpointID, fmt.Sprintf("%d", attribute.Size), "succeed", float64(attribute.TotalDuration))
		}(containerCheckpointPolicy)
	}
	wg.Wait()
}

// CheckpointContainerToStorage delegates the checkpoint to the node's XDM and
// uploads the resulting tar parts to storage.
//
// XDM splits the checkpoint into N independent tars, each one a complete archive
// carrying the full directory skeleton. They land under a single directory as
// part-00.tar, part-01.tar, ... so that the destination node can download and
// extract them in parallel during restore, where download is the dominant cost.
// The returned Path is that directory, and FileCount is the number of parts.
func (item *CheckpointItem) CheckpointContainerToStorage(checkpointPolicy *migrationworker.CheckpointPolicy) (*migrationworker.CheckpointAttributes, error) {
	start := time.Now()
	checkpointDir := filepath.Join(item.CheckpointRequest.PodUID, item.CheckpointRequest.Version,
		checkpointPolicy.ContainerName, storage.CheckpointDirName)
	containerID := trimContainerIDScheme(checkpointPolicy.ContainerID)

	opts := map[string]string{}
	if checkpointPolicy.CheckpointID != "" {
		opts["checkpoint_id"] = checkpointPolicy.CheckpointID
	}
	if len(checkpointPolicy.ExcludeFilepath) > 0 {
		opts["ignore_paths"] = strings.Join(checkpointPolicy.ExcludeFilepath, ",")
	}
	for k, v := range checkpointPolicy.AdvanceOptions {
		opts[k] = v
	}

	streamStartedAt := time.Now()
	partStream, err := item.runtime.CheckpointToStream(item.Ctx, containerID, opts)
	if err != nil {
		klog.Errorf("[checkpoint][request=%s][container=%s] stage=xdm-stream status=failed, took=%s: %v",
			item.RequestID, checkpointPolicy.ContainerName,
			time.Since(streamStartedAt).Truncate(time.Millisecond), err)
		return nil, err
	}
	streamSetupDuration := time.Since(streamStartedAt)
	klog.V(2).Infof("[checkpoint][request=%s][container=%s] stage=xdm-stream status=completed, took=%s",
		item.RequestID, checkpointPolicy.ContainerName, streamSetupDuration.Truncate(time.Millisecond))
	defer partStream.Close()

	tickerCtx, tickerCancel := context.WithCancel(context.Background())
	defer tickerCancel()
	go func() {
		// Owned by this goroutine alone, so the sampler needs no locking.
		sampler := newSpeedSampler(start)
		updateUploadInfo := func() {
			uploadInfo, err := item.storage.GetUploadInfo()
			if err != nil {
				klog.Warningf("[checkpoint][request=%s][container=%s] failed to query upload progress: %v",
					item.RequestID, checkpointPolicy.ContainerName, err)
				return
			}
			extras := make(map[string]string)
			extras["bytesTransferred"] = fmt.Sprintf("%d", uploadInfo.UploadedSize)
			extras[ExtraKeyAvgTransferSpeed] = formatSpeed(avgSpeedMBPerSec(uploadInfo.UploadedSize, time.Since(start)))
			extras[ExtraKeyInstantTransferSpeed] = formatSpeed(sampler.SampleMBPerSec(uploadInfo.UploadedSize))
			if uploadInfo.ChunkSize > 0 && uploadInfo.Parallelism > 0 {
				extras["ossUploadChunkSize"] = fmt.Sprintf("%d", uploadInfo.ChunkSize)
				extras["ossUploadParallelism"] = fmt.Sprintf("%d", uploadInfo.Parallelism)
			}
			if uploadInfo.ReadDuration > 0 {
				extras["ossUploadReadDuration"] = fmt.Sprintf("%.2f", uploadInfo.ReadDuration.Seconds())
			}
			item.UpdateExtrasOfStatus(checkpointPolicy.ContainerName, extras, nil)
		}
		updateUploadInfo()
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-tickerCtx.Done():
				return
			case <-ticker.C:
				updateUploadInfo()
			}
		}
	}()

	// XDM produces and sends the parts strictly one at a time, so uploading them
	// sequentially here matches the wire order and keeps a single upload in
	// flight. Checkpoint latency does not matter; restore latency is what the
	// split buys.
	var totalSize int64
	partCount := 0
	produceUploadStartedAt := time.Now()
	for {
		partStartedAt := time.Now()
		idx, partReader, err := partStream.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			klog.Errorf("[checkpoint][request=%s][container=%s] stage=produce-upload status=failed, partOrdinal=%d, took=%s: %v",
				item.RequestID, checkpointPolicy.ContainerName, partCount,
				time.Since(partStartedAt).Truncate(time.Millisecond), err)
			return nil, err
		}

		objectName := storage.PartObjectName(checkpointDir, idx)
		size, err := item.storage.Upload(partReader, objectName)
		if err != nil {
			klog.Errorf("[checkpoint][request=%s][container=%s] stage=produce-upload status=failed, part=%s, storage=%s, took=%s: %v",
				item.RequestID, checkpointPolicy.ContainerName, objectName, item.storage.Name(),
				time.Since(partStartedAt).Truncate(time.Millisecond), err)
			return nil, err
		}
		totalSize += size
		partCount++
		klog.V(4).Infof("[checkpoint][request=%s][container=%s] stage=produce-upload part=%s completed, bytes=%d, took=%s",
			item.RequestID, checkpointPolicy.ContainerName, objectName, size,
			time.Since(partStartedAt).Truncate(time.Millisecond))
	}

	produceUploadDuration := time.Since(produceUploadStartedAt)
	if partCount == 0 {
		err := fmt.Errorf("x-device-manager produced no checkpoint part for container %s", containerID)
		klog.Errorf("[checkpoint][request=%s][container=%s] stage=produce-upload status=failed, parts=0, took=%s: %v",
			item.RequestID, checkpointPolicy.ContainerName,
			produceUploadDuration.Truncate(time.Millisecond), err)
		return nil, err
	}
	klog.V(2).Infof("[checkpoint][request=%s][container=%s] stage=produce-upload status=completed, parts=%d, bytes=%d, took=%s",
		item.RequestID, checkpointPolicy.ContainerName, partCount, totalSize,
		produceUploadDuration.Truncate(time.Millisecond))

	totalDuration := time.Since(start)
	klog.V(2).Infof("[checkpoint][request=%s][container=%s] container completed, parts=%d, bytes=%d, total=%s, xdmStream=%s, produceUpload=%s",
		item.RequestID, checkpointPolicy.ContainerName, partCount, totalSize,
		totalDuration.Truncate(time.Millisecond), streamSetupDuration.Truncate(time.Millisecond),
		produceUploadDuration.Truncate(time.Millisecond))

	return &migrationworker.CheckpointAttributes{
		Size: totalSize,
		// FileCount carries the number of tar parts here (it used to be a constant
		// 1 for the single-tar layout). Migration-Controller reads it back as the
		// authoritative expected part count and cross-checks it against what it
		// actually probes on the storage before triggering a restore — that check is
		// what catches a lost *trailing* part, which contiguity checks cannot see.
		//
		// Note the field means something else on the restore side, where it is the
		// number of extracted files.
		FileCount:     int64(partCount),
		Path:          checkpointDir,
		TotalDuration: int64(totalDuration.Seconds()),
		Extras: map[string]string{
			"bytesTransferred": fmt.Sprintf("%d", totalSize),
			"checkpointParts":  fmt.Sprintf("%d", partCount),
			// Recomputed from the final size and duration: more accurate than the
			// last ticker sample, which lags by up to one second.
			ExtraKeyAvgTransferSpeed: formatSpeed(avgSpeedMBPerSec(totalSize, totalDuration)),
		},
	}, nil
}

// trimContainerIDScheme strips the runtime prefix kubelet puts on container ids
// in the Pod status (e.g. "containerd://<id>").
func trimContainerIDScheme(containerID string) string {
	if idx := strings.Index(containerID, "://"); idx >= 0 {
		return containerID[idx+3:]
	}
	return containerID
}

func (item *CheckpointItem) CleanTask(cleanData bool) error {
	switch item.State {
	case RunningState:
		klog.V(4).Infof("Checkpoint task %s is RunningState, will try to cancel ...", item.RequestID)
		item.Cancel()
		<-item.Ctx.Done()
		<-item.Done
		klog.V(4).Infof("Finished to cancel Checkpoint task %s, will clean tmp data...", item.RequestID)
		return item.CleanIncompleteData()
	case FailedState:
		klog.V(4).Infof("Checkpoint task %s is FailedState, will clean tmp data...", item.RequestID)
		return item.CleanIncompleteData()
	case SucceedState:
		klog.V(4).Infof("Checkpoint task %s is SucceedState", item.RequestID)
		if cleanData {
			klog.V(4).Info("CleanData is true, will clean complete data ...")
			return item.CleanCompleteRestoreData()
		}
		klog.V(4).Info("CleanData is false, nothing to clean")
	default:
		klog.V(4).Infof("Task %s is WaitingState, nothing to clean", item.RequestID)
	}
	return nil
}

func (item *CheckpointItem) CleanIncompleteData() error {
	klog.V(4).Infof("Will clean incomplete data for checkpoint request: %s", item.CheckpointRequest.GetRequestID())
	cleanPath := filepath.Join(item.CheckpointRequest.PodUID, item.CheckpointRequest.Version)
	if err := item.storage.CleanUpload(cleanPath); err != nil {
		klog.Errorf("Failed to clean incomplete data for checkpoint request: %s, error: %v", item.CheckpointRequest.GetRequestID(), err)
		return err
	}
	klog.V(4).Infof("Succeed to clean incomplete data for checkpoint request: %s", item.CheckpointRequest.GetRequestID())
	return nil
}

// CleanCompleteRestoreData is a no-op for checkpoint tasks: nothing is unpacked
// on the source node, only uploaded.
func (item *CheckpointItem) CleanCompleteRestoreData() error {
	return nil
}
