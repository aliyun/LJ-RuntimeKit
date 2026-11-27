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
	"os"
	"path/filepath"
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

// restoreAttemptedFileName is written by xpu-container-runtime into the
// per-container restore directory when it turns `runc create` into
// `runc restore`. It MUST stay in sync with the constant of the same name in
// AMP-XPU-Toolkit-Collection/cmd/xpu-container-runtime/xdm_restore.go.
const restoreAttemptedFileName = ".restore-attempted"

// ExtraKeyRestoreAttempted is the key this worker reports the evidence under in
// QueryRestoreFromStorageStatus. The controller reads the same key.
const ExtraKeyRestoreAttempted = "restoreAttempted"

type RestoreItem struct {
	RequestID string
	State     WorkState
	Ctx       context.Context
	Cancel    context.CancelFunc
	Done      chan struct{}

	RestoreRequest *migrationworker.RestoreFromStorageRequest
	RestoreStatus  *migrationworker.QueryRestoreFromStorageStatusResponse

	opts    *options.Options
	saveDir string

	storage storage.StorageInterface

	statusMux sync.Mutex
}

func NewRestoreItem(controllerScheduler *Scheduler, request interface{}) (*RestoreItem, error) {
	restoreRequest, ok := request.(*migrationworker.RestoreFromStorageRequest)
	if !ok {
		return nil, fmt.Errorf("invalid item type: %v", request)
	}
	item := &RestoreItem{}
	item.opts = controllerScheduler.Opts
	item.saveDir = controllerScheduler.SaveDir
	item.Ctx, item.Cancel = context.WithCancel(context.Background())
	item.Done = make(chan struct{})

	item.RequestID = restoreRequest.RequestID
	item.State = WaitingState
	item.RestoreRequest = restoreRequest
	item.RestoreStatus = &migrationworker.QueryRestoreFromStorageStatusResponse{}
	item.RestoreStatus.RequestID = restoreRequest.RequestID
	item.RestoreStatus.Status = StatusPending
	item.RestoreStatus.Message = "Waiting for restoring"
	item.RestoreStatus.Data = make(map[string]*migrationworker.CheckpointAttributes)

	var err error
	item.storage, err = storage.NewStorageInterface(controllerScheduler.Opts, restoreRequest.StorageType, restoreRequest.Storage)
	if err != nil {
		return nil, fmt.Errorf("failed to new storage manager, error: %v", err)
	}
	return item, nil
}

// SnapshotStatus returns a deep copy of the current status.
//
// RestoreStatus is a protobuf message holding maps (Data and the nested Extras)
// that the per-second download sampler and reportRestoreEvidence keep writing
// to. Any reader that touches it without this lock — klog's %+v, or the gRPC
// layer marshalling the returned pointer after the handler has returned —
// races with those writes, and a concurrent map iteration is a *fatal* runtime
// error that no recover can catch. Callers outside this type must always go
// through here.
func (item *RestoreItem) SnapshotStatus() *migrationworker.QueryRestoreFromStorageStatusResponse {
	item.statusMux.Lock()
	defer item.statusMux.Unlock()
	return proto.Clone(item.RestoreStatus).(*migrationworker.QueryRestoreFromStorageStatusResponse)
}

func (item *RestoreItem) UpdateExtrasOfStatus(containerName string, extras map[string]string, err error) {
	item.statusMux.Lock()
	defer item.statusMux.Unlock()
	item.updateExtrasLocked(containerName, extras, err)
}

// updateExtrasLocked requires item.statusMux to be held.
func (item *RestoreItem) updateExtrasLocked(containerName string, extras map[string]string, err error) {
	if err != nil {
		item.RestoreStatus.Message = fmt.Sprintf("failed to query download info: %v;%s", err, item.RestoreStatus.Message)
		return
	}
	if len(extras) == 0 {
		return
	}
	if item.RestoreStatus.Data[containerName] == nil {
		item.RestoreStatus.Data[containerName] = &migrationworker.CheckpointAttributes{}
	}
	if item.RestoreStatus.Data[containerName].Extras == nil {
		item.RestoreStatus.Data[containerName].Extras = make(map[string]string)
	}
	for k, v := range extras {
		item.RestoreStatus.Data[containerName].Extras[k] = v
	}
}

func (item *RestoreItem) UpdateStatus(data map[string]*migrationworker.CheckpointAttributes, errs []error) {
	defer close(item.Done)
	item.statusMux.Lock()
	defer item.statusMux.Unlock()
	if len(errs) != 0 {
		item.State = FailedState
		item.RestoreStatus.Status = StatusInternalError
		item.RestoreStatus.Message = fmt.Sprintf("%v", errs)
	} else {
		item.State = SucceedState
		item.RestoreStatus.Status = StatusOK
		item.RestoreStatus.Message = ""
	}
	if len(data) == 0 {
		return
	}
	dataBak := item.RestoreStatus.Data
	item.RestoreStatus.Data = data
	for name, checkpointAttributes := range dataBak {
		item.updateExtrasLocked(name, extrasMissingFrom(data[name], checkpointAttributes.Extras), nil)
	}
}

func (item *RestoreItem) LaunchRestoreItem() {
	restoreRequest := item.RestoreRequest
	var errs []error
	var wg sync.WaitGroup
	var mu sync.Mutex
	var checkpointAttributes = make(map[string]*migrationworker.CheckpointAttributes)
	defer func() {
		item.UpdateStatus(checkpointAttributes, errs)
		klog.V(2).Infof("[restore][request=%s] request completed, containers=%d, succeeded=%d, failed=%d",
			item.RequestID, len(restoreRequest.ContainerCheckpointPath), len(checkpointAttributes), len(errs))
	}()
	for name, path := range restoreRequest.ContainerCheckpointPath {
		wg.Add(1)
		go func(containerName, checkpointPath string) {
			defer wg.Done()
			attribute, err := item.RestoreContainerFromStorage(containerName, checkpointPath)
			if err != nil {
				klog.Errorf("[restore][request=%s][container=%s] container restore failed, checkpoint=%s: %v",
					item.RequestID, containerName, checkpointPath, err)
				mu.Lock()
				errs = append(errs, fmt.Errorf("failed to restore %s : %v", containerName, err))
				mu.Unlock()
				return
			}
			mu.Lock()
			checkpointAttributes[containerName] = attribute
			mu.Unlock()
			mgutil.RecordMigrationWorkerRestoreLatency(migruntime.NodeName, item.storage.Name(), "succeed", attribute.FileCount, attribute.Size, float64(attribute.TotalDuration))
			klog.V(2).Infof("[restore][request=%s][container=%s] container completed, checkpoint=%s, files=%d, bytes=%d, path=%s",
				item.RequestID, containerName, checkpointPath, attribute.FileCount, attribute.Size, attribute.Path)
		}(name, path)
	}
	wg.Wait()
}

// containerDistCheckpointDir returns the directory this task extracts the
// container's checkpoint payload into.
//
//   - flat layout (DataDir set): the controller already resolved the absolute
//     target (<hostPath>/data) and the runtime consumes rootfs/memory/xpu
//     directly under it, so it is returned as-is.
//   - default layout: <homeDir>/<podUID>/<version>/<container>/checkpoint,
//     where homeDir falls back to saveDir when the request leaves it empty.
func (item *RestoreItem) containerDistCheckpointDir(containerName string) string {
	if item.RestoreRequest.DataDir != "" {
		return item.RestoreRequest.DataDir
	}
	return filepath.Join(item.restoreHomeDir(), item.RestoreRequest.PodUID,
		item.RestoreRequest.Version, containerName, "checkpoint")
}

func (item *RestoreItem) RestoreContainerFromStorage(containerName, storagePath string) (*migrationworker.CheckpointAttributes, error) {
	start := time.Now()
	distCheckpointDir := item.containerDistCheckpointDir(containerName)

	if item.RestoreRequest.DataDir != "" {
		// flat layout: data lands directly under the source pod's migration
		// hostPath. Its parent must exist / be creatable.
		if err := os.MkdirAll(filepath.Dir(distCheckpointDir), 0755); err != nil {
			klog.Errorf("[restore][request=%s][container=%s] failed to create data directory parent %s: %v",
				item.RequestID, containerName, filepath.Dir(distCheckpointDir), err)
			return nil, err
		}
	} else {
		// default layout: verify the homeDir parent exists (it should be
		// covered by the migration hostPath mount).
		homeDir := item.restoreHomeDir()
		if exist := mgutil.CheckPathExists(filepath.Dir(homeDir)); !exist {
			klog.Errorf("[restore][request=%s][container=%s] home directory parent %s does not exist",
				item.RequestID, containerName, filepath.Dir(homeDir))
			return nil, fmt.Errorf("home dir parent: %s does not exist", filepath.Dir(homeDir))
		}
	}
	tickerCtx, tickerCancel := context.WithCancel(context.Background())
	defer tickerCancel()
	go func() {
		// Owned by this goroutine alone, so the sampler needs no locking.
		sampler := newSpeedSampler(start)
		updateDownloadInfo := func() {
			downloadInfo, err := item.storage.GetDownloadInfo()
			if err != nil {
				klog.Warningf("[restore][request=%s][container=%s] failed to query download progress: %v",
					item.RequestID, containerName, err)
			} else {
				extras := make(map[string]string)
				extras["bytesTransferred"] = fmt.Sprintf("%d", downloadInfo.FileSize)
				extras[ExtraKeyAvgTransferSpeed] = formatSpeed(avgSpeedMBPerSec(downloadInfo.FileSize, time.Since(start)))
				extras[ExtraKeyInstantTransferSpeed] = formatSpeed(sampler.SampleMBPerSec(downloadInfo.FileSize))
				item.UpdateExtrasOfStatus(containerName, extras, err)
			}
		}
		updateDownloadInfo()
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-tickerCtx.Done():
				return
			case <-ticker.C:
				updateDownloadInfo()
			}
		}
	}()

	restoreInfo, err := item.storage.Restore(item.Ctx, storagePath, distCheckpointDir)
	if err != nil {
		klog.Errorf("[restore][request=%s][container=%s] restore from %s to %s failed: %v",
			item.RequestID, containerName, storagePath, distCheckpointDir, err)
		return nil, err
	}
	totalDuration := time.Since(start)
	return &migrationworker.CheckpointAttributes{
		Size:          restoreInfo.FileSize,
		FileCount:     restoreInfo.FileCount,
		Path:          distCheckpointDir,
		TotalDuration: int64(totalDuration.Seconds()),
		Extras: map[string]string{
			"bytesTransferred": fmt.Sprintf("%d", restoreInfo.FileSize),
			// Recomputed from the final size and duration: more accurate than the
			// last ticker sample, which lags by up to one second.
			ExtraKeyAvgTransferSpeed: formatSpeed(avgSpeedMBPerSec(restoreInfo.FileSize, totalDuration)),
		},
	}, nil
}

// restoreHomeDir returns the root this task extracts into.
func (item *RestoreItem) restoreHomeDir() string {
	if len(item.RestoreRequest.HomeDir) == 0 {
		return item.saveDir
	}
	return item.RestoreRequest.HomeDir
}

// RestoreAttempted reports whether xpu-container-runtime recorded that it took
// over the creation of containerName on this node. The evidence file is at
// filepath.Dir(ckptDir) — consistent with xpu-container-runtime's write path.
func (item *RestoreItem) RestoreAttempted(containerName string) bool {
	path := filepath.Join(filepath.Dir(item.containerDistCheckpointDir(containerName)), restoreAttemptedFileName)
	_, err := os.Stat(path)
	return err == nil
}

func (item *RestoreItem) CleanTask(cleanData bool) error {
	switch item.State {
	case RunningState:
		klog.V(4).Infof("Restore task %s is RunningState, will try to cancel ...", item.RequestID)
		item.Cancel()
		<-item.Ctx.Done()
		<-item.Done
		klog.V(4).Infof("Finished to cancel Restore task %s, will clean tmp data...", item.RequestID)
		return item.CleanIncompleteData()
	case FailedState:
		klog.V(4).Infof("Restore task %s is FailedState, will clean tmp data...", item.RequestID)
		return item.CleanIncompleteData()
	case SucceedState:
		klog.V(4).Infof("Restore task %s is SucceedState", item.RequestID)
		if cleanData {
			klog.V(4).Info("CleanData is true, will clean complete restore data ...")
			return item.CleanCompleteRestoreData()
		}
		klog.V(4).Info("CleanData is false, nothing to clean")
	default:
		klog.V(4).Infof("Task %s is WaitingState, nothing to clean", item.RequestID)
	}
	return nil
}

func (item *RestoreItem) CleanIncompleteData() error {
	klog.V(4).Infof("Will clean incomplete data for restore request: %s", item.RestoreRequest.GetRequestID())
	homeDir := item.restoreHomeDir()
	for containerName := range item.RestoreRequest.ContainerCheckpointPath {
		distCheckpointDir := item.containerDistCheckpointDir(containerName)
		if err := deleteRestoreDirUnder(homeDir, distCheckpointDir+"-tmp"); err != nil {
			klog.Errorf("Failed to clean incomplete data for restore request: %s, error: %v", item.RestoreRequest.GetRequestID(), err)
			return err
		}
		if err := deleteRestoreDirUnder(homeDir, distCheckpointDir); err != nil {
			klog.Errorf("Failed to clean partial checkpoint data for restore request: %s, error: %v", item.RestoreRequest.GetRequestID(), err)
			return err
		}
	}
	klog.V(4).Infof("Succeed to clean incomplete data for restore request: %s", item.RestoreRequest.GetRequestID())
	return nil
}

// CleanCompleteRestoreData removes the transient checkpoint payload that is
// safe to delete right after a restore completes: memory/ (consumed by
// `runc restore` before it returns) and xpu/ (consumed by the async XPU
// task that follows). It intentionally leaves rootfs/ in place.
//
// Since the X-Device-Manager restore switched from `cp -a upperdir` to
// `mount overlay lowerdir=<checkpointDir>/rootfs:...`, the checkpoint rootfs
// is a live overlay layer of the restored container(s) on this node. Deleting
// it here would tear the container's rootfs out from under the process; the
// rootfs must survive until every restored pod that references it is gone,
// which is handled by a separate node-side reaper (see pkg/storage of the
// controller repo / the design note in the project plan).
func (item *RestoreItem) CleanCompleteRestoreData() error {
	for containerName := range item.RestoreRequest.ContainerCheckpointPath {
		base := item.containerDistCheckpointDir(containerName)
		if err := deleteCheckpointNonRootfsData(base); err != nil {
			klog.Errorf("Failed to clean memory/xpu for restore request: %s, error: %v", item.RestoreRequest.GetRequestID(), err)
			return err
		}
	}
	klog.V(4).Infof("Cleaned memory/xpu (rootfs preserved for overlay lowerdir) for restore request: %s", item.RestoreRequest.GetRequestID())
	return nil
}
