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

package gpu

import (
	"context"
	"time"

	"k8s.io/klog/v2"
)

// topoSyncRetryInterval is how long the topology sync waits before re-applying
// a target whose node patch failed, so the node eventually converges on the
// latest snapshot without any manual restart.
const topoSyncRetryInterval = 5 * time.Second

// TopoSync drives the node from a one shot topology report towards the latest
// snapshot xdm pushed: it patches the node annotations / labels and switches
// the rdma resource on or off. Targets are applied one at a time by a single
// worker, so a slow patch can never write an older topology back after a newer
// withdrawal.
type TopoSync struct {
	dm *DeviceManager

	trigger chan struct{}

	// applied records the last successfully published target, so an unchanged
	// snapshot does not patch the node again. The node writes themselves are
	// serialised by dm.annotationMu, which is shared with the startup path and
	// the health check.
	applied     bool
	signature   string
	rdmaHealthy bool
}

func NewTopoSync(dm *DeviceManager) *TopoSync {
	return &TopoSync{
		dm:      dm,
		trigger: make(chan struct{}, 1),
	}
}

// Notify marks a new snapshot as pending. It never blocks: a pending signal
// already covers any number of snapshots because the target is always read
// fresh from the device cache.
func (ts *TopoSync) Notify() {
	select {
	case ts.trigger <- struct{}{}:
	default:
	}
}

// Run applies pending snapshots until ctx is cancelled.
func (ts *TopoSync) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-ts.trigger:
		}

		if err := ts.Sync(); err != nil {
			klog.Errorf("topology sync failed, retrying in %s: %v", topoSyncRetryInterval, err)

			select {
			case <-ctx.Done():
				return
			case <-time.After(topoSyncRetryInterval):
			}
			ts.Notify()
		}
	}
}

// Sync applies the current snapshot once. The startup path calls it directly so
// the first node report is complete before the plugins start serving.
func (ts *TopoSync) Sync() error {
	ts.dm.annotationMu.Lock()
	defer ts.dm.annotationMu.Unlock()

	devices, hasTopo, buildErr := ts.dm.buildNodeAnnotation()

	signature := ts.dm.nodeSignature(devices)
	if ts.applied && signature == ts.signature && hasTopo == ts.rdmaHealthy {
		klog.V(4).Infof("node topology unchanged, skip patching")
		return nil
	}

	// The node is about to change, so the recorded target must not count as
	// applied until the whole transition succeeded. Keeping it marked on failure
	// would let the signature check skip the retry: the withdrawal below flips the
	// rdma health before the patch, so a failed patch followed by the same target
	// signature (the recovered topology equals the one applied before) would
	// leave the resource withdrawn forever.
	ts.applied = false

	if !hasTopo {
		// Withdrawal: stop advertising the rdma resource first, so no new pod is
		// placed on a topology that is about to be removed, then clean the node
		// fields. The empty topology itself is what clears them.
		ts.dm.setRDMAHealth(false)

		if err := ts.dm.addNodeAnnotation(devices); err != nil {
			return err
		}
	} else {
		if err := ts.dm.addNodeAnnotation(devices); err != nil {
			return err
		}

		// Publication: the rdma plugin never ran when the node booted without a
		// topology, start it before offering the resource again.
		if err := ts.dm.ensureRDMAStarted(); err != nil {
			return err
		}
		ts.dm.setRDMAHealth(true)
	}

	ts.signature = signature
	ts.rdmaHealthy = hasTopo
	ts.applied = true
	klog.Infof("node topology synced, topoPublished=%v", hasTopo)

	// the node converged, the error only reports that the snapshot itself could
	// not be translated and was applied as a withdrawal
	return buildErr
}
