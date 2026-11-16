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
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/lj-runtimekit/device-plugin/internal/feature"
	xpuplugin "github.com/lj-runtimekit/device-plugin/internal/plugin"
	"github.com/lj-runtimekit/device-plugin/internal/utils"
	"github.com/lj-runtimekit/device-plugin/internal/watch"

	"github.com/NVIDIA/go-nvml/pkg/nvml"
	"github.com/fsnotify/fsnotify"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

var nodeName string

const (
	StatusOK            = 0
	StatusInternalError = 1005
)

type DeviceManager struct {
	k8sClient         kubernetes.Interface
	deviceMeta        *utils.DeviceMeta
	annotationDevices *GpuDevice
	stopChan          chan struct{}

	// annotationMu serialises every node annotation writer: the topology worker,
	// the startup path and the health check all rewrite the same node fields, and
	// an earlier build must never land after a later one
	annotationMu sync.Mutex

	// unhealthyDevices records the devices NVML reported as failed, keyed by
	// minor. It is kept aside from the xdm snapshot because that snapshot keeps
	// reporting the device as healthy; the annotation merges both views.
	unhealthyMu      sync.Mutex
	unhealthyDevices map[int]bool

	// pluginsMu guards plugins and rdmaPlugin: the topology sync may start the
	// rdma plugin while the restart loop rebuilds the whole set
	pluginsMu  sync.Mutex
	plugins    []xpuplugin.PluginInterface
	rdmaPlugin xpuplugin.PluginInterface
	// rebuilding is set while the restart loop tears the plugin set down or
	// builds it again: a topology sync crossing that window must not start the
	// rdma plugin, it would outlive the set that owns it
	rebuilding bool

	rdmaTopoAnnotation *RdmaTopoAnnotation
	topoLabels         *NodeTopoLabels
}

func NewDeviceManager(bp utils.MemoryUnit) *DeviceManager {
	utils.SetMetric(bp)

	nodeName = os.Getenv("NODE_NAME")
	if nodeName == "" {
		klog.Fatalln("Please set env NODE_NAME")
	}

	utils.SetNodeName(nodeName)

	restCfg, err := clientcmd.BuildConfigFromFlags("", "")
	if err != nil {
		klog.Fatalf("build k8s rest config failed due to %v", err)
	}
	k8sClient, err := kubernetes.NewForConfig(restCfg)

	return &DeviceManager{
		deviceMeta:        utils.NewDeviceMeta(),
		k8sClient:         k8sClient,
		annotationDevices: &GpuDevice{Devices: make(map[int]*Device)},
		stopChan:          make(chan struct{}),
		plugins:           make([]xpuplugin.PluginInterface, 0),
		unhealthyDevices:  make(map[int]bool),
	}
}

func (dm *DeviceManager) Run() error {
	// 在 GC 和插件访问账本之前完成迁移，失败时禁止使用空账本继续启动。
	if err := utils.MigrateDpCheckpoint(); err != nil {
		return fmt.Errorf("initialize dp checkpoint failed: %w", err)
	}

	klog.Infof("starting FS watcher")

	klog.Info("loading NVML")
	if ret := nvml.Init(); ret != nvml.SUCCESS {
		klog.Errorf("Failed to initialize NVML: %v.", ret)
		klog.Errorf("If this is a GPU node, did you set the docker default runtime to `nvidia`?")
		klog.Errorf("If this is not a GPU node, you should set up a toleration or nodeSelector to only deploy this plugin on GPU nodes")
		return errors.New("failed to initialize nvml")
	}

	defer func() { klog.Infof("Shutdown of NVML returned: %v", nvml.Shutdown()) }()

	watcher, err := watch.Files(pluginapi.DevicePluginPath)
	if err != nil {
		klog.Errorf("failed to create FS watcher")
		return err
	}
	defer func() {
		_ = watcher.Close()
	}()

	klog.Info("starting OS watcher")
	sigs := watch.Signals(syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	xmdClient, closer, err := utils.NewXdmGrpcClient()
	if err != nil {
		klog.Fatalf("Failed to create client: %v", err)
	}
	defer func() {
		_ = closer()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var topoSync *TopoSync
	if utils.DefaultFeatureGate.Enabled(feature.TopoWatch) {
		klog.Infof("topology watch enabled, keeping the xdm ListAndWatch stream open")
		topoSync = NewTopoSync(dm)

		// the startup report and the plugin set are both built from the first
		// snapshot, so wait for it before going any further
		firstSnapshot := make(chan struct{})
		var firstOnce sync.Once
		go WatchXdmDevices(ctx, xmdClient, dm.deviceMeta, func() {
			firstOnce.Do(func() { close(firstSnapshot) })
			// the worker is not running yet, the signal is buffered and merged
			// into its first round, see the constraint at the start site below
			topoSync.Notify()
		})

		// xdm may never answer, so the wait has to stay interruptible: a SIGTERM
		// sent during startup must end the process instead of being queued until
		// the plugins are up
	waitFirstSnapshot:
		for {
			select {
			case <-firstSnapshot:
				break waitFirstSnapshot
			case s := <-sigs:
				if exitOnStartupSignal(s) {
					return nil
				}
			}
		}
	} else {
		GrpcListAndWatch(ctx, xmdClient, dm.deviceMeta)
	}

	// The checkpoint GC drops records of deleted pods and releases their
	// allocations in xdm. Started once outside the plugin restart loop.
	if utils.DefaultFeatureGate.Enabled(feature.CheckpointGC) {
		klog.Infof("starting checkpoint GC")
		go utils.NewCheckpointGC(xmdClient).Start()
	} else {
		klog.Infof("checkpoint GC disabled by feature gate")
	}

	podUsedResource, err := dm.GetPodUsed(xmdClient)
	if err != nil {
		klog.Errorf("xdm GetPodUsed failed: %v", err)
	}

	if err := dm.patchPodsAnnotation(podUsedResource); err != nil {
		klog.Errorf("patchPodsAnnotation failed on node %s: %v", nodeName, err)
	}

	// with TopoWatch the first node report runs after the plugins are up, so the
	// rdma plugin it may start is the one the manager already knows about
	if topoSync == nil {
		if err := dm.patchNodeAnnotation(); err != nil {
			klog.Errorf("patchNodeAnnotation failed on node %s: %v", nodeName, err)
		}
	}

	deviceType, err := GetDeviceType(xmdClient)
	if err != nil {
		klog.Errorf("failed to get device type: %v", err)
	}

	var started bool
	var restartTimeout <-chan time.Time
	var topoWorkerStarted bool

restart:
	if started {
		// freeze the topology worker before the set is torn down, see rebuilding:
		// a sync running in this window would start an rdma plugin that nobody
		// stops. Released by endRebuild once the new set is in place.
		dm.beginRebuild()

		err := dm.stopAllPlugins()
		if err != nil {
			return fmt.Errorf("error stopping plugins from previous run: %v", err)
		}

		dm.Stop()
	}

	klog.Info("starting plugins")
	plugins, restartPlugins, err := startPlugins(deviceType, dm.deviceMeta, dm.k8sClient, HasPcieSwitchTopology(dm.deviceMeta))
	if err != nil {
		return fmt.Errorf("error starting plugins: %v", err)
	}
	started = true

	if restartPlugins {
		klog.Infof("Failed to start one or more plugins. Retrying in 30s...")
		restartTimeout = time.After(30 * time.Second)
	} else {
		dm.setPlugins(plugins)

		// the set is in place again: a sync reaching this point may start the
		// rdma plugin, it is now registered in dm.plugins and owned by this run
		dm.endRebuild()

		// publish the snapshot known at this point: it also withdraws or
		// restores the rdma resource according to the current topology
		if topoSync != nil {
			if err := topoSync.Sync(); err != nil {
				klog.Errorf("topology sync failed on node %s: %v", nodeName, err)
			}

			// The worker must never start before setPlugins above. A sync that
			// runs first sees no rdma plugin and calls ensureRDMAStarted, which
			// builds and registers one; startPlugins then builds a second
			// instance for the same socket and setPlugins replaces the field,
			// leaving the first one serving with its socket removed and no
			// reference left to stop it. Snapshots pushed before this point are
			// not lost: they stay buffered in the trigger channel and are merged
			// into the first round, which is a no-op because this sync already
			// published the same target.
			if !topoWorkerStarted {
				klog.Infof("starting topology sync worker")
				go topoSync.Run(ctx)
				topoWorkerStarted = true
			}
		}

		if deviceType == utils.DeviceTypeGPU {
			klog.Infof("Starting device health check for device type: %s", deviceType)
			// hand over this run's stop channel; Stop arms a fresh one for
			// the next run, so the field must not be read from the goroutine
			go dm.DeviceHealthCheck(dm.stopChan)
		}
	}

	// Start an infinite loop, waiting for several indicators to either log
	// some messages, trigger a restart of the plugins, or exit the program.
	for {
		select {
		// If the restart timeout has expired, then restart the plugins
		case <-restartTimeout:
			goto restart

		// Detect a kubelet restart by watching for a newly created
		// 'pluginapi.KubeletSocket' file. When this occurs, restart this loop,
		// restarting all of the plugins in the process.
		case event := <-watcher.Events:
			if event.Name == pluginapi.KubeletSocket && event.Op&fsnotify.Create == fsnotify.Create {
				klog.V(1).Infof("inotify: %s created, restarting.", pluginapi.KubeletSocket)
				goto restart
			}

		// Watch for any other fs errors and log them.
		case err := <-watcher.Errors:
			klog.Infof("inotify: %s", err)

		// Watch for any signals from the OS. On SIGHUP, restart this loop,
		// restarting all of the plugins in the process. On all other
		// signals, exit the loop and exit the program.
		case s := <-sigs:
			switch s {
			case syscall.SIGHUP:
				klog.Info("Received SIGHUP, restarting.")
				goto restart
			case syscall.SIGQUIT:
				writeCoreDump()
			default:
				klog.Infof("received signal %v, shutting down", s)
				goto exit
			}
		}
	}
exit:
	// stop the topology worker before the plugins: a sync running after the
	// plugins were stopped could start the rdma plugin again through
	// ensureRDMAStarted and leave it behind
	cancel()

	// the worker may already be inside a sync that passed its context check
	dm.beginRebuild()

	// dm.plugins is the only source of truth for what is running, whatever
	// round started it
	if err := dm.stopAllPlugins(); err != nil {
		return fmt.Errorf("error stopping plugins: %v", err)
	}

	dm.Stop()
	return nil
}

// exitOnStartupSignal reacts to a signal received before the plugins run. It
// reports whether the process must shut down: SIGHUP keeps waiting for the
// first snapshot because there is nothing to restart yet, SIGQUIT keeps its
// core dump meaning, everything else ends the process.
func exitOnStartupSignal(s os.Signal) bool {
	switch s {
	case syscall.SIGHUP:
		klog.Info("Received SIGHUP before the first device snapshot, still waiting for xdm")
		return false
	case syscall.SIGQUIT:
		writeCoreDump()
		return false
	default:
		klog.Infof("received signal %v, shutting down", s)
		return true
	}
}

// writeCoreDump dumps the goroutine state so a hang can be inspected afterwards.
func writeCoreDump() {
	t := time.Now()
	timestamp := fmt.Sprint(t.Format("20060102150405"))
	klog.Infof("generating core dump")
	coreDump("/etc/kubernetes/go_" + timestamp + ".txt")
}

// Stop signals the goroutines of the current run to exit and arms a fresh
// stop channel for the next run. Run reaches Stop on every plugin restart
// (kubelet restart, SIGHUP, start retry) and on exit, so reusing one channel
// would close it twice and panic, and a closed channel would make the next
// health check exit right away.
func (dm *DeviceManager) Stop() {
	close(dm.stopChan)
	dm.stopChan = make(chan struct{})
}
