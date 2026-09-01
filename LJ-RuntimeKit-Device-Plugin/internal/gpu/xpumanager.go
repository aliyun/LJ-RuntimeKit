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
	"errors"
	"fmt"
	"os"
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
	k8sClient         *kubernetes.Clientset
	deviceMeta        *utils.DeviceMeta
	annotationDevices *GpuDevice
	stopChan          chan struct{}

	plugins []xpuplugin.PluginInterface

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
	}
}

func (dm *DeviceManager) Run() error {
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

	GrpcListAndWatch(xmdClient, dm.deviceMeta)

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

	if err := dm.patchNodeAnnotation(); err != nil {
		klog.Errorf("patchNodeAnnotation failed on node %s: %v", nodeName, err)
	}

	deviceType, err := GetDeviceType(xmdClient)
	if err != nil {
		klog.Errorf("failed to get device type: %v", err)
	}

	var started bool
	var restartTimeout <-chan time.Time
restart:
	if started {
		err := stopPlugins(dm.plugins)
		if err != nil {
			return fmt.Errorf("error stopping plugins from previous run: %v", err)
		}

		dm.Stop()
	}

	klog.Info("starting plugins")
	plugins, restartPlugins, err := startPlugins(deviceType, dm.deviceMeta, dm.k8sClient)
	if err != nil {
		return fmt.Errorf("error starting plugins: %v", err)
	}
	started = true

	if restartPlugins {
		klog.Infof("Failed to start one or more plugins. Retrying in 30s...")
		restartTimeout = time.After(30 * time.Second)
	} else {
		dm.plugins = plugins

		if deviceType == utils.DeviceTypeGPU {
			klog.Infof("Starting device health check for device type: %s", deviceType)
			go dm.DeviceHealthCheck()
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
				t := time.Now()
				timestamp := fmt.Sprint(t.Format("20060102150405"))
				klog.Infof("generating core dump")
				coreDump("/etc/kubernetes/go_" + timestamp + ".txt")
			default:
				klog.Infof("received signal %v, shutting down", s)
				goto exit
			}
		}
	}
exit:
	err = stopPlugins(plugins)
	if err != nil {
		return fmt.Errorf("error stopping plugins: %v", err)
	}

	dm.Stop()
	return nil
}

func (dm *DeviceManager) Stop() {
	close(dm.stopChan)
}
