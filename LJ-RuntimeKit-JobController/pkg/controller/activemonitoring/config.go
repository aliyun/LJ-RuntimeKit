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

package activemonitoring

import (
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/klog/v2"

	"github.com/lj-runtimekit/job-controller/pkg/nodeagent"
)

// detectionSettings are the resolved detection Pod settings: the container
// image and the RuntimeKit socket hostPath/mountPath.
type detectionSettings struct {
	image      string
	socketPath string
}

// defaultDetectionSettings returns the built-in detection Pod settings, used
// until the shared controller ConfigMap has been loaded successfully.
func defaultDetectionSettings() *detectionSettings {
	return &detectionSettings{
		image:      defaultTestImage,
		socketPath: defaultRuntimeKitSocket,
	}
}

// detectionConfig returns the pre-resolved detection settings.
func (c *Controller) detectionConfig() *detectionSettings {
	c.cfgMu.Lock()
	defer c.cfgMu.Unlock()
	if c.lastGoodAM != nil {
		return c.lastGoodAM
	}
	return defaultDetectionSettings()
}

// onConfigMapEvent is the ConfigMap informer Add/Update handler: it re-resolves
// and stores the detection settings so the next Pod creation picks them up.
func (c *Controller) onConfigMapEvent(obj interface{}) {
	if u := unstructuredObject(obj); u != nil {
		c.storeDetectionConfig(u)
	}
}

// loadDetectionConfigFromStore seeds the detection settings synchronously from
// the synced informer store after cache sync, because informer event delivery
// is asynchronous and must not race the first Pod creation.
func (c *Controller) loadDetectionConfigFromStore() {
	obj, exists, err := c.cmInformer.GetStore().GetByKey(cacheKey(c.config.Namespace, c.config.ConfigMapName))
	if err != nil || !exists {
		klog.InfoS("controller ConfigMap not found after cache sync, detection Pods use built-in defaults",
			"configMap", c.config.Namespace+"/"+c.config.ConfigMapName)
		return
	}
	if u, ok := obj.(*unstructured.Unstructured); ok {
		c.storeDetectionConfig(u)
	}
}

// storeDetectionConfig parses the controller ConfigMap and caches the resolved settings.
func (c *Controller) storeDetectionConfig(u *unstructured.Unstructured) {
	revision := u.GetResourceVersion()
	settings, err := resolveDetectionSettings(u)

	c.cfgMu.Lock()
	defer c.cfgMu.Unlock()
	if err == nil {
		if c.lastGoodAM == nil || c.lastGoodAMRV != revision || c.lastBadAMRV != "" {
			klog.InfoS("ActiveMonitoring detection configuration loaded",
				"configMap", c.config.Namespace+"/"+c.config.ConfigMapName,
				"resourceVersion", revision, "previousResourceVersion", c.lastGoodAMRV,
				"recoveredFromInvalid", c.lastBadAMRV != "",
				"image", settings.image, "socketPath", settings.socketPath)
		}
		c.lastGoodAM, c.lastGoodAMRV, c.lastBadAMRV = settings, revision, ""
		return
	}
	if c.lastBadAMRV != revision {
		klog.ErrorS(err, "ActiveMonitoring detection configuration rejected, keeping last good or built-in defaults",
			"configMap", c.config.Namespace+"/"+c.config.ConfigMapName,
			"resourceVersion", revision, "usingLastGood", c.lastGoodAM != nil,
			"lastGoodResourceVersion", c.lastGoodAMRV)
		c.lastBadAMRV = revision
	}
}

// resolveDetectionSettings extracts config.yaml from the ConfigMap object and
// resolves the detection image and socket path with their fallbacks.
func resolveDetectionSettings(u *unstructured.Unstructured) (*detectionSettings, error) {
	data, found, err := unstructured.NestedString(u.Object, "data", nodeagent.ConfigKey)
	if err != nil {
		return nil, fmt.Errorf("read %q from ConfigMap %s/%s: %w", nodeagent.ConfigKey, u.GetNamespace(), u.GetName(), err)
	}
	if !found {
		return nil, fmt.Errorf("ConfigMap %s/%s has no %q key", u.GetNamespace(), u.GetName(), nodeagent.ConfigKey)
	}
	cfg, err := nodeagent.ParseConfig(data)
	if err != nil {
		return nil, err
	}
	settings := &detectionSettings{
		image:      cfg.ActiveMonitoringImage,
		socketPath: cfg.SocketPath,
	}
	if settings.image == "" {
		settings.image = defaultTestImage
	}
	return settings, nil
}
