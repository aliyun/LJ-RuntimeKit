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

package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"k8s.io/klog/v2"
)

// pvcView is the minimal view of a corev1.PersistentVolumeClaim, only caring about spec.volumeName.
type pvcView struct {
	Spec struct {
		VolumeName string `json:"volumeName"`
	} `json:"spec"`
}

// pvView is the minimal view of a corev1.PersistentVolume, only caring about
// the two fields that can express "the sub-path of the PVC mount root inside
// shared storage": csi.volumeAttributes.path (OSS/NAS etc. CSI) and nfs.path
// (traditional NFS). hostPath/local's path is a node-local path with different
// semantics, deliberately not taken.
type pvView struct {
	Spec struct {
		CSI *struct {
			VolumeAttributes map[string]string `json:"volumeAttributes"`
		} `json:"csi"`
		NFS *struct {
			Path string `json:"path"`
		} `json:"nfs"`
	} `json:"spec"`
}

const (
	pvcAPIPathFmt = "/api/v1/namespaces/%s/persistentvolumeclaims/%s"
	pvAPIPathFmt  = "/api/v1/persistentvolumes/%s"
)

// getKubeJSON does a GET and parses JSON via the shared apiserver REST path
// (kubeRESTConfig/kubeHTTPClient, the same mechanism as GetPreheatJob). The
// worker side is plain REST with no informer, so RBAC only needs the get verb.
func getKubeJSON(ctx context.Context, path string, out any) error {
	kubeMu.RLock()
	cfg, client := kubeRESTConfig, kubeHTTPClient
	kubeMu.RUnlock()

	if cfg == nil || client == nil {
		return fmt.Errorf("kubernetes client is not initialized")
	}

	endpoint := strings.TrimRight(cfg.Host, "/") + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return fmt.Errorf("build get request %s: %w", path, err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("get %s: %w", path, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read %s response: %w", path, err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("get %s failed: status=%d body=%s", path, resp.StatusCode, string(body))
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("unmarshal %s: %w", path, err)
	}
	return nil
}

// resolvePVCEFCPrefix derives the EFC relative path prefix from the PVC/PV (field probing, no type judgment):
//
//  1. GET PVC -> spec.volumeName (PVC unbound / read failure -> empty prefix);
//  2. GET PV -> prefer csi.volumeAttributes["path"], else nfs.path; both go
//     through sanitizeEFCRelPath (trim slashes + Clean + escape check);
//  3. neither present (hostPath/local etc. local volumes) or invalid fields ->
//     empty prefix, so the EFC read necessarily misses and the caller falls
//     back quickly.
//
// No step fails with an error: log a Warning and return the empty prefix; the
// caller tries EFC with checkpointDir directly - EFC-first is only an
// accelerator and must not block the restore main path. The empty prefix from
// invalid fields still joins inside /mnt/efc, so the degraded result itself is
// safe.
func resolvePVCEFCPrefix(ctx context.Context, pvcNamespace, pvcName string) string {
	if pvcNamespace == "" || pvcName == "" {
		klog.Warningf("cannot resolve EFC prefix without PVC name/namespace (namespace=%q name=%q), will try EFC without prefix",
			pvcNamespace, pvcName)
		return ""
	}

	var pvc pvcView
	if err := getKubeJSON(ctx, fmt.Sprintf(pvcAPIPathFmt, pvcNamespace, pvcName), &pvc); err != nil {
		klog.Warningf("failed to resolve PVC %s/%s for EFC prefix: %v (will try EFC without prefix)", pvcNamespace, pvcName, err)
		return ""
	}
	if pvc.Spec.VolumeName == "" {
		klog.V(2).Infof("PVC %s/%s is not bound, EFC read will use checkpointDir directly", pvcNamespace, pvcName)
		return ""
	}

	var pv pvView
	if err := getKubeJSON(ctx, fmt.Sprintf(pvAPIPathFmt, pvc.Spec.VolumeName), &pv); err != nil {
		klog.Warningf("failed to resolve PV %s for EFC prefix: %v (will try EFC without prefix)", pvc.Spec.VolumeName, err)
		return ""
	}

	if pv.Spec.CSI != nil {
		if p := pvEFCPrefix(pvc.Spec.VolumeName, "csi.volumeAttributes[path]",
			pv.Spec.CSI.VolumeAttributes["path"]); p != "" {
			return p
		}
	}
	if pv.Spec.NFS != nil {
		if p := pvEFCPrefix(pvc.Spec.VolumeName, "nfs.path", pv.Spec.NFS.Path); p != "" {
			return p
		}
	}
	return ""
}

// pvEFCPrefix validates whether a single PV path field can serve as the EFC
// relative prefix: valid returns the normalized prefix; invalid (".." escaping
// /mnt/efc, contains backslash) logs a Warning and returns "" so the caller
// tries the next field and finally degrades to the empty prefix. Hidden dir
// names (e.g. .hidden) are valid: checkpoint data may be moved to a user-made
// dir; the criterion aligns with validRelPath on the controller side.
//
// PV is a cluster-scoped object; these two fields are written by static PV
// yamls or CSI provisioners, not controlled input from this task's CR. The
// downstream LocalFS.Restore joins paths with filepath.Join, and Join Cleans
// "../" away directly - without validation, the restore's dir listing, reads
// and extracts could be pointed outside the mount root (the worker runs as root
// with host paths mounted).
func pvEFCPrefix(pvName, field, raw string) string {
	p, err := sanitizeEFCRelPath(raw)
	if err != nil {
		klog.Warningf("ignoring %s %q of PV %s as EFC prefix: %v (will try EFC without prefix)",
			field, raw, pvName, err)
		return ""
	}
	return p
}

// efcRelPath joins the relative path under the EFC mount root; when prefix is empty it is checkpointDir itself.
func efcRelPath(prefix, checkpointDir string) string {
	if prefix == "" {
		return checkpointDir
	}
	return prefix + "/" + checkpointDir
}
