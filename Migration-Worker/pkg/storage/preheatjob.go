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
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/container-migration/worker/pkg/apis"

	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
)

// PreheatJob is the minimal view of the controller-side migration.io/v1beta1 PreheatJob CR.
//
// Deliberately no generated clientset here: the worker only needs spec.remoteURL /
// spec.options and status.phase, so one REST GET against the apiserver plus JSON
// decoding is enough, avoiding code-generator artifacts and a whole typed
// client dependency.
type PreheatJob struct {
	Metadata struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	} `json:"metadata"`
	Spec struct {
		// RemoteURL is the full http(s) address of the remote OSS object
		RemoteURL string `json:"remoteURL"`
		// Options is the passthrough params, including OSS accessKeyId / accessKeySecret
		Options map[string]string `json:"options"`
	} `json:"spec"`
	Status struct {
		// Phase: only Ready means data is actually cached on the seed peer
		Phase string `json:"phase"`
		// TaskID is the Dragonfly Manager-side preheat task ID, for log correlation only
		TaskID string `json:"taskID"`
		// RemoteURL is the source address actually used at preheat. A PVC preheat
		// source is generated dynamically by the controller (the ClusterIP of the
		// long-running preheat Service), only frozen in status and never written back
		// to spec; an OSS source also writes status when transitioning to Running.
		// Read status first, spec as fallback.
		RemoteURL string `json:"remoteURL"`
		// EFCPath is the preheat dir assembled by EFC preheat (bucket-relative path).
		// Non-empty means this PreheatJob runs the EFC backend (used to decide when a
		// storageType=dragonfly/efc job carries this CR).
		EFCPath string `json:"efcPath"`
	} `json:"status"`
}

// preheatJobPhaseReady matches v1beta1.PreheatJobPhaseReady on the controller side.
const preheatJobPhaseReady = "Ready"

// preheatJobAPIPathFmt is the namespaced REST path template for PreheatJob.
const preheatJobAPIPathFmt = "/apis/migration.io/v1beta1/namespaces/%s/preheatjobs/%s"

var (
	kubeMu         sync.RWMutex
	kubeRESTConfig *rest.Config
	kubeHTTPClient *http.Client
)

// SetKubeRESTConfig injects the config for accessing the apiserver, called once
// by main at startup. Without it (or if it fails), PreheatJob resolution
// degrades entirely and the legacy oss_mount_point path is unaffected.
func SetKubeRESTConfig(cfg *rest.Config) error {
	if cfg == nil {
		return fmt.Errorf("rest config is nil")
	}
	transport, err := rest.TransportFor(cfg)
	if err != nil {
		return fmt.Errorf("build transport for apiserver: %w", err)
	}

	kubeMu.Lock()
	defer kubeMu.Unlock()
	kubeRESTConfig = cfg
	kubeHTTPClient = &http.Client{Transport: transport, Timeout: 30 * time.Second}
	return nil
}

// GetPreheatJob reads one PreheatJob CR by namespace/name.
func GetPreheatJob(ctx context.Context, namespace, name string) (*PreheatJob, error) {
	kubeMu.RLock()
	cfg, client := kubeRESTConfig, kubeHTTPClient
	kubeMu.RUnlock()

	if cfg == nil || client == nil {
		return nil, fmt.Errorf("kubernetes client is not initialized")
	}

	endpoint := strings.TrimRight(cfg.Host, "/") + fmt.Sprintf(preheatJobAPIPathFmt, namespace, name)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build get preheatjob request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get preheatjob %s/%s: %w", namespace, name, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read preheatjob %s/%s response: %w", namespace, name, err)
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("PreheatJob %s/%s not found", namespace, name)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get preheatjob %s/%s failed: status=%d body=%s",
			namespace, name, resp.StatusCode, string(body))
	}

	job := &PreheatJob{}
	if err := json.Unmarshal(body, job); err != nil {
		return nil, fmt.Errorf("unmarshal preheatjob %s/%s: %w", namespace, name, err)
	}
	return job, nil
}

// preheatJobResolved is the download source params resolved from a PreheatJob.
//
// OSS sources fill bucket/endpoint/credentials; PVC preheat sources (http
// sources) fill only httpSourceURL - remoteURL itself is the checkpoint dir
// address (the ClusterIP of the long-running preheat Service), and the worker
// computes task_id from it to hit the P2P cache; origin reachability is
// guaranteed by the restore worker co-hosting the same PVC (the preheat source
// pod is deleted once preheat becomes Ready).
type preheatJobResolved struct {
	bucketName      string
	basePath        string
	ossEndpoint     string
	accessKeyId     string
	accessKeySecret string
	// httpSourceURL is the checkpoint dir URL for non-OSS sources (trailing slash
	// trimmed). When non-empty, shard URLs are appended to it directly, no OSS
	// signing.
	httpSourceURL string
	// parts is the shard list for http-source preheat: the http source cannot be
	// probed from the worker side, so the count is inferred from the length of the
	// PreheatJob status.taskID list (0..N-1).
	parts []int
}

// resolveFromPreheatJob reads the PreheatJob CR, confirms it is Ready, and
// resolves bucket / endpoint / object and credentials from spec. The second
// return value is the object name the CR specifies: a preheat task targets one
// concrete object, so restore no longer builds the path from basePath+tarFile -
// otherwise the computed URL would differ from the preheat URL, task_id would
// mismatch, and the P2P cache would miss.
func resolveFromPreheatJob(dfConfig *migrationworker.Dragonfly) (*preheatJobResolved, string, error) {
	if dfConfig.PreheatJobNamespace == "" || dfConfig.PreheatJobName == "" {
		return nil, "", fmt.Errorf("preheatJob name or namespace is empty")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pj, err := GetPreheatJob(ctx, dfConfig.PreheatJobNamespace, dfConfig.PreheatJobName)
	if err != nil {
		return nil, "", err
	}

	if pj.Status.Phase != preheatJobPhaseReady {
		return nil, "", fmt.Errorf("PreheatJob %s/%s is not Ready (phase=%q)",
			dfConfig.PreheatJobNamespace, dfConfig.PreheatJobName, pj.Status.Phase)
	}

	// remoteURL prefers the frozen value in status: a PVC preheat source's http
	// address exists only in status (spec only fills pvc/checkpointUID; the address
	// is generated by the controller and frozen here), and only task_id computed
	// from it can hit the P2P cache; OSS sources have it in both spec and status.
	remoteURL := pj.Status.RemoteURL
	if remoteURL == "" {
		remoteURL = pj.Spec.RemoteURL
	}
	if remoteURL == "" {
		return nil, "", fmt.Errorf("PreheatJob %s/%s has empty remoteURL (spec or status)",
			dfConfig.PreheatJobNamespace, dfConfig.PreheatJobName)
	}

	// PVC preheat source: remoteURL is a plain http address (http://<ClusterIP of
	// the long-running preheat Service>:50090/<pvcPath>), not an OSS URL. Shard
	// URLs are appended to it directly, no signing, no AK/SK needed.
	if !isOSSRemoteURL(remoteURL) {
		u, err := url.Parse(remoteURL)
		if err != nil {
			return nil, "", fmt.Errorf("parse PreheatJob remoteURL %q: %w", remoteURL, err)
		}
		objectName := strings.TrimPrefix(u.Path, "/")
		if objectName == "" {
			return nil, "", fmt.Errorf("PreheatJob %s/%s remoteURL %q has no object path",
				dfConfig.PreheatJobNamespace, dfConfig.PreheatJobName, remoteURL)
		}
		klog.V(2).Infof("Resolved http-source preheat from PreheatJob %s/%s: taskID=%s sourceURL=%s object=%s",
			dfConfig.PreheatJobNamespace, dfConfig.PreheatJobName,
			pj.Status.TaskID, remoteURL, objectName)

		// The http source cannot be probed from the restore side; the shard count can
		// only be inferred from the length of the status.taskID comma list (one preheat
		// task per shard).
		partCount, err := inferPartCountFromTaskID(pj.Status.TaskID)
		if err != nil {
			return nil, "", fmt.Errorf("PreheatJob %s/%s: %w",
				dfConfig.PreheatJobNamespace, dfConfig.PreheatJobName, err)
		}
		parts := make([]int, 0, partCount)
		for i := 0; i < partCount; i++ {
			parts = append(parts, i)
		}
		return &preheatJobResolved{
			httpSourceURL: strings.TrimRight(remoteURL, "/"),
			parts:         parts,
		}, objectName, nil
	}

	bucketName, ossEndpoint, objectName, err := parseRemoteOSSURL(remoteURL)
	if err != nil {
		return nil, "", fmt.Errorf("parse PreheatJob remoteURL %q: %w", remoteURL, err)
	}

	accessKeyId := pj.Spec.Options[OSS_ACCESS_KEY_ID]
	accessKeySecret := pj.Spec.Options[OSS_ACCESS_KEY_SECRET]
	if accessKeyId == "" || accessKeySecret == "" {
		return nil, "", fmt.Errorf("PreheatJob %s/%s missing %s or %s in spec.options",
			dfConfig.PreheatJobNamespace, dfConfig.PreheatJobName,
			OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET)
	}

	klog.V(2).Infof("Resolved Dragonfly storage from PreheatJob %s/%s: taskID=%s bucket=%s endpoint=%s object=%s",
		dfConfig.PreheatJobNamespace, dfConfig.PreheatJobName,
		pj.Status.TaskID, bucketName, ossEndpoint, objectName)

	return &preheatJobResolved{
		bucketName:      bucketName,
		basePath:        "/" + filepath.Dir(objectName),
		ossEndpoint:     ossEndpoint,
		accessKeyId:     accessKeyId,
		accessKeySecret: accessKeySecret,
	}, objectName, nil
}

// inferPartCountFromTaskID infers the shard count from the status.taskID comma
// list. An empty TaskID must be explicitly rejected: strings.Split("", ",")
// returns [""] (length 1), and without the guard "preheat not done / status not
// backfilled" would be silently masked as one shard, making restore download a
// nonexistent part-0 whose root cause is then buried under P2P errors.
func inferPartCountFromTaskID(taskID string) (int, error) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return 0, fmt.Errorf("empty taskID in status, cannot infer part count")
	}
	return len(strings.Split(taskID, ",")), nil
}

// isOSSRemoteURL reports whether the preheat remoteURL points at an OSS endpoint
// (virtual-hosted style *.aliyuncs.com). A PVC preheat source's http address (a
// node IP) does not match that form; treat it as a plain http source.
func isOSSRemoteURL(remoteURL string) bool {
	u, err := url.Parse(remoteURL)
	if err != nil {
		return false
	}
	return strings.HasSuffix(u.Hostname(), ".aliyuncs.com")
}

// parseRemoteOSSURL splits https://{bucket}.{endpoint}/{object} into three parts.
func parseRemoteOSSURL(remoteURL string) (bucket, endpoint, objectName string, err error) {
	u, err := url.Parse(remoteURL)
	if err != nil {
		return "", "", "", fmt.Errorf("invalid URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", "", "", fmt.Errorf("unsupported URL scheme %q, want http or https", u.Scheme)
	}
	host := u.Hostname()
	parts := strings.SplitN(host, ".", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", "", fmt.Errorf("cannot extract bucket/endpoint from host %q", host)
	}
	objectName = strings.TrimPrefix(u.Path, "/")
	if objectName == "" {
		return "", "", "", fmt.Errorf("URL %q has no object path", remoteURL)
	}
	return parts[0], parts[1], objectName, nil
}
