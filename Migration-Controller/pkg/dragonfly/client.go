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

// Package dragonfly provides a REST client for interacting with the Dragonfly
// Manager preheat API. The token is read from a Kubernetes Secret on each call.
package dragonfly

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/container-migration/controller/pkg/preheat"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// PreheatScopeAllSeedPeers makes Dragonfly preheat the file to all seed peers
// in the cluster. The Manager's PreheatArgs.Scope defaults to
// single_seed_peer (only one seed peer preheated); other nodes would still
// fetch across nodes at restore, so it is fixed to preheat all seed peers
// here.
const PreheatScopeAllSeedPeers = "all_seed_peers"

// TaskState represents the state of a Dragonfly preheat task. It is an alias
// of the backend-neutral preheat.TaskState: the abstraction in pkg/preheat
// owns the state values and this client conforms to them.
type TaskState = preheat.TaskState

const (
	// TaskStatePending the task is waiting to be scheduled
	TaskStatePending = preheat.TaskStatePending
	// TaskStateRunning the task is being executed
	TaskStateRunning = preheat.TaskStateRunning
	// TaskStateSuccess the task finished successfully
	TaskStateSuccess = preheat.TaskStateSuccess
	// TaskStateFailure the task failed
	TaskStateFailure = preheat.TaskStateFailure
)

// PreheatRequest contains the parameters for creating a preheat task.
type PreheatRequest struct {
	// URL remote OSS file URL to preheat
	URL string
	// BIO is an idempotency key written to the Dragonfly job biography field.
	// It is used to recover the task ID when a previous reconcile created the
	// job but failed to persist the task ID in the CR status.
	BIO string
	// AccessKeyID OSS access key id
	AccessKeyID string
	// AccessKeySecret OSS access key secret
	AccessKeySecret string
	// Options extra options passed from the CR spec
	Options map[string]string
}

// PeerInfo describes a peer that has cached the preheated file.
type PeerInfo struct {
	IP        string
	NodeName  string
	Cached    bool
	LastSeen  *metav1.Time
	ExpiredAt *metav1.Time
}

// Task describes the status of a Dragonfly preheat task.
type Task struct {
	ID      string
	State   TaskState
	Message string
}

// SeedPeer describes an alive Dragonfly seed peer discovered via the
// Dragonfly scheduler ListHosts API.
type SeedPeer struct {
	Hostname string
	IP       string
	Port     int32
	State    string
}

// Client is the interface used by the PreheatJob controller to interact with
// the Dragonfly preheat service.
type Client interface {
	// CreatePreheatTask creates a new preheat task and returns its task ID.
	CreatePreheatTask(ctx context.Context, req PreheatRequest) (string, error)
	// GetPreheatTask returns the current state of the preheat task.
	GetPreheatTask(ctx context.Context, taskID string) (Task, error)
	// FindPreheatTask searches Dragonfly jobs for an existing preheat task whose
	// biography field matches the given idempotency key. It returns the task ID
	// or an empty string if no matching job is found.
	FindPreheatTask(ctx context.Context, bio string) (string, error)
}

// Config holds the configuration for the REST client.
type Config struct {
	// ManagerURL is the base URL of the Dragonfly Manager HTTP API, e.g.
	// "http://dragonfly-manager.dragonfly.svc.cluster.local:8080".
	ManagerURL string
	// K8sClient is the controller-runtime client used to read the PAT Secret.
	K8sClient client.Client
	// SecretNamespace is the namespace where dragonfly-preheat-pat Secret lives.
	// If empty, it falls back to the service account namespace file.
	SecretNamespace string
	// SecretName is the name of the Secret holding the personal access token.
	// Defaults to "dragonfly-preheat-pat".
	SecretName string
	// SecretKey is the data key inside the Secret that holds the personal access token.
	// Defaults to "PAT_TOKEN".
	SecretKey string
	// HTTPClient is the optional HTTP client. A default client is used if nil.
	HTTPClient *http.Client
}

// NewClient creates a real Dragonfly Manager REST client.
func NewClient(cfg Config) (Client, error) {
	if cfg.ManagerURL == "" {
		return nil, fmt.Errorf("manager URL is required")
	}
	if cfg.K8sClient == nil {
		return nil, fmt.Errorf("kubernetes client is required")
	}
	if cfg.SecretName == "" {
		cfg.SecretName = "dragonfly-preheat-pat"
	}
	if cfg.SecretKey == "" {
		cfg.SecretKey = "PAT_TOKEN"
	}
	if cfg.SecretNamespace == "" {
		cfg.SecretNamespace = "dragonfly"
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 30 * time.Second}
	}

	managerURL := strings.TrimRight(cfg.ManagerURL, "/")
	return &restClient{
		managerURL:      managerURL,
		k8sClient:       cfg.K8sClient,
		secretNamespace: cfg.SecretNamespace,
		secretName:      cfg.SecretName,
		secretKey:       cfg.SecretKey,
		httpClient:      cfg.HTTPClient,
	}, nil
}

// restClient implements Client by calling the Dragonfly Manager REST API.
type restClient struct {
	managerURL      string
	k8sClient       client.Client
	secretNamespace string
	secretName      string
	secretKey       string
	httpClient      *http.Client
}

// CreatePreheatTask calls POST /oapi/v1/jobs to create a preheat job.
func (c *restClient) CreatePreheatTask(ctx context.Context, req PreheatRequest) (string, error) {
	token, err := c.patToken(ctx)
	if err != nil {
		return "", fmt.Errorf("get dragonfly pat token: %w", err)
	}

	args := map[string]any{
		"type":  "file",
		"url":   req.URL,
		"scope": PreheatScopeAllSeedPeers,
	}
	if req.AccessKeyID != "" && req.AccessKeySecret != "" && IsOSSURL(req.URL) {
		// OSS origin fetch needs standard signature auth: swap the AK/SK for a
		// presigned URL (OSSAccessKeyId + Expires + Signature). OSS does not
		// recognize custom x-oss-* request headers; passing them through
		// directly would cause a 403 at origin fetch. The signature parameters
		// are stripped automatically when computing task_id, not affecting
		// probing.
		signedURL, err := SignOSSURL(req.URL, req.AccessKeyID, req.AccessKeySecret, DefaultOSSSignExpiry)
		if err != nil {
			return "", fmt.Errorf("sign OSS url: %w", err)
		}
		args["url"] = signedURL
	}

	body := createJobRequest{
		Type: "preheat",
		BIO:  req.BIO,
		Args: args,
	}

	data, err := json.Marshal(body)
	if err != nil {
		return "", fmt.Errorf("marshal create job request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.managerURL+"/oapi/v1/jobs", bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("build create job request: %w", err)
	}
	c.setHeaders(httpReq, token)

	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("create preheat job: %w", err)
	}
	defer httpResp.Body.Close()

	respBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return "", fmt.Errorf("read create job response: %w", err)
	}
	if httpResp.StatusCode != http.StatusOK && httpResp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("create preheat job failed: status=%d body=%s", httpResp.StatusCode, string(respBody))
	}

	var resp createJobResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return "", fmt.Errorf("unmarshal create job response: %w", err)
	}
	if resp.ID == 0 {
		return "", fmt.Errorf("create preheat job returned empty id")
	}
	return fmt.Sprintf("%d", resp.ID), nil
}

// GetPreheatTask calls GET /oapi/v1/jobs/{id} to query the job status.
func (c *restClient) GetPreheatTask(ctx context.Context, taskID string) (Task, error) {
	token, err := c.patToken(ctx)
	if err != nil {
		return Task{}, fmt.Errorf("get dragonfly pat token: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.managerURL+"/oapi/v1/jobs/"+taskID, nil)
	if err != nil {
		return Task{}, fmt.Errorf("build get job request: %w", err)
	}
	c.setHeaders(httpReq, token)

	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return Task{}, fmt.Errorf("get preheat job: %w", err)
	}
	defer httpResp.Body.Close()

	respBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return Task{}, fmt.Errorf("read get job response: %w", err)
	}
	if httpResp.StatusCode != http.StatusOK {
		return Task{}, fmt.Errorf("get preheat job failed: status=%d body=%s", httpResp.StatusCode, string(respBody))
	}

	var resp getJobResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return Task{}, fmt.Errorf("unmarshal get job response: %w", err)
	}

	state, msg := mapJobState(resp.State)
	return Task{
		ID:      fmt.Sprintf("%d", resp.ID),
		State:   state,
		Message: msg,
	}, nil
}

// FindPreheatTask searches existing preheat jobs by the BIO idempotency key.
// It paginates through GET /oapi/v1/jobs and returns the task ID of the first
// job whose biography field matches the given bio. If no match is found, it
// returns an empty string.
func (c *restClient) FindPreheatTask(ctx context.Context, bio string) (string, error) {
	if bio == "" {
		return "", nil
	}

	token, err := c.patToken(ctx)
	if err != nil {
		return "", fmt.Errorf("get dragonfly pat token: %w", err)
	}

	const perPage = 100
	for page := 1; ; page++ {
		jobs, err := c.listJobsPage(ctx, token, page, perPage)
		if err != nil {
			return "", err
		}
		for _, j := range jobs {
			if j.BIO == bio {
				return fmt.Sprintf("%d", j.ID), nil
			}
		}
		if len(jobs) < perPage {
			break
		}
	}
	return "", nil
}

// listJobsPage fetches a single page of jobs from GET /oapi/v1/jobs.
func (c *restClient) listJobsPage(ctx context.Context, token string, page, perPage int) ([]getJobResponse, error) {
	u := fmt.Sprintf("%s/oapi/v1/jobs?type=preheat&page=%d&per_page=%d", c.managerURL, page, perPage)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("build list jobs request: %w", err)
	}
	c.setHeaders(httpReq, token)

	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("list jobs page %d: %w", page, err)
	}
	defer httpResp.Body.Close()

	respBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, fmt.Errorf("read list jobs page %d response: %w", page, err)
	}
	if httpResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("list jobs page %d failed: status=%d body=%s", page, httpResp.StatusCode, string(respBody))
	}

	var resp []getJobResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return nil, fmt.Errorf("unmarshal list jobs page %d response: %w", page, err)
	}
	return resp, nil
}

func (c *restClient) patToken(ctx context.Context) (string, error) {
	secret := &corev1.Secret{}
	if err := c.k8sClient.Get(ctx, types.NamespacedName{
		Namespace: c.secretNamespace,
		Name:      c.secretName,
	}, secret); err != nil {
		return "", fmt.Errorf("get secret %s/%s: %w", c.secretNamespace, c.secretName, err)
	}
	tokenBytes, ok := secret.Data[c.secretKey]
	if !ok {
		return "", fmt.Errorf("secret %s/%s has no key %q", c.secretNamespace, c.secretName, c.secretKey)
	}
	return string(tokenBytes), nil
}

func (c *restClient) setHeaders(r *http.Request, token string) {
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Accept", "application/json")
	r.Header.Set("Authorization", "Bearer "+token)
}

// createJobRequest mirrors the Dragonfly Manager POST /oapi/v1/jobs body.
type createJobRequest struct {
	Type                string         `json:"type"`
	BIO                 string         `json:"bio,omitempty"`
	Args                map[string]any `json:"args"`
	Result              map[string]any `json:"result,omitempty"`
	UserID              uint           `json:"user_id,omitempty"`
	SeedPeerClusterIDs  []uint         `json:"seed_peer_cluster_ids,omitempty"`
	SchedulerClusterIDs []uint         `json:"scheduler_cluster_ids,omitempty"`
}

// createJobResponse mirrors the Dragonfly Manager create job response.
type createJobResponse struct {
	ID     uint64 `json:"id"`
	TaskID string `json:"task_id"`
	Type   string `json:"type"`
	State  string `json:"state"`
}

// getJobResponse mirrors the Dragonfly Manager get job response.
type getJobResponse struct {
	ID         uint64         `json:"id"`
	TaskID     string         `json:"task_id"`
	BIO        string         `json:"bio"`
	Type       string         `json:"type"`
	State      string         `json:"state"`
	Result     map[string]any `json:"result"`
	Args       map[string]any `json:"args"`
	CreatedAt  string         `json:"created_at"`
	UpdatedAt  string         `json:"updated_at"`
	StartedAt  string         `json:"started_at"`
	FinishedAt string         `json:"finished_at"`
}

func mapJobState(state string) (TaskState, string) {
	switch strings.ToUpper(state) {
	case "PENDING", "RECEIVED", "STARTED", "RETRY":
		return TaskStateRunning, ""
	case "SUCCESS":
		return TaskStateSuccess, ""
	case "FAILURE":
		return TaskStateFailure, "job failed"
	default:
		return TaskStatePending, ""
	}
}
