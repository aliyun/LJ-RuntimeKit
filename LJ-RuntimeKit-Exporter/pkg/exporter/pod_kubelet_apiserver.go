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

package exporter

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/transport"
)

// Annotation keys and resource names are externalized to a ConfigMap (see
// loader.go). They are declared as var so LoadProtocol can override them at
// startup; the compiled-in values remain the fallback defaults.
var (
	EGPU_SCHEDULER_ANNOTATION_GPU_ID_STR_LEGACY = "ALIYUN_COM_GPU_MEM_IDX"
	EGPU_SCHEDULER_ANNOTATION_GPU_ID_STR        = "ALIYUN_COM_GPU_IDX_LIST"
	KUBERGPU_SCHEDULER_ANNOTATION_MIG_UUID      = "aliyun.com/mig-ci-uuid"

	EGPU_COMPUTE_RESOURCE_STR = "aliyun.com/gpu-core"
	EGPU_MEMORY_RESOURCE_STR  = "aliyun.com/gpu-memory"
	EGPU_MIG_RESOURCE_STR     = "aliyun.com/gpu-slice"
	EGPU_GPU_NV_RESOURCE_STR  = "nvidia.com/gpu"
	EGPU_GPU_RESOURCE_STR     = "aliyun.com/gpu"
)

const (
	connectTimeout = time.Duration(10) * time.Second
)

type AsiGpuResource struct {
	Core     string `json:"koordinator.sh/gpu-core"`
	Mem      string `json:"koordinator.sh/gpu-memory"`
	MemRatio string `json:"koordinator.sh/gpu-memory-ratio"`
}

type AsiGpuAnnotation struct {
	Minor     int            `json:"minor"`
	Resources AsiGpuResource `json:"resources"`
}

type AsiDevAnnotation struct {
	Gpu []AsiGpuAnnotation `json:"gpu"`
}

type PodInfoKubeletApiServer struct {
	config *Config
	client *kubeletClient
	info   *SystemGpuInfo
}

// transportConfig converts a client config to an appropriate transport config.
func (c *kubeletClientConfig) transportConfig() *transport.Config {
	cfg := &transport.Config{
		TLS: transport.TLSConfig{
			CAFile:   c.CAFile,
			CAData:   c.CAData,
			CertFile: c.CertFile,
			CertData: c.CertData,
			KeyFile:  c.KeyFile,
			KeyData:  c.KeyData,
		},
		BearerToken: c.BearerToken,
	}
	if !cfg.HasCA() {
		cfg.TLS.Insecure = true
	}
	return cfg
}

// KubeletClientConfig defines config parameters for the kubelet client
type kubeletClientConfig struct {
	// Address specifies the kubelet address
	Address string

	// Port
	//the default port - used if no information about Kubelet port can be found in Node.NodeStatus.DaemonEndpoints.
	Port uint

	// TLSClientConfig contains settings to enable transport layer security
	restclient.TLSClientConfig

	// Server requires Bearer authentication
	BearerToken string

	// HTTPTimeout is used by the client to timeout http requests to Kubelet.
	HTTPTimeout time.Duration
}

type kubeletClient struct {
	defaultPort uint
	host        string
	client      *http.Client
}

func (k *kubeletClient) getNodeRunningPods() (*corev1.PodList, error) {
	var resp *http.Response
	var err error
	resp, err = k.client.Get(fmt.Sprintf("https://%v:%d/pods/", k.host, k.defaultPort))
	if err != nil {
		resp, err = k.client.Get(fmt.Sprintf("http://%v:%d/pods/", k.host, k.defaultPort))
		if err != nil {
			return nil, err
		}
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	podLists := &corev1.PodList{}
	if err = json.Unmarshal(body, &podLists); err != nil {
		return nil, err
	}

	return podLists, err
}

// makeTransport creates a RoundTripper for HTTP Transport.
func makeTransport(config *kubeletClientConfig, insecureSkipTLSVerify bool) (http.RoundTripper, error) {
	// do the insecureSkipTLSVerify on the pre-transport *before* we go get a potentially cached connection.
	// transportConfig always produces a new struct pointer.
	preTLSConfig := config.transportConfig()
	if insecureSkipTLSVerify && preTLSConfig != nil {
		preTLSConfig.TLS.Insecure = true
		preTLSConfig.TLS.CAData = nil
		preTLSConfig.TLS.CAFile = ""
	}

	tlsConfig, err := transport.TLSConfigFor(preTLSConfig)
	if err != nil {
		return nil, err
	}

	rt := http.DefaultTransport
	if tlsConfig != nil {
		// If SSH Tunnel is turned on
		rt = utilnet.SetOldTransportDefaults(&http.Transport{
			TLSClientConfig: tlsConfig,
		})
	}

	return transport.HTTPWrappersForConfig(config.transportConfig(), rt)
}

func newKubeletClient(config *kubeletClientConfig) (*kubeletClient, error) {
	trans, err := makeTransport(config, true)
	if err != nil {
		return nil, err
	}

	client := &http.Client{
		Transport: trans,
		Timeout:   config.HTTPTimeout,
	}

	return &kubeletClient{
		host:        config.Address,
		defaultPort: config.Port,
		client:      client,
	}, nil
}

func buildKubeletClient(port uint) (*kubeletClient, error) {
	tokenByte, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	if err != nil {
		return nil, fmt.Errorf("in cluster mode, find token failed, error: %v", err)
	}
	tokenStr := string(tokenByte)
	token := &tokenStr

	kubeletClient, err := newKubeletClient(&kubeletClientConfig{
		Address: "0.0.0.0",
		Port:    port,
		TLSClientConfig: restclient.TLSClientConfig{
			Insecure:   true,
			ServerName: "lj-runtimekit-exporter",
			CertFile:   "",
			KeyFile:    "",
		},
		BearerToken: *token,
		HTTPTimeout: connectTimeout,
	})

	if err != nil {
		return nil, err
	}

	return kubeletClient, nil
}

func NewPodInfoKubeletApiServer(c *Config, info *SystemGpuInfo) (*PodInfoKubeletApiServer, error) {
	// use the current context in kubeconfig
	kubeletC, err := buildKubeletClient(c.ExporterKubeletPort)
	if err != nil {
		return nil, err
	}

	return &PodInfoKubeletApiServer{
		config: c,
		client: kubeletC,
		info:   info,
	}, nil
}

func (p *PodInfoKubeletApiServer) Name() string {
	return "PodInfoKubeletApiServer"
}

func (p *PodInfoKubeletApiServer) GetGpuPodInfo() ([]PodInfo, error) {
	podList, err := p.client.getNodeRunningPods()
	if err != nil {
		return nil, err
	}
	return p.parsePodInfos(podList), nil
}

func (p *PodInfoKubeletApiServer) getPodInfos(pod *corev1.Pod, gpuIdAnnoKeys []string, parseGpuIdFunc func(string) ([]string, error), resourceKeys []string) []PodInfo {
	var existKey bool = false
	var gpuIdAnnoVal string
	for _, key := range gpuIdAnnoKeys {
		if val, ok := pod.Annotations[key]; ok {
			existKey = true
			gpuIdAnnoVal = val
			break
		}
	}
	if !existKey {
		return nil
	}

	gpuIdsOrMigUUid, err := parseGpuIdFunc(gpuIdAnnoVal)
	if err != nil {
		logrus.Warningf("[%s] Failed to parse GPU ID for pod %s: %v", gpuIdAnnoKeys, pod.Name, err)
		return nil
	}

	var devicesId []string
	for _, gpuIdOrMigUUId := range gpuIdsOrMigUUid {
		if strings.HasPrefix(gpuIdOrMigUUId, "MIG") {
			devicesId = append(devicesId, gpuIdOrMigUUId)
		} else {
			devicesId = append(devicesId, getUuidByIdx(gpuIdOrMigUUId, p.info))
		}
	}

	var podInfos []PodInfo
	for _, coSpec := range pod.Spec.Containers {
		isGpuContainer, isWholeGpu := p.isGpuContainer(&coSpec, resourceKeys)
		if !isGpuContainer {
			continue
		}

		for _, coStat := range pod.Status.ContainerStatuses {
			if coStat.Name != coSpec.Name {
				continue
			}

			podInfo := PodInfo{
				Full:          isWholeGpu,
				Name:          pod.Name,
				Namespace:     pod.Namespace,
				Phase:         string(pod.Status.Phase),
				ContainerId:   parseContainerId(coStat.ContainerID),
				ContainerName: coSpec.Name,
			}

			for _, deviceId := range devicesId {
				podInfo.DeviceId = deviceId
				podInfos = append(podInfos, podInfo)
			}
			break
		}
	}
	return podInfos
}

func (p *PodInfoKubeletApiServer) isGpuContainer(coSpec *corev1.Container, gpuResourceKeys []string) (bool, bool) {
	var isWholeGpu bool = true
	for _, resourceKey := range gpuResourceKeys {
		if limit, ok := coSpec.Resources.Limits[corev1.ResourceName(resourceKey)]; ok {
			switch resourceKey {
			case EGPU_COMPUTE_RESOURCE_STR:
				isWholeGpu = limit.Value() >= 100
			case EGPU_MEMORY_RESOURCE_STR, EGPU_MIG_RESOURCE_STR:
				isWholeGpu = false
			case EGPU_GPU_NV_RESOURCE_STR, EGPU_GPU_RESOURCE_STR:
				isWholeGpu = true
			default:
				isWholeGpu = true
			}
			return true, isWholeGpu
		}
	}
	return false, false
}

func (p *PodInfoKubeletApiServer) getGpuPodInfo(pod *corev1.Pod) []PodInfo {
	methods := []struct {
		gpuIdAnnotationKeys []string
		parseGpuIndexFunc   func(string) ([]string, error)
		gpuResourceKeys     []string
	}{
		{[]string{
			KUBERGPU_SCHEDULER_ANNOTATION_MIG_UUID,
			EGPU_SCHEDULER_ANNOTATION_GPU_ID_STR,
			EGPU_SCHEDULER_ANNOTATION_GPU_ID_STR_LEGACY},
			parseGpuIdEgpu,
			[]string{
				EGPU_GPU_NV_RESOURCE_STR,
				EGPU_GPU_RESOURCE_STR,
				EGPU_COMPUTE_RESOURCE_STR,
				EGPU_MEMORY_RESOURCE_STR,
				EGPU_MIG_RESOURCE_STR,
			}},
	}

	for _, method := range methods {
		podInfos := p.getPodInfos(pod, method.gpuIdAnnotationKeys, method.parseGpuIndexFunc, method.gpuResourceKeys)
		if len(podInfos) > 0 {
			return podInfos
		}
	}
	return nil
}

func (p *PodInfoKubeletApiServer) parsePodInfos(pods *corev1.PodList) []PodInfo {
	var podInfos []PodInfo
	for _, pod := range pods.Items {
		if info := p.getGpuPodInfo(&pod); len(info) > 0 {
			podInfos = append(podInfos, info...)
		}
	}
	return podInfos
}

func parseContainerId(id string) string {
	if index := strings.Index(id, "://"); index != -1 {
		return id[index+3:]
	}
	return "unknown id"
}

func parseGpuIdEgpu(gid string) ([]string, error) {
	return strings.Split(gid, ","), nil
}

func getUuidByIdx(idStr string, info *SystemGpuInfo) string {
	idx, err := strconv.Atoi(idStr)
	if err != nil {
		logrus.Warningf("Invalid GPU ID: %s", idStr)
		return "Unknown"
	}

	for _, gpuInfo := range info.Gpus {
		if gpuInfo.GPU == idx {
			return gpuInfo.UUID
		}
	}
	return "Unknown"
}
