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

// Package nodeagent holds the controller-side configuration and Pod templates
// for per-target, one-shot collectors.
package nodeagent

import (
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/yaml"

	"github.com/lj-runtimekit/job-controller/pkg/collectorprotocol"
)

// ConfigKey is the ConfigMap data key holding the YAML configuration.
const ConfigKey = "config.yaml"

// Config parameterizes one-shot collector Pods and their retry policy.
type Config struct {
	Image            string                        `json:"image"`
	ImagePullPolicy  corev1.PullPolicy             `json:"imagePullPolicy,omitempty"`
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`
	// SocketPath is the Runtime Kit socket on the host. Mount its directory so
	// a daemon restart recreating the socket stays visible, and pass it to the
	// collector as --socket.
	SocketPath string `json:"socketPath,omitempty"`
	// PollInterval、RPCTimeout、StartTimeout、StreamIdleTimeout 不再下发给 collector：
	// 轮询、RPC、启动等待与传输空闲超时全部使用 collector 内置默认值，
	// 保留字段只为兼容仍携带这些键的旧 ConfigMap。
	PollInterval      metav1.Duration `json:"pollInterval,omitempty"`
	RPCTimeout        metav1.Duration `json:"rpcTimeout,omitempty"`
	StartTimeout      metav1.Duration `json:"startTimeout,omitempty"`
	StreamIdleTimeout metav1.Duration `json:"streamIdleTimeout,omitempty"`
	// FinishTimeout 只参与 duration+finishTimeout 的溢出校验，collector 使用内置默认值。
	FinishTimeout metav1.Duration `json:"finishTimeout,omitempty"`
	// StopTimeout 决定 Pod 退出宽限期；collector 自身的 Stop RPC 超时使用内置默认值。
	StopTimeout metav1.Duration `json:"stopTimeout,omitempty"`
	// MaxRetries 现在只限制异常 Pod 的重建次数（不含首次尝试），
	// collector 内部安全操作重试固定使用内置默认值；指针保留显式 0。
	MaxRetries *int32 `json:"maxRetries,omitempty"`
	// RetryBackoff、MaxRetryBackoff 只决定 controller 侧重建退避。
	RetryBackoff    metav1.Duration `json:"retryBackoff,omitempty"`
	MaxRetryBackoff metav1.Duration `json:"maxRetryBackoff,omitempty"`
	// PendingTimeout 是 Pod 启动等待的告警阈值，不触发删除或重试。
	PendingTimeout metav1.Duration             `json:"pendingTimeout,omitempty"`
	Resources      corev1.ResourceRequirements `json:"resources,omitempty"`
	// Tolerations default to tolerating every taint, like a DaemonSet.
	Tolerations       []corev1.Toleration `json:"tolerations"`
	PriorityClassName string              `json:"priorityClassName,omitempty"`
	GPUResources      GPUResources        `json:"gpuResources"`
	// ActiveMonitoringImage is the detection image used by active monitoring.
	ActiveMonitoringImage string `json:"activeMonitoringImage,omitempty"`
}

// GPUResources names the container resources that mark a GPU container,
// mirroring the Runtime Kit exporter protocol ConfigMap. Empty fields default.
type GPUResources struct {
	Memory string `json:"resources.memory,omitempty"`
	Core   string `json:"resources.core,omitempty"`
	Slice  string `json:"resources.slice,omitempty"`
	GPU    string `json:"resources.gpu,omitempty"`
	Nvidia string `json:"resources.nvidia,omitempty"`
	// MIGPrefix matches every resource name with this prefix.
	MIGPrefix string `json:"resources.migPrefix,omitempty"`
}

// ParseConfig accepts YAML or marshaled JSON, then defaults and validates it.
func ParseConfig(data string) (*Config, error) {
	cfg := &Config{}
	if err := yaml.UnmarshalStrict([]byte(data), cfg); err != nil {
		return nil, fmt.Errorf("decode %s: %w", ConfigKey, err)
	}
	cfg.setDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *Config) setDefaults() {
	if c.ImagePullPolicy == "" {
		c.ImagePullPolicy = corev1.PullIfNotPresent
	}
	if c.SocketPath == "" {
		c.SocketPath = collectorprotocol.DefaultSocketPath
	}
	if c.MaxRetries == nil {
		c.MaxRetries = ptr.To(int32(collectorprotocol.DefaultMaxRetries))
	}
	// 这些字段不再下发给 collector，默认值必须与 collector 二进制默认值一致，
	// 因为它们仍决定 controller 侧行为（退出宽限期、完成等待溢出校验、Pod 重建退避）。
	for _, d := range []struct {
		field *metav1.Duration
		def   time.Duration
	}{
		{&c.PollInterval, collectorprotocol.DefaultPollInterval},
		{&c.RPCTimeout, collectorprotocol.DefaultRPCTimeout},
		{&c.StartTimeout, collectorprotocol.DefaultStartTimeout},
		{&c.StreamIdleTimeout, collectorprotocol.DefaultStreamIdleTimeout},
		{&c.FinishTimeout, collectorprotocol.DefaultFinishTimeout},
		{&c.StopTimeout, collectorprotocol.DefaultStopTimeout},
		{&c.RetryBackoff, collectorprotocol.DefaultRetryBackoff},
		{&c.MaxRetryBackoff, collectorprotocol.DefaultMaxRetryBackoff},
		{&c.PendingTimeout, 5 * time.Minute},
	} {
		if d.field.Duration == 0 {
			d.field.Duration = d.def
		}
	}
	if c.Tolerations == nil {
		c.Tolerations = []corev1.Toleration{{Operator: corev1.TolerationOpExists}}
	}
	for _, r := range []struct {
		field *string
		def   string
	}{
		{&c.GPUResources.Memory, "aliyun.com/gpu-memory"},
		{&c.GPUResources.Core, "aliyun.com/gpu-core"},
		{&c.GPUResources.Slice, "aliyun.com/gpu-slice"},
		{&c.GPUResources.GPU, "aliyun.com/gpu"},
		{&c.GPUResources.Nvidia, "nvidia.com/gpu"},
		{&c.GPUResources.MIGPrefix, "nvidia.com/mig-"},
	} {
		if *r.field == "" {
			*r.field = r.def
		}
	}
}

func (c *Config) validate() error {
	if c.Image == "" {
		return fmt.Errorf("image is required")
	}
	switch c.ImagePullPolicy {
	case corev1.PullAlways, corev1.PullIfNotPresent, corev1.PullNever:
	default:
		return fmt.Errorf("imagePullPolicy %q must be Always, IfNotPresent or Never", c.ImagePullPolicy)
	}
	for name, limit := range c.Resources.Limits {
		if limit.Sign() < 0 {
			return fmt.Errorf("resources.limits[%s] must not be negative", name)
		}
	}
	for name, request := range c.Resources.Requests {
		if request.Sign() < 0 {
			return fmt.Errorf("resources.requests[%s] must not be negative", name)
		}
		if limit, ok := c.Resources.Limits[name]; ok && request.Cmp(limit) > 0 {
			return fmt.Errorf("resources.requests[%s] must not exceed resources.limits[%s]", name, name)
		}
	}
	if !filepath.IsAbs(c.SocketPath) || filepath.Clean(c.SocketPath) != c.SocketPath || c.SocketPath == "/" {
		return fmt.Errorf("socketPath must be a clean absolute non-root path, got %q", c.SocketPath)
	}
	if c.MaxRetries == nil || *c.MaxRetries < 0 || *c.MaxRetries > 10 {
		return fmt.Errorf("maxRetries must be in range 0..10")
	}
	for name, d := range map[string]time.Duration{
		"pollInterval": c.PollInterval.Duration, "rpcTimeout": c.RPCTimeout.Duration,
		"startTimeout": c.StartTimeout.Duration, "streamIdleTimeout": c.StreamIdleTimeout.Duration,
		"finishTimeout": c.FinishTimeout.Duration, "stopTimeout": c.StopTimeout.Duration,
		"retryBackoff": c.RetryBackoff.Duration, "maxRetryBackoff": c.MaxRetryBackoff.Duration,
		"pendingTimeout": c.PendingTimeout.Duration,
	} {
		if d <= 0 {
			return fmt.Errorf("%s must be positive", name)
		}
	}
	if c.RetryBackoff.Duration > c.MaxRetryBackoff.Duration {
		return fmt.Errorf("retryBackoff must not exceed maxRetryBackoff")
	}
	if c.StopTimeout.Duration > time.Duration(math.MaxInt64)-terminationGraceSlop*time.Second {
		return fmt.Errorf("collector termination grace duration overflow")
	}
	return nil
}

// ValidateDuration 防止 collector 内部阶段超时相加溢出。
// 各阶段独立计时，不计算或限制 Pod 的总运行时间。
func (c *Config) ValidateDuration(duration time.Duration) error {
	if c == nil {
		return fmt.Errorf("collector config is required")
	}
	if err := c.validate(); err != nil {
		return err
	}
	if duration <= 0 {
		return fmt.Errorf("duration must be positive")
	}
	if duration > time.Duration(math.MaxInt64)-c.FinishTimeout.Duration {
		return fmt.Errorf("collector stage timeout duration overflow")
	}
	return nil
}

func ceilSeconds(d time.Duration) int64 {
	n := int64(d / time.Second)
	if d%time.Second != 0 {
		n++
	}
	return n
}

// IsGPUResource reports whether a container resource selects it for profiling.
func (c *Config) IsGPUResource(name corev1.ResourceName) bool {
	r := c.GPUResources
	switch string(name) {
	case r.Memory, r.Core, r.Slice, r.GPU, r.Nvidia:
		return true
	}
	return r.MIGPrefix != "" && strings.HasPrefix(string(name), r.MIGPrefix)
}
