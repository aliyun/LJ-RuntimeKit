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

package nodeagent

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func testConfig(t *testing.T) *Config {
	t.Helper()
	cfg, err := ParseConfig("image: collector:test\n")
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	return cfg
}

func TestParseConfigDefaults(t *testing.T) {
	want := &Config{
		Image: "collector:test", ImagePullPolicy: corev1.PullIfNotPresent,
		SocketPath:        "/run/amp/eKitC/xdm/sockets/dp2mgr.sock",
		PollInterval:      metav1.Duration{Duration: 2 * time.Second},
		RPCTimeout:        metav1.Duration{Duration: 15 * time.Second},
		StartTimeout:      metav1.Duration{Duration: 2 * time.Minute},
		StreamIdleTimeout: metav1.Duration{Duration: 5 * time.Minute},
		FinishTimeout:     metav1.Duration{Duration: time.Minute},
		StopTimeout:       metav1.Duration{Duration: 15 * time.Second},
		MaxRetries:        ptr.To(int32(3)),
		RetryBackoff:      metav1.Duration{Duration: 10 * time.Second},
		MaxRetryBackoff:   metav1.Duration{Duration: 2 * time.Minute},
		PendingTimeout:    metav1.Duration{Duration: 5 * time.Minute},
		Tolerations:       []corev1.Toleration{{Operator: corev1.TolerationOpExists}},
		GPUResources: GPUResources{
			Memory: "aliyun.com/gpu-memory", Core: "aliyun.com/gpu-core", Slice: "aliyun.com/gpu-slice",
			GPU: "aliyun.com/gpu", Nvidia: "nvidia.com/gpu", MIGPrefix: "nvidia.com/mig-",
		},
	}
	if got := testConfig(t); !reflect.DeepEqual(got, want) {
		t.Fatalf("defaults = %#v, want %#v", got, want)
	}
	cfg, err := ParseConfig(`image: collector:test
pollInterval: 0s
rpcTimeout: 0s
startTimeout: 0s
streamIdleTimeout: 0s
finishTimeout: 0s
stopTimeout: 0s
retryBackoff: 0s
maxRetryBackoff: 0s
pendingTimeout: 0s
gpuResources:
  resources.memory: ""
`)
	if err != nil || !reflect.DeepEqual(cfg, want) {
		t.Fatalf("zero duration defaults = %#v, %v", cfg, err)
	}
}

func TestParseConfigOverridesAndJSON(t *testing.T) {
	cfg, err := ParseConfig(`image: registry.example/collector:v2
imagePullPolicy: Always
imagePullSecrets:
- name: registry-secret
socketPath: /run/custom/runtime.sock
pollInterval: 500ms
rpcTimeout: 3s
startTimeout: 20s
streamIdleTimeout: 30s
finishTimeout: 40s
stopTimeout: 5s
maxRetries: 0
retryBackoff: 5s
maxRetryBackoff: 1m
pendingTimeout: 2m
resources:
  requests:
    cpu: 100m
  limits:
    memory: 256Mi
tolerations:
- key: dedicated
  operator: Equal
  value: profiling
  effect: NoSchedule
priorityClassName: profiling-high
gpuResources:
  resources.memory: example.com/vgpu-memory
  resources.core: example.com/vgpu-core
  resources.slice: example.com/vgpu-slice
  resources.gpu: example.com/vgpu
  resources.nvidia: example.com/nv-gpu
  resources.migPrefix: example.com/mig-
`)
	if err != nil {
		t.Fatal(err)
	}
	want := &Config{
		Image: "registry.example/collector:v2", ImagePullPolicy: corev1.PullAlways,
		ImagePullSecrets:  []corev1.LocalObjectReference{{Name: "registry-secret"}},
		SocketPath:        "/run/custom/runtime.sock",
		PollInterval:      metav1.Duration{Duration: 500 * time.Millisecond},
		RPCTimeout:        metav1.Duration{Duration: 3 * time.Second},
		StartTimeout:      metav1.Duration{Duration: 20 * time.Second},
		StreamIdleTimeout: metav1.Duration{Duration: 30 * time.Second},
		FinishTimeout:     metav1.Duration{Duration: 40 * time.Second},
		StopTimeout:       metav1.Duration{Duration: 5 * time.Second},
		MaxRetries:        ptr.To(int32(0)),
		RetryBackoff:      metav1.Duration{Duration: 5 * time.Second},
		MaxRetryBackoff:   metav1.Duration{Duration: time.Minute},
		PendingTimeout:    metav1.Duration{Duration: 2 * time.Minute},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
			Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("256Mi")},
		},
		Tolerations:       []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "profiling", Effect: corev1.TaintEffectNoSchedule}},
		PriorityClassName: "profiling-high",
		GPUResources: GPUResources{
			Memory: "example.com/vgpu-memory", Core: "example.com/vgpu-core", Slice: "example.com/vgpu-slice",
			GPU: "example.com/vgpu", Nvidia: "example.com/nv-gpu", MIGPrefix: "example.com/mig-",
		},
	}
	if !reflect.DeepEqual(cfg, want) {
		t.Fatalf("overrides = %#v, want %#v", cfg, want)
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip, err := ParseConfig(string(b))
	if err != nil || !reflect.DeepEqual(roundTrip, cfg) {
		t.Fatalf("frozen JSON round trip = %#v, %v", roundTrip, err)
	}
	cfg, err = ParseConfig("image: collector:test\ntolerations: []\n")
	if err != nil || cfg.Tolerations == nil || len(cfg.Tolerations) != 0 {
		t.Fatalf("explicit empty tolerations = %#v, %v", cfg, err)
	}
	b, err = json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip, err = ParseConfig(string(b))
	if err != nil || roundTrip.Tolerations == nil || len(roundTrip.Tolerations) != 0 {
		t.Fatalf("empty tolerations lost in frozen JSON: %#v, %v", roundTrip, err)
	}
}

func TestParseConfigStrict(t *testing.T) {
	for _, data := range []string{
		"imag: typo", "resources:\n  request: {}", "imagePullSecrets:\n- typo: secret",
		"tolerations:\n- typo: value", "maxRetries: 1\nmaxRetries: 2", "resources: [",
		"rpcTimeout: yesterday", "resources:\n  requests:\n    cpu: not-a-quantity",
		"port: 9090", "hostDataDir: /data", "dataRetention: 2h", "exportTimeout: 10m", "exporterResources: {}",
	} {
		t.Run(data, func(t *testing.T) {
			cfg, err := ParseConfig("image: collector:test\n" + data + "\n")
			if cfg != nil || err == nil || !strings.Contains(err.Error(), ConfigKey) {
				t.Fatalf("ParseConfig = %#v, %v; want strict decode error", cfg, err)
			}
		})
	}
}

func TestParseConfigRejectsRenamedStreamGrace(t *testing.T) {
	for _, data := range []string{"image: collector:test\nstreamGrace: 5m\n", `{"image":"collector:test","streamGrace":"5m0s"}`} {
		if cfg, err := ParseConfig(data); cfg != nil || err == nil || !strings.Contains(err.Error(), `unknown field "streamGrace"`) {
			t.Fatalf("streamGrace was renamed to streamIdleTimeout and must be rejected: %#v, %v", cfg, err)
		}
	}
}

func TestParseConfigRejectsObsoleteExportFormat(t *testing.T) {
	for _, format := range []string{"json", "protobuf", "chrome-trace", ""} {
		for _, tc := range []struct {
			name, data string
		}{
			{"YAML", fmt.Sprintf("image: collector:test\nexportFormat: %q\n", format)},
			{"JSON", fmt.Sprintf(`{"image":"collector:test","exportFormat":%q}`, format)},
		} {
			t.Run(tc.name+"/"+format, func(t *testing.T) {
				cfg, err := ParseConfig(tc.data)
				if cfg != nil || err == nil || !strings.Contains(err.Error(), `unknown field "exportFormat"`) {
					t.Fatalf("obsolete exportFormat must be rejected as an unknown field: %#v, %v", cfg, err)
				}
			})
		}
	}
}

func TestParseConfigValidation(t *testing.T) {
	for _, data := range []string{"", "{}", "image: ''"} {
		if cfg, err := ParseConfig(data); cfg != nil || err == nil || !strings.Contains(err.Error(), "image is required") {
			t.Errorf("ParseConfig(%q) = %#v, %v", data, cfg, err)
		}
	}
	for _, n := range []int32{-1, 0, 3, 10, 11} {
		cfg, err := ParseConfig(fmt.Sprintf("image: collector:test\nmaxRetries: %d\n", n))
		if n >= 0 && n <= 10 {
			if err != nil || *cfg.MaxRetries != n {
				t.Errorf("valid maxRetries %d = %#v, %v", n, cfg, err)
			}
		} else if cfg != nil || err == nil || !strings.Contains(err.Error(), "maxRetries") {
			t.Errorf("invalid maxRetries %d = %#v, %v", n, cfg, err)
		}
	}
	for _, p := range []string{"relative/path", "/", "/var/../data", "/var/./data", "/var//data", "/var/data/"} {
		cfg, err := ParseConfig(fmt.Sprintf("image: collector:test\nsocketPath: %q\n", p))
		if cfg != nil || err == nil || !strings.Contains(err.Error(), "socketPath") {
			t.Errorf("invalid socketPath %q = %#v, %v", p, cfg, err)
		}
	}
	for _, suffix := range []string{"retryBackoff: 3m", "maxRetryBackoff: 5s"} {
		if cfg, err := ParseConfig("image: collector:test\n" + suffix); cfg != nil || err == nil || !strings.Contains(err.Error(), "retryBackoff") {
			t.Errorf("backoff ordering = %#v, %v", cfg, err)
		}
	}
	if _, err := ParseConfig("image: collector:test\nretryBackoff: 10s\nmaxRetryBackoff: 10s"); err != nil {
		t.Fatalf("equal backoffs: %v", err)
	}
}

func TestConfigPodValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config string
	}{
		{"pull policy", "imagePullPolicy: never"},
		{"negative request", "resources:\n  requests:\n    cpu: -1"},
		{"negative limit", "resources:\n  limits:\n    memory: -1Mi"},
		{"request exceeds limit", "resources:\n  requests:\n    cpu: 2\n  limits:\n    cpu: 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if cfg, err := ParseConfig("image: collector:test\n" + tc.config); cfg != nil || err == nil {
				t.Fatalf("accepted invalid Pod configuration: %#v, %v", cfg, err)
			}
		})
	}
	for _, policy := range []corev1.PullPolicy{corev1.PullAlways, corev1.PullIfNotPresent, corev1.PullNever} {
		if _, err := ParseConfig("image: collector:test\nimagePullPolicy: " + string(policy)); err != nil {
			t.Fatalf("rejected valid pull policy %s: %v", policy, err)
		}
	}
	if _, err := ParseConfig("image: collector:test\nresources:\n  requests:\n    cpu: 1000m\n  limits:\n    cpu: 1"); err != nil {
		t.Fatalf("rejected equivalent request and limit: %v", err)
	}
}

func TestConfigDurationValidation(t *testing.T) {
	for _, f := range []struct {
		name string
		get  func(*Config) *metav1.Duration
	}{
		{"pollInterval", func(c *Config) *metav1.Duration { return &c.PollInterval }},
		{"rpcTimeout", func(c *Config) *metav1.Duration { return &c.RPCTimeout }},
		{"startTimeout", func(c *Config) *metav1.Duration { return &c.StartTimeout }},
		{"streamIdleTimeout", func(c *Config) *metav1.Duration { return &c.StreamIdleTimeout }},
		{"finishTimeout", func(c *Config) *metav1.Duration { return &c.FinishTimeout }},
		{"stopTimeout", func(c *Config) *metav1.Duration { return &c.StopTimeout }},
		{"retryBackoff", func(c *Config) *metav1.Duration { return &c.RetryBackoff }},
		{"maxRetryBackoff", func(c *Config) *metav1.Duration { return &c.MaxRetryBackoff }},
		{"pendingTimeout", func(c *Config) *metav1.Duration { return &c.PendingTimeout }},
	} {
		t.Run(f.name, func(t *testing.T) {
			if cfg, err := ParseConfig("image: collector:test\n" + f.name + ": -1s"); cfg != nil || err == nil || !strings.Contains(err.Error(), f.name) {
				t.Fatalf("negative duration = %#v, %v", cfg, err)
			}
			cfg := testConfig(t)
			f.get(cfg).Duration = 0
			if err := cfg.validate(); err == nil || !strings.Contains(err.Error(), f.name) {
				t.Fatalf("zero duration = %v", err)
			}
		})
	}
}

func TestValidateDuration(t *testing.T) {
	cfg := testConfig(t)
	if err := cfg.ValidateDuration(60*time.Second + time.Nanosecond); err != nil {
		t.Fatalf("valid duration: %v", err)
	}
	for _, d := range []time.Duration{0, -time.Second, time.Duration(math.MaxInt64)} {
		if err := cfg.ValidateDuration(d); err == nil {
			t.Errorf("accepted invalid duration %s", d)
		}
	}
	// 各阶段独立计时，不能再用串行阶段总和限制合法时长；传输只有空闲超时，不与 duration 相加。
	cfg.PendingTimeout.Duration = time.Duration(math.MaxInt64)
	cfg.StreamIdleTimeout.Duration = time.Duration(math.MaxInt64)
	maxDuration := time.Duration(math.MaxInt64) - cfg.FinishTimeout.Duration
	if err := cfg.ValidateDuration(maxDuration); err != nil {
		t.Fatalf("maximum independent stage budget: %v", err)
	}
	if err := cfg.ValidateDuration(maxDuration + 1); err == nil || !strings.Contains(err.Error(), "overflow") {
		t.Fatalf("finish budget overflow: %v", err)
	}
	cfg = testConfig(t)
	cfg.FinishTimeout.Duration = time.Duration(math.MaxInt64)
	if err := cfg.ValidateDuration(time.Nanosecond); err == nil || !strings.Contains(err.Error(), "overflow") {
		t.Fatalf("finish budget overflow: %v", err)
	}
	cfg = testConfig(t)
	cfg.Image = ""
	if err := cfg.ValidateDuration(time.Second); err == nil {
		t.Fatal("invalid config accepted")
	}
	cfg = nil
	if err := cfg.ValidateDuration(time.Second); err == nil {
		t.Fatal("nil config accepted")
	}
}

func TestTerminationGraceDurationValidation(t *testing.T) {
	cfg := testConfig(t)
	cfg.StopTimeout.Duration = time.Duration(math.MaxInt64) - terminationGraceSlop*time.Second
	if err := cfg.validate(); err != nil {
		t.Fatalf("maximum termination grace: %v", err)
	}
	if seconds := ceilSeconds(cfg.StopTimeout.Duration + terminationGraceSlop*time.Second); seconds != 9223372037 {
		t.Fatalf("rounded maximum termination grace = %d", seconds)
	}
	cfg.StopTimeout.Duration++
	if err := cfg.validate(); err == nil || !strings.Contains(err.Error(), "overflow") {
		t.Fatalf("termination grace overflow: %v", err)
	}
}

func TestIsGPUResource(t *testing.T) {
	for _, tc := range []struct {
		name             string
		config           string
		matches, rejects []corev1.ResourceName
	}{
		{
			"default", "",
			[]corev1.ResourceName{"aliyun.com/gpu-memory", "aliyun.com/gpu-core", "aliyun.com/gpu-slice", "aliyun.com/gpu", "nvidia.com/gpu", "nvidia.com/mig-1g.10gb", "nvidia.com/mig-3g.40gb"},
			[]corev1.ResourceName{"cpu", "memory", "amd.com/gpu", "nvidia.com/GPU", "aliyun.com/gpu-mem", "nvidia.com/mig", "example.com/nvidia.com/mig-1g.10gb"},
		},
		{
			"partial override", "gpuResources:\n  resources.nvidia: example.com/gpu\n  resources.migPrefix: example.com/mig-\n",
			[]corev1.ResourceName{"example.com/gpu", "example.com/mig-2g", "aliyun.com/gpu-memory", "aliyun.com/gpu"},
			[]corev1.ResourceName{"nvidia.com/gpu", "nvidia.com/mig-1g.10gb"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := ParseConfig("image: collector:test\n" + tc.config)
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range tc.matches {
				if !cfg.IsGPUResource(name) {
					t.Errorf("IsGPUResource(%q) = false", name)
				}
			}
			for _, name := range tc.rejects {
				if cfg.IsGPUResource(name) {
					t.Errorf("IsGPUResource(%q) = true", name)
				}
			}
		})
	}
	if (&Config{}).IsGPUResource("example.com/anything") {
		t.Error("unparsed config with empty MIG prefix matched every resource")
	}
	if _, err := ParseConfig("image: collector:test\ngpuResources:\n  memory: example.com/gpu-memory\n"); err == nil {
		t.Error("key without resources. prefix accepted")
	}
}

func TestParseConfigActiveMonitoringImage(t *testing.T) {
	cfg, err := ParseConfig(`image: collector:test
socketPath: /run/top/dp2mgr.sock
activeMonitoringImage: registry.example/diag:v2
`)
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if cfg.ActiveMonitoringImage != "registry.example/diag:v2" {
		t.Fatalf("activeMonitoringImage = %q", cfg.ActiveMonitoringImage)
	}

	// The key is optional and stays empty: the ActiveMonitoring controller
	// applies its own default image.
	cfg, err = ParseConfig("image: collector:test\n")
	if err != nil {
		t.Fatalf("ParseConfig without activeMonitoringImage: %v", err)
	}
	if cfg.ActiveMonitoringImage != "" {
		t.Fatalf("activeMonitoringImage = %q, want empty", cfg.ActiveMonitoringImage)
	}

	// A nested activeMonitoring section must be rejected by the strict decoder.
	if _, err := ParseConfig("image: collector:test\nactiveMonitoring:\n  image: registry.example/diag:v2\n"); err == nil {
		t.Fatal("nested activeMonitoring section accepted")
	}
}
