/*
Copyright (c) 2026 Alibaba Cloud.

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

package computedomain

import (
	"k8s.io/klog/v2"
)

// cdiVendor 是生成 CDI 设备 ID 时使用的 vendor 前缀
// (形如: <vendor>=<class>=<device>)。
const cdiVendor = "compute-domain.aliyun.com"

// CDIHandler 负责生成 / 删除 CDI 规格文件,并组装 CDI 设备 ID。
//
// TODO(CDI): 整个 CDI 部分按需求留空,由后续接入填充。
// 建议参考 dra-driver-nvidia-gpu 的 compute-domain-kubelet-plugin/cdi.go:
//   - 引入 tags.cncf.io/container-device-interface/pkg/cdi
//   - NewCDIHandler(...) 按 WithCDIRoot / WithVendor 等 Option 初始化
//   - CreateStandardDeviceSpecFile: 为 channel-0 生成基础 CDI spec
//     (例如注入 /dev/nvidia-caps-imex-channels/channel0 设备节点)
//   - CreateClaimSpecFile: 为单个 claim 生成 CDI spec
//   - GetStandardDevice / GetClaimDevice: 返回
//     "<vendor>=<class>=<device>" 形式的 CDI 设备 ID,填入
//     Device.CDIDeviceIDs,由 kubelet 注入容器
type CDIHandler struct {
	cdiRoot string
	vendor  string
}

// CDIOption 是 NewCDIHandler 的配置项。
type CDIOption func(*CDIHandler)

// WithCDIRoot 设置 CDI 规格文件输出目录。
func WithCDIRoot(root string) CDIOption {
	return func(c *CDIHandler) {
		c.cdiRoot = root
	}
}

// WithVendor 设置 CDI vendor 名称。
func WithVendor(vendor string) CDIOption {
	return func(c *CDIHandler) {
		c.vendor = vendor
	}
}

// NewCDIHandler 创建 CDI 处理器。
func NewCDIHandler(opts ...CDIOption) (*CDIHandler, error) {
	c := &CDIHandler{
		cdiRoot: "/etc/cdi",
		vendor:  cdiVendor,
	}
	for _, opt := range opts {
		opt(c)
	}

	// TODO(CDI): 在此初始化 CDI 库,例如:
	//
	//	cdiapi.GetRegistry(cdiapi.WithSpecDirs(c.cdiRoot))
	//
	// 并做好 vendor 合法性校验等。

	return c, nil
}

// CreateStandardDeviceSpecFile 为所有 allocatable 设备生成基础 CDI spec。
func (c *CDIHandler) CreateStandardDeviceSpecFile(devices AllocatableDevices) error {
	// TODO(CDI): 生成基础 CDI spec 文件(如 compute-domain-aliyun-com.yaml),
	// 描述 channel-0 等标准设备的注入方式(设备节点 / 挂载 / 环境变量)。
	_ = devices
	klog.V(1).Info("TODO(CDI): CreateStandardDeviceSpecFile is not implemented yet")
	return nil
}

// CreateClaimSpecFile 为某个 claim 已 Prepare 的设备生成 CDI spec。
func (c *CDIHandler) CreateClaimSpecFile(claimUID string, devices PreparedDeviceList) error {
	// TODO(CDI): 生成 <claimUID>.yaml 形式的 claim 级 CDI spec。
	_ = claimUID
	_ = devices
	klog.V(1).Info("TODO(CDI): CreateClaimSpecFile is not implemented yet")
	return nil
}

// DeleteClaimSpecFileIfExists 删除某个 claim 的 CDI spec(幂等)。
func (c *CDIHandler) DeleteClaimSpecFileIfExists(claimUID string) error {
	// TODO(CDI): 删除 claim 级 CDI spec 文件。
	_ = claimUID
	klog.V(1).Info("TODO(CDI): DeleteClaimSpecFileIfExists is not implemented yet")
	return nil
}

// GetStandardDevice 返回标准设备的 CDI 设备 ID(空串表示没有)。
func (c *CDIHandler) GetStandardDevice(d *AllocatableDevice) string {
	// TODO(CDI): 返回形如 "compute-domain.aliyun.com=imex=channel-0" 的 ID。
	_ = d
	return ""
}

// GetClaimDevice 返回 claim 级设备的 CDI 设备 ID(空串表示没有)。
func (c *CDIHandler) GetClaimDevice(claimUID string, d *AllocatableDevice) string {
	// TODO(CDI): 返回形如 "compute-domain.aliyun.com=imex-<claimUID>=channel-0" 的 ID。
	_ = claimUID
	_ = d
	return ""
}
