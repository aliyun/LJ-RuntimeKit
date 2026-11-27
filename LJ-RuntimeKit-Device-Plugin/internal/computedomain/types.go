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
	"fmt"

	resourcev1 "k8s.io/api/resource/v1"
)

const (
	// DriverName 是本 DRA driver 的名字,所有 ResourceSlice / DeviceClass /
	// ResourceClaim 的 opaque 参数都通过它关联到本插件。
	DriverName = "compute-domain.aliyun.com"

	// DriverPluginCheckpointFileBasename 是 kubelet 插件 checkpoint 文件名。
	DriverPluginCheckpointFileBasename = "checkpoint.json"

	// DriverPrepUprepFlockFileName 用于保证 nodePrepareResource() /
	// nodeUnprepareResource() 在节点级别上互斥执行。
	DriverPrepUprepFlockFileName = "pu.lock"

	// DRASocketFileName 是 kubelet 用来访问 DRA gRPC 服务的 socket 文件名。
	DRASocketFileName = "dra.sock"

	// RegistrationSocketFileSuffix 是放在 kubelet plugins_registry 目录下、
	// 供 kubelet plugin watcher 发现本插件的 registration socket 后缀。
	RegistrationSocketFileSuffix = "-reg.sock"
)

const ClaimTaskFileName = "claim-tasks.json"

const (
	ComputeDomainChannelType = "channel"
	UnknownDeviceType        = "unknown"
)

// Clique CR 上的标签键，用于 informer 过滤和 Clique 标识。
const (
	CliqueLabelDomain = "aliyun.com/clique-domain"
	CliqueLabelClique = "aliyun.com/clique"
)

// SentinelIPAddress 用于 DNS 映射中尚未注册 daemon 的 slot。
const SentinelIPAddress = "127.0.0.2"

// DNSNameMapping 表示一个 IMEX DNS 名称到 IP 地址的映射条目。
type DNSNameMapping struct {
	Index    int
	IP       string
	DomainID string
}

// ComputeDomainStatusReady / NotReady 是 Clique 中 daemon 的就绪状态。
const (
	ComputeDomainStatusReady    = "Ready"
	ComputeDomainStatusNotReady = "NotReady"
)

func ResourceClaimToString(rc *resourcev1.ResourceClaim) string {
	return fmt.Sprintf("%s/%s:%s", rc.Namespace, rc.Name, rc.UID)
}
