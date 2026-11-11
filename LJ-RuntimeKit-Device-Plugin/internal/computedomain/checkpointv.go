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
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ClaimCheckpointState 表示 claim 在 checkpoint 中的状态。
type ClaimCheckpointState = string

const (
	ClaimCheckpointStatePrepareStarted   ClaimCheckpointState = "PrepareStarted"
	ClaimCheckpointStatePrepareCompleted ClaimCheckpointState = "PrepareCompleted"
	ClaimCheckpointStatePrepareAborted   ClaimCheckpointState = "PrepareAborted"
)

// PreparedClaimV1 是 V1 版本的 claim，不含状态跟踪字段。
type PreparedClaimV1 struct {
	Status          resourceapi.ResourceClaimStatus `json:"status,omitempty"`
	PreparedDevices PreparedDeviceList              `json:"preparedDevices,omitempty"`
}

// PreparedClaimsByUIDV1 是 V1 版本的 claim map。
type PreparedClaimsByUIDV1 map[string]PreparedClaimV1

// PreparedClaimV2 是 V2 版本（当前最新）的 claim，包含完整的状态跟踪字段。
type PreparedClaimV2 struct {
	CheckpointState ClaimCheckpointState            `json:"checkpointState"`
	Status          resourceapi.ResourceClaimStatus `json:"status,omitempty"`
	Name            string                          `json:"name,omitempty"`
	Namespace       string                          `json:"namespace,omitempty"`
	PreparedDevices PreparedDeviceList              `json:"preparedDevices,omitempty"`
	AbortedAt       *metav1.Time                    `json:"abortedAt,omitempty"`
}

// PreparedClaimsByUIDV2 是 V2 版本的 claim map。
type PreparedClaimsByUIDV2 map[string]PreparedClaimV2

// 最新版本别名，所有业务代码使用这些名字。
type PreparedClaim = PreparedClaimV2
type PreparedClaimsByUID = PreparedClaimsByUIDV2

// CheckpointV1 是 V1 版本的 checkpoint 嵌套结构（不含 boot ID 和内部 checksum）。
type CheckpointV1 struct {
	PreparedClaims PreparedClaimsByUIDV1 `json:"preparedClaims,omitempty"`
}

// CheckpointV2 是 V2 版本的 checkpoint 嵌套结构（含独立 checksum 和 NodeBootID）。
type CheckpointV2 struct {
	Checksum       Checksum            `json:"checksum"`
	PreparedClaims PreparedClaimsByUID `json:"preparedClaims,omitempty"`
	NodeBootID     string              `json:"nodeBootID,omitempty"`
}

// ToLatestVersion 将 checkpoint 转换为最新版本（V2）。
// 优先级：V2 > V1.ToV2() > 空 V2。确保返回的 V2.PreparedClaims 非 nil。
func (cp *Checkpoint) ToLatestVersion() *Checkpoint {
	latest := &Checkpoint{}
	switch {
	case cp.V2 != nil:
		latest.V2 = cp.V2
	case cp.V1 != nil:
		latest.V2 = cp.V1.ToV2()
	default:
		latest.V2 = &CheckpointV2{}
	}
	if latest.V2.PreparedClaims == nil {
		latest.V2.PreparedClaims = make(PreparedClaimsByUID)
	}
	return latest
}

// ToV2 将 V1 checkpoint 转换为 V2。
// 所有 V1 claim 标记为 PrepareCompleted（V1 不含状态字段，默认已完成）。
// NodeBootID 留空（V1 没有此字段）。
func (v1 *CheckpointV1) ToV2() *CheckpointV2 {
	v2 := &CheckpointV2{
		PreparedClaims: make(PreparedClaimsByUID),
	}
	for claimUID, v1Claim := range v1.PreparedClaims {
		v2.PreparedClaims[claimUID] = PreparedClaimV2{
			CheckpointState: ClaimCheckpointStatePrepareCompleted,
			Status:          v1Claim.Status,
			PreparedDevices: v1Claim.PreparedDevices,
		}
	}
	return v2
}

// ToV1 将 V2 checkpoint 降级为 V1（用于 MarshalCheckpoint 时的向后兼容）。
// 仅保留 PrepareCompleted 状态的 claim，丢弃 state/name/namespace。
func (v2 *CheckpointV2) ToV1() *CheckpointV1 {
	v1 := &CheckpointV1{
		PreparedClaims: make(PreparedClaimsByUIDV1),
	}
	for claimUID, v2Claim := range v2.PreparedClaims {
		if v2Claim.CheckpointState != ClaimCheckpointStatePrepareCompleted {
			continue
		}
		v1.PreparedClaims[claimUID] = PreparedClaimV1{
			Status:          v2Claim.Status,
			PreparedDevices: v2Claim.PreparedDevices,
		}
	}
	return v1
}

// GetNodeBootID 返回 checkpoint 中存储的 NodeBootID。
func (cp *Checkpoint) GetNodeBootID() string {
	if cp.V2 != nil {
		return cp.V2.NodeBootID
	}
	return ""
}
