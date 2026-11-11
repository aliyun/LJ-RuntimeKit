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
	"encoding/json"
	"fmt"
	"hash/fnv"
)

// Checksum 是 checkpoint 的校验和类型（FNV-1a 32-bit，结果存入 uint64，对齐 kubelet checksum 行为）。
type Checksum uint64

// newChecksum 计算给定数据的 FNV-1a 32-bit checksum。
func newChecksum(data []byte) Checksum {
	h := fnv.New32a()
	h.Write(data)
	return Checksum(h.Sum32())
}

// Checkpoint 持久化所有已 Prepare 的 claim 信息。
// 采用 NVIDIA dra-driver-nvidia-gpu 的 V1/V2 嵌套模式：
//   - 顶层 Checksum 用于 V1 校验
//   - V1 用于向后兼容（降级）
//   - V2 是当前最新版本，含内部 Checksum 和 NodeBootID
type Checkpoint struct {
	Checksum Checksum      `json:"checksum"`
	V1       *CheckpointV1 `json:"v1,omitempty"`
	V2       *CheckpointV2 `json:"v2,omitempty"`
}

// MarshalCheckpoint 将 checkpoint 序列化为 JSON 字节。
// 流程：ToLatestVersion → 生成 V1 投影 → 设置双重 checksum → JSON 序列化。
// 输出同时包含 V1 和 V2，支持向后兼容。
func (cp *Checkpoint) MarshalCheckpoint() ([]byte, error) {
	cp = cp.ToLatestVersion()
	cp.V1 = cp.V2.ToV1()
	if err := cp.SetChecksumV1(); err != nil {
		return nil, fmt.Errorf("error setting V1 checksum: %w", err)
	}
	if err := cp.SetChecksumV2(); err != nil {
		return nil, fmt.Errorf("error setting V2 checksum: %w", err)
	}
	return json.Marshal(*cp)
}

// UnmarshalCheckpoint 从 JSON 字节反序列化 checkpoint。
// 不执行版本迁移，调用方需自行调用 ToLatestVersion()。
func (cp *Checkpoint) UnmarshalCheckpoint(blob []byte) error {
	return json.Unmarshal(blob, cp)
}

// VerifyChecksum 校验 checkpoint 的双重 checksum（V1 + V2）。
// 如果 V1 存在则校验 V1 checksum，如果 V2 存在则校验 V2 checksum。
// 任一失败则返回错误。
func (cp *Checkpoint) VerifyChecksum() error {
	// 没有任何版本数据的 checkpoint 无效，迫使 store 进入 legacy 迁移路径
	if cp.V1 == nil && cp.V2 == nil {
		return fmt.Errorf("checkpoint has no versioned data (V1 and V2 both nil)")
	}
	if cp.V1 != nil {
		if err := cp.VerifyChecksumV1(); err != nil {
			return err
		}
	}
	if cp.V2 != nil {
		if err := cp.VerifyChecksumV2(); err != nil {
			return err
		}
	}
	return nil
}

// SetChecksumV1 计算并设置 V1 的 checksum。
// 方法：临时置空 V2 → 清零顶层 Checksum → 序列化 → 计算 hash → 写入顶层 Checksum → 恢复 V2。
func (cp *Checkpoint) SetChecksumV1() error {
	v2 := cp.V2
	cp.V2 = nil
	defer func() { cp.V2 = v2 }()

	cp.Checksum = 0
	out, err := json.Marshal(*cp)
	if err != nil {
		return fmt.Errorf("error marshalling checkpoint for V1 checksum: %w", err)
	}
	cp.Checksum = newChecksum(out)
	return nil
}

// SetChecksumV2 计算并设置 V2 的内部 checksum。
// 方法：清零 V2.Checksum → 序列化 V2 → 计算 hash → 写入 V2.Checksum。
func (cp *Checkpoint) SetChecksumV2() error {
	if cp.V2 == nil {
		return nil
	}
	cp.V2.Checksum = 0
	out, err := json.Marshal(*cp.V2)
	if err != nil {
		return fmt.Errorf("error marshalling checkpoint V2 for checksum: %w", err)
	}
	cp.V2.Checksum = newChecksum(out)
	return nil
}

// VerifyChecksumV1 校验 V1 的 checksum。
// 重算方式与 SetChecksumV1 一致：临时置空 V2、清零 Checksum 后序列化再比较。
func (cp *Checkpoint) VerifyChecksumV1() error {
	stored := cp.Checksum

	v2 := cp.V2
	cp.V2 = nil
	defer func() { cp.V2 = v2 }()

	cp.Checksum = 0
	out, err := json.Marshal(*cp)
	if err != nil {
		return fmt.Errorf("error marshalling checkpoint for V1 checksum verification: %w", err)
	}

	cp.Checksum = stored
	if newChecksum(out) != stored {
		return fmt.Errorf("checkpoint V1 checksum mismatch (stored=%d)", stored)
	}
	return nil
}

// VerifyChecksumV2 校验 V2 的内部 checksum。
func (cp *Checkpoint) VerifyChecksumV2() error {
	if cp.V2 == nil {
		return nil
	}
	stored := cp.V2.Checksum

	cp.V2.Checksum = 0
	out, err := json.Marshal(*cp.V2)
	if err != nil {
		cp.V2.Checksum = stored
		return fmt.Errorf("error marshalling checkpoint V2 for checksum verification: %w", err)
	}
	cp.V2.Checksum = stored

	if newChecksum(out) != stored {
		return fmt.Errorf("checkpoint V2 checksum mismatch (stored=%d)", stored)
	}
	return nil
}
