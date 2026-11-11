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

// legacyCheckpointV0 是最早版本的扁平 checkpoint 格式（无 version/nodeBootID 字段）。
// Checksum 使用 FNV-64a。
type legacyCheckpointV0 struct {
	Checksum       uint64              `json:"checksum"`
	PreparedClaims PreparedClaimsByUID `json:"preparedClaims"`
}

// legacyCheckpointV1 是上一版本的扁平 checkpoint 格式（含 version/nodeBootID 字段）。
// Checksum 使用 FNV-64a。
type legacyCheckpointV1 struct {
	Checksum       uint64              `json:"checksum"`
	Version        int                 `json:"version"`
	NodeBootID     string              `json:"nodeBootID"`
	PreparedClaims PreparedClaimsByUID `json:"preparedClaims"`
}

// tryUnmarshalLegacy 尝试将数据按旧格式反序列化并迁移为新的 V1/V2 嵌套 Checkpoint。
// 优先尝试格式 B（legacyCheckpointV1，含 version 字段可区分），
// 再尝试格式 A（legacyCheckpointV0）。两者均失败则返回错误。
func tryUnmarshalLegacy(data []byte) (*Checkpoint, error) {
	// 尝试格式 B：legacyCheckpointV1（含 version/nodeBootID）
	if cp, err := tryLegacyV1(data); err == nil {
		return cp, nil
	}

	// 尝试格式 A：legacyCheckpointV0（无 version/nodeBootID）
	if cp, err := tryLegacyV0(data); err == nil {
		return cp, nil
	}

	return nil, fmt.Errorf("unable to parse checkpoint in any known legacy format")
}

// tryLegacyV1 尝试按 legacyCheckpointV1 格式解析并校验。
func tryLegacyV1(data []byte) (*Checkpoint, error) {
	var legacy legacyCheckpointV1
	if err := json.Unmarshal(data, &legacy); err != nil {
		return nil, err
	}

	// 校验：version 字段 > 0 表明这是 V1 格式
	if legacy.Version == 0 {
		return nil, fmt.Errorf("not a legacyV1 checkpoint: version is 0")
	}

	// FNV-64a 校验
	stored := legacy.Checksum
	legacy.Checksum = 0
	verifyData, err := json.Marshal(legacy)
	if err != nil {
		return nil, err
	}
	if fnv64(verifyData) != stored {
		return nil, fmt.Errorf("legacyV1 checkpoint checksum mismatch")
	}

	// 迁移到新格式
	claims := legacy.PreparedClaims
	if claims == nil {
		claims = make(PreparedClaimsByUID)
	}
	return &Checkpoint{
		V2: &CheckpointV2{
			PreparedClaims: claims,
			NodeBootID:     legacy.NodeBootID,
		},
	}, nil
}

// tryLegacyV0 尝试按 legacyCheckpointV0 格式解析并校验。
func tryLegacyV0(data []byte) (*Checkpoint, error) {
	var legacy legacyCheckpointV0
	if err := json.Unmarshal(data, &legacy); err != nil {
		return nil, err
	}

	// FNV-64a 校验
	stored := legacy.Checksum
	legacy.Checksum = 0
	verifyData, err := json.Marshal(legacy)
	if err != nil {
		return nil, err
	}
	if fnv64(verifyData) != stored {
		return nil, fmt.Errorf("legacyV0 checkpoint checksum mismatch")
	}

	// 迁移到新格式（NodeBootID 留空，由 NewDeviceState 处理）
	claims := legacy.PreparedClaims
	if claims == nil {
		claims = make(PreparedClaimsByUID)
	}
	return &Checkpoint{
		V2: &CheckpointV2{
			PreparedClaims: claims,
		},
	}, nil
}

// fnv64 计算 FNV-64a hash，仅供旧格式校验使用。
func fnv64(data []byte) uint64 {
	h := fnv.New64a()
	h.Write(data)
	return h.Sum64()
}
