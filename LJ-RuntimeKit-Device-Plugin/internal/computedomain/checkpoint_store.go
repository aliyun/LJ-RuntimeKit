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
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// checkpointStore 管理 checkpoint 文件的读写，对齐 kubelet
// checkpointmanager.CheckpointManager 的接口契约。
// 所有操作持有进程内 mutex，与 NVIDIA compute-domain 模式一致
// （不使用 cplock，依赖 driver 的 pulock 做跨进程互斥）。
type checkpointStore struct {
	dir   string
	mutex sync.Mutex
}

// newCheckpointStore 创建一个 checkpointStore。
func newCheckpointStore(dir string) *checkpointStore {
	return &checkpointStore{dir: dir}
}

// CreateCheckpoint 序列化并原子写入 checkpoint 文件。
// 使用 tmpFile + fsync + rename 模式保证崩溃安全。
func (s *checkpointStore) CreateCheckpoint(key string, cp *Checkpoint) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	data, err := cp.MarshalCheckpoint()
	if err != nil {
		return fmt.Errorf("error marshalling checkpoint: %w", err)
	}

	filePath := filepath.Join(s.dir, key)
	tmpPath := filePath + ".tmp"

	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("error creating temp checkpoint file: %w", err)
	}

	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("error writing temp checkpoint file: %w", err)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("error syncing temp checkpoint file: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("error closing temp checkpoint file: %w", err)
	}

	if err := os.Rename(tmpPath, filePath); err != nil {
		return fmt.Errorf("error renaming checkpoint file: %w", err)
	}

	return nil
}

// GetCheckpoint 读取 checkpoint 文件并反序列化。
// 如果文件不存在，返回 os.ErrNotExist（由调用方决定是否容忍）。
// 如果新格式校验失败，自动尝试旧格式迁移（tryUnmarshalLegacy）。
func (s *checkpointStore) GetCheckpoint(key string, cp *Checkpoint) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	filePath := filepath.Join(s.dir, key)
	data, err := os.ReadFile(filePath)
	if err != nil {
		return err // 包含 os.ErrNotExist
	}

	// 尝试新格式
	if err := cp.UnmarshalCheckpoint(data); err == nil {
		if verifyErr := cp.VerifyChecksum(); verifyErr == nil {
			return nil
		}
	}

	// 新格式失败，尝试旧格式迁移
	legacyCp, legacyErr := tryUnmarshalLegacy(data)
	if legacyErr != nil {
		return fmt.Errorf("checkpoint corrupt: new format and legacy format both failed: %w", legacyErr)
	}

	// 迁移成功，将结果复制到 cp
	*cp = *legacyCp
	return nil
}

// ListCheckpoints 列出 checkpoint 目录下的所有 checkpoint 文件名。
// 过滤子目录、.tmp 临时文件和 .lock 锁文件。
func (s *checkpointStore) ListCheckpoints() ([]string, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("error listing checkpoint directory: %w", err)
	}

	var checkpoints []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, ".tmp") || strings.HasSuffix(name, ".lock") {
			continue
		}
		checkpoints = append(checkpoints, name)
	}
	return checkpoints, nil
}

// RemoveCheckpoint 删除指定的 checkpoint 文件。
// 文件不存在时视为成功（幂等语义，对齐 kubelet CheckpointManager 契约）。
func (s *checkpointStore) RemoveCheckpoint(key string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	err := os.Remove(filepath.Join(s.dir, key))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}
