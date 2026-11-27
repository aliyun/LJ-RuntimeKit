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
	"context"
	"fmt"
	"os"
	"syscall"
	"time"
)

// flock 实现基于 flock(2) 的文件锁,用于保证 Prepare / Unprepare 在
// 节点级别互斥(对应 NVIDIA pkg/flock)。
type flock struct {
	path string
}

func newFlock(path string) *flock {
	return &flock{path: path}
}

// acquire 获取独占文件锁,成功时返回释放函数。
// ctx 超时或取消时放弃获取。
func (f *flock) acquire(ctx context.Context, timeout time.Duration) (func(), error) {
	fd, err := syscall.Open(f.path, syscall.O_RDWR|syscall.O_CREAT, 0600)
	if err != nil {
		return nil, fmt.Errorf("error opening lock file %q: %w", f.path, err)
	}

	// 先异步加锁,再等待 ctx / timeout,避免无限阻塞。
	acquired := make(chan error, 1)
	go func() {
		for {
			err := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB)
			if err == nil {
				acquired <- nil
				return
			}
			select {
			case <-ctx.Done():
				acquired <- ctx.Err()
				return
			case <-time.After(50 * time.Millisecond):
				// retry
			}
		}
	}()

	select {
	case err := <-acquired:
		if err != nil {
			syscall.Close(fd)
			return nil, fmt.Errorf("error acquiring lock %q: %w", f.path, err)
		}
	case <-time.After(timeout):
		syscall.Close(fd)
		return nil, fmt.Errorf("timed out acquiring lock %q after %v", f.path, timeout)
	case <-ctx.Done():
		syscall.Close(fd)
		return nil, fmt.Errorf("context done while acquiring lock %q: %w", f.path, ctx.Err())
	}

	release := func() {
		if err := syscall.Flock(fd, syscall.LOCK_UN); err != nil {
			fmt.Fprintf(os.Stderr, "error releasing lock %q: %v\n", f.path, err)
		}
		syscall.Close(fd)
	}
	return release, nil
}
