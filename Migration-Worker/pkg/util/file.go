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

package util

import (
	"crypto/md5"
	"encoding/hex"
	"os"
	"path/filepath"
	"time"
)

type ExecFunc func() (err error)

type TimeoutFunc func() (err error)

func GetDirData(root string) (int64, int64, error) {
	var count, size int64
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		count++
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return count, size, err
}

func WaitUntilTimeout(timeout time.Duration, execFunc ExecFunc, timeoutFunc TimeoutFunc) error {
	done := make(chan bool)
	var err error

	go func() {
		err = execFunc()
		done <- true
	}()

	select {
	case <-done:
		return err
	case <-time.After(timeout):
		return timeoutFunc()
	}
}

func CalculateMD5(data []byte) string {
	hash := md5.New()
	hash.Write(data)
	md5Sum := hash.Sum(nil)
	return hex.EncodeToString(md5Sum)
}

func CheckPathExists(path string) bool {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}

	return info.IsDir()
}
