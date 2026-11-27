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
	"fmt"
	"io"
	"os"
)

func CopyCheckPointFromRemote(reader io.Reader, checkpointTar string) error {
	checkpointTmpTar := fmt.Sprintf("%s.tmp", checkpointTar)
	if _, err := os.Stat(checkpointTmpTar); err == nil {
		err = os.Remove(checkpointTmpTar)
		if err != nil {
			return err
		}
	}
	checkpointTmpTarFs, err := os.Create(checkpointTmpTar)
	if err != nil {
		return err
	}
	defer checkpointTmpTarFs.Close()
	_, err = io.Copy(checkpointTmpTarFs, reader)
	if err != nil {
		return err
	}

	os.RemoveAll(checkpointTar) // guard
	err = os.Rename(checkpointTmpTar, checkpointTar)
	if err != nil {
		return err
	}
	return nil
}
