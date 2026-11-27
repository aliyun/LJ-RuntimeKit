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

package collectorprotocol

import (
	"crypto/sha256"
	"fmt"
	"strings"
)

// NormalizeContainerID strips a runtime scheme such as "containerd://".
func NormalizeContainerID(id string) string {
	if i := strings.Index(id, "://"); i >= 0 {
		return id[i+3:]
	}
	return id
}

const partialSuffix = ".partial"

func boundedFilename(name string, maxBytes int) string {
	if len(name) <= maxBytes {
		return name
	}
	hash := sha256.Sum256([]byte(name))
	return fmt.Sprintf("%s-%x", name[:maxBytes-1-16], hash[:8])
}

func containerFilenameID(containerID string) string {
	id := NormalizeContainerID(containerID)
	if len(id) <= 12 {
		return id
	}
	return id[:12]
}

func OutputFileName(podNamespace, podName, containerID, fileName string) string {
	return outputFileName(podNamespace, podName, containerID, fileName, 64, 255)
}

func outputFileName(podNamespace, podName, containerID, fileName string, componentBytes, maxBytes int) string {
	base := fmt.Sprintf("%s_%s_%s_", boundedFilename(podNamespace, componentBytes), boundedFilename(podName, componentBytes),
		boundedFilename(containerFilenameID(containerID), componentBytes))
	return base + boundedFilename(fileName, maxBytes-len(base)-len(partialSuffix))
}
