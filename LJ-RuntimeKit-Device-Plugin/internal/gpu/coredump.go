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

package gpu

import (
	"os"
	"runtime"

	"k8s.io/klog/v2"
)

func coreDump(fileName string) {
	klog.Infof("dumping stacktrace to %s", fileName)
	_ = os.WriteFile(fileName, []byte(stackTrace(true)), 0644)
}

func stackTrace(all bool) string {
	buf := make([]byte, 10240)
	for {
		size := runtime.Stack(buf, all)
		if size == len(buf) {
			buf = make([]byte, len(buf)<<1)
			continue
		}
		return string(buf[:size])
	}
}
