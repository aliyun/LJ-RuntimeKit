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

package cache

// CycleState keys used to share state between PreFilter and subsequent scheduling phases.
const (
	// ClusterDevicesStateKey stores the AvailableClusterDevices built during PreFilter.
	ClusterDevicesStateKey = "PreFilterClusterDevicesState"
	// VStandardTaskReqStateKey stores the parsed StandardTaskReq for the current pod.
	VStandardTaskReqStateKey = "PreFilterStandardTaskReqState"
	// ClusterMigDeviceStateKey stores MIG device information for all nodes.
	ClusterMigDeviceStateKey = "PreFilterClusterMigDevicesState"
)
