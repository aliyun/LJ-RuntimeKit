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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
	nvscheme "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/nvidia.com/clientset/versioned/scheme"
)

const (
	// ComputeDomainCliqueGroup 定义 ComputeDomainClique 的 API 分组。
	ComputeDomainCliqueGroup   = "resource.aliyun.com"
	ComputeDomainCliqueVersion = "v1beta1"
)

// SchemeGroupVersion 是本项目使用的 GroupVersion。
var SchemeGroupVersion = schema.GroupVersion{
	Group:   ComputeDomainCliqueGroup,
	Version: ComputeDomainCliqueVersion,
}

// channelConfigDecoder 是解码 opaque 参数中 ComputeDomainChannelConfig 的本地 decoder。
var channelConfigDecoder runtime.Decoder

func init() {
	// 1. 向 NVIDIA clientset 的公开 Scheme 增量注册新 GroupVersion 的 Clique 类型，
	//    使 ParameterCodec 和序列化器能处理新 Group 的请求/响应。
	nvscheme.Scheme.AddKnownTypes(SchemeGroupVersion,
		&nvapi.ComputeDomainClique{},
		&nvapi.ComputeDomainCliqueList{},
	)
	metav1.AddToGroupVersion(nvscheme.Scheme, SchemeGroupVersion)

	// 2. 构造 ChannelConfig 的本地 decoder（独立 scheme）。
	localScheme := runtime.NewScheme()
	localScheme.AddKnownTypes(SchemeGroupVersion,
		&nvapi.ComputeDomainChannelConfig{},
	)
	// 同时注册旧 Group 以兼容存量 claim 的 opaque 参数
	localScheme.AddKnownTypes(nvapi.SchemeGroupVersion,
		&nvapi.ComputeDomainChannelConfig{},
	)
	localCodecs := serializer.NewCodecFactory(localScheme)
	channelConfigDecoder = localCodecs.UniversalDeserializer()
}
