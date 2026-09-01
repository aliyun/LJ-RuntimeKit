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

package utils

import (
	"strconv"

	v1 "k8s.io/api/core/v1"
)

// GetResourceFromNode returns the allocatable resource quantity from a node's status.
func GetResourceFromNode(node *v1.Node, resourceName string) int64 {
	res := int64(0)
	val, ok := node.Status.Allocatable[v1.ResourceName(resourceName)]
	if ok {
		res = val.Value()
	}
	return res
}

// GetLabelsFromNodeStr returns the string value of a node label.
func GetLabelsFromNodeStr(node *v1.Node, labelKey string) (string, error) {
	val, ok := node.Labels[labelKey]
	if !ok {
		return "", NotFoundErr
	}
	return val, nil
}

// GetLabelsFromNodeInt returns the integer value of a node label.
func GetLabelsFromNodeInt(node *v1.Node, labelKey string) (int, error) {
	val, ok := node.Labels[labelKey]
	if !ok {
		return 0, NotFoundErr
	}
	ret, err := strconv.Atoi(val)
	if err != nil {
		return 0, err
	}
	return ret, nil
}
