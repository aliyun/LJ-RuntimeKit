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

package computedomain

import (
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"
)

const (
	indexByUID                = "uid"
	indexByComputeDomainLabel = "computeDomainLabel"
)

type indexer interface {
	ByIndex(indexName, indexedValue string) ([]interface{}, error)
}

func uidIndexer(obj interface{}) ([]string, error) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("expected unstructured object, got %T", obj)
	}
	return []string{string(u.GetUID())}, nil
}

func addComputeDomainLabelIndexer(informer cache.SharedIndexInformer, labelKey string) error {
	return informer.AddIndexers(cache.Indexers{
		indexByComputeDomainLabel: func(obj interface{}) ([]string, error) {
			u, ok := obj.(*unstructured.Unstructured)
			if !ok {
				return nil, fmt.Errorf("expected unstructured object, got %T", obj)
			}
			value := u.GetLabels()[labelKey]
			if value == "" {
				return nil, nil
			}
			return []string{value}, nil
		},
	})
}

func getByComputeDomainUID(source indexer, uid, labelKey string) ([]*unstructured.Unstructured, error) {
	objects, err := source.ByIndex(indexByComputeDomainLabel, uid)
	if err != nil {
		return nil, err
	}
	result := make([]*unstructured.Unstructured, 0, len(objects))
	for _, obj := range objects {
		u, ok := obj.(*unstructured.Unstructured)
		if !ok {
			return nil, fmt.Errorf("expected unstructured object, got %T", obj)
		}
		// Re-verify the label value to guard against stale MutationCache
		// entries where a deleted object's index slot is reused by a new
		// object (same namespace/name) that belongs to a different CD.
		if u.GetLabels()[labelKey] != uid {
			continue
		}
		result = append(result, u)
	}
	return result, nil
}
