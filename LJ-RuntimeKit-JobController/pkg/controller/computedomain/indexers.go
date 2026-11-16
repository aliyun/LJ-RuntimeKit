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
