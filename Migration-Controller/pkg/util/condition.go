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

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// GetOrCreateCondition gets a Condition instance from conditions by
// conditionType. When the target conditionType does not exist in conditions,
// it returns a newly created instance in ConditionUnknown state.
func GetOrCreateCondition(conditionType string, conditions []metav1.Condition) *metav1.Condition {
	condition := GetCondition(conditionType, conditions)
	if condition != nil {
		return condition
	}
	return &metav1.Condition{
		Type:               conditionType,
		Status:             metav1.ConditionUnknown,
		LastTransitionTime: metav1.Now(),
	}
}

// GetCondition gets the target Condition from conditions; nil when it does not exist.
func GetCondition(conditionType string, conditions []metav1.Condition) *metav1.Condition {
	for i := range conditions {
		condition := &conditions[i]
		if condition.Type == conditionType {
			return condition
		}
	}
	return nil
}

// AddOrUpdateCondition adds a condition to conditions; when a condition of
// the same type already exists, updates its content.
//
// LastTransitionTime follows the standard Kubernetes semantics (aligned with
// apimachinery's meta.SetStatusCondition): the timestamp is updated only when
// the Status actually transitions (True<->False<->Unknown); when Status is
// unchanged the original value is kept, even if the caller passes a new
// metav1.Now().
//
// This is necessary: AddOrUpdateCondition's callers generally pass
// metav1.Now() unconditionally in every reconcile round. If the timestamp were
// overwritten unconditionally, then even with Status/Reason/Message all
// unchanged, the condition would be judged "status changed" by
// apiequality.Semantic.DeepEqual because of the timestamp difference -> one
// Status().Update fires -> that write immediately wakes the next reconcile of
// this CR -> refresh the timestamp again... forming a self-feedback idle loop
// (measured: about 3x more etcd writes and reconcile rounds during an active
// restore, with clustered resourceVersion conflicts). Keeping the timestamp
// cuts the loop.
//
// Reason/Message/ObservedGeneration are still updated per the passed values:
// they carry real information and should be written back when changed; only
// changes to these fields do not touch LastTransitionTime.
func AddOrUpdateCondition(condition *metav1.Condition, conditions []metav1.Condition) []metav1.Condition {
	if conditions == nil {
		conditions = []metav1.Condition{}
	}
	for i := range conditions {
		if conditions[i].Type != condition.Type {
			continue
		}
		next := *condition
		if conditions[i].Status == next.Status {
			// Status not transitioned: keep the original LastTransitionTime, not drifting with every round's metav1.Now().
			next.LastTransitionTime = conditions[i].LastTransitionTime
		} else if next.LastTransitionTime.IsZero() {
			// Status transitioned but the caller gave no timestamp: fill in the current time.
			next.LastTransitionTime = metav1.Now()
		}
		conditions[i] = next
		return conditions
	}
	if condition.LastTransitionTime.IsZero() {
		condition.LastTransitionTime = metav1.Now()
	}
	conditions = append(conditions, *condition)
	return conditions
}

// AssertConditionStatus judges whether the Condition matches the expected status; false when the condition is nil.
func AssertConditionStatus(condition *metav1.Condition, expectStatus metav1.ConditionStatus) bool {
	return condition != nil && condition.Status == expectStatus
}

// AssertConditionInStatus judges whether the status of the condition of
// conditionType in conditions is one of expectedStatus; when the Condition
// does not exist in conditions it is treated as Unknown.
func AssertConditionInStatus(conditions []metav1.Condition, conditionType string, expectedStatus ...metav1.ConditionStatus) bool {
	condition := GetOrCreateCondition(conditionType, conditions)
	for _, expected := range expectedStatus {
		if AssertConditionStatus(condition, expected) {
			return true
		}
	}
	return false
}
