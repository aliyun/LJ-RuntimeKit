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

package handler

import (
	"context"
	"testing"

	"github.com/container-migration/controller/api/v1beta1"
	"github.com/container-migration/controller/pkg/controllers"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Cold-start fallback pods must strip the migration-injected env: they do not
// participate in migration and no checkpoint session will release the
// steady-state hang; an app carrying KUBERGPU_MIGRATION_ENABLE would be hung
// at the steady-state point by the migration stack and never serve
// (health=000, no port listening observed in practice).
func TestBuildColdStartPod_StripsMigrationEnvs(t *testing.T) {
	template := &corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
		Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{{
				Name: "business-init",
				Env: []corev1.EnvVar{
					{Name: "KUBERGPU_MIGRATION_ENABLE", Value: "1"},
					{Name: "BUSINESS_ENV", Value: "keep"},
				},
			}},
			Containers: []corev1.Container{{
				Name: "app",
				Env: []corev1.EnvVar{
					{Name: "KUBERGPU_MIGRATION_ENABLE", Value: "1"},
					{Name: "AMP_VGPU_ENABLE", Value: "1"},
					{Name: "KUBERGPU_CR_BACKUP_PATH", Value: "/data/migration/kubergpu_backup"},
					{Name: "BUSINESS_ENV", Value: "keep"},
				},
			}},
		},
	}

	pod := buildColdStartPod(&v1beta1.ContainerStateMigrationJobRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "job", Namespace: "default"},
	}, template, "cold-pod", "node-1")

	// Both the main container and the business init container must strip the two migration-injected env vars.
	for i := range pod.Spec.Containers {
		if envHas(pod.Spec.Containers[i].Env, "KUBERGPU_MIGRATION_ENABLE") {
			t.Fatalf("container %s still has KUBERGPU_MIGRATION_ENABLE: %v",
				pod.Spec.Containers[i].Name, pod.Spec.Containers[i].Env)
		}
		if envHas(pod.Spec.Containers[i].Env, "AMP_VGPU_ENABLE") {
			t.Fatalf("container %s still has AMP_VGPU_ENABLE: %v",
				pod.Spec.Containers[i].Name, pod.Spec.Containers[i].Env)
		}
		if !envHas(pod.Spec.Containers[i].Env, "BUSINESS_ENV") {
			t.Fatalf("container %s lost business env: %v",
				pod.Spec.Containers[i].Name, pod.Spec.Containers[i].Env)
		}
	}
	for i := range pod.Spec.InitContainers {
		if envHas(pod.Spec.InitContainers[i].Env, "KUBERGPU_MIGRATION_ENABLE") {
			t.Fatalf("init container %s still has KUBERGPU_MIGRATION_ENABLE: %v",
				pod.Spec.InitContainers[i].Name, pod.Spec.InitContainers[i].Env)
		}
		if envHas(pod.Spec.InitContainers[i].Env, "AMP_VGPU_ENABLE") {
			t.Fatalf("init container %s still has AMP_VGPU_ENABLE: %v",
				pod.Spec.InitContainers[i].Name, pod.Spec.InitContainers[i].Env)
		}
	}

	// Strip only those two: KUBERGPU_CR_BACKUP_PATH alone does not trigger migration stack injection, keep it.
	foundBackup := false
	for i := range pod.Spec.Containers {
		if envHas(pod.Spec.Containers[i].Env, "KUBERGPU_CR_BACKUP_PATH") {
			foundBackup = true
		}
	}
	if !foundBackup {
		t.Fatalf("KUBERGPU_CR_BACKUP_PATH should be kept in cold start pod, got: %v",
			pod.Spec.Containers[0].Env)
	}
}

// Restore pods must keep all migration env: they go through runc restore and
// need the migration stack injection. Stripping only applies to the cold-start
// fallback path; the restore path migration capability is unaffected.
func TestBuildDestPod_KeepsMigrationEnvs(t *testing.T) {
	template := &corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "app",
				Image: "example.invalid/app:v1",
				Env: []corev1.EnvVar{
					{Name: "KUBERGPU_MIGRATION_ENABLE", Value: "1"},
					{Name: "AMP_VGPU_ENABLE", Value: "1"},
				},
			}},
		},
	}

	pod := buildDestPod(&v1beta1.ContainerStateMigrationJobRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "job", Namespace: "default"},
	}, template, "restore-pod", "/data", "", "")

	if !envHas(pod.Spec.Containers[0].Env, "KUBERGPU_MIGRATION_ENABLE") {
		t.Fatalf("restore pod lost KUBERGPU_MIGRATION_ENABLE: %v", pod.Spec.Containers[0].Env)
	}
	if !envHas(pod.Spec.Containers[0].Env, "AMP_VGPU_ENABLE") {
		t.Fatalf("restore pod lost AMP_VGPU_ENABLE: %v", pod.Spec.Containers[0].Env)
	}
}

// envHas judges whether an env var with the given name exists in the env list.
func envHas(env []corev1.EnvVar, name string) bool {
	for _, e := range env {
		if e.Name == name {
			return true
		}
	}
	return false
}

// TestColdStartHandler_DisableColdStartFallbackKeepsReplica: with the CR field
// on, a ColdStarting replica must not advance (no pod deletion / recreation) -
// the failing restore pod stays in place for debugging. Clearing the field
// resumes the cold start.
func TestColdStartHandler_DisableColdStartFallbackKeepsReplica(t *testing.T) {
	h := &ColdStartHandler{sharedContext: &controllers.SharedContext{}}
	jobReq := &v1beta1.ContainerStateMigrationJobRequest{
		Spec: v1beta1.ContainerStateMigrationJobRequestSpec{
			Restore: &v1beta1.RestoreSpec{DisableColdStartFallback: true},
		},
		Status: v1beta1.ContainerStateMigrationJobRequestStatus{
			Phase: v1beta1.JobPhaseRestoring,
			RestoreStatus: &v1beta1.JobRestoreStatus{
				Phase: v1beta1.JobPhaseRestoring,
				Destinations: []v1beta1.JobDestinationStatus{
					{Phase: v1beta1.DestPhaseColdStarting},
				},
			},
		},
	}

	result := h.Handle(context.Background(), jobReq)

	dest := &jobReq.Status.RestoreStatus.Destinations[0]
	if dest.Phase != v1beta1.DestPhaseColdStarting {
		t.Fatalf("phase must stay ColdStarting, got %q", dest.Phase)
	}
	if dest.ColdStart != nil {
		t.Fatalf("cold start must not be initialized (no advancement), got %+v", dest.ColdStart)
	}
	if dest.Message == "" {
		t.Fatal("expected a message explaining the disabled fallback")
	}
	if result.IsSkip() {
		t.Fatal("handler should continue, not skip")
	}
}

// TestColdStartHandler_DefaultKeepsFallbackOn: the CR field defaults to false
// - the cold-start fallback must stay enabled unless explicitly disabled.
func TestColdStartHandler_DefaultKeepsFallbackOn(t *testing.T) {
	spec := v1beta1.RestoreSpec{}
	if spec.DisableColdStartFallback {
		t.Fatal("cold start fallback must be enabled by default")
	}
}
