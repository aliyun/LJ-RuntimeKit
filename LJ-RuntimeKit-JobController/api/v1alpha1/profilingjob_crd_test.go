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

package v1alpha1

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	crdvalidation "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/validation"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/defaulting"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/pruning"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
	"sigs.k8s.io/yaml"
)

const crdPath = "../../deploy/profilingjobs-crd.yaml"

func loadCRD(t *testing.T) *apiextensions.CustomResourceDefinition {
	t.Helper()
	b, err := os.ReadFile(crdPath)
	if err != nil {
		t.Fatal(err)
	}
	var v1crd apiextensionsv1.CustomResourceDefinition
	if err := yaml.UnmarshalStrict(b, &v1crd); err != nil {
		t.Fatal(err)
	}
	s := runtime.NewScheme()
	if err := apiextensionsv1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := apiextensions.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	s.Default(&v1crd)
	var crd apiextensions.CustomResourceDefinition
	if err := s.Convert(&v1crd, &crd, nil); err != nil {
		t.Fatal(err)
	}
	return &crd
}

func TestCRDPassesAPIServerValidation(t *testing.T) {
	crd := loadCRD(t)
	if errs := crdvalidation.ValidateCustomResourceDefinition(context.Background(), crd); len(errs) > 0 {
		t.Fatalf("CRD rejected: %v", errs.ToAggregate())
	}
}

func TestCRDProfilingModes(t *testing.T) {
	root := loadCRD(t).Spec.Validation.OpenAPIV3Schema
	specMode := root.Properties["spec"].Properties["profilingMode"]
	runMode := root.Properties["status"].Properties["runConfig"].Properties["profilingMode"]
	want := []apiextensions.JSON{"light", "full"}
	for name, mode := range map[string]apiextensions.JSONSchemaProps{"spec": specMode, "status.runConfig": runMode} {
		if !reflect.DeepEqual(mode.Enum, want) {
			t.Errorf("%s.profilingMode 枚举 = %v，期望 %v", name, mode.Enum, want)
		}
	}
	if string(ProfilingModeLight) != "light" || string(ProfilingModeFull) != "full" {
		t.Fatal("API 常量必须与 CRD 枚举一致")
	}
	if specMode.Default == nil || *specMode.Default != "light" || runMode.Default != nil {
		t.Fatalf("默认值错误：spec=%v，status.runConfig=%v", specMode.Default, runMode.Default)
	}
	ss, err := schema.NewStructural(root)
	if err != nil {
		t.Fatal(err)
	}
	spec := baseSpec()
	delete(spec, "profilingMode")
	obj := object(spec)
	defaulting.Default(obj, ss)
	if spec["profilingMode"] != "light" {
		t.Fatalf("未指定模式时应默认 light，实际为 %v", spec["profilingMode"])
	}
}

func TestProfilingModeAdmission(t *testing.T) {
	validator, _, err := validation.NewSchemaValidator(loadCRD(t).Spec.Validation.OpenAPIV3Schema)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"spec.profilingMode", "status.runConfig.profilingMode"} {
		t.Run(path, func(t *testing.T) {
			for _, tc := range []struct {
				mode string
				want bool
			}{
				{"light", true}, {"full", true}, {"lightweight", false}, {"", false}, {"Light", false},
			} {
				t.Run(tc.mode, func(t *testing.T) {
					spec := baseSpec()
					obj := object(spec)
					if path == "spec.profilingMode" {
						spec["profilingMode"] = tc.mode
					} else {
						obj["status"] = map[string]interface{}{"runConfig": map[string]interface{}{
							"duration": "1m", "profilingMode": tc.mode,
						}}
					}
					errs := validation.ValidateCustomResource(field.NewPath("job"), obj, validator)
					if tc.want {
						if len(errs) != 0 {
							t.Fatalf("合法模式被拒绝：%v", errs)
						}
					} else if len(errs) != 1 || errs[0].Field != "job."+path {
						t.Fatalf("应拒绝 %s=%q，实际错误：%v", path, tc.mode, errs)
					}
				})
			}
		})
	}
}

func TestCRDPhaseEnumMatchesLifecycle(t *testing.T) {
	status := loadCRD(t).Spec.Validation.OpenAPIV3Schema.Properties["status"]
	for name, phase := range map[string]apiextensions.JSONSchemaProps{
		"status":  status.Properties["phase"],
		"history": status.Properties["history"].Items.Schema.Properties["phase"],
	} {
		t.Run(name, func(t *testing.T) {
			want := map[string]bool{
				string(PhasePending): true, string(PhaseRunning): true,
				string(PhaseCompleted): true, string(PhaseFailed): true, string(PhaseCanceled): true,
			}
			if len(phase.Enum) != len(want) {
				t.Fatalf("phase enum = %v, want %v", phase.Enum, want)
			}
			for _, value := range phase.Enum {
				text, ok := value.(string)
				if !ok || !want[text] {
					t.Fatalf("unexpected phase %v", value)
				}
				delete(want, text)
			}
		})
	}
}

func TestJobStatusMessageReplacesConditions(t *testing.T) {
	crd := loadCRD(t)
	status := crd.Spec.Validation.OpenAPIV3Schema.Properties["status"]
	if _, exists := status.Properties["conditions"]; exists || status.Properties["message"].Type != "string" {
		t.Fatal("CRD 应移除 conditions 并包含 message 字符串")
	}
	target := status.Properties["targets"].Items.Schema
	if target.Properties["reason"].Type != "string" || target.Properties["message"].Type != "string" {
		t.Fatal("目标级诊断字段丢失")
	}
	ss, err := schema.NewStructural(crd.Spec.Validation.OpenAPIV3Schema)
	if err != nil {
		t.Fatal(err)
	}
	obj := object(baseSpec())
	obj["status"] = map[string]interface{}{
		"phase": "Pending", "message": "PVCNotReady: waiting for PVC",
		"conditions": []interface{}{map[string]interface{}{"type": "PVCReady", "status": "False"}},
	}
	pruning.Prune(obj, ss, true)
	stored := obj["status"].(map[string]interface{})
	if _, exists := stored["conditions"]; exists || stored["message"] != "PVCNotReady: waiting for PVC" {
		t.Fatalf("旧字段裁剪或新消息保存错误：%v", stored)
	}
	job := &ProfilingJob{Status: ProfilingJobStatus{Phase: PhaseFailed, Message: "InvalidDuration: duration must be positive"}}
	data, err := json.Marshal(job.DeepCopy())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"message":"InvalidDuration: duration must be positive"`) || strings.Contains(string(data), `"conditions"`) {
		t.Fatalf("API/deepcopy 序列化未同步：%s", data)
	}
}

func TestJobStatusStartTimeReplacesResumeTime(t *testing.T) {
	crd := loadCRD(t)
	statusSchema := crd.Spec.Validation.OpenAPIV3Schema.Properties["status"]
	if _, exists := statusSchema.Properties["resumeTime"]; exists {
		t.Fatal("CRD 仍包含已删除的 resumeTime")
	}
	for _, key := range []string{"startTime", "endTime"} {
		if statusSchema.Properties[key].Type != "string" || statusSchema.Properties[key].Format != "date-time" {
			t.Fatalf("时间字段 %s 的 schema 错误", key)
		}
	}
	started := metav1.NewTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	ended := metav1.NewTime(started.Add(7 * time.Second))
	job := &ProfilingJob{Status: ProfilingJobStatus{
		Phase: PhaseCompleted, StartTime: &started, EndTime: &ended, CollectedDuration: 7000,
	}}
	wantStart, wantEnd := started.Time, ended.Time
	copied := job.DeepCopy()
	copied.Status.StartTime.Time = copied.Status.StartTime.Add(time.Hour)
	copied.Status.EndTime.Time = copied.Status.EndTime.Add(time.Hour)
	if !job.Status.StartTime.Time.Equal(wantStart) || !job.Status.EndTime.Time.Equal(wantEnd) {
		t.Fatal("时间字段的 DeepCopy 与原状态共享内存")
	}
	data, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	stored := raw["status"].(map[string]interface{})
	if _, exists := stored["resumeTime"]; exists || stored["startTime"] != "2026-01-01T00:00:00Z" {
		t.Fatalf("API 序列化未同步：%s", data)
	}
	stored["resumeTime"] = "2025-12-31T23:00:00Z"
	ss, err := schema.NewStructural(crd.Spec.Validation.OpenAPIV3Schema)
	if err != nil {
		t.Fatal(err)
	}
	obj := object(baseSpec())
	obj["status"] = stored
	pruning.Prune(obj, ss, true)
	if _, exists := stored["resumeTime"]; exists || stored["startTime"] != "2026-01-01T00:00:00Z" || stored["collectedDuration"] != float64(7000) {
		t.Fatalf("旧字段裁剪改变本次采集的计时：%v", stored)
	}
	var decoded ProfilingJob
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if !decoded.Status.StartTime.Time.Equal(wantStart) || !decoded.Status.EndTime.Time.Equal(wantEnd) {
		t.Fatalf("时间字段在反序列化后改变：%+v", decoded.Status)
	}
	roundtrip, err := json.Marshal(&decoded)
	if err != nil || string(roundtrip) != string(data) {
		t.Fatalf("计时状态无法完整往返序列化：%s, %v", roundtrip, err)
	}
}

func TestRunConfigurationContainsOnlyFrozenRunInputs(t *testing.T) {
	crd := loadCRD(t)
	runSchema := crd.Spec.Validation.OpenAPIV3Schema.Properties["status"].Properties["runConfig"]
	wantProperties := map[string]string{"duration": "string", "profilingMode": "string", "profilingRules": "string"}
	if len(runSchema.Properties) != len(wantProperties) {
		t.Fatalf("runConfig schema must contain only frozen run inputs: %v", runSchema.Properties)
	}
	for key, typ := range wantProperties {
		if runSchema.Properties[key].Type != typ {
			t.Fatalf("runConfig.%s type = %q, want %q", key, runSchema.Properties[key].Type, typ)
		}
	}
	job := &ProfilingJob{Status: ProfilingJobStatus{RunConfig: &RunConfiguration{
		Duration: metav1.Duration{Duration: 10 * time.Second}, ProfilingMode: ProfilingModeFull, ProfilingRules: "frozen-rules",
	}}}
	copied := job.DeepCopy()
	if copied.Status.RunConfig == job.Status.RunConfig || !reflect.DeepEqual(copied.Status.RunConfig, job.Status.RunConfig) {
		t.Fatal("runConfig deepcopy must preserve values without sharing the original pointer")
	}
	copied.Status.RunConfig.Duration.Duration = time.Minute
	copied.Status.RunConfig.ProfilingMode = ProfilingModeLight
	copied.Status.RunConfig.ProfilingRules = "changed"
	if job.Status.RunConfig.Duration.Duration != 10*time.Second || job.Status.RunConfig.ProfilingMode != ProfilingModeFull || job.Status.RunConfig.ProfilingRules != "frozen-rules" {
		t.Fatal("runConfig deepcopy changed original frozen inputs")
	}
	data, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	stored := raw["status"].(map[string]interface{})["runConfig"].(map[string]interface{})
	want := map[string]interface{}{"duration": "10s", "profilingMode": "full", "profilingRules": "frozen-rules"}
	if !reflect.DeepEqual(stored, want) {
		t.Fatalf("status.runConfig JSON contains non-run inputs or loses frozen inputs: %s", data)
	}
	obsolete := map[string]interface{}{
		"targetSelector":            map[string]interface{}{"matchLabels": map[string]interface{}{"app": "old"}},
		"persistentVolumeClaimName": "old-pvc", "persistentVolumeClaimNamespace": "old-storage",
		"collectorConfig": `{"image":"collector:v1"}`,
	}
	for key, value := range obsolete {
		stored[key] = value
	}
	ss, err := schema.NewStructural(crd.Spec.Validation.OpenAPIV3Schema)
	if err != nil {
		t.Fatal(err)
	}
	obj := object(baseSpec())
	obj["status"] = map[string]interface{}{"runConfig": stored}
	pruning.Prune(obj, ss, true)
	if !reflect.DeepEqual(stored, want) {
		t.Fatalf("obsolete runConfig fields survived pruning or frozen inputs were lost: %v", stored)
	}
}

func TestStatusOmitsAttempt(t *testing.T) {
	crd := loadCRD(t)
	if _, exists := crd.Spec.Validation.OpenAPIV3Schema.Properties["status"].Properties["attempt"]; exists {
		t.Fatal("attempt remains in the status schema")
	}
	ss, err := schema.NewStructural(crd.Spec.Validation.OpenAPIV3Schema)
	if err != nil {
		t.Fatal(err)
	}
	obj := object(baseSpec())
	obj["status"] = map[string]interface{}{"phase": "Canceled", "attempt": int64(2)}
	pruning.Prune(obj, ss, true)
	if _, exists := obj["status"].(map[string]interface{})["attempt"]; exists {
		t.Fatal("obsolete attempt field survived pruning")
	}
	data, err := json.Marshal((&ProfilingJob{Status: ProfilingJobStatus{Phase: PhaseCanceled}}).DeepCopy())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), `"attempt"`) {
		t.Fatalf("attempt remains in serialized status: %s", data)
	}
}

func TestTargetStatusPrefixedFields(t *testing.T) {
	crd := loadCRD(t)
	targetSchema := crd.Spec.Validation.OpenAPIV3Schema.Properties["status"].Properties["targets"].Items.Schema
	oldFields := []string{"namespace", "name", "uid", "nodeName", "containerIDs", "containerNames"}
	for _, key := range oldFields {
		if _, exists := targetSchema.Properties[key]; exists {
			t.Fatalf("CRD 仍包含旧目标字段：%s", key)
		}
	}
	newFields := []string{"targetNamespace", "targetName", "targetUID", "targetNodeName", "targetContainerIds"}
	for _, key := range newFields {
		wantType := "string"
		if key == "targetContainerIds" {
			wantType = "array"
		}
		if targetSchema.Properties[key].Type != wantType {
			t.Fatalf("目标字段 %s 类型错误", key)
		}
		found := false
		for _, required := range targetSchema.Required {
			found = found || required == key
		}
		if !found {
			t.Fatalf("目标字段 %s 应为必填", key)
		}
	}
	target := TargetStatus{
		TargetNamespace: "workload", TargetName: "gpu-pod", TargetUID: "pod-uid", TargetNodeName: "node-a",
		TargetContainerIds: []string{"containerd://aaa", "containerd://zzz"}, Stage: StageQueued,
	}
	job := &ProfilingJob{Status: ProfilingJobStatus{Targets: []TargetStatus{target}}}
	copied := job.DeepCopy()
	copied.Status.Targets[0].TargetContainerIds[0] = "changed"
	if job.Status.Targets[0].TargetContainerIds[0] != "containerd://aaa" {
		t.Fatal("目标容器 ID 的 DeepCopy 与原数组共享内存")
	}
	data, err := json.Marshal(target)
	if err != nil {
		t.Fatal(err)
	}
	var stored map[string]interface{}
	if err := json.Unmarshal(data, &stored); err != nil {
		t.Fatal(err)
	}
	want := map[string]interface{}{
		"targetNamespace": "workload", "targetName": "gpu-pod", "targetUID": "pod-uid", "targetNodeName": "node-a",
		"targetContainerIds": []interface{}{"containerd://aaa", "containerd://zzz"}, "stage": "Queued",
	}
	if !reflect.DeepEqual(stored, want) {
		t.Fatalf("目标 JSON 字段未同步：%s", data)
	}
	var decoded TargetStatus
	if err := json.Unmarshal(data, &decoded); err != nil || !reflect.DeepEqual(decoded, target) {
		t.Fatalf("目标状态无法完整往返序列化：%+v, %v", decoded, err)
	}
	ss, err := schema.NewStructural(crd.Spec.Validation.OpenAPIV3Schema)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range oldFields {
		stored[key] = "obsolete"
	}
	obj := object(baseSpec())
	obj["status"] = map[string]interface{}{"targets": []interface{}{stored}}
	pruning.Prune(obj, ss, true)
	if !reflect.DeepEqual(stored, want) {
		t.Fatalf("旧目标字段未裁剪或新字段丢失：%v", stored)
	}
	validator, _, err := validation.NewSchemaValidator(crd.Spec.Validation.OpenAPIV3Schema)
	if err != nil {
		t.Fatal(err)
	}
	if errs := validation.ValidateCustomResource(field.NewPath(""), obj, validator); len(errs) != 0 {
		t.Fatalf("新目标状态未通过 schema 验证：%v", errs)
	}
	for _, key := range newFields {
		value := stored[key]
		delete(stored, key)
		if errs := validation.ValidateCustomResource(field.NewPath(""), obj, validator); len(errs) == 0 {
			t.Errorf("缺失目标字段 %s 未被拒绝", key)
		}
		stored[key] = value
	}
}

func baseSpec() map[string]interface{} {
	return map[string]interface{}{
		"targetSelector":                 map[string]interface{}{"matchLabels": map[string]interface{}{"app": "trainer"}},
		"profilingRules":                 "rule-a",
		"runIndex":                       int64(1),
		"duration":                       "1m",
		"profilingMode":                  "light",
		"persistentVolumeClaimName":      "pvc",
		"persistentVolumeClaimNamespace": "data",
		"cancel":                         false,
	}
}

func object(spec map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{
		"apiVersion": "profiling.runtimekit.io/v1alpha1",
		"kind":       "ProfilingJob",
		"metadata":   map[string]interface{}{"name": "job", "namespace": "default"},
		"spec":       spec,
	}
}

func TestSpecUpdateRules(t *testing.T) {
	crd := loadCRD(t)
	ss, err := schema.NewStructural(crd.Spec.Validation.OpenAPIV3Schema)
	if err != nil {
		t.Fatal(err)
	}
	validator := cel.NewValidator(ss, true, celconfig.PerCallLimit)

	for _, tc := range []struct {
		name    string
		mutate  func(map[string]interface{})
		wantErr string
	}{
		{"unchanged", func(map[string]interface{}) {}, ""},
		{"profilingRules", func(s map[string]interface{}) { s["profilingRules"] = "rule-b" }, ""},
		{"remove profilingRules", func(s map[string]interface{}) { delete(s, "profilingRules") }, ""},
		{"increase runIndex by 1", func(s map[string]interface{}) { s["runIndex"] = int64(2) }, ""},
		{"duration", func(s map[string]interface{}) { s["duration"] = "2h" }, ""},
		{"profilingMode", func(s map[string]interface{}) { s["profilingMode"] = "full" }, ""},
		{"cancel", func(s map[string]interface{}) { s["cancel"] = true }, ""},
		{"increase runIndex by 2", func(s map[string]interface{}) { s["runIndex"] = int64(3) }, "runIndex can only stay unchanged or increase by 1"},
		{"decrease runIndex", func(s map[string]interface{}) { s["runIndex"] = int64(0) }, "runIndex can only stay unchanged or increase by 1"},
		{"matchLabels value", func(s map[string]interface{}) {
			s["targetSelector"] = map[string]interface{}{"matchLabels": map[string]interface{}{"app": "other"}}
		}, "targetSelector is immutable"},
		{"matchLabels extra key", func(s map[string]interface{}) {
			s["targetSelector"] = map[string]interface{}{"matchLabels": map[string]interface{}{"app": "trainer", "x": "y"}}
		}, "targetSelector is immutable"},
		{"selector kind", func(s map[string]interface{}) {
			s["targetSelector"] = map[string]interface{}{"podName": "p", "podNamespace": "default"}
		}, "targetSelector is immutable"},
		{"pvc name", func(s map[string]interface{}) { s["persistentVolumeClaimName"] = "other" }, "persistentVolumeClaimName is immutable"},
		{"pvc namespace", func(s map[string]interface{}) { s["persistentVolumeClaimNamespace"] = "other" }, "persistentVolumeClaimNamespace is immutable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := baseSpec()
			tc.mutate(spec)
			errs, _ := validator.Validate(context.Background(), field.NewPath(""), ss, object(spec), object(baseSpec()), celconfig.RuntimeCELCostBudget)
			if tc.wantErr == "" {
				if len(errs) > 0 {
					t.Fatalf("unexpected errors: %v", errs.ToAggregate())
				}
				return
			}
			if len(errs) != 1 || !strings.Contains(errs[0].Error(), tc.wantErr) {
				t.Fatalf("errors = %v, want %q", errs.ToAggregate(), tc.wantErr)
			}
		})
	}
}

// Admission must not accept a duration the decoder rejects: a single such
// object breaks every typed List and the controller informer with it.
func TestDurationAdmissionRejectsUndecodableValues(t *testing.T) {
	crd := loadCRD(t)
	ss, err := schema.NewStructural(crd.Spec.Validation.OpenAPIV3Schema)
	if err != nil {
		t.Fatal(err)
	}
	celValidator := cel.NewValidator(ss, true, celconfig.PerCallLimit)
	openAPIValidator, _, err := validation.NewSchemaValidator(crd.Spec.Validation.OpenAPIV3Schema)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		value string
		want  bool
	}{
		{"15s", true}, {"1m0s", true}, {"1.000000001s", true}, {"500ms", true}, {"100µs", true},
		{"2562047h", true}, {"999999999999999999h", false}, {"9999999h", false},
		{"1e3s", false}, {"-1s", false}, {"", false},
	} {
		t.Run(tc.value, func(t *testing.T) {
			spec := baseSpec()
			spec["duration"] = tc.value
			obj := object(spec)
			celErrs, _ := celValidator.Validate(context.Background(), field.NewPath(""), ss, obj, nil, celconfig.RuntimeCELCostBudget)
			result := openAPIValidator.Validate(obj)
			accepted := len(celErrs) == 0 && len(result.Errors) == 0
			if accepted != tc.want {
				t.Fatalf("accepted=%v want=%v cel=%v openapi=%v", accepted, tc.want, celErrs.ToAggregate(), result.AsError())
			}
			if !accepted {
				return
			}
			var decoded metav1.Duration
			if err := json.Unmarshal([]byte(strconv.Quote(tc.value)), &decoded); err != nil {
				t.Fatalf("admitted a duration the decoder rejects: %v", err)
			}
		})
	}
}

func TestCreateAllowsAnyRunIndex(t *testing.T) {
	crd := loadCRD(t)
	ss, err := schema.NewStructural(crd.Spec.Validation.OpenAPIV3Schema)
	if err != nil {
		t.Fatal(err)
	}
	validator := cel.NewValidator(ss, true, celconfig.PerCallLimit)
	for _, runIndex := range []int64{0, 1, 5} {
		spec := baseSpec()
		spec["runIndex"] = runIndex
		if errs, _ := validator.Validate(context.Background(), field.NewPath(""), ss, object(spec), nil, celconfig.RuntimeCELCostBudget); len(errs) > 0 {
			t.Fatalf("runIndex %d create rejected: %v", runIndex, errs.ToAggregate())
		}
	}
}

func TestLaterUpdateUsesCurrentRunIndex(t *testing.T) {
	crd := loadCRD(t)
	ss, err := schema.NewStructural(crd.Spec.Validation.OpenAPIV3Schema)
	if err != nil {
		t.Fatal(err)
	}
	validator := cel.NewValidator(ss, true, celconfig.PerCallLimit)
	oldSpec, newSpec := baseSpec(), baseSpec()
	oldSpec["runIndex"], newSpec["runIndex"] = int64(4), int64(5)
	if errs, _ := validator.Validate(context.Background(), field.NewPath(""), ss, object(newSpec), object(oldSpec), celconfig.RuntimeCELCostBudget); len(errs) > 0 {
		t.Fatalf("4 -> 5 rejected: %v", errs.ToAggregate())
	}
}
