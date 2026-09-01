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

package exporter

import (
	"testing"
)

func TestNewOutputProxyConfig_HasOutputType(t *testing.T) {
	config := &Config{
		ExporterOutputType:   OutputTypeHttpPull,
		ExporterOutputFormat: OutputFmtProm,
	}

	proxyConfig, err := NewOutputProxyConfig(config)
	if err != nil {
		t.Fatalf("NewOutputProxyConfig() returned unexpected error: %v", err)
	}

	if !proxyConfig.HasOutputType(OutputTypeHttpPull) {
		t.Errorf("HasOutputType(OutputTypeHttpPull) = false, want true")
	}

	if proxyConfig.HasOutputType("unknown") {
		t.Errorf("HasOutputType(\"unknown\") = true, want false")
	}
}

func TestNewOutputProxyConfig_HasOutputFmt(t *testing.T) {
	config := &Config{
		ExporterOutputType:   OutputTypeHttpPull,
		ExporterOutputFormat: OutputFmtProm,
	}

	proxyConfig, err := NewOutputProxyConfig(config)
	if err != nil {
		t.Fatalf("NewOutputProxyConfig() returned unexpected error: %v", err)
	}

	if !proxyConfig.HasOutputFmt(OutputFmtProm) {
		t.Errorf("HasOutputFmt(OutputFmtProm) = false, want true")
	}

	if proxyConfig.HasOutputFmt("unknown") {
		t.Errorf("HasOutputFmt(\"unknown\") = true, want false")
	}
}

func TestNewOutputProxyConfig_EgpuFormat(t *testing.T) {
	config := &Config{
		ExporterOutputType:   OutputTypeHttpPull,
		ExporterOutputFormat: OutputFmtKubergpu,
	}

	proxyConfig, err := NewOutputProxyConfig(config)
	if err != nil {
		t.Fatalf("NewOutputProxyConfig() returned unexpected error: %v", err)
	}

	if !proxyConfig.HasOutputFmt(OutputFmtKubergpu) {
		t.Errorf("HasOutputFmt(OutputFmtEgpu) = false, want true")
	}
}
