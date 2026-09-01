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
	"github.com/lj-runtimekit/exporter/pkg/common"
)

type outputScene struct {
	AppenderName       string
	FormatName         string
	AppenderConfigJson string
	FormatConfigJson   string
}

// OutputProxyConfig holds the configuration for output proxy scenes.
type OutputProxyConfig struct {
	httpChan chan common.FormattedData
	config   *Config
	scenes   []outputScene
}

// NewOutputProxyConfig creates a new OutputProxyConfig from the given config.
func NewOutputProxyConfig(config *Config) (*OutputProxyConfig, error) {

	proxyConfig := &OutputProxyConfig{
		config: config,
		scenes: make([]outputScene, 0),
	}

	scenes := []outputScene{{config.ExporterOutputType, config.ExporterOutputFormat, "", ""}}

	// remove vsock out-type for gpu share mode
	for _, scene := range scenes {
		proxyConfig.scenes = append(proxyConfig.scenes, scene)
	}

	return proxyConfig, nil
}

func (c *OutputProxyConfig) HasOutputType(oType string) bool {
	for _, scene := range c.scenes {
		if scene.AppenderName == oType {
			return true
		}
	}

	return false
}

func (c *OutputProxyConfig) HasOutputFmt(oFmt string) bool {
	for _, scene := range c.scenes {
		if scene.FormatName == oFmt {
			return true
		}
	}

	return false
}

func (c *OutputProxyConfig) RegisterHttpServer(httpChan chan common.FormattedData) {
	c.httpChan = httpChan
}
