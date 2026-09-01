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

package common

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
)

// Process represents a named process component for log level management.
type Process string

const (
	LogLevelFile = "/run/amperf/log_level"

	ProcessExporter Process = "Exporter"
)

// LogLevel holds the dynamic log level configuration for process components.
type LogLevel struct {
	Modules map[Process]string `json:"modules"`
}

// CreateConfigFile writes the dynamic log level configuration file.
func CreateConfigFile(targets []Process, level logrus.Level) error {
	modules := map[Process]string{}
	for _, process := range targets {
		modules[process] = level.String()
	}
	config := LogLevel{
		Modules: modules,
	}

	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal log level config: %w", err)
	}
	if err = os.WriteFile(LogLevelFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp file: %w", err)
	}

	return nil
}

// ReadConfigFile reads the dynamic log level configuration file.
func ReadConfigFile() (*LogLevel, error) {
	data, err := os.ReadFile(LogLevelFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read log level file: %w", err)
	}

	var config LogLevel
	if err = json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("invalid JSON format in %s: %w", LogLevelFile, err)
	}

	//logrus.Printf("Read log level config from %s: %+v", LogLevelFile, config)
	return &config, nil
}

// GetLogLevel retrieves the configured log level for the given process.
func GetLogLevel(process Process) (logrus.Level, error) {
	logLevel, err := ReadConfigFile()
	if err != nil {
		return logrus.InfoLevel, err
	}
	if level, ok := logLevel.Modules[process]; ok {
		return logrus.ParseLevel(level)
	} else {
		return logrus.InfoLevel, fmt.Errorf("no log level configured for process '%s'", process)
	}
}
