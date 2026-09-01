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
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// WaitWithTimeout waits for the WaitGroup with a timeout.
// Returns an error if the timeout is exceeded.
func WaitWithTimeout(wg *sync.WaitGroup, timeout time.Duration) error {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("Timeout waiting for WaitGroup")
	}
}

// NewOSWatcher creates a channel that listens for the specified OS signals.
func NewOSWatcher(sigs ...os.Signal) chan os.Signal {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, sigs...)

	return sigChan
}

// GetHostSerialNumber retrieves the system serial number using dmidecode.
func GetHostSerialNumber() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "sh", "-c", "/usr/sbin/dmidecode -s system-serial-number")
	output, err := cmd.CombinedOutput()

	if err != nil {
		return "N/A", fmt.Errorf("command execution failed: %v", err)
	}

	serialNumber := strings.TrimSpace(string(output))
	if serialNumber == "" {
		return "N/A", nil
	}
	return serialNumber, nil
}

// LogFormatter implements logrus.Formatter with colored output.
type LogFormatter struct {
	Name string
}

const (
	red    = 31
	yellow = 33
	blue   = 36
	gray   = 37
)

// SetupLogger initializes the global logger with the given component name.
func SetupLogger(name string) {
	logrus.SetFormatter(&LogFormatter{Name: name})
	logrus.SetReportCaller(true)
	level, err := logrus.ParseLevel(strings.ToLower(os.Getenv("AMPERF_LOG_LEVEL")))
	if err != nil {
		level = logrus.InfoLevel
	}
	logrus.SetLevel(level)
}

func printColoredString(str string, color int) string {
	return fmt.Sprintf("\x1b[%dm%s\x1b[0m", color, str)
}

func (m *LogFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	var levelColor int
	switch entry.Level {
	case logrus.DebugLevel, logrus.TraceLevel:
		levelColor = gray
	case logrus.WarnLevel:
		levelColor = yellow
	case logrus.ErrorLevel, logrus.FatalLevel, logrus.PanicLevel:
		levelColor = red
	case logrus.InfoLevel:
		levelColor = blue
	default:
		levelColor = blue
	}
	var b *bytes.Buffer
	if entry.Buffer != nil {
		b = entry.Buffer
	} else {
		b = &bytes.Buffer{}
	}
	timestamp := entry.Time.Format("2006-01-02 15:04:05")
	levelStr := printColoredString(entry.Level.String()[:4], levelColor)
	var newLog string
	if entry.HasCaller() {
		fName := filepath.Base(entry.Caller.File)
		newLog = fmt.Sprintf("[%s %s %s %s:%d] %s\n",
			timestamp, levelStr, m.Name, fName, entry.Caller.Line, entry.Message)
	} else {
		newLog = fmt.Sprintf("[%s %s %s] %s\n", timestamp, levelStr, m.Name, entry.Message)
	}

	b.WriteString(newLog)
	return b.Bytes(), nil
}

// GetLabels returns a merged map of all labels from RcvLabels and Labels.
func (m *MetricItem) GetLabels() map[string]string {
	labels := map[string]string{}
	if m == nil {
		return labels
	}
	if m.RcvLabels != nil && len(m.RcvLabels) > 0 {
		for k, v := range m.RcvLabels {
			labels[k] = v
		}
	}
	if m.Labels != nil && len(m.Labels) > 0 {
		for k, v := range m.Labels {
			labels[k] = v
		}
	}
	return labels
}

// GetLabelValue returns the value of the specified label key,
// searching Labels first, then falling back to RcvLabels.
func (m *MetricItem) GetLabelValue(key string) string {
	if m == nil {
		return ""
	}

	if m.Labels != nil && len(m.Labels) > 0 {
		if value, ok := m.Labels[key]; ok {
			return value
		}
	}

	if m.RcvLabels != nil && len(m.RcvLabels) > 0 {
		if value, ok := m.RcvLabels[key]; ok {
			return value
		}
	}

	return ""
}
