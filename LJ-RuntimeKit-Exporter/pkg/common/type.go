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

// DataType constants identify the type of metric data being processed.
const (
	CollectDataType int32 = iota
	DetectDataType
	AmperfDataMax
)

// DataSource constants identify the origin of metric data.
const (
	LocalDataSource int32 = iota
)

// ReportType constants identify how metric data should be reported.
const (
	ReportTypeMetric int32 = iota
	ReportTypeEvent
)

// FormattedData holds formatted output data ready for export.
type FormattedData struct {
	Type int32
	Data string
}

// MetricItem represents a single collected metric data point.
type MetricItem struct {
	Counter       *Counter
	Egpu          bool
	Full          bool
	Pid           uint32
	Pname         string
	Time          int64
	Value         string
	ID            string
	UUID          string
	ModelName     string
	MigUUID       string
	MigProfile    string
	GPUInstanceID string
	ComputeId     string
	Message       string
	HostName      string
	NodeName      string
	HostSN        string
	NodeIP        string
	Cid           string
	Cgroup        string
	Labels        map[string]string
	RcvLabels     map[string]string // labels from receiver
}

func (c *Counter) Clone() *Counter {
	return &Counter{
		PodMap:     c.PodMap,
		PullOnly:   c.PullOnly,
		FieldID:    c.FieldID,
		FieldName:  c.FieldName,
		MetricName: c.MetricName,
		ShortName:  c.ShortName,
		PromType:   c.PromType,
		Help:       c.Help,
	}
}

// Clone returns a deep copy of the MetricItem.
func (m *MetricItem) Clone() *MetricItem {
	if m == nil {
		return nil
	}
	dst := &MetricItem{
		Counter:       m.Counter.Clone(),
		Egpu:          m.Egpu,
		Full:          m.Full,
		Pid:           m.Pid,
		Pname:         m.Pname,
		Time:          m.Time,
		Value:         m.Value,
		ID:            m.ID,
		UUID:          m.UUID,
		ModelName:     m.ModelName,
		MigUUID:       m.MigUUID,
		MigProfile:    m.MigProfile,
		GPUInstanceID: m.GPUInstanceID,
		ComputeId:     m.ComputeId,
		Message:       m.Message,
		HostName:      m.HostName,
		NodeName:      m.NodeName,
		HostSN:        m.HostSN,
		NodeIP:        m.NodeIP,
		Cid:           m.Cid,
		Cgroup:        m.Cgroup,
		Labels:        make(map[string]string),
		RcvLabels:     make(map[string]string),
	}
	// Copy Labels
	for k, v := range m.Labels {
		dst.Labels[k] = v
	}
	// Copy RcvLabels
	for k, v := range m.RcvLabels {
		dst.RcvLabels[k] = v
	}
	return dst
}
