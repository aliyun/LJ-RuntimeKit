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
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/lj-runtimekit/exporter/pkg/common"

	"github.com/sirupsen/logrus"
)

// AmperfExporter is the core exporter that orchestrates receivers, processors, and output.
type AmperfExporter struct {
	config     *Config
	dataLock   sync.Mutex
	receivers  []Receiver
	processors []func([]common.MetricItem) ([]common.MetricItem, error)

	proxy         *OutputProxy
	metricsLocalC []common.MetricItem
	metricsLocalD []common.MetricItem

	sysInfo     *SystemInfo
	collectChan chan bool
	detectChan  chan bool
}

func NewAmperfExporter(proxycfg *OutputProxyConfig, sysInfo *SystemInfo) (*AmperfExporter, error) {
	var err error

	counters, err := GenerateCounters(proxycfg.config, false)
	if err != nil {
		return nil, err
	}

	receiverTypes := []string{ReceiverTypeGrpc}
	logrus.Infof("receivers: %v", receiverTypes)
	xptHandle := &AmperfExporter{
		config:      proxycfg.config,
		sysInfo:     sysInfo,
		collectChan: make(chan bool, 10),
		detectChan:  make(chan bool, 10),
	}

	receivers := make([]Receiver, 0)
	for _, receiverType := range receiverTypes {
		receiver, err := ReceiverFactory(receiverType, xptHandle.uploadMetrics, proxycfg.config, counters, &sysInfo.Host)
		if err != nil {
			return nil, err
		}
		receivers = append(receivers, receiver)
	}
	xptHandle.receivers = receivers
	err = xptHandle.UpdateGpuInfoByReceivers()
	if err != nil {
		logrus.Warningf("%v", err)
	} else {
		logrus.Infof("gpu infomation: ")
		logrus.Infof("    driver:%s", xptHandle.sysInfo.Gpu.Driver)
		for i := 0; i < xptHandle.sysInfo.Gpu.GpuCount; i++ {
			gpu := xptHandle.sysInfo.Gpu.Gpus[i]
			logrus.Infof("    Idx:%d, UUID:%s, Model:%s, Serial:%s", gpu.GPU, gpu.UUID, gpu.Model, gpu.Serial)
		}
	}
	hostInfoJSON, err := json.Marshal(sysInfo.Host)
	if err != nil {
		logrus.Info("Error marshalling to JSON:", err)
	} else {
		logrus.Infof("host information: %s", string(hostInfoJSON))
	}

	proxy, err := NewOutputProxy(proxycfg, xptHandle.sysInfo, counters)
	if err != nil {
		return nil, err
	}
	xptHandle.proxy = proxy

	processors := make([]func([]common.MetricItem) ([]common.MetricItem, error), 0)
	podMapper, err := NewPodMapper(proxycfg.config, proxy.GetFormatters(), &xptHandle.sysInfo.Gpu)
	if err != nil {
		logrus.Warningf("failed create pod mapper: %v", err)
	} else {
		processors = append(processors, podMapper.Process)
	}

	xptHandle.processors = processors
	return xptHandle, nil
}

func (xpt *AmperfExporter) UpdateGpuInfoByReceivers() error {
	var err error
	var gpuInfo *SystemGpuInfo
	for _, receiver := range xpt.receivers {
		gpuInfo, err = receiver.GetGpuInfo()
		if err != nil {
			continue
		} else {
			xpt.sysInfo.Gpu = *gpuInfo
			return nil // Only the first receiver (GrpcReceiver) takes effect
		}
	}

	return fmt.Errorf("failed to initialze gpu info by receiver: %v", err)
}

func (xpt *AmperfExporter) updateMetricCache(metrics interface{}, dataType int32, dataSrc int32) error {
	xpt.dataLock.Lock()
	defer xpt.dataLock.Unlock()
	switch dataType {
	case common.CollectDataType:
		switch dataSrc {
		case common.LocalDataSource:
			xpt.metricsLocalC = metrics.([]common.MetricItem)
		default:
			return fmt.Errorf("unknown data source:%d", dataType)
		}
	case common.DetectDataType:
		switch dataSrc {
		case common.LocalDataSource:
			xpt.metricsLocalD = metrics.([]common.MetricItem)
		default:
			return fmt.Errorf("unknown data source:%d", dataType)
		}
	default:
		return fmt.Errorf("unknown data type:%d", dataType)
	}
	return nil
}

func (xpt *AmperfExporter) uploadMetrics(metrics interface{}, dataType int32, dataSrc int32, reportType int32) {
	if reportType == common.ReportTypeEvent { // Event type does not require cache, handle separately
		switch dataType {
		case common.DetectDataType:
			xpt.reportEventD(metrics)
		default:
			logrus.Warningf("unable to handle event for data type:%d", dataType)
		}
		return
	}

	err := xpt.updateMetricCache(metrics, dataType, dataSrc)
	if err != nil {
		logrus.Warningf("failed update metric cache: %v", err)
		return
	}
	switch dataType {
	case common.CollectDataType:
		xpt.collectChan <- true
	case common.DetectDataType:
		xpt.detectChan <- true
	default:
		logrus.Warningf("Unknown data type:%d", dataType)
	}
}

func (xpt *AmperfExporter) copyMetrics(sources ...[]common.MetricItem) []common.MetricItem {
	totalLen := 0
	for _, s := range sources {
		totalLen += len(s)
	}
	metrics := make([]common.MetricItem, totalLen)

	// Deep copy
	index := 0
	for _, s := range sources {
		if len(s) > 0 {
			copy(metrics[index:index+len(s)], s)
		}
		index += len(s)
	}

	return metrics
}

func (xpt *AmperfExporter) reportMetricsD() {
	xpt.dataLock.Lock()
	metrics := xpt.copyMetrics(xpt.metricsLocalD)
	xpt.dataLock.Unlock()
	var err error
	if len(xpt.processors) > 0 {
		for _, processFunc := range xpt.processors {
			metrics, err = processFunc(metrics)
			if err != nil {
				logrus.Warningf("failed process detector metrics: %v", err)
			}
		}
	}

	xpt.proxy.DoMetricReport(common.DetectDataType, metrics)
}

func (xpt *AmperfExporter) reportEventD(metrics interface{}) {
	var err error
	metricsEvent := metrics.([]common.MetricItem)
	if len(xpt.processors) > 0 {
		for _, processFunc := range xpt.processors {
			metrics, err = processFunc(metricsEvent)
			if err != nil {
				logrus.Warningf("failed process detector metrics: %v", err)
			}
		}
	}

	xpt.proxy.DoEventReport(common.DetectDataType, metricsEvent)
}

func (xpt *AmperfExporter) reportMetricsC() {
	xpt.dataLock.Lock()
	metrics := xpt.copyMetrics(xpt.metricsLocalC)
	xpt.dataLock.Unlock()
	var err error
	if len(xpt.processors) > 0 {
		for _, processFunc := range xpt.processors {
			metrics, err = processFunc(metrics)
			if err != nil {
				logrus.Warningf("failed process collector metrics: %v", err)
			}
		}
	}

	xpt.proxy.DoMetricReport(common.CollectDataType, metrics)
}

func (xpt *AmperfExporter) appenderLoop(stopChan chan interface{}, local *sync.WaitGroup) {
	defer local.Done()
	for {
		select {
		case <-stopChan:
			return
		case <-xpt.collectChan:
			xpt.reportMetricsC()
		case <-xpt.detectChan:
			xpt.reportMetricsD()
		}
	}
}

func (xpt *AmperfExporter) Run(stopChan chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()

	var local sync.WaitGroup
	local.Add(1)
	go xpt.appenderLoop(stopChan, &local)

	for _, receiver := range xpt.receivers {
		err := receiver.Start()
		if err != nil {
			logrus.Fatalf("failed to start receiver %s: %v", receiver.Name(), err)
		}
	}

	<-stopChan

	for _, receiver := range xpt.receivers {
		err := receiver.Stop()
		if err != nil {
			logrus.Fatalf("failed to stop receiver %s: %v", receiver.Name(), err)
		}
	}

	if err := common.WaitWithTimeout(&local, 3*time.Second); err != nil {
		logrus.Fatalf("Failed waiting for exporter to shutdown, with err: `%v`", err)
	}
}

func (xpt *AmperfExporter) GpuChangeDetectRun(stop chan interface{}, wg *sync.WaitGroup, changeChan chan interface{}) {
	defer wg.Done()

	if xpt.config.GpuChangeDetectInterval < 0 {
		logrus.Warningf("system info change detect interval less than 0, change detector terminated.")
		return
	}
	logrus.Infof("Start system change detect, interval %d second", xpt.config.GpuChangeDetectInterval)
	ticker := time.NewTicker(time.Second * time.Duration(xpt.config.GpuChangeDetectInterval))
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			oldGpuInfo := *&xpt.sysInfo.Gpu
			err := xpt.UpdateGpuInfoByReceivers() // new gpu info
			if err != nil {
				continue
			}
			if checkGpuInfoChange(&xpt.sysInfo.Gpu, &oldGpuInfo) {
				logrus.Warning("system info change detected:")
				logrus.Warningf("  Old info %v", oldGpuInfo)
				logrus.Warningf("  New info %v", xpt.sysInfo.Gpu)
				changeChan <- true
			}
		}
	}
}
