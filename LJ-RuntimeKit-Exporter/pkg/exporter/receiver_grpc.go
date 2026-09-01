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
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/lj-runtimekit/exporter/pkg/common"
	xdmgrpc "github.com/lj-runtimekit/exporter/pkg/grpc/xdm"

	"github.com/sirupsen/logrus"
)

const (
	ReceiverTypeGrpc = "GrpcReceiver"
)

// GrpcReceiver implements Receiver using gRPC streams for metric collection.
type GrpcReceiver struct {
	counters   []common.Counter
	collectCli *XdmGrpcClient
	detectCli  *XdmGrpcClient
	initC      bool
	initD      bool
	running        bool
	config         *Config
	stopChan       chan interface{}
	waitGroup      *sync.WaitGroup
	uploadCallback func(metrics interface{}, dataType int32, dataSrc int32, reportType int32)
}

func NewGrpcReceiver(config *Config, counters []common.Counter, hostInfo *HostInfo, uploadCallback func(metrics interface{}, dataType int32, dataSrc int32, reportType int32)) (*GrpcReceiver, error) {
	var err error
	var cClient *XdmGrpcClient
	var dClient *XdmGrpcClient

	fieldByCounterC := NewCollectorCounters(counters)
	fieldByCounterD := NewDetectorCounters(counters)

	if len(fieldByCounterC) == 0 {
		logrus.Warning("not specify collector field")
	}
	if len(fieldByCounterD) == 0 {
		logrus.Warning("not specify detector field")
	}

	cClient, err = NewXdmGrpcClient(config, hostInfo, xdmgrpc.ExporterType_Collector)
	if err != nil {
		return nil, err
	}

	dClient, err = NewXdmGrpcClient(config, hostInfo, xdmgrpc.ExporterType_Detector)
	if err != nil {
		return nil, err
	}

	return &GrpcReceiver{
		config:         config,
		counters:       counters,
		collectCli:     cClient,
		detectCli:      dClient,
		stopChan:       make(chan interface{}),
		waitGroup:      &sync.WaitGroup{},
		uploadCallback: uploadCallback,
	}, nil
}

func (rcv *GrpcReceiver) Name() string {
	return ReceiverTypeGrpc
}

func (rcv *GrpcReceiver) getGrpcSupportedCounters(dataType int32) ([]int32, map[int32][]common.Counter, GrpcErrCode) {
	var client *XdmGrpcClient
	var counterMaps map[int32][]common.Counter

	if dataType == common.CollectDataType {
		client = rcv.collectCli
		counterMaps = NewCollectorCounters(rcv.counters)
	} else if dataType == common.DetectDataType {
		client = rcv.detectCli
		counterMaps = NewDetectorCounters(rcv.counters)
	} else {
		return nil, nil, GrpcErrCodeUnExpected
	}

	config, ret := client.GetConfig()
	if ret != GrpcRetSuccess {
		return nil, nil, ret
	}

	countersRet := make([]int32, 0)
	counterMapsRet := make(map[int32][]common.Counter)
	for _, f := range config.SuptFieldID {
		if _, ok := counterMaps[f]; ok {
			countersRet = append(countersRet, f)
			counterMapsRet[f] = counterMaps[f]
		}
	}

	return countersRet, counterMapsRet, GrpcRetSuccess
}

func (rcv *GrpcReceiver) initCollectStreamHandle() (*MetricStream, func(), GrpcErrCode) {
	counters, counterMaps, ret := rcv.getGrpcSupportedCounters(common.CollectDataType)
	if ret != GrpcRetSuccess {
		return nil, nil, ret
	}

	rcv.collectCli.RegisterCounters(counters, counterMaps)
	rcv.initC = true
	fieldIds := make([]int32, 0)
	for fieldId := range rcv.collectCli.counterMaps {
		fieldIds = append(fieldIds, fieldId)
	}

	return rcv.collectCli.GetMetricsStream(fieldIds, counterMaps)
}

func (rcv *GrpcReceiver) collectStreamLoop(stopChan chan interface{}, local *sync.WaitGroup) {
	defer local.Done()
	for {
	restart:
		select {
		case <-stopChan:
			return
		default:
		}

		stream, cleanup, ret := rcv.initCollectStreamHandle()
		if ret == GrpcRetSuccess {
			logrus.Info("Get collector stream success")
		} else {
			time.Sleep(time.Second)
			continue
		}

		for {
			select {
			case <-stopChan:
				cleanup()
				return
			default:
				metrics, err := stream.Read()
				if err == io.EOF {
					cleanup()
					rcv.uploadCallback([]common.MetricItem{}, common.CollectDataType, common.LocalDataSource, common.ReportTypeMetric) // clear cache
					goto restart
				}
				if err != nil {
					logrus.Warningf("recv err :%v ", err)
					cleanup()
					rcv.uploadCallback([]common.MetricItem{}, common.CollectDataType, common.LocalDataSource, common.ReportTypeMetric) // clear cache
					goto restart
				}
				rcv.uploadCallback(metrics, common.CollectDataType, common.LocalDataSource, common.ReportTypeMetric)
			}
		}
	}
}

func (rcv *GrpcReceiver) getCollectorMetrics() interface{} {
	if rcv.collectCli != nil {
		me, ret := rcv.collectCli.GetMetrics()
		if ret == GrpcRetSuccess {
			return me
		} else if ret == GrpcErrCodeSrvUnavailable {
			logrus.Errorf("grpc get collect failed: %s", grpcErrString(ret))
			rcv.initC = false
			return []common.MetricItem{}
		} else {
			rcv.initC = false
			logrus.Errorf("grpc get collect failed: %s", grpcErrString(ret))
			return []common.MetricItem{}
		}
	}

	rcv.initC = false
	return []common.MetricItem{}
}

func (rcv *GrpcReceiver) getToMetricsC() {
	metrics := rcv.getCollectorMetrics().([]common.MetricItem)
	rcv.uploadCallback(metrics, common.CollectDataType, common.LocalDataSource, common.ReportTypeMetric)
}

func (rcv *GrpcReceiver) collectLoop(stopChan chan interface{}, local *sync.WaitGroup) {
	defer local.Done()
	c := time.NewTicker(time.Millisecond * time.Duration(rcv.config.CollectInterval))
	defer c.Stop()

	rcv.initC = false
	for {
		select {
		case <-stopChan:
			return
		case <-c.C:
			if !rcv.initC {
				counters, counterMaps, ret := rcv.getGrpcSupportedCounters(common.CollectDataType)
				if ret != GrpcRetSuccess {
					continue
				}

				rcv.collectCli.RegisterCounters(counters, counterMaps)
				rcv.initC = true
				logrus.Info("collectLoop init success")
			} else {
				rcv.getToMetricsC()
			}
		}
	}
}

func (rcv *GrpcReceiver) getDetectorMetrics() interface{} {
	if rcv.detectCli != nil {
		me, ret := rcv.detectCli.GetMetrics()
		if ret == GrpcRetSuccess {
			return me
		} else if ret == GrpcErrCodeSrvUnavailable {
			logrus.Errorf("grpc get detect failed: %s", grpcErrString(ret))
			rcv.initD = false
			return []common.MetricItem{}
		} else {
			rcv.initD = false
			logrus.Errorf("grpc get detect failed: %s", grpcErrString(ret))
			return []common.MetricItem{}
		}
	}

	return []common.MetricItem{}
}

func (rcv *GrpcReceiver) getToMetricsD() {
	metricD := rcv.getDetectorMetrics().([]common.MetricItem)
	rcv.uploadCallback(metricD, common.DetectDataType, common.LocalDataSource, common.ReportTypeMetric)
}

func (rcv *GrpcReceiver) detectLoop(stopChan chan interface{}, local *sync.WaitGroup) {
	defer local.Done()
	c := time.NewTicker(time.Millisecond * time.Duration(rcv.config.CollectInterval))
	defer c.Stop()

	for {
		select {
		case <-stopChan:
			return
		case <-c.C:
			if !rcv.initD {
				counters, counterMaps, ret := rcv.getGrpcSupportedCounters(common.DetectDataType)
				if ret != GrpcRetSuccess {
					continue
				}

				rcv.detectCli.RegisterCounters(counters, counterMaps)
				rcv.initD = true
				logrus.Info("detectLoop init success")
			} else {
				rcv.getToMetricsD()
			}
		}
	}
}

func (rcv *GrpcReceiver) initDetectorStreamHandle() (*MetricStream, func(), GrpcErrCode) {
	counters, counterMaps, ret := rcv.getGrpcSupportedCounters(common.DetectDataType)
	if ret != GrpcRetSuccess {
		return nil, nil, ret
	}

	rcv.detectCli.RegisterCounters(counters, counterMaps)
	rcv.initD = true
	fieldIds := make([]int32, 0)
	for fieldId := range rcv.detectCli.counterMaps {
		fieldIds = append(fieldIds, fieldId)
	}

	return rcv.detectCli.GetMetricsStream(fieldIds, counterMaps)
}

func (rcv *GrpcReceiver) detectorStreamLoop(stopChan chan interface{}, local *sync.WaitGroup) {
	defer local.Done()
	for {
	restart:
		select {
		case <-stopChan:
			return
		default:
		}

		stream, cleanup, ret := rcv.initDetectorStreamHandle()
		if ret == GrpcRetSuccess {
			logrus.Info("Get detector stream success")
		} else {
			time.Sleep(time.Second)
			continue
		}

		for {
			select {
			case <-stopChan:
				cleanup()
				return
			default:
				metrics, err := stream.Read()
				if err != nil {
					logrus.Warningf("recv err :%v ", err)
					cleanup()
					rcv.uploadCallback([]common.MetricItem{}, common.DetectDataType, common.LocalDataSource, common.ReportTypeMetric)
					rcv.initD = false
					goto restart
				}
				rcv.uploadCallback(metrics, common.DetectDataType, common.LocalDataSource, common.ReportTypeMetric)
			}
		}
	}
}

func (rcv *GrpcReceiver) IsRunning() bool {
	return rcv.running
}

func (rcv *GrpcReceiver) Start() error {
	if rcv.IsRunning() {
		return fmt.Errorf("%s is already running", rcv.Name())
	}
	rcv.running = true
	if rcv.collectCli != nil {
		rcv.waitGroup.Add(1)
		if rcv.config.ExporterDataAcqMode == DataAcqPull {
			go rcv.collectLoop(rcv.stopChan, rcv.waitGroup)
		} else if rcv.config.ExporterDataAcqMode == DataAcqPush {
			go rcv.collectStreamLoop(rcv.stopChan, rcv.waitGroup)
		} else {
			rcv.waitGroup.Done()
			logrus.Warning("Error data acquire mode:", rcv.config.ExporterDataAcqMode)
		}
	}

	if rcv.detectCli != nil {
		rcv.waitGroup.Add(1)
		if rcv.config.ExporterDataAcqMode == DataAcqPull {
			go rcv.detectLoop(rcv.stopChan, rcv.waitGroup)
		} else if rcv.config.ExporterDataAcqMode == DataAcqPush {
			go rcv.detectorStreamLoop(rcv.stopChan, rcv.waitGroup)
		} else {
			rcv.waitGroup.Done()
			logrus.Warning("Error data acquire mode:", rcv.config.ExporterDataAcqMode)
		}
	}

	return nil
}

func (rcv *GrpcReceiver) Stop() error {
	rcv.running = false
	if rcv.collectCli != nil {
		defer rcv.collectCli.conn.Close()
	}
	if rcv.detectCli != nil {
		defer rcv.detectCli.conn.Close()
	}

	close(rcv.stopChan)
	if err := common.WaitWithTimeout(rcv.waitGroup, 3*time.Second); err != nil {
		return fmt.Errorf("failed waiting for %s to stop, with err: `%v`", rcv.Name(), err)
	}
	return nil
}

func (rcv *GrpcReceiver) GetGpuInfo() (*SystemGpuInfo, error) {
	if rcv.collectCli == nil && rcv.detectCli == nil {
		return nil, fmt.Errorf("amperf clients is all nil")
	}
	retry := 5
	delayFactor := 10
	var sysInfoCol, sysInfoDet *SystemGpuInfo
	var grpcCodeCol, grpcCodeDet GrpcErrCode
	var unImpCol, unImpDet bool
	for i := 0; i < retry; i++ {
		time.Sleep(time.Duration(i*delayFactor) * time.Second)
		if i > 0 {
			logrus.Warnf("Retry #%d to get system info", i)
		}
		sysInfoCol, grpcCodeCol = getSystemInfoByGrpc(rcv.collectCli)
		if sysInfoCol != nil {
			return sysInfoCol, nil
		} else if grpcCodeCol == GrpcErrCodeUnimplemented {
			unImpCol = true
		}
		sysInfoDet, grpcCodeDet = getSystemInfoByGrpc(rcv.detectCli)
		if sysInfoDet != nil {
			return sysInfoDet, nil
		} else if grpcCodeDet == GrpcErrCodeUnimplemented {
			unImpDet = true
		}
		logrus.Warningf("failed to initialize system info: collector err:%s; detector err:%s",
			grpcErrString(grpcCodeCol), grpcErrString(grpcCodeDet))
		if unImpCol && unImpDet {
			break
		}
	}
	return nil, fmt.Errorf("failed to initialize system info: collector err:%s; detector err:%s",
		grpcErrString(grpcCodeCol), grpcErrString(grpcCodeDet))
}
