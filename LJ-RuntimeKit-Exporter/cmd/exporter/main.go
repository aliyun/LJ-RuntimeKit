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

package main

import (
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

	"github.com/lj-runtimekit/exporter/pkg/common"
	"github.com/lj-runtimekit/exporter/pkg/exporter"
)

var (
	BuildVersion = "Filled by the build system"
)

const (
	CLIFieldsFile       = "collectors"
	CLIListenAddr       = "address"
	CLICollectInterval  = "collect-interval"
	CLIDataAcqMode      = "data-acq-mode"
	CLIPodCollectType   = "pod-collector-type"
	CLIKubeletPort      = "kubelet-port"
	CLIOutputFormat     = "output-format"
	CLIOutputType       = "output-type"
	CLIXdmGrpcPath      = "xdm-grpc-path"
	CLIGpuCheckInterval = "gpu-change-interval"
)

func main() {
	common.SetupLogger("exporter")
	exporter.LoadProtocol()
	c := cli.NewApp()
	c.Name = "Amperf-exporter"
	c.Usage = "Exposes fine-grained GPU monitoring indicators"
	c.Version = BuildVersion

	c.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    CLIFieldsFile,
			Aliases: []string{"f"},
			Usage:   "Path to the file, that contains the fields to collect",
			Value:   "default-counters.csv",
			EnvVars: []string{"AMPERF_COLLECTORS_FILE"},
		},
		&cli.StringFlag{
			Name:    CLIListenAddr,
			Aliases: []string{"a"},
			Value:   ":9400",
			Usage:   "Address",
			EnvVars: []string{"AMPERF_EXPORTER_LISTEN"},
		},
		&cli.IntFlag{
			Name:    CLICollectInterval,
			Aliases: []string{"c"},
			Value:   30000,
			Usage:   "Interval of time at which point metrics are collected(ms).",
			EnvVars: []string{"AMPERF_EXPORTER_INTERVAL"},
		},
		&cli.StringFlag{
			Name:    CLIDataAcqMode,
			Usage:   "Data acquisition mode",
			Value:   exporter.DataAcqPush,
			EnvVars: []string{"AMPERF_DATA_ACQ_MODE"},
		},
		&cli.StringFlag{
			Name:    CLIPodCollectType,
			Usage:   "Specify the type to collect pod info. Possible values: 'none', 'kubelet-http'",
			Value:   exporter.PodCollectTypeKubeletHttp,
			EnvVars: []string{"AMPERF_POD_COLLECTOR_TYPE"},
		},
		&cli.StringFlag{
			Name:    CLIOutputFormat,
			Value:   exporter.OutputFmtKubergpu,
			Usage:   "Specify the output format. Possible values: 'prom', 'kubergpu'",
			EnvVars: []string{"AMPERF_OUTPUT_FORMAT"},
		},
		&cli.StringFlag{
			Name:    CLIOutputType,
			Usage:   "Specify the output type. current support values: 'http-pull'",
			Value:   exporter.OutputTypeHttpPull,
			EnvVars: []string{"AMPERF_OUTPUT_TYPE"},
		},
		&cli.UintFlag{
			Name:    CLIKubeletPort,
			Aliases: []string{"P"},
			Value:   10250,
			Usage:   "Kubelet communication port",
			EnvVars: []string{"AMPERF_KUBELET_PORT"},
		},
		&cli.StringFlag{
			Name:    CLIXdmGrpcPath,
			Value:   "/run/amp/eKitC/xdm/sockets/dp2mgr.sock",
			Usage:   "The path of uds to connect the collector",
			EnvVars: []string{"AMPERF_GRPC_PATH"},
		},
		&cli.IntFlag{
			Name:    CLIGpuCheckInterval,
			Value:   300,
			Usage:   "Interval of time to check if gpu info has change (in second).",
			EnvVars: []string{"AMPERF_GPU_CHANGE_CHECK_INTERVAL"},
		},
	}

	c.Action = func(c *cli.Context) error {
		return Run(c)
	}

	if err := c.Run(os.Args); err != nil {
		logrus.Fatal(err)
	}
}

func Run(c *cli.Context) error {
restart:
	config, err := contextToConfig(c)
	if err != nil {
		logrus.Warning("Parse config error:", err)
		return err
	}

	sysInfo := exporter.GetSystemInfo()
	var xptHandle *exporter.AmperfExporter
	var server *exporter.MetricsServer

	proxyConfig, err := exporter.NewOutputProxyConfig(config)
	if err != nil {
		logrus.Fatal(err)
	}

	if proxyConfig.HasOutputType(exporter.OutputTypeHttpPull) {
		chMetrics := make(chan common.FormattedData, 20)
		server, err = exporter.NewMetricsServer(config, chMetrics)
		if err != nil {
			logrus.Fatal(err)
		}
		proxyConfig.RegisterHttpServer(chMetrics)
	}

	xptHandle, err = exporter.NewAmperfExporter(proxyConfig, sysInfo)
	if err != nil {
		logrus.Fatal(err)
	}

	var wg sync.WaitGroup
	stop := make(chan interface{})

	if server != nil {
		wg.Add(1)
		go server.Run(stop, &wg)
	}

	wg.Add(1)
	go xptHandle.Run(stop, &wg)

	restartChan := make(chan interface{})

	wg.Add(1)
	go xptHandle.GpuChangeDetectRun(stop, &wg, restartChan)

	sigs := common.NewOSWatcher(syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGUSR1)
	logrus.Info("Amperf exporter successfully initialized!")
	for {
		select {
		case sig := <-sigs:
			switch sig {
			case syscall.SIGUSR1: // Handle log level adjustment signal
				if logLevel, err := common.GetLogLevel(common.ProcessExporter); err != nil {
					logrus.Warning(err)
				} else {
					logrus.SetLevel(logLevel)
					fmt.Printf("Log level for process '%s' was successfully set to: %s\n",
						string(common.ProcessExporter), logLevel)
				}
				continue
			}

			// Exit the program
			logrus.Println("Receive signal", sig)
			close(stop)
			err := common.WaitWithTimeout(&wg, time.Second*3)
			if err != nil {
				logrus.Fatalf("Exporter main loop shutdown with err: `%v`", err)
			}

			return nil
		case <-restartChan:
			logrus.Warningf("GPU info change detected")
			goto restart
		}
	}
}

func contextToConfig(c *cli.Context) (*exporter.Config, error) {
	fieldsFile := c.String(CLIFieldsFile)

	return &exporter.Config{
		CollectorsFile:           fieldsFile,
		Address:                  c.String(CLIListenAddr),
		CollectInterval:          c.Int(CLICollectInterval),
		ExporterDataAcqMode:      c.String(CLIDataAcqMode),
		ExporterPodCollectorType: c.String(CLIPodCollectType),
		ExporterOutputFormat:     c.String(CLIOutputFormat),
		ExporterOutputType:       c.String(CLIOutputType),
		ExporterKubeletPort:      c.Uint(CLIKubeletPort),
		XdmGrpcPath:              c.String(CLIXdmGrpcPath),
		GpuChangeDetectInterval:  c.Int(CLIGpuCheckInterval),
	}, nil
}
