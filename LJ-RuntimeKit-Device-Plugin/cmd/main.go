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
	"flag"
	"fmt"
	"os"
	"time"

	_ "github.com/lj-runtimekit/device-plugin/internal/feature"
	xpuserver "github.com/lj-runtimekit/device-plugin/internal/gpu"
	"github.com/lj-runtimekit/device-plugin/internal/utils"

	"github.com/urfave/cli"
	"k8s.io/klog/v2"
)

type options struct {
	flags         []cli.Flag
	memoryUnit    string
	featureGates  string
	logDir        string
	logFile       string
	logToStderr   bool
	verbosity     int
	checkReady    bool
	retryInterval time.Duration
}

func main() {
	c := cli.NewApp()
	o := &options{}

	c.Name = "LJ RuntimeKit Device Plugin"
	c.Usage = "LJ RuntimeKit Device Plugin for Kubernetes"
	c.Action = func(ctx *cli.Context) error {
		if o.checkReady {
			runHealthCheck(o.retryInterval)
			os.Exit(0)
		}
		return start(ctx, o)
	}

	c.Before = func(c *cli.Context) error {
		// Load cross-component protocol constants from the mounted ConfigMap
		// (falls back to compiled-in defaults when absent) before any business
		// logic reads them.
		utils.LoadProtocol()
		klog.InitFlags(nil)
		if dir := c.String("log_dir"); dir != "" {
			flag.CommandLine.Set("log_dir", dir)
		}

		if file := c.String("log_file"); file != "" {
			flag.CommandLine.Set("log_file", file)
		}

		toStderr := c.Bool("logtostderr")
		flag.CommandLine.Set("logtostderr", fmt.Sprintf("%t", toStderr))

		flag.CommandLine.Set("v", fmt.Sprintf("%d", o.verbosity))
		flag.CommandLine.Parse(nil)

		if val := c.String("feature-gates"); val != "" {
			if err := utils.DefaultMutableFeatureGate.Set(val); err != nil {
				return fmt.Errorf("invalid feature-gates configuration: %w", err)
			}
		}

		return nil
	}

	c.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:        "memory-unit",
			Value:       "GiB",
			Usage:       "the desired unit for exposing GPU memory: [GiB | 128MiB]",
			Destination: &o.memoryUnit,
		},
		&cli.StringFlag{
			Name:        "feature-gates",
			Value:       "",
			Usage:       "a set of key=value pairs that describe feature gates for alpha/experimental features. Options are: \n" + utils.GetAllFeatureGatesDescriptions(),
			Destination: &o.featureGates,
		},
		&cli.StringFlag{
			Name:        "log_dir",
			Value:       "",
			Usage:       "the desired directory for log files",
			Destination: &o.logDir,
		},
		&cli.StringFlag{
			Name:        "log_file",
			Value:       "",
			Usage:       "the desired file name for log files",
			Destination: &o.logFile,
		},
		&cli.BoolFlag{
			Name:        "logtostderr",
			Usage:       "log to standard error instead of files",
			Destination: &o.logToStderr,
		},
		&cli.IntFlag{
			Name:        "v",
			Value:       0,
			Usage:       "number for the log level verbosity",
			Destination: &o.verbosity,
		},
		&cli.BoolFlag{
			Name:        "check-ready",
			Usage:       "run health check to verify x-device-manager is ready, then exit",
			Destination: &o.checkReady,
		},
		&cli.DurationFlag{
			Name:        "retry-interval",
			Value:       500 * time.Millisecond,
			Usage:       "retry interval for health check",
			Destination: &o.retryInterval,
		},
	}

	o.flags = c.Flags

	err := c.Run(os.Args)
	if err != nil {
		klog.Error(err)
		os.Exit(1)
	}
}

func start(c *cli.Context, o *options) error {
	klog.Infof("starting LJ RuntimeKit Device Plugin server")

	memoryUnit := utils.MemoryUnit(o.memoryUnit)
	switch memoryUnit {
	case utils.GiBPrefix:
	case utils.MiBPrefix:
	default:
		klog.Warningf("unsupported memory unit: %s, falling back to GiB", o.memoryUnit)
		memoryUnit = utils.GiBPrefix
	}

	dm := xpuserver.NewDeviceManager(memoryUnit)
	err := dm.Run()
	if err != nil {
		klog.Fatalf("failed to start device manager: %v", err)
	}

	return nil
}
