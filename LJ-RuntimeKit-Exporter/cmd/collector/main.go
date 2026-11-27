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

// Command collector profiles one target pod and writes its data directly to a PVC.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	pb "github.com/lj-runtimekit/exporter/api/rtkit/profiling"
	"github.com/lj-runtimekit/exporter/pkg/collector"
)

const (
	exitSucceeded   = 0
	exitFailed      = 1
	exitInvalidArgs = 2
)

var BuildVersion = "dev"

var log = slog.New(slog.NewJSONHandler(os.Stderr, nil))

func main() { os.Exit(run(os.Args[1:])) }

func run(args []string) int {
	terminationPath := reportPath(args)
	if len(args) == 0 {
		usage()
		return writeReport(terminationPath, collector.Failed(collector.ReasonInvalidArgs, "collect subcommand is required"), exitInvalidArgs)
	}
	switch args[0] {
	case "collect":
		return runCollect(args[1:])
	case "version", "--version":
		fmt.Fprintln(os.Stdout, BuildVersion)
		return exitSucceeded
	case "help", "--help", "-h":
		usage()
		return exitSucceeded
	default:
		usage()
		return writeReport(terminationPath, collector.Failed(collector.ReasonInvalidArgs, fmt.Sprintf("unknown subcommand %q", args[0])), exitInvalidArgs)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "Usage: profiling-collector collect [flags]")
	fmt.Fprintln(os.Stderr, "  collect  profile one pod's containers directly into an absolute PVC output directory")
	fmt.Fprintln(os.Stderr, "  version  print the build version (also --version)")
}

func runCollect(args []string) int {
	var cfg collector.Config
	var ids, mode string
	terminationPath := reportPath(args)
	fs := flag.NewFlagSet("profiling-collector collect", flag.ContinueOnError)
	fs.StringVar(&cfg.SocketPath, "socket", collector.DefaultSocketPath, "Runtime Kit unix socket")
	fs.StringVar(&cfg.PodNamespace, "pod-namespace", "", "target pod namespace")
	fs.StringVar(&cfg.PodName, "pod-name", "", "target pod name")
	fs.StringVar(&cfg.PodUID, "pod-uid", "", "target pod UID")
	fs.StringVar(&ids, "container-ids", "", "comma-separated target container IDs")
	fs.StringVar(&mode, "mode", "light", "profiling mode: light or full")
	fs.DurationVar(&cfg.Duration, "duration", time.Minute, "collection duration")
	fs.StringVar(&cfg.OutputDir, "output-dir", "", "absolute PVC mount subdirectory")
	fs.DurationVar(&cfg.PollInterval, "poll-interval", collector.DefaultPollInterval, "Runtime Kit status poll interval")
	fs.DurationVar(&cfg.RPCTimeout, "rpc-timeout", collector.DefaultRPCTimeout, "Runtime Kit RPC timeout")
	fs.DurationVar(&cfg.StartTimeout, "start-timeout", collector.DefaultStartTimeout, "max wait for RUNNING or an already COMPLETED session")
	fs.DurationVar(&cfg.StreamIdleTimeout, "stream-idle-timeout", collector.DefaultStreamIdleTimeout, "fail the transfer after this long without receiving data or finishing a write; no total transfer limit")
	fs.DurationVar(&cfg.FinishTimeout, "finish-timeout", collector.DefaultFinishTimeout, "extra wait for COMPLETED beyond collection duration")
	fs.DurationVar(&cfg.StopTimeout, "stop-timeout", collector.DefaultStopTimeout, "independent StopProfiling timeout")
	fs.IntVar(&cfg.MaxRetries, "max-retries", collector.DefaultMaxRetries, "每个安全操作的额外重试次数，0 禁用")
	fs.DurationVar(&cfg.RetryBackoff, "retry-backoff", collector.DefaultRetryBackoff, "操作重试的初始退避")
	fs.DurationVar(&cfg.MaxRetryBackoff, "max-retry-backoff", collector.DefaultMaxRetryBackoff, "操作重试的最大退避")
	fs.IntVar(&cfg.StartBusyRetries, "start-busy-retries", collector.DefaultStartBusyRetries, "Start 返回 1006（无空闲 worker）时的等待重试次数")
	fs.DurationVar(&cfg.StartBusyBackoff, "start-busy-backoff", collector.DefaultStartBusyBackoff, "1006 等待重试的固定间隔")
	fs.StringVar(&terminationPath, "termination-message-path", terminationPath, "termination report path")
	// 默认开启：controller 保证同一目标串行且旧 Pod 已退出后才创建新 Pod，新 Pod 必须先清掉 daemon 里的残留会话。
	// 只有手工排查、确认无并行采集时才用 --cleanup-leftovers=false 关闭。
	fs.BoolVar(&cfg.CleanupLeftovers, "cleanup-leftovers", true, "stop leftover sessions before starting; requires serialized access to the target container")

	invalid := func(err error) int {
		res := collector.Failed(collector.ReasonInvalidArgs, err.Error())
		res.PodNamespace, res.PodName, res.PodUID = cfg.PodNamespace, cfg.PodName, cfg.PodUID
		return writeReport(reportPath(args), res, exitInvalidArgs)
	}
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return exitSucceeded
		}
		return invalid(err)
	}
	if fs.NArg() != 0 {
		return invalid(fmt.Errorf("unexpected positional arguments"))
	}
	if ids != "" {
		cfg.ContainerIDs = strings.Split(ids, ",")
	}
	var err error
	if cfg.Mode, err = parseMode(mode); err != nil {
		return invalid(err)
	}
	if err := cfg.Validate(); err != nil {
		return invalid(err)
	}
	if !filepath.IsAbs(terminationPath) {
		return invalid(fmt.Errorf("termination message path must be absolute"))
	}
	// Check the termination file before starting any Runtime Kit session. This
	// provisional failure also prevents an abrupt exit from leaving a success.
	initial := collector.Failed(collector.ReasonInterrupted, "collection did not finish")
	initial.PodNamespace, initial.PodName, initial.PodUID = cfg.PodNamespace, cfg.PodName, cfg.PodUID
	if err := collector.WriteTerminationMessage(terminationPath, initial); err != nil {
		log.Error("validate termination report file", "path", terminationPath, "err", err)
		return exitFailed
	}
	sigCtx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stopSignals()
	res := collector.Run(sigCtx, cfg, log)
	code := exitFailed
	if res.Phase == collector.PhaseSucceeded {
		code = exitSucceeded
	} else if res.Reason == collector.ReasonInvalidArgs {
		code = exitInvalidArgs
	}
	return writeReport(terminationPath, res, code)
}

func writeReport(path string, res collector.Result, code int) int {
	if err := collector.WriteTerminationMessage(path, res); err != nil {
		log.Error("write termination report", "path", path, "err", err)
		if code != exitInvalidArgs {
			return exitFailed
		}
	}
	return code
}

func reportPath(args []string) string {
	return stringFlag(args, "termination-message-path", "/dev/termination-log")
}

func stringFlag(args []string, name, fallback string) string {
	value := fallback
	for i, arg := range args {
		if !strings.HasPrefix(arg, "-") {
			continue
		}
		flagName := strings.TrimPrefix(strings.TrimPrefix(arg, "-"), "-")
		if v, ok := strings.CutPrefix(flagName, name+"="); ok {
			value = v
		} else if flagName == name && i+1 < len(args) {
			value = args[i+1]
		}
	}
	return value
}

// parseMode 将 CLI 模式名映射到协议枚举；LIGHT 仍为 0，FULL 仍为 1。
func parseMode(s string) (pb.ProfilingMode, error) {
	switch s {
	case "light":
		return pb.ProfilingMode_PROFILING_MODE_LIGHT, nil
	case "full":
		return pb.ProfilingMode_PROFILING_MODE_FULL, nil
	default:
		return 0, fmt.Errorf("invalid --mode %q, want light or full", s)
	}
}
