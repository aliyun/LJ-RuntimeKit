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
	"math/rand"
	"os"
	"time"

	"github.com/lj-runtimekit/scheduler/pkg/gang"
	"github.com/lj-runtimekit/scheduler/pkg/mig"
	"github.com/lj-runtimekit/scheduler/pkg/standard"
	"github.com/lj-runtimekit/scheduler/pkg/utils"

	"k8s.io/component-base/logs"
	"k8s.io/kubernetes/cmd/kube-scheduler/app"
)

func main() {
	rand.Seed(time.Now().UTC().UnixNano())
	utils.LoadProtocol()
	command := app.NewSchedulerCommand(
		app.WithPlugin(standard.Name, standard.New),
		app.WithPlugin(mig.Name, mig.New),
		app.WithPlugin(gang.Name, gang.New),
	)

	logs.InitLogs()
	defer logs.FlushLogs()

	if err := command.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}
