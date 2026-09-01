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
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	// Label name that drives enable/disable actions.
	gpuLabel = "lj_runtimekit_enable"

	// Annotation keys tracked by AMP-Controller.
	lastActionAnnotation       = "lj-runtimekit-controller/last-action"
	lastActionStatusAnnotation = "lj-runtimekit-controller/last-action-status"

	// Job labels set by AMP-Controller.
	jobLabelNode   = "lj-runtimekit-controller/target-node"
	jobLabelAction = "lj-runtimekit-controller/action"
	managedByLabel = "app.kubernetes.io/managed-by"
	managedByValue = "lj-runtimekit-controller"
)

func main() {
	var (
		kubeconfig  = flag.String("kubeconfig", "", "Path to a kubeconfig file. If unset, the KUBECONFIG env var or default kubeconfig path is used.")
		namespace   = flag.String("namespace", "lj-runtimekit-system", "Namespace where AMP-Controller Jobs run.")
		allNodes    = flag.Bool("all", false, "Show all nodes, including completed ones.")
		showHelp    = flag.Bool("help", false, "Show help.")
		showVersion = flag.Bool("version", false, "Show version.")
	)
	flag.Parse()

	if *showHelp {
		fmt.Fprintf(os.Stdout, "Usage: kubectl amp status [options]\n\n")
		flag.PrintDefaults()
		os.Exit(0)
	}

	if *showVersion {
		fmt.Println("kubectl-amp v0.0.1")
		os.Exit(0)
	}

	if err := run(*kubeconfig, *namespace, *allNodes); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run(kubeconfig, namespace string, allNodes bool) error {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfig != "" {
		loadingRules.ExplicitPath = kubeconfig
	}

	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		loadingRules,
		&clientcmd.ConfigOverrides{},
	).ClientConfig()
	if err != nil {
		return fmt.Errorf("failed to build kubeconfig: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	ctx := context.Background()

	nodeList, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list nodes: %w", err)
	}

	jobList, err := clientset.BatchV1().Jobs(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(map[string]string{
			managedByLabel: managedByValue,
		}).String(),
	})
	if err != nil {
		return fmt.Errorf("failed to list jobs: %w", err)
	}

	jobsByNode := make(map[string][]batchv1.Job)
	for _, job := range jobList.Items {
		nodeName := job.Labels[jobLabelNode]
		if nodeName != "" {
			jobsByNode[nodeName] = append(jobsByNode[nodeName], job)
		}
	}

	rows := make([]row, 0, len(nodeList.Items))
	for _, node := range nodeList.Items {
		desired := desiredAction(&node)
		lastAction := node.Annotations[lastActionAnnotation]
		lastStatus := node.Annotations[lastActionStatusAnnotation]

		if !allNodes && !isIncomplete(&node, desired) && len(jobsByNode[node.Name]) == 0 {
			continue
		}

		jobStatus, jobAge := summarizeJobs(jobsByNode[node.Name])
		rows = append(rows, row{
			Node:       node.Name,
			Desired:    desired,
			LastAction: lastAction,
			LastStatus: lastStatus,
			JobStatus:  jobStatus,
			Age:        jobAge,
		})
	}

	if len(rows) == 0 {
		fmt.Println("No incomplete AMP actions found.")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "NODE\tDESIRED\tLAST_ACTION\tLAST_STATUS\tJOB_STATUS\tAGE")
	for _, r := range rows {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
			r.Node, r.Desired, r.LastAction, r.LastStatus, r.JobStatus, r.Age)
	}
	return w.Flush()
}

type row struct {
	Node       string
	Desired    string
	LastAction string
	LastStatus string
	JobStatus  string
	Age        string
}

func desiredAction(node *corev1.Node) string {
	if v, ok := node.Labels[gpuLabel]; ok && strings.EqualFold(v, "true") {
		return "enable"
	}
	return "disable"
}

func isIncomplete(node *corev1.Node, desired string) bool {
	lastAction := node.Annotations[lastActionAnnotation]
	lastStatus := node.Annotations[lastActionStatusAnnotation]

	if desired == "enable" {
		return lastAction != "enable" || lastStatus != "success"
	}

	// desired == "disable": only incomplete if we previously enabled and it hasn't succeeded.
	if lastAction == "enable" {
		return lastStatus != "success"
	}
	return false
}

func summarizeJobs(jobs []batchv1.Job) (string, string) {
	if len(jobs) == 0 {
		return "-", "-"
	}

	// Use the newest job for age.
	newest := jobs[0]
	for _, job := range jobs[1:] {
		if job.CreationTimestamp.After(newest.CreationTimestamp.Time) {
			newest = job
		}
	}

	statusParts := make([]string, 0, 3)
	if newest.Status.Active > 0 {
		statusParts = append(statusParts, fmt.Sprintf("Active:%d", newest.Status.Active))
	}
	if newest.Status.Succeeded > 0 {
		statusParts = append(statusParts, fmt.Sprintf("Succeeded:%d", newest.Status.Succeeded))
	}
	if newest.Status.Failed > 0 {
		statusParts = append(statusParts, fmt.Sprintf("Failed:%d", newest.Status.Failed))
	}
	if len(statusParts) == 0 {
		statusParts = append(statusParts, "Pending")
	}

	age := time.Since(newest.CreationTimestamp.Time).Round(time.Second).String()
	return strings.Join(statusParts, "/"), age
}
