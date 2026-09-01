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
	"log"
	"net"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/lj-runtimekit/controller/pkg/configpb"
)

const (
	// SocketPath is the path to the config-daemon socket
	SocketPath = "/run/amp/eKitC/xdm/sockets/config-daemon.sock"
	// ActionEnable enables the feature
	ActionEnable = "enable"
	// ActionDisable disables the feature
	ActionDisable = "disable"
)

func main() {
	action := flag.String("action", "", "The action to perform: enable or disable")
	socketPath := flag.String("socket", SocketPath, "The path to the config-daemon socket")
	timeout := flag.Duration("timeout", 30*time.Second, "The timeout for the gRPC call")
	flag.Parse()

	if *action == "" {
		log.Fatal("--action is required, must be 'enable' or 'disable'")
	}

	if *action != ActionEnable && *action != ActionDisable {
		log.Fatalf("invalid action %q, must be 'enable' or 'disable'", *action)
	}

	log.Printf("AMP Job Agent starting, action=%s, socket=%s", *action, *socketPath)

	// Check if the socket file exists
	if _, err := os.Stat(*socketPath); os.IsNotExist(err) {
		log.Fatalf("socket file does not exist: %s", *socketPath)
	}

	// Connect to the Unix socket via gRPC
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		"unix:"+*socketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			var d net.Dialer
			// gRPC may pass the target as "unix:///path" or "/path";
			// strip any "unix:" prefix so we dial the actual filesystem path.
			addr = strings.TrimPrefix(addr, "unix://")
			addr = strings.TrimPrefix(addr, "unix:")
			return d.DialContext(ctx, "unix", addr)
		}),
	)
	if err != nil {
		log.Fatalf("failed to dial gRPC server at %s: %v", *socketPath, err)
	}
	defer conn.Close()

	client := configpb.NewAmpConfigServiceClient(conn)

	// Call the appropriate RPC
	switch *action {
	case ActionEnable:
		if err := callEnable(ctx, client); err != nil {
			log.Fatalf("enable failed: %v", err)
		}
		log.Printf("Enable succeeded")
	case ActionDisable:
		if err := callDisable(ctx, client); err != nil {
			log.Fatalf("disable failed: %v", err)
		}
		log.Printf("Disable succeeded")
	}
}

// callEnable invokes the Enable RPC.
func callEnable(ctx context.Context, client configpb.AmpConfigServiceClient) error {
	resp, err := client.Enable(ctx, &configpb.EnableRequest{})
	if err != nil {
		return fmt.Errorf("gRPC Enable call failed: %w", err)
	}

	if resp.Status != 0 {
		return fmt.Errorf("enable returned non-zero status %d: %s (stdout=%s, stderr=%s, exit_code=%d, duration_ms=%d)",
			resp.Status, resp.Message, resp.Stdout, resp.Stderr, resp.ExitCode, resp.DurationMs)
	}

	log.Printf("Enable response: %s", resp.Message)
	return nil
}

// callDisable invokes the Disable RPC.
func callDisable(ctx context.Context, client configpb.AmpConfigServiceClient) error {
	resp, err := client.Disable(ctx, &configpb.DisableRequest{})
	if err != nil {
		return fmt.Errorf("gRPC Disable call failed: %w", err)
	}

	if resp.Status != 0 {
		return fmt.Errorf("disable returned non-zero status %d: %s (stdout=%s, stderr=%s, exit_code=%d, duration_ms=%d)",
			resp.Status, resp.Message, resp.Stdout, resp.Stderr, resp.ExitCode, resp.DurationMs)
	}

	log.Printf("Disable response: %s", resp.Message)
	return nil
}
