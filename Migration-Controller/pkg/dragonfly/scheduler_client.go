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

package dragonfly

import (
	"context"
	"fmt"
	"sync"

	"github.com/container-migration/controller/pkg/dragonfly/schedulerpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

// Host type values defined by the Dragonfly scheduler (see d7y.io/api
// common/v2 Host.type and dragonfly/v2 pkg/types.HostType).
const (
	hostTypeNormal    = uint32(0)
	hostTypeSuperSeed = uint32(1)
)

// seedPeerStateActive marks seed peers returned by the scheduler. A host
// listed by the scheduler has announced itself and is considered alive, so
// it is always reported as active.
const seedPeerStateActive = "active"

// SeedPeerSource is the interface used to discover the currently alive
// Dragonfly seed peers.
type SeedPeerSource interface {
	// GetSeedPeers returns all seed peers currently registered in the
	// Dragonfly scheduler.
	GetSeedPeers(ctx context.Context) ([]SeedPeer, error)
}

// schedulerClient implements SeedPeerSource by calling the Dragonfly
// scheduler grpc ListHosts API. Compared with the manager seed peer table,
// the scheduler is the first-hand, real-time registry of Rust-client seed
// peers: a dfdaemon announces itself to the scheduler on startup and leaves
// on shutdown, so presence in ListHosts means the seed peer is alive and
// reachable for P2P distribution.
type schedulerClient struct {
	addr string

	mu   sync.Mutex
	conn *grpc.ClientConn
}

// NewSchedulerClient creates a SeedPeerSource backed by the Dragonfly
// scheduler grpc API. addr is the scheduler grpc endpoint, e.g.
// "dragonfly-scheduler.dragonfly.svc.cluster.local:8002".
func NewSchedulerClient(addr string) (SeedPeerSource, error) {
	if addr == "" {
		return nil, fmt.Errorf("scheduler address is required")
	}
	return &schedulerClient{addr: addr}, nil
}

// GetSeedPeers calls scheduler.v2.Scheduler/ListHosts filtered by the seed
// peer host type and maps the returned hosts to SeedPeer entries.
func (c *schedulerClient) GetSeedPeers(ctx context.Context) ([]SeedPeer, error) {
	client, err := c.grpcClient()
	if err != nil {
		return nil, err
	}

	req := &schedulerpb.ListHostsRequest{
		Type: proto.Uint32(hostTypeSuperSeed),
	}
	resp, err := client.ListHosts(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("list hosts from scheduler %s: %w", c.addr, err)
	}

	out := make([]SeedPeer, 0, len(resp.GetHosts()))
	for _, host := range resp.GetHosts() {
		// Defensive filter: only seed peer hosts are of interest, even if a
		// scheduler ignored the type filter and returned all hosts.
		if host.GetType() == hostTypeNormal {
			continue
		}
		if host.GetIp() == "" {
			continue
		}
		out = append(out, SeedPeer{
			Hostname: host.GetHostname(),
			IP:       host.GetIp(),
			Port:     host.GetPort(),
			State:    seedPeerStateActive,
		})
	}
	return out, nil
}

// grpcClient returns a lazily created grpc client connection. The scheduler
// grpc service inside the cluster is served without TLS.
func (c *schedulerClient) grpcClient() (schedulerpb.SchedulerClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		conn, err := grpc.NewClient(c.addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, fmt.Errorf("dial scheduler %s: %w", c.addr, err)
		}
		c.conn = conn
	}
	return schedulerpb.NewSchedulerClient(c.conn), nil
}

// Close releases the underlying grpc connection.
func (c *schedulerClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}

var _ SeedPeerSource = (*schedulerClient)(nil)
