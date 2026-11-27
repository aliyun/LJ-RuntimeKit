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

	"github.com/container-migration/controller/pkg/dragonfly/dfdaemonpb"
	"github.com/container-migration/controller/pkg/preheat"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// TaskCacheState describes the state of a preheat task in a seed peer's
// dfdaemon local cache. It is an alias of the backend-neutral
// preheat.CacheState: the abstraction in pkg/preheat owns the state values
// and this prober maps the dfdaemon responses onto them.
type TaskCacheState = preheat.CacheState

const (
	// CacheStateNotFound means the task is not registered on the dfdaemon at
	// all (never preheated there, or already evicted by GC).
	CacheStateNotFound = preheat.CacheStateNotFound
	// CacheStateRunning means the task is registered but not yet finished,
	// i.e. the download is still in progress.
	CacheStateRunning = preheat.CacheStateRunning
	// CacheStateFailed means the download failed (e.g. the backend returned
	// 403). StatLocalTask still succeeds for such a task, but no content is
	// cached.
	CacheStateFailed = preheat.CacheStateFailed
	// CacheStateCached means the content has been fully downloaded and is
	// served from the local cache.
	CacheStateCached = preheat.CacheStateCached
)

// SeedPeerProber checks the cache state of a preheat task on a seed peer by
// talking to the seed peer's own dfdaemon grpc endpoint directly.
type SeedPeerProber interface {
	// ProbeTask reports the cache state of taskID in the local cache of the
	// dfdaemon at addr ("ip:port" as announced to the scheduler). It returns
	// (CacheStateNotFound, nil) when the dfdaemon answered that the task does
	// not exist. Note that a successful StatLocalTask does NOT imply the file
	// is cached: failed and still-downloading tasks are also stat-able.
	ProbeTask(ctx context.Context, addr string, taskID string) (TaskCacheState, error)
}

// dfdaemonProber implements SeedPeerProber by calling
// dfdaemon.v2.DfdaemonUpload/StatLocalTask. The preheated file is cached by
// the seed peer's dfdaemon, so probing that exact instance is the only
// authoritative cache check: the scheduler ListHosts entry already carries
// the dfdaemon grpc endpoint (host.ip:host.port), no Pod discovery or node
// side worker is involved.
type dfdaemonProber struct {
	mu      sync.Mutex
	conns   map[string]*grpc.ClientConn
	clients map[string]dfdaemonpb.DfdaemonUploadClient
}

// NewDfdaemonProber creates a SeedPeerProber that dials seed peer dfdaemons
// lazily and caches one connection per address.
func NewDfdaemonProber() SeedPeerProber {
	return &dfdaemonProber{
		conns:   make(map[string]*grpc.ClientConn),
		clients: make(map[string]dfdaemonpb.DfdaemonUploadClient),
	}
}

// ProbeTask calls StatLocalTask on the dfdaemon at addr and maps the task
// metadata to a cache state: codes.NotFound means the task does not exist;
// a failed_at timestamp means the download failed; a finished_at timestamp
// means the content is fully cached; otherwise the download is still in
// progress.
func (p *dfdaemonProber) ProbeTask(ctx context.Context, addr string, taskID string) (TaskCacheState, error) {
	client, err := p.grpcClient(addr)
	if err != nil {
		return CacheStateNotFound, err
	}

	resp, err := client.StatLocalTask(ctx, &dfdaemonpb.StatLocalTaskRequest{TaskId: taskID})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return CacheStateNotFound, nil
		}
		return CacheStateNotFound, fmt.Errorf("stat local task %s on dfdaemon %s: %w", taskID, addr, err)
	}
	switch {
	case resp.GetFailedAt() != nil:
		return CacheStateFailed, nil
	case resp.GetFinishedAt() != nil:
		return CacheStateCached, nil
	default:
		return CacheStateRunning, nil
	}
}

// grpcClient returns a lazily created grpc client for the given dfdaemon
// address. The dfdaemon grpc service inside the cluster is served without TLS.
func (p *dfdaemonProber) grpcClient(addr string) (dfdaemonpb.DfdaemonUploadClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if client, ok := p.clients[addr]; ok {
		return client, nil
	}
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("dial dfdaemon %s: %w", addr, err)
	}
	client := dfdaemonpb.NewDfdaemonUploadClient(conn)
	p.conns[addr] = conn
	p.clients[addr] = client
	return client, nil
}

// Close releases all cached grpc connections.
func (p *dfdaemonProber) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var firstErr error
	for addr, conn := range p.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(p.conns, addr)
		delete(p.clients, addr)
	}
	return firstErr
}

var _ SeedPeerProber = (*dfdaemonProber)(nil)
