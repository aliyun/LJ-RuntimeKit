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

package runtime

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/container-migration/worker/pkg/apis/xdm"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/klog/v2"
)

const (
	// xdmName identifies this engine in logs and metrics.
	xdmName = "XDM"
	// xdmMaxRecvMsgSize must be able to hold one streamed chunk plus framing.
	xdmMaxRecvMsgSize = 128 * 1024 * 1024
	// xdmDialTimeout bounds establishing the unix socket connection.
	xdmDialTimeout = 10 * time.Second

	// checkpointPartEOFPrefix marks the end of one checkpoint part inside the
	// single Checkpoint stream.
	//
	// XDM streams N independent tars back to back over one RPC, so the receiver
	// needs the boundaries. They travel in the message field rather than a new
	// proto field: this side already skips responses with an empty data_chunk as
	// progress-only messages, so reusing message keeps the proto untouched. The
	// leading \x00 guarantees it never collides with human readable progress text.
	//
	// ⚠ Must stay byte-for-byte identical to
	// the XDM checkpoint part EOF prefix.
	checkpointPartEOFPrefix = "\x00XDM-PART-EOF:"
)

// XDMRuntime drives checkpointing through the node-local X-Device-Manager.
type XDMRuntime struct {
	socket string
}

var _ ContainerRuntime = (*XDMRuntime)(nil)

// NewXDMRuntime creates a runtime that talks to the XDM socket at the given path.
func NewXDMRuntime(socket string) *XDMRuntime {
	return &XDMRuntime{socket: socket}
}

func (r *XDMRuntime) Name() string { return xdmName }

// Init verifies that the XDM socket is reachable, so that a misconfigured mount
// surfaces at worker startup rather than in the middle of a migration.
func (r *XDMRuntime) Init() error {
	conn, err := net.DialTimeout("unix", r.socket, xdmDialTimeout)
	if err != nil {
		return fmt.Errorf("cannot reach x-device-manager socket %s: %w", r.socket, err)
	}
	defer conn.Close()
	klog.V(4).Infof("x-device-manager socket %s is reachable", r.socket)
	return nil
}

// CheckpointToStream starts the XDM checkpoint for containerID and returns an
// iterator over the resulting tar parts. Closing it aborts the RPC.
//
// XDM splits the checkpoint into N independent tars (each one a complete
// archive carrying the full directory skeleton) so that the destination node can
// download and extract them in parallel during restore. They arrive back to back
// on one stream, delimited by checkpointPartEOFPrefix sentinels.
func (r *XDMRuntime) CheckpointToStream(ctx context.Context, containerID string, opts map[string]string) (CheckpointPartStream, error) {
	if containerID == "" {
		return nil, fmt.Errorf("container id is required")
	}

	dialCtx, cancelDial := context.WithTimeout(ctx, xdmDialTimeout)
	defer cancelDial()

	conn, err := grpc.DialContext(dialCtx, r.socket,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", addr)
		}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(xdmMaxRecvMsgSize)),
	)
	if err != nil {
		return nil, fmt.Errorf("cannot connect to x-device-manager at %s: %w", r.socket, err)
	}

	streamCtx, cancelStream := context.WithCancel(ctx)
	stream, err := xdm.NewXpuDeviceManagerServiceClient(conn).Checkpoint(streamCtx,
		&xdm.CheckpointRequest{ContainerId: containerID, Options: opts})
	if err != nil {
		cancelStream()
		conn.Close()
		return nil, fmt.Errorf("x-device-manager Checkpoint rpc failed: %w", err)
	}

	return &checkpointPartStream{
		stream: stream,
		conn:   conn,
		cancel: cancelStream,
	}, nil
}

// RestoreXPU forwards to XDM's synchronous RestoreXPU: XDM resolves the
// container pid/rootfs through containerd and runs cr-tool while the RPC is
// held open. A missing xpu/ directory (CPU-only container) is answered with
// success by XDM itself.
//
// The caller must only invoke this once the container is Running: CRIU reads
// memory/ during `runc restore`, and cr-tool needs the restored, live pid.
// The call carries no deadline of its own: it returns when XDM returns, which
// is bounded by the cr-tool timeout on the XDM side (2h). The upstream ctx
// (held by the controller RPC) still aborts the call if the controller goes
// away.
func (r *XDMRuntime) RestoreXPU(ctx context.Context, containerID, checkpointDir string) error {
	if containerID == "" || checkpointDir == "" {
		return fmt.Errorf("container id and checkpoint dir are required")
	}

	dialCtx, cancelDial := context.WithTimeout(ctx, xdmDialTimeout)
	defer cancelDial()

	conn, err := grpc.DialContext(dialCtx, r.socket,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", addr)
		}),
	)
	if err != nil {
		return fmt.Errorf("cannot connect to x-device-manager at %s: %w", r.socket, err)
	}
	defer conn.Close()

	resp, err := xdm.NewXpuDeviceManagerServiceClient(conn).RestoreXPU(ctx, &xdm.RestoreXPURequest{
		ContainerId:  containerID,
		SnapshotPath: checkpointDir,
	})
	if err != nil {
		return fmt.Errorf("x-device-manager RestoreXPU rpc failed: %w", err)
	}
	if resp.GetStatus() != 0 {
		return fmt.Errorf("x-device-manager RestoreXPU failed: %s", resp.GetMessage())
	}

	return nil
}

// checkpointPartStream walks the server-streaming Checkpoint response part by
// part. Each part is exposed as an io.Reader over that part's tar bytes only.
type checkpointPartStream struct {
	stream xdm.XpuDeviceManagerService_CheckpointClient
	conn   *grpc.ClientConn
	cancel context.CancelFunc

	// buf holds the not-yet-consumed bytes of the most recently received chunk.
	buf []byte
	// partDone is set when the sentinel for the current part has been seen.
	partDone bool
	// err latches the first terminal error (including io.EOF for the whole RPC).
	err error

	nextIndex int
}

var _ CheckpointPartStream = (*checkpointPartStream)(nil)

// Next advances to the following part.
//
// It deliberately pre-reads until the first data chunk of that part has landed
// in buf. Without that look-ahead the last sentinel would be indistinguishable
// from "another part is coming", and the caller would create one extra uploader
// and push an empty tar object.
func (s *checkpointPartStream) Next() (int, io.Reader, error) {
	s.partDone = false
	if err := s.fill(); err != nil {
		// io.EOF here means the RPC ended cleanly: no more parts.
		return 0, nil, err
	}
	if s.partDone && len(s.buf) == 0 {
		// A sentinel with no payload in between: XDM produced an empty part,
		// which would mean an empty tar object on the storage side. Treat it as
		// a protocol violation rather than silently uploading nothing.
		return 0, nil, fmt.Errorf("x-device-manager sent an empty checkpoint part at index %d", s.nextIndex)
	}

	idx := s.nextIndex
	s.nextIndex++
	return idx, &checkpointPartReader{s: s, index: idx}, nil
}

func (s *checkpointPartStream) Close() error {
	s.cancel()
	return s.conn.Close()
}

// fill receives until buf holds data, the current part ends, or the RPC ends.
func (s *checkpointPartStream) fill() error {
	for len(s.buf) == 0 && !s.partDone {
		if s.err != nil {
			return s.err
		}
		resp, err := s.stream.Recv()
		if err != nil {
			s.err = err
			return err
		}
		if resp.GetStatus() != 0 {
			s.err = fmt.Errorf("x-device-manager checkpoint failed: %s", resp.GetMessage())
			return s.err
		}

		if msg := resp.GetMessage(); strings.HasPrefix(msg, checkpointPartEOFPrefix) {
			s.partDone = true
			return nil
		}
		if len(resp.GetDataChunk()) == 0 {
			// Progress-only message.
			if msg := resp.GetMessage(); msg != "" {
				klog.V(5).Infof("[XDM] %s", msg)
			}
			continue
		}
		s.buf = resp.GetDataChunk()
	}
	return nil
}

// checkpointPartReader exposes exactly one part's bytes.
type checkpointPartReader struct {
	s     *checkpointPartStream
	index int
}

func (r *checkpointPartReader) Read(p []byte) (int, error) {
	s := r.s
	if len(s.buf) == 0 {
		if s.partDone {
			return 0, io.EOF
		}
		if err := s.fill(); err != nil {
			// The RPC ending here means the part was cut short. Returning io.EOF
			// would make the uploader treat a truncated tar as a complete object,
			// so this must surface as an error.
			if err == io.EOF {
				return 0, fmt.Errorf("x-device-manager stream ended in the middle of checkpoint part %d", r.index)
			}
			return 0, err
		}
		if len(s.buf) == 0 {
			// fill returned because the sentinel arrived: this part is complete.
			return 0, io.EOF
		}
	}

	n := copy(p, s.buf)
	s.buf = s.buf[n:]
	return n, nil
}
