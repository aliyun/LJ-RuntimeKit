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
	"fmt"
	"time"

	"github.com/lj-runtimekit/device-plugin/internal/utils"

	"google.golang.org/grpc"
	"k8s.io/klog/v2"
)

const (
	healthCheckMethod = "/grpc.health.v1.Health/Check"
	statusServing     = 1
)

// healthCheckRequest implements proto.Marshaler and proto.Message for an empty health check request.
type healthCheckRequest struct{}

func (h *healthCheckRequest) Reset()         {}
func (h *healthCheckRequest) String() string { return "" }
func (h *healthCheckRequest) ProtoMessage()  {}
func (h *healthCheckRequest) Marshal() ([]byte, error) {
	return []byte{}, nil
}

// healthCheckResponse implements proto.Unmarshaler and proto.Message for health check response.
type healthCheckResponse struct {
	Status int32
}

func (h *healthCheckResponse) Reset()         { h.Status = 0 }
func (h *healthCheckResponse) String() string { return fmt.Sprintf("status:%d", h.Status) }
func (h *healthCheckResponse) ProtoMessage()  {}
func (h *healthCheckResponse) Marshal() ([]byte, error) {
	return nil, nil
}
func (h *healthCheckResponse) Unmarshal(data []byte) error {
	// Parse protobuf wire format: field 1, varint type (wire type 0)
	// Tag byte = (field_number << 3) | wire_type = 0x08
	h.Status = 0
	for i := 0; i < len(data); {
		if i >= len(data) {
			break
		}
		// Read tag
		tag := data[i]
		i++
		fieldNumber := tag >> 3
		wireType := tag & 0x07

		if wireType == 0 { // varint
			var val int32
			shift := uint(0)
			for i < len(data) {
				b := data[i]
				i++
				val |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
				shift += 7
			}
			if fieldNumber == 1 {
				h.Status = val
				return nil
			}
		} else {
			// Skip unknown wire types
			return nil
		}
	}
	return nil
}

func runHealthCheck(retryInterval time.Duration) {
	socketPath := "unix:" + utils.XDMGrpcSocket
	klog.Infof("Starting health check for xdm at %s, retry interval: %v", socketPath, retryInterval)

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		conn, err := grpc.DialContext(ctx, socketPath,
			grpc.WithInsecure(),
			grpc.WithBlock(),
		)
		if err != nil {
			cancel()
			klog.Infof("Waiting for xdm ready, dial failed: %v, retry in %v", err, retryInterval)
			time.Sleep(retryInterval)
			continue
		}

		req := &healthCheckRequest{}
		resp := &healthCheckResponse{}
		err = conn.Invoke(ctx, healthCheckMethod, req, resp)
		conn.Close()
		cancel()

		if err == nil && resp.Status == statusServing {
			klog.Infof("xdm is ready, health check passed")
			return
		}
		klog.Infof("xdm not ready, status: %v, err: %v, retry in %v", resp, err, retryInterval)
		time.Sleep(retryInterval)
	}
}
