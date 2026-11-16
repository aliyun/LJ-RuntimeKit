package exporter

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	xdmgrpc "github.com/lj-runtimekit/exporter/pkg/grpc/xdm"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type clientHealthServer struct {
	xdmgrpc.UnimplementedXpuDeviceManagerServiceServer
	fetch      func(context.Context) (*xdmgrpc.GetAllHealthResponse, error)
	categories func(context.Context) (*xdmgrpc.GetCategoriesResponse, error)
	config     func(context.Context, *xdmgrpc.ExporterRequest) (*xdmgrpc.ExporterConfigResponse, error)
}

func (s *clientHealthServer) GetAllHealth(ctx context.Context, _ *xdmgrpc.GetAllHealthRequest) (*xdmgrpc.GetAllHealthResponse, error) {
	return s.fetch(ctx)
}

func (s *clientHealthServer) GetCategories(ctx context.Context, _ *xdmgrpc.GetCategoriesRequest) (*xdmgrpc.GetCategoriesResponse, error) {
	return s.categories(ctx)
}

func (s *clientHealthServer) ExporterGetConfig(ctx context.Context, req *xdmgrpc.ExporterRequest) (*xdmgrpc.ExporterConfigResponse, error) {
	return s.config(ctx, req)
}

func testClientSnapshot() *xdmgrpc.GetAllHealthResponse {
	return &xdmgrpc.GetAllHealthResponse{
		NodeName: "node-a", SampledAtMs: 1000, TtlMs: 500,
		Categories: []*xdmgrpc.CategoryHealth{
			{Category: "dynamic", Status: xdmgrpc.HealthStatus_UNHEALTHY, Reason: "device failure"},
		},
	}
}

func testClientCategories() *xdmgrpc.GetCategoriesResponse {
	return &xdmgrpc.GetCategoriesResponse{Categories: []*xdmgrpc.CategoryInfo{
		{Category: "dynamic", Condition: "DEVICE_FAILURE", OkReason: "DeviceHealthy", OkMessage: "device healthy", BadReason: "DeviceUnhealthy"},
		{Category: "new-category"},
	}}
}

func startXdmClientServer(t *testing.T, handler *clientHealthServer, options ...grpc.ServerOption) string {
	t.Helper()
	address := filepath.Join(t.TempDir(), "health.sock")
	listener, err := net.Listen("unix", address)
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(options...)
	xdmgrpc.RegisterXpuDeviceManagerServiceServer(server, handler)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	return listener.Addr().String()
}

func TestXdmGrpcClientUnix(t *testing.T) {
	endpoint := startXdmClientServer(t, &clientHealthServer{
		fetch:      func(context.Context) (*xdmgrpc.GetAllHealthResponse, error) { return testClientSnapshot(), nil },
		categories: func(context.Context) (*xdmgrpc.GetCategoriesResponse, error) { return testClientCategories(), nil },
	}, grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod != "/XpuDeviceManagerService/GetAllHealth" && info.FullMethod != "/XpuDeviceManagerService/GetCategories" {
			t.Errorf("wire method changed: %s", info.FullMethod)
		}
		return handler(ctx, req)
	}))
	client, err := NewXdmGrpcClient(&Config{XdmGrpcPath: endpoint}, nil, xdmgrpc.ExporterType_Collector)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("Close = %v", err)
		}
		if client.conn.GetState() != connectivity.Shutdown {
			t.Error("Close must shut down the connection")
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := client.GetAllHealth(ctx)
	if err != nil || !proto.Equal(got, testClientSnapshot()) {
		t.Fatalf("GetAllHealth = %v, %v", got, err)
	}
	categories, err := client.GetCategories(ctx)
	if err != nil || !proto.Equal(categories, testClientCategories()) {
		t.Fatalf("GetCategories = %v, %v", categories, err)
	}
}

func TestXdmGrpcClientDeadlineAndCancellation(t *testing.T) {
	endpoint := startXdmClientServer(t, &clientHealthServer{
		fetch: func(ctx context.Context) (*xdmgrpc.GetAllHealthResponse, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
		categories: func(ctx context.Context) (*xdmgrpc.GetCategoriesResponse, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	})
	client, err := NewXdmGrpcClient(&Config{XdmGrpcPath: endpoint}, nil, xdmgrpc.ExporterType_Collector)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	for _, method := range []string{"GetAllHealth", "GetCategories"} {
		for _, canceled := range []bool{false, true} {
			name := method + "/deadline"
			if canceled {
				name = method + "/cancellation"
			}
			t.Run(name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
				defer cancel()
				want := codes.DeadlineExceeded
				if canceled {
					cancel()
					want = codes.Canceled
				}
				var err error
				if method == "GetAllHealth" {
					_, err = client.GetAllHealth(ctx)
				} else {
					_, err = client.GetCategories(ctx)
				}
				if status.Code(err) != want {
					t.Fatalf("RPC status = %v, want %v", err, want)
				}
			})
		}
	}
}

type healthContextClient struct {
	xdmgrpc.XpuDeviceManagerServiceClient
	fetch      func(context.Context, *xdmgrpc.GetAllHealthRequest) (*xdmgrpc.GetAllHealthResponse, error)
	categories func(context.Context, *xdmgrpc.GetCategoriesRequest) (*xdmgrpc.GetCategoriesResponse, error)
}

func (c *healthContextClient) GetAllHealth(ctx context.Context, req *xdmgrpc.GetAllHealthRequest, _ ...grpc.CallOption) (*xdmgrpc.GetAllHealthResponse, error) {
	return c.fetch(ctx, req)
}

func (c *healthContextClient) GetCategories(ctx context.Context, req *xdmgrpc.GetCategoriesRequest, _ ...grpc.CallOption) (*xdmgrpc.GetCategoriesResponse, error) {
	return c.categories(ctx, req)
}

func TestXdmGrpcClientHealthContextAndErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	wantErr := status.Error(codes.Unavailable, "health service unavailable")
	fetchCalls, categoryCalls := 0, 0
	client := &XdmGrpcClient{client: &healthContextClient{
		fetch: func(got context.Context, req *xdmgrpc.GetAllHealthRequest) (*xdmgrpc.GetAllHealthResponse, error) {
			fetchCalls++
			if got != ctx || req == nil {
				t.Error("GetAllHealth must pass the caller's context and a non-nil request")
			}
			return nil, wantErr
		},
		categories: func(got context.Context, req *xdmgrpc.GetCategoriesRequest) (*xdmgrpc.GetCategoriesResponse, error) {
			categoryCalls++
			if got != ctx || req == nil {
				t.Error("GetCategories must pass the caller's context and a non-nil request")
			}
			return nil, wantErr
		},
	}}
	if response, err := client.GetAllHealth(ctx); response != nil || err != wantErr {
		t.Fatalf("GetAllHealth = %v, %v; want the original RPC error", response, err)
	}
	if response, err := client.GetCategories(ctx); response != nil || err != wantErr {
		t.Fatalf("GetCategories = %v, %v; want the original RPC error", response, err)
	}
	if fetchCalls != 1 || categoryCalls != 1 {
		t.Fatalf("RPC calls = (%d, %d), want (1, 1)", fetchCalls, categoryCalls)
	}
}

func TestXdmGrpcClientLegacyConstructor(t *testing.T) {
	endpoint := startXdmClientServer(t, &clientHealthServer{
		fetch:      func(context.Context) (*xdmgrpc.GetAllHealthResponse, error) { return testClientSnapshot(), nil },
		categories: func(context.Context) (*xdmgrpc.GetCategoriesResponse, error) { return testClientCategories(), nil },
		config: func(_ context.Context, req *xdmgrpc.ExporterRequest) (*xdmgrpc.ExporterConfigResponse, error) {
			if req.GetExporterType() != xdmgrpc.ExporterType_Detector {
				t.Errorf("exporter type = %v, want Detector", req.GetExporterType())
			}
			return &xdmgrpc.ExporterConfigResponse{
				Status: StatusOK, ExporterType: req.GetExporterType(), Interval: 250,
			}, nil
		},
	})
	config := &Config{XdmGrpcPath: endpoint}
	info := &HostInfo{NodeName: "node-a"}
	client, err := NewXdmGrpcClient(config, info, xdmgrpc.ExporterType_Detector)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if client.config != config || client.hostInfo != info || client.exportType != xdmgrpc.ExporterType_Detector {
		t.Fatal("legacy constructor must retain exporter configuration")
	}
	if got := client.conn.Target(); got != protocol+endpoint {
		t.Fatalf("legacy target = %q, want %q", got, protocol+endpoint)
	}
	metrics, code := client.GetConfig()
	if code != GrpcRetSuccess || metrics == nil || metrics.interval != 250 || metrics.module != int32(xdmgrpc.ExporterType_Detector) {
		t.Fatalf("GetConfig = %v, %v", metrics, code)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if got, err := client.GetAllHealth(ctx); err != nil || !proto.Equal(got, testClientSnapshot()) {
		t.Fatalf("GetAllHealth = %v, %v", got, err)
	}
	if got, err := client.GetCategories(ctx); err != nil || !proto.Equal(got, testClientCategories()) {
		t.Fatalf("GetCategories = %v, %v", got, err)
	}
}
