package main

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	statsv1 "github.com/kefiriaus/otus-go-final-project/pkg/api/stats/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type cpuTestServer struct {
	statsv1.UnimplementedStatsServiceServer
	snapshots []*statsv1.Snapshot
	request   chan *statsv1.SubscribeRequest
	err       error
	block     bool
}

func (s *cpuTestServer) Subscribe(
	request *statsv1.SubscribeRequest,
	stream grpc.ServerStreamingServer[statsv1.Snapshot],
) error {
	s.request <- request
	if s.block {
		<-stream.Context().Done()
		return stream.Context().Err()
	}
	for _, snapshot := range s.snapshots {
		if err := stream.Send(snapshot); err != nil {
			return err
		}
	}
	return s.err
}

func newCPUTestClient(t *testing.T, service statsv1.StatsServiceServer) statsv1.StatsServiceClient {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	statsv1.RegisterStatsServiceServer(server, service)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = server.Serve(listener)
	}()
	dialer := grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return listener.DialContext(ctx)
	})
	connection, err := grpc.NewClient(
		"passthrough:///test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		dialer,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		connection.Close()
		server.Stop()
		listener.Close()
		<-done
	})
	return statsv1.NewStatsServiceClient(connection)
}

func TestReceiveSnapshotsRealGRPC(t *testing.T) {
	snapshots := []*statsv1.Snapshot{
		clientSnapshot(statsv1.CollectionState_COLLECTION_STATE_OK),
		clientSnapshot(statsv1.CollectionState_COLLECTION_STATE_PARTIAL),
	}
	service := &cpuTestServer{
		snapshots: snapshots,
		request:   make(chan *statsv1.SubscribeRequest, 1),
	}
	client := newCPUTestClient(t, service)
	options := clientOptions{intervalSeconds: 2, windowSeconds: 30}
	var output bytes.Buffer
	if err := receiveSnapshots(context.Background(), client, options, &output); err != nil {
		t.Fatal(err)
	}
	request := <-service.request
	if request.IntervalSeconds != 2 || request.WindowSeconds != 30 {
		t.Fatalf("request=%+v", request)
	}
	first, err := formatSnapshot(snapshots[0])
	if err != nil {
		t.Fatal(err)
	}
	second, err := formatSnapshot(snapshots[1])
	if err != nil {
		t.Fatal(err)
	}
	if output.String() != first+second {
		t.Fatalf("output=%q, want=%q", output.String(), first+second)
	}
}

type countingClient struct {
	statsv1.StatsServiceClient
	receives *int
}

func (c countingClient) Subscribe(
	ctx context.Context,
	request *statsv1.SubscribeRequest,
	options ...grpc.CallOption,
) (grpc.ServerStreamingClient[statsv1.Snapshot], error) {
	stream, err := c.StatsServiceClient.Subscribe(ctx, request, options...)
	if err != nil {
		return nil, err
	}
	return &countingStream{ServerStreamingClient: stream, receives: c.receives}, nil
}

type countingStream struct {
	grpc.ServerStreamingClient[statsv1.Snapshot]
	receives *int
}

func (s *countingStream) Recv() (*statsv1.Snapshot, error) {
	*s.receives++
	return s.ServerStreamingClient.Recv()
}

type failingWriter struct {
	err error
}

func (w failingWriter) Write([]byte) (int, error) {
	return 0, w.err
}

func TestReceiveSnapshotsServerError(t *testing.T) {
	service := &cpuTestServer{
		request: make(chan *statsv1.SubscribeRequest, 1),
		err:     status.Error(codes.InvalidArgument, "invalid window"),
	}
	client := newCPUTestClient(t, service)
	options := clientOptions{intervalSeconds: 1, windowSeconds: 3601}
	err := receiveSnapshots(context.Background(), client, options, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "invalid window") {
		t.Fatalf("error=%v", err)
	}
}

func TestReceiveSnapshotsWriterError(t *testing.T) {
	service := &cpuTestServer{
		snapshots: []*statsv1.Snapshot{
			clientSnapshot(statsv1.CollectionState_COLLECTION_STATE_OK),
			clientSnapshot(statsv1.CollectionState_COLLECTION_STATE_OK),
		},
		request: make(chan *statsv1.SubscribeRequest, 1),
	}
	receives := 0
	client := countingClient{StatsServiceClient: newCPUTestClient(t, service), receives: &receives}
	writeErr := errors.New("output failed")
	options := clientOptions{intervalSeconds: 1, windowSeconds: 15}
	err := receiveSnapshots(context.Background(), client, options, failingWriter{err: writeErr})
	if !errors.Is(err, writeErr) {
		t.Fatalf("error=%v, want %v", err, writeErr)
	}
	if receives != 1 {
		t.Fatalf("receives=%d, want 1", receives)
	}
}

func TestReceiveSnapshotsCancellation(t *testing.T) {
	service := &cpuTestServer{request: make(chan *statsv1.SubscribeRequest, 1), block: true}
	client := newCPUTestClient(t, service)
	ctx, cancel := context.WithCancel(context.Background())
	var output bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- receiveSnapshots(ctx, client, clientOptions{intervalSeconds: 1, windowSeconds: 15}, &output)
	}()
	<-service.request
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("receive did not stop after cancellation")
	}
	if output.Len() != 0 {
		t.Fatalf("output=%q", output.String())
	}
}
