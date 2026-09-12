package p2p

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	crypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	pb "github.com/zeptoclaw/zeptomesh/gen/zeptomesh/v1"
	"github.com/zeptoclaw/zeptomesh/internal/config"
)

// testNode wires a Host plus a Service with the given handlers on a live
// transport (QUIC+TCP on ephemeral ports), exactly as the daemon runs it.
func testNode(t *testing.T, handlers Handlers) (*Host, *Service, peer.AddrInfo) {
	t.Helper()
	cfg := config.Default()
	cfg.Node.Listen = []string{"/ip4/127.0.0.1/tcp/0", "/ip4/127.0.0.1/udp/0/quic-v1"}
	cfg.Discovery.DHT = false
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	key, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	h, err := New(context.Background(), Options{Config: cfg, Key: key, Logger: logger})
	if err != nil {
		t.Fatalf("host: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h, NewService(h, handlers, logger), h.AddrInfo()
}

func swapHandlers() Handlers {
	return Handlers{
		Task: func(_ context.Context, _ peer.ID, env *pb.TaskEnvelope) (*pb.TaskAck, error) {
			return &pb.TaskAck{
				TaskId:     env.GetTaskId(),
				Status:     pb.AckStatus_ACK_STATUS_QUEUED,
				Reason:     env.GetPayload().GetInstruction(),
				AcceptedBy: "echo",
			}, nil
		},
		Result: func(_ context.Context, _ peer.ID, res *pb.TaskResult) (*pb.ResultAck, error) {
			return &pb.ResultAck{TaskId: res.GetTaskId(), Accepted: true, Reason: res.GetText()}, nil
		},
		RPC: func(_ context.Context, _ peer.ID, req *pb.RpcRequest) (*pb.RpcResponse, error) {
			if k, ok := req.GetKind().(*pb.RpcRequest_Ping); ok {
				return &pb.RpcResponse{Kind: &pb.RpcResponse_Ping{
					Ping: &pb.PingResponse{Nonce: k.Ping.GetNonce(), PeerTime: time.Now().Unix()},
				}}, nil
			}
			if k, ok := req.GetKind().(*pb.RpcRequest_SkillLookup); ok {
				// Answer far larger than the request: a response spanning several
				// transport frames is where a premature stream reset shows up.
				rec := &pb.PeerRecord{
					PeerId: "12D3KooWEchoPeerRecordForRelayTest",
					Skills: k.SkillLookup.GetSkills(),
					Addrs:  []string{"/ip4/127.0.0.1/tcp/4001", "/ip4/127.0.0.1/udp/4001/quic-v1"},
					SeenAt: time.Now().Unix(),
				}
				out := make([]*pb.PeerRecord, 0, 64)
				for i := 0; i < 64; i++ {
					out = append(out, rec)
				}
				return &pb.RpcResponse{Kind: &pb.RpcResponse_SkillLookup{
					SkillLookup: &pb.SkillLookupResponse{Peers: out, ResponderRefreshed: true},
				}}, nil
			}
			return nil, fmt.Errorf("test: unhandled rpc kind %T", req.GetKind())
		},
	}
}

// TestExchangeSmallAndLargeResponses is the regression guard for a stream that
// was reset after its response had been written. Reset cancels bytes the peer
// has not acknowledged yet, so the requester read "stream reset" instead of the
// answer: capability verification never completed, peers kept an unverified
// skill set, and delegation died with "no eligible peers reachable" even
// though the executor was connected and advertising the skill.
func TestExchangeSmallAndLargeResponses(t *testing.T) {
	srvHost, _, addr := testNode(t, swapHandlers())
	cliHost, cli, _ := testNode(t, Handlers{})

	ctx := context.Background()
	if err := cliHost.Connect(ctx, addr); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if !cliHost.IsConnected(addr.ID) {
		t.Fatal("not connected after Connect")
	}

	for i := 0; i < 40; i++ {
		resp, err := cli.RPC(ctx, addr.ID, &pb.RpcRequest{Kind: &pb.RpcRequest_Ping{
			Ping: &pb.PingRequest{Nonce: int64(1000 + i)},
		}})
		if err != nil {
			t.Fatalf("rpc ping #%d: %v", i, err)
		}
		if got := resp.GetPing().GetNonce(); got != int64(1000+i) {
			t.Fatalf("ping #%d nonce = %d, want %d", i, got, 1000+i)
		}
	}

	for _, size := range []int{16, 4096, 300 << 10} {
		ack, err := cli.SendTask(ctx, addr, &pb.TaskEnvelope{
			TaskId:  fmt.Sprintf("task-%d", size),
			Payload: &pb.TaskPayload{Instruction: makeString(size, 'x')},
		})
		if err != nil {
			t.Fatalf("SendTask payload=%d: %v", size, err)
		}
		if ack.GetStatus() != pb.AckStatus_ACK_STATUS_QUEUED {
			t.Fatalf("payload=%d ack status = %v", size, ack.GetStatus())
		}
		if len(ack.GetReason()) != size {
			t.Fatalf("payload=%d echoed %d bytes of instruction, want %d", size, len(ack.GetReason()), size)
		}
	}

	res, err := cli.SendResult(ctx, addr.ID, &pb.TaskResult{
		TaskId: "task-res", Text: makeString(200<<10, 'y'),
		Status: pb.TaskStatus_TASK_STATUS_COMPLETED,
	})
	if err != nil {
		t.Fatalf("SendResult: %v", err)
	}
	if !res.GetAccepted() || len(res.GetReason()) != 200<<10 {
		t.Fatalf("result ack = %+v", res)
	}

	resp, err := cli.RPC(ctx, addr.ID, &pb.RpcRequest{Kind: &pb.RpcRequest_SkillLookup{
		SkillLookup: &pb.SkillLookupRequest{Skills: []string{"ocr"}, FullRefresh: true, RelayBudget: 2},
	}})
	if err != nil {
		t.Fatalf("skill lookup rpc: %v", err)
	}
	sl := resp.GetSkillLookup()
	if sl == nil || len(sl.GetPeers()) != 64 || !sl.GetResponderRefreshed() {
		t.Fatalf("skill lookup response malformed: %d peers, refreshed=%v",
			len(sl.GetPeers()), sl.GetResponderRefreshed())
	}
	_ = srvHost
}

// TestStreamHandlerRejectsOversizedMessage verifies the inbound frame ceiling:
// a peer must not be able to make the node buffer an unbounded message, and the
// handler must stay healthy afterwards.
func TestStreamHandlerRejectsOversizedMessage(t *testing.T) {
	srvCfg := config.Default()
	srvCfg.Node.Listen = []string{"/ip4/127.0.0.1/tcp/0"}
	srvCfg.Discovery.DHT = false
	srvCfg.Security.MaxMessageBytes = 1 << 10
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	srvKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	srvHost, err := New(context.Background(), Options{Config: srvCfg, Key: srvKey, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	defer srvHost.Close()
	NewService(srvHost, swapHandlers(), logger)

	cliHost, cli, _ := testNode(t, Handlers{})
	ctx := context.Background()
	if err := cliHost.Connect(ctx, srvHost.AddrInfo()); err != nil {
		t.Fatal(err)
	}

	st, err := cliHost.OpenStream(ctx, srvHost.ID(), ProtoRPC)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFrame(st, make([]byte, 8<<10)); err != nil {
		t.Fatalf("write oversized frame: %v", err)
	}
	_ = st.CloseWrite()

	readDone := make(chan error, 1)
	go func() {
		resp := &pb.RpcResponse{}
		readDone <- cliHost.ReadMsg(st, int(srvCfg.Security.MaxMessageBytes), resp)
	}()
	select {
	case err := <-readDone:
		if err == nil {
			t.Fatal("oversized frame was accepted by the server")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("server neither answered nor closed the stream")
	}
	st.Reset()

	// The server must still serve a well-formed request afterwards.
	if _, err := cli.RPC(ctx, srvHost.ID(), &pb.RpcRequest{
		Kind: &pb.RpcRequest_Ping{Ping: &pb.PingRequest{Nonce: 7}},
	}); err != nil {
		t.Fatalf("server unhealthy after oversized frame: %v", err)
	}
}

// TestNotifeeReportsConnected guards the assumption the routing table is built
// on: Connected/Disconnected notifications fire, otherwise Connected flags and
// the peers_connected metric drift from reality.
func TestNotifeeReportsConnected(t *testing.T) {
	_, _, addr := testNode(t, Handlers{})
	cliHost, _, _ := testNode(t, Handlers{})

	var connected, disconnected int
	nn := &network.NotifyBundle{
		ConnectedF:    func(network.Network, network.Conn) { connected++ },
		DisconnectedF: func(network.Network, network.Conn) { disconnected++ },
	}
	cliHost.Underlying().Network().Notify(nn)
	defer cliHost.Underlying().Network().StopNotify(nn)

	if err := cliHost.Connect(context.Background(), addr); err != nil {
		t.Fatal(err)
	}
	if connected == 0 {
		t.Fatal("ConnectedF never fired")
	}
	if err := cliHost.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for disconnected == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if disconnected == 0 {
		t.Fatal("DisconnectedF never fired")
	}
}

func makeString(n int, c byte) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = c
	}
	return string(b)
}
