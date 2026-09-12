package p2p

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"google.golang.org/protobuf/proto"

	pb "github.com/zeptoclaw/zeptomesh/gen/zeptomesh/v1"
)

// TaskHandler processes an inbound envelope and produces the acknowledgement
// that goes back to the sending peer on the same stream.
type TaskHandler func(ctx context.Context, remote peer.ID, env *pb.TaskEnvelope) (*pb.TaskAck, error)

// ResultHandler processes an inbound result (local completion or relay).
type ResultHandler func(ctx context.Context, remote peer.ID, res *pb.TaskResult) (*pb.ResultAck, error)

// RPCHandler processes a control-plane request.
type RPCHandler func(ctx context.Context, remote peer.ID, req *pb.RpcRequest) (*pb.RpcResponse, error)

// Handlers groups the node-side implementations of the mesh protocols.
type Handlers struct {
	Task   TaskHandler
	Result ResultHandler
	RPC    RPCHandler
}

// Service binds protocol handlers to a Host and offers typed outbound calls.
// It is the only place that speaks the wire format.
type Service struct {
	host *Host
	log  *slog.Logger
	h    Handlers
	// streamTimeout bounds one request/response exchange.
	streamTimeout time.Duration
}

// NewService installs the inbound handlers on the host.
func NewService(h *Host, handlers Handlers, logger *slog.Logger) *Service {
	s := &Service{host: h, log: logger, h: handlers, streamTimeout: 30 * time.Second}
	h.Underlying().SetStreamHandler(ProtoTask, s.handleTask)
	h.Underlying().SetStreamHandler(ProtoResult, s.handleResult)
	h.Underlying().SetStreamHandler(ProtoRPC, s.handleRPC)
	return s
}

// Host exposes the wrapped host.
func (s *Service) Host() *Host { return s.host }

// SetStreamTimeout adjusts the per-exchange deadline (used by tests).
func (s *Service) SetStreamTimeout(d time.Duration) { s.streamTimeout = d }

// Timeout is the per-exchange deadline applied to every outbound call.
func (s *Service) Timeout() time.Duration { return s.streamTimeout }

func (s *Service) handleTask(st network.Stream) {
	ok := false
	defer func() { s.finish(st, ok) }()
	remote := st.Conn().RemotePeer()

	var env pb.TaskEnvelope
	if err := s.readInto(st, &env); err != nil {
		if !errors.Is(err, io.EOF) {
			s.log.Warn("inbound_task_decode", "remote", remote.String(), "err", err.Error())
		}
		return
	}
	if s.h.Task == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.streamTimeout)
	defer cancel()

	ack, err := s.h.Task(ctx, remote, &env)
	if err != nil {
		s.log.Warn("inbound_task_handler", "remote", remote.String(), "err", err.Error())
		return
	}
	if ack == nil {
		return
	}
	if err := s.host.WriteMsg(st, ack); err != nil {
		s.log.Warn("inbound_task_write", "remote", remote.String(), "err", err.Error())
		return
	}
	if err := st.CloseWrite(); err != nil {
		s.log.Warn("inbound_task_close", "remote", remote.String(), "err", err.Error())
		return
	}
	ok = true
}

func (s *Service) handleResult(st network.Stream) {
	ok := false
	defer func() { s.finish(st, ok) }()
	remote := st.Conn().RemotePeer()

	var res pb.TaskResult
	if err := s.readInto(st, &res); err != nil {
		if !errors.Is(err, io.EOF) {
			s.log.Warn("inbound_result_decode", "remote", remote.String(), "err", err.Error())
		}
		return
	}
	if s.h.Result == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.streamTimeout)
	defer cancel()

	ack, err := s.h.Result(ctx, remote, &res)
	if err != nil {
		s.log.Warn("inbound_result_handler", "remote", remote.String(), "err", err.Error())
		return
	}
	if ack == nil {
		return
	}
	if err := s.host.WriteMsg(st, ack); err != nil {
		s.log.Warn("inbound_result_write", "remote", remote.String(), "err", err.Error())
		return
	}
	if err := st.CloseWrite(); err != nil {
		s.log.Warn("inbound_result_close", "remote", remote.String(), "err", err.Error())
		return
	}
	ok = true
}

func (s *Service) handleRPC(st network.Stream) {
	ok := false
	defer func() { s.finish(st, ok) }()
	remote := st.Conn().RemotePeer()

	var req pb.RpcRequest
	if err := s.readInto(st, &req); err != nil {
		if !errors.Is(err, io.EOF) {
			s.log.Warn("inbound_rpc_decode", "remote", remote.String(), "err", err.Error())
		}
		return
	}
	if s.h.RPC == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.streamTimeout)
	defer cancel()

	resp, err := s.h.RPC(ctx, remote, &req)
	if err != nil {
		s.log.Warn("inbound_rpc_handler", "remote", remote.String(), "err", err.Error())
		return
	}
	if resp == nil {
		return
	}
	if err := s.host.WriteMsg(st, resp); err != nil {
		s.log.Warn("inbound_rpc_write", "remote", remote.String(), "err", err.Error())
		return
	}
	if err := st.CloseWrite(); err != nil {
		s.log.Warn("inbound_rpc_close", "remote", remote.String(), "err", err.Error())
		return
	}
	ok = true
}

// finish ends an inbound stream. When the response was fully written (ok) the
// stream is closed normally; otherwise it is reset. Reset cancels bytes the
// peer has not acknowledged yet, so a handler that wrote a reply and then
// unconditionally reset would make the requester read "stream reset" instead of
// its answer — which is how capability verification silently failed and every
// delegation fell back to "no eligible peers".
func (s *Service) finish(st network.Stream, ok bool) {
	if ok {
		_ = st.Close()
		return
	}
	st.Reset()
}

func (s *Service) readInto(st io.Reader, m proto.Message) error {
	b, err := readFrame(st, s.host.MaxMessageBytes())
	if err != nil {
		return err
	}
	if len(b) == 0 {
		return io.EOF
	}
	return proto.Unmarshal(b, m)
}

// SendTask delivers an envelope and waits for the peer's acknowledgement.
func (s *Service) SendTask(ctx context.Context, to peer.AddrInfo, env *pb.TaskEnvelope) (*pb.TaskAck, error) {
	var ack pb.TaskAck
	if err := s.host.Connect(ctx, to); err != nil {
		return nil, err
	}
	if err := s.exchange(ctx, to.ID, ProtoTask, env, &ack); err != nil {
		return nil, err
	}
	return &ack, nil
}

// SendResult relays a result one hop back along the route.
func (s *Service) SendResult(ctx context.Context, to peer.ID, res *pb.TaskResult) (*pb.ResultAck, error) {
	var ack pb.ResultAck
	if err := s.exchange(ctx, to, ProtoResult, res, &ack); err != nil {
		return nil, err
	}
	return &ack, nil
}

// RPC performs a control-plane call.
func (s *Service) RPC(ctx context.Context, to peer.ID, req *pb.RpcRequest) (*pb.RpcResponse, error) {
	var resp pb.RpcResponse
	if err := s.exchange(ctx, to, ProtoRPC, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (s *Service) exchange(ctx context.Context, to peer.ID, pid protocol.ID, req, resp proto.Message) error {
	st, err := s.host.OpenStream(ctx, to, pid)
	if err != nil {
		return err
	}
	defer st.Reset()

	if err := st.SetDeadline(time.Now().Add(s.streamTimeout)); err != nil {
		return fmt.Errorf("p2p: deadline: %w", err)
	}
	if err := s.host.WriteMsg(st, req); err != nil {
		return err
	}
	if err := st.CloseWrite(); err != nil {
		return fmt.Errorf("p2p: close write: %w", err)
	}
	if err := s.host.ReadMsg(st, s.host.MaxMessageBytes(), resp); err != nil {
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("p2p: peer %s closed the stream without answering", to)
		}
		return err
	}
	return nil
}
