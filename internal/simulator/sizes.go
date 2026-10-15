package simulator

// Sizes are measured once, from the real generated protobuf, so every byte the
// model later charges is anchored to a frame a node would actually write. A change
// to the wire format moves the simulation's numbers instead of quietly diverging
// from them.

import (
	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
)

// descriptorBytes is the length a skill description is modelled at. The shipped
// configuration caps a description at 4 KiB and a sync answer at 64 descriptors;
// the model uses a mid-length prose value, which is a workload assumption rather
// than a protocol fact, and the report states it.
const descriptorBytes = 240

// measureSizes asks the generated code what each protocol message costs.
func (s *Sim) measureSizes() {
	nd := s.nodes[0]
	s.descBytes = descriptorBytes
	s.pxRecordBytes = len(nd.state.Addrs[0])*len(nd.addrs) + 64 + 32

	// Heartbeat publication: one PeerState, as Membership.Publish marshals it.
	own := s.mustMarshal(&pb.MembershipGossip{
		States:     []*pb.PeerState{nd.state},
		FromPeerId: nd.id.String(),
	})
	s.sizeOwnGossip = gossipFrameBytes(s.topic, nd.id, own)

	// Full-sync batch: fullSyncBatchStates states, as Membership.PublishFull does.
	n := min(fullSyncBatchStates, s.n)
	states := make([]*pb.PeerState, 0, n)
	for i := 0; i < n; i++ {
		states = append(states, s.stateOf(s.nodes[i], nd.state.Timestamp))
	}
	full := s.mustMarshal(&pb.MembershipGossip{States: states, FromPeerId: nd.id.String()})
	s.sizeSyncBatch = gossipFrameBytes(s.topic, nd.id, full)

	// Capabilities round trip: the request is empty, the answer carries the whole
	// signed self-description, including skill documents when exchange is on.
	caps := s.capsOf(nd)
	if s.p.Cfg.Capabilities.SkillExchange.Enabled {
		caps.SkillDocs = s.descriptors(nd, len(nd.skills))
	}
	s.sizeCapsReq = p2pFrameBytes(&pb.RpcRequest{
		Kind: &pb.RpcRequest_Capabilities{Capabilities: &pb.CapabilitiesRequest{}},
	})
	s.sizeCapsRT = p2pFrameBytes(&pb.RpcResponse{
		Kind: &pb.RpcResponse_Capabilities{Capabilities: &pb.CapabilitiesResponse{Capabilities: caps}},
	})

	// Peer exchange, capped at the answer limit the node asks for.
	s.sizePxReq = p2pFrameBytes(&pb.RpcRequest{
		Kind: &pb.RpcRequest_PeerExchange{PeerExchange: &pb.PeerExchangeRequest{
			Count: uint32(peerExchangeAnswerLimit),
		}},
	})
	s.sizePxRT = p2pFrameBytes(&pb.RpcResponse{
		Kind: &pb.RpcResponse_PeerExchange{PeerExchange: &pb.PeerExchangeResponse{
			Peers: s.peerRecords(peerExchangeAnswerLimit),
		}},
	})

	// Skill-descriptor sync and skill lookup.
	s.sizeSyncReq = p2pFrameBytes(&pb.RpcRequest{
		Kind: &pb.RpcRequest_SkillsSync{SkillsSync: &pb.SkillsSyncRequest{
			Known: []*pb.SkillVersion{{Name: nd.skills[0], Version: 1}},
		}},
	})
	s.sizeSyncRT = p2pFrameBytes(&pb.RpcResponse{
		Kind: &pb.RpcResponse_SkillsSync{SkillsSync: &pb.SkillsSyncResponse{
			Skills:        s.descriptors(nd, len(nd.skills)),
			SkillsVersion: nd.state.SkillsVersion,
			PeerId:        nd.id.String(),
			Signature:     make([]byte, 64),
		}},
	})
	s.sizeLookupRT = p2pFrameBytes(&pb.RpcResponse{
		Kind: &pb.RpcResponse_SkillLookup{SkillLookup: &pb.SkillLookupResponse{
			Peers: s.peerRecords(s.p.Cfg.Tasks.Forwarding.SearchRelay.Fanout),
		}},
	})

	// Task path. The envelope carries the instruction, the route stack and two
	// Ed25519 signatures, so its cost is dominated by the payload and the ids.
	env := s.probeEnvelope(nd)
	s.sizeEnvelope = p2pFrameBytes(env)
	s.sizeAck = p2pFrameBytes(&pb.TaskAck{
		TaskId:     env.TaskId,
		Status:     pb.AckStatus_ACK_STATUS_QUEUED,
		AcceptedBy: nd.id.String(),
		Timestamp:  s.secs,
		Signature:  make([]byte, 64),
	})
	s.sizeResult = p2pFrameBytes(&pb.TaskResult{
		TaskId:          env.TaskId,
		WorkerPeerId:    nd.id.String(),
		Status:          pb.TaskStatus_TASK_STATUS_COMPLETED,
		Text:            pad("result", s.p.TaskPayloadBytes/2),
		ResultDigest:    make([]byte, 32),
		SenderPeerId:    nd.id.String(),
		Signature:       make([]byte, 64),
		WorkerSignature: make([]byte, 64),
		RouteStack:      []string{nd.id.String()},
	})
}

// probeEnvelope builds a representative routed task envelope: the fields a relay
// actually puts on the wire, including the route stack a multi-hop task carries.
func (s *Sim) probeEnvelope(nd *node) *pb.TaskEnvelope {
	want := []string{s.p.SkillCatalog[0]}
	return &pb.TaskEnvelope{
		TaskId:         pad("t", 26),
		OriginPeerId:   nd.id.String(),
		SenderPeerId:   nd.id.String(),
		CreatedAt:      s.secs,
		Ttl:            int32(s.p.Cfg.Tasks.DefaultTTL),
		Priority:       5,
		RequiredSkills: want,
		Payload: &pb.TaskPayload{
			Instruction:   pad("instruction", s.p.TaskPayloadBytes),
			ContextDigest: "sha256:" + pad("c", 64),
		},
		Constraints:     &pb.TaskConstraints{AllowDelegation: true},
		ContextDigest:   "sha256:" + pad("d", 64),
		RouteStack:      []string{nd.id.String(), s.nodes[1].id.String()},
		Signature:       make([]byte, 64),
		SignatureScheme: "zeptomesh-task-v1",
		OriginSignature: make([]byte, 64),
	}
}
