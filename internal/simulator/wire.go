package simulator

// The mesh speaks two transports, and this file is the only place in the
// package that knows how much either of them costs:
//
//   - the pubsub membership topic (internal/discovery/gossip.go): a
//     gossipsub RPC holding one signed pb.Message, written to the stream
//     behind a uvarint length prefix;
//   - the stream protocols (internal/p2p/service.go): a protobuf message
//     behind the same uvarint prefix (internal/p2p/framing.go).
//
// Every size here is proto.Marshal of the real generated message from
// gen/zeptomesh/v1 — never an estimate — so a change to the wire format moves
// the simulation's numbers automatically.

import (
	"encoding/binary"
	"strings"

	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"
)

// pubsubSignatureBytes is the size of the Signature field a gossipsub
// publication carries. go-libp2p-pubsub signs with the host's Ed25519 key
// (sign.go: signMessage), and it leaves Key empty because an Ed25519 peer id
// already embeds the public key — so the field is exactly 64 bytes.
const pubsubSignatureBytes = 64

// pubsubSeqnoBytes is the width of the Seqno field (pubsub.nextSeqno writes a
// big-endian uint64 counter).
const pubsubSeqnoBytes = 8

// pubsubSignPrefix is the constant prepended to the frame before signing
// (pubsub.SignPrefix). It never reaches the wire; it is named here because the
// envelope shape it protects is described in doc.go.
const pubsubSignPrefix = "libp2p-pubsub:"

// gossipFrameBytes is the exact number of bytes one gossipsub publication of
// payload costs on the wire. from is the publisher's id, topic is the
// membership topic the node joins, seq is the publisher's counter width.
func gossipFrameBytes(topic string, from peer.ID, payload []byte) int {
	t := topic
	rpc := &pubsubpb.RPC{Publish: []*pubsubpb.Message{{
		From:      []byte(from),
		Data:      payload,
		Seqno:     make([]byte, pubsubSeqnoBytes),
		Topic:     &t,
		Signature: make([]byte, pubsubSignatureBytes),
	}}}
	return framed(rpc)
}

// ihaveBytes renders one gossipsub heartbeat's pending IHAVE set for a single
// peer — advertisements covering ids message ids on the given topics — and
// returns the framed length. The router queues one IHAVE per (peer, topic) per
// heartbeat and coalesces the queue into a single control RPC in flush(), so the
// advertisement cost of a wide window is paid once per peer per gossipsub
// heartbeat, not once per message. idsPerTopic is split across nTopics
// advertisements, each truncated by MaxIHaveLength by the caller.
func ihaveBytes(topic string, idSize, nIHAVE, ids int) int {
	if nIHAVE < 1 {
		nIHAVE = 1
	}
	if ids < nIHAVE {
		nIHAVE = max(ids, 1)
	}
	ctl := &pubsubpb.ControlMessage{}
	left := ids
	for k := 0; k < nIHAVE; k++ {
		per := left / (nIHAVE - k)
		if k == nIHAVE-1 {
			per = left
		}
		left -= per
		ctl.Ihave = append(ctl.Ihave, &pubsubpb.ControlIHave{
			TopicID:    ptr(topic),
			MessageIDs: idList(idSize, per),
		})
	}
	return framed(&pubsubpb.RPC{Control: ctl})
}

// idontwantBytes is one IDONTWANT control frame for n message ids. The router
// sends a single IDONTWANT per receipt carrying every id in the accepted batch,
// which for this topic is one message.
func idontwantBytes(idSize, n int) int {
	return framed(&pubsubpb.RPC{Control: &pubsubpb.ControlMessage{
		Idontwant: []*pubsubpb.ControlIDontWant{{MessageIDs: idList(idSize, n)}},
	}})
}

// iwantBytes is one IWANT control frame for n message ids.
func iwantBytes(idSize, n int) int {
	return framed(&pubsubpb.RPC{Control: &pubsubpb.ControlMessage{
		Iwant: []*pubsubpb.ControlIWant{{MessageIDs: idList(idSize, n)}},
	}})
}

// graftBytes is a GRAFT for one topic.
func graftBytes(topic string) int {
	return framed(&pubsubpb.RPC{Control: &pubsubpb.ControlMessage{
		Graft: []*pubsubpb.ControlGraft{{TopicID: ptr(topic)}},
	}})
}

// pruneBytes is a PRUNE for one topic carrying px peer-exchange records. The
// records are the expensive part: each holds a peer id and a signed peer record,
// so the cost of a mesh overflow is proportional to how many alternatives the
// pruned peer is told about.
func pruneBytes(topic string, px, idSize, signedRecordBytes int) int {
	peers := make([]*pubsubpb.PeerInfo, 0, px)
	for i := 0; i < px; i++ {
		peers = append(peers, &pubsubpb.PeerInfo{
			PeerID:           idBytes(idSize),
			SignedPeerRecord: make([]byte, signedRecordBytes),
		})
	}
	backoff := uint64(1)
	return framed(&pubsubpb.RPC{Control: &pubsubpb.ControlMessage{
		Prune: []*pubsubpb.ControlPrune{{TopicID: ptr(topic), Peers: peers, Backoff: &backoff}},
	}})
}

// framed is the length-prefixed frame a gossipsub control RPC is written in.
func framed(rpc *pubsubpb.RPC) int {
	b, err := proto.Marshal(rpc)
	if err != nil {
		panic("simulator: marshal control rpc: " + err.Error())
	}
	return len(binary.AppendUvarint(nil, uint64(len(b)))) + len(b)
}

// p2pFrameBytes is the length-prefixed frame internal/p2p writes for one stream
// protocol message (framing.go: writeFrame), i.e. what a task envelope, an ack
// or an RPC costs on /zeptomesh/*/0.1.0.
func p2pFrameBytes(m proto.Message) int {
	b, err := proto.Marshal(m)
	if err != nil {
		panic("simulator: marshal p2p frame: " + err.Error())
	}
	return len(binary.AppendUvarint(nil, uint64(len(b)))) + len(b)
}

// msgIDSize is the length of a gossipsub message id. The default id function is
// string(from) + string(seqno), so an id is as wide as a peer id plus the
// counter — a fact that makes the IHAVE window the dominant overhead it is.
func msgIDSize(id peer.ID) int { return len([]byte(id)) + pubsubSeqnoBytes }

// idList builds n message ids of the requested width. The bytes are filler: a
// message id is opaque and its length, not its content, is what costs.
func idList(idSize, n int) []string {
	if n <= 0 {
		return nil
	}
	// A base64-ish id is a multihash when the topic uses signed-peer-record
	// ids and raw from+seqno bytes otherwise; either way the field is a string,
	// and a non-ASCII filler byte would inflate it out of protobuf's UTF-8
	// validation, so the filler is a printable run.
	pad := strings.Repeat("A", idSize)
	out := make([]string, n)
	for i := range out {
		out[i] = pad
	}
	return out
}

func idBytes(n int) []byte { return []byte(strings.Repeat("\x01", n)) }

func ptr(s string) *string { return &s }
