// Package p2p builds the libp2p host and the mesh's application protocols.
package p2p

import (
	"fmt"

	"github.com/libp2p/go-libp2p/core/protocol"
)

// Application protocol ids. Bump the version when the wire semantics change.
const (
	ProtoTask       protocol.ID = "/zeptomesh/task/0.1.0"
	ProtoResult     protocol.ID = "/zeptomesh/result/0.1.0"
	ProtoRPC        protocol.ID = "/zeptomesh/rpc/0.1.0"
	ProtoMembership             = "/zeptomesh/membership/0.1.0"
	ProtoIdentify               = "/ipfs/identify/1.0.0"
)

// SkillNamespace prefixes DHT keys that index a skill advertisement.
const SkillNamespace = "zeptomesh/skill/"

// ErrUnsupportedProtocol reports a stream for an unknown protocol id.
var ErrUnsupportedProtocol = fmt.Errorf("p2p: unsupported protocol")
