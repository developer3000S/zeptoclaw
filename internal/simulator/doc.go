// Package simulator models membership gossip and task routing at mesh sizes that
// cannot be run for real on one machine (ТЗ 17.3 asks for 100/500/1000 nodes
// «в симуляции»).
//
// What the model reproduces, in the same order the running node does:
//
//   - the application-level membership protocol of internal/discovery: one
//     PeerState per heartbeat (Membership.Publish), full-view sync in batches of
//     32 states paced at heartbeat/8 (Membership.PublishFull), expiry at
//     failure_timeout (Membership.expireLoop), stale pruning at 3x that
//     (Node.maintenance) and a dial attempt for every fresh state as long as the
//     table is below neighbors.max (Node.onGossipPeer);
//   - the routing decision of internal/routing: Table.Select scores with the
//     shipped weights, the skill filter, the trust filter and the not-connected
//     penalty, so the candidate a task is handed to is the real choice, not a
//     guess;
//   - the forwarding rules of gossipsub as go-libp2p-pubsub implements them for
//     the node's own configuration: mesh degree D with Dlo/Dhi bounds, no flood
//     publish, IHAVE gossip to max(Dlazy, GossipFactor of non-mesh peers) drawn
//     from a HistoryGossip-beat message cache, and IDONTWANT suppression for
//     payloads at or above IDontWantMessageThreshold — the regime the membership
//     topic lives in once the view is wide;
//   - honest byte counts: every figure is proto.Marshal of the real generated
//     messages from gen/zeptomesh/v1, plus the real uvarint frame prefix from
//     internal/p2p, plus the signed pubsub envelope (From/Seqno/Ed25519
//     signature over the "libp2p-pubsub:" prefixed frame) that a gossipsub
//     publication actually costs on the wire.
//
// What the model does NOT reproduce, and no number reported by this package may
// be read as covering it: TCP/TLS (noise) handshakes and their bandwidth, NAT
// traversal and circuit relay, channel latency, jitter, packet loss, the CPU or
// memory cost of a real libp2p host, disk I/O, and the effect of libp2p's
// connection-manager pruning on the overlay. Rates are computed over simulated
// beats rather than wall-clock seconds, so the model has no queueing: nothing in
// it can saturate a link and back off.
package simulator
