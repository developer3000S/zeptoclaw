package discovery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"

	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
	"github.com/developer3000S/zeptoclaw/internal/config"
	"github.com/developer3000S/zeptoclaw/internal/security"
)

// clockSkewSeconds bounds how far a peer's clock may drift from ours before its
// announcements are treated as hostile rather than merely unsynchronised.
const clockSkewSeconds = 300

// Membership maintains the epidemic view of live nodes.
//
// Each node publishes its own PeerState on a gossip topic and keeps the last
// state it heard per peer. A peer is considered failed when its last update is
// older than failure_timeout. This is the "gossip/SWIM" role of the design: it
// is not a strong-consensus membership list, it is an eventually consistent
// view that is good enough to route with.
type Membership struct {
	topic  *pubsub.Topic
	sub    *pubsub.Subscription
	ps     *pubsub.PubSub
	h      host.Host
	cfg    config.GossipConfig
	log    *slog.Logger
	policy *security.Policy
	audit  *security.Audit

	mu     sync.RWMutex
	states map[peer.ID]*pb.PeerState
	// self is the state this node publishes.
	self *pb.PeerState

	// selfState supplies the freshest view of this node at publish time.
	selfState func() *pb.PeerState
	onPeer    func(peer.AddrInfo, *pb.PeerState)
	onExpire  func(peer.ID)
	// rebindSource supplies the identity-handover statements this node holds, so
	// they ride along with heartbeats and reach peers that never met the new key
	// (ТЗ 11.2 п.3–4).
	rebindSource func() []*pb.KeyRebind
	// onRebind is fired for every statement that verified. A rebind is signed by
	// both identities it names, so verification needs no trust in the relay and
	// no prior contact with either key — which is exactly why gossip can carry
	// it at all.
	onRebind func(*pb.KeyRebind)

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewMembership joins the gossip topic. The PubSub router is passed in, not
// created here: a second NewGossipSub on the same host re-registers the same
// stream handler and silently takes the first router's subscriptions away, so
// every topic this node uses (membership and the skill search plane) must ride
// one shared router (ТЗ 6.9.5 п.5).
func NewMembership(ctx context.Context, h host.Host, ps *pubsub.PubSub, cfg config.GossipConfig, policy *security.Policy, audit *security.Audit, logger *slog.Logger) (*Membership, error) {
	if h == nil {
		return nil, errors.New("discovery: nil host")
	}
	if ps == nil {
		return nil, errors.New("discovery: pubsub router required")
	}
	if cfg.Topic == "" {
		return nil, errors.New("discovery: gossip.topic is required")
	}
	if cfg.Heartbeat.D() <= 0 {
		return nil, errors.New("discovery: gossip.heartbeat must be > 0")
	}
	topic, err := ps.Join(cfg.Topic)
	if err != nil {
		return nil, fmt.Errorf("discovery: join topic: %w", err)
	}
	sub, err := topic.Subscribe()
	if err != nil {
		return nil, fmt.Errorf("discovery: subscribe: %w", err)
	}
	m := &Membership{
		topic:  topic,
		sub:    sub,
		ps:     ps,
		h:      h,
		cfg:    cfg,
		log:    logger,
		policy: policy,
		audit:  audit,
		states: make(map[peer.ID]*pb.PeerState),
	}
	return m, nil
}

// SetSelfStateFunc installs the callback that renders this node's state.
func (m *Membership) SetSelfStateFunc(fn func() *pb.PeerState) {
	m.mu.Lock()
	m.selfState = fn
	m.mu.Unlock()
}

// SetOnPeer installs a callback fired for every fresh peer state, which the
// node uses to dial peers it has never connected to.
func (m *Membership) SetOnPeer(fn func(peer.AddrInfo, *pb.PeerState)) {
	m.mu.Lock()
	m.onPeer = fn
	m.mu.Unlock()
}

// SetOnExpire installs a callback fired when a peer is declared failed.
func (m *Membership) SetOnExpire(fn func(peer.ID)) {
	m.mu.Lock()
	m.onExpire = fn
	m.mu.Unlock()
}

// SetRebindSource installs the provider of held handover statements and the
// callback for statements learned from others.
func (m *Membership) SetRebindSource(src func() []*pb.KeyRebind, fn func(*pb.KeyRebind)) {
	m.mu.Lock()
	m.rebindSource = src
	m.onRebind = fn
	m.mu.Unlock()
}

// Start launches the publish and receive loops.
func (m *Membership) Start(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	m.mu.Lock()
	m.cancel = cancel
	m.mu.Unlock()

	m.wg.Add(3)
	go m.publishLoop(runCtx)
	go m.receiveLoop(runCtx)
	go m.expireLoop(runCtx)
	if m.log != nil {
		m.log.Info("membership_started", "topic", m.cfg.Topic, "heartbeat", m.cfg.Heartbeat.String())
	}
	return nil
}

func (m *Membership) publishLoop(ctx context.Context) {
	defer m.wg.Done()
	t := time.NewTicker(m.cfg.Heartbeat.D())
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := m.Publish(ctx); err != nil && m.log != nil {
				m.log.Debug("membership_publish", "err", err.Error())
			}
		}
	}
}

// Publish sends this node's current state to the topic.
func (m *Membership) Publish(ctx context.Context) error {
	m.mu.RLock()
	fn := m.selfState
	m.mu.RUnlock()
	if fn == nil {
		return nil
	}
	state := fn()
	if state == nil || state.PeerId == "" {
		return nil
	}
	state.Timestamp = time.Now().UTC().Unix()
	m.mu.Lock()
	m.self = state
	m.mu.Unlock()
	return m.publishState(ctx, state)
}

// PublishState sends one explicit state (graceful-departure announcements).
func (m *Membership) PublishState(ctx context.Context, state *pb.PeerState) error {
	if state == nil || state.PeerId == "" {
		return nil
	}
	return m.publishState(ctx, state)
}

func (m *Membership) publishState(ctx context.Context, state *pb.PeerState) error {
	msg := &pb.MembershipGossip{States: []*pb.PeerState{state}, FromPeerId: state.PeerId}
	// Handover statements ride every publication, which makes their spread
	// epidemic. The local ledger is deduplicated and bounded by (old id,
	// sequence), so piggybacking cannot grow without limit.
	m.mu.RLock()
	src := m.rebindSource
	m.mu.RUnlock()
	if src != nil {
		msg.Rebinds = src()
	}
	b, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return m.topic.Publish(ctx, b)
}

// PublishFull shares the states of peers we know about, which is how a new node
// learns a wide view quickly instead of waiting for heartbeats to trickle in.
//
// The view is sent in batches rather than truncated: with a cap-and-drop the
// nodes that happen to sort last are never shared at all, so a 100+ mesh would
// keep a structurally blind spot. Batching bounds one message while still
// eventually disclosing everything, and the inter-batch pause keeps a full-sync
// from becoming a traffic spike.
func (m *Membership) PublishFull(ctx context.Context) error {
	m.mu.RLock()
	states := make([]*pb.PeerState, 0, len(m.states))
	for _, s := range m.states {
		states = append(states, s)
	}
	rebinds := m.rebindSourceSnapshotLocked()
	m.mu.RUnlock()
	if len(states) == 0 && len(rebinds) == 0 {
		return nil
	}
	sort.Slice(states, func(i, j int) bool {
		if states[i].Timestamp == states[j].Timestamp {
			return states[i].PeerId < states[j].PeerId
		}
		return states[i].Timestamp > states[j].Timestamp
	})

	const batchSize = 32
	for off := 0; off*batchSize < len(states); off++ {
		lo := off * batchSize
		hi := lo + batchSize
		if hi > len(states) {
			hi = len(states)
		}
		msg := &pb.MembershipGossip{States: states[lo:hi], FromPeerId: m.h.ID().String()}
		// Handover statements go with the first batch only: they are small, and
		// every peer that receives any part of the sync gets them.
		if off == 0 {
			msg.Rebinds = rebinds
		}
		b, err := proto.Marshal(msg)
		if err != nil {
			return err
		}
		if err := m.topic.Publish(ctx, b); err != nil {
			return err
		}
		if hi < len(states) {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(m.cfg.Heartbeat.D() / 8):
			}
		}
	}
	return nil
}

// rebindSourceSnapshotLocked reads the held handover statements; callers hold
// m.mu, so the source itself must not take it.
func (m *Membership) rebindSourceSnapshotLocked() []*pb.KeyRebind {
	if m.rebindSource == nil {
		return nil
	}
	return m.rebindSource()
}

func (m *Membership) receiveLoop(ctx context.Context) {
	defer m.wg.Done()
	for {
		msg, err := m.sub.Next(ctx)
		if err != nil {
			if ctx.Err() == nil && m.log != nil {
				m.log.Debug("membership_receive", "err", err.Error())
			}
			return
		}
		// Trust the authenticated sender of the pubsub message, not a claim
		// inside the payload.
		m.ingest(peer.ID(msg.From), msg.Data)
	}
}

func (m *Membership) ingest(from peer.ID, data []byte) {
	// Gossipsub delivers our own publications back to our own subscription.
	// Our own view of the mesh cannot teach us anything, and accepting it meant
	// every PublishFull batch audited itself as "gossip_third_party_state" (the
	// relay — ourselves — holds no routing-trust observation of its own id),
	// burying real rejections under self-echo noise.
	if from == m.h.ID() {
		return
	}
	msg := &pb.MembershipGossip{}
	if err := proto.Unmarshal(data, msg); err != nil {
		m.security(from, "gossip_unparseable", err.Error())
		return
	}
	now := time.Now().UTC()
	accepted := make([]*pb.PeerState, 0, len(msg.States))
	for _, st := range msg.States {
		if st == nil || st.PeerId == "" {
			continue
		}
		pid, err := peer.Decode(st.PeerId)
		if err != nil {
			m.security(from, "gossip_bad_peer_id", st.PeerId)
			continue
		}
		if pid == m.h.ID() {
			continue // our own echo
		}
		// A state may be relayed about a third party only by a peer we already
		// trust to route; otherwise the table is trivially poisonable.
		if pid != from && m.policy != nil && !m.policy.AllowDelegationTo(from) {
			m.security(from, "gossip_third_party_state", st.PeerId)
			continue
		}
		// Reject states that claim to be from the future or are absurdly old.
		age := now.Unix() - st.Timestamp
		if age < -int64(clockSkewSeconds) || age > int64(m.cfg.FailureTimeout.D().Seconds())*4 {
			m.security(from, "gossip_bad_timestamp", fmt.Sprint(st.Timestamp))
			continue
		}
		accepted = append(accepted, st)
	}
	// Handover statements are applied before the state gate below. A PeerState
	// about a third party is a claim that needs routing trust in its relay, but a
	// KeyRebind proves its own authorship — so a message whose states were all
	// rejected must still be allowed to carry a rotation. Returning early here
	// would mean an untrusted (or merely stale-state) relay could never pass a
	// handover along, and a mesh that prunes states would prune revocations with
	// them.
	m.ingestRebinds(from, append(append([]*pb.KeyRebind(nil), msg.GetRebinds()...), msg.GetRevocations()...))
	if len(accepted) == 0 {
		return
	}

	m.mu.Lock()
	type pending struct {
		ai peer.AddrInfo
		st *pb.PeerState
	}
	var cbs []pending
	cb := m.onPeer
	for _, st := range accepted {
		pid, _ := peer.Decode(st.PeerId)
		prev, had := m.states[pid]
		if had && prev.Timestamp > st.Timestamp {
			continue // monotonically apply newer state only
		}
		m.states[pid] = st
		if cb != nil {
			cbs = append(cbs, pending{ai: addrInfoFrom(st), st: st})
		}
	}
	m.mu.Unlock()

	for _, p := range cbs {
		cb(p.ai, p.st)
	}
}

// ingestRebinds validates and applies identity-handover statements carried by a
// gossip message.
//
// The trust rule here differs from PeerState on purpose: a PeerState about a
// third party is accepted only from a peer we trust to route, because it is an
// unverifiable claim. A KeyRebind is signed by both identities it names — the
// retiring key and the incoming key — and both public keys are recoverable from
// the peer ids themselves, so the statement proves its own authorship. That is
// why it can be adopted from any gossip source, including one that never met
// either identity, and why rotation needs no operator intervention (ТЗ 11.2).
func (m *Membership) ingestRebinds(from peer.ID, all []*pb.KeyRebind) {
	if len(all) == 0 {
		return
	}
	m.mu.RLock()
	cb := m.onRebind
	m.mu.RUnlock()
	if cb == nil {
		return
	}
	now := time.Now().UTC()
	for _, k := range all {
		if k == nil || k.GetOldPeerId() == "" {
			continue
		}
		age := now.Unix() - k.GetIssuedAt()
		if age < -int64(clockSkewSeconds) || age > rebindMaxAgeSeconds {
			m.security(from, "rebind_bad_timestamp", fmt.Sprint(k.GetIssuedAt()))
			continue
		}
		if err := security.VerifyRebind(k, nil); err != nil {
			// Only the two named identities could produce a valid statement, so a
			// failure here is a forgery attempt or a corrupted relay — worth an
			// audit entry, not a silent drop.
			m.security(from, "rebind_bad_signature", err.Error())
			continue
		}
		cb(k)
	}
}

// rebindMaxAgeSeconds bounds how old a handover statement may be to still be
// adopted: an arbitrarily old one is history rather than a live rotation, and
// accepting it would let a captured message re-open a long-retired identity.
const rebindMaxAgeSeconds = 86400

// addrInfoFrom converts a gossip state into a dialable address info.
func addrInfoFrom(st *pb.PeerState) peer.AddrInfo {
	ai := peer.AddrInfo{}
	if pid, err := peer.Decode(st.PeerId); err == nil {
		ai.ID = pid
	}
	for _, a := range st.Addrs {
		if m, err := ParseAddrInfo(a); err == nil {
			ai.Addrs = append(ai.Addrs, m.Addrs...)
		}
	}
	return ai
}

func (m *Membership) expireLoop(ctx context.Context) {
	defer m.wg.Done()
	t := time.NewTicker(m.cfg.Heartbeat.D())
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			cutoff := time.Now().UTC().Add(-m.cfg.FailureTimeout.D()).Unix()
			m.mu.Lock()
			var expired []peer.ID
			for pid, st := range m.states {
				if st.Timestamp < cutoff {
					expired = append(expired, pid)
					delete(m.states, pid)
				}
			}
			cb := m.onExpire
			m.mu.Unlock()
			if cb != nil {
				for _, pid := range expired {
					cb(pid)
				}
			}
		}
	}
}

// MarkLeft records a graceful departure announcement.
func (m *Membership) MarkLeft(pid peer.ID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.states, pid)
}

// State returns the last seen state of a peer.
func (m *Membership) State(pid peer.ID) (*pb.PeerState, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	st, ok := m.states[pid]
	if !ok {
		return nil, false
	}
	return proto.Clone(st).(*pb.PeerState), true
}

// PeersBySkill lists peers whose last state advertises every requested skill.
func (m *Membership) PeersBySkill(want []string) []peer.ID {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []peer.ID
	for pid, st := range m.states {
		if skillsCover(st.Skills, want) {
			out = append(out, pid)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}

// Snapshot returns every live state, newest first.
func (m *Membership) Snapshot() []*pb.PeerState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*pb.PeerState, 0, len(m.states))
	for _, st := range m.states {
		out = append(out, proto.Clone(st).(*pb.PeerState))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Timestamp > out[j].Timestamp })
	return out
}

// Len is the number of live peers in the membership view.
func (m *Membership) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.states)
}

// TopicStats reports gossip fan-out for observability.
func (m *Membership) TopicStats() map[string]any {
	return map[string]any{
		"topic":           m.cfg.Topic,
		"peers_published": m.topic.ListPeers(),
		"known_states":    m.Len(),
	}
}

// Refresh reloads this node's skill view from the gossip sources it holds.
//
// Membership gossip is a cache, so a reload cannot be forced on remote nodes:
// what a refresh does buy is (a) a re-render of our own state, (b) a full-table
// re-broadcast that prompts peers to re-send their view, and (c) a bounded wait
// for the answers to arrive. found counts peers matching want after that wait;
// complete is always false because an eventually consistent cache can never
// certify the absence of an executor.
//
// It implements the same (found, complete) contract as DHT.Refresh so both can
// be composed behind tasks.SkillSource.
func (m *Membership) Refresh(ctx context.Context, want []string, wait time.Duration) (int, bool) {
	if m == nil {
		return 0, false
	}
	before := m.Len()
	if err := m.PublishFull(ctx); err != nil && m.log != nil {
		m.log.Debug("membership_refresh_publish_failed", "err", err.Error())
	}
	if wait > 0 {
		deadline := time.After(wait)
		// Poll until the view stops growing or the window closes.
		last := before
		for {
			select {
			case <-ctx.Done():
				return len(m.PeersBySkill(want)), false
			case <-deadline:
				return len(m.PeersBySkill(want)), false
			case <-time.After(m.cfg.Heartbeat.D() / 3):
				now := m.Len()
				if now <= last {
					return len(m.PeersBySkill(want)), false
				}
				last = now
			}
		}
	}
	return len(m.PeersBySkill(want)), false
}

func (m *Membership) security(from peer.ID, reason, detail string) {
	if m.audit != nil {
		m.audit.Log(security.AuditEvent{
			Event:  "gossip_rejected",
			PeerID: from.String(),
			Reason: reason,
			Detail: truncateStr(detail, 200),
		})
	}
	if m.log != nil {
		m.log.Debug("gossip_rejected", "from", from.String(), "reason", reason)
	}
}

// Close leaves the topic and stops the loops.
func (m *Membership) Close() error {
	m.mu.Lock()
	cancel := m.cancel
	m.cancel = nil
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	m.wg.Wait()
	if m.sub != nil {
		m.sub.Cancel()
	}
	if m.topic != nil {
		return m.topic.Close()
	}
	return nil
}

// skillsCover reports whether have satisfies want.
func skillsCover(have, want []string) bool {
	set := make(map[string]bool, len(have))
	for _, s := range have {
		set[strings.ToLower(strings.TrimSpace(s))] = true
	}
	if set["general"] || set["any"] {
		return true
	}
	for _, w := range want {
		if !set[strings.ToLower(strings.TrimSpace(w))] {
			return false
		}
	}
	return true
}

func truncateStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
