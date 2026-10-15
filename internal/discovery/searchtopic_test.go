package discovery

import (
	"context"
	"testing"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
	"github.com/developer3000S/zeptoclaw/internal/config"
	"github.com/developer3000S/zeptoclaw/internal/p2p"
	"github.com/developer3000S/zeptoclaw/internal/security"
)

// topicNode is a host with its identity, a shared pubsub router and a search
// topic — the exact shape node.New builds, so the test exercises the same
// wiring the daemon uses (membership and search on one router).
type topicNode struct {
	h      *p2p.Host
	id     *security.Identity
	s      *SearchTopic
	ps     *pubsub.PubSub
	policy *security.Policy
	evts   chan string
}

func topicCfg() config.SearchTopicConfig {
	return config.SearchTopicConfig{
		Enabled: true, Name: "/zeptomesh/test-search/0.1.0",
		RequestTTL: config.Duration(30 * time.Second), MaxAnswers: 8,
		AnswerCooldown: config.Duration(60 * time.Second),
	}
}

func newTopicNode(t *testing.T, view SearchView) *topicNode {
	t.Helper()
	if view == nil {
		view = func([]string) []*pb.PeerRecord { return nil }
	}
	id := testIdentity(t)
	cfg := config.Default()
	cfg.Node.Listen = []string{"/ip4/127.0.0.1/tcp/0"}
	cfg.Discovery.DHT = false
	h, err := p2p.New(context.Background(), p2p.Options{
		Config: cfg, Logger: quietLogger(), Key: id.PrivKey(),
	})
	if err != nil {
		t.Fatalf("host: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	u := h.Underlying()
	lookup := func(p peer.ID) (ic.PubKey, error) {
		pk := u.Peerstore().PubKey(p)
		if pk == nil {
			return nil, context.DeadlineExceeded
		}
		return pk, nil
	}
	policy := security.NewPolicy(security.ModeOpen, security.TrustKnown)
	ps, err := NewPubSub(context.Background(), u, policy, nil, quietLogger())
	if err != nil {
		t.Fatalf("pubsub: %v", err)
	}
	evts := make(chan string, 64)
	s, err := NewSearchTopic(SearchTopicParams{
		Host: u, PubSub: ps, Config: topicCfg(),
		Signer: security.NewSigner(id), KeyLookup: lookup,
		Policy: policy, View: view, Logger: quietLogger(),
		OnEvent: func(e string) { evts <- e },
	})
	if err != nil {
		t.Fatalf("search topic: %v", err)
	}
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return &topicNode{h: h, id: id, s: s, ps: ps, policy: policy, evts: evts}
}

func (n *topicNode) hasEvent(t *testing.T, want string) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case e := <-n.evts:
			if e == want {
				return
			}
		case <-deadline:
			t.Fatalf("event %q never observed", want)
		}
	}
}

func (n *topicNode) drainWithin(d time.Duration) []string {
	var out []string
	deadline := time.After(d)
	for {
		select {
		case e := <-n.evts:
			out = append(out, e)
		case <-deadline:
			return out
		}
	}
}

// TestSearchTopicCoexistsWithMembershipOnOneRouter is the shared-router proof
// and the happy path at once: both topic planes ride ONE pubsub instance (a
// second NewGossipSub on the same host would silently orphan membership's
// subscription), a signed skills-only request from A gets a signed answer from
// B naming a third peer, and membership heartbeats keep flowing.
func TestSearchTopicCoexistsWithMembershipOnOneRouter(t *testing.T) {
	carol := testIdentity(t)
	bobRecord := []*pb.PeerRecord{{
		PeerId: carol.PeerID().String(), Addrs: []string{"/ip4/127.0.0.1/tcp/9999"},
		Skills: []string{"ocr"}, SeenAt: time.Now().UTC().Unix(),
	}}
	a := newTopicNode(t, nil)
	b := newTopicNode(t, func(want []string) []*pb.PeerRecord {
		for _, w := range want {
			if w == "ocr" {
				return bobRecord
			}
		}
		return nil
	})

	// Membership over the same routers — if the search topic had needed its
	// own second router, membership would have stopped receiving (the first
	// router's stream handler gets clobbered), and this state would never land.
	mA, err := NewMembership(context.Background(), a.h.Underlying(), a.ps, gossipCfg(), a.policy, nil, quietLogger())
	if err != nil {
		t.Fatalf("membership A: %v", err)
	}
	defer mA.Close()
	mB, err := NewMembership(context.Background(), b.h.Underlying(), b.ps, gossipCfg(), b.policy, nil, quietLogger())
	if err != nil {
		t.Fatalf("membership B: %v", err)
	}
	defer mB.Close()
	mA.SetSelfStateFunc(selfState(a.h))
	mB.SetSelfStateFunc(selfState(b.h))
	ctx := context.Background()
	if err := mA.Start(ctx); err != nil {
		t.Fatalf("start membership A: %v", err)
	}
	if err := mB.Start(ctx); err != nil {
		t.Fatalf("start membership B: %v", err)
	}
	if err := a.h.Connect(ctx, b.h.AddrInfo()); err != nil {
		t.Fatalf("connect: %v", err)
	}
	// Let the routers graft, then publish A's state and wait for B to learn it.
	time.Sleep(400 * time.Millisecond)
	if err := mA.Publish(ctx); err != nil {
		t.Fatalf("publish: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := mB.State(a.h.ID()); ok {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, ok := mB.State(a.h.ID()); !ok {
		t.Fatal("membership stopped propagating once the search topic joined — routers collided")
	}

	done := make(chan []*pb.PeerRecord, 1)
	go func() {
		rctx, cancel := context.WithTimeout(ctx, 4*time.Second)
		defer cancel()
		done <- a.s.Search(rctx, []string{"ocr"})
	}()

	a.hasEvent(t, "requested")
	b.hasEvent(t, "answered")
	a.hasEvent(t, "replied")

	got := <-done
	if len(got) != 1 || got[0].GetPeerId() != carol.PeerID().String() {
		t.Fatalf("search returned %d records, want Carol's: %+v", len(got), got)
	}
}

// TestSearchTopicIgnoresForgedReply pins the claimed-id gate: a reply whose
// claimed responder is not the authenticated pubsub sender must never reach the
// requester's results, however well-formed it is.
func TestSearchTopicIgnoresForgedReply(t *testing.T) {
	a := newTopicNode(t, nil)
	mallory := newTopicNode(t, nil)
	unrelated := testIdentity(t)

	ctx := context.Background()
	if err := a.h.Connect(ctx, mallory.h.AddrInfo()); err != nil {
		t.Fatalf("connect: %v", err)
	}
	// Let the topic mesh graft before publishing: gossipsub fans out to mesh
	// peers only, and the graft happens on the first heartbeat (~1s).
	time.Sleep(1500 * time.Millisecond)
	searchDone := make(chan []*pb.PeerRecord, 1)
	go func() {
		rctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		searchDone <- a.s.Search(rctx, []string{"missing"})
	}()
	a.hasEvent(t, "requested")

	// A reply claiming to be from `unrelated`, signed by `unrelated`, published
	// by mallory. claimed != sender fails before any crypto runs.
	reply := &pb.SearchReply{
		RequestId: "forged-1", ResponderPeerId: unrelated.PeerID().String(),
		Peers: []*pb.PeerRecord{{PeerId: unrelated.PeerID().String(), Skills: []string{"missing"}}},
	}
	if err := security.NewSigner(unrelated).SignSearchReply(reply); err != nil {
		t.Fatal(err)
	}
	pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	if err := mallory.s.publish(pctx, &pb.SearchMessage{
		Kind: &pb.SearchMessage_Reply{Reply: reply},
	}); err != nil {
		cancel()
		t.Fatalf("publish forged reply: %v", err)
	}
	cancel()

	if recs := <-searchDone; len(recs) != 0 {
		t.Fatalf("forged reply leaked into results: %+v", recs)
	}
	a.hasEvent(t, "rejected")
}

// TestSearchTopicIgnoresStaleRequest: a request whose issued_at is far in the
// past must not be answered, so a redelivered epidemic backlog cannot keep
// burning responder work indefinitely.
func TestSearchTopicIgnoresStaleRequest(t *testing.T) {
	respond := newTopicNode(t, func([]string) []*pb.PeerRecord {
		return []*pb.PeerRecord{{PeerId: "12D3KooWStaleTarget", Skills: []string{"ocr"}}}
	})
	stranger := newTopicNode(t, nil)
	ctx := context.Background()
	if err := stranger.h.Connect(ctx, respond.h.AddrInfo()); err != nil {
		t.Fatalf("connect: %v", err)
	}
	// Gossipsub only fans out to mesh peers; grafting takes the first heartbeat.
	time.Sleep(1500 * time.Millisecond)
	req := &pb.SearchRequest{
		RequestId: "stale-1", RequesterPeerId: stranger.id.PeerID().String(),
		Skills: []string{"ocr"}, IssuedAt: time.Now().UTC().Add(-time.Hour).Unix(),
	}
	if err := security.NewSigner(stranger.id).SignSearchRequest(req); err != nil {
		t.Fatal(err)
	}
	if err := stranger.s.publish(ctx, &pb.SearchMessage{Kind: &pb.SearchMessage_Request{Request: req}}); err != nil {
		t.Fatalf("publish stale: %v", err)
	}
	respond.hasEvent(t, "rejected")
	if evts := respond.drainWithin(700 * time.Millisecond); containsStr(evts, "answered") {
		t.Fatal("a stale request must never be answered")
	}
}

// TestSearchTopicAnswerCooldown caps reply storms: repeated requests for one
// skill set are answered once per cooldown window, whoever asks.
func TestSearchTopicAnswerCooldown(t *testing.T) {
	b := newTopicNode(t, func([]string) []*pb.PeerRecord {
		return []*pb.PeerRecord{{PeerId: "12D3KooWAnsweredPeer", Skills: []string{"ocr"}}}
	})
	a1 := newTopicNode(t, nil)
	a2 := newTopicNode(t, nil)
	ctx := context.Background()
	if err := a1.h.Connect(ctx, b.h.AddrInfo()); err != nil {
		t.Fatal(err)
	}
	if err := a2.h.Connect(ctx, b.h.AddrInfo()); err != nil {
		t.Fatal(err)
	}
	// Let the mesh settle, then fire both searches. Whichever request arrives
	// first is answered; the other must be silenced by the cooldown.
	time.Sleep(1500 * time.Millisecond)
	for _, n := range []*topicNode{a1, a2} {
		rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		go func() {
			defer cancel()
			n.s.Search(rctx, []string{"ocr"})
		}()
	}
	b.hasEvent(t, "answered")
	if evts := b.drainWithin(2500 * time.Millisecond); containsStr(evts, "answered") {
		t.Fatal("second request inside the cooldown window must not be answered")
	}
}

// TestSearchTopicConstructorGates guards the join-time preconditions.
func TestSearchTopicConstructorGates(t *testing.T) {
	n := newTopicNode(t, nil)
	u := n.h.Underlying()
	lookup := func(peer.ID) (ic.PubKey, error) { return n.id.PubKey(), nil }
	view := func([]string) []*pb.PeerRecord { return nil }
	signer := security.NewSigner(n.id)

	cfg := topicCfg()
	ps := n.ps
	// newTopicNode already joined the default test topic, and pubsub refuses a
	// second Join of the same name; the positive control uses a distinct one.
	cfg.Name = "/zeptomesh/test-search/0.1.0-gate"
	dup, err := NewSearchTopic(SearchTopicParams{Host: u, PubSub: ps, Config: cfg,
		Signer: signer, KeyLookup: lookup, View: view})
	if err != nil {
		t.Fatalf("valid params must join: %v", err)
	}
	if err := dup.Close(); err != nil {
		t.Fatalf("close dup: %v", err)
	}
	cfg.Enabled = false
	if _, err := NewSearchTopic(SearchTopicParams{Host: u, PubSub: ps, Config: cfg,
		Signer: signer, KeyLookup: lookup, View: view}); err == nil {
		t.Fatal("disabled config must not join")
	}
	cfg = topicCfg()
	cfg.Name = "no-slash-topic"
	if _, err := NewSearchTopic(SearchTopicParams{Host: u, PubSub: ps, Config: cfg,
		Signer: signer, KeyLookup: lookup, View: view}); err == nil {
		t.Fatal("topic name without leading slash must be refused")
	}
}

func containsStr(hay []string, needle string) bool {
	for _, s := range hay {
		if s == needle {
			return true
		}
	}
	return false
}
