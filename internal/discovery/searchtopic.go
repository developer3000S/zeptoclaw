package discovery

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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

// NewPubSub builds the one gossip router a host may have.
//
// The router is a constructor parameter of Membership and SearchTopic rather
// than something each builds itself: go-libp2p-pubsub registers its stream
// handler under a fixed protocol id, so a second NewGossipSub on the same host
// silently replaces the first router and orphans its subscriptions. A node
// that joined membership and search through two routers would see one of the
// two topics stop working with no error anywhere. The peer filter is the mesh
// policy: a blocked peer must not learn which topics we subscribe to, let
// alone deliver to them (ТЗ 11.4).
func NewPubSub(ctx context.Context, h host.Host, policy *security.Policy, audit *security.Audit, logger *slog.Logger) (*pubsub.PubSub, error) {
	if h == nil {
		return nil, errors.New("discovery: nil host")
	}
	return pubsub.NewGossipSub(ctx, h,
		pubsub.WithPeerFilter(func(p peer.ID, topic string) bool {
			if policy != nil && !policy.AllowConnection(p) {
				if audit != nil {
					audit.Log(security.AuditEvent{Event: "gossip_filtered", PeerID: p.String(), Reason: "blocked"})
				}
				if logger != nil {
					logger.Debug("pubsub_peer_filtered", "peer", p.String(), "topic", topic)
				}
				return false
			}
			return true
		}),
	)
}

// clockSkewSeconds is declared in gossip.go and shared by every timestamp gate.

// maxAnswerKeys bounds the cooldown bookkeeping: the key space is
// caller-influenced (novel skill sets mint novel keys), so expired stamps are
// pruned once the map reaches this size.
const maxAnswerKeys = 512

// SearchView answers "who, as far as this node knows, covers want". The
// returned records are claims this node is willing to sign — the consumer on
// the other side still dials and verifies each peer's own signed capabilities
// before routing anything (tasks.SkillSource.Adopt).
type SearchView func(want []string) []*pb.PeerRecord

// SearchTopic is the epidemic skill-search plane (ТЗ 6.9.5 п.5): when the
// addressed ladder (local → table → cache → relay RPC) finds no executor, a
// node publishes a signed, skills-only lookup on a shared topic and anyone
// that can help answers in public.
//
// The security shape mirrors Membership: a message is only ever attributed to
// the authenticated pubsub sender, the payload's claimed id must match that
// sender, and the signature must verify against the sender's key. Nothing
// heard here is trusted as a fact — answers are candidate records that go
// through the same dial-and-verify adoption path as relay answers, so even a
// malicious responder can only waste an adoption attempt against peers the
// adopter will confirm by their own signature (or refuse).
//
// The request deliberately names no task id and no instruction: an epidemic
// lookup reaches every subscriber, so only the *need* (a set of skills) is
// disclosed, never what will be executed.
type SearchTopic struct {
	topic  *pubsub.Topic
	sub    *pubsub.Subscription
	h      host.Host
	cfg    config.SearchTopicConfig
	signer *security.Signer
	lookup security.KeyLookup
	policy *security.Policy
	audit  *security.Audit
	view   SearchView
	log    *slog.Logger
	// observe receives metric events ("requested", "answered", "rejected",
	// "replied") without discovery importing the metrics package.
	observe func(event string)

	mu      sync.Mutex
	pending map[string]chan *pb.SearchReply
	// lastAnswer gates reply storms: one skill set is answered at most once per
	// AnswerCooldown, however many requests reach us for it.
	lastAnswer map[string]time.Time

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewSearchTopic joins the search topic on the shared pubsub router.
type SearchTopicParams struct {
	Host      host.Host
	PubSub    *pubsub.PubSub
	Config    config.SearchTopicConfig
	Signer    *security.Signer
	KeyLookup security.KeyLookup
	Policy    *security.Policy
	Audit     *security.Audit
	View      SearchView
	Logger    *slog.Logger
	OnEvent   func(event string)
}

func NewSearchTopic(p SearchTopicParams) (*SearchTopic, error) {
	if p.Host == nil {
		return nil, errors.New("discovery: search topic: nil host")
	}
	if p.PubSub == nil {
		return nil, errors.New("discovery: search topic: pubsub router required")
	}
	if !p.Config.Enabled {
		return nil, errors.New("discovery: search topic: disabled by config")
	}
	if p.Config.Name == "" || !strings.HasPrefix(p.Config.Name, "/") {
		return nil, errors.New("discovery: search topic: invalid topic name")
	}
	if p.Signer == nil || p.KeyLookup == nil || p.View == nil {
		return nil, errors.New("discovery: search topic: signer, key lookup and view are required")
	}
	topic, err := p.PubSub.Join(p.Config.Name)
	if err != nil {
		return nil, fmt.Errorf("discovery: search topic join: %w", err)
	}
	sub, err := topic.Subscribe()
	if err != nil {
		topic.Close()
		return nil, fmt.Errorf("discovery: search topic subscribe: %w", err)
	}
	return &SearchTopic{
		topic:      topic,
		sub:        sub,
		h:          p.Host,
		cfg:        p.Config,
		signer:     p.Signer,
		lookup:     p.KeyLookup,
		policy:     p.Policy,
		audit:      p.Audit,
		view:       p.View,
		log:        p.Logger,
		observe:    p.OnEvent,
		pending:    make(map[string]chan *pb.SearchReply),
		lastAnswer: make(map[string]time.Time),
	}, nil
}

// Start launches the receive loop.
func (s *SearchTopic) Start(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.cancel = cancel
	s.mu.Unlock()
	s.wg.Add(1)
	go s.receiveLoop(runCtx)
	if s.log != nil {
		s.log.Info("search_topic_started", "topic", s.cfg.Name,
			"request_ttl", s.cfg.RequestTTL.String(), "max_answers", s.cfg.MaxAnswers)
	}
	return nil
}

// Close stops the loop and leaves the topic. The shared router outlives the
// topic — it belongs to Membership's lifecycle peer, not to either of them.
func (s *SearchTopic) Close() error {
	s.mu.Lock()
	cancel := s.cancel
	s.cancel = nil
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	s.wg.Wait()
	s.sub.Cancel()
	return s.topic.Close()
}

// Search publishes a signed lookup for want and collects matching replies
// until ctx is done. It returns the candidate records (deduplicated, self
// excluded); they are claims, not facts — the caller adopts them through the
// verify-before-route path. Returning nothing is a normal outcome: the topic
// widens the search, it does not guarantee an executor exists.
func (s *SearchTopic) Search(ctx context.Context, want []string) []*pb.PeerRecord {
	want = cleanSkills(want)
	if len(want) == 0 || ctx.Err() != nil {
		return nil
	}
	id, err := randomRequestID()
	if err != nil {
		if s.log != nil {
			s.log.Warn("search_topic_request_id", "err", err.Error())
		}
		return nil
	}
	ch := make(chan *pb.SearchReply, s.cfg.MaxAnswers)
	s.mu.Lock()
	s.pending[id] = ch
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.pending, id)
		s.mu.Unlock()
	}()

	req := &pb.SearchRequest{
		RequestId:       id,
		RequesterPeerId: s.h.ID().String(),
		Skills:          want,
		IssuedAt:        time.Now().UTC().Unix(),
	}
	if err := s.signer.SignSearchRequest(req); err != nil {
		if s.log != nil {
			s.log.Warn("search_topic_sign", "err", err.Error())
		}
		return nil
	}
	msg := &pb.SearchMessage{Kind: &pb.SearchMessage_Request{Request: req}}
	if err := s.publish(ctx, msg); err != nil {
		if s.log != nil {
			s.log.Debug("search_topic_publish", "err", err.Error())
		}
		return nil
	}
	if s.observe != nil {
		s.observe("requested")
	}

	collected := make(map[string]*pb.PeerRecord)
	answers := 0
	// Once one answer is in, late repliers rarely change the routing decision
	// but do add latency, so keep draining only while replies keep arriving.
	const answerGrace = 400 * time.Millisecond
	var grace *time.Timer
	for answers < s.cfg.MaxAnswers {
		var wait <-chan time.Time
		if grace != nil {
			wait = grace.C
		}
		select {
		case <-ctx.Done():
			stopGrace(grace)
			return finishRecords(collected)
		case <-wait:
			return finishRecords(collected)
		case reply := <-ch:
			answers++
			if grace == nil {
				grace = time.NewTimer(answerGrace)
			} else {
				rearmGrace(grace, answerGrace)
			}
			for _, rec := range reply.GetPeers() {
				if rec == nil || rec.GetPeerId() == "" {
					continue
				}
				if rec.GetPeerId() == s.h.ID().String() {
					continue
				}
				// The newest claim wins: SeenAt is the responder's own
				// freshness stamp, and a later one is the less stale view.
				if old, had := collected[rec.GetPeerId()]; !had || rec.GetSeenAt() >= old.GetSeenAt() {
					collected[rec.GetPeerId()] = rec
				}
			}
		}
	}
	stopGrace(grace)
	return finishRecords(collected)
}

// rearmGrace resets a timer that may have already fired into its channel; the
// non-blocking drain covers the race between Stop returning false and the value
// being received by nobody yet.
func rearmGrace(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}

func stopGrace(t *time.Timer) {
	if t != nil {
		t.Stop()
	}
}

func finishRecords(byID map[string]*pb.PeerRecord) []*pb.PeerRecord {
	out := make([]*pb.PeerRecord, 0, len(byID))
	for _, r := range byID {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PeerId < out[j].PeerId })
	return out
}

func (s *SearchTopic) publish(ctx context.Context, msg *pb.SearchMessage) error {
	b, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return s.topic.Publish(ctx, b)
}

func (s *SearchTopic) receiveLoop(ctx context.Context) {
	defer s.wg.Done()
	for {
		msg, err := s.sub.Next(ctx)
		if err != nil {
			if ctx.Err() == nil && s.log != nil {
				s.log.Debug("search_topic_receive", "err", err.Error())
			}
			return
		}
		s.ingest(peer.ID(msg.From), msg.Data)
	}
}

func (s *SearchTopic) ingest(from peer.ID, data []byte) {
	// pubsub delivers our own publications back to our own subscription; our
	// request cannot help us and our reply is already in our own view.
	if from == s.h.ID() {
		return
	}
	msg := &pb.SearchMessage{}
	if err := proto.Unmarshal(data, msg); err != nil {
		s.reject(from, "search_unparseable", err.Error())
		return
	}
	switch k := msg.GetKind().(type) {
	case *pb.SearchMessage_Request:
		s.onRequest(from, k.Request)
	case *pb.SearchMessage_Reply:
		s.onReply(from, k.Reply)
	}
}

func (s *SearchTopic) onRequest(from peer.ID, r *pb.SearchRequest) {
	if err := security.VerifySearchRequest(r, from, s.lookup); err != nil {
		s.reject(from, "search_request_invalid", err.Error())
		return
	}
	// A stale request must not be answered: the search it prompted is already
	// over, so a reply would only feed a queue that no one drains. Future
	// timestamps are refused past the same skew bound Membership uses.
	now := time.Now().UTC().Unix()
	age := now - r.GetIssuedAt()
	if age < -int64(clockSkewSeconds) || age > int64(s.cfg.RequestTTL.D().Seconds()) {
		s.reject(from, "search_request_stale", fmt.Sprint(r.GetIssuedAt()))
		return
	}
	// Answering is not delegating: the whole point of an epidemic plane is
	// that strangers can ask. A blocked peer gets nothing (the router's peer
	// filter already drops them; AllowConnection keeps that explicit). What
	// task-trust gates is the *disclosure of other peers*: to an untrusted
	// requester a responder only ever names itself, so the answer is the
	// responder's own signed self-claim and never a routing-table dump.
	if s.policy != nil && !s.policy.AllowConnection(from) {
		s.reject(from, "search_request_refused", "blocked")
		return
	}
	disclosePeers := true
	if s.policy != nil {
		disclosePeers = s.policy.AllowTasksFrom(from)
	}
	want := cleanSkills(r.GetSkills())
	if len(want) == 0 {
		return
	}
	key := strings.Join(want, ",")
	if !s.allowAnswer(key) {
		return
	}
	recs := s.view(want)
	if !disclosePeers {
		self := s.h.ID().String()
		own := make([]*pb.PeerRecord, 0, 1)
		for _, rec := range recs {
			if rec.GetPeerId() == self {
				own = append(own, rec)
				break
			}
		}
		recs = own
	}
	recs = capRecords(recs, s.cfg.MaxAnswers)
	if len(recs) == 0 {
		// Silence is an answer too ("I know nobody"), but a node that does not
		// cover the skills and has no record of anyone who does contributes
		// nothing but traffic.
		s.resetAnswer(key)
		return
	}
	reply := &pb.SearchReply{
		RequestId:       r.GetRequestId(),
		ResponderPeerId: s.h.ID().String(),
		Peers:           recs,
		IssuedAt:        time.Now().UTC().Unix(),
	}
	if err := s.signer.SignSearchReply(reply); err != nil {
		s.resetAnswer(key)
		if s.log != nil {
			s.log.Warn("search_topic_reply_sign", "err", err.Error())
		}
		return
	}
	pctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	msg := &pb.SearchMessage{Kind: &pb.SearchMessage_Reply{Reply: reply}}
	if err := s.publish(pctx, msg); err != nil {
		s.resetAnswer(key)
		if s.log != nil {
			s.log.Debug("search_topic_reply_publish", "err", err.Error())
		}
		return
	}
	if s.observe != nil {
		s.observe("answered")
	}
	if s.log != nil {
		s.log.Info("search_topic_answered", "request_id", r.GetRequestId(),
			"requester", from.String(), "skills", strings.Join(want, ","), "peers", len(recs))
	}
}

func (s *SearchTopic) onReply(from peer.ID, r *pb.SearchReply) {
	if err := security.VerifySearchReply(r, from, s.lookup); err != nil {
		s.reject(from, "search_reply_invalid", err.Error())
		return
	}
	now := time.Now().UTC().Unix()
	age := now - r.GetIssuedAt()
	if age < -int64(clockSkewSeconds) || age > int64(s.cfg.RequestTTL.D().Seconds()) {
		s.reject(from, "search_reply_stale", fmt.Sprint(r.GetIssuedAt()))
		return
	}
	s.mu.Lock()
	ch := s.pending[r.GetRequestId()]
	s.mu.Unlock()
	if ch == nil {
		// A reply to a request we never sent (or already gave up on). Dropping
		// is correct; auditing keeps an unsolicited-reply flood visible.
		s.reject(from, "search_reply_unsolicited", r.GetRequestId())
		return
	}
	select {
	case ch <- r:
	default:
	}
	if s.observe != nil {
		s.observe("replied")
	}
}

// allowAnswer reports whether the per-skill-set cooldown has elapsed, and
// stamps the answer time when it has. The key space is caller-influenced
// (a valid-signature peer can mint novel skill sets), so the map is pruned of
// fully-expired stamps before growing past its bound.
func (s *SearchTopic) allowAnswer(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	cd := s.cfg.AnswerCooldown.D()
	if cd > 0 {
		if last, ok := s.lastAnswer[key]; ok && time.Since(last) < cd {
			return false
		}
	}
	if len(s.lastAnswer) >= maxAnswerKeys {
		for k, t := range s.lastAnswer {
			if cd <= 0 || time.Since(t) >= cd {
				delete(s.lastAnswer, k)
			}
		}
	}
	s.lastAnswer[key] = time.Now()
	return true
}

func (s *SearchTopic) resetAnswer(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.lastAnswer, key)
}

func (s *SearchTopic) reject(from peer.ID, reason, detail string) {
	if s.observe != nil {
		s.observe("rejected")
	}
	if s.audit != nil {
		s.audit.Log(security.AuditEvent{Event: reason, PeerID: from.String(), Detail: detail})
	}
}

// capRecords deduplicates by peer id and bounds the answer size, so one
// reply cannot turn into a routing-table exfiltration.
func capRecords(recs []*pb.PeerRecord, max int) []*pb.PeerRecord {
	seen := make(map[string]bool, len(recs))
	out := make([]*pb.PeerRecord, 0, len(recs))
	for _, r := range recs {
		if r == nil || r.GetPeerId() == "" || seen[r.GetPeerId()] {
			continue
		}
		seen[r.GetPeerId()] = true
		out = append(out, r)
		if max > 0 && len(out) >= max {
			break
		}
	}
	return out
}

func cleanSkills(in []string) []string {
	out := make([]string, 0, len(in))
	seen := make(map[string]bool, len(in))
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	sort.Strings(out)
	if len(out) > 32 {
		out = out[:32]
	}
	return out
}

func randomRequestID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
