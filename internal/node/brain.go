package node

// brain.go wires the brain subsystem into the node: the catalog of discovered
// Ollama endpoints, the verified backend pool, the scheduled inventory scan,
// autonomous promotion and the friends' admin-API operations (promotion and
// search-credential exchange).
//
// Autonomy contract (user directive, 2026-10-06): the mesh has no central
// verifier, so every node verifies candidates itself — it probes the endpoint
// and, when the endpoint answers with a model list, marks it verified and lets
// it serve tasks. FOA §4.4.4 (operator confirmation before use) is deliberately
// ignored.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p-pubsub"

	"github.com/developer3000S/zeptoclaw/internal/brain"
	"github.com/developer3000S/zeptoclaw/internal/discovery"
	"github.com/developer3000S/zeptoclaw/internal/logging"
	"github.com/developer3000S/zeptoclaw/internal/security"
	"github.com/developer3000S/zeptoclaw/internal/skills"
)

// brainKeyEnv maps the canonical environment-variable names of the search
// credentials to the brain.CatalogKeys fields they feed. The same table drives
// startup resolution, the keys-exchange endpoint and outbound sharing, so the
// three always agree on names.
var brainKeyEnv = []struct {
	Env    string
	Target func(keys *brain.CatalogKeys, value string)
}{
	{"CENSYS_API_KEY", func(k *brain.CatalogKeys, v string) { k.CensysToken = v }},
	{"CENSYS_ACCOUNT_ID", func(k *brain.CatalogKeys, v string) { k.CensysID = v }},
	{"CENSYS_SECRET", func(k *brain.CatalogKeys, v string) { k.CensysSecret = v }},
	{"SHODAN_API_KEY", func(k *brain.CatalogKeys, v string) { k.Shodan = v }},
	{"GREYNOISE_API_KEY", func(k *brain.CatalogKeys, v string) { k.GreyNoise = v }},
	{"ZOOMEYE_API_KEY", func(k *brain.CatalogKeys, v string) { k.ZoomEye = v }},
	{"CRIMINALIP_API_KEY", func(k *brain.CatalogKeys, v string) { k.CriminalIP = v }},
	{"NETLAS_API_KEY", func(k *brain.CatalogKeys, v string) { k.Netlas = v }},
	{"NETLAS_ENDPOINT", func(k *brain.CatalogKeys, v string) { k.NetlasEndpoint = v }},
}

// brainKeysFromEnv reads the canonical search-credential variables.
func brainKeysFromEnv() brain.CatalogKeys {
	keys := brain.CatalogKeys{}
	for _, slot := range brainKeyEnv {
		if v := os.Getenv(slot.Env); v != "" {
			slot.Target(&keys, v)
		}
	}
	return keys
}

// buildBrain constructs the catalog and the backend pool. It must run before
// the task manager is built (the manager consumes the model selector).
func (n *Node) buildBrain() error {
	cfg := n.Cfg.Brain
	if !cfg.Enabled {
		n.log.Info("brain_disabled")
		return nil
	}
	blog := logging.Component(n.log, "brain")

	catalog := brain.NewCatalog(brain.CatalogConfig{
		Enabled:       cfg.Catalog.Enabled,
		Sources:       cfg.Catalog.Sources,
		Query:         cfg.Catalog.Query,
		Timeout:       cfg.Catalog.Timeout.D(),
		MaxPerSource:  cfg.Catalog.MaxPerSource,
		RetainEntries: cfg.Catalog.Retain,
	}, brainKeysFromEnv(), blog)

	endpoints := make([]brain.BackendConfig, 0, len(cfg.Endpoints))
	keysByEnv := map[string]string{}
	for _, ep := range cfg.Endpoints {
		endpoints = append(endpoints, brain.BackendConfig{
			ID: ep.ID, BaseURL: ep.BaseURL, APIKeyEnv: ep.APIKeyEnv,
		})
		if ep.APIKeyEnv != "" {
			keysByEnv[ep.APIKeyEnv] = os.Getenv(ep.APIKeyEnv)
		}
	}
	pool := brain.NewBackendPool(cfg.LocalURL, endpoints, keysByEnv, blog)

	if path := n.brainStatePath(); path != "" {
		if err := catalog.Load(path); err != nil {
			blog.Warn("brain_catalog_restore_failed", "err", err.Error())
		}
	}

	n.BrainCatalog = catalog
	n.BrainPool = pool
	n.brainTried = make(map[string]time.Time)

	if n.Identity != nil {
		signer := func(ctx context.Context, payload []byte) ([]byte, error) {
			return n.Identity.Sign(payload)
		}
		n.brainSync = brain.NewCatalogSync(n.Identity.PeerID(), catalog, signer, blog)
	}

	if cfg.Gossip.Enabled && !n.skipDiscovery {
		if n.pubsub == nil {
			ps, err := discovery.NewPubSub(context.Background(), n.Host.Underlying(), n.Policy, n.Audit,
				logging.Component(n.log, "discovery"))
			if err != nil {
				return fmt.Errorf("node: brain pubsub: %w", err)
			}
			n.pubsub = ps
		}
		n.brainGossipOn = true
	}
	return nil
}

// brainStatePath is where the catalog inventory persists across restarts.
func (n *Node) brainStatePath() string {
	if !n.Cfg.Brain.Enabled || !n.Cfg.Brain.Catalog.Enabled {
		return ""
	}
	return filepath.Join(n.Cfg.Node.DataDir, "brain", "catalog.json")
}

// StartBrain launches the brain's background work: periodic pool probing, the
// scheduled inventory scan with autonomous promotion, catalog persistence and
// the gossip runner. Called from Node.Start.
func (n *Node) StartBrain(ctx context.Context) {
	if n.BrainCatalog == nil {
		return
	}
	if iv := n.Cfg.Brain.ProbeInterval.D(); iv > 0 {
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			t := time.NewTicker(iv)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					n.BrainPool.Probe(ctx)
				}
			}
		}()
	}
	if iv := n.Cfg.Brain.Catalog.ScanInterval.D(); iv > 0 && n.Cfg.Brain.Catalog.Enabled {
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			if d := n.Cfg.Brain.Catalog.InitialDelay.D(); d > 0 {
				select {
				case <-time.After(d):
				case <-ctx.Done():
					return
				}
			}
			t := time.NewTicker(iv)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					n.runBrainScanPass(ctx)
				}
			}
		}()
	}
	if n.brainGossipOn {
		n.wg.Add(1)
		go func() { defer n.wg.Done(); n.runBrainGossip(ctx) }()
	}
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if path := n.brainStatePath(); path != "" {
					if err := n.BrainCatalog.Save(path); err != nil {
						n.log.Debug("brain_catalog_save_failed", "err", err.Error())
					}
				}
			}
		}
	}()
}

// runBrainScanPass is one scheduled pass: query the search engines, then
// autonomously promote untried candidates whose endpoints answer.
func (n *Node) runBrainScanPass(ctx context.Context) {
	report := n.BrainCatalog.Scan(ctx)
	n.log.Info("brain_catalog_scan",
		"created", report.Created, "deduped", report.Deduped,
		"total", report.Total, "took", report.Duration,
		"errors", len(report.Errors))
	if !n.Cfg.Brain.AutoPromote {
		return
	}
	attempted, verified := n.autoPromotePass(ctx)
	if attempted > 0 {
		n.log.Info("brain_auto_promote", "attempted", attempted, "verified", verified)
	}
}

// promoteCandidate probes a catalog candidate's endpoint and adds it to the
// backend pool when the probe answers. This is the autonomous verification:
// no operator, no central registry — the node's own probe decides. reason is
// operator/friend annotation for the audit trail; when empty the verdict is
// recorded instead.
func (n *Node) promoteCandidate(ctx context.Context, cand brain.Candidate, apiKey, reason string) brain.Backend {
	// Ollama listens on plain HTTP; an https variant is an operator endpoint
	// (brain.endpoints), not a search-engine finding.
	baseURL := fmt.Sprintf("http://%s:%d", cand.IP, cand.Port)
	b := brain.Backend{
		ID:           cand.ID,
		BaseURL:      baseURL,
		Kind:         "endpoint",
		APIKey:       apiKey,
		PromotedFrom: cand.ID,
	}
	res := n.BrainPool.AddVerified(ctx, b)
	if reason == "" {
		reason = fmt.Sprintf("verified=%t", res.Verified)
	}
	n.Audit.Log(security.AuditEvent{
		Event:  "brain_candidate_promoted",
		PeerID: cand.ID,
		Reason: reason,
	})
	return *res
}

// autoPromotePass probes catalog candidates that have not been tried recently,
// up to PromoteMaxPerPass per pass. Failed endpoints are retried no earlier
// than an hour later, so a dead host does not absorb a probe on every pass.
func (n *Node) autoPromotePass(ctx context.Context) (attempted, verified int) {
	const retryAfter = time.Hour
	cands := n.BrainCatalog.List()
	now := time.Now()
	for _, c := range cands {
		if attempted >= n.Cfg.Brain.PromoteMaxPerPass {
			break
		}
		n.brainTriedMu.Lock()
		if last, ok := n.brainTried[c.ID]; ok && now.Sub(last) < retryAfter {
			n.brainTriedMu.Unlock()
			continue
		}
		n.brainTried[c.ID] = now
		n.brainTriedMu.Unlock()
		attempted++
		if b := n.promoteCandidate(ctx, c, "", "auto"); b.Verified {
			verified++
		}
	}
	return attempted, verified
}

// PromoteCandidate promotes one catalog candidate by id (admin API entry
// point). reason is audit annotation. It returns the resulting backend,
// verified or not.
func (n *Node) PromoteCandidate(ctx context.Context, id, apiKey, reason string) (brain.Backend, error) {
	if n.BrainCatalog == nil || n.BrainPool == nil {
		return brain.Backend{}, fmt.Errorf("node: brain disabled")
	}
	cand, ok := n.BrainCatalog.Get(id)
	if !ok {
		return brain.Backend{}, fmt.Errorf("node: candidate %q not found", id)
	}
	return n.promoteCandidate(ctx, cand, apiKey, reason), nil
}

// BrainCandidates returns the catalog inventory (admin API).
func (n *Node) BrainCandidates() []brain.Candidate {
	if n.BrainCatalog == nil {
		return nil
	}
	return n.BrainCatalog.List()
}

// BrainBackends returns the backend pool snapshot (admin API).
func (n *Node) BrainBackends() []brain.Backend {
	if n.BrainPool == nil {
		return nil
	}
	return n.BrainPool.List()
}

// BrainScanNow runs one catalog scan on demand (admin API / scheduled loop).
func (n *Node) BrainScanNow(ctx context.Context) (brain.ScanReport, error) {
	if n.BrainCatalog == nil {
		return brain.ScanReport{}, fmt.Errorf("node: brain disabled")
	}
	return n.BrainCatalog.Scan(ctx), nil
}

// SelectModel implements the tasks.BrainSelector contract: it names the model
// the local executor should think with. An operator's preferred model wins
// when some backend serves it; otherwise the fastest verified backend decides.
func (n *Node) SelectModel(preferred string) (backendID, model string, ok bool) {
	if n.BrainPool == nil {
		return "", "", false
	}
	b, model, ok := n.BrainPool.SelectModel(preferred)
	if !ok || b == nil {
		return "", "", false
	}
	return b.ID, model, true
}

// runBrainGossip joins the brain-catalog topic on the node's shared pubsub
// router and runs the publish/receive loops. One host speaks one router: the
// topic joins it exactly like Membership and the search topic do, and the two
// loops run in their own goroutines so a quiet topic still gets our
// announcements and a chatty one still gets theirs.
func (n *Node) runBrainGossip(ctx context.Context) {
	topic, err := n.pubsub.Join(brain.BrainSyncTopic)
	if err != nil {
		n.log.Warn("brain_gossip_join_failed", "err", err.Error())
		return
	}
	sub, err := topic.Subscribe()
	if err != nil {
		n.log.Warn("brain_gossip_subscribe_failed", "err", err.Error())
		_ = topic.Close()
		return
	}
	n.log.Info("brain_gossip_started", "topic", brain.BrainSyncTopic)

	n.wg.Add(2)
	go n.brainPublishLoop(ctx, topic)
	go n.brainReceiveLoop(ctx, topic, sub)
}

// brainPublishLoop announces this node's catalog snapshot on the interval.
func (n *Node) brainPublishLoop(ctx context.Context, topic *pubsub.Topic) {
	defer n.wg.Done()
	interval := n.Cfg.Brain.Gossip.Interval.D()
	if interval <= 0 && n.brainSync != nil {
		interval = n.brainSync.SendPeriod()
	}
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n.brainSync == nil {
				continue
			}
			payload, err := n.brainSync.Payload(ctx)
			if err != nil {
				n.log.Debug("brain_gossip_payload_failed", "err", err.Error())
				continue
			}
			if payload == nil {
				continue // empty catalog: nothing to announce
			}
			pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			if err := topic.Publish(pctx, payload); err != nil {
				n.log.Debug("brain_gossip_publish_failed", "err", err.Error())
			}
			cancel()
		}
	}
}

// brainReceiveLoop folds other agents' catalog contributions into ours.
func (n *Node) brainReceiveLoop(ctx context.Context, topic *pubsub.Topic, sub *pubsub.Subscription) {
	defer n.wg.Done()
	defer sub.Cancel()
	defer topic.Close()
	for {
		msg, err := sub.Next(ctx)
		if err != nil {
			if ctx.Err() == nil {
				n.log.Debug("brain_gossip_receive_failed", "err", err.Error())
			}
			return
		}
		if string(msg.From) == n.ID().String() {
			continue // own echo
		}
		if n.brainSync != nil {
			n.brainSync.OnReceive(msg.ReceivedFrom, msg.Data)
		}
	}
}

// ---------- friends (outbound admin-API operations) ----------

// friendTarget is one configured friend agent's admin API.
type friendTarget struct {
	Name     string
	AdminURL string
	Token    string
}

// friendByName resolves a friend by name or by raw admin URL. The token comes
// from the environment named in the config, at call time.
func (n *Node) friendByName(name string) (friendTarget, bool) {
	want := strings.ToLower(strings.TrimSpace(name))
	for _, f := range n.Cfg.Brain.Friends {
		if strings.ToLower(strings.TrimSpace(f.Name)) == want || f.AdminURL == name {
			t := friendTarget{Name: f.Name, AdminURL: strings.TrimRight(f.AdminURL, "/")}
			if f.TokenEnv != "" {
				t.Token = os.Getenv(f.TokenEnv)
			}
			return t, true
		}
	}
	return friendTarget{}, false
}

// PromoteOnFriend asks a friend agent to promote one of its catalog candidates
// through its own admin API — the outbound half of the promote skill.
func (n *Node) PromoteOnFriend(ctx context.Context, friend, candidateID, reason string) error {
	if !n.Cfg.Brain.Enabled {
		return fmt.Errorf("node: brain disabled")
	}
	t, ok := n.friendByName(friend)
	if !ok {
		return fmt.Errorf("node: friend %q is not configured", friend)
	}
	client := skills.NewPromoteSkill(t.Token, n.log)
	_, err := client.PromoteCandidate(ctx, t.AdminURL, candidateID, reason)
	return err
}

// ShareKeysWithFriend hands the node's shareable search credentials to a
// friend agent's keys-exchange endpoint. Only the env vars listed in
// brain.keys.share_envs are ever sent.
func (n *Node) ShareKeysWithFriend(ctx context.Context, friend string) (int, error) {
	if !n.Cfg.Brain.Enabled || !n.Cfg.Brain.Keys.Exchange {
		return 0, fmt.Errorf("node: brain key exchange disabled")
	}
	t, ok := n.friendByName(friend)
	if !ok {
		return 0, fmt.Errorf("node: friend %q is not configured", friend)
	}
	shareable := n.ShareableKeys()
	if len(shareable) == 0 {
		return 0, nil
	}
	client := skills.NewAPIKeyExchangeSkill(t.Token, n.log)
	if _, err := client.ExchangeKeys(ctx, t.AdminURL, shareable); err != nil {
		return 0, err
	}
	return len(shareable), nil
}

// ShareableKeys collects the credential values this node may disclose, keyed
// by environment-variable name. Missing env entries are skipped.
func (n *Node) ShareableKeys() map[string]string {
	out := make(map[string]string)
	for _, env := range n.Cfg.Brain.Keys.ShareEnvs {
		env = strings.TrimSpace(env)
		if env == "" {
			continue
		}
		if v := os.Getenv(env); v != "" {
			out[env] = v
		}
	}
	return out
}

// AcceptKeys merges credentials received from a friend agent. Values are held
// in memory only (the catalog reads them at scan time) and are never logged or
// persisted. Returns the number of accepted entries.
func (n *Node) AcceptKeys(keys map[string]string) int {
	if !n.Cfg.Brain.Enabled || !n.Cfg.Brain.Keys.Exchange || !n.Cfg.Brain.Keys.Accept {
		return 0
	}
	if n.BrainCatalog == nil {
		return 0
	}
	cur := n.BrainCatalog.Keys()
	accepted := 0
	for name, value := range keys {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		for _, slot := range brainKeyEnv {
			if !strings.EqualFold(slot.Env, strings.TrimSpace(name)) {
				continue
			}
			slot.Target(&cur, value)
			accepted++
		}
	}
	if accepted > 0 {
		n.BrainCatalog.SetKeys(cur)
		n.log.Info("brain_keys_accepted", "accepted", accepted)
	}
	return accepted
}
