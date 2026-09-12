package discovery

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	cid "github.com/ipfs/go-cid"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	mh "github.com/multiformats/go-multihash"

	"github.com/libp2p/go-libp2p/core/peer"
)

// SkillKeyNamespace domain-separates our provider keys from any other
// application sharing a DHT.
const SkillKeyNamespace = "zeptomesh/skill/v1/"

// DHT wraps the Kademlia routing table with skill advertisement and lookup.
//
// A skill is published as a provider record for a CID derived from the skill
// name; a lookup therefore returns the peers that recently announced it. This
// degrades gracefully: when the DHT is unavailable the caller falls back to the
// neighbour table, which is the behaviour the specification requires.
type DHT struct {
	inst   *dht.IpfsDHT
	log    *slog.Logger
	skills []string

	mu       sync.Mutex
	provided map[string]time.Time
	lookups  int64
	hits     int64
}

// NewDHT wraps an existing DHT instance. A nil instance yields a disabled
// discovery source rather than an error, so the node keeps running.
func NewDHT(inst *dht.IpfsDHT, skills []string, logger *slog.Logger) *DHT {
	return &DHT{inst: inst, skills: append([]string(nil), skills...), log: logger, provided: make(map[string]time.Time)}
}

// Enabled reports whether DHT discovery is active.
func (d *DHT) Enabled() bool { return d != nil && d.inst != nil }

// Start bootstraps the routing table and advertises this node's skills.
func (d *DHT) Start(ctx context.Context, bootstraps []peer.AddrInfo) error {
	if !d.Enabled() {
		return nil
	}
	if err := d.inst.Bootstrap(ctx); err != nil {
		return fmt.Errorf("discovery: dht bootstrap: %w", err)
	}
	for _, ai := range bootstraps {
		if ai.ID == "" {
			continue
		}
		dctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		if err := d.inst.Host().Connect(dctx, ai); err != nil && d.log != nil {
			d.log.Debug("dht_bootstrap_connect", "peer", ai.ID.String(), "err", err.Error())
		}
		cancel()
	}
	// Routing.FindProviders only returns useful results once we have neighbours
	// in the table, so re-provide periodically.
	go d.reprovide(ctx)
	if d.log != nil {
		d.log.Info("dht_started", "skills", d.skills, "mode", dhtModeName(d.inst.Mode()))
	}
	return nil
}

// dhtModeName renders the DHT operating mode for logs.
func dhtModeName(m dht.ModeOpt) string {
	switch m {
	case dht.ModeServer:
		return "server"
	case dht.ModeClient:
		return "client"
	case dht.ModeAutoServer:
		return "auto-server"
	default:
		return "auto"
	}
}

// republish advertises every skill, then repeats on a period well inside the
// provider-record expiry.
func (d *DHT) reprovide(ctx context.Context) {
	interval := 12 * time.Minute
	for {
		if err := d.ProvideSkills(ctx); err != nil && d.log != nil {
			d.log.Debug("dht_provide", "err", err.Error())
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

// ProvideSkills announces this node under each of its skills.
func (d *DHT) ProvideSkills(ctx context.Context) error {
	if !d.Enabled() {
		return nil
	}
	var errs []string
	for _, s := range d.skills {
		c, err := SkillKey(s)
		if err != nil {
			errs = append(errs, err.Error())
			continue
		}
		sctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		err = d.inst.Provide(sctx, c, true)
		cancel()
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", s, err))
			continue
		}
		d.mu.Lock()
		d.provided[s] = time.Now().UTC()
		d.mu.Unlock()
	}
	if len(errs) > 0 {
		return fmt.Errorf("discovery: provide failed for %d skills: %s", len(errs), strings.Join(errs, "; "))
	}
	return nil
}

// SetSkills replaces the advertised skill set (operator reload).
func (d *DHT) SetSkills(skills []string) {
	d.mu.Lock()
	d.skills = append([]string(nil), skills...)
	d.mu.Unlock()
}

// FindPeersBySkill returns up to count peers that advertised every skill in the
// list. The intersection is taken because a task needs all of them.
func (d *DHT) FindPeersBySkill(ctx context.Context, skills []string, count int) ([]peer.AddrInfo, error) {
	if !d.Enabled() {
		return nil, errors.New("discovery: dht disabled")
	}
	if count <= 0 {
		count = 20
	}
	var intersection map[peer.ID]peer.AddrInfo
	for _, s := range skills {
		key, err := SkillKey(s)
		if err != nil {
			return nil, err
		}
		lctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		found, err := d.inst.FindProviders(lctx, key)
		cancel()
		d.mu.Lock()
		d.lookups++
		d.mu.Unlock()
		if err != nil {
			return nil, fmt.Errorf("discovery: dht lookup %s: %w", s, err)
		}
		set := make(map[peer.ID]peer.AddrInfo, len(found))
		for _, ai := range found {
			if ai.ID == "" {
				continue
			}
			set[ai.ID] = ai
		}
		if intersection == nil {
			intersection = set
			continue
		}
		for p := range intersection {
			if _, ok := set[p]; !ok {
				delete(intersection, p)
			}
		}
	}
	if len(intersection) == 0 {
		return nil, nil
	}
	out := make([]peer.AddrInfo, 0, len(intersection))
	for _, ai := range intersection {
		out = append(out, ai)
		if len(out) >= count {
			break
		}
	}
	d.mu.Lock()
	d.hits += int64(len(out))
	d.mu.Unlock()
	return out, nil
}

// Providers returns the raw DHT provider records for want, so the caller can
// fold them into its neighbour table. The bool reports whether the lookup
// actually resolved (a disabled DHT or an error means "view is partial").
func (d *DHT) Providers(ctx context.Context, want []string) ([]peer.AddrInfo, bool) {
	if !d.Enabled() || len(want) == 0 {
		return nil, false
	}
	found, err := d.FindPeersBySkill(ctx, want, 32)
	if err != nil {
		return nil, false
	}
	return found, true
}

// Stats reports DHT discovery counters.
func (d *DHT) Stats() (lookups, hits int64, provided []string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for s := range d.provided {
		provided = append(provided, s)
	}
	return d.lookups, d.hits, provided
}

// Close releases the DHT.
func (d *DHT) Close() error {
	if !d.Enabled() {
		return nil
	}
	return d.inst.Close()
}

// SkillKey derives the provider key for a skill name.
func SkillKey(skill string) (cid.Cid, error) {
	skill = strings.ToLower(strings.TrimSpace(skill))
	if skill == "" {
		return cid.Undef, errors.New("discovery: empty skill")
	}
	sum, err := mh.Sum([]byte(SkillKeyNamespace+skill), mh.SHA2_256, -1)
	if err != nil {
		return cid.Undef, fmt.Errorf("discovery: skill key: %w", err)
	}
	return cid.NewCidV1(cid.Raw, sum), nil
}

// TaskKey derives the DHT key under which a task's result is announced.
func TaskKey(taskID string) cid.Cid {
	sum := sha256.Sum256([]byte("zeptomesh/task/v1/" + taskID))
	m, err := mh.Sum(sum[:], mh.ID, -1)
	if err != nil {
		// mh.ID (identity) on a 32-byte digest cannot fail; return the raw sum
		// so the caller still gets a stable key.
		m = sum[:]
	}
	return cid.NewCidV1(cid.Raw, m)
}
