// Package skills implements skill exchange between mesh nodes (ТЗ 6.3, 6.4):
// every node advertises a monotonic skill epoch together with canonical
// descriptors of the skills it can serve, learns its neighbours' descriptors,
// and refreshes any view that is strictly older than the peer's own claim.
//
// The package deliberately handles metadata only. A descriptor describes what
// some peer can do; importing it never grants the importing node that skill —
// execution rights stay with the node's own operator configuration.
package skills

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
	"github.com/developer3000S/zeptoclaw/internal/wire"
)

// Descriptor is one documented skill. Version and digest are what make
// "newer" decidable; Description/Models/Attributes are operator metadata that
// routing and remote agents can use to pick the right executor.
type Descriptor struct {
	Name        string            `json:"name"`
	Version     int64             `json:"version"`
	UpdatedAt   time.Time         `json:"updated_at"`
	Digest      string            `json:"digest"`
	Description string            `json:"description,omitempty"`
	Models      []string          `json:"models,omitempty"`
	Attributes  map[string]string `json:"attributes,omitempty"`
}

// ToProto renders the canonical wire form, recomputing the digest so a stored
// or hand-edited descriptor cannot ship a stale content id.
func (d Descriptor) ToProto() *pb.SkillDescriptor {
	up := d.UpdatedAt
	if up.IsZero() {
		up = time.Now().UTC()
	}
	out := &pb.SkillDescriptor{
		Name:        normalize(d.Name),
		Version:     maxInt64(d.Version, 1),
		UpdatedAt:   up.UTC().Unix(),
		Description: d.Description,
		Models:      append([]string(nil), d.Models...),
		Attributes:  copyMap(d.Attributes),
	}
	if sum, err := wire.SkillDescriptorDigest(out); err == nil {
		out.Digest = sum
	}
	return out
}

// DescriptorFromProto converts a wire descriptor back, keeping its digest.
func DescriptorFromProto(d *pb.SkillDescriptor) Descriptor {
	if d == nil {
		return Descriptor{}
	}
	out := Descriptor{
		Name:        d.GetName(),
		Version:     d.GetVersion(),
		UpdatedAt:   time.Unix(d.GetUpdatedAt(), 0).UTC(),
		Digest:      d.GetDigest(),
		Description: d.GetDescription(),
		Models:      append([]string(nil), d.GetModels()...),
	}
	if len(d.GetAttributes()) > 0 {
		out.Attributes = make(map[string]string, len(d.GetAttributes()))
		for k, v := range d.GetAttributes() {
			out.Attributes[k] = v
		}
	}
	return out
}

func copyMap(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// contentKey captures everything an operator can say about a skill except the
// bookkeeping fields (version, timestamp, digest). Two descriptors with the
// same key state the same thing, so re-stating one is not a new claim and must
// not bump the epoch — otherwise every restart would make every peer re-pull
// descriptors that have not changed.
func (d Descriptor) contentKey() string {
	mods := append([]string(nil), d.Models...)
	sort.Strings(mods)
	parts := []string{normalize(d.Name), d.Description, strings.Join(mods, ",")}
	keys := make([]string, 0, len(d.Attributes))
	for k := range d.Attributes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		parts = append(parts, k+"="+d.Attributes[k])
	}
	return strings.Join(parts, "\x00")
}

// Newer reports whether d's content is a strictly newer statement of the same
// skill than `have`. Version is authoritative; on equal version a different
// digest means "changed and re-versioned wrongly", which is resolved by
// updated_at so a peer cannot pin us to stale bytes by replaying an old record.
func Newer(have, d Descriptor) bool {
	if d.Name != have.Name {
		return false
	}
	if d.Version != have.Version {
		return d.Version > have.Version
	}
	if d.Digest != have.Digest && d.Digest != "" {
		return d.UpdatedAt.After(have.UpdatedAt)
	}
	return false
}

// Registry holds this node's own advertised skills and the descriptors learned
// from peers, with a file-backed persistence for the local set.
type Registry struct {
	mu sync.RWMutex

	// own is the node's advertised skill set: name → descriptor.
	own map[string]Descriptor
	// epoch is the monotonic statement of "my skills changed".
	epoch int64
	// peers is the learned view: peer id → name → descriptor, plus the epoch
	// we believe that peer currently publishes.
	peers map[string]map[string]Descriptor
	// peerEpoch mirrors Capabilities.skills_version per known peer.
	peerEpoch map[string]int64

	path string
	log  *slog.Logger

	// importLimit bounds the total number of peer-learned descriptors this
	// node keeps, so a chatty (or hostile) neighbourhood cannot grow the view
	// without end. 0 = unlimited.
	importLimit int
	learned     int
}

// Options configure a Registry.
type Options struct {
	// Path is where the local skill set persists (empty = memory only).
	Path string
	// Skills are the operator-configured skill names.
	Skills []string
	// Docs optionally attach descriptions/versions to those names.
	Docs []Descriptor
	// Epoch seeds the counter when restoring state that already had one.
	Epoch int64
	// ImportLimit bounds total peer-learned descriptors (0 = unlimited).
	ImportLimit int
	Logger      *slog.Logger
}

// New builds a registry and loads any persisted local set.
func New(opts Options) (*Registry, error) {
	r := &Registry{
		own:         make(map[string]Descriptor),
		peers:       make(map[string]map[string]Descriptor),
		peerEpoch:   make(map[string]int64),
		path:        opts.Path,
		importLimit: opts.ImportLimit,
		log:         opts.Logger,
	}
	if r.log == nil {
		r.log = slog.Default()
	}
	// Order matters: the persisted ledger first, then this boot's intent on top
	// of it. Restoring last would let the config's bare skill names erase
	// documentation the node was told about after that config was written.
	if opts.Path != "" {
		if err := r.load(); err != nil {
			return nil, err
		}
	}
	seed := time.Now().UTC()
	for _, name := range opts.Skills {
		n := normalize(name)
		if n == "" {
			continue
		}
		if _, had := r.own[n]; had {
			continue // a bare name announces, it does not re-describe
		}
		r.own[n] = Descriptor{Name: n, Version: 1, UpdatedAt: seed}
		r.epoch++
	}
	for _, d := range opts.Docs {
		if !d.hasContent() {
			continue // documentation-only update with nothing to say
		}
		r.setLocked(d)
	}
	if r.epoch < opts.Epoch {
		r.epoch = opts.Epoch
	}
	if r.epoch == 0 {
		r.epoch = 1
	}
	if opts.Path != "" {
		if err := r.saveLocked(); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// hasContent reports whether a descriptor says anything beyond the name. A
// name-only entry is an announcement, not a description, and must not wipe the
// documentation already held for that skill.
func (d Descriptor) hasContent() bool {
	return strings.TrimSpace(d.Description) != "" || len(d.Models) > 0 || len(d.Attributes) > 0
}

// persisted is the on-disk shape; the epoch must survive a restart so peers do
// not see the version clock rewind to 1 on every boot.
type persisted struct {
	Epoch  int64        `json:"epoch"`
	Skills []Descriptor `json:"skills"`
}

func (r *Registry) load() error {
	raw, err := os.ReadFile(r.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("skills: read %s: %w", r.path, err)
	}
	var p persisted
	if err := json.Unmarshal(raw, &p); err != nil {
		return fmt.Errorf("skills: parse %s: %w", r.path, err)
	}
	for _, d := range p.Skills {
		n := normalize(d.Name)
		if n == "" {
			continue
		}
		d.Name = n
		if d.Version < 1 {
			d.Version = 1
		}
		r.own[n] = d
	}
	if p.Epoch > r.epoch {
		r.epoch = p.Epoch
	}
	return nil
}

func (r *Registry) saveLocked() error {
	if r.path == "" {
		return nil
	}
	p := persisted{Epoch: r.epoch}
	for _, d := range r.own {
		p.Skills = append(p.Skills, d)
	}
	sort.Slice(p.Skills, func(i, j int) bool { return p.Skills[i].Name < p.Skills[j].Name })
	raw, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(r.path), 0o700); err != nil {
		return err
	}
	tmp := r.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, r.path)
}

// Epoch is this node's current skill epoch, advertised in Capabilities and
// PeerState so a peer can tell at a glance whether its view is stale.
func (r *Registry) Epoch() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.epoch
}

// Names lists the locally advertised skills.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.own))
	for n := range r.own {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Descriptors returns the local skill set as proto, for Capabilities.
func (r *Registry) Descriptors() []*pb.SkillDescriptor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*pb.SkillDescriptor, 0, len(r.own))
	for _, d := range r.own {
		out = append(out, d.ToProto())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GetName() < out[j].GetName() })
	return out
}

// Versions renders the local set as the "what I already have" half of a sync
// request — the compact form a peer diffs against.
func (r *Registry) Versions() []*pb.SkillVersion {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*pb.SkillVersion, 0, len(r.own))
	for _, d := range r.own {
		pd := d.ToProto()
		out = append(out, &pb.SkillVersion{Name: pd.GetName(), Version: pd.GetVersion(), Digest: pd.GetDigest()})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GetName() < out[j].GetName() })
	return out
}

// Set adds or updates one local skill. Whether the statement is new is decided
// by comparing content, not the caller's version field: the API and the config
// both say what a skill is without knowing what version that makes it, and
// trusting a caller-supplied version would let a stale restation silently win.
//
// A changed skill bumps its own version and the node epoch; announcing is then
// up to the caller (the gossip heartbeat picks it up, the admin endpoint
// publishes immediately). Re-stating identical content is a no-op, so a restart
// does not make every peer re-pull descriptors nobody edited.
func (r *Registry) Set(d Descriptor) (Descriptor, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.setLocked(d)
}

func (r *Registry) setLocked(d Descriptor) (Descriptor, bool) {
	n := normalize(d.Name)
	if n == "" {
		return Descriptor{}, false
	}
	d.Name = n
	if d.UpdatedAt.IsZero() {
		d.UpdatedAt = time.Now().UTC()
	}
	cur, had := r.own[n]
	switch {
	case !had:
		d.Version = 1
	case cur.contentKey() == d.contentKey():
		return cur, false
	default:
		d.Version = cur.Version + 1
	}
	r.own[n] = d
	r.epoch++
	_ = r.saveLocked()
	return d, true
}

// Remove retires a local skill. It bumps the epoch too, so peers refresh and
// drop it from their view of this node.
func (r *Registry) Remove(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := normalize(name)
	if _, ok := r.own[n]; !ok {
		return false
	}
	delete(r.own, n)
	r.epoch++
	_ = r.saveLocked()
	return true
}

// DropDoc removes only a skill's documentation, keeping the name advertised.
// The epoch still bumps: peers must learn that what they hold about this name
// is no longer current. It reports whether a descriptor was actually dropped.
func (r *Registry) DropDoc(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := normalize(name)
	cur, ok := r.own[n]
	if !ok {
		return false
	}
	// Keep the name with a bare version-1-equivalent descriptor so the set of
	// advertised skills is unchanged; bumping the version marks the doc removal
	// as a new statement peers must re-pull.
	r.own[n] = Descriptor{Name: n, Version: cur.Version + 1, UpdatedAt: time.Now().UTC()}
	r.epoch++
	_ = r.saveLocked()
	return true
}

// PeerEpoch records what a peer currently claims. Returns true when the claim
// is newer than what we hold, i.e. a descriptor refresh is worth doing.
func (r *Registry) PeerEpoch(peerID string, epoch int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if peerID == "" || epoch <= 0 {
		return false
	}
	held := r.peerEpoch[peerID]
	if epoch > held {
		r.peerEpoch[peerID] = epoch
		return true
	}
	// Equal epoch but a lost/absent local copy of the descriptors also counts:
	// after a restart the registry knows the epoch but not the docs.
	return epoch == held && len(r.peers[peerID]) == 0
}

// KnownVersions returns what we hold about a peer, for a diffing sync request.
func (r *Registry) KnownVersions(peerID string) []*pb.SkillVersion {
	r.mu.RLock()
	defer r.mu.RUnlock()
	set := r.peers[peerID]
	out := make([]*pb.SkillVersion, 0, len(set))
	for _, d := range set {
		pd := d.ToProto()
		out = append(out, &pb.SkillVersion{Name: pd.GetName(), Version: pd.GetVersion(), Digest: pd.GetDigest()})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GetName() < out[j].GetName() })
	return out
}

// ImportPeer folds a peer's disclosed descriptors into our view, keeping only
// strictly newer entries per skill. It reports how many actually changed and
// the resulting name list, which the node writes into its neighbour table.
func (r *Registry) ImportPeer(peerID string, docs []*pb.SkillDescriptor) (updated int, names []string) {
	if peerID == "" {
		return 0, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	set := r.peers[peerID]
	if set == nil {
		set = make(map[string]Descriptor)
		r.peers[peerID] = set
	}
	for _, pd := range docs {
		if pd == nil {
			continue
		}
		// Reject a descriptor whose own digest does not match its bytes: the
		// whole point of the digest is that a peer cannot describe one skill
		// as another.
		want, err := wire.SkillDescriptorDigest(pd)
		if err == nil && pd.GetDigest() != "" && pd.GetDigest() != want {
			r.log.Debug("skill_descriptor_digest_mismatch", "peer", peerID, "skill", pd.GetName())
			continue
		}
		d := DescriptorFromProto(pd)
		d.Name = normalize(d.Name)
		if d.Name == "" {
			continue
		}
		cur, had := set[d.Name]
		if had && !Newer(cur, d) {
			continue
		}
		if had {
			d.Version = maxInt64(cur.Version, d.Version)
		} else if r.importLimit > 0 && r.learned >= r.importLimit {
			// Budget for the learned view is per node, not per peer: otherwise
			// a large mesh turns "remember what peers do" into unbounded state.
			r.log.Debug("skill_import_limit_reached", "peer", peerID, "skill", d.Name)
			continue
		}
		if !had {
			r.learned++
		}
		set[d.Name] = d
		updated++
	}
	for n := range set {
		names = append(names, n)
	}
	sort.Strings(names)
	return updated, names
}

// PeerSkills lists the descriptors learned about a peer.
func (r *Registry) PeerSkills(peerID string) []Descriptor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Descriptor, 0, len(r.peers[peerID]))
	for _, d := range r.peers[peerID] {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// PeersBySkill answers "which peers claim this skill", using the learned
// descriptors rather than only the coarse name list — the routing layer can
// then prefer a peer that documents the skill, not merely names it.
func (r *Registry) PeersBySkill(want string) []string {
	n := normalize(want)
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []string
	for pid, set := range r.peers {
		if _, ok := set[n]; ok {
			out = append(out, pid)
		}
	}
	sort.Strings(out)
	return out
}

// DropPeer forgets a peer's learned view (departure, or a key rotation that
// produced a brand-new identity).
func (r *Registry) DropPeer(peerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.learned -= len(r.peers[peerID])
	if r.learned < 0 {
		r.learned = 0
	}
	delete(r.peers, peerID)
	delete(r.peerEpoch, peerID)
}

// SelectDelta answers a peer's sync request from the local set: it returns the
// descriptors the requester does not already have (or has older).
func (r *Registry) SelectDelta(known []*pb.SkillVersion, full bool, limit int) []*pb.SkillDescriptor {
	have := make(map[string]Descriptor, len(known))
	for _, k := range known {
		if k == nil {
			continue
		}
		have[normalize(k.GetName())] = Descriptor{
			Name: normalize(k.GetName()), Version: k.GetVersion(), Digest: k.GetDigest(),
		}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []*pb.SkillDescriptor
	for _, d := range r.own {
		if full {
			out = append(out, d.ToProto())
			continue
		}
		h, ok := have[d.Name]
		if !ok || Newer(h, d) {
			out = append(out, d.ToProto())
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GetName() < out[j].GetName() })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

func normalize(s string) string { return strings.ToLower(strings.TrimSpace(s)) }

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
