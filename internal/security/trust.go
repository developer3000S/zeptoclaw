package security

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// Trust is the local classification of a peer. Higher value = less trusted.
type Trust int

// Trust levels, ordered from most to least privileged.
const (
	TrustTrusted Trust = iota
	TrustKnown
	TrustLimited
	TrustUntrusted
	TrustBlocked
)

// QuarantineLevel is the active defense isolation level applied to a peer.
type QuarantineLevel int

const (
	// QuarantineNone means no active defense is in effect.
	QuarantineNone QuarantineLevel = iota
	// QuarantineMonitored: peer connections observed but limited rate/suspicion score.
	QuarantineMonitored
	// QuarantineChallenged: peer must complete a challenge-response to progress.
	QuarantineChallenged
	// QuarantineIsolated: peer connections are throttled; only probe traffic allowed.
	QuarantineIsolated
	// QuarantineBlocked: same as TrustBlocked but may auto-expire with TTL.
	QuarantineBlocked
)

// AttackDefenseLevel defines the active defense posture applied to hostile peers.
type AttackDefenseLevel int

const (
	// AttackDefenseNone means no active defense against hostile peers.
	AttackDefenseNone AttackDefenseLevel = iota
	// AttackDefenseMonitor: monitor hostile peer behavior for intelligence gathering.
	AttackDefenseMonitor
	// AttackDefenseChallenge: issue cryptographic challenges to hostile peers.
	AttackDefenseChallenge
	// AttackDefenseReformat: attempt to reprogram hostile peer behavior.
	AttackDefenseReformat
	// AttackDefenseIntegrate: attempt to integrate hostile peer as a friend.
	AttackDefenseIntegrate
	// AttackDefenseCoerce: force hostile peer to cooperate through strategic pressure.
	AttackDefenseCoerce
)

// String implements fmt.Stringer.
func (t Trust) String() string {
	switch t {
	case TrustTrusted:
		return "trusted"
	case TrustKnown:
		return "known"
	case TrustLimited:
		return "limited"
	case TrustUntrusted:
		return "untrusted"
	case TrustBlocked:
		return "blocked"
	default:
		return "unknown"
	}
}

// ParseTrust maps a configuration string to a Trust level.
func ParseTrust(s string) (Trust, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "trusted":
		return TrustTrusted, nil
	case "known":
		return TrustKnown, nil
	case "limited":
		return TrustLimited, nil
	case "untrusted", "":
		return TrustUntrusted, nil
	case "blocked":
		return TrustBlocked, nil
	default:
		return TrustUntrusted, fmt.Errorf("security: unknown trust level %q", s)
	}
}

// AtLeast reports whether t is at least as trusted as want.
func (t Trust) AtLeast(want Trust) bool { return t <= want }

// String implements fmt.Stringer for QuarantineLevel.
func (q QuarantineLevel) String() string {
	switch q {
	case QuarantineMonitored:
		return "monitored"
	case QuarantineChallenged:
		return "challenged"
	case QuarantineIsolated:
		return "isolated"
	case QuarantineBlocked:
		return "blocked"
	default:
		return "none"
	}
}

// String implements fmt.Stringer for AttackDefenseLevel.
func (d AttackDefenseLevel) String() string {
	switch d {
	case AttackDefenseMonitor:
		return "monitor"
	case AttackDefenseChallenge:
		return "challenge"
	case AttackDefenseReformat:
		return "reformat"
	case AttackDefenseIntegrate:
		return "integrate"
	case AttackDefenseCoerce:
		return "coerce"
	default:
		return "none"
	}
}

// PeerDefenseState tracks the active defense posture for a single peer.
// It is purely LOCAL state — no peer can be forced to change its behavior.
// The local node decides how much to restrict, challenge, or monitor based on
// observed behavior. All levels are opt-in by the local operator's policy.
type PeerDefenseState struct {
	mu sync.RWMutex

	// Current quarantine level applied to this peer.
	QuarantineLevel QuarantineLevel
	// Current attack defense posture.
	AttackDefenseLevel AttackDefenseLevel
	// Suspicion score accumulates on failures, decays on success.
	SuspicionScore int
	// Challenge nonce for the current challenge round (if challenged).
	ChallengeNonce []byte
	// Consecutive failure streak for the challenge.
	FailureStreak int
	// When the current state auto-expires (0 = no expiry).
	ExpiresAt time.Time
	// Timestamp of the last state change (for debugging/audit).
	UpdatedAt time.Time
}

// NewPeerDefenseState creates a clean defense state for a peer.
func NewPeerDefenseState() *PeerDefenseState {
	return &PeerDefenseState{
		QuarantineLevel:    QuarantineNone,
		AttackDefenseLevel: AttackDefenseNone,
		UpdatedAt:          time.Now().UTC(),
	}
}

// SetQuarantine applies a quarantine level with an optional TTL.
func (d *PeerDefenseState) SetQuarantine(level QuarantineLevel, ttl time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.QuarantineLevel = level
	d.UpdatedAt = time.Now().UTC()
	if ttl > 0 {
		d.ExpiresAt = d.UpdatedAt.Add(ttl)
	} else {
		d.ExpiresAt = time.Time{}
	}
}

// SetAttackDefense applies an attack defense posture.
func (d *PeerDefenseState) SetAttackDefense(level AttackDefenseLevel) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.AttackDefenseLevel = level
	d.UpdatedAt = time.Now().UTC()
}

// AddSuspicion increments the suspicion score and checks for escalation.
// Returns true if the quarantine level should escalate.
func (d *PeerDefenseState) AddSuspicion(delta int) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.SuspicionScore += delta
	d.UpdatedAt = time.Now().UTC()

	// Escalation thresholds (local policy, not forced on peer)
	oldLevel := d.QuarantineLevel
	switch {
	case d.SuspicionScore >= 100 && d.QuarantineLevel < QuarantineBlocked:
		d.QuarantineLevel = QuarantineBlocked
	case d.SuspicionScore >= 50 && d.QuarantineLevel < QuarantineIsolated:
		d.QuarantineLevel = QuarantineIsolated
	case d.SuspicionScore >= 20 && d.QuarantineLevel < QuarantineChallenged:
		d.QuarantineLevel = QuarantineChallenged
	case d.SuspicionScore >= 5 && d.QuarantineLevel < QuarantineMonitored:
		d.QuarantineLevel = QuarantineMonitored
	}
	return d.QuarantineLevel != oldLevel
}

// ReduceSuspicion decays the suspicion score on successful interactions.
func (d *PeerDefenseState) ReduceSuspicion(delta int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.SuspicionScore -= delta
	if d.SuspicionScore < 0 {
		d.SuspicionScore = 0
	}
	d.UpdatedAt = time.Now().UTC()
	// De-escalation (only if no explicit TTL pinning the level)
	if d.ExpiresAt.IsZero() {
		switch {
		case d.SuspicionScore < 5 && d.QuarantineLevel > QuarantineNone:
			d.QuarantineLevel = QuarantineNone
		case d.SuspicionScore < 20 && d.QuarantineLevel > QuarantineMonitored:
			d.QuarantineLevel = QuarantineMonitored
		case d.SuspicionScore < 50 && d.QuarantineLevel > QuarantineChallenged:
			d.QuarantineLevel = QuarantineChallenged
		case d.SuspicionScore < 100 && d.QuarantineLevel > QuarantineIsolated:
			d.QuarantineLevel = QuarantineIsolated
		}
	}
}

// SetChallenge starts a new challenge round for this peer.
func (d *PeerDefenseState) SetChallenge(nonce []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ChallengeNonce = nonce
	d.FailureStreak = 0
	d.AttackDefenseLevel = AttackDefenseChallenge
	d.UpdatedAt = time.Now().UTC()
}

// RecordChallengeFailure increments the failure streak.
func (d *PeerDefenseState) RecordChallengeFailure() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.FailureStreak++
	d.UpdatedAt = time.Now().UTC()
	return d.FailureStreak
}

// ClearChallenge resets the challenge state.
func (d *PeerDefenseState) ClearChallenge() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ChallengeNonce = nil
	d.FailureStreak = 0
	d.AttackDefenseLevel = AttackDefenseNone
	d.UpdatedAt = time.Now().UTC()
}

// IsExpired reports whether a TTL-pinned state has expired.
func (d *PeerDefenseState) IsExpired() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.ExpiresAt.IsZero() {
		return false
	}
	return time.Now().UTC().After(d.ExpiresAt)
}

// GetQuarantine returns the current quarantine level (thread-safe).
func (d *PeerDefenseState) GetQuarantine() QuarantineLevel {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.QuarantineLevel
}

// GetAttackDefense returns the current attack defense level (thread-safe).
func (d *PeerDefenseState) GetAttackDefense() AttackDefenseLevel {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.AttackDefenseLevel
}

// GetSuspicion returns the current suspicion score (thread-safe).
func (d *PeerDefenseState) GetSuspicion() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.SuspicionScore
}

// Mode is the node-wide trust posture.
type Mode int

// Trust modes.
const (
	// ModeOpen accepts tasks from any authenticated peer.
	ModeOpen Mode = iota
	// ModeLimited accepts tasks from peers that reached a minimum trust level.
	ModeLimited
	// ModePrivate accepts tasks only from the explicit allow list.
	ModePrivate
)

// ParseMode maps a configuration string to a Mode.
func ParseMode(s string) (Mode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "open":
		return ModeOpen, nil
	case "limited", "":
		return ModeLimited, nil
	case "private", "allowlist":
		return ModePrivate, nil
	default:
		return ModeLimited, fmt.Errorf("security: unknown trust mode %q", s)
	}
}

// Policy decides whether a peer may perform an action on this node.
//
// Allow lists and deny lists are loaded from files with one base58 peer id per
// line; '#' starts a comment. Trust is sticky: an operator entry always beats
// an observed value.
type Policy struct {
	mu sync.RWMutex

	mode Mode
	// minTrustForTasks is the lowest trust level accepted for task execution.
	minTrustForTasks Trust

	allow    map[peer.ID]bool
	deny     map[peer.ID]bool
	observed map[peer.ID]Trust
	// rebinds, when installed, makes identity rotation and revocation part of
	// the trust decision instead of an operator chore (ТЗ 11.2 п.3–4): a retired
	// id resolves to its successor's trust, and an id revoked by its own key is
	// blocked everywhere without editing files on every node.
	rebinds *RebindStore
	// defense holds per-peer active defense state (quarantine, suspicion,
	// challenge). It is purely LOCAL: the node decides how to protect itself,
	// never what another peer must do.
	defense *DefenseStore
}

// DefenseStore holds per-peer active defense state. It is purely LOCAL:
// each node decides how to protect itself (quarantine, suspicion, challenge)
// without forcing any behavior on the remote peer.
type DefenseStore struct {
	mu   sync.RWMutex
	slot map[peer.ID]*PeerDefenseState
}

// NewDefenseStore creates an empty defense store.
func NewDefenseStore() *DefenseStore {
	return &DefenseStore{
		slot: make(map[peer.ID]*PeerDefenseState),
	}
}

// Get returns the defense state for a peer, creating it if missing.
func (s *DefenseStore) Get(p peer.ID) *PeerDefenseState {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st := s.slot[p]; st != nil {
		return st
	}
	st := NewPeerDefenseState()
	s.slot[p] = st
	return st
}

// Delete removes the defense state for a peer (e.g. on permanent ban).
func (s *DefenseStore) Delete(p peer.ID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.slot, p)
}

// Len returns the number of tracked peers.
func (s *DefenseStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.slot)
}

// NewPolicy builds a policy from configuration values.
func NewPolicy(mode Mode, minTrustForTasks Trust) *Policy {
	return &Policy{
		mode:             mode,
		minTrustForTasks: minTrustForTasks,
		allow:            make(map[peer.ID]bool),
		deny:             make(map[peer.ID]bool),
		observed:         make(map[peer.ID]Trust),
		defense:          NewDefenseStore(),
	}
}

// LoadPeerFile reads a peer-id list file. A missing file is not an error: it
// simply contributes no entries.
func (p *Policy) LoadPeerFile(path string, allow bool) (int, error) {
	if path == "" {
		return 0, nil
	}
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("security: open %s: %w", path, err)
	}
	defer f.Close()

	n := 0
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 4096), 1<<16)
	for line := 1; sc.Scan(); line++ {
		text := strings.TrimSpace(sc.Text())
		if i := strings.IndexByte(text, '#'); i >= 0 {
			text = strings.TrimSpace(text[:i])
		}
		if text == "" {
			continue
		}
		pid, err := peer.Decode(text)
		if err != nil {
			return n, fmt.Errorf("security: %s:%d: %q is not a peer id", path, line, text)
		}
		p.SetListed(pid, allow)
		n++
	}
	if err := sc.Err(); err != nil {
		return n, fmt.Errorf("security: read %s: %w", path, err)
	}
	return n, nil
}

// SetListed records an explicit operator decision.
func (p *Policy) SetListed(pid peer.ID, allow bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if allow {
		p.allow[pid] = true
		delete(p.deny, pid)
		return
	}
	p.deny[pid] = true
	delete(p.allow, pid)
}

// SetRebindStore installs the identity-handover ledger. Safe to call once at
// startup; readers see either the old or the new store, never a torn value.
func (p *Policy) SetRebindStore(s *RebindStore) {
	p.mu.Lock()
	p.rebinds = s
	p.mu.Unlock()
}

// RebindStoreRef returns the installed ledger (nil if none).
func (p *Policy) RebindStoreRef() *RebindStore {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.rebinds
}

// Observe records an inferred trust level for a peer we have met.
func (p *Policy) Observe(pid peer.ID, t Trust) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if cur, ok := p.observed[pid]; ok && cur <= t {
		return // never downgrade silently on a single observation
	}
	p.observed[pid] = t
}

// TrustOf returns the effective trust level for a peer.
//
// Rotation is handled by identity class rather than by following a pointer
// forward: a KeyRebind declares that two identifiers are the same node, so the
// operator's decision about that node has to apply to every key in the class.
// A deny or a self-revocation anywhere in the class blocks all of it (a
// compromised key must not escape its block by minting a successor — the
// successor is bound to the blocked id by the attacker's own forged-looking
// statement, which is precisely what makes it traceable), while the most
// privileged allow/observation in the class is what admits it.
func (p *Policy) TrustOf(pid peer.ID) Trust {
	// Resolved before taking p.mu: the ledger has its own lock, and nesting the
	// two in an unspecified order would be a deadlock waiting for a future
	// callback from one to the other.
	cls := []peer.ID{pid}
	if rb := p.RebindStoreRef(); rb != nil {
		if members := rb.ClassOf(pid); len(members) > 0 {
			cls = members
		}
		if rb.ClassRevoked(pid) {
			return TrustBlocked
		}
	}

	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, m := range cls {
		if p.deny[m] {
			return TrustBlocked
		}
	}
	listed := false
	best, haveBest := TrustBlocked, false
	for _, m := range cls {
		if p.allow[m] {
			listed = true
		}
		if t, ok := p.observed[m]; ok && (!haveBest || t < best) {
			best, haveBest = t, true
		}
	}
	if listed {
		// An allow-list entry is at least "known"; in private mode it is trusted.
		if p.mode == ModePrivate {
			return TrustTrusted
		}
		if haveBest && best <= TrustKnown {
			return best
		}
		return TrustKnown
	}
	if haveBest {
		return best
	}
	if p.mode == ModePrivate {
		return TrustBlocked
	}
	return TrustUntrusted
}

// AllowConnection reports whether an inbound/outbound connection to pid should
// be accepted at all.
func (p *Policy) AllowConnection(pid peer.ID) bool {
	return p.TrustOf(pid) != TrustBlocked
}

// AllowTasksFrom reports whether pid may submit or forward tasks here.
func (p *Policy) AllowTasksFrom(pid peer.ID) bool {
	t := p.TrustOf(pid)
	if t == TrustBlocked {
		return false
	}
	switch p.mode {
	case ModeOpen:
		return true
	case ModePrivate:
		return p.explicitlyAllowed(pid)
	default:
		return t.AtLeast(p.minTrustForTasks)
	}
}

// AllowDelegationTo reports whether we may hand work to pid.
//
// Blocked is always fatal. Open mode deliberately permits untrusted peers —
// that is the mode's contract, and refusing them here would make an open mesh
// unable to route at all, because a freshly discovered peer is untrusted by
// definition until it is allow-listed.
func (p *Policy) AllowDelegationTo(pid peer.ID) bool {
	t := p.TrustOf(pid)
	if t == TrustBlocked {
		return false
	}
	switch p.mode {
	case ModeOpen:
		return true
	case ModePrivate:
		return p.explicitlyAllowed(pid)
	default:
		return t != TrustUntrusted
	}
}

// explicitlyAllowed reports whether pid, or any identity known to be the same
// node, is on the operator allow list. In private mode this is the admission
// decision, so it must be class-aware: otherwise a planned rotation would lock
// the node out of its own private mesh.
func (p *Policy) explicitlyAllowed(pid peer.ID) bool {
	cls := []peer.ID{pid}
	if rb := p.RebindStoreRef(); rb != nil {
		if members := rb.ClassOf(pid); len(members) > 0 {
			cls = members
		}
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, m := range cls {
		if p.allow[m] {
			return true
		}
	}
	return false
}

// Mode returns the current posture.
func (p *Policy) Mode() Mode {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.mode
}

// SetPosture swaps the live trust posture. Explicit allow/deny entries and
// per-peer observations survive unchanged: revoking or admitting a peer is
// its own deliberate act, never a side effect of a config reload (ТЗ 11.2 п.4).
func (p *Policy) SetPosture(mode Mode, minTrustForTasks Trust) {
	p.mu.Lock()
	p.mode = mode
	p.minTrustForTasks = minTrustForTasks
	p.mu.Unlock()
}

// Snapshot lists peers with an explicit operator decision.
func (p *Policy) Snapshot() (allowed, blocked []string) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for pid := range p.allow {
		allowed = append(allowed, pid.String())
	}
	for pid := range p.deny {
		blocked = append(blocked, pid.String())
	}
	return allowed, blocked
}

// MinTrustForTasks is the currently effective floor for accepting work.
func (p *Policy) MinTrustForTasks() Trust {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.minTrustForTasks
}

// DefenseOf returns the active defense state for a peer.
func (p *Policy) DefenseOf(pid peer.ID) *PeerDefenseState {
	if p == nil || p.defense == nil {
		return nil
	}
	return p.defense.Get(pid)
}

// String implements fmt.Stringer for log and status output.
func (m Mode) String() string {
	switch m {
	case ModeOpen:
		return "open"
	case ModePrivate:
		return "private"
	default:
		return "limited"
	}
}
