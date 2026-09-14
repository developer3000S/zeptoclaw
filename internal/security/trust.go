package security

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

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
}

// NewPolicy builds a policy from configuration values.
func NewPolicy(mode Mode, minTrustForTasks Trust) *Policy {
	return &Policy{
		mode:             mode,
		minTrustForTasks: minTrustForTasks,
		allow:            make(map[peer.ID]bool),
		deny:             make(map[peer.ID]bool),
		observed:         make(map[peer.ID]Trust),
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
