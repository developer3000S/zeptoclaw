package routing

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/developer3000S/zeptoclaw/internal/config"
	"github.com/developer3000S/zeptoclaw/internal/security"
)

// TestTable_RecordFailureIncreasesSuspicion verifies RecordFailure escalates suspicion by 20
func TestTable_RecordFailureIncreasesSuspicion(t *testing.T) {
	st := openStore(t)
	pid := peerID(t)

	policy := security.NewPolicy(security.ModeOpen, security.TrustUntrusted)
	nb := &Neighbor{PeerID: pid, Skills: []string{"coding"}}
	tab := NewTable(config.NeighborsConfig{}, st, quietLog(), policy)
	tab.Upsert(nb)

	// Verify initial state (should be 0 suspicion)
	if got := policy.DefenseOf(pid).GetSuspicion(); got != 0 {
		t.Fatalf("initial suspicion = %d, want 0", got)
	}

	// Record failure should increase suspicion by 20
	tab.RecordFailure(pid)

	if got := policy.DefenseOf(pid).GetSuspicion(); got != 20 {
		t.Fatalf("after RecordFailure, got %d, want 20", got)
	}
}

// TestTable_RecordSuccessDecaysSuspicion verifies RecordSuccess decays suspicion by 20
func TestTable_RecordSuccessDecaysSuspicion(t *testing.T) {
	st := openStore(t)
	pid := peerID(t)

	policy := security.NewPolicy(security.ModeOpen, security.TrustUntrusted)
	nb := &Neighbor{PeerID: pid, Skills: []string{"coding"}}
	tab := NewTable(config.NeighborsConfig{}, st, quietLog(), policy)
	tab.Upsert(nb)

	// Set initial suspicion to 100 to simulate prior failures
	policy.DefenseOf(pid).SetQuarantine(security.QuarantineNone, 0)
	policy.DefenseOf(pid).AddSuspicion(100)
	if got := policy.DefenseOf(pid).GetSuspicion(); got != 100 {
		t.Fatalf("setup: suspicion = %d, want 100", got)
	}

	// Record success should reduce suspicion by 20
	tab.RecordSuccess(pid)

	if got := policy.DefenseOf(pid).GetSuspicion(); got != 80 {
		t.Fatalf("after RecordSuccess, got %d, want 80", got)
	}
}

// TestTable_SuspicionTriggersQuarantineChange verifies escalation thresholds
func TestTable_SuspicionTriggersQuarantineChange(t *testing.T) {
	st := openStore(t)
	pid := peerID(t)

	policy := security.NewPolicy(security.ModeOpen, security.TrustUntrusted)
	nb := &Neighbor{PeerID: pid, Skills: []string{"coding"}}
	tab := NewTable(config.NeighborsConfig{}, st, quietLog(), policy)
	tab.Upsert(nb)

	d := policy.DefenseOf(pid)
	if got := d.GetQuarantine(); got != security.QuarantineNone {
		t.Fatalf("initial = %v, want none", got)
	}

	// Add suspicion up to 5 -> Monitored
	d.AddSuspicion(5)
	if got := d.GetQuarantine(); got != security.QuarantineMonitored {
		t.Fatalf("after +5 = %v, want monitored", got)
	}

	// Add more -> 20 -> Challenged
	d.AddSuspicion(15)
	if got := d.GetQuarantine(); got != security.QuarantineChallenged {
		t.Fatalf("after +15 (total 20) = %v, want challenged", got)
	}

	// Add more -> 50 -> Isolated
	d.AddSuspicion(30)
	if got := d.GetQuarantine(); got != security.QuarantineIsolated {
		t.Fatalf("after +30 (total 50) = %v, want isolated", got)
	}

	// Add more -> 100 -> Blocked
	d.AddSuspicion(50)
	if got := d.GetQuarantine(); got != security.QuarantineBlocked {
		t.Fatalf("after +50 (total 100) = %v, want blocked", got)
	}
}

// TestTable_SuspicionDecayDeescalates verifies ReduceSuspicion de-escalates levels
func TestTable_SuspicionDecayDeescalates(t *testing.T) {
	st := openStore(t)
	pid := peerID(t)

	policy := security.NewPolicy(security.ModeOpen, security.TrustUntrusted)
	nb := &Neighbor{PeerID: pid, Skills: []string{"coding"}}
	tab := NewTable(config.NeighborsConfig{}, st, quietLog(), policy)
	tab.Upsert(nb)

	d := policy.DefenseOf(pid)
	d.AddSuspicion(100) // Blocked
	if got := d.GetQuarantine(); got != security.QuarantineBlocked {
		t.Fatalf("after +100 = %v, want blocked", got)
	}

	// Successes should de-escalate when no TTL pins the level
	for i := 0; i < 10; i++ {
		tab.RecordSuccess(pid)
	}

	// Should be back to none after enough decays
	if got := d.GetQuarantine(); got != security.QuarantineNone {
		t.Fatalf("after 10 successes = %v, want none", got)
	}
}

// TestTable_SuspicionWithTTLDoesNotDeescalate verifies TTL pins quarantine level
func TestTable_SuspicionWithTTLDoesNotDeescalate(t *testing.T) {
	st := openStore(t)
	pid := peerID(t)

	policy := security.NewPolicy(security.ModeOpen, security.TrustUntrusted)
	nb := &Neighbor{PeerID: pid, Skills: []string{"coding"}}
	tab := NewTable(config.NeighborsConfig{}, st, quietLog(), policy)
	tab.Upsert(nb)

	d := policy.DefenseOf(pid)
	d.AddSuspicion(20)                                        // Challenged
	d.SetQuarantine(security.QuarantineChallenged, time.Hour) // TTL pins
	d.AddSuspicion(30)                                        // Isolated (60 total)

	if got := d.GetQuarantine(); got != security.QuarantineIsolated {
		t.Fatalf("after TTL set = %v, want isolated", got)
	}

	// Even successes should not de-escalate while TTL is active
	tab.RecordSuccess(pid)
	tab.RecordSuccess(pid)
	tab.RecordSuccess(pid)

	if got := d.GetQuarantine(); got != security.QuarantineIsolated {
		t.Fatalf("with active TTL, got %v, want isolated", got)
	}
}

// TestTable_RecordSuccessNilPolicyIsSafe verifies nil policy doesn't panic
func TestTable_RecordSuccessNilPolicyIsSafe(t *testing.T) {
	st := openStore(t)
	pid := peerID(t)

	tab := NewTable(config.NeighborsConfig{}, st, quietLog(), nil)
	nb := &Neighbor{PeerID: pid, Skills: []string{"coding"}}
	tab.Upsert(nb)

	// Should not panic with nil policy
	tab.RecordSuccess(pid)
	tab.RecordFailure(pid)
}

// TestTable_RecordSuccessUnknownPeerIsNoOp verifies unknown peer is skipped
func TestTable_RecordSuccessUnknownPeerIsNoOp(t *testing.T) {
	st := openStore(t)
	_ = peerID(t)

	policy := security.NewPolicy(security.ModeOpen, security.TrustUntrusted)
	tab := NewTable(config.NeighborsConfig{}, st, quietLog(), policy)

	unknown := peer.ID("12D3KooWMNvFs8Hnv5kpA4GgrnPn1J1n5L5D9Y2k6d8hF2a3YhQ5")

	// Should not panic for unknown peer
	tab.RecordSuccess(unknown)
	tab.RecordFailure(unknown)
}
