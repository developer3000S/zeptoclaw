package security

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

func mustPeerID(s string) peer.ID {
	p, err := peer.Decode(s)
	if err != nil {
		panic(err)
	}
	return p
}

// PeerDefenseState -----------------------------------------------------------

func TestPeerDefenseState_SetQuarantine(t *testing.T) {
	d := NewPeerDefenseState()
	if got := d.GetQuarantine(); got != QuarantineNone {
		t.Fatalf("initial: got %v, want QuarantineNone", got)
	}
	d.SetQuarantine(QuarantineBlocked, 0)
	if got := d.GetQuarantine(); got != QuarantineBlocked {
		t.Fatalf("after set: got %v, want QuarantineBlocked", got)
	}
	if !d.ExpiresAt.IsZero() {
		t.Fatal("zero TTL should leave ExpiresAt zero")
	}
	d.SetQuarantine(QuarantineIsolated, time.Hour)
	if got := d.GetQuarantine(); got != QuarantineIsolated {
		t.Fatalf("after TTL set: got %v, want QuarantineIsolated", got)
	}
	if d.ExpiresAt.IsZero() {
		t.Fatal("TTL set should produce non-zero ExpiresAt")
	}
}

func TestPeerDefenseState_SetAttackDefense(t *testing.T) {
	d := NewPeerDefenseState()
	d.SetAttackDefense(AttackDefenseCoerce)
	if got := d.GetAttackDefense(); got != AttackDefenseCoerce {
		t.Fatalf("got %v, want AttackDefenseCoerce", got)
	}
}

func TestPeerDefenseState_AddSuspicion_Escalation(t *testing.T) {
	d := NewPeerDefenseState()
	check := func(name string, want QuarantineLevel) {
		t.Helper()
		if got := d.GetQuarantine(); got != want {
			t.Errorf("%s: got %v, want %v", name, got, want)
		}
	}
	check("start", QuarantineNone)
	d.AddSuspicion(4)
	check("4pts", QuarantineNone)
	d.AddSuspicion(1) // total 5
	check("5pts", QuarantineMonitored)
	d.AddSuspicion(14) // total 19
	check("19pts", QuarantineMonitored)
	d.AddSuspicion(1) // total 20
	check("20pts", QuarantineChallenged)
	d.AddSuspicion(29) // total 49
	check("49pts", QuarantineChallenged)
	d.AddSuspicion(1) // total 50
	check("50pts", QuarantineIsolated)
	d.AddSuspicion(49) // total 99
	check("99pts", QuarantineIsolated)
	d.AddSuspicion(1) // total 100
	check("100pts", QuarantineBlocked)
}

func TestPeerDefenseState_ReduceSuspicion_DeEscalation(t *testing.T) {
	d := NewPeerDefenseState()
	d.SetQuarantine(QuarantineChallenged, 0)
	d.AddSuspicion(15)   // 15 < 20 → de-escalate to monitored
	d.ReduceSuspicion(0) // trigger de-escalation path
	if got := d.GetQuarantine(); got != QuarantineMonitored {
		t.Fatalf("after reduce: got %v, want QuarantineMonitored", got)
	}
	d.ReduceSuspicion(20) // score 0 → none
	if got := d.GetQuarantine(); got != QuarantineNone {
		t.Fatalf("after reduce to 0: got %v, want QuarantineNone", got)
	}
	if got := d.GetSuspicion(); got != 0 {
		t.Fatalf("suspicion %d, want 0", got)
	}
}

func TestPeerDefenseState_ChallengeHelpers(t *testing.T) {
	d := NewPeerDefenseState()
	nonce := []byte("abc123")
	d.SetChallenge(nonce)
	if got := d.GetAttackDefense(); got != AttackDefenseChallenge {
		t.Fatalf("after SetChallenge defense=%v, want challenge", got)
	}
	if got := d.RecordChallengeFailure(); got != 1 {
		t.Fatalf("first failure: got %d, want 1", got)
	}
	if got := d.RecordChallengeFailure(); got != 2 {
		t.Fatalf("second failure: got %d, want 2", got)
	}
	d.ClearChallenge()
	if got := d.GetAttackDefense(); got != AttackDefenseNone {
		t.Fatalf("after ClearChallenge defense=%v, want none", got)
	}
}

func TestPeerDefenseState_IsExpired(t *testing.T) {
	d := NewPeerDefenseState()
	if d.IsExpired() {
		t.Fatal("fresh state should not be expired")
	}
	// TTL in the past: set a future ExpiresAt and then check after advancing time.
	// Since SetQuarantine only sets ExpiresAt for positive TTL, we manually set it.
	d.mu.Lock()
	d.ExpiresAt = time.Now().UTC().Add(-time.Second)
	d.mu.Unlock()
	if !d.IsExpired() {
		t.Fatal("past TTL should be expired")
	}
}

// DefenseStore ----------------------------------------------------------------

func TestDefenseStore_Get(t *testing.T) {
	s := NewDefenseStore()
	p := mustPeerID("12D3KooWMNvFs8Hnv5kpA4GgrnPn1J1n5L5D9Y2k6d8hF2a3YhQ5")
	a := s.Get(p)
	if a == nil {
		t.Fatal("Get returned nil")
	}
	b := s.Get(p)
	if a != b {
		t.Fatal("Get should return the same pointer for the same peer")
	}
	if got := s.Len(); got != 1 {
		t.Fatalf("len=%d, want 1", got)
	}
}

func TestDefenseStore_Delete(t *testing.T) {
	s := NewDefenseStore()
	p := mustPeerID("12D3KooWMNvFs8Hnv5kpA4GgrnPn1J1n5L5D9Y2k6d8hF2a3YhQ5")
	s.Get(p)
	s.Delete(p)
	if got := s.Len(); got != 0 {
		t.Fatalf("len=%d, want 0", got)
	}
	after := s.Get(p)
	if after == nil {
		t.Fatal("Get after Delete should recreate entry")
	}
}

// Policy.DefenseOf -----------------------------------------------------------

func TestPolicy_DefenseOf(t *testing.T) {
	p := NewPolicy(ModeOpen, TrustUntrusted)
	pid := mustPeerID("12D3KooWMNvFs8Hnv5kpA4GgrnPn1J1n5L5D9Y2k6d8hF2a3YhQ5")
	d := p.DefenseOf(pid)
	if d == nil {
		t.Fatal("DefenseOf returned nil")
	}
	d.SetQuarantine(QuarantineIsolated, 0)
	if got := d.GetQuarantine(); got != QuarantineIsolated {
		t.Fatalf("got %v, want QuarantineIsolated", got)
	}
	// nil policy is safe
	var nilP *Policy
	if r := nilP.DefenseOf(pid); r != nil {
		t.Fatal("nil Policy.DefenseOf should return nil")
	}
}
