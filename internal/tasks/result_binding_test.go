package tasks

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
	"github.com/developer3000S/zeptoclaw/internal/security"
	"github.com/developer3000S/zeptoclaw/internal/storage"
)

// A result must come from a peer this node actually gave the task to. The
// signature alone cannot establish that: Ed25519 peer ids are self-certifying,
// so any node on earth can produce a well-formed, correctly signed COMPLETED for
// a task id it merely heard about — task ids travel in gossip state, journal
// lookups and acks. And the worker signature is no better, because it names its
// own author: an intruder's result verifies against the intruder's key. Without
// the expected-set binding a third party could settle somebody else's task.
func TestResultFromUndelegatedPeerIsRefused(t *testing.T) {
	m := newLocalManager(t)
	delegate := foreignIdentity(t)
	stranger := foreignIdentity(t)

	env := Build(m.self.String(), NewID(), BuildRequest{Instruction: "do the work", TTL: 3}, time.Now().UTC())
	if err := m.signer.SignTask(env); err != nil {
		t.Fatalf("sign env: %v", err)
	}
	h := &handle{env: env, deadline: time.Now().UTC().Add(time.Minute),
		waiter: make(chan *pb.TaskResult, 1)}
	if !m.register(env.GetTaskId(), h) {
		t.Fatal("register")
	}

	// Perfectly signed by a peer that was never asked.
	forged := workerResult(t, stranger, env, "finished, trust me")
	ack, err := m.OnResult(context.Background(), stranger.PeerID(), forged)
	if err != nil {
		t.Fatalf("OnResult: %v", err)
	}
	if ack.GetAccepted() {
		t.Fatal("a peer we never delegated to delivered the result")
	}
	if ack.GetReason() == "" {
		t.Fatal("refusal carries no reason")
	}
	select {
	case got := <-h.waiter:
		t.Fatalf("task settled by an undelegated peer: %q", got.GetText())
	default:
	}
	// The refusal happens before anything is recorded, so the journal cannot be
	// poisoned by an attempted delivery.
	if _, err := m.store.GetResult(env.GetTaskId()); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("forged result was journalled despite the refusal (err %v)", err)
	}

	// Declaring the delegate admits exactly that peer's answer, and nobody else's.
	m.mu.Lock()
	h.expected = map[peer.ID]bool{delegate.PeerID(): true}
	m.mu.Unlock()

	other := workerResult(t, stranger, env, "second attempt")
	if ack, err := m.OnResult(context.Background(), stranger.PeerID(), other); err != nil || ack.GetAccepted() {
		t.Fatalf("stranger still accepted after binding (ack %v, err %v)", ack.GetAccepted(), err)
	}
	res := workerResult(t, delegate, env, "finished as asked")
	ack2, err := m.OnResult(context.Background(), delegate.PeerID(), res)
	if err != nil {
		t.Fatalf("OnResult from delegate: %v", err)
	}
	if !ack2.GetAccepted() {
		t.Fatalf("delegate refused: %s", ack2.GetReason())
	}
	select {
	case got := <-h.waiter:
		if got.GetText() != "finished as asked" {
			t.Fatalf("delivered text = %q", got.GetText())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the delegated peer's answer never reached the origin")
	}
}

// The relay case is what could break the binding, since an answer legitimately
// arrives from a peer the receiving node delegated to and must then travel on.
// It must never be refused *as* undelegated. The full multi-hop version
// (A→B→C, where A accepts from B and B accepts from C) is covered where it is
// real, in TestIntegrationDelegationThroughIntermediateNode; this pins the
// admission decision itself on a single hop.
func TestExpectedPeerIsNeverRefusedByBinding(t *testing.T) {
	m := newLocalManager(t)
	worker := foreignIdentity(t)

	env := Build(m.self.String(), NewID(), BuildRequest{Instruction: "relay me", TTL: 3}, time.Now().UTC())
	if err := m.signer.SignTask(env); err != nil {
		t.Fatalf("sign env: %v", err)
	}
	// An intermediate hop whose own upstream is gone: the answer is admitted and
	// only then cannot be forwarded. That is a routing outcome, and the point is
	// that it is not reported as a security refusal.
	h := &handle{env: env, deadline: time.Now().UTC().Add(time.Minute),
		expected: map[peer.ID]bool{worker.PeerID(): true}}
	if !m.register(env.GetTaskId(), h) {
		t.Fatal("register")
	}
	ack, err := m.OnResult(context.Background(), worker.PeerID(),
		workerResult(t, worker, env, "done via the relay"))
	if err != nil {
		t.Fatalf("OnResult: %v", err)
	}
	if ack.GetReason() == "task was not delegated to this peer" {
		t.Fatal("the expected peer's answer was refused by the binding")
	}
	if ack.GetReason() != "no upstream path" {
		t.Fatalf("reason = %q, want the routing outcome rather than an admission failure", ack.GetReason())
	}
}

// workerResult builds a result as the named peer would: its own transport and
// worker signatures over the shared wire bodies.
func workerResult(t *testing.T, who *security.Identity, env *pb.TaskEnvelope, text string) *pb.TaskResult {
	t.Helper()
	res := &pb.TaskResult{
		TaskId:       env.GetTaskId(),
		WorkerPeerId: who.PeerID().String(),
		SenderPeerId: who.PeerID().String(),
		Status:       pb.TaskStatus_TASK_STATUS_COMPLETED,
		Text:         text,
		StartedAt:    env.GetCreatedAt(),
		FinishedAt:   time.Now().UTC().Unix(),
		RouteStack:   []string{env.GetOriginPeerId()},
	}
	s := security.NewSigner(who)
	if err := s.SignWorkerResult(res); err != nil {
		t.Fatalf("sign result: %v", err)
	}
	return res
}

func foreignIdentity(t *testing.T) *security.Identity {
	t.Helper()
	id, err := security.LoadOrGenerate(filepath.Join(t.TempDir(), "foreign.key"))
	if err != nil {
		t.Fatalf("foreign identity: %v", err)
	}
	return id
}
