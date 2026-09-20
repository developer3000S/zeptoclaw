package tasks

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
	"github.com/developer3000S/zeptoclaw/internal/config"
	"github.com/developer3000S/zeptoclaw/internal/p2p"
	"github.com/developer3000S/zeptoclaw/internal/picoclaw"
	"github.com/developer3000S/zeptoclaw/internal/routing"
	"github.com/developer3000S/zeptoclaw/internal/security"
	"github.com/developer3000S/zeptoclaw/internal/storage"
)

func quarantineManager(t *testing.T, policy *security.Policy) *Manager {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := config.Default()
	dir := t.TempDir()
	cfg.Node.DataDir = dir
	cfg.Node.Listen = []string{"/ip4/127.0.0.1/tcp/0"}
	cfg.Discovery.DHT = false
	cfg.Identity.KeyFile = filepath.Join(dir, "peer.key")
	cfg.Tasks.Forwarding.SearchRelay.Enabled = false
	cfg.Security.RequireTaskSignature = false
	cfg.Security.RequireOriginSignature = false
	cfg.Security.TrustMode = "open"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("config validate: %v", err)
	}
	id, err := security.LoadOrGenerate(cfg.Identity.KeyFile)
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	store, err := storage.Open(storage.Options{
		InMemory:     true,
		ArtifactsDir: filepath.Join(dir, "artifacts"),
	})
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	h, err := p2p.New(context.Background(), p2p.Options{Config: cfg, Key: id.PrivKey(), Logger: logger})
	if err != nil {
		t.Fatalf("host: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	svc := p2p.NewService(h, p2p.Handlers{}, logger)
	adapter := picoclaw.NewStub(config.StubConfig{Echo: true}, cfg.Capabilities.Skills)
	m, err := NewManager(Options{
		Config:   cfg,
		Identity: id,
		Policy:   policy,
		Store:    store,
		Table:    routing.NewTable(cfg.Neighbors, store, logger, policy),
		Adapter:  adapter,
		Service:  svc,
		Logger:   logger,
	})
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.Start(ctx)
	t.Cleanup(func() { cancel(); m.Stop() })
	return m
}

func makeTestPeerID(t *testing.T) peer.ID {
	t.Helper()
	id, err := peer.Decode("12D3KooWMNvFs8Hnv5kpA4GgrnPn1J1n5L5D9Y2k6d8hF2a3YhQ5")
	if err != nil {
		t.Fatalf("peer.Decode: %v", err)
	}
	return id
}

func makeTaskEnvelope(t *testing.T, taskID string, origin peer.ID, now time.Time, signer *security.Signer) *pb.TaskEnvelope {
	t.Helper()
	env := Build(origin.String(), "", BuildRequest{
		Instruction:    "test task",
		RequiredSkills: []string{"coding"},
		TTL:            5,
		Priority:       5,
	}, now)
	env.TaskId = taskID
	env.SenderPeerId = origin.String()
	env.OriginPeerId = origin.String()

	env.ContextDigest = ContentDigest(env)
	if err := Validate(env, 0, now); err != nil {
		t.Fatalf("validEnv rejected: %v", err)
	}
	if signer != nil && origin == signer.PeerID() {
		if err := signer.SignTask(env); err != nil {
			t.Fatalf("sign task: %v", err)
		}
		if err := signer.SignTaskOrigin(env); err != nil {
			t.Fatalf("sign task origin: %v", err)
		}
	}
	return env
}

func TestManager_OnTask_QuarantineBlocked_Rejects(t *testing.T) {
	policy := security.NewPolicy(security.ModeOpen, security.TrustUntrusted)
	pid := makeTestPeerID(t)
	policy.DefenseOf(pid).SetQuarantine(security.QuarantineBlocked, 0)

	m := quarantineManager(t, policy)
	ctx := context.Background()
	now := time.Now().UTC()
	signer := m.signer
	if signer == nil {
		t.Fatal("signer is nil")
	}
	env := makeTaskEnvelope(t, "task-blocked", pid, now, signer)
	ack, err := m.OnTask(ctx, pid, env)
	if err != nil {
		t.Fatalf("OnTask error: %v", err)
	}
	if ack.GetStatus() != pb.AckStatus_ACK_STATUS_REJECTED {
		t.Fatalf("expected REJECTED, got %v", ack.GetStatus())
	}
	if ack.GetReason() != "peer under quarantine block" {
		t.Fatalf("unexpected reason: %s", ack.GetReason())
	}
}

func TestManager_OnTask_QuarantineIsolated_Rejects(t *testing.T) {
	policy := security.NewPolicy(security.ModeOpen, security.TrustUntrusted)
	pid := makeTestPeerID(t)
	policy.DefenseOf(pid).SetQuarantine(security.QuarantineIsolated, 0)

	m := quarantineManager(t, policy)
	ctx := context.Background()
	now := time.Now().UTC()
	signer := m.signer
	if signer == nil {
		t.Fatal("signer is nil")
	}
	env := makeTaskEnvelope(t, "task-isolated", pid, now, signer)
	ack, err := m.OnTask(ctx, pid, env)
	if err != nil {
		t.Fatalf("OnTask error: %v", err)
	}
	if ack.GetStatus() != pb.AckStatus_ACK_STATUS_REJECTED {
		t.Fatalf("expected REJECTED, got %v", ack.GetStatus())
	}
	if ack.GetReason() != "peer under quarantine isolate" {
		t.Fatalf("unexpected reason: %s", ack.GetReason())
	}
}

func TestManager_OnTask_QuarantineChallenged_Allows(t *testing.T) {
	policy := security.NewPolicy(security.ModeOpen, security.TrustUntrusted)
	pid := makeTestPeerID(t)
	policy.DefenseOf(pid).SetQuarantine(security.QuarantineChallenged, 0)

	m := quarantineManager(t, policy)
	ctx := context.Background()
	now := time.Now().UTC()

	env := makeTaskEnvelope(t, "task-challenged", pid, now, m.signer)
	ack, err := m.OnTask(ctx, pid, env)
	if err != nil {
		t.Fatalf("OnTask error: %v", err)
	}
	// Challenged is allowed through (local challenge happens later)
	if ack.GetStatus() == pb.AckStatus_ACK_STATUS_REJECTED && ack.GetReason() == "peer under quarantine block" {
		t.Fatal("QuarantineChallenged should not be rejected by task admission")
	}
}

func TestManager_OnTask_QuarantineMonitored_Allows(t *testing.T) {
	policy := security.NewPolicy(security.ModeOpen, security.TrustUntrusted)
	pid := makeTestPeerID(t)
	policy.DefenseOf(pid).SetQuarantine(security.QuarantineMonitored, 0)

	m := quarantineManager(t, policy)
	ctx := context.Background()
	now := time.Now().UTC()

	env := makeTaskEnvelope(t, "task-monitored", pid, now, m.signer)
	ack, err := m.OnTask(ctx, pid, env)
	if err != nil {
		t.Fatalf("OnTask error: %v", err)
	}
	// Monitored is allowed through
	if ack.GetStatus() == pb.AckStatus_ACK_STATUS_REJECTED && ack.GetReason() == "peer under quarantine block" {
		t.Fatal("QuarantineMonitored should not be rejected by task admission")
	}
}
