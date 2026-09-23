package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/developer3000S/zeptoclaw/internal/routing"
	"github.com/developer3000S/zeptoclaw/internal/security"
	"github.com/developer3000S/zeptoclaw/internal/skills"

	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
)

// RotateKeyResult describes a completed identity rotation.
type RotateKeyResult struct {
	OldPeerID   peer.ID       `json:"old_peer_id"`
	NewPeerID   peer.ID       `json:"new_peer_id"`
	Rebind      *pb.KeyRebind `json:"-"`
	AnnouncedTo []string      `json:"announced_to,omitempty"`
	Restart     bool          `json:"restart_required"`
}

// RotateKey replaces this node's identity key with a freshly generated one.
func (n *Node) RotateKey(ctx context.Context, reason string) (*RotateKeyResult, error) {
	if reason == "" {
		reason = "rotation"
	}
	old := n.Identity
	fresh, err := security.NewEphemeral()
	if err != nil {
		return nil, fmt.Errorf("node: generate new identity: %w", err)
	}
	k, err := security.IssueRebind(old, fresh, reason, n.Rebinds.NextSequence(old.PeerID()))
	if err != nil {
		return nil, fmt.Errorf("node: build rebind statement: %w", err)
	}

	res := &RotateKeyResult{OldPeerID: old.PeerID(), NewPeerID: fresh.PeerID(), Rebind: k, Restart: true}
	if err := n.PublishRebind(k); err != nil {
		return nil, fmt.Errorf("node: record rebind statement: %w", err)
	}
	res.AnnouncedTo = n.pushRebind(ctx, k)

	if err := fresh.Save(n.Cfg.Identity.KeyFile); err != nil {
		return nil, fmt.Errorf("node: install new key: %w", err)
	}
	n.log.Info("identity_rotated", "old", res.OldPeerID.String(), "new", res.NewPeerID.String(),
		"reason", reason, "announced", len(res.AnnouncedTo))
	return res, nil
}

// pushRebind sends the statement directly to every connected peer.
func (n *Node) pushRebind(ctx context.Context, k *pb.KeyRebind) []string {
	var sent []string
	for _, nb := range n.Table.List() {
		if nb.PeerID == n.ID() || !nb.Connected || nb.Left {
			continue
		}
		dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		resp, err := n.Service.RPC(dctx, nb.PeerID, &pb.RpcRequest{Kind: &pb.RpcRequest_Rebind{
			Rebind: &pb.RebindRequest{Rebind: k},
		}})
		cancel()
		if err != nil {
			n.log.Debug("rebind_push_failed", "peer", nb.PeerID.String(), "err", err.Error())
			continue
		}
		if r := resp.GetRebind(); r != nil && r.GetAccepted() {
			sent = append(sent, nb.PeerID.String())
		}
	}
	return sent
}

// RevokeSelf retires this node's identity for good.
func (n *Node) RevokeSelf(ctx context.Context, reason string) error {
	if reason == "" {
		reason = "retire"
	}
	k, err := security.IssueRevocation(n.Identity, reason, n.Rebinds.NextSequence(n.ID()))
	if err != nil {
		return fmt.Errorf("node: build revocation: %w", err)
	}
	if err := n.PublishRebind(k); err != nil {
		return fmt.Errorf("node: record revocation: %w", err)
	}
	pushed := n.pushRebind(ctx, k)
	n.log.Warn("identity_revoked", "peer", n.ID().String(), "reason", reason, "announced", len(pushed))
	if err := n.retireKeyFile(); err != nil {
		n.log.Error("revocation_key_retirement_failed", "err", err.Error(),
			"hint", "remove "+n.Cfg.Identity.KeyFile+" manually before the next start")
	}
	n.haltedOnce.Do(func() { close(n.halted) })
	return nil
}

func (n *Node) retireKeyFile() error {
	path := n.Cfg.Identity.KeyFile
	if path == "" {
		return nil
	}
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("node: stat key file: %w", err)
	}
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, filepath.Base(path)+".revoked-*")
	if err != nil {
		return fmt.Errorf("node: name key archive: %w", err)
	}
	dest := f.Name()
	_ = f.Close()
	if err := os.Rename(path, dest); err != nil {
		_ = os.Remove(dest)
		return fmt.Errorf("node: retire key file: %w", err)
	}
	n.log.Warn("key_file_retired", "from", path, "to", dest,
		"hint", "the node cannot start again until identity.key_file points at a live key")
	return nil
}

// ShareRebinds pushes held handover statements directly to a peer.
func (n *Node) ShareRebinds(ctx context.Context, pid peer.ID) int {
	sent := 0
	for _, k := range n.Rebinds.Statements() {
		resp, err := n.Service.RPC(ctx, pid, &pb.RpcRequest{Kind: &pb.RpcRequest_Rebind{
			Rebind: &pb.RebindRequest{Rebind: k},
		}})
		if err != nil {
			n.log.Debug("rebind_share_failed", "peer", pid.String(), "err", err.Error())
			return sent
		}
		if r := resp.GetRebind(); r != nil && r.GetAccepted() {
			sent++
		}
	}
	return sent
}

// acceptRebind applies one verified identity-handover statement.
func (n *Node) acceptRebind(k *pb.KeyRebind) (bool, string) {
	old, err := peer.Decode(k.GetOldPeerId())
	if err != nil {
		return false, "unparsable old peer id"
	}
	if old == n.ID() {
		if k.GetNewPeerId() == "" {
			if _, err := n.Rebinds.Apply(k); err != nil {
				n.log.Warn("self_revocation_ledger", "err", err.Error())
			}
			n.log.Error("own_identity_revoked_stopping", "peer", old.String(), "reason", k.GetReason())
			n.Audit.Log(security.AuditEvent{Event: "self_revoked_learned", PeerID: old.String(),
				Reason: k.GetReason()})
			if err := n.retireKeyFile(); err != nil {
				n.log.Error("revocation_key_retirement_failed", "err", err.Error())
			}
			n.haltedOnce.Do(func() { close(n.halted) })
		}
		return true, ""
	}
	if n.Rebinds.Len() >= rebindMaxHeld && !n.Rebinds.HoldsStatement(old) {
		n.log.Warn("rebind_ledger_full", "old", old.String())
		return false, "ledger full"
	}
	applied, err := n.Rebinds.Apply(k)
	if err != nil {
		n.Audit.Log(security.AuditEvent{Event: "rebind_rejected", PeerID: old.String(),
			Reason: err.Error()})
		n.Metrics.Security("rebind_rejected")
		return false, err.Error()
	}
	if !applied {
		return false, "older sequence already held"
	}
	n.Metrics.RebindsApplied.Inc()
	if k.GetNewPeerId() == "" {
		n.revokePeer(old)
		n.log.Info("peer_revoked", "peer", old.String(), "reason", k.GetReason())
		return true, ""
	}
	n.rotatePeer(old, k)
	return true, ""
}

func (n *Node) rotatePeer(old peer.ID, k *pb.KeyRebind) {
	nid, err := peer.Decode(k.GetNewPeerId())
	if err != nil {
		return
	}
	if prev, ok := n.Table.Get(old); ok {
		prev.PeerID = nid
		prev.Connected = n.Host.IsConnected(nid)
		prev.LastSeen = time.Now().UTC()
		n.Table.Upsert(&prev)
		n.Table.Remove(old)
	}
	if docs := n.Skills.PeerSkills(old.String()); len(docs) > 0 {
		if updated, names := n.Skills.ImportPeer(nid.String(), descriptorsToProto(docs)); updated > 0 {
			if nb, ok := n.Table.Get(nid); ok {
				n.Table.Upsert(&routing.Neighbor{PeerID: nid, Skills: names, Connected: nb.Connected})
			}
		}
		n.Skills.DropPeer(old.String())
	}
	n.Policy.Observe(nid, n.Policy.TrustOf(old))
	if n.Bootstrap != nil && n.Bootstrap.Rename(old, nid) {
		n.log.Info("bootstrap_entry_renamed", "old", old.String(), "new", nid.String(),
			"hint", "update discovery.bootstrap in the config file when convenient")
	}
	n.Audit.Log(security.AuditEvent{Event: "peer_rebound", PeerID: old.String(),
		Reason: fmt.Sprintf("-> %s (%s)", nid.String(), k.GetReason())})
	n.Metrics.Security("peer_rebound")
	n.log.Info("peer_rebound", "old", old.String(), "new", nid.String(), "reason", k.GetReason())
}

func (n *Node) revokePeer(old peer.ID) {
	n.Manager.OnPeerDisconnected(old)
	n.Table.Remove(old)
	n.Skills.DropPeer(old.String())
	n.Policy.SetListed(old, false)
	n.Audit.Log(security.AuditEvent{Event: "peer_revoked", PeerID: old.String(), Reason: "revoked by own key"})
	n.Metrics.Security("peer_revoked")
}

func descriptorsToProto(in []skills.Descriptor) []*pb.SkillDescriptor {
	out := make([]*pb.SkillDescriptor, 0, len(in))
	for _, d := range in {
		out = append(out, d.ToProto())
	}
	return out
}

func (n *Node) PublishRebind(k *pb.KeyRebind) error {
	applied, err := n.Rebinds.Apply(k)
	if err != nil {
		return err
	}
	if applied {
		n.Metrics.RebindsApplied.Inc()
	}
	if n.Membership != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if st := n.peerState(); st != nil {
			if err := n.Membership.PublishState(ctx, st); err != nil {
				n.log.Debug("rebind_announce_failed", "err", err.Error())
			}
		}
	}
	return nil
}
