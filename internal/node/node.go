// Package node assembles every mesh component into one running node and owns
// its lifecycle: start, maintenance, graceful stop.
package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"

	pubsub "github.com/libp2p/go-libp2p-pubsub"

	"github.com/developer3000S/zeptoclaw/internal/config"
	"github.com/developer3000S/zeptoclaw/internal/discovery"
	"github.com/developer3000S/zeptoclaw/internal/logging"
	"github.com/developer3000S/zeptoclaw/internal/metrics"
	"github.com/developer3000S/zeptoclaw/internal/p2p"
	"github.com/developer3000S/zeptoclaw/internal/picoclaw"
	"github.com/developer3000S/zeptoclaw/internal/routing"
	"github.com/developer3000S/zeptoclaw/internal/security"
	"github.com/developer3000S/zeptoclaw/internal/skills"
	"github.com/developer3000S/zeptoclaw/internal/storage"
	"github.com/developer3000S/zeptoclaw/internal/tasks"
	"github.com/developer3000S/zeptoclaw/internal/triggers"
	"github.com/developer3000S/zeptoclaw/internal/version"

	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
)

// SubmitRequest is re-exported so callers outside the tasks package (the admin
// API, the CLI) do not need to import it.
type SubmitRequest = tasks.SubmitRequest

// buildDiscovery prepares the discovery layer. skipDiscovery keeps the node
// fully manual: only peers dialed by the operator (or the tests) are ever
// reachable, which is what a deterministic topology needs. A disabled gossip
// section alone still permits bootstrap/local-registry/DHT discovery.
func (n *Node) buildDiscovery() error {
	cfg := n.Cfg
	// Every constructor below gets this logger: the discovery mechanisms are six
	// separate objects, and an operator reading "peer found" wants to know it came
	// from the registry rather than gossip (ТЗ 14.1).
	dlog := logging.Component(n.log, "discovery")
	wantTopic := cfg.Tasks.Forwarding.SearchRelay.Topic.Enabled
	if n.skipDiscovery {
		n.log.Info("discovery_disabled_manual_peers_only")
		// The search plane is still allowed on a manual topology: pubsub flows
		// only over the connections that exist, so joining a topic adds signed
		// skill claims between dialed peers, not new reachability.
		if wantTopic {
			return n.buildSearchTopic()
		}
		return nil
	}
	if cfg.Discovery.LocalRegistry {
		rec := discovery.LocalRecord{
			NodeName: cfg.Node.Name,
			Addrs:    n.DiscoverableAddrs(),
			Skills:   cfg.EffectiveSkills(),
		}
		reg, err := discovery.NewLocalRegistry(cfg.Discovery.LocalSocketDir, n.Identity.PeerID(), rec,
			cfg.Discovery.Gossip.FailureTimeout.D()*2, dlog)
		if err != nil {
			n.log.Warn("local_registry_disabled", "err", err.Error())
		} else {
			n.Registry = reg
			probe, perr := discovery.NewProbe(cfg.Discovery.LocalSocketDir, n.Identity.PeerID().String(), dlog)
			if perr != nil {
				n.log.Warn("probe_socket_disabled", "err", perr.Error())
			} else {
				n.Probe = probe
				probe.SetStatus(func() any { return n.Status() })
				reg.SetSocket(probe.Path())
			}
		}
	}

	b, err := discovery.NewBootstrap(n.Host.Underlying(), cfg.Discovery.Bootstrap,
		cfg.Discovery.BootstrapInterval.D(), 10*time.Second, dlog)
	if err != nil {
		return fmt.Errorf("node: bootstrap: %w", err)
	}
	n.Bootstrap = b

	if cfg.Discovery.DHT && n.Host.DHT() != nil {
		n.DHT = discovery.NewDHT(n.Host.DHT(), cfg.EffectiveSkills(), dlog)
	}

	// One pubsub router per host: a second GossipSub instance would re-register
	// the same stream protocol id and silently orphan the first router's
	// subscriptions, so membership and the search topic must share it. Built
	// lazily — only when at least one topic plane is enabled.
	if cfg.Discovery.Gossip.Enabled || wantTopic {
		ps, err := discovery.NewPubSub(context.Background(), n.Host.Underlying(), n.Policy, n.Audit, dlog)
		if err != nil {
			return fmt.Errorf("node: pubsub: %w", err)
		}
		n.pubsub = ps
	}

	if cfg.Discovery.Gossip.Enabled {
		m, err := discovery.NewMembership(context.Background(), n.Host.Underlying(), n.pubsub,
			cfg.Discovery.Gossip, n.Policy, n.Audit, dlog)
		if err != nil {
			return fmt.Errorf("node: membership: %w", err)
		}
		n.Membership = m
		m.SetSelfStateFunc(n.peerState)
		m.SetOnPeer(n.onGossipPeer)
		m.SetOnExpire(n.onPeerExpire)
		m.SetRebindSource(n.rebindsToRepublish, func(k *pb.KeyRebind) { n.acceptRebind(k) })
	} else {
		// Gossip is the only mechanism that learns peers it was not told about,
		// so disabling it makes the mesh strictly explicit: reachability comes
		// from bootstrap entries and manual dials. Everything else — signed
		// capabilities, task routing, result relay — keeps working.
		n.log.Info("gossip_disabled", "hint", "peers are reachable only via discovery.bootstrap or manual dialing")
	}

	if wantTopic {
		return n.buildSearchTopic()
	}
	return nil
}

// buildSearchTopic joins the epidemic skill-search plane (ТЗ 6.9.5 п.5). The
// shared pubsub router is created on demand when it does not already exist: a
// node that runs the topic without gossip still needs one, and a node that
// runs both must not build two routers on the same host (the second would
// silently clobber the first). Membership passes its router in from
// buildDiscovery, which creates it before calling here.
func (n *Node) buildSearchTopic() error {
	if n.pubsub == nil {
		ps, err := discovery.NewPubSub(context.Background(), n.Host.Underlying(), n.Policy, n.Audit,
			logging.Component(n.log, "discovery"))
		if err != nil {
			return fmt.Errorf("node: pubsub: %w", err)
		}
		n.pubsub = ps
	}
	st, err := discovery.NewSearchTopic(discovery.SearchTopicParams{
		Host:      n.Host.Underlying(),
		PubSub:    n.pubsub,
		Config:    n.Cfg.Tasks.Forwarding.SearchRelay.Topic,
		Signer:    security.NewSigner(n.Identity),
		KeyLookup: n.keyLookup(),
		Policy:    n.Policy,
		Audit:     n.Audit,
		View:      n.searchTopicView,
		Logger:    logging.Component(n.log, "search"),
		OnEvent:   n.observeSearchTopic,
	})
	if err != nil {
		return fmt.Errorf("node: search topic: %w", err)
	}
	n.SearchTopic = st
	return nil
}

// Start runs every component. ctx governs the whole node lifetime: cancelling
// it stops background work, and Stop then tears the node down.
func (n *Node) Start(ctx context.Context) error {
	n.mu.Lock()
	if n.running {
		n.mu.Unlock()
		return errors.New("node: already started")
	}
	n.running = true
	n.started = time.Now().UTC()
	runCtx, cancel := context.WithCancel(ctx)
	n.cancel = cancel
	n.mu.Unlock()

	n.Manager.Start(runCtx)
	// Started after the manager so the first tick can submit; it sleeps until
	// the next minute boundary anyway.
	n.Scheduler.Start(runCtx)

	if n.Registry != nil {
		if err := n.Registry.Start(runCtx); err != nil {
			n.log.Warn("local_registry_start_failed", "err", err.Error())
			_ = n.Registry.Stop()
			n.Registry = nil
		}
	}
	if n.Probe != nil {
		n.wg.Add(1)
		go func() { defer n.wg.Done(); n.Probe.Serve() }()
	}

	if n.Cfg.Discovery.MDNS && !n.skipDiscovery {
		m, err := discovery.NewMDNS(n.Host.Underlying(), n.Cfg.Discovery.MDNSServiceName, n.log)
		switch {
		case err != nil:
			n.log.Warn("mdns_init_failed", "err", err.Error())
		case m.Start(func(ai peer.AddrInfo) { n.onDiscovered(ai, routing.CatLAN, "mdns") }) != nil:
			n.log.Warn("mdns_start_failed", "err", m.Start(nil).Error())
			_ = m.Close()
		default:
			n.MDNS = m
		}
	}

	boot, err := n.bootstrapAddrInfos()
	if err != nil {
		return err
	}
	if n.DHT != nil {
		if err := n.DHT.Start(runCtx, boot); err != nil {
			n.log.Warn("dht_start_failed", "err", err.Error())
		}
	}
	if n.Membership != nil {
		if err := n.Membership.Start(runCtx); err != nil {
			return fmt.Errorf("node: membership start: %w", err)
		}
	}
	if n.SearchTopic != nil {
		if err := n.SearchTopic.Start(runCtx); err != nil {
			return fmt.Errorf("node: search topic start: %w", err)
		}
	}

	if n.Bootstrap != nil {
		n.wg.Add(1)
		go func() { defer n.wg.Done(); n.Bootstrap.Run(runCtx, n.needMorePeers) }()
	}
	n.wg.Add(1)
	go func() { defer n.wg.Done(); n.maintenance(runCtx) }()

	n.log.Info("node_started",
		"node_name", n.Cfg.Node.Name,
		"addrs", n.DiscoverableAddrs(),
		"skills", n.Cfg.EffectiveSkills(),
		"adapter", n.Adapter.Name(),
		"version", version.Version,
	)
	return nil
}

// Stop announces departure while the transport still works, then tears every
// component down in reverse order.
func (n *Node) Stop(ctx context.Context) error {
	n.mu.Lock()
	if !n.running {
		n.mu.Unlock()
		return nil
	}
	n.running = false
	cancel := n.cancel
	n.cancel = nil
	n.mu.Unlock()

	var errs []error
	if n.Membership != nil {
		if st := n.peerState(); st != nil {
			st.Status = "left"
			pctx, pcancel := context.WithTimeout(context.Background(), 3*time.Second)
			if err := n.Membership.PublishState(pctx, st); err != nil {
				errs = append(errs, fmt.Errorf("departure announcement: %w", err))
			}
			pcancel()
		}
	}

	if n.Probe != nil {
		errs = append(errs, n.Probe.Close())
	}
	if n.Registry != nil {
		errs = append(errs, n.Registry.Stop())
	}
	if n.MDNS != nil {
		errs = append(errs, n.MDNS.Close())
	}
	if n.Membership != nil {
		errs = append(errs, n.Membership.Close())
	}
	if n.SearchTopic != nil {
		errs = append(errs, n.SearchTopic.Close())
	}
	if n.DHT != nil {
		errs = append(errs, n.DHT.Close())
	}
	if cancel != nil {
		cancel()
	}
	done := make(chan struct{})
	go func() { n.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		errs = append(errs, errors.New("node: background work still running"))
	}
	// Stopped before the manager: a trigger that fires during shutdown would
	// otherwise submit into a pipeline that is already draining.
	if n.Scheduler != nil {
		sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := n.Scheduler.Stop(sctx); err != nil {
			errs = append(errs, err)
		}
		scancel()
	}
	n.Manager.Stop()
	errs = append(errs, n.Adapter.Close(), n.Host.Close(), n.Store.Close(), n.Audit.Close())

	n.log.Info("node_stopped", "uptime", time.Since(n.started).Round(time.Second).String())
	return errors.Join(errs...)
}

// ID exposes the peer id.
func (n *Node) ID() peer.ID { return n.Identity.PeerID() }

// rebindMaxHeld bounds the local handover ledger so a hostile gossip source
// cannot grow our state by flooding statements — each must verify, but the set
// of distinct old ids is unbounded in principle.
const rebindMaxHeld = 4096

// rebindsToRepublish is the gossip source for handover statements. Only recent
// statements are republished: an older one is still true and still resolved
// locally, but pushing it forever would have every recipient log it as too old
// to adopt, and the propagation window that matters is the live one.
func (n *Node) rebindsToRepublish() []*pb.KeyRebind {
	return n.Rebinds.StatementsFresh(rebindRepublishWindow)
}

// rebindRepublishWindow bounds how long a statement keeps being piggybacked on
// heartbeats. It is a transport horizon, not a validity rule.
const rebindRepublishWindow = 12 * time.Hour

// acceptRebind applies one verified identity-handover statement and reports
// whether it took effect, plus why not. It returns a status rather than just
// logging because the same entry point serves gossip (fire-and-forget) and the
// control RPC, whose peer legitimately asks "did you accept this?".
//
// A rotation carries the retiring id's standing over to the successor: the
// neighbour-table entry (metrics, skills, category) moves so a planned key
// change does not reset the relationship the node earned, and the trust policy
// resolves the old id through the ledger on every decision. A revocation
// removes the peer from the view and drops its learned skills — an id retired
// by its own key must stop being trusted everywhere without anyone editing
// files (ТЗ 11.2 п.4).
func (n *Node) acceptRebind(k *pb.KeyRebind) (bool, string) {
	old, err := peer.Decode(k.GetOldPeerId())
	if err != nil {
		return false, "unparsable old peer id"
	}
	if old == n.ID() {
		// Our own handover statement echoed back from a peer: nothing to adopt.
		// The exception worth acting on is a revocation of this very id that we
		// did not issue ourselves — a retired key being reused (the stolen-key
		// case). The mesh already gates that identity everywhere, so the honest
		// outcome is to stop this process rather than let it run as a zombie
		// whose local API still looks healthy. It cannot be forged: only this
		// node's own key signs its revocation, and the key in hand just did.
		if k.GetNewPeerId() == "" {
			// Recorded locally so the restart guard covers this identity even if
			// the key file is put back in place by hand.
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
		return false, "older sequence already held" // stale replay
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

// rotatePeer migrates state from the retired id to its successor and announces
// the successor under this node's own observation, so the handover spreads.
func (n *Node) rotatePeer(old peer.ID, k *pb.KeyRebind) {
	nid, err := peer.Decode(k.GetNewPeerId())
	if err != nil {
		return
	}
	// Inherit the neighbour record wholesale — scores, skills, capacity — then
	// keep the connection truth: the new id has not been dialled yet.
	if prev, ok := n.Table.Get(old); ok {
		prev.PeerID = nid
		prev.Connected = n.Host.IsConnected(nid)
		prev.LastSeen = time.Now().UTC()
		n.Table.Upsert(&prev)
		// The retired identity no longer exists as far as routing is concerned;
		// leaving it behind would let Select keep preferring a dead id.
		n.Table.Remove(old)
	}
	// Carry the learned skill view across the rotation and credit the successor
	// with the predecessor's trust: the same operator, a new key.
	if docs := n.Skills.PeerSkills(old.String()); len(docs) > 0 {
		if updated, names := n.Skills.ImportPeer(nid.String(), descriptorsToProto(docs)); updated > 0 {
			if nb, ok := n.Table.Get(nid); ok {
				n.Table.Upsert(&routing.Neighbor{PeerID: nid, Skills: names, Connected: nb.Connected})
			}
		}
		n.Skills.DropPeer(old.String())
	}
	n.Policy.Observe(nid, n.Policy.TrustOf(old))
	// A bootstrap entry is addressed by identity ("/…/p2p/<old>"), so after the
	// handover it would dial an address whose peer fails the Noise handshake.
	// The address is still correct — only the id part went stale.
	if n.Bootstrap != nil && n.Bootstrap.Rename(old, nid) {
		n.log.Info("bootstrap_entry_renamed", "old", old.String(), "new", nid.String(),
			"hint", "update discovery.bootstrap in the config file when convenient")
	}
	n.Audit.Log(security.AuditEvent{Event: "peer_rebound", PeerID: old.String(),
		Reason: fmt.Sprintf("-> %s (%s)", nid.String(), k.GetReason())})
	n.Metrics.Security("peer_rebound")
	n.log.Info("peer_rebound", "old", old.String(), "new", nid.String(), "reason", k.GetReason())
}

// revokePeer removes a retired identity from every local view.
func (n *Node) revokePeer(old peer.ID) {
	// Tasks that were waiting on this peer must not linger until their timeout:
	// the identity is gone by declaration, which is stronger information than a
	// dropped connection.
	n.Manager.OnPeerDisconnected(old)
	n.Table.Remove(old)
	n.Skills.DropPeer(old.String())
	n.Policy.SetListed(old, false) // operator-grade block, survives restarts in-memory
	n.Audit.Log(security.AuditEvent{Event: "peer_revoked", PeerID: old.String(), Reason: "revoked by own key"})
	n.Metrics.Security("peer_revoked")
}

// descriptorsToProto re-renders learned descriptors for an import under a new
// peer key (import re-verifies digests, so this is a format conversion only).
func descriptorsToProto(in []skills.Descriptor) []*pb.SkillDescriptor {
	out := make([]*pb.SkillDescriptor, 0, len(in))
	for _, d := range in {
		out = append(out, d.ToProto())
	}
	return out
}

// PublishRebind records a locally produced handover statement and announces it
// immediately on every channel, so the mesh learns of a rotation before the
// next heartbeat would have carried it.
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
		// Publishing our own state piggybacks the whole held ledger (the
		// rebindSource callback), which is also how the statement propagates.
		if st := n.peerState(); st != nil {
			if err := n.Membership.PublishState(ctx, st); err != nil {
				n.log.Debug("rebind_announce_failed", "err", err.Error())
			}
		}
	}
	return nil
}

// RotateKeyResult describes a completed identity rotation.
type RotateKeyResult struct {
	OldPeerID peer.ID       `json:"old_peer_id"`
	NewPeerID peer.ID       `json:"new_peer_id"`
	Rebind    *pb.KeyRebind `json:"-"`
	// AnnouncedTo lists the peers the statement was pushed to directly (the
	// gossip carrier is best-effort, so the operator wants to know who heard it
	// synchronously).
	AnnouncedTo []string `json:"announced_to,omitempty"`
	Restart     bool     `json:"restart_required"`
}

// RotateKey replaces this node's identity key with a freshly generated one and
// announces the handover to the mesh (ТЗ 11.2 п.3).
//
// The rotation is self-certifying: the statement is signed by the old key and
// the new key over the same canonical body, so a peer that never met the new
// key can still accept it, and no operator has to edit an allow list on every
// node. Ordering matters for exactly that reason — the handover must be spread
// while the old key is still the live one. If the process died after writing
// the new key but before the announcement, peers would see a stranger rather
// than the successor of a node they trusted. So: build and verify the
// statement, publish it (ledger + gossip + direct RPC), and only then install
// the new key file. The caller restarts the process; the config file still
// points at the same path, so no YAML edit is needed.
//
// In-flight work is the operator's decision, not this function's: tasks this
// node executes finish under the old identity (the signed result is already
// bound to it), and originators see the new id on subsequent hops.
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

	// Announce first, from the old identity — see the ordering note above.
	res := &RotateKeyResult{OldPeerID: old.PeerID(), NewPeerID: fresh.PeerID(), Rebind: k, Restart: true}
	if err := n.PublishRebind(k); err != nil {
		return nil, fmt.Errorf("node: record rebind statement: %w", err)
	}
	res.AnnouncedTo = n.pushRebind(ctx, k)

	// Now move the identity on disk. Our own ledger already holds the
	// statement, so after the restart the node recognises its predecessor and
	// keeps the trust it had earned rather than relearning from zero.
	if err := fresh.Save(n.Cfg.Identity.KeyFile); err != nil {
		return nil, fmt.Errorf("node: install new key: %w", err)
	}
	n.log.Info("identity_rotated", "old", res.OldPeerID.String(), "new", res.NewPeerID.String(),
		"reason", reason, "announced", len(res.AnnouncedTo))
	return res, nil
}

// pushRebind sends the statement directly to every connected peer. Gossip
// carries it epidemically, but a direct push guarantees the neighbours that
// already route work here — the ones whose in-flight delegation would otherwise
// break — hear about the handover immediately, and it is the only channel in a
// mesh running with gossip disabled.
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

// RevokeSelf retires this node's identity for good (ТЗ 11.2 п.4): it signs the
// revocation with the key being retired — the only authority that has — spreads
// it, and then asks the process to stop and stay stopped.
//
// Unlike a rotation this is not a handover, so there is no successor to keep
// routing to: the point is that every other node starts refusing this
// identifier, which is what an operator needs after a key leak. The ledger on
// this node keeps the statement too, so the retired id cannot be revived by a
// restart with the same key file.
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
	// A revocation that the supervisor can undo is not a revocation. Restart
	// policies ignore exit codes (`restart: unless-stopped` in particular), so
	// signalling "stay down" is not enough — the key material has to leave the
	// path the config points at. It is moved aside rather than destroyed: after
	// a leak the private key is still evidence, and the operator may legitimately
	// un-revoke a node they retired by mistake. The suffix is random because the
	// destination sits in a directory the node user can write, and a predictable
	// name there is a file-clobber primitive.
	if err := n.retireKeyFile(); err != nil {
		n.log.Error("revocation_key_retirement_failed", "err", err.Error(),
			"hint", "remove "+n.Cfg.Identity.KeyFile+" manually before the next start")
	}
	n.haltedOnce.Do(func() { close(n.halted) })
	return nil
}

// retireKeyFile moves the retired node's key file out of the way and reports the
// new location.
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
	// CreateTemp picks a unique name (O_EXCL, unpredictable), so the destination
	// cannot be raced or pre-planted by another writer in the key directory.
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

// ---------- connection bookkeeping ----------

// installConnNotifier keeps Connected flags honest and absorbs addresses
// learned during connection setup.
func (n *Node) installConnNotifier() {
	bundle := &network.NotifyBundle{
		ConnectedF: func(_ network.Network, conn network.Conn) {
			pid := conn.RemotePeer()
			// A socket opening is not a judgement: the table holds no trust, and the
			// peer's standing comes from the policy when something asks for it.
			n.Table.Upsert(&routing.Neighbor{
				PeerID: pid, Addrs: []string{conn.RemoteMultiaddr().String()},
				Connected: true, Category: n.classify(conn),
			})
			n.Metrics.PeersConnected.Set(float64(n.Table.ConnectedCount()))
			n.Metrics.PeersTotal.Set(float64(n.Table.Len()))
			go n.introduce(pid)
		},
		DisconnectedF: func(_ network.Network, conn network.Conn) {
			n.Table.SetConnected(conn.RemotePeer(), false)
			n.Metrics.PeersConnected.Set(float64(n.Table.ConnectedCount()))
			if n.Manager != nil {
				n.Manager.OnPeerDisconnected(conn.RemotePeer())
			}
		},
	}
	n.notifee = bundle
	n.Host.Underlying().Network().Notify(bundle)
}

// introduce completes the capability handshake for a peer whose connection we
// did not dial ourselves. Without it a passively-connected node stays blind:
// it has no signed skill list and, under the limited posture, no trust
// elevation for the peer — so it can never delegate work back the way the
// task arrived. One fetch per peer is enough; gossip keeps it refreshed.
//
// "Enough" is measured per process, not per neighbour table entry: the table is
// persisted and the trust a handshake earns is not. Skipping the handshake
// because a *restored* record already lists skills left the policy with nothing
// observed for that peer, so every neighbour's tasks were refused as
// "peer not trusted for tasks" after a restart while the peer list still read
// "trusted" — the two views disagreed and only the wrong one was visible.
func (n *Node) introduce(pid peer.ID) {
	if pid == "" || pid == n.Identity.PeerID() {
		return
	}
	n.introduceMu.Lock()
	if n.handshaked[pid] || n.introducing[pid] {
		n.introduceMu.Unlock()
		return
	}
	if n.introducing == nil {
		n.introducing = make(map[peer.ID]bool)
	}
	n.introducing[pid] = true
	n.introduceMu.Unlock()
	defer func() {
		n.introduceMu.Lock()
		delete(n.introducing, pid)
		n.introduceMu.Unlock()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	n.refreshCapabilities(ctx, pid)

	// Handover statements ride along once the peer is known. Gossip carries them
	// too, but a mesh with discovery.gossip.enabled=false has no epidemic
	// channel at all: without this a node that joins after a rotation would keep
	// treating the successor as a stranger (and, worse, would happily accept a
	// key the rest of the network retired more than a republication window ago).
	if n.completedHandshake(pid) {
		sctx, scancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer scancel()
		if sent := n.ShareRebinds(sctx, pid); sent > 0 {
			n.log.Debug("rebinds_shared", "peer", pid.String(), "statements", sent)
		}
	}
}

// completedHandshake reports whether this process has verified a peer's
// capabilities (the condition that also earns it observed trust).
func (n *Node) completedHandshake(pid peer.ID) bool {
	n.introduceMu.Lock()
	defer n.introduceMu.Unlock()
	return n.handshaked[pid]
}

// classify labels a connection's locality: same host, same LAN, or WAN.
func (n *Node) classify(conn network.Conn) string {
	r4 := firstStringComponent(conn.RemoteMultiaddr(), ma.P_IP4)
	if r4 == "" {
		return routing.CatWAN
	}
	if r4 == "127.0.0.1" {
		return routing.CatLocal
	}
	var l4 string
	for _, a := range n.Host.Underlying().Addrs() {
		if v := firstStringComponent(a, ma.P_IP4); v != "" && v != "0.0.0.0" {
			l4 = v
			break
		}
	}
	if l4 != "" && samePrivateSubnet(l4, r4) {
		return routing.CatLAN
	}
	return routing.CatWAN
}

func firstStringComponent(m ma.Multiaddr, code int) string {
	if m == nil {
		return ""
	}
	var out string
	ma.ForEach(m, func(c ma.Component) bool {
		if c.Protocol().Code == code {
			out = c.Value()
			return false
		}
		return true
	})
	return out
}

// samePrivateSubnet compares /24 prefixes for the common RFC1918 case. It is
// a heuristic for the routing category label, not an access-control decision.
func samePrivateSubnet(a, b string) bool {
	if a == b {
		return true
	}
	if !privateIPv4(a) || !privateIPv4(b) {
		return false
	}
	trim := func(s string) string {
		for i, seen := 0, 0; i < len(s); i++ {
			if s[i] == '.' {
				seen++
				if seen == 3 {
					return s[:i]
				}
			}
		}
		return s
	}
	return trim(a) == trim(b)
}

func privateIPv4(ip string) bool {
	switch {
	case strings.HasPrefix(ip, "10."), strings.HasPrefix(ip, "192.168."):
		return true
	case strings.HasPrefix(ip, "172.16."), strings.HasPrefix(ip, "172.17."),
		strings.HasPrefix(ip, "172.18."), strings.HasPrefix(ip, "172.19."),
		strings.HasPrefix(ip, "172.2"), strings.HasPrefix(ip, "172.30."),
		strings.HasPrefix(ip, "172.31."):
		return true
	default:
		return false
	}
}

// ---------- maintenance ----------

// maintenance is the node's slow loop: local-registry sync, full gossip sync,
// peer exchange when thin, stale pruning and metric refresh.
func (n *Node) maintenance(ctx context.Context) {
	beat := n.Cfg.Discovery.Gossip.Heartbeat.D()
	t := time.NewTicker(beat)
	full := time.NewTicker(n.Cfg.Discovery.Gossip.FullSync.D())
	local := time.NewTicker(5 * time.Second)
	rtt := time.NewTicker(15 * time.Second)
	skillEvery := n.Cfg.Capabilities.SkillExchange.Interval.D()
	if skillEvery <= 0 {
		skillEvery = 30 * time.Second
	}
	skillTick := time.NewTicker(skillEvery)
	defer t.Stop()
	defer full.Stop()
	defer local.Stop()
	defer rtt.Stop()
	defer skillTick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-local.C:
			n.syncLocalRegistry(ctx)
			n.syncDHT(ctx)
		case <-rtt.C:
			n.measureRTT(ctx)
		case <-skillTick.C:
			n.reconcileSkills(ctx)
			n.Metrics.SkillsVersion.Set(float64(n.Skills.Epoch()))
		case <-t.C:
			if n.Table.ConnectedCount() < n.Cfg.Neighbors.Min && n.Cfg.Discovery.PeerExchange {
				n.requestPeerExchange(ctx)
			}
			n.reapSuspects(ctx)
			n.Metrics.PeersTotal.Set(float64(n.Table.Len()))
			n.Metrics.PeersConnected.Set(float64(n.Table.ConnectedCount()))
		case <-full.C:
			if n.Membership == nil {
				continue
			}
			if err := n.Membership.PublishFull(ctx); err != nil {
				n.log.Debug("gossip_full_sync", "err", err.Error())
			}
		}
	}
}

// syncLocalRegistry dials co-located nodes announced in the registry.
func (n *Node) syncLocalRegistry(ctx context.Context) {
	if n.Registry == nil {
		return
	}
	recs, err := n.Registry.List()
	if err != nil {
		n.log.Debug("local_registry_list_failed", "err", err.Error())
		return
	}
	for _, r := range recs {
		if n.Table.ConnectedCount() >= n.Cfg.Neighbors.Max {
			return
		}
		pid, err := peer.Decode(r.PeerID)
		if err != nil || pid == n.Identity.PeerID() {
			continue
		}
		if nb, ok := n.Table.Get(pid); ok && nb.Connected {
			continue
		}
		ai := peer.AddrInfo{ID: pid}
		for _, a := range r.Addrs {
			if parsed, perr := discovery.ParseAddrInfo(a); perr == nil {
				ai.Addrs = append(ai.Addrs, parsed.Addrs...)
			}
		}
		if len(ai.Addrs) == 0 {
			continue
		}
		n.Table.Upsert(&routing.Neighbor{PeerID: pid, Addrs: addrsOf(ai), Category: routing.CatLocal, Skills: r.Skills})
		n.onDiscovered(ai, routing.CatLocal, "local")
		_ = ctx
	}
}

// syncDHT looks up this node's skills in the DHT and dials unseen providers.
func (n *Node) syncDHT(ctx context.Context) {
	if n.DHT == nil || !n.DHT.Enabled() || !n.Cfg.Discovery.DHT {
		return
	}
	if n.Table.ConnectedCount() >= n.Cfg.Neighbors.Target {
		return
	}
	peers, err := n.DHT.FindPeersBySkill(ctx, n.Cfg.EffectiveSkills()[:min(2, len(n.Cfg.EffectiveSkills()))], 20)
	if err != nil {
		n.log.Debug("dht_lookup_failed", "err", err.Error())
		return
	}
	for _, ai := range peers {
		if nb, ok := n.Table.Get(ai.ID); ok && nb.Connected {
			continue
		}
		n.onDiscovered(ai, routing.CatWAN, "dht")
	}
}

// requestPeerExchange asks up to two neighbours for more peers.
func (n *Node) requestPeerExchange(ctx context.Context) {
	for _, nb := range n.Table.Sample(2) {
		if !nb.Connected {
			continue
		}
		pctx, cancel := context.WithTimeout(ctx, 8*time.Second)
		resp, err := n.Service.RPC(pctx, nb.PeerID, &pb.RpcRequest{
			Kind: &pb.RpcRequest_PeerExchange{PeerExchange: &pb.PeerExchangeRequest{Count: 16}},
		})
		cancel()
		if err != nil || resp == nil {
			continue
		}
		for _, rec := range resp.GetPeerExchange().GetPeers() {
			pid, err := peer.Decode(rec.GetPeerId())
			if err != nil || pid == n.Identity.PeerID() || pid == nb.PeerID {
				continue
			}
			if !n.Policy.AllowConnection(pid) {
				continue
			}
			if existing, ok := n.Table.Get(pid); ok && existing.Connected {
				continue
			}
			ai := peer.AddrInfo{ID: pid}
			for _, a := range rec.GetAddrs() {
				if parsed, perr := discovery.ParseAddrInfo(a); perr == nil {
					ai.Addrs = append(ai.Addrs, parsed.Addrs...)
				}
			}
			if len(ai.Addrs) == 0 {
				continue
			}
			n.Table.Upsert(&routing.Neighbor{PeerID: pid, Addrs: addrsOf(ai), Skills: rec.GetSkills(), Category: routing.CatWAN})
			n.log.Debug("pex_candidate", "peer", pid.String(), "from", nb.PeerID.String())
		}
	}
}

// measureRTT pings a bounded sample of connected neighbours so the routing
// score's latency term (ТЗ 6.9.2) reflects reality: an unmeasured peer keeps
// the neutral 0.5 and a stale average decays only as new samples arrive.
func (n *Node) measureRTT(ctx context.Context) {
	for _, nb := range n.Table.Sample(8) {
		if !nb.Connected {
			continue
		}
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		start := time.Now()
		resp, err := n.Service.RPC(pctx, nb.PeerID, &pb.RpcRequest{
			Kind: &pb.RpcRequest_Ping{Ping: &pb.PingRequest{Nonce: time.Now().UnixNano()}},
		})
		elapsed := time.Since(start)
		cancel()
		// A failed ping is not a task-execution failure: it must not drain the
		// peer's success-rate term, it just means we keep the old estimate.
		if err != nil || resp == nil || resp.GetPing() == nil {
			continue
		}
		n.Table.RecordRTT(nb.PeerID, elapsed)
	}
}

// suspectAfter is how long a neighbour may stay silent before this node asks it
// directly. Three heartbeats' worth of lost gossip used to be the point where
// the entry was deleted; now it is only the point where confirmation is asked.
func (n *Node) suspectAfter() time.Duration {
	return n.Cfg.Discovery.Gossip.FailureTimeout.D() * 3
}

// reapSuspects evicts silent neighbours, but only after asking them directly
// (ТЗ 6.5.3). Silence is weak evidence: gossip is best-effort, and a peer that
// is alive but quiet — a delayed pubsub batch, a dropped message, a busy
// inbound queue on our side — used to be dropped from the routing view, which
// reached the operator as "no eligible peers reachable" for tasks that had a
// perfectly good executor. A live libp2p connection makes the question cheap, so
// each suspect is pinged over it, and only those that answer nothing are removed.
//
// A probe is not proof of capacity: answering a ping costs the peer almost
// nothing. That is deliberate — the mesh protocol has no "are you free to work"
// message, and inventing one is out of scope here. The probe answers "is this
// identity reachable at all", which is the question eviction actually asks.
func (n *Node) reapSuspects(ctx context.Context) {
	suspects := n.Table.Suspects(n.suspectAfter())
	// Bound the pass: each probe may wait out its timeout, and the maintenance
	// loop also carries peer exchange and metric refresh. Oldest first, so the
	// remainder is confirmed on the next tick rather than starving the loop.
	if len(suspects) > maxProbesPerPass {
		suspects = suspects[:maxProbesPerPass]
	}
	for _, nb := range suspects {
		if nb.PeerID == n.ID() {
			continue
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
		if n.probePeer(ctx, nb.PeerID) {
			// Alive: refresh the sighting so the next pass sees it as healthy.
			n.Table.Touch(nb.PeerID, n.Host != nil && n.Host.IsConnected(nb.PeerID))
			n.log.Debug("neighbor_reconfirmed", "peer", nb.PeerID.String())
			continue
		}
		if n.Membership != nil {
			n.Membership.MarkLeft(nb.PeerID)
		}
		n.Table.Remove(nb.PeerID)
		n.Skills.DropPeer(nb.PeerID.String())
		n.log.Debug("neighbor_dropped", "peer", nb.PeerID.String(), "reason", "unreachable on probe")
	}
}

// maxProbesPerPass bounds one eviction sweep so a partition cannot stall
// maintenance behind a long row of timeouts.
const maxProbesPerPass = 8

// probePeer asks one neighbour directly whether it is there. It is bounded so a
// partition cannot hang the loop, and silent on purpose: the answer decides
// eviction, and the log line belongs to the caller.
func (n *Node) probePeer(ctx context.Context, pid peer.ID) bool {
	if n.Service == nil {
		// No transport to ask over: keep the entry rather than evict on a guess.
		return true
	}
	pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	resp, err := n.Service.RPC(pctx, pid, &pb.RpcRequest{
		Kind: &pb.RpcRequest_Ping{Ping: &pb.PingRequest{Nonce: time.Now().UnixNano()}},
	})
	return err == nil && resp != nil && resp.GetPing() != nil
}

// ReloadConfig re-reads the configuration file the node started with and
// applies the section that live objects own (ТЗ 13.1.1 reload). The config
// struct itself is left untouched — components that read it without
// synchronisation keep a consistent startup snapshot — while the trust
// policy, the rate limiter and the log level are swapped through their own
// locks. The diff says honestly which sections need a restart.
func (n *Node) ReloadConfig() (config.ReloadDiff, error) {
	if n.configPath == "" {
		return config.ReloadDiff{}, errors.New("node: no config file to reload (started without -config)")
	}
	next, err := config.Load(n.configPath)
	if err != nil {
		return config.ReloadDiff{}, err
	}
	d := n.Cfg.Diff(next)

	mode, err := security.ParseMode(next.Security.TrustMode)
	if err != nil {
		return d, err
	}
	minTrust, err := security.ParseTrust(next.Security.MinTrustForTasks)
	if err != nil {
		return d, err
	}
	n.Policy.SetPosture(mode, minTrust)
	// List files merge in: entries added since startup (or edited in place)
	// take effect; existing entries are never silently dropped.
	if _, err := n.Policy.LoadPeerFile(next.Security.AllowedPeersFile, true); err != nil {
		n.log.Warn("reload_allowed_peers", "err", err.Error())
	}
	if _, err := n.Policy.LoadPeerFile(next.Security.BlockedPeersFile, false); err != nil {
		n.log.Warn("reload_blocked_peers", "err", err.Error())
	}
	if rps := next.Security.RateLimit.RequestsPerSecond; rps > 0 {
		n.Limiter.SetRate(rps, next.Security.RateLimit.Burst)
	}
	if n.levelVar != nil {
		n.levelVar.Set(logging.Level(next.Telemetry.LogLevel))
	}
	// Trigger schedules are hot: the scheduler owns a live copy of the
	// config-declared list. SetConfigTriggers validates the group before
	// swapping, so a rejected reload leaves the previous schedules running.
	if n.Scheduler != nil {
		if err := n.Scheduler.SetConfigTriggers(next.ConfigTriggers()); err != nil {
			return d, fmt.Errorf("triggers not reloaded (previous schedules stay active): %w", err)
		}
	}
	n.log.Info("config_reloaded", "hot", len(d.Hot), "requires_restart", len(d.RequiresRestart))
	if n.Audit != nil {
		n.Audit.Log(security.AuditEvent{Event: "config_reload", Detail: fmt.Sprintf("hot=%v restart=%v", d.Hot, d.RequiresRestart)})
	}
	return d, nil
}

// Leave announces departure and asks the daemon to exit (ТЗ 13.1.1
// /admin/leave). The supervisor (systemd Restart=on-failure + ExitCode=75,
// docker restart policy) starts a fresh process; the mesh learns of the
// departure from the announcement instead of a timeout.
func (n *Node) Leave(ctx context.Context) {
	if n.Membership != nil {
		if st := n.peerState(); st != nil {
			st.Status = "left"
			pctx, pcancel := context.WithTimeout(ctx, 3*time.Second)
			if err := n.Membership.PublishState(pctx, st); err != nil {
				n.log.Warn("leave_announce_failed", "err", err.Error())
			}
			pcancel()
		}
	}
	n.quitOnce.Do(func() { close(n.quit) })
}

// ---------- helpers ----------

func addrsOf(ai peer.AddrInfo) []string {
	out := make([]string, 0, len(ai.Addrs))
	for _, a := range ai.Addrs {
		out = append(out, a.String())
	}
	return out
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ---------- composite skill source ----------

// skillSource is the tasks.SkillSource the manager queries: it folds gossip
// membership, DHT provider records and the neighbour table into one view and
// can reload all of them on demand (a "full refresh").
//
// Why a composite instead of the raw *discovery.Membership: Select() requires an
// exact skill match, so a peer whose capabilities were never fetched scores
// zero and is invisible. When a task needs an executor nobody nearby has, the
// only way out is to widen the view — which means pulling in DHT providers and
// peers learned from peer-exchange, not just gossip.
type skillSource struct {
	n *Node
}

func (n *Node) skillsSource() tasks.SkillSource { return &skillSource{n: n} }

// PeersBySkill returns every known peer that covers want, from any source.
func (s *skillSource) PeersBySkill(want []string) []peer.ID {
	seen := make(map[peer.ID]bool)
	out := make([]peer.ID, 0, 8)
	add := func(pid peer.ID) {
		if pid == "" || pid == s.n.Identity.PeerID() || seen[pid] {
			return
		}
		seen[pid] = true
		out = append(out, pid)
	}
	if s.n.Membership != nil {
		for _, pid := range s.n.Membership.PeersBySkill(want) {
			add(pid)
		}
	}
	// The table also holds peers gossip never mentioned (bootstraps, DHT dials,
	// peer-exchange records) whose skills were confirmed by a capabilities RPC.
	for _, c := range s.n.Table.Select(want, nil, s.n.Identity.PeerID(), 0, nil) {
		add(c.Neighbor.PeerID)
	}
	return out
}

// State resolves a peer's last advertised state across sources.
func (s *skillSource) State(pid peer.ID) (*pb.PeerState, bool) {
	if s.n.Membership != nil {
		if st, ok := s.n.Membership.State(pid); ok {
			return st, true
		}
	}
	if nb, ok := s.n.Table.Get(pid); ok && len(nb.Skills) > 0 {
		return &pb.PeerState{
			PeerId:           pid.String(),
			Timestamp:        nb.LastSeen.Unix(),
			Skills:           append([]string(nil), nb.Skills...),
			Load:             nb.Load,
			MaxParallelTasks: nb.MaxPar,
			Version:          nb.Version,
			Addrs:            append([]string(nil), nb.Addrs...),
		}, true
	}
	return nil, false
}

// Refresh reloads the whole skill view and folds the result into the neighbour
// table, so a subsequent Table.Select can actually pick the newly learned peers.
//
// Order matters: the DHT lookup is the only source that can be re-queried
// synchronously, so it runs first and its completeness decides the return value.
// Gossip is refreshed afterwards (it can only re-broadcast and wait), and peers
// learned from either source are dialed and have their signed capabilities
// fetched — that is what turns "someone claims X has this skill" into "X
// actually advertises this skill", which is the precondition for Select.
func (s *skillSource) Refresh(ctx context.Context, want []string) (int, bool) {
	if len(want) == 0 {
		return 0, false
	}
	rctx, cancel := context.WithTimeout(ctx, s.refreshBudget())
	defer cancel()

	dhtComplete := false
	if s.n.DHT != nil && s.n.DHT.Enabled() {
		providers, ok := s.n.DHT.Providers(rctx, want)
		dhtComplete = ok
		for _, ai := range providers {
			s.n.onDiscovered(ai, routing.CatWAN, "skill_relay")
		}
	}

	gossipWait := time.Duration(0)
	if s.n.Membership != nil {
		gossipWait = s.refreshBudget() / 2
		if dhtComplete {
			gossipWait = s.n.Cfg.Discovery.Gossip.Heartbeat.D()
		}
		s.n.Membership.Refresh(rctx, want, gossipWait)
	}

	// Every candidate the widened view produced must be reachable and must have
	// confirmed its skills, otherwise Select still cannot see it.
	matched := s.PeersBySkill(want)
	for _, pid := range matched {
		if !s.n.Host.IsConnected(pid) {
			if st, ok := s.State(pid); ok {
				ai := peer.AddrInfo{ID: pid}
				for _, a := range st.GetAddrs() {
					if m, err := ma.NewMultiaddr(a); err == nil {
						ai.Addrs = append(ai.Addrs, m)
					}
				}
				if len(ai.Addrs) == 0 {
					continue
				}
				if err := s.n.Host.Connect(rctx, ai); err != nil {
					continue
				}
				s.n.Table.SetConnected(pid, true)
			}
		}
		if nb, ok := s.n.Table.Get(pid); !ok || len(nb.Skills) == 0 {
			s.n.refreshCapabilities(rctx, pid)
		}
	}

	found := len(s.PeersBySkill(want))
	// Never claim completeness on a partial network view: that would let the
	// caller conclude "no executor exists" from a lookup that simply did not
	// reach far enough.
	return found, dhtComplete && s.n.Table.ConnectedCount() >= s.n.Cfg.Neighbors.Min
}

// Adopt folds peer records returned by a search relay into the neighbour table.
//
// A relay answer is a *claim* made by whoever answered the lookup, so it is not
// trusted: each record is dialed and, once connected, its signed capabilities
// are fetched (refreshCapabilities verifies the signature and only then writes
// the skills). A peer we cannot reach contributes nothing. Returns how many
// peers ended up connected with confirmed skills.
func (s *skillSource) Adopt(ctx context.Context, recs []*pb.PeerRecord) int {
	adopted := 0
	for _, r := range recs {
		if r.GetPeerId() == "" || r.GetPeerId() == s.n.Identity.PeerID().String() {
			continue
		}
		pid, err := peer.Decode(r.GetPeerId())
		if err != nil {
			continue
		}
		if !s.n.Policy.AllowConnection(pid) {
			s.n.Audit.Log(security.AuditEvent{
				Event: "search_relay_blocked", PeerID: pid.String(), Reason: "blocked", Detail: "search_relay",
			})
			continue
		}
		ai := peer.AddrInfo{ID: pid}
		for _, a := range r.GetAddrs() {
			if m, err := ma.NewMultiaddr(a); err == nil {
				ai.Addrs = append(ai.Addrs, m)
			}
		}
		// Seed the table with the advertised skills as a *hint* (so we have an
		// address to dial), then confirm them with a signed capabilities RPC.
		if len(ai.Addrs) > 0 {
			s.n.Table.Upsert(&routing.Neighbor{
				PeerID: pid, Addrs: addrsOf(ai), Category: routing.CatWAN,
				Connected: s.n.Host.IsConnected(pid),
			})
		}
		if !s.n.Host.IsConnected(pid) {
			if len(ai.Addrs) == 0 {
				continue
			}
			dctx, cancel := context.WithTimeout(ctx, 6*time.Second)
			derr := s.n.Host.Connect(dctx, ai)
			cancel()
			if derr != nil {
				continue
			}
			s.n.Table.SetConnected(pid, true)
		}
		before := s.tableSkills(pid)
		s.n.refreshCapabilities(ctx, pid)
		after := s.tableSkills(pid)
		if len(after) > 0 && (len(before) == 0 || !sameSkills(before, after)) {
			adopted++
		}
	}
	return adopted
}

// tableSkills reads the confirmed skill set for a peer from the neighbour table.
func (s *skillSource) tableSkills(pid peer.ID) []string {
	if nb, ok := s.n.Table.Get(pid); ok {
		return nb.Skills
	}
	return nil
}

func sameSkills(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]bool, len(a))
	for _, x := range a {
		set[x] = true
	}
	for _, y := range b {
		if !set[y] {
			return false
		}
	}
	return true
}

// refreshBudget bounds one Refresh: it must fit inside the task deadline that
// triggered it and stay below the RPC stream timeout of the peers we fan out to.
func (s *skillSource) refreshBudget() time.Duration {
	budget := s.n.Cfg.Tasks.Forwarding.SearchRelay.RequestTimeout.D()
	if budget <= 0 {
		budget = 8 * time.Second
	}
	if stream := s.n.Service.Timeout(); stream > 0 && budget >= stream {
		budget = stream / 2
	}
	return budget
}

// SearchTopic publishes the skills-only lookup on the epidemic search plane and
// collects candidate records until ctx expires. The records are claims — the
// caller adopts them through Adopt, which dials and verifies each peer's own
// signed capabilities before routing to it.
func (s *skillSource) SearchTopic(ctx context.Context, want []string) []*pb.PeerRecord {
	if s.n.SearchTopic == nil {
		return nil
	}
	return s.n.SearchTopic.Search(ctx, want)
}

// searchTopicView renders what this node is willing to sign an answer with:
// every peer its combined view says covers want, plus itself when it does. The
// records carry addresses so the requester can dial them; a peer known only by
// id is still worth disclosing (the requester resolves routes via the table).
func (n *Node) searchTopicView(want []string) []*pb.PeerRecord {
	if len(want) == 0 {
		return nil
	}
	covers := func(skills []string) bool {
		set := make(map[string]bool, len(skills))
		for _, s := range skills {
			set[s] = true
		}
		for _, w := range want {
			if !set[w] {
				return false
			}
		}
		return true
	}
	self := n.Identity.PeerID().String()
	out := make([]*pb.PeerRecord, 0, 8)
	if covers(n.Cfg.EffectiveSkills()) {
		out = append(out, &pb.PeerRecord{
			PeerId: self, Addrs: n.DiscoverableAddrs(),
			Skills: n.Cfg.EffectiveSkills(), SeenAt: time.Now().UTC().Unix(),
		})
	}
	seen := map[string]bool{self: true}
	if n.Membership != nil {
		for _, st := range n.Membership.Snapshot() {
			if st.GetPeerId() == "" || seen[st.GetPeerId()] {
				continue
			}
			if !covers(st.GetSkills()) {
				continue
			}
			seen[st.GetPeerId()] = true
			out = append(out, &pb.PeerRecord{
				PeerId: st.GetPeerId(), Addrs: st.GetAddrs(),
				Skills: st.GetSkills(), SeenAt: st.GetTimestamp(),
			})
		}
	}
	for _, nb := range n.Table.List() {
		if nb.PeerID.String() == "" || seen[nb.PeerID.String()] || nb.Left {
			continue
		}
		if !covers(nb.Skills) {
			continue
		}
		seen[nb.PeerID.String()] = true
		out = append(out, &pb.PeerRecord{
			PeerId: nb.PeerID.String(), Addrs: nb.Addrs,
			Skills: nb.Skills, SeenAt: nb.LastSeen.Unix(),
		})
	}
	return out
}

// observeSearchTopic maps the topic plane's events onto metrics counters.
func (n *Node) observeSearchTopic(event string) {
	if n.Metrics == nil {
		return
	}
	switch event {
	case "requested":
		n.Metrics.SearchTopicRequests.Inc()
	case "answered":
		n.Metrics.SearchTopicAnswers.Inc()
	case "rejected":
		n.Metrics.SearchTopicRejected.Inc()
	}
}
