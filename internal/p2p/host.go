package p2p

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/pnet"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	quic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	ma "github.com/multiformats/go-multiaddr"
	"google.golang.org/protobuf/proto"

	"github.com/zeptoclaw/zeptomesh/internal/config"
	"github.com/zeptoclaw/zeptomesh/internal/version"
)

// Host wraps a libp2p host with the mesh's protocol wiring.
type Host struct {
	h   host.Host
	dht *dht.IpfsDHT
	cfg *config.Config
	log *slog.Logger

	// rec receives transport byte counters from the framing helpers.
	rec MetricsRecorder
}

// MetricsRecorder is the minimal surface p2p needs from the metrics package;
// it keeps this package free of a Prometheus dependency.
type MetricsRecorder interface {
	AddBytesSent(uint64)
	AddBytesReceived(uint64)
}

// Options carries the collaborators a host needs at construction time.
type Options struct {
	Config   *config.Config
	Key      crypto.PrivKey
	Gater    connmgr.ConnectionGater
	Logger   *slog.Logger
	Handlers map[protocol.ID]network.StreamHandler
	Metrics  MetricsRecorder
}

// New builds the libp2p host: QUIC primary with TCP fallback, Noise security,
// Yamux multiplexing, Ed25519 identity, optional Kademlia DHT and PSK.
func New(ctx context.Context, opts Options) (*Host, error) {
	cfg := opts.Config
	if cfg == nil {
		return nil, fmt.Errorf("p2p: config is required")
	}
	if opts.Key == nil {
		return nil, fmt.Errorf("p2p: identity key is required")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	libopts := []libp2p.Option{
		libp2p.Identity(opts.Key),
		libp2p.ListenAddrStrings(cfg.Node.Listen...),
		// Noise is the security channel for TCP; QUIC carries its own TLS 1.3
		// handshake but still authenticates with the same Ed25519 key.
		libp2p.Security(noise.ID, noise.New),
		libp2p.Muxer(yamux.ID, yamux.DefaultTransport),
		libp2p.Transport(quic.NewTransport),
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.UserAgent("zeptomesh/" + version.Version),
		libp2p.DisableRelay(),
	}
	if len(cfg.Node.Announce) > 0 {
		libopts = append(libopts, libp2p.AddrsFactory(staticAddrsFactory(cfg.Node.Announce)))
	}
	if opts.Gater != nil {
		libopts = append(libopts, libp2p.ConnectionGater(opts.Gater))
	}
	psk, err := decodePSK(cfg.Node.PrivateNetwork)
	if err != nil {
		return nil, fmt.Errorf("p2p: private network: %w", err)
	}
	if psk != nil {
		libopts = append(libopts, libp2p.PrivateNetwork(psk))
		logger.Info("private_network_enabled")
	}

	var dhtInstance *dht.IpfsDHT
	if cfg.Discovery.DHT {
		mode := dht.ModeClient
		switch strings.ToLower(cfg.Discovery.DHTMode) {
		case "server":
			mode = dht.ModeServer
		case "auto", "":
			mode = dht.ModeAuto
		case "auto-server":
			mode = dht.ModeAutoServer
		}
		libopts = append(libopts, libp2p.Routing(func(h host.Host) (routing.PeerRouting, error) {
			inst, err := dht.New(h, dht.Mode(mode))
			if err != nil {
				return nil, fmt.Errorf("p2p: dht: %w", err)
			}
			dhtInstance = inst
			return inst, nil
		}))
	}

	h, err := libp2p.New(libopts...)
	if err != nil {
		return nil, fmt.Errorf("p2p: new host: %w", err)
	}

	mh := &Host{h: h, dht: dhtInstance, cfg: cfg, log: logger, rec: opts.Metrics}
	for pid, handler := range opts.Handlers {
		h.SetStreamHandler(pid, handler)
	}

	logger.Info("host_started",
		"peer_id", h.ID().String(),
		"addrs", addrsToStrings(h.Addrs()),
		"dht", cfg.Discovery.DHT,
	)
	return mh, nil
}

// ID returns this node's peer id.
func (h *Host) ID() peer.ID { return h.h.ID() }

// Underlying exposes the libp2p host to sibling packages.
func (h *Host) Underlying() host.Host { return h.h }

// DHT returns the Kademlia instance, or nil when discovery.dht is false.
func (h *Host) DHT() *dht.IpfsDHT { return h.dht }

// Addrs lists the listening multiaddrs.
func (h *Host) Addrs() []string { return addrsToStrings(h.h.Addrs()) }

// AddrInfo is this node's dialable description.
func (h *Host) AddrInfo() peer.AddrInfo {
	return peer.AddrInfo{ID: h.h.ID(), Addrs: h.h.Addrs()}
}

// Connect dials a peer, absorbing its advertised addresses.
func (h *Host) Connect(ctx context.Context, ai peer.AddrInfo) error {
	if ai.ID == h.h.ID() {
		return fmt.Errorf("p2p: refusing to connect to self")
	}
	if err := h.h.Connect(ctx, ai); err != nil {
		return fmt.Errorf("p2p: connect %s: %w", ai.ID, err)
	}
	return nil
}

// OpenStream starts a protocol stream to a peer, wrapping it with byte counters
// when a metrics recorder is configured.
func (h *Host) OpenStream(ctx context.Context, p peer.ID, pid protocol.ID) (network.Stream, error) {
	s, err := h.h.NewStream(ctx, p, pid)
	if err != nil {
		return nil, fmt.Errorf("p2p: open stream %s to %s: %w", pid, p, err)
	}
	return s, nil
}

// WriteMsg frames a message onto w, counting bytes when metrics are enabled.
func (h *Host) WriteMsg(w io.Writer, m proto.Message) error {
	if h.rec == nil {
		return writeMsg(w, m)
	}
	return writeMsg(NewCountedWriter(w, h.rec), m)
}

// ReadMsg reads one framed message from r.
func (h *Host) ReadMsg(r io.Reader, limit int, m proto.Message) error {
	if h.rec == nil {
		return readMsg(r, limit, m)
	}
	return readMsg(NewCountedReader(r, h.rec), limit, m)
}

// MaxMessageBytes is the configured inbound frame ceiling.
func (h *Host) MaxMessageBytes() int { return int(h.cfg.Security.MaxMessageBytes) }

// Close shuts the host and DHT down.
func (h *Host) Close() error {
	var errs []string
	if h.dht != nil {
		if err := h.dht.Close(); err != nil {
			errs = append(errs, "dht: "+err.Error())
		}
	}
	if err := h.h.Close(); err != nil {
		errs = append(errs, "host: "+err.Error())
	}
	if len(errs) > 0 {
		return fmt.Errorf("p2p: close: %s", strings.Join(errs, "; "))
	}
	return nil
}

// ConnectedPeers returns the current peer set.
func (h *Host) ConnectedPeers() []peer.ID { return h.h.Network().Peers() }

// IsConnected reports whether a live connection to p exists.
func (h *Host) IsConnected(p peer.ID) bool {
	return h.h.Network().Connectedness(p) == network.Connected
}

// PeerAddrs returns the addresses libp2p knows for a peer.
func (h *Host) PeerAddrs(p peer.ID) []string {
	return addrsToStrings(h.h.Peerstore().Addrs(p))
}

// CountedWriter accounts outbound bytes.
type CountedWriter struct {
	w   io.Writer
	rec MetricsRecorder
}

// NewCountedWriter wraps w, adding every write length to rec.
func NewCountedWriter(w io.Writer, rec MetricsRecorder) *CountedWriter {
	return &CountedWriter{w: w, rec: rec}
}

func (c *CountedWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	if n > 0 && c.rec != nil {
		c.rec.AddBytesSent(uint64(n))
	}
	return n, err
}

// CountedReader accounts inbound bytes.
type CountedReader struct {
	r   io.Reader
	rec MetricsRecorder
}

// NewCountedReader wraps r, adding every read length to rec.
func NewCountedReader(r io.Reader, rec MetricsRecorder) *CountedReader {
	return &CountedReader{r: r, rec: rec}
}

func (c *CountedReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if n > 0 && c.rec != nil {
		c.rec.AddBytesReceived(uint64(n))
	}
	return n, err
}

// staticAddrsFactory pins the advertised addresses, which is what an operator
// behind NAT or with multiple interfaces needs.
func staticAddrsFactory(announce []string) func([]ma.Multiaddr) []ma.Multiaddr {
	return func(_ []ma.Multiaddr) []ma.Multiaddr {
		out := make([]ma.Multiaddr, 0, len(announce))
		for _, a := range announce {
			if m, err := ma.NewMultiaddr(a); err == nil {
				out = append(out, m)
			}
		}
		return out
	}
}

// decodePSK accepts the standard swarm.key form: "/1/<base32 of 32 key bytes>".
func decodePSK(s string) (pnet.PSK, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	if !strings.HasPrefix(s, "/1/") {
		return nil, fmt.Errorf("expected a /1/<base32> psk, got %q", redact(s))
	}
	raw, err := base32.StdEncoding.DecodeString(strings.TrimPrefix(s, "/1/"))
	if err != nil {
		return nil, fmt.Errorf("psk is not valid base32: %w", err)
	}
	if len(raw) != 32 {
		return nil, fmt.Errorf("psk must be 32 bytes, got %d", len(raw))
	}
	return pnet.PSK(raw), nil
}

// GeneratePSK builds a fresh private-network key for operators.
func GeneratePSK() (string, error) {
	var key [32]byte
	if _, err := io.ReadFull(rand.Reader, key[:]); err != nil {
		return "", fmt.Errorf("p2p: generate psk: %w", err)
	}
	return "/1/" + base32.StdEncoding.EncodeToString(key[:]), nil
}

func redact(s string) string {
	if len(s) <= 6 {
		return "***"
	}
	return s[:3] + "***"
}

func addrsToStrings(addrs []ma.Multiaddr) []string {
	out := make([]string, 0, len(addrs))
	for _, a := range addrs {
		out = append(out, a.String())
	}
	return out
}
