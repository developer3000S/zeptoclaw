// Package discovery implements the layered peer discovery mechanisms: a local
// unix-socket registry for co-located nodes, mDNS for the LAN, bootstrap peers
// and a Kademlia DHT for the Internet, plus gossip for membership upkeep.
package discovery

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// LocalRecord is what a node publishes into the on-host registry.
type LocalRecord struct {
	PeerID    string    `json:"peer_id"`
	NodeName  string    `json:"node_name"`
	Socket    string    `json:"socket"`
	Addrs     []string  `json:"addrs"`
	Skills    []string  `json:"skills,omitempty"`
	APIListen string    `json:"api_listen,omitempty"`
	StartedAt time.Time `json:"started_at"`
	PID       int       `json:"pid"`
}

// LocalRegistry is a directory of unix sockets, one file per node on this host.
//
// It is deliberately dumb: each node owns exactly one file named after its peer
// id and refreshes it periodically. A stale file (not refreshed within TTL, or
// whose socket no longer answers) is treated as a dead node by readers, which
// is what keeps the registry correct after a crash without any coordination.
type LocalRegistry struct {
	dir    string
	peerID peer.ID
	rec    LocalRecord
	log    *slog.Logger
	ttl    time.Duration

	mu      sync.Mutex
	path    string
	cancel  context.CancelFunc
	running bool
}

// ErrNoRegistry reports that no registry directory could be opened.
var ErrNoRegistry = errors.New("discovery: local registry unavailable")

// NewLocalRegistry prepares a registry entry for this node.
func NewLocalRegistry(dir string, pid peer.ID, rec LocalRecord, ttl time.Duration, logger *slog.Logger) (*LocalRegistry, error) {
	if dir == "" {
		return nil, fmt.Errorf("%w: empty directory", ErrNoRegistry)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrNoRegistry, err)
	}
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	rec.PeerID = pid.String()
	rec.StartedAt = time.Now().UTC()
	rec.PID = os.Getpid()
	return &LocalRegistry{
		dir:    dir,
		peerID: pid,
		rec:    rec,
		log:    logger,
		ttl:    ttl,
		path:   filepath.Join(dir, pid.String()+".json"),
	}, nil
}

// Publish writes this node's record.
func (r *LocalRegistry) Publish() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	b, err := json.MarshalIndent(r.rec, "", "  ")
	if err != nil {
		return err
	}
	tmp := r.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return fmt.Errorf("discovery: publish registry: %w", err)
	}
	return os.Rename(tmp, r.path)
}

// UpdateSkills republishes the record with a fresh skill set (and, when given,
// fresh addresses). Co-located nodes read the registry on their maintenance
// tick, so a skill change on this host becomes visible without a restart.
func (r *LocalRegistry) UpdateSkills(skills []string, addrs []string) error {
	r.mu.Lock()
	r.rec.Skills = append([]string(nil), skills...)
	if len(addrs) > 0 {
		r.rec.Addrs = append([]string(nil), addrs...)
	}
	r.mu.Unlock()
	return r.Publish()
}

// Start publishes immediately and refreshes on a heartbeat.
func (r *LocalRegistry) Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return nil
	}
	r.running = true
	r.mu.Unlock()

	if err := r.Publish(); err != nil {
		return err
	}
	hbCtx, cancel := context.WithCancel(ctx)
	r.mu.Lock()
	r.cancel = cancel
	r.mu.Unlock()

	go func() {
		t := time.NewTicker(r.ttl / 2)
		defer t.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-t.C:
				if err := r.Publish(); err != nil && r.log != nil {
					r.log.Debug("local_registry_publish", "err", err.Error())
				}
			}
		}
	}()
	if r.log != nil {
		r.log.Info("local_registry_published", "path", r.path, "dir", r.dir)
	}
	return nil
}

// Stop removes this node's record.
func (r *LocalRegistry) Stop() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cancel != nil {
		r.cancel()
		r.cancel = nil
	}
	r.running = false
	err := os.Remove(r.path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// List reads every live record in the registry except our own.
func (r *LocalRegistry) List() ([]LocalRecord, error) {
	entries, err := os.ReadDir(r.dir)
	if err != nil {
		return nil, fmt.Errorf("discovery: read registry dir: %w", err)
	}
	var out []LocalRecord
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		path := filepath.Join(r.dir, e.Name())
		rec, err := readRecord(path)
		if err != nil {
			continue
		}
		if rec.PeerID == r.peerID.String() {
			continue
		}
		if time.Since(rec.StartedAt) > 0 && time.Since(rec.StartedAt) < -time.Minute {
			continue // clock skew: a future timestamp is not trustworthy
		}
		if !alive(path, r.ttl) {
			continue
		}
		out = append(out, *rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PeerID < out[j].PeerID })
	return out, nil
}

// readRecord parses one registry file.
func readRecord(path string) (*LocalRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 64<<10))
	if err != nil {
		return nil, err
	}
	rec := &LocalRecord{}
	if err := json.Unmarshal(b, rec); err != nil {
		return nil, err
	}
	if rec.PeerID == "" {
		return nil, errors.New("discovery: registry record without peer_id")
	}
	return rec, nil
}

// alive decides whether a registry entry still refers to a running node. The
// probe socket is authoritative when present: a crashed process leaves its
// socket file behind, and mtime alone would then be a lie. Without a probe
// socket we fall back to the mtime heartbeat.
func alive(path string, ttl time.Duration) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	if time.Since(info.ModTime()) > ttl {
		return false
	}
	rec, err := readRecord(path)
	if err != nil || rec.Socket == "" {
		return true
	}
	c, err := net.DialTimeout("unix", rec.Socket, 500*time.Millisecond)
	if err != nil {
		return false
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(time.Second))
	if _, err := c.Write([]byte(ProbeRequest + "\n")); err != nil {
		return false
	}
	line, err := bufio.NewReader(c).ReadString('\n')
	if err != nil && line == "" {
		return false
	}
	// The answer must name the same peer the record claims.
	return strings.Contains(line, `"peer_id":"`+rec.PeerID+`"`)
}

// ProbeRequest is the single line a probe client writes.
const ProbeRequest = "zeptomesh-probe"

// Probe serves a node's liveness socket. Each accepted connection gets one
// JSON line describing the node, then the connection closes.
type Probe struct {
	ln     net.Listener
	path   string
	mu     sync.RWMutex
	status func() any
	done   chan struct{}
	log    *slog.Logger
}

// NewProbe prepares (and removes any stale) socket path.
func NewProbe(dir, peerID string, logger *slog.Logger) (*Probe, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, peerID+".sock")
	// A leftover socket from a killed process is ours to reclaim: nothing can
	// be listening on it, or the bind below would fail.
	if _, err := os.Stat(path); err == nil {
		if c, derr := net.DialTimeout("unix", path, 200*time.Millisecond); derr == nil {
			c.Close()
			return nil, fmt.Errorf("discovery: %s is already in use", path)
		}
		_ = os.Remove(path)
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("discovery: listen %s: %w", path, err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = ln.Close()
		return nil, err
	}
	return &Probe{ln: ln, path: path, done: make(chan struct{}), log: logger}, nil
}

// SetStatus installs the callback that renders the probe answer.
func (p *Probe) SetStatus(fn func() any) {
	p.mu.Lock()
	p.status = fn
	p.mu.Unlock()
}

// Path is the socket location to publish in the registry.
func (p *Probe) Path() string { return p.path }

// Serve accepts connections until Close.
func (p *Probe) Serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			select {
			case <-p.done:
				return
			default:
			}
			if p.log != nil {
				p.log.Debug("probe_accept", "err", err.Error())
			}
			return
		}
		go p.handle(c)
	}
}

func (p *Probe) handle(c net.Conn) {
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(2 * time.Second))
	var req [64]byte
	n, _ := c.Read(req[:])
	if !strings.Contains(string(req[:n]), ProbeRequest) {
		return
	}
	p.mu.RLock()
	fn := p.status
	p.mu.RUnlock()
	if fn == nil {
		return
	}
	b, err := json.Marshal(fn())
	if err != nil {
		return
	}
	_, _ = c.Write(append(b, '\n'))
}

// Close removes the socket.
func (p *Probe) Close() error {
	if p == nil {
		return nil
	}
	select {
	case <-p.done:
		return nil
	default:
		close(p.done)
	}
	err := p.ln.Close()
	_ = os.Remove(p.path)
	if err != nil && !errors.Is(err, net.ErrClosed) {
		return err
	}
	return nil
}

// Dir reports the registry location.
func (r *LocalRegistry) Dir() string { return r.dir }

// SocketPath is the probe socket this node should serve.
func (r *LocalRegistry) SocketPath() string {
	return filepath.Join(r.dir, r.peerID.String()+".sock")
}

// SetSocket records the probe socket in the published entry.
func (r *LocalRegistry) SetSocket(path string) {
	r.mu.Lock()
	r.rec.Socket = path
	r.mu.Unlock()
}
