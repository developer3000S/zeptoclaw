package security

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// Limiter is a per-peer token bucket with a global backstop.
type Limiter struct {
	mu      sync.Mutex
	perPeer map[string]*rate.Limiter
	rps     rate.Limit
	burst   int
	lastGC  time.Time
	gcEvery time.Duration
}

// NewLimiter builds a limiter from configuration values.
func NewLimiter(requestsPerSecond float64, burst int) *Limiter {
	if burst <= 0 {
		burst = 1
	}
	return &Limiter{
		perPeer: make(map[string]*rate.Limiter),
		rps:     rate.Limit(requestsPerSecond),
		burst:   burst,
		lastGC:  time.Now(),
		gcEvery: 10 * time.Minute,
	}
}

// Allow consumes one token for a peer.
func (l *Limiter) Allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if time.Since(l.lastGC) > l.gcEvery {
		l.gcLocked()
	}
	lim, ok := l.perPeer[key]
	if !ok {
		lim = rate.NewLimiter(l.rps, l.burst)
		l.perPeer[key] = lim
	}
	return lim.Allow()
}

// Wait blocks until a token is available for the peer or ctx is done.
func (l *Limiter) Wait(ctx context.Context, key string) error {
	l.mu.Lock()
	lim, ok := l.perPeer[key]
	if !ok {
		lim = rate.NewLimiter(l.rps, l.burst)
		l.perPeer[key] = lim
	}
	l.mu.Unlock()
	// The bucket's own Wait: it parks until the earliest token and cancels the
	// reservation when the context expires. Probing with Allow and returning
	// ctx.Err() afterwards did neither — it refused callers a token they were
	// entitled to wait for, and admitted them the instant a refill happened to
	// land, making the outcome a race against machine speed.
	if err := lim.Wait(ctx); err != nil {
		return fmt.Errorf("security: rate limit exceeded for %s: %w", key, err)
	}
	return nil
}

func (l *Limiter) gcLocked() {
	// Idle buckets are cheap but unbounded in number; drop them periodically.
	l.perPeer = make(map[string]*rate.Limiter)
	l.lastGC = time.Now()
}

// Size reports the number of tracked peers.
func (l *Limiter) Size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.perPeer)
}

// SetRate applies new limits to every existing bucket, backing the admin
// config reload (ТЗ 13.1.1 reload). Peers learned afterwards get the new
// defaults too; in-flight tokens are not retroactively spent.
func (l *Limiter) SetRate(requestsPerSecond float64, burst int) {
	if burst <= 0 {
		burst = 1
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.rps = rate.Limit(requestsPerSecond)
	l.burst = burst
	for _, lim := range l.perPeer {
		lim.SetLimit(l.rps)
		lim.SetBurst(burst)
	}
}

// AuditEvent is one security-relevant record.
type AuditEvent struct {
	Time   time.Time `json:"ts"`
	Event  string    `json:"event"`
	PeerID string    `json:"peer_id,omitempty"`
	TaskID string    `json:"task_id,omitempty"`
	Remote string    `json:"remote,omitempty"`
	Reason string    `json:"reason,omitempty"`
	Trust  string    `json:"trust,omitempty"`
	Bytes  int64     `json:"bytes,omitempty"`
	Detail string    `json:"detail,omitempty"`
}

// Audit is an append-only JSONL journal of security events.
type Audit struct {
	mu     sync.Mutex
	f      *os.File
	path   string
	logger *slog.Logger
	closed bool
}

// OpenAudit creates or appends the journal. A nil path yields a logger-only
// audit, which keeps tests and ephemeral nodes simple.
func OpenAudit(path string, logger *slog.Logger) (*Audit, error) {
	a := &Audit{path: path, logger: logger}
	if path == "" {
		return a, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("security: audit dir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("security: open audit: %w", err)
	}
	a.f = f
	return a, nil
}

// Log records an event. It never fails the caller: an unwritable journal is
// reported through the logger instead.
func (a *Audit) Log(ev AuditEvent) {
	if ev.Time.IsZero() {
		ev.Time = time.Now().UTC()
	}
	if a.logger != nil {
		// The event name is the record's own text, which is where the logger puts
		// the `event` field: naming it again as an attribute would give a record
		// with the key twice.
		name := ev.Event
		if name == "" {
			name = "security_event"
		}
		a.logger.Warn(name,
			"peer_id", ev.PeerID, "task_id", ev.TaskID, "reason", ev.Reason)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.f == nil || a.closed {
		return
	}
	b, err := json.Marshal(ev)
	if err != nil {
		return
	}
	_, _ = a.f.Write(append(b, '\n'))
}

// Close flushes and closes the journal.
func (a *Audit) Close() error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.f == nil || a.closed {
		return nil
	}
	a.closed = true
	err := a.f.Close()
	if errors.Is(err, os.ErrClosed) {
		return nil
	}
	return err
}

// Path returns the journal location, if any.
func (a *Audit) Path() string { return a.path }
