// Package storage persists node-local state: task journal, peer records,
// deduplication keys and content-addressed artifacts. It is deliberately a
// single-node store — the mesh has no shared database.
package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
)

// ErrNotFound is returned when a key is absent.
var ErrNotFound = errors.New("storage: not found")

// Bucket prefixes keep unrelated record families in one keyspace.
const (
	pfxTask     = "t:" // t:<task_id>        -> TaskRecord
	pfxResult   = "r:" // r:<task_id>        -> ResultRecord
	pfxPeer     = "p:" // p:<peer_id>        -> PeerRecord
	pfxDedup    = "d:" // d:<task_id>        -> DedupRecord
	pfxSkill    = "s:" // s:<skill>/<peer>   -> 1
	pfxMeta     = "m:" // m:<key>            -> bytes
	pfxChild    = "c:" // c:<parent>/<child> -> 1
	pfxParentOf = "o:" // o:<child>          -> parent task id
)

// Store is the node's persistent state.
type Store struct {
	db      *badger.DB
	artRoot string
	opened  time.Time
}

// Options configures Open.
type Options struct {
	Dir          string
	ArtifactsDir string
	InMemory     bool
	Logger       badger.Logger
}

// Open creates or loads the store.
func Open(opts Options) (*Store, error) {
	if !opts.InMemory {
		if err := os.MkdirAll(opts.Dir, 0o700); err != nil {
			return nil, fmt.Errorf("storage: mkdir %s: %w", opts.Dir, err)
		}
		if err := os.MkdirAll(opts.ArtifactsDir, 0o700); err != nil {
			return nil, fmt.Errorf("storage: mkdir artifacts: %w", err)
		}
	}
	bopts := badger.DefaultOptions(opts.Dir).
		WithLogger(opts.Logger).
		WithInMemory(opts.InMemory).
		WithCompression(options.None)
	if opts.InMemory {
		bopts.ValueDir = ""
	}
	db, err := badger.Open(bopts)
	if err != nil {
		return nil, fmt.Errorf("storage: open badger: %w", err)
	}
	return &Store{db: db, artRoot: opts.ArtifactsDir, opened: time.Now().UTC()}, nil
}

// Close flushes the database.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	return s.db.Close()
}

// PutTask upserts a task journal record.
func (s *Store) PutTask(rec *TaskRecord) error {
	if rec == nil || rec.TaskID == "" {
		return errors.New("storage: task record requires task_id")
	}
	rec.UpdatedAt = time.Now().UTC().Unix()
	return s.putJSON(pfxTask+rec.TaskID, rec)
}

// GetTask loads a task journal record.
func (s *Store) GetTask(taskID string) (*TaskRecord, error) {
	var rec TaskRecord
	if err := s.getJSON(pfxTask+taskID, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

// ListTasks walks the journal newest-first, honouring an optional status filter.
func (s *Store) ListTasks(limit int, statusFilter string) ([]*TaskRecord, error) {
	if limit <= 0 {
		limit = 100
	}
	var out []*TaskRecord
	err := s.iterate(pfxTask, func(k, v []byte) error {
		rec := &TaskRecord{}
		if err := json.Unmarshal(v, rec); err != nil {
			return nil // tolerate a corrupt record rather than fail the listing
		}
		if statusFilter != "" && rec.Status != statusFilter {
			return nil
		}
		out = append(out, rec)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt > out[j].UpdatedAt })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// PutResult stores the outcome of a task.
func (s *Store) PutResult(rec *ResultRecord) error {
	if rec == nil || rec.TaskID == "" {
		return errors.New("storage: result record requires task_id")
	}
	return s.putJSON(pfxResult+rec.TaskID, rec)
}

// GetResult loads a task outcome.
func (s *Store) GetResult(taskID string) (*ResultRecord, error) {
	var rec ResultRecord
	if err := s.getJSON(pfxResult+taskID, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

// PutPeer upserts a peer record and rebuilds its skill index in one transaction.
//
// The index has to describe what the peer advertises *now*: entries left over
// from a dropped skill would keep offering a node that no longer serves it, and
// nothing else ever reclaims them (Prune touches only the task journal). Skill
// names are normalised because every other comparison in the mesh lowercases
// and trims them (tasks.SkillsMatch) — an index that stored "OCR" would hide the
// peer from a lookup for "ocr".
func (s *Store) PutPeer(rec *PeerRecord) error {
	if rec == nil || rec.PeerID == "" {
		return errors.New("storage: peer record requires peer_id")
	}
	rec.SeenAt = time.Now().UTC().Unix()
	want := normalizedSkills(rec.Skills)
	return s.db.Update(func(txn *badger.Txn) error {
		if err := deletePeerSkillIndex(txn, rec.PeerID); err != nil {
			return err
		}
		b, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("storage: marshal peer %s: %w", rec.PeerID, err)
		}
		if err := txn.Set([]byte(pfxPeer+rec.PeerID), b); err != nil {
			return err
		}
		for _, sk := range want {
			if err := txn.Set(skillKey(sk, rec.PeerID), []byte{1}); err != nil {
				return err
			}
		}
		return nil
	})
}

// deletePeerSkillIndex removes every index entry naming peerID. It covers both
// the normalised and the verbatim spelling of each stored skill so that keys
// written before normalisation existed are reclaimed too.
func deletePeerSkillIndex(txn *badger.Txn, peerID string) error {
	item, err := txn.Get([]byte(pfxPeer + peerID))
	switch {
	case errors.Is(err, badger.ErrKeyNotFound):
		// The index can only outlive the record it describes; nothing to reclaim.
		return nil
	case err != nil:
		return err
	}
	prev := &PeerRecord{}
	if err := item.Value(func(v []byte) error { return json.Unmarshal(v, prev) }); err != nil {
		return nil // an unreadable record must not block the rewrite
	}
	for _, sk := range prev.Skills {
		for _, name := range []string{sk, normalizeSkill(sk)} {
			if name == "" {
				continue
			}
			if err := txn.Delete(skillKey(name, peerID)); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
		}
	}
	return nil
}

// normalizedSkills returns the distinct, non-empty skill names in canonical
// form, which is how the reverse index keys them.
func normalizedSkills(skills []string) []string {
	out := make([]string, 0, len(skills))
	seen := make(map[string]bool, len(skills))
	for _, sk := range skills {
		n := normalizeSkill(sk)
		if n == "" || seen[n] {
			continue
		}
		seen[n] = true
		out = append(out, n)
	}
	return out
}

func normalizeSkill(s string) string { return strings.ToLower(strings.TrimSpace(s)) }

func skillKey(skill, peerID string) []byte {
	return []byte(pfxSkill + skill + "/" + peerID)
}

// GetPeer loads a peer record.
func (s *Store) GetPeer(peerID string) (*PeerRecord, error) {
	var rec PeerRecord
	if err := s.getJSON(pfxPeer+peerID, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

// ListPeers returns every persisted peer.
func (s *Store) ListPeers() ([]*PeerRecord, error) {
	var out []*PeerRecord
	err := s.iterate(pfxPeer, func(_ []byte, v []byte) error {
		rec := &PeerRecord{}
		if err := json.Unmarshal(v, rec); err != nil {
			return err
		}
		out = append(out, rec)
		return nil
	})
	return out, err
}

// PeersBySkill lists peers indexed under a skill. The query is normalised the
// same way PutPeer normalises what it stores, so callers may pass any casing.
func (s *Store) PeersBySkill(skill string) ([]string, error) {
	pre := pfxSkill + normalizeSkill(skill) + "/"
	var out []string
	err := s.iterate(pre, func(k, _ []byte) error {
		out = append(out, strings.TrimPrefix(string(k), pre))
		return nil
	})
	return out, err
}

// MarkSeen refreshes the liveness timestamp of a peer.
func (s *Store) MarkSeen(peerID string) error {
	rec, err := s.GetPeer(peerID)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	rec.SeenAt = time.Now().UTC().Unix()
	return s.putJSON(pfxPeer+peerID, rec)
}

// ClaimDedup atomically reserves a task_id for the dedup window. The boolean
// result reports whether this call is the first to claim it (true = execute).
func (s *Store) ClaimDedup(taskID string, window time.Duration) (bool, *DedupRecord, error) {
	if taskID == "" {
		return false, nil, errors.New("storage: empty task id")
	}
	now := time.Now().UTC()
	key := []byte(pfxDedup + taskID)
	claimed := false
	var prev DedupRecord

	err := s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		switch {
		case err == nil:
			if err := item.Value(func(v []byte) error { return json.Unmarshal(v, &prev) }); err != nil {
				return err
			}
			if now.Unix()-prev.FirstSeenUnix < int64(window.Seconds()) {
				return nil // inside the window: duplicate delivery
			}
		case errors.Is(err, badger.ErrKeyNotFound):
		default:
			return err
		}
		rec := DedupRecord{TaskID: taskID, FirstSeenUnix: now.Unix(), Count: prev.Count + 1}
		b, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		if err := txn.Set(key, b); err != nil {
			return err
		}
		claimed = true
		return nil
	})
	if err != nil {
		return false, nil, fmt.Errorf("storage: dedup claim: %w", err)
	}
	return claimed, &prev, nil
}

// LinkChild records a parent/subtask edge.
func (s *Store) LinkChild(parentID, childID string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set([]byte(pfxChild+parentID+"/"+childID), []byte{1}); err != nil {
			return err
		}
		return txn.Set([]byte(pfxParentOf+childID), []byte(parentID))
	})
}

// ChildrenOf lists subtask ids of a parent.
func (s *Store) ChildrenOf(parentID string) ([]string, error) {
	pre := pfxChild + parentID + "/"
	var out []string
	err := s.iterate(pre, func(k, _ []byte) error {
		out = append(out, strings.TrimPrefix(string(k), pre))
		return nil
	})
	return out, err
}

// ParentOf resolves the parent of a subtask.
func (s *Store) ParentOf(childID string) (string, error) {
	var out []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(pfxParentOf + childID))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			out = append(out, v...)
			return nil
		})
	})
	if errors.Is(err, ErrNotFound) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// PutMeta stores an arbitrary node-local value (identity, watermarks).
func (s *Store) PutMeta(key string, val []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(pfxMeta+key), val)
	})
}

// GetMeta reads a node-local value.
func (s *Store) GetMeta(key string) ([]byte, error) {
	var out []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(pfxMeta + key))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			out = append(out, v...)
			return nil
		})
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, ErrNotFound
	}
	return out, err
}

// StoreArtifact writes content-addressed bytes and returns "sha256:<hex>" and size.
func (s *Store) StoreArtifact(data []byte) (string, int64, error) {
	sum := sha256.Sum256(data)
	name := hex.EncodeToString(sum[:])
	hash := "sha256:" + name
	dir := filepath.Join(s.artRoot, "sha256", name[:2])
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", 0, fmt.Errorf("storage: artifact dir: %w", err)
	}
	path := filepath.Join(dir, name)
	if _, err := os.Stat(path); err == nil {
		return hash, int64(len(data)), nil
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return "", 0, fmt.Errorf("storage: write artifact: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return "", 0, fmt.Errorf("storage: rename artifact: %w", err)
	}
	return hash, int64(len(data)), nil
}

// LoadArtifact reads content-addressed bytes.
func (s *Store) LoadArtifact(hash string) ([]byte, error) {
	path, err := s.ArtifactPath(hash)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: artifact %s", ErrNotFound, hash)
		}
		return nil, err
	}
	return data, nil
}

// ArtifactPath resolves the on-disk location of an artifact.
func (s *Store) ArtifactPath(hash string) (string, error) {
	name := strings.TrimPrefix(hash, "sha256:")
	if len(name) != 64 {
		return "", fmt.Errorf("storage: malformed artifact hash %q", hash)
	}
	if _, err := hex.DecodeString(name); err != nil {
		return "", fmt.Errorf("storage: malformed artifact hash %q", hash)
	}
	return filepath.Join(s.artRoot, "sha256", name[:2], name), nil
}

// Prune drops finished journal records older than keep and returns how many
// were removed.
func (s *Store) Prune(ctx context.Context, keep time.Duration) (int, error) {
	if keep <= 0 {
		keep = 7 * 24 * time.Hour
	}
	cutoff := time.Now().UTC().Add(-keep).Unix()
	removed := 0

	var delTasks, delDedup []string
	err := s.iterate(pfxTask, func(k, v []byte) error {
		rec := &TaskRecord{}
		if err := json.Unmarshal(v, rec); err != nil {
			return nil
		}
		if rec.FinishedAt.IsZero() || rec.FinishedAt.Unix() >= cutoff {
			return nil
		}
		delTasks = append(delTasks, string(k), pfxResult+rec.TaskID)
		return nil
	})
	if err != nil {
		return 0, err
	}
	dedupCutoff := time.Now().UTC().Add(-max(keep, 15*time.Minute)).Unix()
	if err := s.iterate(pfxDedup, func(k, v []byte) error {
		rec := &DedupRecord{}
		if err := json.Unmarshal(v, rec); err != nil {
			return nil
		}
		if rec.FirstSeenUnix < dedupCutoff {
			delDedup = append(delDedup, string(k))
		}
		return nil
	}); err != nil {
		return 0, err
	}

	err = s.db.Update(func(txn *badger.Txn) error {
		for _, k := range append(delTasks, delDedup...) {
			if err := txn.Delete([]byte(k)); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
			removed++
		}
		return nil
	})
	if err != nil {
		return removed, err
	}
	if err := ctx.Err(); err != nil {
		return removed, err
	}
	return removed, nil
}

// Stats summarises store occupancy for the admin API.
type Stats struct {
	Tasks     int64     `json:"tasks"`
	Peers     int64     `json:"peers"`
	DedupKeys int64     `json:"dedup_keys"`
	OpenedAt  time.Time `json:"opened_at"`
}

// Stats reports rough record counts.
func (s *Store) Stats() (Stats, error) {
	var st Stats
	st.OpenedAt = s.opened
	counts := []struct {
		pfx string
		dst *int64
	}{{pfxTask, &st.Tasks}, {pfxPeer, &st.Peers}, {pfxDedup, &st.DedupKeys}}
	for _, c := range counts {
		n := int64(0)
		if err := s.iterate(c.pfx, func([]byte, []byte) error { n++; return nil }); err != nil {
			return st, err
		}
		*c.dst = n
	}
	return st, nil
}

func (s *Store) putJSON(key string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("storage: marshal %s: %w", key, err)
	}
	return s.db.Update(func(txn *badger.Txn) error { return txn.Set([]byte(key), b) })
}

func (s *Store) getJSON(key string, dst any) error {
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error { return json.Unmarshal(v, dst) })
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return ErrNotFound
	}
	return err
}

func (s *Store) iterate(prefix string, fn func(k, v []byte) error) error {
	return s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefix)
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.KeyCopy(nil)
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if err := fn(k, v); err != nil {
				return err
			}
		}
		return nil
	})
}

func max(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

// TaskRecord is the durable journal entry for one task seen by this node.
type TaskRecord struct {
	TaskID         string    `json:"task_id"`
	ParentTaskID   string    `json:"parent_task_id,omitempty"`
	OriginPeerID   string    `json:"origin_peer_id"`
	SenderPeerID   string    `json:"sender_peer_id"`
	Status         string    `json:"status"`
	RequiredSkills []string  `json:"required_skills,omitempty"`
	ReceivedAt     time.Time `json:"received_at"`
	FinishedAt     time.Time `json:"finished_at,omitempty"`
	WorkerPeerID   string    `json:"worker_peer_id,omitempty"`
	DelegatedTo    []string  `json:"delegated_to,omitempty"`
	Error          string    `json:"error,omitempty"`
	ResultDigest   string    `json:"result_digest,omitempty"`
	Attempts       int       `json:"attempts"`
	TTL            int32     `json:"ttl"`
	Priority       int32     `json:"priority"`
	PayloadHash    string    `json:"payload_hash,omitempty"`
	UpdatedAt      int64     `json:"updated_at"`
}

// ResultRecord stores the outcome payload of a completed task.
type ResultRecord struct {
	TaskID       string    `json:"task_id"`
	Status       string    `json:"status"`
	Text         string    `json:"text,omitempty"`
	ErrorMessage string    `json:"error_message,omitempty"`
	WorkerPeerID string    `json:"worker_peer_id,omitempty"`
	StartedAt    time.Time `json:"started_at,omitempty"`
	FinishedAt   time.Time `json:"finished_at,omitempty"`
	// Model names the model that answered a task executed on *this* node
	// (ТЗ 10.3): what the local agent disclosed, or the model it was asked to
	// use. It is journal-only — TaskResult carries no model field on the wire, so
	// the model a remote worker used stays unknown here.
	Model     string         `json:"model,omitempty"`
	Artifacts []ArtifactInfo `json:"artifacts,omitempty"`
	Signature []byte         `json:"signature,omitempty"`
	// WorkerSignature is the chain signature of the executing peer (ТЗ 6.10.3).
	// The transport Signature only proves who forwarded the result; this one
	// proves what the worker returned and must survive a restart so the origin
	// can re-verify a stored answer (ТЗ 9.2, 11.5).
	WorkerSignature []byte `json:"worker_signature,omitempty"`
	ResultDigest    string `json:"result_digest,omitempty"`
	Aggregated      bool   `json:"aggregated,omitempty"`
	// ErrorClass is the machine-readable failure taxonomy of ТЗ 6.12 kept as a
	// short name ("NO_WORKER"). Relays and the decomposition planner decide
	// whether a task may be retried by this class rather than by the message,
	// so it has to survive a journal read: a lost class silently turns a
	// retryable failure into an unspecified one.
	ErrorClass string `json:"error_class,omitempty"`
}

// ArtifactInfo describes one stored artifact.
type ArtifactInfo struct {
	Name string `json:"name"`
	Hash string `json:"hash"`
	Size int64  `json:"size"`
}

// PeerRecord is the durable view of a neighbour.
type PeerRecord struct {
	PeerID         string   `json:"peer_id"`
	Addrs          []string `json:"addrs,omitempty"`
	Skills         []string `json:"skills,omitempty"`
	Trust          string   `json:"trust"`
	Category       string   `json:"category"`
	Version        string   `json:"version,omitempty"`
	Load           float64  `json:"load"`
	MaxParallel    int32    `json:"max_parallel"`
	Running        int32    `json:"running"`
	Successes      uint64   `json:"successes"`
	Failures       uint64   `json:"failures"`
	ProtocolErrors uint64   `json:"protocol_errors"`
	AvgRTTMillis   int64    `json:"avg_rtt_ms"`
	SeenAt         int64    `json:"seen_at"`
	Connected      bool     `json:"connected"`
	LastStatus     string   `json:"last_status,omitempty"`
	// SkillsVersion is the peer's advertised skill epoch at last sighting; it
	// must survive a restart, otherwise the node would forget that it already
	// holds a peer's newest descriptors and re-sync them needlessly.
	SkillsVersion int64 `json:"skills_version,omitempty"`
}

// DedupRecord is the idempotency guard for task delivery.
type DedupRecord struct {
	TaskID        string `json:"task_id"`
	FirstSeenUnix int64  `json:"first_seen_unix"`
	Count         int    `json:"count"`
}
