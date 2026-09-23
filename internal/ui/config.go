// Package ui implements the ZeptoClaw mesh dashboard backend (BFF): it polls
// the admin APIs of the configured agent nodes, merges their view of the mesh
// into one network graph, and proxies operator actions back to a chosen node.
package ui

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Defaults match the agent's admin API conventions (loopback-first, short
// timeouts); every one of them is overridable through the environment.
const (
	defaultListen     = "127.0.0.1:28090"
	defaultDataDir    = "./zeptomesh-ui-data"
	defaultNodesFile  = "nodes.json"
	defaultPollString = "5s"
	defaultLogLevel   = slog.LevelInfo

	pollTimeout       = 5 * time.Second
	actionTimeout     = 60 * time.Second
	maxNodesBodyBytes = 1 << 20
)

// NodeEntry is one configured agent node. Token is optional and never leaves
// the backend: GET /api/v1/nodes serves only the public fields.
type NodeEntry struct {
	Name  string `json:"name"`
	URL   string `json:"url"`
	Token string `json:"token,omitempty"`
}

// Config is the dashboard backend configuration.
type Config struct {
	Listen       string
	DataDir      string
	NodesFile    string
	Token        string // access token for this UI's own API; empty = no auth
	APIToken     string // bearer used for nodes without their own token
	PollInterval time.Duration
	LogLevel     slog.Level
	Nodes        []NodeEntry
}

// Load builds the config from the environment, then resolves the node list:
// the persistent nodes.json wins, otherwise the ZETOMESH_UI_NODES seed is
// written into it so the UI has one editable source of truth.
func Load() (Config, error) {
	cfg := Config{
		Listen:       envOr("ZETOMESH_UI_LISTEN", defaultListen),
		DataDir:      envOr("ZETOMESH_UI_DATA", defaultDataDir),
		Token:        os.Getenv("ZETOMESH_UI_TOKEN"),
		APIToken:     os.Getenv("ZETOMESH_UI_API_TOKEN"),
		PollInterval: 0,
		LogLevel:     defaultLogLevel,
	}
	if v := os.Getenv("ZETOMESH_UI_POLL_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, fmt.Errorf("ZETOMESH_UI_POLL_INTERVAL: %w", err)
		}
		if d < time.Second {
			return Config{}, fmt.Errorf("ZETOMESH_UI_POLL_INTERVAL must be >= 1s, got %s", d)
		}
		cfg.PollInterval = d
	} else {
		d, err := time.ParseDuration(defaultPollString)
		if err != nil {
			return Config{}, err
		}
		cfg.PollInterval = d
	}
	if v := os.Getenv("ZETOMESH_UI_LOG_LEVEL"); v != "" {
		var level slog.Level
		if err := level.UnmarshalText([]byte(v)); err != nil {
			return Config{}, fmt.Errorf("ZETOMESH_UI_LOG_LEVEL: %w", err)
		}
		cfg.LogLevel = level
	}

	if v := os.Getenv("ZETOMESH_UI_NODES_FILE"); v != "" {
		cfg.NodesFile = v
	} else {
		cfg.NodesFile = filepath.Join(cfg.DataDir, defaultNodesFile)
	}

	seed := parseNodesEnv(os.Getenv("ZETOMESH_UI_NODES"))
	stored, err := loadNodes(cfg.NodesFile)
	if err != nil {
		return Config{}, err
	}
	if stored != nil {
		cfg.Nodes = stored
	} else {
		cfg.Nodes = seed
		if err := saveNodes(cfg.NodesFile, cfg.Nodes); err != nil {
			return Config{}, err
		}
	}
	return cfg, nil
}

// parseNodesEnv splits "http://a:8081,http://b:8082" (optionally
// "name=url" per entry) into node entries with generated names.
func parseNodesEnv(s string) []NodeEntry {
	var out []NodeEntry
	for _, raw := range strings.Split(s, ",") {
		entry := strings.TrimSpace(raw)
		if entry == "" {
			continue
		}
		name, url, found := strings.Cut(entry, "=")
		if !found {
			// Bare URL: derive a name from its host part.
			name = hostOf(entry)
			url = entry
		}
		out = append(out, NodeEntry{Name: strings.TrimSpace(name), URL: strings.TrimSpace(url)})
	}
	return out
}

// hostOf turns "http://zepto-0:33498" into "zepto-0" for a default label.
func hostOf(rawURL string) string {
	rest := rawURL
	if i := strings.Index(rest, "://"); i >= 0 {
		rest = rest[i+3:]
	}
	rest, _, _ = strings.Cut(rest, "/")
	host, _, _ := strings.Cut(rest, ":")
	return host
}

// loadNodes reads the persistent node list. A missing file is not an error:
// it means a first start, and the caller seeds the file.
func loadNodes(path string) ([]NodeEntry, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read nodes file %s: %w", path, err)
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return nil, nil
	}
	var nodes []NodeEntry
	if err := json.Unmarshal(data, &nodes); err != nil {
		return nil, fmt.Errorf("parse nodes file %s: %w", path, err)
	}
	return nodes, nil
}

// saveNodes writes the node list atomically: the file is the UI's source of
// truth for the mesh membership, so a half-written copy would survive a crash.
func saveNodes(path string, nodes []NodeEntry) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create nodes dir: %w", err)
	}
	data, err := json.MarshalIndent(nodes, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write nodes file: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("commit nodes file: %w", err)
	}
	return nil
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}
