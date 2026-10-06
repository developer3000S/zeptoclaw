// Package brain discovers and selects the LLM backends an agent may think with.
//
// There are two kinds of knowledge about an LLM endpoint, and the distinction
// is the whole point of this package:
//
//   - A *backend* is an endpoint the operator pointed the agent at — a local
//     Ollama instance or an explicit entry in brain.backends. The agent probes
//     it, learns its models and may run tasks on it. Consent is recorded by the
//     fact that the operator configured it (see Backend).
//   - A *candidate* is an internet host a search engine reported as exposing an
//     open Ollama port. It is inventory metadata only: the agent never connects
//     to a candidate, never verifies it and never routes traffic to it.
//     Promoting a candidate to a backend is an explicit operator action, which
//     is where consent is recorded.
//
// The catalog therefore answers "what LLM hardware is out there" while the
// backend pool answers "what may I actually think with".
package brain

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// catalogDedupKey is the identity of a candidate: the same host seen on the
// same port over the same protocol is one candidate, however many search
// engines reported it (FOA §4.4.2).
func catalogDedupKey(ip string, port int, protocol string) string {
	if protocol == "" {
		protocol = "tcp"
	}
	return fmt.Sprintf("%s:%d:%s", ip, port, protocol)
}

// Candidate is an internet-hosted Ollama endpoint a search engine reported. It
// is inventory metadata: nothing in the mesh ever dials it. See the package
// doc for why promotion to a Backend is a separate, explicit step.
type Candidate struct {
	ID          string    `json:"candidate_id"`
	Sources     []string  `json:"sources"`
	IP          string    `json:"ip"`
	Port        int       `json:"port"`
	Protocol    string    `json:"protocol"`
	DNSNames    []string  `json:"dns_names,omitempty"`
	Country     string    `json:"country,omitempty"`
	ASN         string    `json:"asn,omitempty"`
	ServiceHint string    `json:"service_hint,omitempty"`
	BannerHash  string    `json:"banner_hash,omitempty"`
	RiskScore   int       `json:"risk_score"`
	ObservedAt  time.Time `json:"observed_at"`
}

// CatalogKeys are the search-engine credentials. They are read from the
// environment only — a key never belongs in a config file that gets committed.
type CatalogKeys struct {
	CensysToken     string
	CensysID        string
	CensysSecret    string
	Shodan          string
	GreyNoise       string
	ZoomEye         string
	CriminalIP      string
	NetlasEndpoint  string
	Netlas          string
}

// CatalogConfig bounds the internet-search inventory scan.
type CatalogConfig struct {
	Enabled       bool          `yaml:"enabled"`
	Sources       []string      `yaml:"sources"`
	Query         string        `yaml:"query"`
	Timeout       time.Duration `yaml:"timeout"`
	MaxPerSource  int           `yaml:"max_per_source"`
	RetainEntries int           `yaml:"retain_entries"`
}

// ScanReport is the operator-facing result of one inventory scan.
type ScanReport struct {
	StartedAt time.Time         `json:"started_at"`
	Duration  string            `json:"duration"`
	Created   int               `json:"created"`
	Deduped   int               `json:"deduped"`
	Total     int               `json:"total"`
	BySource  map[string]int    `json:"by_source"`
	Errors    map[string]string `json:"errors,omitempty"`
}

// Catalog collects Ollama endpoints reported by internet search engines. It is
// a pure inventory: scanning queries the search APIs (with the operator's own
// keys) and stores what they report about other people's hosts. No candidate is
// ever dialed, verified or used to serve a task.
type Catalog struct {
	cfg       CatalogConfig
	keys      CatalogKeys
	endpoints CatalogEndpoints
	http      *http.Client
	log       *slog.Logger

	mu         sync.Mutex
	candidates map[string]*Candidate
	last       *ScanReport
}

// CatalogEndpoints holds the search API base URLs. Defaults point at the real
// services; tests override them to serve canned responses from an httptest
// server, which is how the parsing of each engine's document shape is covered.
type CatalogEndpoints struct {
	Censys     string
	CensysV2   string
	Shodan     string
	GreyNoise  string
	ZoomEye    string
	CriminalIP string
	Netlas     string
}

// DefaultCatalogEndpoints are the production search API URLs.
func DefaultCatalogEndpoints() CatalogEndpoints {
	return CatalogEndpoints{
		Censys:     "https://api.platform.censys.io/v3/global/search/query",
		CensysV2:   "https://search.censys.io/api/v2/hosts/search",
		Shodan:     "https://api.shodan.io/shodan/host/search",
		GreyNoise:  "https://api.greynoise.io/v2/experimental/gnql",
		ZoomEye:    "https://api.zoomeye.org/host/search",
		CriminalIP: "https://api.criminalip.io/v1/banner/search",
		Netlas:     "https://app.netlas.io/api",
	}
}

// NewCatalog builds an inventory catalog. Keys are resolved from the
// environment here so a config file never carries a credential.
func NewCatalog(cfg CatalogConfig, keys CatalogKeys, log *slog.Logger) *Catalog {
	if cfg.Query == "" {
		cfg.Query = "port:11434"
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.MaxPerSource <= 0 {
		cfg.MaxPerSource = 25
	}
	return &Catalog{
		cfg:        cfg,
		keys:       keys,
		endpoints:  DefaultCatalogEndpoints(),
		http:       &http.Client{Timeout: 15 * time.Second},
		log:        log,
		candidates: make(map[string]*Candidate),
	}
}

// WithEndpoints overrides the search API URLs, for tests.
func (c *Catalog) WithEndpoints(e CatalogEndpoints) *Catalog {
	c.endpoints = e
	return c
}

// configuredSources returns the search engines that are both enabled in config
// and actually carrying a key. A source without a key is silently skipped
// rather than reported as an error: the operator simply did not supply it.
func (c *Catalog) configuredSources() []string {
	available := map[string]bool{
		"censys":     c.keys.CensysToken != "",
		"shodan":     c.keys.Shodan != "",
		"greynoise":  c.keys.GreyNoise != "",
		"zoomeye":    c.keys.ZoomEye != "",
		"criminal_ip": c.keys.CriminalIP != "",
		"netlas":     c.keys.Netlas != "",
	}
	var out []string
	for _, s := range c.cfg.Sources {
		if available[s] {
			out = append(out, s)
		}
	}
	return out
}

// Scan queries every configured search engine and merges the results into the
// catalog. Each source is independent: one engine failing (bad key, outage,
// quota) never aborts the scan, it is recorded in the report's Errors. The scan
// itself never touches a candidate host — only the search APIs.
func (c *Catalog) Scan(ctx context.Context) ScanReport {
	start := time.Now()
	if !c.cfg.Enabled {
		return ScanReport{StartedAt: start, Duration: "0s", Errors: map[string]string{"catalog": "disabled"}}
	}
	sctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()

	sources := c.configuredSources()
	report := ScanReport{
		StartedAt: start,
		BySource:  map[string]int{},
		Errors:    map[string]string{},
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, src := range sources {
		wg.Add(1)
		go func(src string) {
			defer wg.Done()
			items, err := c.scanSource(sctx, src)
			mu.Lock()
			defer mu.Unlock()
			report.BySource[src] = len(items)
			if err != nil {
				report.Errors[src] = err.Error()
				return
			}
			created, deduped := c.merge(items)
			report.Created += created
			report.Deduped += deduped
		}(src)
	}
	wg.Wait()

	c.mu.Lock()
	report.Total = len(c.candidates)
	c.last = &report
	c.mu.Unlock()

	report.Duration = time.Since(start).Round(time.Millisecond).String()
	return report
}

// merge folds raw search results into the catalog, deduplicating by
// (ip, port, protocol) and unioning the sources and DNS names that reported a
// host (FOA §4.4.2). It also scores the candidate (FOA §4.4.3): the score is
// advisory metadata for the operator, never a gate the agent acts on. The
// returned counts feed the scan report's Created and Deduped fields.
func (c *Catalog) merge(items []rawItem) (created, deduped int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	for _, it := range items {
		key := catalogDedupKey(it.IP, it.Port, it.Protocol)
		if ex, ok := c.candidates[key]; ok {
			ex.Sources = union(ex.Sources, it.Source)
			ex.DNSNames = union(ex.DNSNames, it.DNSNames...)
			ex.ObservedAt = now
			deduped++
		} else {
			c.candidates[key] = &Candidate{
				ID:          fmt.Sprintf("cnd_%x", sha256.Sum256([]byte(key)))[:12],
				Sources:     []string{it.Source},
				IP:          it.IP,
				Port:        it.Port,
				Protocol:    orDefault(it.Protocol, "tcp"),
				DNSNames:    append([]string(nil), it.DNSNames...),
				Country:     orDefault(it.Country, "US"),
				ASN:         it.ASN,
				ServiceHint: orDefault(it.ServiceHint, "ollama"),
				BannerHash:  bannerHash(it.Banner),
				RiskScore:   riskScore(it),
				ObservedAt:  now,
			}
			created++
		}
	}
	return created, deduped
}

// riskScore reproduces the FOA §4.4.3 advisory scoring: a host with no DNS
// names and no ASN, flagged by an engine that also reports abuse, is riskier.
// The blacklist clause is omitted — this catalog carries no blacklist, and a
// candidate cannot be blacklisted because it was never contacted.
func riskScore(it rawItem) int {
	score := 15
	if len(it.DNSNames) == 0 {
		score += 10
	}
	if it.ASN == "" {
		score += 10
	}
	if it.Source == "criminal_ip" || it.Source == "greynoise" {
		score += 15
	}
	return score
}

func bannerHash(banner string) string {
	if banner == "" {
		return ""
	}
	h := sha256.Sum256([]byte(banner))
	return fmt.Sprintf("sha256:%x", h[:8])
}

// List returns a snapshot of the catalog for the admin API, newest first.
func (c *Catalog) List() []Candidate {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Candidate, 0, len(c.candidates))
	for _, cand := range c.candidates {
		out = append(out, *cand)
	}
	sortCandidatesByObserved(out)
	return out
}

// Get returns one candidate by its catalog id (cnd_<sha256[:12]>).
func (c *Catalog) Get(id string) (Candidate, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, cand := range c.candidates {
		if cand.ID == id {
			return *cand, true
		}
	}
	return Candidate{}, false
}

// mergeReceived folds one gossip-contributed candidate into the catalog,
// mirroring the scan merge's dedup semantics: same (ip, port, protocol) unions
// the sources and refreshes observed_at; a new key is added. Received entries
// keep their original risk score unless ours is higher — risk is advisory
// metadata, and the highest warning wins.
func (c *Catalog) mergeReceived(cand Candidate) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := catalogDedupKey(cand.IP, cand.Port, cand.Protocol)
	if ex, ok := c.candidates[key]; ok {
		ex.Sources = union(ex.Sources, cand.Sources...)
		ex.DNSNames = union(ex.DNSNames, cand.DNSNames...)
		if cand.ObservedAt.After(ex.ObservedAt) {
			ex.ObservedAt = cand.ObservedAt
		}
		if cand.RiskScore > ex.RiskScore {
			ex.RiskScore = cand.RiskScore
		}
		return 1
	}
	added := cand
	c.candidates[key] = &added
	return 1
}

// SetKeys replaces the search-engine credentials held in memory (the key
// exchange endpoint uses it; keys are never persisted to disk).
func (c *Catalog) SetKeys(keys CatalogKeys) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.keys = keys
}

// Keys returns a copy of the current search-engine credentials.
func (c *Catalog) Keys() CatalogKeys {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.keys
}

// LastScan returns the most recent scan report, or nil if no scan ran yet.
func (c *Catalog) LastScan() *ScanReport {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.last == nil {
		return nil
	}
	cp := *c.last
	return &cp
}

// Save persists the catalog as JSON so a restart keeps the inventory. The file
// holds only public internet metadata — no credentials, no usable endpoints.
func (c *Catalog) Save(path string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	list := make([]Candidate, 0, len(c.candidates))
	for _, cand := range c.candidates {
		list = append(list, *cand)
	}
	sortCandidatesByObserved(list)
	data, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

// Load restores a previously saved catalog.
func (c *Catalog) Load(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var list []Candidate
	if err := json.Unmarshal(data, &list); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.candidates = make(map[string]*Candidate, len(list))
	for i := range list {
		cand := list[i]
		c.candidates[catalogDedupKey(cand.IP, cand.Port, cand.Protocol)] = &cand
	}
	return nil
}

// scanSource dispatches to one engine's query function. It is the only place
// that knows a source name maps to a search API.
func (c *Catalog) scanSource(ctx context.Context, source string) ([]rawItem, error) {
	switch source {
	case "censys":
		return c.fetchCensys(ctx)
	case "shodan":
		return c.fetchShodan(ctx)
	case "greynoise":
		return c.fetchGreyNoise(ctx)
	case "zoomeye":
		return c.fetchZoomEye(ctx)
	case "criminal_ip":
		return c.fetchCriminalIP(ctx)
	case "netlas":
		return c.fetchNetlas(ctx)
	default:
		return nil, fmt.Errorf("brain: unknown catalog source %q", source)
	}
}

func (c *Catalog) getJSON(ctx context.Context, url string, headers map[string]string) (map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Catalog) postJSON(ctx context.Context, url string, body map[string]any, headers map[string]string) (map[string]any, error) {
	buf, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytesReader(buf))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

// --- JSON navigation helpers ----------------------------------------------

// The search engines return loosely-typed documents whose shapes drift between
// API versions; these accessors mirror the TS reference implementation's
// optional chaining — a missing field yields its zero value, not a panic.

func asObj(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return nil
}

func asStr(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func asStrSlice(v any) []string {
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, e := range arr {
		if s, ok := e.(string); ok && s != "" {
			out = append(out, s)
		}
	}
	return out
}

func asInt(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case string:
		var i int
		fmt.Sscanf(n, "%d", &i)
		return i
	}
	return 0
}

func orDefault(s, d string) string {
	if s == "" {
		return d
	}
	return s
}

func union(base []string, add ...string) []string {
	seen := make(map[string]bool, len(base))
	for _, s := range base {
		seen[s] = true
	}
	for _, s := range add {
		if !seen[s] {
			seen[s] = true
			base = append(base, s)
		}
	}
	return base
}

func sortCandidatesByObserved(out []Candidate) {
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1].ObservedAt.Before(out[j].ObservedAt); j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
}
