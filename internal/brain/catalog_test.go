package brain

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// testCatalog wires a catalog to an httptest server standing in for every
// search engine at once, so a scan touches no real network and no candidate
// host. The server also counts hits so a test can assert which sources ran.
func testCatalog(t *testing.T, keys CatalogKeys, body map[string]any) (*Catalog, *httptest.Server, *int) {
	t.Helper()
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.Header().Set("Content-Type", "application/json")
		_ = jsonWrite(w, body)
	}))
	t.Cleanup(srv.Close)

	cat := NewCatalog(CatalogConfig{
		Enabled:      true,
		Sources:      []string{"censys", "shodan", "greynoise", "zoomeye", "criminal_ip", "netlas"},
		MaxPerSource: 25,
	}, keys, slog.New(slog.DiscardHandler))
	cat.WithEndpoints(CatalogEndpoints{
		Censys:     srv.URL + "/censys",
		CensysV2:   srv.URL + "/censys-v2",
		Shodan:     srv.URL + "/shodan",
		GreyNoise:  srv.URL + "/greynoise",
		ZoomEye:    srv.URL + "/zoomeye",
		CriminalIP: srv.URL + "/criminalip",
		Netlas:     srv.URL + "/netlas",
	})
	return cat, srv, &hits
}

// shodanDoc is the document shape the Shodan host/search API returns, with the
// fields the parser reads.
func shodanDoc() map[string]any {
	return map[string]any{
		"matches": []any{
			map[string]any{
				"ip_str": "203.0.113.10", "port": 11434, "transport": "tcp",
				"hostnames": []any{"gpu-fra.example.de"},
				"location":  map[string]any{"country_code": "DE"},
				"asn":       "AS24940",
				"data":      "Ollama is running",
			},
			map[string]any{
				"ip_str": "198.51.100.7", "port": 11434,
				"location": map[string]any{"country_code": "US"},
			},
		},
	}
}

func TestScan_ParsesShodan(t *testing.T) {
	cat, _, hits := testCatalog(t, CatalogKeys{Shodan: "k"}, shodanDoc())
	rep := cat.Scan(context.Background())
	if *hits == 0 {
		t.Fatal("scan never queried the search API")
	}
	cands := cat.List()
	if len(cands) != 2 {
		t.Fatalf("expected 2 candidates, got %d: %+v", len(cands), cands)
	}
	var got *Candidate
	for i := range cands {
		if cands[i].IP == "203.0.113.10" {
			got = &cands[i]
		}
	}
	if got == nil {
		t.Fatalf("shodan candidate 203.0.113.10 missing: %+v", cands)
	}
	if got.Port != 11434 || got.Protocol != "tcp" {
		t.Fatalf("bad port/proto: %d/%q", got.Port, got.Protocol)
	}
	if got.Country != "DE" || got.ASN != "AS24940" {
		t.Fatalf("bad geo: %q %q", got.Country, got.ASN)
	}
	if len(got.DNSNames) != 1 || got.DNSNames[0] != "gpu-fra.example.de" {
		t.Fatalf("bad dns: %v", got.DNSNames)
	}
	if got.BannerHash == "" {
		t.Fatal("banner was not hashed")
	}
	if rep.BySource["shodan"] != 2 {
		t.Fatalf("by_source shodan = %d, want 2", rep.BySource["shodan"])
	}
}

// TestScan_ParsesAllSources feeds each engine one document shaped like its real
// API and asserts the parser pulled the host out of every one. A parser that
// silently returns nothing for one engine would otherwise look like "the engine
// just found nothing".
func TestScan_ParsesAllSources(t *testing.T) {
	keys := CatalogKeys{
		CensysToken: "t", Shodan: "k", GreyNoise: "g", ZoomEye: "z",
		CriminalIP: "c", Netlas: "n",
	}
	// One host per engine, each on a distinct address so dedup keeps them all.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		var doc map[string]any
		switch {
		case strings.Contains(path, "shodan"):
			doc = map[string]any{"matches": []any{map[string]any{"ip_str": "203.0.113.10", "port": 11434}}}
		case strings.Contains(path, "greynoise"):
			doc = map[string]any{"data": []any{map[string]any{"ip": "203.0.113.11", "metadata": map[string]any{"country_code": "FR"}}}}
		case strings.Contains(path, "zoomeye"):
			doc = map[string]any{"matches": []any{map[string]any{"ip": "203.0.113.12", "portinfo": map[string]any{"port": 11434}}}}
		case strings.Contains(path, "criminalip"):
			doc = map[string]any{"data": map[string]any{"result": []any{map[string]any{"ip_address": "203.0.113.13", "open_port_no": 11434}}}}
		case strings.Contains(path, "netlas"):
			doc = map[string]any{"items": []any{map[string]any{"data": map[string]any{"ip": "203.0.113.14", "port": 11434}}}}
		default: // censys v3
			doc = map[string]any{"result": map[string]any{"hits": []any{map[string]any{"ip": "203.0.113.15"}}}}
		}
		_ = jsonWrite(w, doc)
	}))
	t.Cleanup(srv.Close)

	cat := NewCatalog(CatalogConfig{
		Enabled: true,
		Sources: []string{"censys", "shodan", "greynoise", "zoomeye", "criminal_ip", "netlas"},
	}, keys, slog.New(slog.DiscardHandler))
	cat.WithEndpoints(CatalogEndpoints{
		Censys: srv.URL + "/censys", CensysV2: srv.URL + "/censys-v2",
		Shodan: srv.URL + "/shodan", GreyNoise: srv.URL + "/greynoise",
		ZoomEye: srv.URL + "/zoomeye", CriminalIP: srv.URL + "/criminalip",
		Netlas: srv.URL + "/netlas",
	})

	cat.Scan(context.Background())
	cands := cat.List()
	if len(cands) != 6 {
		t.Fatalf("expected one candidate per source (6), got %d", len(cands))
	}
	seen := map[string]bool{}
	for _, c := range cands {
		seen[c.Sources[0]] = true
	}
	for _, want := range []string{"censys", "shodan", "greynoise", "zoomeye", "criminal_ip", "netlas"} {
		if !seen[want] {
			t.Errorf("source %q produced no candidate", want)
		}
	}
}

// TestScan_DedupesAcrossSources: the same host reported by two engines is one
// candidate whose Sources list both (FOA §4.4.2).
func TestScan_DedupesAcrossSources(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var doc map[string]any
		switch {
		case strings.Contains(r.URL.Path, "shodan"):
			doc = map[string]any{"matches": []any{map[string]any{"ip_str": "203.0.113.20", "port": 11434}}}
		default: // censys
			doc = map[string]any{"result": map[string]any{"hits": []any{map[string]any{"ip": "203.0.113.20"}}}}
		}
		_ = jsonWrite(w, doc)
	}))
	t.Cleanup(srv.Close)

	cat := NewCatalog(CatalogConfig{
		Enabled: true, Sources: []string{"censys", "shodan"},
	}, CatalogKeys{CensysToken: "t", Shodan: "k"}, slog.New(slog.DiscardHandler))
	cat.WithEndpoints(CatalogEndpoints{
		Censys: srv.URL + "/censys", Shodan: srv.URL + "/shodan",
	})

	rep := cat.Scan(context.Background())
	cands := cat.List()
	if len(cands) != 1 {
		t.Fatalf("duplicate host should collapse to 1 candidate, got %d", len(cands))
	}
	if len(cands[0].Sources) != 2 {
		t.Fatalf("merged candidate should carry both sources, got %v", cands[0].Sources)
	}
	if rep.Deduped != 1 {
		t.Fatalf("report.Deduped = %d, want 1", rep.Deduped)
	}
}

// TestScan_SourceErrorIsNotFatal: one engine returning an error must not abort
// the scan or drop the results of the engines that worked.
func TestScan_SourceErrorIsNotFatal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "shodan") {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_ = jsonWrite(w, map[string]any{"result": map[string]any{"hits": []any{map[string]any{"ip": "203.0.113.30"}}}})
	}))
	t.Cleanup(srv.Close)

	cat := NewCatalog(CatalogConfig{
		Enabled: true, Sources: []string{"censys", "shodan"},
	}, CatalogKeys{CensysToken: "t", Shodan: "k"}, slog.New(slog.DiscardHandler))
	cat.WithEndpoints(CatalogEndpoints{Censys: srv.URL + "/censys", Shodan: srv.URL + "/shodan"})

	rep := cat.Scan(context.Background())
	if _, ok := rep.Errors["shodan"]; !ok {
		t.Fatalf("shodan failure should be recorded, errors = %v", rep.Errors)
	}
	if rep.Total == 0 {
		t.Fatal("censys results were lost because shodan failed")
	}
}

// TestScan_NoKeyNoSource: a source without a configured key is skipped rather
// than reported as an error — the operator simply did not supply one.
func TestScan_NoKeyNoSource(t *testing.T) {
	cat, _, hits := testCatalog(t, CatalogKeys{}, shodanDoc())
	rep := cat.Scan(context.Background())
	if *hits != 0 {
		t.Fatal("a source with no key must not be queried")
	}
	if len(rep.Errors) != 0 {
		t.Fatalf("missing keys should be silent, got errors %v", rep.Errors)
	}
}

// TestScan_Disabled: a disabled catalog scans nothing and reports disabled.
func TestScan_Disabled(t *testing.T) {
	cat, _, hits := testCatalog(t, CatalogKeys{Shodan: "k"}, shodanDoc())
	cat.cfg.Enabled = false
	rep := cat.Scan(context.Background())
	if *hits != 0 {
		t.Fatal("a disabled catalog must not query anything")
	}
	if rep.Errors["catalog"] != "disabled" {
		t.Fatalf("disabled catalog should say so, got %v", rep.Errors)
	}
}

// TestScan_NeverTouchesCandidates is the invariant that makes the catalog safe:
// scanning may only reach the search APIs. A candidate address that answers is
// proof of a violation, so the test serves a candidate endpoint and asserts the
// scan never connects to it.
func TestScan_NeverTouchesCandidates(t *testing.T) {
	var candidateHits int
	candSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		candidateHits++
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(candSrv.Close)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The search engine reports the *candidate's* address as a hit.
		_ = jsonWrite(w, map[string]any{"matches": []any{map[string]any{
			"ip_str": serverHost(candSrv), "port": serverPort(candSrv),
		}}})
	}))
	t.Cleanup(srv.Close)

	cat := NewCatalog(CatalogConfig{Enabled: true, Sources: []string{"shodan"}},
		CatalogKeys{Shodan: "k"}, slog.New(slog.DiscardHandler))
	cat.WithEndpoints(CatalogEndpoints{Shodan: srv.URL + "/shodan"})

	cat.Scan(context.Background())
	if candidateHits != 0 {
		t.Fatalf("catalog contacted a candidate host %d time(s); the inventory must never touch them", candidateHits)
	}
	for _, c := range cat.List() {
		if c.Sources == nil {
			t.Error("candidate has no source attribution")
		}
	}
}

// TestRiskScore reproduces the FOA §4.4.3 advisory scoring: bare IP, no ASN and
// an abuse-reporting engine each add to the score.
func TestRiskScore(t *testing.T) {
	if got := riskScore(rawItem{IP: "1.2.3.4", Source: "shodan", ASN: "AS1", DNSNames: []string{"a"}}); got != 15 {
		t.Errorf("fully-attributed host = %d, want 15", got)
	}
	if got := riskScore(rawItem{IP: "1.2.3.4", Source: "shodan"}); got != 35 {
		t.Errorf("bare host (no dns, no asn) = %d, want 35", got)
	}
	if got := riskScore(rawItem{IP: "1.2.3.4", Source: "greynoise", ASN: "AS1", DNSNames: []string{"a"}}); got != 30 {
		t.Errorf("greynoise host = %d, want 30", got)
	}
}

func TestCatalogDedupKey(t *testing.T) {
	if got := catalogDedupKey("1.2.3.4", 11434, ""); got != "1.2.3.4:11434:tcp" {
		t.Errorf("empty protocol should default to tcp, got %q", got)
	}
	if a, b := catalogDedupKey("1.2.3.4", 11434, "tcp"), catalogDedupKey("1.2.3.4", 11434, "tcp"); a != b {
		t.Fatalf("same host must key alike: %q vs %q", a, b)
	}
}

// TestCatalog_RoundTrip exercises persistence: save then load must reproduce the
// catalog, dedup key included.
func TestCatalog_RoundTrip(t *testing.T) {
	cat, _, _ := testCatalog(t, CatalogKeys{Shodan: "k"}, shodanDoc())
	cat.Scan(context.Background())
	path := t.TempDir() + "/catalog.json"
	if err := cat.Save(path); err != nil {
		t.Fatalf("save: %v", err)
	}

	cat2 := NewCatalog(CatalogConfig{}, CatalogKeys{}, slog.New(slog.DiscardHandler))
	if err := cat2.Load(path); err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(cat2.List()) != len(cat.List()) {
		t.Fatalf("round trip lost candidates: %d -> %d", len(cat.List()), len(cat2.List()))
	}
}
