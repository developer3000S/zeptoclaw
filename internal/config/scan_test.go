package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestScanSection covers the agent environment sweep: the defaults must keep
// the background scanner off (it is opt-in), an enabled section must parse,
// and its bounds must validate.
func TestScanSection(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "node.yaml")
	write := func(body string) (*Config, error) {
		full := "node:\n  name: t\n  data_dir: " + filepath.Join(dir, "d") + "\n" + body
		if err := os.WriteFile(path, []byte(full), 0o600); err != nil {
			t.Fatal(err)
		}
		return Load(path)
	}

	d := Default()
	if d.Discovery.Scan.Enabled {
		t.Fatal("the background scan must be disabled by default")
	}
	if d.Discovery.Scan.SubnetScan {
		t.Fatal("the LAN subnet sweep must be disabled by default")
	}
	if d.Discovery.Scan.Interval.D() <= 0 {
		t.Fatal("the default scan interval must be positive")
	}
	if d.Discovery.Scan.DialTimeout.D() <= 0 || d.Discovery.Scan.Timeout.D() <= 0 {
		t.Fatal("scan timeouts must be positive by default")
	}
	if d.Discovery.Scan.MaxCandidates <= 0 {
		t.Fatal("the candidate cap must be positive by default")
	}
	if err := d.Validate(); err != nil {
		t.Fatalf("defaults must validate: %v", err)
	}

	cfg, err := write(`
discovery:
  scan:
    enabled: true
    interval: 5m
    subnet_scan: true
    dial_timeout: 4s
    timeout: 30s
    max_candidates: 40
`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("enabled scan must validate: %v", err)
	}
	sc := cfg.Discovery.Scan
	if !sc.Enabled || sc.Interval.D() != 5*time.Minute || !sc.SubnetScan ||
		sc.DialTimeout.D() != 4*time.Second || sc.Timeout.D() != 30*time.Second ||
		sc.MaxCandidates != 40 {
		t.Fatalf("section not parsed: %+v", sc)
	}

	// Enabled without an interval is useless — the ticker would never fire.
	// Load validates, so the bad config surfaces as a parse error.
	if _, err := write(`
discovery:
  scan:
    enabled: true
    interval: 0s
`); err == nil || !strings.Contains(err.Error(), "discovery.scan.interval") {
		t.Fatalf("enabled scan with zero interval must fail validation, got: %v", err)
	}

	// A dial timeout longer than the whole pass makes no sense.
	if _, err := write(`
discovery:
  scan:
    enabled: true
    interval: 5m
    dial_timeout: 60s
    timeout: 10s
`); err == nil || !strings.Contains(err.Error(), "dial_timeout") {
		t.Fatalf("dial_timeout > timeout must fail validation, got: %v", err)
	}

	// A negative candidate cap is nonsense.
	if _, err := write(`
discovery:
  scan:
    max_candidates: -1
`); err == nil || !strings.Contains(err.Error(), "max_candidates") {
		t.Fatalf("negative max_candidates must fail validation, got: %v", err)
	}

	// The scan is part of the discovery section, so changing it requires a
	// restart like the rest of it.
	next := Default()
	next.Discovery.Scan.Enabled = true
	diffs := d.Diff(next)
	found := false
	for _, r := range diffs.RequiresRestart {
		if strings.Contains(r, "discovery") {
			found = true
		}
	}
	if !found {
		t.Fatalf("changing the scan config must require restart, got %+v", diffs)
	}
}
