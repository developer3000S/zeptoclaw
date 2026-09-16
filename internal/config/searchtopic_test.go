package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestSearchTopicSection covers the epidemic search plane (ТЗ 6.9.5 п.5): the
// defaults must keep the plane off (it is the widest-blast-radius widening
// step), an enabled section must be parsed, and its bounds validated.
func TestSearchTopicSection(t *testing.T) {
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
	if d.Tasks.Forwarding.SearchRelay.Topic.Enabled {
		t.Fatal("the search topic must be disabled by default")
	}
	if !strings.HasPrefix(d.Tasks.Forwarding.SearchRelay.Topic.Name, "/") {
		t.Fatalf("default topic name %q must start with /", d.Tasks.Forwarding.SearchRelay.Topic.Name)
	}
	if err := d.Validate(); err != nil {
		t.Fatalf("defaults must validate: %v", err)
	}

	cfg, err := write(`
tasks:
  forwarding:
    search_relay:
      enabled: true
      topic:
        enabled: true
        name: "/zeptomesh/skillsearch/0.1.0"
        request_ttl: 45s
        max_answers: 12
        answer_cooldown: 5s
`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("enabled topic must validate: %v", err)
	}
	st := cfg.Tasks.Forwarding.SearchRelay.Topic
	if !st.Enabled || st.Name != "/zeptomesh/skillsearch/0.1.0" ||
		st.RequestTTL.D() != 45*time.Second || st.MaxAnswers != 12 ||
		st.AnswerCooldown.D() != 5*time.Second {
		t.Fatalf("section not parsed: %+v", st)
	}

	bad := []struct {
		name  string
		yaml  string
		slice string
	}{
		{"name without slash", `
tasks:
  forwarding:
    search_relay:
      topic:
        enabled: true
        name: "skillsearch"
`, "topic.name"},
		{"zero ttl", `
tasks:
  forwarding:
    search_relay:
      topic:
        enabled: true
        request_ttl: 0s
`, "request_ttl"},
		{"zero max_answers", `
tasks:
  forwarding:
    search_relay:
      topic:
        enabled: true
        max_answers: 0
`, "max_answers"},
		{"negative cooldown", `
tasks:
  forwarding:
    search_relay:
      topic:
        enabled: true
        answer_cooldown: -1s
`, "answer_cooldown"},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			// Load validates, so refusal surfaces there rather than at a
			// separate Validate call.
			_, err := write(tc.yaml)
			if err == nil || !strings.Contains(err.Error(), tc.slice) {
				t.Fatalf("Load err = %v, want it to name %q", err, tc.slice)
			}
		})
	}

	// A disabled section is not validated: operators keep the knobs around.
	offCfg, err := write(`
tasks:
  forwarding:
    search_relay:
      topic:
        enabled: false
        name: "garbage"
        max_answers: 0
`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if err := offCfg.Validate(); err != nil {
		t.Fatalf("disabled topic must not be validated: %v", err)
	}

	// Joining the topic happens once at startup, so a change requires restart.
	next := Default()
	next.Tasks.Forwarding.SearchRelay.Topic.Enabled = true
	diffs := d.Diff(next)
	found := false
	for _, r := range diffs.RequiresRestart {
		if strings.Contains(r, "tasks") {
			found = true
		}
	}
	if !found {
		t.Fatalf("enabling the search topic must require restart, got %+v", diffs)
	}
}
