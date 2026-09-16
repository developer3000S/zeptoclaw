package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExpandEnv(t *testing.T) {
	t.Setenv("ZT_TEST_A", "alpha")
	t.Setenv("ZT_TEST_EMPTY", "")

	cases := []struct {
		name string
		in   string
		want string
	}{
		{"plain", "no substitution", "no substitution"},
		{"set var", "${ZT_TEST_A}", "alpha"},
		{"missing var no default", "${ZT_TEST_MISSING}", ""},
		{"missing var with default", "${ZT_TEST_MISSING:-fallback}", "fallback"},
		{"empty var takes default", "${ZT_TEST_EMPTY:-fallback}", "fallback"},
		{"default containing braces", "${ZT_TEST_MISSING:-a{b}c}", "a{b}c"},
		{"adjacent expressions", "${ZT_TEST_A}-${ZT_TEST_MISSING:-x}", "alpha-x"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := expandEnv(tc.in); got != tc.want {
				t.Fatalf("expandEnv(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// The multi-instance template nests references:
// data_dir: ${ZETOMESH_DATA:-./zeptomesh-data}/${ZETOMESH_INDEX:-0}. The old
// first-'}' scan cut the expression early and leaked "}" into names.
func TestExpandEnvNestedDefaults(t *testing.T) {
	t.Setenv("ZETOMESH_DATA", "/srv/zm")
	t.Setenv("ZETOMESH_INDEX", "3")
	in := "name: ${ZETOMESH_NAME:-zepto-${ZETOMESH_INDEX:-0}}"
	want := "name: zepto-3"
	if got := expandEnv(in); got != want {
		t.Fatalf("expandEnv(%q) = %q, want %q", in, got, want)
	}
	in2 := "dir: ${ZETOMESH_DATA:-./zeptomesh-data}/${ZETOMESH_INDEX:-0}/keys"
	want2 := "dir: /srv/zm/3/keys"
	if got := expandEnv(in2); got != want2 {
		t.Fatalf("expandEnv(%q) = %q, want %q", in2, got, want2)
	}

	// With nothing set, both defaults apply inside out.
	os.Unsetenv("ZETOMESH_DATA")
	os.Unsetenv("ZETOMESH_INDEX")
	in3 := "${ZETOMESH_DATA:-./zeptomesh-data}/${ZETOMESH_INDEX:-0}"
	if got := expandEnv(in3); got != "./zeptomesh-data/0" {
		t.Fatalf("expandEnv(%q) = %q, want ./zeptomesh-data/0", in3, got)
	}
}

func TestLoadAppliesBootstrapFromEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "node.yaml")
	body := "node:\n  name: t\n  data_dir: " + filepath.Join(dir, "d") + "\n  listen: [\"/ip4/127.0.0.1/tcp/4101\"]\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ZETOMESH_BOOTSTRAP", "/ip4/10.0.0.1/tcp/4001/p2p/A, /dns4/b.example/tcp/4001/p2p/B")
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Discovery.Bootstrap) != 2 {
		t.Fatalf("bootstrap = %v, want 2 entries", cfg.Discovery.Bootstrap)
	}
	if cfg.Discovery.Bootstrap[0] != "/ip4/10.0.0.1/tcp/4001/p2p/A" {
		t.Fatalf("first bootstrap = %q", cfg.Discovery.Bootstrap[0])
	}
	if !strings.HasPrefix(cfg.Discovery.Bootstrap[1], "/dns4/") {
		t.Fatalf("second bootstrap = %q", cfg.Discovery.Bootstrap[1])
	}
}

// The shipped template must survive expansion with an empty environment:
// one broken expression makes every instance misconfigured the same way.
func TestDefaultTemplateExpands(t *testing.T) {
	for _, v := range []string{"ZETOMESH_DATA", "ZETOMESH_INDEX", "ZETOMESH_NAME", "ZETOMESH_MESH_PORT", "ZETOMESH_API_PORT", "ZETOMESH_PROM_LISTEN", "ZETOMESH_BOOTSTRAP", "ZETOMESH_PSK"} {
		t.Setenv(v, "")
	}
	tmpl := filepath.Join("..", "..", "configs", "node.yaml")
	raw, err := os.ReadFile(tmpl)
	if err != nil {
		t.Skipf("template not available: %v", err)
	}
	got := expandEnv(string(raw))
	if strings.Contains(got, "${") || strings.Contains(got, ":-") {
		for _, line := range strings.Split(got, "\n") {
			// The file's own comments document the syntax and contain literals.
			if strings.HasPrefix(strings.TrimSpace(line), "#") {
				continue
			}
			if strings.Contains(line, "${") || strings.Contains(line, ":-") {
				t.Fatalf("unexpanded reference remained: %q", line)
			}
		}
	}
}

// Every config we ship is documentation an operator is meant to copy, so a
// typo in a key or a value Normalize rejects is a real defect. Validate also
// normalises skill_exchange, so this is where those defaults get pinned.
func TestShippedConfigsLoadAndValidate(t *testing.T) {
	for _, v := range []string{"ZETOMESH_DATA", "ZETOMESH_INDEX", "ZETOMESH_NAME", "ZETOMESH_MESH_PORT",
		"ZETOMESH_API_PORT", "ZETOMESH_PROM_PORT", "ZETOMESH_PROM_LISTEN", "ZETOMESH_BOOTSTRAP",
		"ZETOMESH_PSK", "ZETOMESH_PICO_MODE", "ZETOMESH_PICO_BIN", "ZETOMESH_PICO_CONFIG",
		"ZETOMESH_PICO_WS_URL", "ZETOMESH_PICO_MODEL", "ZETOMESH_PICO_WORKSPACE", "ZETOMESH_NODES"} {
		t.Setenv(v, "")
	}
	dir := t.TempDir()
	t.Setenv("ZETOMESH_DATA", filepath.Join(dir, "data"))

	for _, name := range []string{"node.yaml", "examples/lan.yaml", "examples/wan.yaml", "examples/dev-node.yaml"} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join("..", "..", "configs", name)
			cfg, err := Load(path)
			if err != nil {
				t.Fatalf("Load(%s): %v", name, err)
			}
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate(%s): %v", name, err)
			}
			// Skill descriptors are only shareable if the exchange block parsed;
			// a silently dropped section would leave every node unable to sync.
			if !cfg.Capabilities.SkillExchange.Enabled {
				t.Fatalf("%s: skill_exchange.enabled must be on in the shipped config", name)
			}
			if got := cfg.Capabilities.SkillExchange.DiscloseTo; got != "trusted" && got != "known" && got != "any" {
				t.Fatalf("%s: disclose_to = %q", name, got)
			}
			if cfg.Capabilities.SkillExchange.MaxDescriptors <= 0 || cfg.Capabilities.SkillExchange.ImportLimit <= 0 {
				t.Fatalf("%s: descriptor budgets = %d/%d", name,
					cfg.Capabilities.SkillExchange.MaxDescriptors, cfg.Capabilities.SkillExchange.ImportLimit)
			}
			// Documenting a skill the node cannot serve would be a lie on the wire,
			// so the templates must keep skill_docs inside the advertisement.
			for _, d := range cfg.Capabilities.SkillDocs {
				if !containsFold(cfg.Capabilities.Skills, d.Name) {
					t.Fatalf("%s: skill_docs %q is not in capabilities.skills %v", name, d.Name, cfg.Capabilities.Skills)
				}
			}
		})
	}
}

// ТЗ 10.3: the model is a node property, so it has to survive both a literal
// yaml value and the ${VAR:-} form the shipped templates use.
func TestPicoClawModelConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "node.yaml")
	body := "node:\n  name: t\n  data_dir: " + filepath.Join(dir, "d") +
		"\npicoclaw:\n  mode: binary\n  model: llm-a\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.PicoClaw.Model != "llm-a" {
		t.Fatalf("model = %q", cfg.PicoClaw.Model)
	}

	t.Setenv("ZETOMESH_PICO_MODEL", "llm-env")
	body = "node:\n  name: t\n  data_dir: " + filepath.Join(dir, "d2") +
		"\npicoclaw:\n  model: ${ZETOMESH_PICO_MODEL:-}\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err = Load(path)
	if err != nil {
		t.Fatalf("Load with env: %v", err)
	}
	if cfg.PicoClaw.Model != "llm-env" {
		t.Fatalf("expanded model = %q", cfg.PicoClaw.Model)
	}
	// An unset variable must collapse to "no opinion", not the literal text.
	t.Setenv("ZETOMESH_PICO_MODEL", "")
	cfg, err = Load(path)
	if err != nil {
		t.Fatalf("Load with empty env: %v", err)
	}
	if cfg.PicoClaw.Model != "" {
		t.Fatalf("model with empty env = %q, want \"\"", cfg.PicoClaw.Model)
	}
}

func containsFold(hay []string, needle string) bool {
	for _, h := range hay {
		if strings.EqualFold(h, needle) {
			return true
		}
	}
	return false
}

// ТЗ 6.6.1 п.4: cron-declared triggers live in the node config and must be
// refused at startup when malformed — a typo in a schedule is otherwise a job
// that silently never runs.
func TestTriggersSectionParsesAndValidates(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "node.yaml")
	write := func(body string) (*Config, error) {
		full := "node:\n  name: t\n  data_dir: " + filepath.Join(dir, "d") + "\n" + body
		if err := os.WriteFile(path, []byte(full), 0o600); err != nil {
			t.Fatal(err)
		}
		return Load(path)
	}

	cfg, err := write(`
triggers:
  - id: nightly
    name: Ночная уборка
    schedule: "0 3 * * *"
    max_runs: 10
    job:
      instruction: "чистка"
      required_skills: [general]
      ttl: 5
      allow_network_tools: true
  - id: quiet
    enabled: false
    schedule: "*/15 * * * *"
    job:
      instruction: "ping"
`)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Triggers) != 2 {
		t.Fatalf("triggers = %d, want 2", len(cfg.Triggers))
	}
	dom := cfg.ConfigTriggers()
	if dom[0].ID != "nightly" || dom[0].MaxRuns != 10 || dom[0].Job.TTL != 5 {
		t.Fatalf("first trigger = %+v", dom[0])
	}
	if !dom[0].EnabledOrDefault() {
		t.Fatal("a trigger without an explicit enabled must default to on")
	}
	if dom[1].EnabledOrDefault() {
		t.Fatal("enabled: false must survive the conversion")
	}
	if dom[1].Job.Instruction != "ping" {
		t.Fatalf("job mapping = %+v", dom[1].Job)
	}

	if _, err := write("triggers:\n  - id: bad\n    schedule: \"61 * * * *\"\n    job:\n      instruction: x\n"); err == nil {
		t.Fatal("an out-of-range cron field must fail validation")
	} else if !strings.Contains(err.Error(), "minute") {
		t.Fatalf("error must name the broken field: %v", err)
	}
	if _, err := write("triggers:\n  - id: a\n    schedule: \"* * * * *\"\n    job:\n      instruction: x\n  - id: a\n    schedule: \"0 4 * * *\"\n    job:\n      instruction: y\n"); err == nil || !strings.Contains(err.Error(), "twice") {
		t.Fatalf("duplicate trigger ids must be refused: %v", err)
	}
	if _, err := write("triggers:\n  - schedule: \"* * * * *\"\n    job:\n      instruction: x\n"); err == nil || !strings.Contains(err.Error(), "id is required") {
		t.Fatalf("a trigger without an id must be refused: %v", err)
	}
	if _, err := write("triggers:\n  - id: empty\n    schedule: \"* * * * *\"\n    job: {}\n"); err == nil || !strings.Contains(err.Error(), "instruction") {
		t.Fatalf("an empty instruction must be refused: %v", err)
	}

	// Diff reports the section as hot, because ReloadConfig pushes the new list
	// through the scheduler's live owner.
	base := Default()
	base.Node.DataDir = dir
	next := Default()
	next.Node.DataDir = dir
	next.Triggers = []TriggerConfig{{ID: "nightly", Schedule: "0 3 * * *", Job: JobConfig{Instruction: "x"}}}
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := next.Validate(); err != nil {
		t.Fatal(err)
	}
	d := base.Diff(next)
	if !containsFold(d.Hot, "triggers") {
		t.Fatalf("Diff().Hot = %v, want triggers", d.Hot)
	}
}

// TestTracingSectionParsesAndValidates (ТЗ 14.3): the tracing block is optional
// and off by default; a malformed endpoint or ratio is refused only when it
// would actually be used — a disabled section must never stop a node.
func TestTracingSectionParsesAndValidates(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "node.yaml")
	write := func(body string) (*Config, error) {
		full := "node:\n  name: t\n  data_dir: " + filepath.Join(dir, "d") + "\n" + body
		if err := os.WriteFile(path, []byte(full), 0o600); err != nil {
			t.Fatal(err)
		}
		return Load(path)
	}

	// Absent section: defaults, disabled.
	cfg, err := write("")
	if err != nil {
		t.Fatalf("Load without tracing: %v", err)
	}
	if cfg.Telemetry.Tracing.Enabled {
		t.Fatal("tracing must be off unless configured")
	}
	if cfg.Telemetry.Tracing.ServiceName != "zeptomesh-node" {
		t.Fatalf("default service name = %q", cfg.Telemetry.Tracing.ServiceName)
	}

	cfg, err = write(`
telemetry:
  tracing:
    enabled: true
    endpoint: "localhost:4318"
    insecure: true
    sample_ratio: 0.25
    service_name: "zepto-a"
    environment: "staging"
`)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	tr := cfg.Telemetry.Tracing
	if !tr.Enabled || tr.Endpoint != "localhost:4318" || !tr.Insecure ||
		tr.SampleRatio != 0.25 || tr.ServiceName != "zepto-a" || tr.Environment != "staging" {
		t.Fatalf("tracing = %+v", tr)
	}

	// A disabled section is not validated at all: operators keep stale or
	// experimental values in the file without paying a startup failure.
	if _, err := write("telemetry:\n  tracing:\n    enabled: false\n    endpoint: \"http://bad/url\"\n    sample_ratio: 9\n"); err != nil {
		t.Fatalf("disabled tracing must not be validated: %v", err)
	}

	for _, body := range []string{
		"telemetry:\n  tracing:\n    enabled: true\n    endpoint: \"http://localhost:4318\"\n",
		"telemetry:\n  tracing:\n    enabled: true\n    endpoint: \"localhost:4318/v1/traces\"\n",
		"telemetry:\n  tracing:\n    enabled: true\n    endpoint: \"localhost:4318 localhost:4319\"\n",
		"telemetry:\n  tracing:\n    enabled: true\n    endpoint: \"localhost:\"\n",
	} {
		if _, err := write(body); err == nil || !strings.Contains(err.Error(), "telemetry.tracing.endpoint") {
			t.Fatalf("endpoint %q must be refused with a named error, got %v", body, err)
		}
	}
	for _, ratio := range []string{"-0.1", "1.5"} {
		body := "telemetry:\n  tracing:\n    enabled: true\n    sample_ratio: " + ratio + "\n"
		if _, err := write(body); err == nil || !strings.Contains(err.Error(), "sample_ratio") {
			t.Fatalf("sample_ratio %s must be refused: %v", ratio, err)
		}
	}

	// Turning tracing on requires a restart: the SDK installs globals once.
	base := Default()
	base.Node.DataDir = dir
	next := Default()
	next.Node.DataDir = dir
	next.Telemetry.Tracing.Enabled = true
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := next.Validate(); err != nil {
		t.Fatal(err)
	}
	d := base.Diff(next)
	if !containsFold(d.RequiresRestart, "telemetry.tracing") {
		t.Fatalf("Diff().RequiresRestart = %v, want telemetry.tracing", d.RequiresRestart)
	}
}
