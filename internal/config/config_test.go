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
