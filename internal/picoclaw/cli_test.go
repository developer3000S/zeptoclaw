package picoclaw

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/developer3000S/zeptoclaw/internal/config"
)

// fakePicoClaw writes an executable that answers with its own argv, one token
// per line, so a test can read back exactly what the mesh asked the CLI to run.
func fakePicoClaw(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "picoclaw")
	body := "#!/bin/sh\nif [ \"$1\" = \"version\" ]; then echo fake; exit 0; fi\nprintf '%s' '🦞'\nfor a in \"$@\"; do printf '%s\\n' \"$a\"; done\n"
	if err := os.WriteFile(path, []byte(body), 0o700); err != nil {
		t.Fatalf("write fake binary: %v", err)
	}
	return path
}

func runFake(t *testing.T, cfg config.PicoClawConfig, req Request) []string {
	t.Helper()
	cfg.Binary = fakePicoClaw(t)
	cfg.TimeoutSeconds = 20
	a, err := NewCLI(cfg, nil)
	if err != nil {
		t.Fatalf("NewCLI: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	if req.Instruction == "" {
		req.Instruction = "do the thing"
	}
	resp, err := a.Execute(context.Background(), req)
	if err != nil {
		diag := ""
		if resp != nil {
			diag = resp.Stderr
		}
		t.Fatalf("Execute: %v (stderr=%q)", err, diag)
	}
	return strings.Split(strings.TrimSpace(resp.Text), "\n")
}

// The node-level model of ТЗ 10.3 must reach the CLI as exactly one --model,
// ahead of the generic extra_args tail. `picoclaw agent` declares only
// -d/-m/-s/--model and rejects an unknown flag, so the mesh must not invent any
// other model argument.
func TestCLIModelArgs(t *testing.T) {
	cases := []struct {
		name    string
		cfg     config.PicoClawConfig
		req     Request
		want    []string
		notWant []string
	}{
		{
			name: "node default",
			cfg:  config.PicoClawConfig{Model: "llm-a"},
			want: []string{"agent", "-m", "do the thing", "--model", "llm-a"},
		},
		{
			name: "no model configured leaves the choice to picoclaw",
			cfg:  config.PicoClawConfig{},
			want: []string{"agent", "-m", "do the thing"},
		},
		{
			name: "per-request override replaces the node default",
			cfg:  config.PicoClawConfig{Model: "llm-a"},
			req:  Request{Model: "llm-b"},
			want: []string{"agent", "-m", "do the thing", "--model", "llm-b"},
		},
		{
			name: "extra args still come last",
			cfg: config.PicoClawConfig{
				Model:     "llm-a",
				ExtraArgs: []string{"--debug"},
			},
			want: []string{"agent", "-m", "do the thing", "--model", "llm-a", "--debug"},
		},
		{
			name:    "whitespace-only model is not a model",
			cfg:     config.PicoClawConfig{Model: "   "},
			want:    []string{"agent", "-m", "do the thing"},
			notWant: []string{"--model"},
		},
		{
			name:    "whitespace-only model with an empty request model emits nothing",
			cfg:     config.PicoClawConfig{Model: " \t "},
			req:     Request{Model: ""},
			notWant: []string{"--model"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := runFake(t, tc.cfg, tc.req)
			// got is: agent -m <prompt> [-s key] [--model x] <extra>
			want := tc.want
			if len(got) < len(want) {
				t.Fatalf("argv = %q, want prefix %q", got, want)
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("argv[%d] = %q, want %q (full argv %q)", i, got[i], want[i], got)
				}
			}
			joined := strings.Join(got, "\x00")
			for _, bad := range tc.notWant {
				if strings.Contains(joined, bad) {
					t.Fatalf("argv %q must not contain %q", got, bad)
				}
			}
			// Exactly one --model: a second one makes the CLI's choice undefined.
			var flags int
			for _, a := range got {
				if a == "--model" {
					flags++
				}
			}
			if flags > 1 {
				t.Fatalf("argv passes --model %d times: %q", flags, got)
			}
			// The prompt is one argument: a model value must never split it.
			if strings.Contains(got[2], "--model") {
				t.Fatalf("model leaked into the prompt argument: %q", got[2])
			}
		})
	}
}

// A configured session key precedes the model flags; -s and --model are both
// optional and their relative order is part of the documented contract.
func TestCLISessionAndModel(t *testing.T) {
	got := runFake(t, config.PicoClawConfig{Model: "llm-a"}, Request{SessionKey: "mesh:t1"})
	want := []string{"agent", "-m", "do the thing", "-s", "mesh:t1", "--model", "llm-a"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("argv[%d] = %q, want %q (full %q)", i, got[i], want[i], got)
		}
	}
}

func TestCLIModelReporter(t *testing.T) {
	a, err := NewCLI(config.PicoClawConfig{Model: " llm-a "}, nil)
	if err != nil {
		t.Fatalf("NewCLI: %v", err)
	}
	if got := a.Model(); got != "llm-a" {
		t.Fatalf("Model() = %q", got)
	}
	if got := ModelOf(a); got != "llm-a" {
		t.Fatalf("ModelOf(adapter) = %q", got)
	}
	// The stub and the gateway adapter cannot name a model, so the mesh reports
	// nothing instead of an unfounded value.
	if got := ModelOf(NewStub(config.StubConfig{}, nil)); got != "" {
		t.Fatalf("ModelOf(stub) = %q, want empty", got)
	}
}

// Template rendering and model flags must not interfere.
func TestCLIPromptTemplateWithModel(t *testing.T) {
	got := runFake(t, config.PicoClawConfig{Model: "llm-a", PromptTemplate: "ctx: %s"}, Request{Instruction: "hi"})
	want := []string{"agent", "-m", "ctx: hi", "--model", "llm-a"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("argv[%d] = %q, want %q (full %q)", i, got[i], want[i], got)
		}
	}
}
