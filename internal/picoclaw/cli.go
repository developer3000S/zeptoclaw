package picoclaw

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/zeptoclaw/zeptomesh/internal/config"
)

// Logo is the emoji PicoClaw prefixes to every CLI answer.
const Logo = "\U0001F99E"

// CLIAdapter drives `picoclaw agent -m "<prompt>"` as a child process.
//
// Contract relied upon (verified against the upstream source):
//   - success: the answer is printed to stdout, exit code 0
//   - failure: a message is printed to stderr, exit code 1
//   - config/state relocation is env-only: PICOCLAW_CONFIG, PICOCLAW_HOME,
//     PICOCLAW_AGENTS_DEFAULTS_WORKSPACE
type CLIAdapter struct {
	binary   string
	cfgFile  string
	env      []string
	template string
	extra    []string
	timeout  time.Duration
	log      *slog.Logger

	mu       sync.Mutex
	inFlight int
	slots    chan struct{}
}

// NewCLI builds a CLI adapter from configuration.
func NewCLI(cfg config.PicoClawConfig, logger *slog.Logger) (*CLIAdapter, error) {
	binary := strings.TrimSpace(cfg.Binary)
	if binary == "" {
		binary = "picoclaw"
	}
	// A bare name is resolved through PATH at exec time; a path must exist now.
	if strings.ContainsRune(binary, os.PathSeparator) {
		if _, err := os.Stat(binary); err != nil {
			return nil, fmt.Errorf("picoclaw: binary %s: %w", binary, err)
		}
	}
	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	slots := cfg.MaxConcurrentAgents
	if slots <= 0 {
		slots = 1
	}
	env := make([]string, 0, len(cfg.Env))
	for k, v := range cfg.Env {
		env = append(env, k+"="+v)
	}
	return &CLIAdapter{
		binary:   binary,
		cfgFile:  cfg.ConfigFile,
		env:      env,
		template: cfg.PromptTemplate,
		timeout:  timeout,
		extra:    append([]string(nil), cfg.ExtraArgs...),
		log:      logger,
		slots:    make(chan struct{}, slots),
	}, nil
}

// Name implements Adapter.
func (a *CLIAdapter) Name() string { return "picoclaw-cli" }

// Healthy verifies the binary is present and answers `version`.
func (a *CLIAdapter) Healthy(ctx context.Context) error {
	if _, err := exec.LookPath(a.binary); err != nil {
		return fmt.Errorf("%w: %s not found in PATH", ErrUnavailable, a.binary)
	}
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(cctx, a.binary, "version")
	cmd.Env = a.baseEnv(Request{})
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%w: %s version failed: %v (%s)", ErrUnavailable, a.binary, err, truncate(out.String(), 200))
	}
	return nil
}

// Capabilities cannot be introspected from the CLI, so the adapter reports the
// skills it was configured to serve; the node policy is the real gate.
func (a *CLIAdapter) Capabilities(context.Context) ([]string, error) { return nil, nil }

// Execute runs one agent turn.
func (a *CLIAdapter) Execute(ctx context.Context, req Request) (*Response, error) {
	if strings.TrimSpace(req.Instruction) == "" {
		return nil, errors.New("picoclaw: empty instruction")
	}
	select {
	case a.slots <- struct{}{}:
		defer func() { <-a.slots }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	a.track(1)
	defer a.track(-1)

	timeout := req.Timeout
	if timeout <= 0 {
		timeout = a.timeout
	}
	ectx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	args := []string{"agent", "-m", a.renderPrompt(req)}
	if req.SessionKey != "" {
		args = append(args, "-s", req.SessionKey)
	}
	if req.Model != "" {
		args = append(args, "--model", req.Model)
	}
	args = append(args, a.extra...)

	cmd := exec.CommandContext(ectx, a.binary, args...)
	cmd.Dir = req.Workspace
	cmd.Env = a.baseEnv(req)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	started := time.Now().UTC()
	runErr := cmd.Run()
	finished := time.Now().UTC()

	resp := &Response{
		StartedAt:  started,
		FinishedAt: finished,
		Stderr:     stderr.String(),
	}
	if exitErr := new(exec.ExitError); errors.As(runErr, &exitErr) {
		resp.ExitCode = exitErr.ExitCode()
	} else if runErr != nil && !errors.Is(runErr, context.DeadlineExceeded) {
		resp.ExitCode = -1
	}

	arts, artErr := collectArtifacts(req.Workspace)
	if artErr != nil && a.log != nil {
		a.log.Debug("picoclaw_artifact_scan", "task_id", req.TaskID, "err", artErr.Error())
	}
	resp.Artifacts = arts

	if errors.Is(runErr, context.DeadlineExceeded) {
		return resp, fmt.Errorf("%w after %s", ErrTimeout, timeout)
	}
	if runErr != nil {
		return resp, fmt.Errorf("picoclaw: agent exited %d: %s", resp.ExitCode, sanitize(stderr.String()))
	}
	resp.Text = ExtractAnswer(stdout.String())
	if strings.TrimSpace(resp.Text) == "" {
		return resp, fmt.Errorf("picoclaw: agent produced no answer (exit %d)", resp.ExitCode)
	}
	return resp, nil
}

// Close is a no-op; child processes are tied to their context.
func (a *CLIAdapter) Close() error { return nil }

// Running reports concurrent executions, for the load metric.
func (a *CLIAdapter) Running() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.inFlight
}

func (a *CLIAdapter) track(delta int) {
	a.mu.Lock()
	a.inFlight += delta
	a.mu.Unlock()
}

func (a *CLIAdapter) renderPrompt(req Request) string {
	if a.template == "" {
		return req.Instruction
	}
	if !strings.Contains(a.template, "%s") {
		return a.template + " " + req.Instruction
	}
	return fmt.Sprintf(a.template, req.Instruction)
}

// baseEnv assembles the child environment. Per-task PICOCLAW_HOME keeps
// sessions and memory out of other tasks' way.
func (a *CLIAdapter) baseEnv(req Request) []string {
	env := append([]string(nil), os.Environ()...)
	env = append(env, a.env...)
	if a.cfgFile != "" {
		env = setEnv(env, "PICOCLAW_CONFIG", a.cfgFile)
	}
	if req.Home != "" {
		env = setEnv(env, "PICOCLAW_HOME", req.Home)
	}
	if req.Workspace != "" {
		env = setEnv(env, "PICOCLAW_AGENTS_DEFAULTS_WORKSPACE", req.Workspace)
	}
	// The CLI paints a banner and uses colours for humans; a mesh node wants
	// the least noisy stdout it can get.
	env = setEnv(env, "NO_COLOR", "1")
	env = setEnv(env, "TERM", "dumb")
	return env
}

func setEnv(env []string, key, val string) []string {
	out := make([]string, 0, len(env)+1)
	prefix := key + "="
	for _, e := range env {
		if strings.HasPrefix(e, prefix) {
			continue
		}
		out = append(out, e)
	}
	return append(out, prefix+val)
}

// ExtractAnswer strips the startup banner and the logo prefix from CLI stdout.
//
// PicoClaw prints an ASCII banner, then "\n🦞 <answer>\n". When the answer
// itself contains the logo we keep everything after the first occurrence,
// because the banner never contains it.
func ExtractAnswer(out string) string {
	s := strings.ReplaceAll(out, "\r\n", "\n")
	if i := strings.Index(s, Logo); i >= 0 {
		s = s[i+len(Logo):]
		return strings.TrimSpace(s)
	}
	lines := strings.Split(strings.TrimSpace(s), "\n")
	kept := make([]string, 0, len(lines))
	for _, l := range lines {
		if isBannerLine(l) {
			continue
		}
		kept = append(kept, l)
	}
	return strings.TrimSpace(strings.Join(kept, "\n"))
}

// isBannerLine recognises the decorative and informational noise the CLI emits
// before the answer: the ASCII logo block and the timezone notice.
func isBannerLine(l string) bool {
	t := strings.TrimSpace(l)
	if t == "" {
		return false
	}
	if strings.Contains(t, "█") || strings.Contains(t, "▀") || strings.Contains(t, "▄") {
		return true
	}
	for _, p := range []string{"PICOCLAW", "TZ environment:", "ZONEINFO environment:", "Debug mode enabled"} {
		if strings.HasPrefix(t, p) {
			return true
		}
	}
	return false
}

func collectArtifacts(root string) ([]Artifact, error) {
	if root == "" {
		return nil, nil
	}
	var out []Artifact
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // a vanished temp file is not a failure
		}
		if info.IsDir() {
			// PicoClaw keeps its own state under the workspace; it is not output.
			base := filepath.Base(path)
			if base == "sessions" || base == "memory" || base == "logs" || base == "state" {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(path, ".tmp") {
			return nil
		}
		rel, rerr := filepath.Rel(root, path)
		if rerr != nil {
			rel = filepath.Base(path)
		}
		out = append(out, Artifact{Name: filepath.ToSlash(rel), Path: path, Size: info.Size()})
		return nil
	})
	if err != nil {
		return out, err
	}
	return out, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// sanitize keeps secrets out of logs and error strings.
func sanitize(s string) string {
	l := strings.ToLower(s)
	for _, marker := range []string{"api_key", "apikey", "token", "secret", "password", "authorization"} {
		if i := strings.Index(l, marker); i >= 0 {
			end := i + len(marker) + 24
			if end > len(s) {
				end = len(s)
			}
			s = s[:i] + "***" + s[end:]
			l = strings.ToLower(s)
		}
	}
	return truncate(strings.TrimSpace(s), 512)
}

var _ Adapter = (*CLIAdapter)(nil)
