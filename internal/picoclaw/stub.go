package picoclaw

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/zeptoclaw/zeptomesh/internal/config"
)

// StubAdapter is a deterministic executor used for tests, demos and meshes
// that have no model credentials yet. It never calls out to a network provider.
type StubAdapter struct {
	latency time.Duration
	echo    bool
	skills  []string

	mu       sync.Mutex
	inFlight int
	calls    int
}

// NewStub builds the offline adapter.
func NewStub(cfg config.StubConfig, skills []string) *StubAdapter {
	return &StubAdapter{
		latency: cfg.Latency.D(),
		echo:    cfg.Echo,
		skills:  append([]string(nil), skills...),
	}
}

// Name implements Adapter.
func (a *StubAdapter) Name() string { return "stub" }

// Healthy always succeeds.
func (a *StubAdapter) Healthy(context.Context) error { return nil }

// Capabilities returns the configured skill list.
func (a *StubAdapter) Capabilities(context.Context) ([]string, error) {
	return append([]string(nil), a.skills...), nil
}

// Execute sleeps for the configured latency and returns a canned answer.
func (a *StubAdapter) Execute(ctx context.Context, req Request) (*Response, error) {
	a.mu.Lock()
	a.calls++
	a.inFlight++
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		a.inFlight--
		a.mu.Unlock()
	}()

	started := time.Now().UTC()
	if a.latency > 0 {
		select {
		case <-time.After(a.latency):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrTimeout, err)
	}

	text := fmt.Sprintf("[zeptomesh-stub] task=%s skills=%v accepted", req.TaskID, req.Skills)
	if a.echo {
		text = fmt.Sprintf("%s\ninstruction: %s", text, strings.TrimSpace(req.Instruction))
	}
	// Writing a file proves the workspace plumbing end to end.
	var arts []Artifact
	if req.Workspace != "" {
		path := filepath.Join(req.Workspace, "result.txt")
		if err := os.MkdirAll(req.Workspace, 0o700); err == nil {
			if err := os.WriteFile(path, []byte(text), 0o600); err == nil {
				info, _ := os.Stat(path)
				size := int64(len(text))
				if info != nil {
					size = info.Size()
				}
				arts = append(arts, Artifact{Name: "result.txt", Path: path, Size: size})
			}
		}
	}
	return &Response{
		Text:       text,
		StartedAt:  started,
		FinishedAt: time.Now().UTC(),
		Artifacts:  arts,
		ExitCode:   0,
	}, nil
}

// Calls reports how many executions were served (used by tests).
func (a *StubAdapter) Calls() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls
}

// Running reports concurrent executions.
func (a *StubAdapter) Running() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.inFlight
}

// Close implements Adapter.
func (a *StubAdapter) Close() error { return nil }

var _ Adapter = (*StubAdapter)(nil)
