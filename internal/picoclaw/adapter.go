// Package picoclaw adapts the mesh node to a locally installed PicoClaw agent.
//
// PicoClaw exposes two integration surfaces that an external process can drive
// (verified against github.com/sipeed/picoclaw):
//
//  1. CLI: `picoclaw agent -m "<prompt>"` runs one turn non-interactively,
//     prints the answer to stdout and exits 0 (success) or 1 (any failure).
//     Configuration and state are relocated with PICOCLAW_CONFIG / PICOCLAW_HOME
//     / PICOCLAW_AGENTS_DEFAULTS_WORKSPACE — there are no --config/--workspace
//     flags.
//  2. Pico Protocol: a WebSocket channel served by the gateway at /pico/ws
//     (default port 18790), authenticated with a bearer token. There is no REST
//     endpoint that accepts a prompt.
//
// Both surfaces are implemented here behind one interface, plus a deterministic
// stub so a mesh can be exercised without any model credentials.
package picoclaw

import (
	"context"
	"errors"
	"time"
)

// ErrUnavailable reports that the local agent cannot take work right now.
var ErrUnavailable = errors.New("picoclaw: adapter unavailable")

// ErrTimeout reports that the agent exceeded its deadline.
var ErrTimeout = errors.New("picoclaw: execution timeout")

// Request is one unit of work handed to the local agent.
type Request struct {
	TaskID string
	// Instruction is the prompt text delivered to the agent.
	Instruction string
	// Skills are the mesh-level capabilities the task requires. The adapter
	// maps them onto PicoClaw tools/skills where a mapping is configured.
	Skills []string
	// Workspace is a directory this task may write into. It is created by the
	// manager and removed according to the retention policy.
	Workspace string
	// Home is the per-task PICOCLAW_HOME, isolating sessions and state.
	Home string
	// AllowShell and AllowNetwork are the mesh-side constraints; the adapter
	// refuses to run a task whose demands exceed them.
	AllowShell   bool
	AllowNetwork bool
	// Timeout bounds the whole execution.
	Timeout time.Duration
	// MaxWorkspaceBytes caps the total size of artifacts collected from
	// Workspace (ТЗ 6.8.4 "максимальный объем дискового пространства"). Files
	// beyond the quota are not returned as artifacts. 0 = unlimited.
	MaxWorkspaceBytes int64
	// MaxMemoryBytes caps the child process address space via RLIMIT_AS
	// (ТЗ 6.8.4 "максимальный объем памяти процесса"). CLI mode only, 0 = no
	// limit: the gateway mode has no child process to bound here.
	MaxMemoryBytes int64
	// Model optionally overrides the configured model.
	Model string
	// SessionKey isolates conversation state for this task.
	SessionKey string
}

// Response is what the agent produced.
type Response struct {
	Text       string
	StartedAt  time.Time
	FinishedAt time.Time
	// Artifacts lists files created inside Request.Workspace.
	Artifacts []Artifact
	// ExitCode is the process exit status for the CLI mode (0 on success).
	ExitCode int
	// Stderr carries diagnostics; it is logged, never returned to the mesh.
	Stderr string
	// Model reports which model answered, when the agent disclosed it.
	Model string
}

// Artifact is a file produced during execution.
type Artifact struct {
	Name string
	Path string
	Size int64
}

// Adapter executes tasks with a local PicoClaw instance.
type Adapter interface {
	// Name identifies the implementation in status output.
	Name() string
	// Execute runs one task to completion. Implementations must honour
	// ctx cancellation and return ErrTimeout on deadline expiry.
	Execute(ctx context.Context, req Request) (*Response, error)
	// Healthy reports whether the agent is usable right now.
	Healthy(ctx context.Context) error
	// Capabilities lists the skills this adapter can actually serve.
	Capabilities(ctx context.Context) ([]string, error)
	// Close releases resources.
	Close() error
}

// Info describes the adapter for the admin API.
type Info struct {
	Name    string    `json:"name"`
	Healthy bool      `json:"healthy"`
	Version string    `json:"version,omitempty"`
	Checked time.Time `json:"checked,omitempty"`
	Detail  string    `json:"detail,omitempty"`
}

// Describe runs Healthy and packages the result for status output.
func Describe(ctx context.Context, a Adapter) Info {
	info := Info{Name: a.Name()}
	err := a.Healthy(ctx)
	info.Healthy = err == nil
	if err != nil {
		info.Detail = err.Error()
	}
	info.Checked = time.Now().UTC()
	return info
}
