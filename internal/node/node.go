// Package node assembles every mesh component into one running node and owns
// its lifecycle: start, maintenance, graceful stop.
package node

import "github.com/developer3000S/zeptoclaw/internal/tasks"

// SubmitRequest is re-exported so callers outside the tasks package (the admin
// API, the CLI) do not need to import it.
type SubmitRequest = tasks.SubmitRequest

// AddBackend adds a new backend to the node's backend pool
func (n *Node) AddBackend(backend brain.Backend) error {
	return n.brain.AddBackend(backend)
}

// Backends returns the current backend pool
func (n *Node) Backends() []brain.Backend {
	return n.brain.Backends()
}
