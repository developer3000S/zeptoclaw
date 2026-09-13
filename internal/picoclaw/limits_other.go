//go:build !unix

package picoclaw

import "os/exec"

// prepareChild: process-group and parent-death semantics exist only on Unix.
func prepareChild(cmd *exec.Cmd) {}

// limitedCommand has no ulimit here; the memory bound is not enforced off
// Unix and the operator keeps the timeout and concurrency limits.
func limitedCommand(maxMemory int64, binary string, args []string) (string, []string) {
	return binary, args
}

// killChildGroup falls back to killing the direct child.
func killChildGroup(cmd *exec.Cmd) {
	if cmd.Process != nil {
		cmd.Process.Kill() //nolint:errcheck // already dead
	}
}
