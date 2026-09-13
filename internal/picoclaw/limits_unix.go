//go:build unix

package picoclaw

import (
	"fmt"
	"os/exec"
	"syscall"
)

// prepareChild gives a PicoClaw child its own process group (so a hung agent
// cannot leak grandchildren when the mesh gives up) and a parent-death signal.
func prepareChild(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
	cmd.SysProcAttr.Pdeathsig = syscall.SIGKILL
}

// limitedCommand wraps the agent invocation so the child runs under
// RLIMIT_AS (ТЗ 6.8.4 "максимальный объем памяти процесса"): sh applies
// `ulimit -v` and exec-replaces itself, so no extra process lingers and the
// argv stays intact. maxMemory 0 returns the command unchanged.
func limitedCommand(maxMemory int64, binary string, args []string) (string, []string) {
	if maxMemory <= 0 {
		return binary, args
	}
	kb := (maxMemory + 1023) / 1024
	// "|| true": if the operator's hard limit is lower than the requested
	// soft limit, sh cannot raise it — run unbounded rather than fail the task.
	script := fmt.Sprintf("ulimit -v %d 2>/dev/null || true\nexec \"$@\"", kb)
	argv := make([]string, 0, len(args)+3)
	argv = append(argv, "-c", script, "zeptomesh-limit", binary)
	argv = append(argv, args...)
	return "/bin/sh", argv
}

// killChildGroup SIGKILLs the child's whole process group; used as the
// CommandContext cancel func so shell-spawned grandchildren die with it.
func killChildGroup(cmd *exec.Cmd) {
	if cmd.Process == nil {
		return
	}
	syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) //nolint:errcheck // already dead
}
