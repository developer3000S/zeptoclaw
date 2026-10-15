//go:build unix

package tasks

import (
	"syscall"
)

// diskFree reports the bytes available to the unprivileged user on the
// filesystem holding path.
func diskFree(path string) (uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, err
	}
	return st.Bavail * uint64(st.Bsize), nil
}
