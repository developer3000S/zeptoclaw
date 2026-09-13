//go:build !unix

package tasks

import "errors"

// diskFree has no portable statfs implementation here; the node treats the
// disk guard as unavailable rather than blocking all work.
func diskFree(path string) (uint64, error) {
	return 0, errors.New("tasks: disk accounting unsupported")
}
