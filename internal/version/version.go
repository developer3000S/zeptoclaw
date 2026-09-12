// Package version holds build-time identity of the mesh daemon.
package version

import (
	"fmt"
	"runtime"
)

// Values injected via -ldflags "-X ...".
var (
	GitCommit = "dev"
	BuildDate = "unknown"
)

const (
	// Version is the semantic version of the mesh protocol implementation.
	Version = "0.1.0"
	// ProtocolVersion must change whenever the wire format changes incompatibly.
	ProtocolVersion = "0.1.0"
)

// String renders a human readable build stamp.
func String() string {
	return fmt.Sprintf("%s (proto %s, commit %s, built %s, %s/%s, %s)",
		Version, ProtocolVersion, GitCommit, BuildDate,
		runtime.GOOS, runtime.GOARCH, runtime.Version())
}
