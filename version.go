package main

import (
	"fmt"
	"runtime/debug"
)

// version is the release version. Bump it with every change that is deployed.
var version = "2.0.0"

// buildString describes the binary: version, VCS revision (with "-dirty" for
// a build from uncommitted changes, which should never be deployed) and the
// Go toolchain, all read from the build info Go embeds.
func buildString() string {
	rev, dirty, gover := "unknown", "", "unknown"
	if bi, ok := debug.ReadBuildInfo(); ok {
		gover = bi.GoVersion
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				rev = s.Value
				if len(rev) > 12 {
					rev = rev[:12]
				}
			case "vcs.modified":
				if s.Value == "true" {
					dirty = "-dirty"
				}
			}
		}
	}
	return fmt.Sprintf("hcbridge %s (rev %s%s, %s)", version, rev, dirty, gover)
}
