//go:build playwright

package stage

import (
	"context"
	"os"
	"time"
)

// touchHealthFile periodically touches a file to signal container health.
// Docker healthcheck reads the file's mtime to determine if the worker is alive.
//
// Optional probes make the signal honest (Operational Invariant #7): if any
// probe reports false the file is NOT refreshed, so its mtime goes stale and
// the container healthcheck eventually fails. This lets a worker that is up but
// not doing useful work (e.g. reenrich whose eligibility query times out every
// loop — issue #28) surface to autoheal instead of reporting "healthy" forever.
// With no probes the behavior is unchanged: always healthy. Recovery is
// automatic — once the probes pass again the next tick refreshes the file.
func touchHealthFile(ctx context.Context, path string, probes ...func() bool) {
	healthy := func() bool {
		for _, p := range probes {
			if p != nil && !p() {
				return false
			}
		}
		return true
	}

	// Touch immediately on startup.
	if healthy() {
		os.WriteFile(path, []byte("ok"), 0644)
	}

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			os.Remove(path)
			return
		case <-ticker.C:
			if healthy() {
				os.WriteFile(path, []byte("ok"), 0644)
			}
		}
	}
}
