//go:build playwright

package stage

import (
	"context"
	"os"
	"time"
)

// touchHealthFile periodically touches a file to signal container health.
// Docker healthcheck reads the file's mtime to determine if the worker is alive.
func touchHealthFile(ctx context.Context, path string) {
	// Touch immediately on startup.
	os.WriteFile(path, []byte("ok"), 0644)

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			os.Remove(path)
			return
		case <-ticker.C:
			os.WriteFile(path, []byte("ok"), 0644)
		}
	}
}
