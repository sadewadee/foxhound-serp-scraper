//go:build playwright

package scraper

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/sadewadee/foxhound/fetch"

	"github.com/sadewadee/serp-scraper/internal/config"
)

// BrowserLifecycle tracks request counts and manages browser restarts
// with temp file cleanup and zombie process prevention.
type BrowserLifecycle struct {
	cfg               *config.Config
	requestCount      atomic.Int64
	restartCount      int
	pageReuseLimit    int
	browserReuseLimit int
	browserFactory    func(*config.Config) (*fetch.CamoufoxFetcher, error)
	label             string // "serp" or "enrich" for logging
}

// NewBrowserLifecycle creates a lifecycle manager.
// factory is the function to create a new browser (e.g. NewSERPBrowser or NewBrowserWithPool).
// PageReuseLimit and BrowserReuseLimit are read from cfg.Fetch.
func NewBrowserLifecycle(cfg *config.Config, factory func(*config.Config) (*fetch.CamoufoxFetcher, error), label string) *BrowserLifecycle {
	return &BrowserLifecycle{
		cfg:               cfg,
		pageReuseLimit:    cfg.Fetch.PageReuseLimit,
		browserReuseLimit: cfg.Fetch.BrowserReuseLimit,
		browserFactory:    factory,
		label:             label,
	}
}

// IncrementAndCheck increments the request counter and returns true if a
// browser restart is needed (request count has exceeded the page reuse limit).
func (bl *BrowserLifecycle) IncrementAndCheck() bool {
	count := bl.requestCount.Add(1)
	return count > int64(bl.pageReuseLimit)
}

// Restart closes the old browser, cleans temp files, and creates a new one.
// Returns the new browser or an error. The caller is responsible for replacing
// its browser pointer with the returned value.
func (bl *BrowserLifecycle) Restart(old *fetch.CamoufoxFetcher) (*fetch.CamoufoxFetcher, error) {
	bl.restartCount++
	isFullCleanup := bl.restartCount >= bl.browserReuseLimit

	slog.Info(fmt.Sprintf("%s: restarting browser", bl.label),
		"restart_count", bl.restartCount,
		"full_cleanup", isFullCleanup)

	// Close old browser.
	if old != nil {
		old.Close()
	}

	// Always kill orphan browser processes after Close() — child firefox
	// processes can outlive the parent camoufox and become zombies.
	killOrphanBrowserProcesses(bl.label)

	// Clean foxhound temp addon dirs.
	cleanTempAddonDirs()

	// Full cleanup: reset restart counter.
	if isFullCleanup {
		bl.restartCount = 0
	}

	// Reset request counter.
	bl.requestCount.Store(0)

	// Create new browser.
	browser, err := bl.browserFactory(bl.cfg)
	if err != nil {
		return nil, fmt.Errorf("%s: browser restart failed: %w", bl.label, err)
	}

	slog.Info(fmt.Sprintf("%s: browser restarted successfully", bl.label))
	return browser, nil
}

// Reset resets the request counter (call after a manual restart).
func (bl *BrowserLifecycle) Reset() {
	bl.requestCount.Store(0)
}

// cleanTempAddonDirs removes /tmp/foxhound-addon-* directories left behind
// by previous browser instances.
func cleanTempAddonDirs() {
	matches, err := filepath.Glob("/tmp/foxhound-addon-*")
	if err != nil {
		return
	}
	for _, dir := range matches {
		if err := os.RemoveAll(dir); err != nil {
			slog.Warn("lifecycle: failed to remove temp dir", "dir", dir, "error", err)
		} else {
			slog.Debug("lifecycle: removed temp dir", "dir", dir)
		}
	}
}

// killOrphanBrowserProcesses reads /proc to find firefox/camoufox processes
// whose parent is PID 1 (i.e. orphaned after our process restarted them) and
// sends SIGKILL to each one. No-ops on non-Linux systems where /proc is absent.
func killOrphanBrowserProcesses(label string) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return // Not on Linux, or /proc not mounted.
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		// Only look at numeric directory names (PIDs).
		pid := entry.Name()
		if !isAllDigits(pid) {
			continue
		}

		// Read /proc/<pid>/cmdline to identify browser processes.
		cmdlineBytes, err := os.ReadFile(filepath.Join("/proc", pid, "cmdline"))
		if err != nil {
			continue
		}
		// cmdline uses NUL bytes as argument separators; replace for easy search.
		cmd := strings.ToLower(strings.ReplaceAll(string(cmdlineBytes), "\x00", " "))
		if !strings.Contains(cmd, "camoufox") && !strings.Contains(cmd, "firefox") {
			continue
		}

		// Read /proc/<pid>/stat to check the parent PID.
		// Format: "pid (comm with spaces) S ppid ..."
		// comm can contain spaces and parens, so find last ')' first.
		statBytes, err := os.ReadFile(filepath.Join("/proc", pid, "stat"))
		if err != nil {
			continue
		}
		stat := string(statBytes)
		rp := strings.LastIndex(stat, ")")
		if rp < 0 || rp+2 >= len(stat) {
			continue
		}
		rest := strings.Fields(stat[rp+2:])
		// rest[0] = state, rest[1] = ppid
		if len(rest) < 2 || rest[1] != "1" {
			continue // Not an orphan (ppid != 1).
		}

		slog.Warn(fmt.Sprintf("%s: killing orphan browser process", label), "pid", pid)
		if pidInt, err := strconv.Atoi(pid); err == nil {
			if proc, err := os.FindProcess(pidInt); err == nil && proc != nil {
				proc.Kill()
			}
		}
	}
}

// isAllDigits returns true if s consists entirely of ASCII digit characters.
func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// atoiSafe converts a string of ASCII digits to int without importing strconv.
