// Package health provides startup diagnostics for the serp-scraper process.
// It checks that browser dependencies (Camoufox Firefox base, NopeCHA) are
// reasonably current and logs a WARN when they drift too far behind upstream.
package health

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// mozVersionURL is the Mozilla product-details endpoint for current Firefox versions.
	mozVersionURL = "https://product-details.mozilla.org/1.0/firefox_versions.json"

	// mozVersionCacheKey is the Redis key where the fetched Firefox version is cached.
	mozVersionCacheKey = "health:firefox_latest_version"

	// mozVersionCacheTTL is how long the fetched Firefox version is cached in Redis.
	// 24 h is sufficient — Firefox releases are not more frequent than weekly.
	mozVersionCacheTTL = 24 * time.Hour

	// staleDays is the threshold in calendar days beyond which a Camoufox Firefox
	// base is considered stale relative to the current upstream Firefox release.
	staleDays = 30

	// httpTimeout is the maximum time to spend fetching the Mozilla endpoint.
	httpTimeout = 10 * time.Second
)

// mozVersionsResponse is the relevant subset of the Mozilla product-details JSON.
type mozVersionsResponse struct {
	LatestFirefoxVersion string `json:"LATEST_FIREFOX_VERSION"`
}

// CheckCamoufoxStaleness detects the Camoufox browser's embedded Firefox version,
// fetches the current upstream Firefox release (cached in Redis for 24 h), and
// logs an INFO or WARN message at startup.
//
// It is best-effort: any error (Mozilla endpoint down, camoufox not installed,
// Redis unavailable) is logged at DEBUG and execution continues normally.
//
// Call this once from cmd/run.go after Redis is ready.
func CheckCamoufoxStaleness(ctx context.Context, rdb *redis.Client) {
	localFF, err := detectCamoufoxFirefoxVersion(ctx)
	if err != nil {
		slog.Debug("health: cannot detect Camoufox Firefox version", "error", err)
		return
	}

	upstreamFF, err := currentFirefoxVersion(ctx, rdb)
	if err != nil {
		// Best-effort: log what we know, skip the comparison.
		slog.Info("health: Camoufox Firefox base detected (upstream check unavailable)",
			"camoufox_ff", localFF,
			"error", err,
		)
		return
	}

	localMajor := parseMajor(localFF)
	upstreamMajor := parseMajor(upstreamFF)

	if localMajor == 0 || upstreamMajor == 0 {
		slog.Info("health: Camoufox Firefox base detected",
			"camoufox_ff", localFF,
			"upstream_ff", upstreamFF,
		)
		return
	}

	// Approximate staleness: each major Firefox release is roughly 4 weeks.
	// One major version gap ≈ 28 days; we warn at >staleDays (≈ >1 major behind).
	approxDays := (upstreamMajor - localMajor) * 28

	if approxDays > staleDays {
		slog.Warn("health: Camoufox Firefox base is stale — consider bumping CAMOUFOX_VERSION in Dockerfile",
			"camoufox_ff", localFF,
			"upstream_ff", upstreamFF,
			"approx_days_behind", approxDays,
			"threshold_days", staleDays,
		)
	} else {
		slog.Info("health: Camoufox Firefox base is current",
			"camoufox_ff", localFF,
			"upstream_ff", upstreamFF,
			"approx_days_behind", approxDays,
		)
	}
}

// detectCamoufoxFirefoxVersion runs `python3 -m camoufox version` and parses
// the Firefox version from its output.
//
// Camoufox prints lines like:
//
//	Camoufox 0.4.11 (Firefox 135.0.1)
//
// We extract the Firefox version from that line.
func detectCamoufoxFirefoxVersion(ctx context.Context) (string, error) {
	cmdCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	out, err := exec.CommandContext(cmdCtx, "python3", "-m", "camoufox", "version").CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("camoufox version: %w (output: %s)", err, strings.TrimSpace(string(out)))
	}

	return parseCamoufoxVersionOutput(string(out))
}

// versionLineRe matches "Firefox 135.0.1" or "Firefox 135.0.1-beta.24" anywhere in a line.
var versionLineRe = regexp.MustCompile(`(?i)firefox\s+([\d]+[\d.\-a-zA-Z]*)`)

// parseCamoufoxVersionOutput extracts the Firefox version string from camoufox
// version command output.  It is exported for unit testing.
func parseCamoufoxVersionOutput(output string) (string, error) {
	m := versionLineRe.FindStringSubmatch(output)
	if len(m) < 2 {
		return "", fmt.Errorf("could not find Firefox version in camoufox output: %q", output)
	}
	return strings.TrimRight(m[1], ".-"), nil
}

// currentFirefoxVersion returns the latest upstream Firefox release version.
// Results are cached in Redis for mozVersionCacheTTL to avoid hammering Mozilla.
func currentFirefoxVersion(ctx context.Context, rdb *redis.Client) (string, error) {
	// Try Redis cache first.
	if rdb != nil {
		cached, err := rdb.Get(ctx, mozVersionCacheKey).Result()
		if err == nil && cached != "" {
			return cached, nil
		}
	}

	// Fetch from Mozilla.
	version, err := fetchMozillaFirefoxVersion(ctx)
	if err != nil {
		return "", err
	}

	// Cache result.
	if rdb != nil {
		if setErr := rdb.Set(ctx, mozVersionCacheKey, version, mozVersionCacheTTL).Err(); setErr != nil {
			slog.Debug("health: failed to cache Firefox version in Redis", "error", setErr)
		}
	}

	return version, nil
}

// fetchMozillaFirefoxVersion fetches the current Firefox release version from
// Mozilla's product-details API.
func fetchMozillaFirefoxVersion(ctx context.Context) (string, error) {
	httpCtx, cancel := context.WithTimeout(ctx, httpTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(httpCtx, http.MethodGet, mozVersionURL, nil)
	if err != nil {
		return "", fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("User-Agent", "serp-scraper/health-check")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetch %s: %w", mozVersionURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("fetch %s: HTTP %d", mozVersionURL, resp.StatusCode)
	}

	var payload mozVersionsResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", fmt.Errorf("decode Mozilla versions: %w", err)
	}

	if payload.LatestFirefoxVersion == "" {
		return "", fmt.Errorf("Mozilla API returned empty LATEST_FIREFOX_VERSION")
	}

	return payload.LatestFirefoxVersion, nil
}

// parseMajor extracts the major version integer from a version string like
// "135.0.1" or "135.0.1-beta.24".  Returns 0 on parse failure.
func parseMajor(version string) int {
	parts := strings.SplitN(version, ".", 2)
	if len(parts) == 0 {
		return 0
	}
	// Strip any pre-release suffix from the major component (e.g. "135-beta").
	major := strings.Split(parts[0], "-")[0]
	n, err := strconv.Atoi(major)
	if err != nil {
		return 0
	}
	return n
}
