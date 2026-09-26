//go:build playwright

package stage

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/sadewadee/foxhound/fetch"

	"github.com/sadewadee/serp-scraper/internal/config"
	"github.com/sadewadee/serp-scraper/internal/dedup"
	"github.com/sadewadee/serp-scraper/internal/directory"
	internalScraper "github.com/sadewadee/serp-scraper/internal/scraper"
)

// DataDome directory fetching (Yelp, TripAdvisor).
//
// Callers must check c.dataDomeActive() before routing here: ModuleActive is
// true only when DIRECTORY_DATADOME_ENABLED=1 AND DIRECTORY_PROXY_URL is set,
// and an enabled flag with an empty proxy falls open to "off" with a logged
// warning.
//
// Nothing here has been exercised against a live DataDome challenge: both
// sites 403 every method available today, so the module ships disabled and
// the activation checklist in docs/directory-datadome.md governs switching it
// on. The retry/backoff shape mirrors the SearXNG suspension philosophy: a
// block burns at most one enrichment attempt per window, and the rest requeue
// with their attempt counter untouched.

// datadomeWarnOnce logs the "enabled but no proxy" misconfiguration once per
// process so every worker's job does not spam the log.
var datadomeWarnOnce sync.Once

// dataDomeActive reports whether directory fetches for DataDome sites should
// go through the residential proxy. It logs once when the flag is on but no
// proxy is configured (Invariant #7: fail-open paths must log).
func (c *EnrichStage) dataDomeActive() bool {
	active := directory.ModuleActive(c.cfg.Directory.DataDomeEnabled, c.cfg.Directory.ProxyURL)
	if !active && c.cfg.Directory.DataDomeEnabled {
		datadomeWarnOnce.Do(func() {
			slog.Warn("enrich: DIRECTORY_DATADOME_ENABLED=1 but DIRECTORY_PROXY_URL is empty — module stays off")
		})
	}
	return active
}

// newDataDomeBrowser builds the worker's pooled browser for DataDome sites. It
// routes through DIRECTORY_PROXY_URL (residential), never the enrich default.
// The caller creates one instance per worker (Invariant #5).
func newDataDomeBrowser(cfg *config.Config) (*fetch.CamoufoxFetcher, error) {
	if !directory.ModuleActive(cfg.Directory.DataDomeEnabled, cfg.Directory.ProxyURL) {
		return nil, fmt.Errorf("datadome module is not active (flag off or no proxy)")
	}
	// Point a config copy at the residential proxy so the shared browser
	// factory routes this browser through it.
	proxied := *cfg
	proxied.Proxy.URL = cfg.Directory.ProxyURL
	return internalScraper.NewBrowserWithPool(&proxied, 1)
}

// dataDomeSessionID derives a sticky session id for one directory URL so a
// solved datadome cookie can be reused on the same exit. Without
// DIRECTORY_PROXY_STICKY the session is per fetch.
func dataDomeSessionID(cfg *config.Config, pageURL string) string {
	if !cfg.Directory.ProxySticky {
		return fmt.Sprintf("ephemeral-%d", time.Now().UnixNano())
	}
	return "datadome-" + dedup.HashURL(pageURL)
}

// datadomeOutcome is what the worker does with one DataDome fetch result.
type datadomeOutcome int

const (
	// datadomeOK means the body is a usable page: hand it to the extractor.
	datadomeOK datadomeOutcome = iota
	// datadomeBlocked means the page is a challenge: the job row is already
	// settled by settleDataDomeBlock, and the worker must move to the next job
	// without treating this as a fetch error.
	datadomeBlocked
)

// handleDataDomeFetch fetches one directory URL through the module browser and
// folds a challenge into the right job-row update. On (body, datadomeOK, nil)
// the caller runs extraction. On ("", datadomeBlocked, nil) the job row is
// already settled and the caller just continues. A non-nil error means the
// module browser itself is broken; the caller should fail the job.
func handleDataDomeFetch(ctx context.Context, c *EnrichStage, browser *fetch.CamoufoxFetcher, pageURL, domain, urlHash string) (string, datadomeOutcome, error) {
	if browser == nil {
		return "", datadomeOK, fmt.Errorf("datadome browser is nil")
	}
	timeout := time.Duration(c.cfg.Enrich.TimeoutMs) * time.Millisecond
	fetchCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	body, err := internalScraper.FetchWithBrowserString(fetchCtx, browser, pageURL, pageURL)
	if err != nil {
		// A transport error from the browser fetch is a genuine failure; the
		// caller's generic retry path handles it.
		return "", datadomeOK, err
	}

	if _, blocked := directory.DetectChallenge(200, nil, []byte(body)); !blocked {
		if st := datadomeStateFor(domain); st != nil {
			st.RecordSolved()
		}
		return body, datadomeOK, nil
	}

	// One challenge per window may cost a single attempt; the rest requeue
	// with the counter untouched (same philosophy as a SearXNG suspension).
	now := time.Now()
	deadline, firstInWindow := datadomeStateFor(domain).RecordBlock(now)
	slog.Warn("enrich: datadome challenge — backing off site",
		"url", pageURL, "domain", domain,
		"first_in_window", firstInWindow,
		"backed_off_until", deadline.Format(time.Kitchen))
	if firstInWindow {
		c.db.Exec(`UPDATE enrichment_jobs SET status = 'failed', attempt_count = attempt_count + 1, error_msg = $1, updated_at = NOW() WHERE url_hash = $2`,
			"datadome challenge unsolved", urlHash)
	} else {
		c.db.Exec(`UPDATE enrichment_jobs SET status = 'new', next_attempt_at = NOW() + ($1 || ' seconds')::interval,
			error_msg = $2, locked_by = NULL, picked_at = NULL, updated_at = NOW() WHERE url_hash = $3`,
			fmt.Sprintf("%d", int(time.Until(deadline).Seconds())+1), "datadome site in backoff window", urlHash)
	}
	return "", datadomeBlocked, nil
}

// datadomeStateFor returns the per-site backoff state. Sites are few and
// fixed, so a small package registry keyed by domain is enough.
var datadomeStates = struct {
	mu sync.Mutex
	m  map[string]*directory.DataDomeBackoff
}{m: map[string]*directory.DataDomeBackoff{}}

func datadomeStateFor(domain string) *directory.DataDomeBackoff {
	datadomeStates.mu.Lock()
	defer datadomeStates.mu.Unlock()
	st, ok := datadomeStates.m[domain]
	if !ok {
		st = &directory.DataDomeBackoff{}
		datadomeStates.m[domain] = st
	}
	return st
}
