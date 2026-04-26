//go:build playwright

package scraper

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	foxhound "github.com/sadewadee/foxhound"
	"github.com/sadewadee/foxhound/behavior"
	"github.com/sadewadee/foxhound/fetch"
	"github.com/sadewadee/foxhound/identity"
	"github.com/sadewadee/foxhound/middleware"

	"github.com/sadewadee/serp-scraper/internal/config"
)

// refererForURL returns a realistic Referer for an enrich/contact-page fetch.
// Mimics the natural click chain: a user on the site's homepage clicks a link
// to the contact/about page. Without a Referer header every request looks like
// a cold cold-start direct navigation, which is itself a bot signal.
func refererForURL(targetURL string) string {
	u, err := url.Parse(targetURL)
	if err != nil || u.Host == "" {
		return ""
	}
	scheme := u.Scheme
	if scheme == "" {
		scheme = "https"
	}
	return scheme + "://" + u.Host + "/"
}

// refererHeader builds an http.Header containing only a Referer entry, or
// returns nil when the URL is unparseable.
func refererHeader(targetURL string) http.Header {
	ref := refererForURL(targetURL)
	if ref == "" {
		return nil
	}
	return http.Header{"Referer": []string{ref}}
}

// engineHomepageReferer returns the SERP engine homepage to use as Referer
// for page-2+ stealth requests. For page-1 the Referer is already realistic
// from the engine's own navigation; this is mainly for paginated fetches.
func engineHomepageReferer(serpURL string) http.Header {
	u, err := url.Parse(serpURL)
	if err != nil || u.Host == "" {
		return nil
	}
	return http.Header{"Referer": []string{u.Scheme + "://" + u.Host + "/"}}
}

// NewSERPBrowser creates a Camoufox browser optimized for Google SERP scraping.
// Uses persistent session, page pooling, and geo-matched identity.
func NewSERPBrowser(cfg *config.Config) (*fetch.CamoufoxFetcher, error) {
	profile := identity.Generate(identity.WithBrowser(identity.BrowserFirefox))
	bp := behavior.CarefulProfile().Jitter()

	headless := "virtual"
	if !cfg.Fetch.Headless {
		headless = "false"
	}

	browserTimeout := time.Duration(cfg.Fetch.BrowserTimeoutSec) * time.Second

	opts := []fetch.CamoufoxOption{
		fetch.WithBrowserIdentity(profile),
		fetch.WithHeadless(headless),
		fetch.WithBrowserTimeout(browserTimeout),
		fetch.WithMaxBrowserRequests(cfg.Fetch.SERPMaxRequests / 2),
		fetch.WithBehaviorProfile(bp),
		// No WithUserDataDir — persistent context serializes all page operations
		// through a single Playwright channel, killing concurrency.
		// Use StorageState for cookie/session persistence across restarts instead.
		fetch.WithStorageState("/home/scraper/.sessions/serp-session.json"),
		fetch.WithBrowserCookies(googleConsentCookies()),
	}
	if cfg.Proxy.URL != "" {
		opts = append(opts, fetch.WithBrowserProxy(cfg.Proxy.URL))
	}

	browser, err := fetch.NewCamoufox(opts...)
	if err != nil {
		return nil, fmt.Errorf("fetcher: creating serp browser: %w", err)
	}
	return browser, nil
}

// NewSERPBrowserWithPool creates a Camoufox browser for SERP scraping with a
// pre-warmed tab pool sized to match the concurrency level. All tab workers
// share a single browser process — each goroutine acquires a pool slot, fetches,
// then releases. MaxBrowserRequests is set to 200 to match the lifecycle limit.
func NewSERPBrowserWithPool(cfg *config.Config, poolSize int) (*fetch.CamoufoxFetcher, error) {
	// Random identity (no hardcoded country — diverse profiles avoid clustering).
	profile := identity.Generate(identity.WithBrowser(identity.BrowserFirefox))

	headless := "virtual"
	if !cfg.Fetch.Headless {
		headless = "false"
	}

	// Jittered CarefulProfile — max stealth mouse/scroll/keyboard, ±15% per session.
	bp := behavior.CarefulProfile().Jitter()

	browserTimeout := time.Duration(cfg.Fetch.BrowserTimeoutSec) * time.Second

	opts := []fetch.CamoufoxOption{
		fetch.WithBrowserIdentity(profile),
		fetch.WithHeadless(headless),
		// NO BlockImages for SERP — Google detects missing subresource loading.
		// Real browsers always load images; blocking is a bot signal.
		fetch.WithBrowserTimeout(browserTimeout),
		fetch.WithMaxBrowserRequests(cfg.Fetch.SERPMaxRequests),
		// Anti-detection (foxhound v0.0.10):
		fetch.WithBehaviorProfile(bp),                    // careful mouse/scroll/keyboard
		fetch.WithBrowserCookies(googleConsentCookies()), // skip consent banner
		// Page pool: each tab worker gets its own isolated BrowserContext+Page.
		// WithUserDataDir uses LaunchPersistentContext which funnels ALL pages
		// through a single context — Playwright serializes operations and one
		// slow/stuck page blocks every other goroutine. Pool mode gives true
		// concurrency with per-slot context isolation.
		fetch.WithPoolSize(poolSize),
		fetch.WithPageReuseLimit(150),
		fetch.WithStorageState("/home/scraper/.sessions/serp-session.json"),
		// NopeCHA extension stays enabled — v0.0.10 fixes the solve gate.
		// Extension now actually solves reCAPTCHA/Turnstile when encountered.
	}
	if cfg.Proxy.URL != "" {
		opts = append(opts, fetch.WithBrowserProxy(cfg.Proxy.URL))
	}

	browser, err := fetch.NewCamoufox(opts...)
	if err != nil {
		return nil, fmt.Errorf("fetcher: creating serp browser with pool: %w", err)
	}
	return browser, nil
}

// NewBrowser creates a Camoufox browser for website scraping fallback.
// poolSize controls concurrent tabs (0 = create/destroy context per request).
// For shared use by multiple goroutines, set poolSize >= number of goroutines.
func NewBrowser(cfg *config.Config) (*fetch.CamoufoxFetcher, error) {
	return NewBrowserWithPool(cfg, 0)
}

// NewBrowserWithPool creates a Camoufox browser with pre-warmed page pool.
// Each pool slot is a BrowserContext+Page pair — concurrent Fetch() calls
// acquire a slot, navigate, then release. No context creation overhead after warmup.
//
// Architecture:
//
//	1 browser process → N pooled tabs (N = poolSize)
//	Multiple goroutines call Fetch() concurrently
//	Each gets a pre-warmed tab from the pool
//	Cookies/state cleared between uses (no session bleed)
func NewBrowserWithPool(cfg *config.Config, poolSize int) (*fetch.CamoufoxFetcher, error) {
	profile := identity.Generate(identity.WithBrowser(identity.BrowserFirefox))
	bp := behavior.ModerateProfile().Jitter() // moderate for enrich (faster, less stealth needed)

	headless := "virtual"
	if !cfg.Fetch.Headless {
		headless = "false"
	}

	enrichTimeout := time.Duration(cfg.Fetch.BrowserTimeoutSec/2) * time.Second
	if enrichTimeout < 15*time.Second {
		enrichTimeout = 15 * time.Second
	}

	opts := []fetch.CamoufoxOption{
		fetch.WithBrowserIdentity(profile),
		fetch.WithHeadless(headless),
		fetch.WithBlockImages(cfg.Fetch.BlockImages),
		fetch.WithBrowserTimeout(enrichTimeout),
		fetch.WithMaxBrowserRequests(cfg.Fetch.EnrichMaxRequests),
		// Anti-detection:
		fetch.WithBehaviorProfile(bp),
		fetch.WithPageReuseLimit(150),
		fetch.WithStorageState("/tmp/enrich-session.json"),
	}
	if poolSize > 0 {
		opts = append(opts, fetch.WithPoolSize(poolSize))
	}
	if cfg.Proxy.URL != "" {
		opts = append(opts, fetch.WithBrowserProxy(cfg.Proxy.URL))
	}

	browser, err := fetch.NewCamoufox(opts...)
	if err != nil {
		return nil, fmt.Errorf("fetcher: creating browser: %w", err)
	}
	return browser, nil
}

// NewStealth creates a stealth HTTP fetcher for website scraping.
// Real JA3/JA4 + HTTP/2 fingerprint impersonation requires the binary to be
// built with `-tags tls` (see Dockerfile); without it, foxhound falls back to
// stealth_default.go which uses Go's standard net/http transport — headers are
// browser-like but the TLS ClientHello is Go's default and trivially detected.
//
// Geo-match: when PROXY_COUNTRY is set, the identity profile is constrained
// to that country so locale, timezone, and Accept-Language match the proxy
// exit IP. A US Firefox profile exiting via a DE proxy is itself a bot signal.
func NewStealth(cfg *config.Config) *fetch.StealthFetcher {
	idOpts := []identity.Option{}
	if cfg.Proxy.Country != "" {
		idOpts = append(idOpts, identity.WithCountry(cfg.Proxy.Country))
	}
	profile := identity.Generate(idOpts...)

	opts := []fetch.StealthOption{
		fetch.WithIdentity(profile),
		fetch.WithTimeout(time.Duration(cfg.Enrich.TimeoutMs) * time.Millisecond),
	}
	if cfg.Proxy.URL != "" {
		opts = append(opts, fetch.WithProxy(cfg.Proxy.URL))
	}
	return fetch.NewStealth(opts...)
}

// FetchSERP fetches a Google SERP page with consent banner handling.
// Tries with consent-click steps first, falls back to plain fetch if steps fail.
func FetchSERP(ctx context.Context, browser *fetch.CamoufoxFetcher, serpURL, jobID string) ([]byte, error) {
	job := &foxhound.Job{
		ID:        jobID,
		URL:       serpURL,
		Method:    "GET",
		FetchMode: foxhound.FetchBrowser,
		Steps: []foxhound.JobStep{
			// Click Google consent "Accept all" button if present.
			{Action: foxhound.JobStepClick, Selector: "button#L2AGLb", Optional: true},
			{Action: foxhound.JobStepClick, Selector: "button[jsname='higCR']", Optional: true},
			// Wait for results — optional so we still get content even if captcha blocks.
			{Action: foxhound.JobStepWait, Selector: "div#search", Duration: 10 * time.Second, Optional: true},
		},
	}

	resp, err := browser.Fetch(ctx, job)
	if err != nil {
		// Fallback: fetch without steps in case steps caused issues.
		slog.Debug("serp: fetch with steps failed, retrying plain", "error", err)
		return FetchWithBrowser(ctx, browser, serpURL, jobID+"-plain")
	}
	if resp == nil {
		return nil, fmt.Errorf("nil response")
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return resp.Body, nil
}

// FetchPage tries stealth first, falls back to browser on failure/block.
func FetchPage(ctx context.Context, stealth *fetch.StealthFetcher, browser *fetch.CamoufoxFetcher, pageURL, jobID string) (string, error) {
	referer := refererHeader(pageURL)
	resp, err := stealth.Fetch(ctx, &foxhound.Job{
		ID:      jobID,
		URL:     pageURL,
		Method:  "GET",
		Headers: referer,
	})

	if err != nil || resp == nil || resp.StatusCode >= 400 {
		slog.Debug("fetch: stealth failed, trying browser",
			"url", pageURL, "err", err)

		resp, err = browser.Fetch(ctx, &foxhound.Job{
			ID:        "browser-" + jobID,
			URL:       pageURL,
			Method:    "GET",
			FetchMode: foxhound.FetchBrowser,
			Headers:   referer,
		})
		if err != nil {
			return "", fmt.Errorf("fetch failed: %w", err)
		}
	}

	if resp == nil {
		return "", fmt.Errorf("nil response")
	}
	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return string(resp.Body), nil
}

// FetchSERPWithEngine fetches a SERP page using browser with engine-specific steps.
// Used by engines that need browser but have different steps than Google.
func FetchSERPWithEngine(ctx context.Context, browser *fetch.CamoufoxFetcher, serpURL, jobID string, steps []foxhound.JobStep) ([]byte, error) {
	job := &foxhound.Job{
		ID:        jobID,
		URL:       serpURL,
		Method:    "GET",
		FetchMode: foxhound.FetchBrowser,
		Steps:     steps,
	}
	resp, err := browser.Fetch(ctx, job)
	if err != nil {
		return FetchWithBrowser(ctx, browser, serpURL, jobID+"-plain")
	}
	if resp == nil {
		return nil, fmt.Errorf("nil response")
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return resp.Body, nil
}

// FetchSERPStealth fetches a SERP page via stealth HTTP (no browser).
// Used by engines that do not require a full browser (e.g. DuckDuckGo).
func FetchSERPStealth(ctx context.Context, stealth *fetch.StealthFetcher, serpURL, jobID string) ([]byte, error) {
	resp, err := stealth.Fetch(ctx, &foxhound.Job{
		ID:      jobID,
		URL:     serpURL,
		Method:  "GET",
		Headers: engineHomepageReferer(serpURL),
	})
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("nil response")
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return resp.Body, nil
}

// FetchWithBrowser fetches a page using only the browser.
func FetchWithBrowser(ctx context.Context, browser *fetch.CamoufoxFetcher, pageURL, jobID string) ([]byte, error) {
	resp, err := browser.Fetch(ctx, &foxhound.Job{
		ID:        jobID,
		URL:       pageURL,
		Method:    "GET",
		FetchMode: foxhound.FetchBrowser,
	})
	if err != nil {
		return nil, fmt.Errorf("browser fetch: %w", err)
	}
	if resp == nil {
		return nil, fmt.Errorf("nil response")
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return resp.Body, nil
}

// NewSERPBrowserDirect creates a Camoufox browser WITHOUT proxy for direct SERP fetching.
// Used as fallback when proxy is blocked by search engines.
// Only 1 tab — rate-limited to protect server IP.
func NewSERPBrowserDirect(cfg *config.Config) (*fetch.CamoufoxFetcher, error) {
	profile := identity.Generate(identity.WithBrowser(identity.BrowserFirefox))
	bp := behavior.CarefulProfile().Jitter()

	headless := "virtual"
	if !cfg.Fetch.Headless {
		headless = "false"
	}

	opts := []fetch.CamoufoxOption{
		fetch.WithBrowserIdentity(profile),
		fetch.WithHeadless(headless),
		// NO BlockImages — same as proxy browser, avoid detection.
		fetch.WithBrowserTimeout(60 * time.Second),
		fetch.WithMaxBrowserRequests(100),
		fetch.WithBehaviorProfile(bp),
		// No WithUserDataDir — single persistent context kills concurrency.
		fetch.WithStorageState("/home/scraper/.sessions/serp-direct-session.json"),
		fetch.WithBrowserCookies(googleConsentCookies()),
	}
	// NO proxy — uses server IP directly.

	browser, err := fetch.NewCamoufox(opts...)
	if err != nil {
		return nil, fmt.Errorf("fetcher: creating direct serp browser: %w", err)
	}
	slog.Info("fetch: created direct (no-proxy) SERP browser")
	return browser, nil
}

// googleConsentCookies returns pre-baked cookies that tell Google
// "user already accepted cookies". Eliminates consent banner entirely.
func googleConsentCookies() []fetch.BrowserCookie {
	return []fetch.BrowserCookie{
		{Name: "SOCS", Value: "CAISHAgBEhJnd3NfMjAyMzA4MDktMF9SQzEaAmVuIAEaBgiA_LGYBA", Domain: ".google.com", Path: "/"},
		{Name: "CONSENT", Value: "YES+cb.20230809-04-p0.en+FX+410", Domain: ".google.com", Path: "/"},
	}
}

// FetchWithBrowserString fetches a page using only the browser, returns string body.
// Used by enrich domain scorer when it decides to skip stealth entirely.
func FetchWithBrowserString(ctx context.Context, browser *fetch.CamoufoxFetcher, pageURL, jobID string) (string, error) {
	body, err := FetchWithBrowser(ctx, browser, pageURL, "ds-"+jobID)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// ──────────────────────────────────────────────────────────────
// Beta features (foxhound v0.0.7) — enabled via BETA_FEATURES=1
// ──────────────────────────────────────────────────────────────

// NewCircuitBreaker creates a per-domain circuit breaker for SERP fetches.
// When google.com fails >50% in a 20-request window, the circuit opens
// and rejects requests for 30s→10min (exponential backoff with 50% jitter).
// This prevents wasting browser tabs on domains that are actively blocking.
//
// Returns nil when beta features are disabled.
func NewCircuitBreaker(cfg *config.Config) foxhound.Middleware {
	if !cfg.Fetch.BetaFeatures && !cfg.Fetch.CircuitBreaker {
		return nil
	}
	slog.Info("beta: circuit breaker enabled for SERP")
	return middleware.NewCircuitBreaker(middleware.CircuitBreakerConfig{
		FailureThreshold: 0.5,
		MinObservations:  5,
		WindowSize:       20,
		BaseTimeout:      30 * time.Second,
		MaxTimeout:       10 * time.Minute,
		MaxTrips:         8,
	})
}

// NewDomainScorer creates a Bayesian domain risk scorer for enrich SmartFetcher.
// Learns which domains block stealth HTTP and auto-escalates to browser.
// Returns nil when beta features are disabled.
func NewDomainScorer(cfg *config.Config) *fetch.DomainScorer {
	if !cfg.Fetch.BetaFeatures && !cfg.Fetch.DomainScorer {
		return nil
	}
	slog.Info("beta: domain scorer enabled for enrich")
	return fetch.NewDomainScorer(fetch.DefaultDomainScoreConfig())
}

// NewSmartFetcherWithScorer creates a SmartFetcher (stealth→browser auto-router) with
// optional domain scoring. When scorer is non-nil, it learns per-domain risk
// and skips stealth for domains that always block (e.g. LinkedIn, Instagram).
func NewSmartFetcherWithScorer(stealth foxhound.Fetcher, browser foxhound.Fetcher, scorer *fetch.DomainScorer) *fetch.SmartFetcher {
	opts := []fetch.SmartOption{}
	if scorer != nil {
		opts = append(opts, fetch.WithDomainScorer(scorer))
		opts = append(opts, fetch.WithCautiousTimeout(5*time.Second))
	}
	return fetch.NewSmart(stealth, browser, opts...)
}

// NewSessionFatigue creates a session fatigue model for human-like timing.
// Returns nil when beta features are disabled.
func NewSessionFatigue(cfg *config.Config) *behavior.SessionFatigue {
	if !cfg.Fetch.BetaFeatures && !cfg.Fetch.SessionFatigue {
		return nil
	}
	slog.Info("beta: session fatigue enabled")
	f := behavior.NewSessionFatigue(behavior.DefaultFatigueConfig())
	f.Start()
	return f
}
