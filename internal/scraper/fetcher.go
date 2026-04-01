//go:build playwright

package scraper

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	foxhound "github.com/sadewadee/foxhound"
	"github.com/sadewadee/foxhound/fetch"
	"github.com/sadewadee/foxhound/identity"

	"github.com/sadewadee/serp-scraper/internal/config"
)

// NewSERPBrowser creates a Camoufox browser optimized for Google SERP scraping.
// Uses persistent session, page pooling, and geo-matched identity.
func NewSERPBrowser(cfg *config.Config) (*fetch.CamoufoxFetcher, error) {
	// Generate identity with geo-matching if proxy is set.
	idOpts := []identity.Option{identity.WithBrowser(identity.BrowserFirefox)}
	if cfg.Proxy.URL != "" {
		// Geo-match identity to proxy for consistent timezone/locale.
		idOpts = append(idOpts, identity.WithCountry("ID")) // Default Indonesia
	}
	profile := identity.Generate(idOpts...)

	headless := "virtual"
	if !cfg.Fetch.Headless {
		headless = "false"
	}

	browserTimeout := time.Duration(cfg.Fetch.BrowserTimeoutSec) * time.Second

	opts := []fetch.CamoufoxOption{
		fetch.WithBrowserIdentity(profile),
		fetch.WithHeadless(headless),
		fetch.WithBlockImages(cfg.Fetch.BlockImages),
		fetch.WithBrowserTimeout(browserTimeout),

		// Session persistence — maintain cookies across requests.
		// Google won't re-captcha every page if session persists.
		fetch.WithPersistSession(true),

		// Rotate browser after N requests to avoid fingerprint accumulation.
		// Uses half of SERP max since single-browser mode is more conservative.
		fetch.WithMaxBrowserRequests(cfg.Fetch.SERPMaxRequests / 2),

		// Page pooling — reuse warm pages, eliminates ~3s overhead per request.
		fetch.WithPoolSize(3),
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
	idOpts := []identity.Option{identity.WithBrowser(identity.BrowserFirefox)}
	if cfg.Proxy.URL != "" {
		idOpts = append(idOpts, identity.WithCountry("ID"))
	}
	profile := identity.Generate(idOpts...)

	headless := "virtual"
	if !cfg.Fetch.Headless {
		headless = "false"
	}

	browserTimeout := time.Duration(cfg.Fetch.BrowserTimeoutSec) * time.Second

	opts := []fetch.CamoufoxOption{
		fetch.WithBrowserIdentity(profile),
		fetch.WithHeadless(headless),
		fetch.WithBlockImages(cfg.Fetch.BlockImages),
		fetch.WithBrowserTimeout(browserTimeout),
		fetch.WithPersistSession(true),
		fetch.WithMaxBrowserRequests(cfg.Fetch.SERPMaxRequests),
		fetch.WithPoolSize(poolSize),
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
//   1 browser process → N pooled tabs (N = poolSize)
//   Multiple goroutines call Fetch() concurrently
//   Each gets a pre-warmed tab from the pool
//   Cookies/state cleared between uses (no session bleed)
func NewBrowserWithPool(cfg *config.Config, poolSize int) (*fetch.CamoufoxFetcher, error) {
	profile := identity.Generate(identity.WithBrowser(identity.BrowserFirefox))

	headless := "virtual"
	if !cfg.Fetch.Headless {
		headless = "false"
	}

	// Enrich browser uses half the SERP timeout — pages are simpler than Google SERPs.
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

// NewStealth creates a TLS-impersonating HTTP fetcher for website scraping.
// Geo-matches identity to proxy country to avoid detection from IP/identity mismatch.
func NewStealth(cfg *config.Config) *fetch.StealthFetcher {
	var idOpts []identity.Option
	if cfg.Proxy.URL != "" {
		idOpts = append(idOpts, identity.WithCountry("ID"))
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
	resp, err := stealth.Fetch(ctx, &foxhound.Job{
		ID:     jobID,
		URL:    pageURL,
		Method: "GET",
	})

	if err != nil || resp == nil || resp.StatusCode >= 400 {
		slog.Debug("fetch: stealth failed, trying browser",
			"url", pageURL, "err", err)

		resp, err = browser.Fetch(ctx, &foxhound.Job{
			ID:        "browser-" + jobID,
			URL:       pageURL,
			Method:    "GET",
			FetchMode: foxhound.FetchBrowser,
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
		ID:     jobID,
		URL:    serpURL,
		Method: "GET",
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
