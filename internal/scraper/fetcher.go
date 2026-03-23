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

	opts := []fetch.CamoufoxOption{
		fetch.WithBrowserIdentity(profile),
		fetch.WithHeadless(headless),
		fetch.WithBlockImages(cfg.Fetch.BlockImages),
		fetch.WithBrowserTimeout(60 * time.Second),

		// Session persistence — maintain cookies across requests.
		// Google won't re-captcha every page if session persists.
		fetch.WithPersistSession(true),

		// Rotate browser after 100 requests to avoid fingerprint accumulation.
		fetch.WithMaxBrowserRequests(100),

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

// NewBrowser creates a basic Camoufox browser for website scraping fallback.
func NewBrowser(cfg *config.Config) (*fetch.CamoufoxFetcher, error) {
	profile := identity.Generate(identity.WithBrowser(identity.BrowserFirefox))

	headless := "virtual"
	if !cfg.Fetch.Headless {
		headless = "false"
	}

	opts := []fetch.CamoufoxOption{
		fetch.WithBrowserIdentity(profile),
		fetch.WithHeadless(headless),
		fetch.WithBlockImages(cfg.Fetch.BlockImages),
		fetch.WithBrowserTimeout(30 * time.Second),
		fetch.WithMaxBrowserRequests(300),
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
func NewStealth(cfg *config.Config) *fetch.StealthFetcher {
	profile := identity.Generate()

	opts := []fetch.StealthOption{
		fetch.WithIdentity(profile),
		fetch.WithTimeout(time.Duration(cfg.Contact.TimeoutMs) * time.Millisecond),
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
