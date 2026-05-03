// Package geo resolves the country (ISO 3166-1 alpha-2) of a proxy's exit IP
// by making a one-shot HTTP call to ipinfo.io through the proxy at startup.
//
// Used to auto-populate cfg.Proxy.Country so identity geo-matching works even
// when PROXY_COUNTRY is not set explicitly. Manual env-var configuration is
// fragile — operators forget, container redeploys lose the override, etc.
//
// This is a startup-time helper, not a per-request hot path. The cost is one
// HTTP round-trip per container boot (~500ms typical). Failure is non-fatal:
// the caller logs and continues with empty country (foxhound falls back to
// random identity).
package geo

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// DefaultEndpoint is the IP-info service used to resolve country code.
// ipinfo.io/country returns just a 2-letter code in plain text (no JSON parse).
const DefaultEndpoint = "https://ipinfo.io/country"

// DefaultTimeout caps the lookup at 10 seconds so a slow proxy doesn't stall startup.
const DefaultTimeout = 10 * time.Second

// ResolveCountryFromProxy returns the ISO 3166-1 alpha-2 country code of the
// given proxy's exit IP by calling ipinfo.io/country through it.
//
// Returns "" + error on any failure (parse, network, non-2xx, malformed body).
// Callers should log the error and continue — geo-matching is best-effort.
func ResolveCountryFromProxy(ctx context.Context, proxyURL string) (string, error) {
	if proxyURL == "" {
		return "", fmt.Errorf("geo: empty proxy URL")
	}
	pu, err := url.Parse(proxyURL)
	if err != nil {
		return "", fmt.Errorf("geo: parsing proxy URL: %w", err)
	}

	transport := &http.Transport{
		Proxy:                 http.ProxyURL(pu),
		ResponseHeaderTimeout: DefaultTimeout,
	}
	// One-shot client: release the proxy TCP connection from the idle pool
	// immediately after the call so it doesn't park on a slow SOCKS5 hop
	// until OS keepalive fires.
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Transport: transport,
		Timeout:   DefaultTimeout,
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, DefaultEndpoint, nil)
	if err != nil {
		return "", fmt.Errorf("geo: building request: %w", err)
	}
	// A plain UA string — ipinfo.io rejects empty UA on free tier.
	req.Header.Set("User-Agent", "serp-scraper/geo-resolver")

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("geo: ipinfo call failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("geo: ipinfo returned HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 16))
	if err != nil {
		return "", fmt.Errorf("geo: reading response: %w", err)
	}
	code := strings.TrimSpace(string(body))
	if len(code) != 2 {
		return "", fmt.Errorf("geo: unexpected country code %q (expected 2 chars)", code)
	}
	return strings.ToUpper(code), nil
}
