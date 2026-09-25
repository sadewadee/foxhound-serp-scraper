//go:build playwright

package scraper

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// PlainHTTPEngine marks a SearchEngine that must be fetched with a plain
// pooled net/http client instead of the stealth fetcher: no proxy, no TLS
// impersonation. SearXNG is an internal service and tripping either would
// only add failure modes. The stage type-asserts engines against this.
type PlainHTTPEngine interface {
	usesPlainHTTP()
}

// plainFetchLimit caps how much of a response body we read. SearXNG JSON pages
// are well under this; the cap only guards against a misconfigured upstream
// streaming forever.
const plainFetchLimit = 8 << 20 // 8 MiB

// PlainHTTPFetcher fetches internal services (SearXNG) with a single reused
// net/http client: no proxy, no TLS impersonation. One instance per worker,
// never one per request (Invariant #5).
type PlainHTTPFetcher struct {
	client *http.Client
}

// NewPlainHTTPFetcher returns a fetcher whose one http.Client lives for the
// fetcher's lifetime. timeout bounds each request.
func NewPlainHTTPFetcher(timeout time.Duration) *PlainHTTPFetcher {
	return &PlainHTTPFetcher{client: &http.Client{Timeout: timeout}}
}

// FetchPlain GETs rawURL and returns the body and status code. A non-2xx
// status is returned as an error so the caller retries it like any other
// fetch failure.
func (f *PlainHTTPFetcher) FetchPlain(ctx context.Context, rawURL string) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("plainhttp: building request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "serp-scraper/searxng")

	resp, err := f.client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("plainhttp: fetching: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, plainFetchLimit))
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("plainhttp: reading body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return body, resp.StatusCode, fmt.Errorf("plainhttp: status %d", resp.StatusCode)
	}
	return body, resp.StatusCode, nil
}
