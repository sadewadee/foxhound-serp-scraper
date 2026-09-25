//go:build playwright

package scraper

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	foxhound "github.com/sadewadee/foxhound"
)

// SearXNGEngine implements SearchEngine for a self-hosted SearXNG instance
// queried through its JSON API. Unlike Bing/DDG it is an internal service, so
// it is fetched with a plain pooled net/http client (see PlainHTTPFetcher) —
// no proxy and no TLS impersonation.
type SearXNGEngine struct {
	// BaseURL is the SearXNG origin, e.g. "http://searxng:8080".
	BaseURL string
	// Engines, when non-empty, is passed as the `engines` query parameter to
	// restrict which upstream engines SearXNG queries (e.g. "google,brave").
	Engines string
	// Pages is the number of result pages to fan out per query.
	Pages int
}

func (e *SearXNGEngine) Name() string { return "searxng" }

func (e *SearXNGEngine) BuildURL(query string, page, perPage int, gl, hl string) string {
	base := strings.TrimRight(e.BaseURL, "/")
	v := url.Values{}
	v.Set("q", query)
	v.Set("format", "json")
	// SearXNG pages are 1-based; our fan-out counts pages from 0.
	v.Set("pageno", fmt.Sprintf("%d", page+1))
	if hl != "" {
		v.Set("language", hl)
	}
	if e.Engines != "" {
		v.Set("engines", e.Engines)
	}
	return base + "/search?" + v.Encode()
}

// searxngResponse is the subset of the SearXNG JSON API we consume.
type searxngResponse struct {
	Results             []searxngResult `json:"results"`
	UnresponsiveEngines [][]string      `json:"unresponsive_engines"`
}

type searxngResult struct {
	URL     string `json:"url"`
	Title   string `json:"title"`
	Content string `json:"content"`
}

func (e *SearXNGEngine) ParseResults(body []byte) ([]SERPResult, error) {
	var resp searxngResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("searxng: parsing JSON: %w", err)
	}

	ownHost := ""
	if u, err := url.Parse(e.BaseURL); err == nil {
		ownHost = strings.ToLower(u.Hostname())
	}

	var out []SERPResult
	seen := make(map[string]bool)
	for _, r := range resp.Results {
		if !strings.HasPrefix(r.URL, "http://") && !strings.HasPrefix(r.URL, "https://") {
			continue
		}
		// SearXNG-internal links (its own host, or a /search? page) are not
		// business results.
		if u, err := url.Parse(r.URL); err == nil {
			if ownHost != "" && strings.EqualFold(u.Hostname(), ownHost) {
				continue
			}
		}
		if strings.Contains(r.URL, "/search?") {
			continue
		}
		if seen[r.URL] {
			continue
		}
		seen[r.URL] = true
		out = append(out, SERPResult{URL: r.URL, Title: r.Title, Snippet: r.Content})
	}
	return out, nil
}

func (e *SearXNGEngine) FetchSteps() []foxhound.JobStep { return nil }

// IsCaptchaPage reports a response we should retry rather than trust: a body
// that is not JSON at all, or a JSON body with zero results whose upstream
// engines all failed (unresponsive_engines non-empty — captcha/rate-limit
// upstream). Zero results with no unresponsive engines is a legitimate empty
// page and returns false.
func (e *SearXNGEngine) IsCaptchaPage(body []byte) bool {
	var resp searxngResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return true
	}
	return len(resp.Results) == 0 && len(resp.UnresponsiveEngines) > 0
}

func (e *SearXNGEngine) ExcludedDomains() []string { return nil }

func (e *SearXNGEngine) MaxPages() int {
	if e.Pages <= 0 {
		return 2
	}
	return e.Pages
}

func (e *SearXNGEngine) NeedsBrowser() bool { return false }

// usesPlainHTTP marks SearXNG for the plain pooled HTTP fetch path.
func (e *SearXNGEngine) usesPlainHTTP() {}
