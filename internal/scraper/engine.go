//go:build playwright

package scraper

import (
	"strings"

	foxhound "github.com/sadewadee/foxhound"
)

// SearchEngine abstracts a search engine for multi-engine SERP scraping.
// Each implementation knows how to build URLs, parse results, and detect blocks
// for its specific engine.
type SearchEngine interface {
	// Name returns the engine identifier (e.g. "google", "bing", "duckduckgo").
	Name() string

	// BuildURL constructs a search URL for the given query and pagination.
	// gl = geo/country code (e.g. "us"), hl = language code (e.g. "en").
	BuildURL(query string, page, perPage int, gl, hl string) string

	// ParseResults extracts organic result URLs from the raw HTML body.
	ParseResults(body []byte) ([]string, error)

	// FetchSteps returns browser automation steps to execute after page load
	// (e.g. consent banner dismissal, wait for results). Empty for non-browser engines.
	FetchSteps() []foxhound.JobStep

	// IsCaptchaPage returns true if the body indicates a captcha or block page.
	IsCaptchaPage(body []byte) bool

	// ExcludedDomains returns domains owned by this engine that should be
	// filtered from results (e.g. google.com, bing.com).
	ExcludedDomains() []string

	// MaxPages returns the maximum number of result pages to scrape.
	MaxPages() int

	// NeedsBrowser returns true if this engine requires a full browser (Camoufox)
	// rather than stealth HTTP fetching.
	NeedsBrowser() bool
}

// engines is the registry of all available search engines, keyed by name.
var engines = map[string]SearchEngine{
	"google":     &GoogleEngine{},
	"bing":       &BingEngine{},
	"duckduckgo": &DuckDuckGoEngine{},
}

// GetEngine returns the SearchEngine for the given name, or nil if not found.
func GetEngine(name string) SearchEngine {
	return engines[name]
}

// AllEngines returns all registered SearchEngine implementations.
func AllEngines() []SearchEngine {
	return []SearchEngine{&GoogleEngine{}, &BingEngine{}, &DuckDuckGoEngine{}}
}

// EnabledEngines returns engines filtered by the config string.
// "all" = all engines, "google" = only google, "google,bing" = google+bing, etc.
func EnabledEngines(enginesCfg string) []SearchEngine {
	if enginesCfg == "" || enginesCfg == "all" {
		return AllEngines()
	}
	var enabled []SearchEngine
	for _, name := range strings.Split(enginesCfg, ",") {
		name = strings.TrimSpace(strings.ToLower(name))
		if eng := GetEngine(name); eng != nil {
			enabled = append(enabled, eng)
		}
	}
	if len(enabled) == 0 {
		return AllEngines()
	}
	return enabled
}
