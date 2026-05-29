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

// EngineUsesBrowserFallback returns true if the engine uses stealth-primary
// fetching but should fall back to a single browser attempt when stealth
// returns a captcha or error.
//
// Bing is included because foxhound's azuretls JA3 preset does not match
// Bing's expected fingerprint, causing ~100 % of stealth requests to receive
// a captcha challenge.  DDG is excluded — it rarely captchas on stealth and
// its HTML endpoint does not benefit from a browser path.
//
// Engines that already use NeedsBrowser() = true (e.g. Google) never reach
// this code path; they have no stealth phase to fall back from.
func EngineUsesBrowserFallback(eng SearchEngine) bool {
	return eng.Name() == "bing"
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
