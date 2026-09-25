//go:build playwright

package stage

import (
	"log/slog"
	"strings"

	"github.com/sadewadee/serp-scraper/internal/config"
	"github.com/sadewadee/serp-scraper/internal/scraper"
)

// resolveEngines turns the SERP_ENGINES config string into concrete engines.
// SearXNG is special-cased: it is only usable when SEARXNG_URL is set, and the
// engine is dropped (with a single warning, Invariant #7) when 'searxng' is
// requested but unconfigured. Other engines are returned untouched.
func resolveEngines(cfg *config.Config) []scraper.SearchEngine {
	configured := scraper.EnabledEngines(cfg.SERP.Engines)

	// Only warn when the operator actually asked for searxng: with the default
	// "all" it is present as an unconfigured placeholder and silently dropped.
	explicit := false
	for _, name := range strings.Split(cfg.SERP.Engines, ",") {
		if strings.EqualFold(strings.TrimSpace(name), "searxng") {
			explicit = true
			break
		}
	}

	var enabled []scraper.SearchEngine
	for _, eng := range configured {
		if eng.Name() != "searxng" {
			enabled = append(enabled, eng)
			continue
		}
		if strings.TrimSpace(cfg.SERP.SearXNGURL) == "" {
			if explicit {
				// Warn exactly once per boot (Invariant #7).
				slog.Warn("serp: searxng listed in SERP_ENGINES but SEARXNG_URL is empty — dropping the engine",
					"engine", "searxng")
			}
			continue
		}
		enabled = append(enabled, &scraper.SearXNGEngine{
			BaseURL: cfg.SERP.SearXNGURL,
			Engines: cfg.SERP.SearXNGEngines,
			Pages:   cfg.SERP.SearXNGMaxPages,
		})
	}
	return enabled
}

// engineLookup indexes the stage's configured engines by name.
//
// The static scraper.GetEngine registry cannot be used here: by design it
// never holds a configured SearXNG engine, so resolving a searxng job through
// it returned nil and every such job was skipped in prod
// ("serp: unknown engine, skipping engine=searxng").
func engineLookup(engines []scraper.SearchEngine) map[string]scraper.SearchEngine {
	m := make(map[string]scraper.SearchEngine, len(engines))
	for _, e := range engines {
		if e != nil {
			m[e.Name()] = e
		}
	}
	return m
}
