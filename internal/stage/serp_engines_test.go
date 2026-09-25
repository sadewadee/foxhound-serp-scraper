//go:build playwright

package stage

import (
	"testing"

	"github.com/sadewadee/serp-scraper/internal/config"
	"github.com/sadewadee/serp-scraper/internal/scraper"
)

func hasEngine(engines []scraper.SearchEngine, name string) bool {
	for _, e := range engines {
		if e.Name() == name {
			return true
		}
	}
	return false
}

func TestResolveEngines_DropsSearxngWhenURLEmpty(t *testing.T) {
	cfg := &config.Config{}
	cfg.SERP.Engines = "searxng,duckduckgo"
	cfg.SERP.SearXNGURL = ""

	engines := resolveEngines(cfg)
	if hasEngine(engines, "searxng") {
		t.Error("searxng kept despite empty SEARXNG_URL — it must be dropped")
	}
	if !hasEngine(engines, "duckduckgo") {
		t.Error("duckduckgo dropped — only searxng should be dropped")
	}
}

func TestResolveEngines_KeepsSearxngWhenConfigured(t *testing.T) {
	cfg := &config.Config{}
	cfg.SERP.Engines = "searxng"
	cfg.SERP.SearXNGURL = "http://searxng:8080"
	cfg.SERP.SearXNGMaxPages = 3
	cfg.SERP.SearXNGEngines = "google,brave"

	engines := resolveEngines(cfg)
	if len(engines) != 1 || engines[0].Name() != "searxng" {
		t.Fatalf("got %d engines, want 1 named searxng", len(engines))
	}
	sx, ok := engines[0].(*scraper.SearXNGEngine)
	if !ok {
		t.Fatalf("engine is %T, want *scraper.SearXNGEngine", engines[0])
	}
	if sx.BaseURL != "http://searxng:8080" {
		t.Errorf("BaseURL = %q", sx.BaseURL)
	}
	if sx.Engines != "google,brave" {
		t.Errorf("Engines = %q", sx.Engines)
	}
	if sx.MaxPages() != 3 {
		t.Errorf("MaxPages = %d, want 3", sx.MaxPages())
	}
}

func TestResolveEngines_OtherEnginesUnchanged(t *testing.T) {
	cfg := &config.Config{}
	cfg.SERP.Engines = "bing,duckduckgo"
	engines := resolveEngines(cfg)
	if len(engines) != 2 || !hasEngine(engines, "bing") || !hasEngine(engines, "duckduckgo") {
		t.Fatalf("got %d engines, want bing+duckduckgo", len(engines))
	}
}

// With the default "all", searxng must not appear: it has no base URL and would
// be a stage that cannot fetch anything.
func TestResolveEngines_DefaultExcludesSearxng(t *testing.T) {
	cfg := &config.Config{}
	cfg.SERP.Engines = "all"
	// Deliberately set the URL: "all" must still not pick SearXNG up, because
	// only an explicit SERP_ENGINES entry opts in.
	cfg.SERP.SearXNGURL = "http://searxng:8080"

	engines := resolveEngines(cfg)
	if hasEngine(engines, "searxng") {
		t.Error("searxng enabled under SERP_ENGINES=all — it must require an explicit entry")
	}
	if len(engines) != 3 {
		t.Errorf("got %d engines, want 3 (google+bing+duckduckgo)", len(engines))
	}
}
