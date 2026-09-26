//go:build playwright

package stage

import (
	"testing"

	"github.com/sadewadee/serp-scraper/internal/config"
	"github.com/sadewadee/serp-scraper/internal/directory"
)

// skipViaSharedPredicate drives the exact predicate both the SERP
// pre-INSERT filter (serp.go) and the enrich early skip (enrich.go) use:
// the plain blocklist verdict unless the module is active and the domain is
// one of its sites.
func skipViaSharedPredicate(domain string, stage *EnrichStage) bool {
	return directory.ShouldSkipDomain(isSkipDomain(domain),
		directory.ModuleActive(stage.cfg.Directory.DataDomeEnabled, stage.cfg.Directory.ProxyURL),
		domain)
}

// routeViaSharedBranch drives the exact branch decision the enrich worker
// uses after the skip: only an active module routes a DataDome site to its
// own browser. In particular, a NON-blocklisted subdomain such as es.yelp.com
// passes the early skip and must still reach the normal fetch path while the
// module is off — routing it to the DataDome branch would burn an attempt on
// a browser that cannot be built.
func routeViaSharedBranch(domain string, stage *EnrichStage) bool {
	return stage.dataDomeActive() && directory.IsDataDomeDirectorySite(domain)
}

// TestDataDomeRouting_FlagOffKeepsSitesBlocked is the regression that matters
// most: with the module shipped disabled — the default in every compose file —
// Yelp and TripAdvisor must stay blocked exactly as they are today, so the
// module being present changes nothing until it is deliberately switched on.
func TestDataDomeRouting_FlagOffKeepsSitesBlocked(t *testing.T) {
	cfg := &config.Config{}
	stage := &EnrichStage{cfg: cfg}

	if stage.dataDomeActive() {
		t.Fatal("module active with the default (flag off, no proxy)")
	}
	for _, d := range []string{"www.yelp.com", "m.yelp.com", "www.tripadvisor.com"} {
		if !directory.IsDataDomeDirectorySite(d) {
			t.Errorf("%s not recognised as a DataDome directory site", d)
		}
		// The gate is off, so the site must still be skipped, exactly as before.
		if !skipViaSharedPredicate(d, stage) {
			t.Errorf("%s is no longer skipped with the module off — the module must be inert by default", d)
		}
	}
}

// TestDataDomeRouting_FlagOnUnblocksOnlyWithProxy walks the three states the
// routing gate has to distinguish.
func TestDataDomeRouting_FlagOnUnblocksOnlyWithProxy(t *testing.T) {
	tests := []struct {
		name     string
		enabled  bool
		proxy    string
		active   bool
		unblocks bool
	}{
		{"shipped default", false, "", false, false},
		{"enabled without a proxy", true, "", false, false},
		{"enabled with a proxy", true, "http://residential.example:8080", true, true},
		{"proxy set but flag off", false, "http://residential.example:8080", false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.Directory.DataDomeEnabled = tt.enabled
			cfg.Directory.ProxyURL = tt.proxy
			stage := &EnrichStage{cfg: cfg}

			if got := stage.dataDomeActive(); got != tt.active {
				t.Fatalf("dataDomeActive() = %v, want %v", got, tt.active)
			}
			skipped := skipViaSharedPredicate("www.yelp.com", stage)
			if skipped == tt.unblocks {
				t.Errorf("%s: yelp skipped = %v, want skipped = %v", tt.name, skipped, !tt.unblocks)
			}
			// A site the module never serves is untouched either way.
			if skipViaSharedPredicate("www.smallbiz.com", stage) {
				t.Errorf("%s: unrelated domain skipped — the module must not widen the blocklist", tt.name)
			}
		})
	}
}

func TestDataDomeSessionID(t *testing.T) {
	cfg := &config.Config{}
	if a, b := dataDomeSessionID(cfg, "https://www.yelp.com/biz/x"), dataDomeSessionID(cfg, "https://www.yelp.com/biz/x"); a == b {
		t.Error("non-sticky sessions must differ per fetch")
	}
	cfg.Directory.ProxySticky = true
	if a, b := dataDomeSessionID(cfg, "https://www.yelp.com/biz/x"), dataDomeSessionID(cfg, "https://www.yelp.com/biz/x"); a != b {
		t.Errorf("sticky sessions must match for the same URL: %q vs %q", a, b)
	}
}

// TestDataDomeRouting_NonBlocklistedSubdomainStaysNormal is the regression for
// the flag-off behaviour that matters most: blockedDomains lists exact hosts
// (www.yelp.com, m.yelp.com, …), so es.yelp.com and bare yelp.com are NOT
// blocklisted. They passed the early skip even before this module existed and
// took the normal fetch path; with the module off they must keep doing so.
func TestDataDomeRouting_NonBlocklistedSubdomainStaysNormal(t *testing.T) {
	off := &EnrichStage{cfg: &config.Config{}}
	for _, d := range []string{"es.yelp.com", "yelp.com", "tripadvisor.com", "fr.tripadvisor.com"} {
		if isSkipDomain(d) {
			continue // blocklisted exact host: skipping it is correct
		}
		if routeViaSharedBranch(d, off) {
			t.Errorf("%s routed to the DataDome branch while the module is off — it must take the normal fetch path", d)
		}
	}

	// With the module on, those same hosts are the ones the module serves.
	on := &EnrichStage{cfg: &config.Config{}}
	on.cfg.Directory.DataDomeEnabled = true
	on.cfg.Directory.ProxyURL = "http://residential.example:8080"
	if !routeViaSharedBranch("es.yelp.com", on) {
		t.Error("es.yelp.com must route to the module once it is active")
	}
}
