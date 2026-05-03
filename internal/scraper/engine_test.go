//go:build playwright

package scraper

import (
	"testing"

	foxhound "github.com/sadewadee/foxhound"
)

// TestEngineUsesBrowserFallback verifies the browser-fallback gate:
//   - Bing: stealth-primary engine that captchas via azuretls JA3 mismatch → fallback ON
//   - DuckDuckGo: stealth-primary, rarely captchas, excluded by design → fallback OFF
//   - Google: NeedsBrowser()=true (no stealth phase) → fallback OFF
func TestEngineUsesBrowserFallback(t *testing.T) {
	cases := []struct {
		name     string
		engine   SearchEngine
		wantFall bool
	}{
		{"bing enables fallback", &BingEngine{}, true},
		{"duckduckgo no fallback", &DuckDuckGoEngine{}, false},
		{"google no fallback", &GoogleEngine{}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := EngineUsesBrowserFallback(tc.engine)
			if got != tc.wantFall {
				t.Errorf("EngineUsesBrowserFallback(%s) = %v, want %v",
					tc.engine.Name(), got, tc.wantFall)
			}
		})
	}
}

// TestEngineUsesBrowserFallback_NeedsBrowserConsistency verifies the invariant:
// engines that NeedsBrowser()=true should NOT also want browser fallback,
// because they never take the stealth path in the first place.
func TestEngineUsesBrowserFallback_NeedsBrowserConsistency(t *testing.T) {
	all := AllEngines()
	for _, eng := range all {
		if eng.NeedsBrowser() && EngineUsesBrowserFallback(eng) {
			t.Errorf("engine %q has NeedsBrowser()=true AND EngineUsesBrowserFallback()=true — "+
				"contradictory: browser-primary engines have no stealth phase to fall back from",
				eng.Name())
		}
	}
}

// TestEngineUsesBrowserFallback_UnknownEngineDefaultsToFalse documents that new
// engines without an explicit case receive no fallback by default.
func TestEngineUsesBrowserFallback_UnknownEngineDefaultsToFalse(t *testing.T) {
	stub := &stubEngine{name: "yahoo"}
	if EngineUsesBrowserFallback(stub) {
		t.Errorf("unknown engine %q should not receive browser fallback by default", stub.Name())
	}
}

// stubEngine is a minimal SearchEngine for testing unknown/future engines.
type stubEngine struct{ name string }

// Compile-time interface satisfaction check.
var _ SearchEngine = (*stubEngine)(nil)

func (s *stubEngine) Name() string                                    { return s.name }
func (s *stubEngine) BuildURL(_ string, _, _ int, _, _ string) string { return "" }
func (s *stubEngine) ParseResults(_ []byte) ([]string, error)         { return nil, nil }
func (s *stubEngine) FetchSteps() []foxhound.JobStep                  { return nil }
func (s *stubEngine) IsCaptchaPage(_ []byte) bool                     { return false }
func (s *stubEngine) ExcludedDomains() []string                       { return nil }
func (s *stubEngine) MaxPages() int                                   { return 1 }
func (s *stubEngine) NeedsBrowser() bool                              { return false }
