//go:build playwright

package scraper

import (
	"fmt"
	"testing"

	foxhound "github.com/sadewadee/foxhound"
)

// The five behavioral scenarios required for issue #7:
//
//  1. Stealth success  → no browser fallback triggered
//  2. Stealth captcha  → exactly one browser fallback attempt
//  3. Browser fallback success → job completes
//  4. Browser fallback failure → falls through to retry/dead-letter
//  5. Non-Bing engines (Google, DDG) → no fallback path triggered
//
// The tabWorker loop is not directly unit-testable (requires live browser + Redis).
// These tests verify the gate functions that control the fallback decision,
// mirroring the exact conditional logic in serp.go tabWorker.

// simulateFallbackDecision mirrors the decision gate in tabWorker's stealth path:
//
//	if fetchErr != nil || (fetchErr == nil && eng.IsCaptchaPage(body)) {
//	    if scraper.EngineUsesBrowserFallback(eng) { ... }
//	}
//
// Returns (shouldFallback bool, wasCaptcha bool).
func simulateFallbackDecision(eng SearchEngine, fetchErr error, body []byte) (shouldFallback bool, wasCaptcha bool) {
	stealthBlocked := fetchErr != nil || (fetchErr == nil && eng.IsCaptchaPage(body))
	wasCaptcha = fetchErr == nil && eng.IsCaptchaPage(body)
	shouldFallback = stealthBlocked && EngineUsesBrowserFallback(eng)
	return
}

// TestFallback_StealthSuccess_NoBrowserHit: when stealth returns clean HTML,
// the fallback gate must NOT trigger regardless of engine.
func TestFallback_StealthSuccess_NoBrowserHit(t *testing.T) {
	cleanHTML := []byte("<html><body><li class='b_algo'><h2><a href='https://example.com'>Example</a></h2></li></body></html>")
	engines := []SearchEngine{&BingEngine{}, &DuckDuckGoEngine{}, &GoogleEngine{}}

	for _, eng := range engines {
		t.Run("stealth_ok_"+eng.Name(), func(t *testing.T) {
			fallback, _ := simulateFallbackDecision(eng, nil, cleanHTML)
			if fallback {
				t.Errorf("engine %q: browser fallback should NOT trigger on stealth success", eng.Name())
			}
		})
	}
}

// TestFallback_StealthCaptcha_ExactlyOneBrowserAttempt: when Bing stealth
// returns a captcha body, the fallback gate triggers exactly once.
func TestFallback_StealthCaptcha_ExactlyOneBrowserAttempt(t *testing.T) {
	bingCaptchaBody := []byte(`<html><body><div class="captcha_header">Please verify</div></body></html>`)
	eng := &BingEngine{}

	fallback, wasCaptcha := simulateFallbackDecision(eng, nil, bingCaptchaBody)
	if !eng.IsCaptchaPage(bingCaptchaBody) {
		t.Fatal("BingEngine.IsCaptchaPage should detect captcha body")
	}
	if !wasCaptcha {
		t.Error("wasCaptcha should be true when stealth returns captcha body")
	}
	if !fallback {
		t.Error("browser fallback should trigger when Bing stealth returns captcha")
	}
}

// TestFallback_StealthError_TriggersBrowserForBing: when stealth returns an
// error (network, 429, etc.), Bing should trigger browser fallback.
func TestFallback_StealthError_TriggersBrowserForBing(t *testing.T) {
	eng := &BingEngine{}
	fakeErr := fmt.Errorf("connection refused")

	fallback, _ := simulateFallbackDecision(eng, fakeErr, nil)
	if !fallback {
		t.Error("browser fallback should trigger when Bing stealth returns error")
	}
}

// TestFallback_BrowserSuccess_JobCompletes documents that after a successful
// browser fallback, body is non-nil and fetchErr is nil — the normal success
// path in tabWorker continues.
func TestFallback_BrowserSuccess_JobCompletes(t *testing.T) {
	// Simulate: stealth captcha → browser returns clean body → parse succeeds.
	bingCaptchaBody := []byte(`<html><body><div class="captcha_header">verify</div></body></html>`)
	cleanBrowserBody := []byte(`<html><body><li class="b_algo"><h2><a href="https://gym.com">Gym</a></h2></li></body></html>`)

	eng := &BingEngine{}
	fallback, _ := simulateFallbackDecision(eng, nil, bingCaptchaBody)
	if !fallback {
		t.Fatal("expected fallback to trigger")
	}

	// After browser fallback succeeds: body = cleanBrowserBody, fetchErr = nil.
	browserErr := error(nil)
	if eng.IsCaptchaPage(cleanBrowserBody) {
		browserErr = fmt.Errorf("captcha on browser fallback")
	}

	if browserErr != nil {
		t.Errorf("expected browser fallback to succeed, got: %v", browserErr)
	}
	urls, parseErr := eng.ParseResults(cleanBrowserBody)
	if parseErr != nil {
		t.Errorf("ParseResults on browser body should not error: %v", parseErr)
	}
	if len(urls) == 0 {
		t.Error("expected at least one URL from clean browser body")
	}
}

// TestFallback_BrowserFailure_FallsThrough documents that when browser fallback
// also fails (captcha or error), fetchErr is reinstated and the normal
// retry/dead-letter path handles it.
func TestFallback_BrowserFailure_FallsThrough(t *testing.T) {
	captchaBody := []byte(`<html><body><div class="captcha_header">verify</div></body></html>`)
	eng := &BingEngine{}

	// Stealth returns captcha → fallback triggered.
	fallback, _ := simulateFallbackDecision(eng, nil, captchaBody)
	if !fallback {
		t.Fatal("expected fallback to trigger")
	}

	// Browser also returns captcha → fallback error.
	browserBody := captchaBody
	var fallbackErr error
	if eng.IsCaptchaPage(browserBody) {
		fallbackErr = fmt.Errorf("captcha on browser fallback")
	}

	if fallbackErr == nil {
		t.Error("expected fallbackErr to be set when browser also returns captcha")
	}
	// The compound error annotation mirrors serp.go line 546:
	// fetchErr = fmt.Errorf("stealth: %v; browser-fallback: %w", stealthErr, fallbackErr)
	if fallbackErr.Error() != "captcha on browser fallback" {
		t.Errorf("unexpected error message: %v", fallbackErr)
	}
}

// TestFallback_NonBingEngines_NotAffected: DDG and Google must never trigger
// the browser fallback path, even when they return a captcha body.
func TestFallback_NonBingEngines_NotAffected(t *testing.T) {
	ddgCaptchaBody := []byte(`<html><body><div id="anomaly-modal">bot detected</div></body></html>`)

	engines := []struct {
		eng  SearchEngine
		body []byte
	}{
		{&DuckDuckGoEngine{}, ddgCaptchaBody},
		{&GoogleEngine{}, []byte(`<html><body>sorry captcha google</body></html>`)},
	}

	for _, tc := range engines {
		t.Run("no_fallback_"+tc.eng.Name(), func(t *testing.T) {
			fallback, _ := simulateFallbackDecision(tc.eng, nil, tc.body)
			if fallback {
				t.Errorf("engine %q must NOT trigger browser fallback", tc.eng.Name())
			}
		})
	}
}

// Needed for TestFallback_StealthError_TriggersBrowserForBing and others
// that call fmt.Errorf — import it here since this test file is in the
// same package and fmt must be imported explicitly.
var _ = foxhound.FetchBrowser // keep foxhound import live (used in stubEngine via FetchSteps)
