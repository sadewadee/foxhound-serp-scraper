//go:build playwright

package scraper

import (
	"testing"
)

func TestDuckDuckGoEngine_ParseResults(t *testing.T) {
	html := `<!DOCTYPE html>
<html>
<body>
<div class="results">
	<div class="web-result">
		<h2 class="result__title">
			<a class="result__a" href="//duckduckgo.com/l/?kh=-1&uddg=https%3A%2F%2Fwww.spaluanahonolulu.com%2F">Spa Luana Honolulu</a>
		</h2>
		<div class="result__snippet">Luxury day spa in Honolulu offering wellness and massage treatments.</div>
	</div>
	<div class="web-result">
		<h2 class="result__title">
			<a class="result__a" href="//duckduckgo.com/l/?kh=-1&uddg=https%3A%2F%2Fhonolulumedspa.com%2Fservices">Honolulu MedSpa</a>
		</h2>
		<div class="result__snippet">Medical aesthetics and skin therapy clinic in Honolulu Hawaii.</div>
	</div>
</div>
</body>
</html>`

	ddg := &DuckDuckGoEngine{}
	results, err := ddg.ParseResults([]byte(html))
	if err != nil {
		t.Fatalf("ParseResults failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("ParseResults returned %d results; want 2", len(results))
	}

	if results[0].URL != "https://www.spaluanahonolulu.com/" {
		t.Errorf("result[0].URL = %q; want %q", results[0].URL, "https://www.spaluanahonolulu.com/")
	}
	if results[0].Title != "Spa Luana Honolulu" {
		t.Errorf("result[0].Title = %q; want %q", results[0].Title, "Spa Luana Honolulu")
	}
	if results[0].Snippet != "Luxury day spa in Honolulu offering wellness and massage treatments." {
		t.Errorf("result[0].Snippet = %q; want snippet text", results[0].Snippet)
	}

	kept, ratio := FilterRelevant(results, "day spa honolulu")
	if len(kept) != 2 || ratio < 0.6 {
		t.Errorf("FilterRelevant kept %d, ratio %f; want 2, >=0.6", len(kept), ratio)
	}
}

// DDG serves its anomaly page to a GET with a self-referencing Referer
// (probe: 2/50 pages ok with it, 46/50 without). With the flag on the
// request must carry no Referer; with it off the legacy header stays.
func TestSerpStealthHeadersDDGReferer(t *testing.T) {
	u := "https://html.duckduckgo.com/html/?q=yoga&kl=us-en-"

	if got := serpStealthHeaders(u, false).Get("Referer"); got != "https://html.duckduckgo.com/" {
		t.Errorf("flag off: Referer = %q, want engine homepage", got)
	}
	if got := serpStealthHeaders(u, true).Get("Referer"); got != "" {
		t.Errorf("flag on: Referer = %q, want none", got)
	}
}
