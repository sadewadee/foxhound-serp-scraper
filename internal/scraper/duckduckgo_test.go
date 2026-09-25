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
