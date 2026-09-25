//go:build playwright

package scraper

import (
	"testing"
)

func TestGoogleEngine_ParseResults(t *testing.T) {
	html := `<!DOCTYPE html>
<html>
<body>
<div id="search">
	<div class="g">
		<div class="yuRUbf">
			<div>
				<span>
					<a href="https://www.spaluanahonolulu.com/">
						<h3>Spa Luana Honolulu - Premier Day Spa</h3>
					</a>
				</span>
			</div>
		</div>
		<div class="VwiC3b">
			<span>Relaxing massage, body treatments, and facials in Honolulu Hawaii.</span>
		</div>
	</div>
</div>
</body>
</html>`

	g := &GoogleEngine{}
	results, err := g.ParseResults([]byte(html))
	if err != nil {
		t.Fatalf("ParseResults failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("ParseResults returned %d results; want 1", len(results))
	}

	if results[0].URL != "https://www.spaluanahonolulu.com/" {
		t.Errorf("result[0].URL = %q; want %q", results[0].URL, "https://www.spaluanahonolulu.com/")
	}
	if results[0].Title != "Spa Luana Honolulu - Premier Day Spa" {
		t.Errorf("result[0].Title = %q; want %q", results[0].Title, "Spa Luana Honolulu - Premier Day Spa")
	}
	if results[0].Snippet != "Relaxing massage, body treatments, and facials in Honolulu Hawaii." {
		t.Errorf("result[0].Snippet = %q", results[0].Snippet)
	}

	kept, ratio := FilterRelevant(results, "day spa honolulu")
	if len(kept) != 1 || ratio < 0.6 {
		t.Errorf("FilterRelevant kept %d, ratio %f; want 1, >=0.6", len(kept), ratio)
	}
}
