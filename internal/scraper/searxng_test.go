//go:build playwright

package scraper

import (
	"net/url"
	"os"
	"strings"
	"testing"
)

func loadSearxngFixture(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/searxng_day_spa_honolulu.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return b
}

func TestSearXNGParseResults(t *testing.T) {
	e := &SearXNGEngine{BaseURL: "http://searxng:8080"}
	results, err := e.ParseResults(loadSearxngFixture(t))
	if err != nil {
		t.Fatalf("ParseResults: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected results from fixture, got none")
	}
	for i, r := range results {
		if !strings.HasPrefix(r.URL, "http://") && !strings.HasPrefix(r.URL, "https://") {
			t.Errorf("result %d: non-http URL %q", i, r.URL)
		}
		if r.Title == "" {
			t.Errorf("result %d (%s): empty title", i, r.URL)
		}
		if r.Snippet == "" {
			t.Errorf("result %d (%s): empty snippet", i, r.URL)
		}
		if strings.Contains(r.URL, "searxng:8080") || strings.Contains(r.URL, "/search?") {
			t.Errorf("result %d: internal SearXNG link leaked: %q", i, r.URL)
		}
	}
}

func TestSearXNGParseResultsSkipsInternal(t *testing.T) {
	body := `{"results":[
	  {"url":"http://searxng:8080/search?q=x","title":"self","content":"c"},
	  {"url":"ftp://example.com/a","title":"ftp","content":"c"},
	  {"url":"https://real.example.com/page","title":"real","content":"snippet"}
	]}`
	e := &SearXNGEngine{BaseURL: "http://searxng:8080"}
	results, err := e.ParseResults([]byte(body))
	if err != nil {
		t.Fatalf("ParseResults: %v", err)
	}
	if len(results) != 1 || results[0].URL != "https://real.example.com/page" {
		t.Fatalf("got %+v; want only the external result", results)
	}
}

func TestSearXNGBuildURL(t *testing.T) {
	e := &SearXNGEngine{BaseURL: "http://searxng:8080", Engines: "google,brave"}
	got := e.BuildURL("day spa & honolulu", 2, 10, "us", "en")
	u, err := url.Parse(got)
	if err != nil {
		t.Fatalf("parse built URL: %v", err)
	}
	if u.Scheme+"://"+u.Host != "http://searxng:8080" {
		t.Errorf("base = %q", u.Scheme+"://"+u.Host)
	}
	if u.Path != "/search" {
		t.Errorf("path = %q, want /search", u.Path)
	}
	if q := u.Query().Get("q"); q != "day spa & honolulu" {
		t.Errorf("q = %q; want the raw query, properly escaped", q)
	}
	if p := u.Query().Get("pageno"); p != "3" {
		t.Errorf("pageno = %q, want 3 (page 2 -> 1-based 3)", p)
	}
	if l := u.Query().Get("language"); l != "en" {
		t.Errorf("language = %q, want en", l)
	}
	if g := u.Query().Get("engines"); g != "google,brave" {
		t.Errorf("engines = %q, want google,brave", g)
	}
	if f := u.Query().Get("format"); f != "json" {
		t.Errorf("format = %q, want json", f)
	}

	// hl empty -> no language param; no Engines -> no engines param.
	e2 := &SearXNGEngine{BaseURL: "http://searxng:8080/"}
	u2, _ := url.Parse(e2.BuildURL("q", 0, 10, "us", ""))
	if _, ok := u2.Query()["language"]; ok {
		t.Error("language param present with empty hl")
	}
	if _, ok := u2.Query()["engines"]; ok {
		t.Error("engines param present with no configured engines")
	}
	if u2.Query().Get("pageno") != "1" {
		t.Errorf("pageno for page 0 = %q, want 1", u2.Query().Get("pageno"))
	}
}

func TestSearXNGIsCaptchaPage(t *testing.T) {
	e := &SearXNGEngine{BaseURL: "http://searxng:8080"}
	tests := []struct {
		name string
		body string
		want bool
	}{
		{"non-JSON", "<html><body>502 Bad Gateway</body></html>", true},
		{"valid results", `{"results":[{"url":"https://a.com","title":"t","content":"c"}]}`, false},
		{"empty + unresponsive", `{"results":[],"unresponsive_engines":[["duckduckgo","CAPTCHA"]]}`, true},
		{"empty + no unresponsive", `{"results":[],"unresponsive_engines":[]}`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := e.IsCaptchaPage([]byte(tt.body)); got != tt.want {
				t.Errorf("IsCaptchaPage = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSearXNGMaxPagesDefault(t *testing.T) {
	if got := (&SearXNGEngine{}).MaxPages(); got != 2 {
		t.Errorf("MaxPages with unset Pages = %d, want 2", got)
	}
	if got := (&SearXNGEngine{Pages: 4}).MaxPages(); got != 4 {
		t.Errorf("MaxPages with Pages=4 = %d, want 4", got)
	}
}
