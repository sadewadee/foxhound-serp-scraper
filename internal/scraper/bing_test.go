//go:build playwright

package scraper

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBingEngine_ParseResults_PoisonedChainedHonolulu(t *testing.T) {
	testFilePath := filepath.Join("testdata", "chained_honolulu-p0.html")
	body, err := os.ReadFile(testFilePath)
	if err != nil {
		t.Fatalf("failed to read testdata file: %v", err)
	}

	bing := &BingEngine{}
	results, err := bing.ParseResults(body)
	if err != nil {
		t.Fatalf("BingEngine.ParseResults() returned unexpected error: %v", err)
	}

	if len(results) == 0 {
		t.Fatalf("BingEngine.ParseResults() returned 0 results; expected organic results from HTML")
	}

	// Verify that titles and URLs are populated
	for i, r := range results {
		if r.URL == "" {
			t.Errorf("result[%d] has empty URL", i)
		}
		if r.Title == "" {
			t.Errorf("result[%d] (%s) has empty Title", i, r.URL)
		}
	}

	// Verify that relevance guard catches the poisoned SERP results
	query := "day spa honolulu"
	kept, ratio := FilterRelevant(results, query)
	if ratio >= 0.3 {
		t.Errorf("FilterRelevant(%q) ratio = %f; want < 0.3 (kept %d/%d results: %+v)", query, ratio, len(kept), len(results), kept)
	}
}
