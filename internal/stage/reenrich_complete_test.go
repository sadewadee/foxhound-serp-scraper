//go:build playwright

package stage

import (
	"context"
	"testing"

	"github.com/sadewadee/serp-scraper/internal/db"
	internalScraper "github.com/sadewadee/serp-scraper/internal/scraper"
	"github.com/sadewadee/serp-scraper/internal/testpg"
)

// Re-enrichment results must reach business_emails whether the job row already
// exists as 'completed' (ON CONFLICT path) or is brand new (INSERT path). A
// plain upsert fired neither case through trg_enrichment_normalize.
func TestCompleteEnrichmentJob_FiresNormalizeTrigger(t *testing.T) {
	d := testpg.Start(t, testpg.StartOpts{Prefix: "xxchk_reenrich_"}).DB()
	defer d.Close()
	if err := db.Migrate(d); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	ctx := context.Background()

	// Existing completed job for the listing URL, no emails found first time.
	const existing = "https://studio-a.example.com/"
	if _, err := d.Exec(`INSERT INTO enrichment_jobs (url, url_hash, domain, source, status)
		VALUES ($1, 'hash-a', 'studio-a.example.com', 'serp_result', 'pending')`, existing); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Exec(`UPDATE enrichment_jobs SET status='completed' WHERE url_hash='hash-a'`); err != nil {
		t.Fatal(err)
	}

	cases := []struct{ name, url, hash, domain, email string }{
		{"existing completed row", existing, "hash-a", "studio-a.example.com", "hello@studio-a.example.com"},
		{"new row", "https://studio-b.example.org/", "hash-b", "studio-b.example.org", "hello@studio-b.example.org"},
	}
	for _, tc := range cases {
		cd := &internalScraper.ContactData{BusinessName: "Studio " + tc.name}
		if err := completeEnrichmentJob(ctx, d, tc.url, tc.hash, tc.domain, []string{tc.email}, nil, []byte(`{}`), cd); err != nil {
			t.Fatalf("%s: complete: %v", tc.name, err)
		}
		var n int
		if err := d.QueryRow(`SELECT COUNT(*) FROM business_emails be
			JOIN business_listings bl ON bl.id = be.business_id
			JOIN emails e ON e.id = be.email_id
			WHERE bl.domain = $1 AND e.email = $2`, tc.domain, tc.email).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Errorf("%s: business_emails rows = %d, want 1 (trigger did not fire)", tc.name, n)
		}
		var status string
		if err := d.QueryRow(`SELECT status FROM enrichment_jobs WHERE url_hash = $1`, tc.hash).Scan(&status); err != nil || status != "completed" {
			t.Errorf("%s: job status = %q err=%v, want completed", tc.name, status, err)
		}
	}
}
