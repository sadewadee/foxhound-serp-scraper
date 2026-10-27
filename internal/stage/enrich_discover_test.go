//go:build playwright

package stage

import (
	"testing"

	"github.com/sadewadee/serp-scraper/internal/testpg"
)

func TestIsPermanentError_DNS(t *testing.T) {
	tests := []struct {
		msg  string
		want bool
	}{
		{"fetch/stealth: request to https://a.example/ failed: failed to dial: dial tcp: lookup a.example on 127.0.0.11:53: no such host", true},
		{"dial tcp: lookup a.example on 127.0.0.11:53: server misbehaving", true},
		{"dial tcp: lookup a.example: Temporary failure in name resolution", true},
		{"query a.example: NXDOMAIN", true},
		{"page.goto: net::ERR_NAME_NOT_RESOLVED at https://a.example/", true},
		{"HTTP 404", true},
		{"dial tcp: lookup a.example on 127.0.0.11:53: read udp: i/o timeout", false},
		{"context deadline exceeded", false},
		{"HTTP 503", false},
	}
	for _, tc := range tests {
		if got := isPermanentError(tc.msg); got != tc.want {
			t.Errorf("isPermanentError(%q) = %v, want %v", tc.msg, got, tc.want)
		}
	}
}

func TestQueueDiscoveredLinks(t *testing.T) {
	db := testpg.Start(t, testpg.StartOpts{Prefix: "xxchk_discover_"}).DB()
	defer db.Close()
	if _, err := db.Exec(`
		CREATE EXTENSION IF NOT EXISTS pgcrypto;
		CREATE TABLE queries (id BIGSERIAL PRIMARY KEY);
		CREATE TABLE enrichment_jobs (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			url TEXT NOT NULL, url_hash TEXT NOT NULL UNIQUE, domain TEXT NOT NULL,
			parent_query_id BIGINT REFERENCES queries(id),
			source TEXT NOT NULL DEFAULT 'serp_result',
			status TEXT NOT NULL DEFAULT 'pending');
		INSERT INTO queries DEFAULT VALUES;`); err != nil {
		t.Fatalf("schema: %v", err)
	}
	var qid int64
	if err := db.QueryRow(`SELECT id FROM queries`).Scan(&qid); err != nil {
		t.Fatal(err)
	}
	c := &EnrichStage{db: db}
	html := []byte(`<a href="/kontakt">K</a><a href="/our-team">T</a><a href="/contact">guessed</a><a href="/blog">B</a>`)

	// Run twice: the second pass must be a no-op (ON CONFLICT DO NOTHING).
	for i := 0; i < 2; i++ {
		c.queueDiscoveredLinks("https://studio.example/", "studio.example", qid, html)
	}

	var n, same int
	if err := db.QueryRow(`SELECT count(*),
		count(*) FILTER (WHERE url_hash = encode(digest(url, 'sha256'), 'hex')
			AND source = 'contact_page' AND status = 'pending'
			AND domain = 'studio.example' AND parent_query_id = $1)
		FROM enrichment_jobs`, qid).Scan(&n, &same); err != nil {
		t.Fatal(err)
	}
	if n != 2 || same != 2 {
		t.Errorf("rows=%d matching-trigger-shape=%d, want 2 and 2", n, same)
	}

	// No parent query: NULL, not 0 (FK would reject 0).
	c.queueDiscoveredLinks("https://other.example/", "other.example", 0, []byte(`<a href="/impressum">I</a>`))
	if err := db.QueryRow(`SELECT count(*) FROM enrichment_jobs WHERE domain='other.example' AND parent_query_id IS NULL`).Scan(&n); err != nil || n != 1 {
		t.Errorf("null-parent rows = %d (err %v), want 1", n, err)
	}
}
