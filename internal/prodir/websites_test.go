package prodir

import (
	"bytes"
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/lib/pq"

	"github.com/sadewadee/serp-scraper/internal/db"
	"github.com/sadewadee/serp-scraper/internal/dedup"
	"github.com/sadewadee/serp-scraper/internal/testpg"
)

func TestWebsites(t *testing.T) {
	d := testpg.Start(t, testpg.StartOpts{Prefix: "xxchk_prodir_"}).DB()
	defer d.Close()
	if err := db.Migrate(d); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	ctx := context.Background()
	must := func(q string, args ...any) {
		t.Helper()
		if _, err := d.Exec(q, args...); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	listing := func(domain, website, phone string) int64 {
		t.Helper()
		var id int64
		if err := d.QueryRow(`INSERT INTO business_listings (domain, url, business_name, category, website, phone)
			VALUES ($1, 'https://x.example.com/', 'Fixture', $2, NULLIF($3,''), NULLIF($4,'')) RETURNING id`,
			domain, aceCategory, website, phone).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	n := func(q string, args ...any) int {
		t.Helper()
		var c int
		if err := d.QueryRow(q, args...).Scan(&c); err != nil {
			t.Fatal(err)
		}
		return c
	}

	// Directory rows (synthetic domains) in id order.
	d1 := listing("1.pro.acefitness.org", "https://www.linkme-fixture.example.com/", "")
	d2 := listing("2.pro.acefitness.org", "http://linkme-fixture.example.com", "555-0100")
	d3 := listing("3.pro.acefitness.org", "https://fresh-fixture.example.com/", "")
	listing("4.pro.acefitness.org", "https://fresh2-fixture.example.com/about", "")
	listing("5.pro.acefitness.org", "https://linktr.ee/fixture", "")
	listing("6.pro.acefitness.org", "https://www.facebook.com/fixture", "")
	listing("7.pro.acefitness.org", "https://www.acefitness.org/pro/7", "")
	listing("8.pro.acefitness.org", "https://paperbell.com/fixture", "")
	listing("9.pro.acefitness.org", "https://queued-fixture.example.com/", "")
	// An organic row with scraped contact data (host written with www.).
	org := listing("www.linkme-fixture.example.com", "", "555-0199")
	// A listed host that must not be enqueued.
	listing("fresh3-fixture.example.com", "", "")
	listing("10.pro.acefitness.org", "https://fresh3-fixture.example.com/", "")
	for _, e := range []string{"a@linkme-fixture.example.com", "b@linkme-fixture.example.com"} {
		var eid int64
		if err := d.QueryRow(`INSERT INTO emails (email, domain, local_part) VALUES ($1,'linkme-fixture.example.com','x') RETURNING id`, e).Scan(&eid); err != nil {
			t.Fatal(err)
		}
		must(`INSERT INTO business_emails (business_id, email_id, source) VALUES ($1,$2,'enrichment')`, org, eid)
	}
	// Yoga Alliance rows (teacher + school) are selected through "yogaalliance"/"all".
	for _, dom := range []string{"y1.ryt.yogaalliance.org", "y2.rys.yogaalliance.org"} {
		must(`INSERT INTO business_listings (domain, url, category, website) VALUES ($1,'https://x.example.com/','yogaalliance',$2)`,
			dom, "https://"+strings.SplitN(dom, ".", 2)[0]+"-yoga-fixture.example.com.com/")
	}
	// Jobs as `prodir sites` creates them: a serp_results row, the trigger queues it.
	for _, u := range []string{"https://fresh-fixture.example.com/", "https://fresh2-fixture.example.com/about", "https://queued-fixture.example.com/"} {
		must(`INSERT INTO serp_results (url, url_hash, domain) VALUES ($1,$2,$3)`, u, dedup.HashURL(u), dedup.ExtractDomain(u))
	}
	must(`UPDATE enrichment_jobs SET status = 'dead' WHERE domain = 'fresh2-fixture.example.com'`)

	run := func(sel string, dry bool) string {
		t.Helper()
		var buf bytes.Buffer
		if err := Websites(ctx, d, sel, dry, &buf); err != nil {
			t.Fatal(err)
		}
		return strings.TrimSpace(buf.String())
	}
	linkCount := func() int { return n(`SELECT COUNT(*) FROM business_emails WHERE source = 'website_match'`) }
	check := func(label, got, want string) {
		t.Helper()
		if got != want {
			t.Errorf("%s:\n got %s\nwant %s", label, got, want)
		}
	}

	if got := run("yogaalliance", true); !strings.Contains(got, "yogaalliance: selected=2 platform_skipped=0 hosts=2 linked_rows=0 emails_linked=0 phones_filled=0 jobs_completed=0 jobs_pending=0 jobs_dead=0 jobs_none=2") {
		t.Errorf("yogaalliance: %q", got)
	}

	// dry-run: exact counts, nothing written.
	check("dry-run", run("ace", true),
		"ace: selected=10 platform_skipped=4 hosts=5 linked_rows=3 emails_linked=4 phones_filled=1 jobs_completed=0 jobs_pending=2 jobs_dead=1 jobs_none=2 dry_run=true")
	if linkCount() != 0 || n(`SELECT COUNT(*) FROM business_listings WHERE id=$1 AND phone IS NOT NULL`, d1) != 0 {
		t.Error("dry-run wrote something")
	}

	// link: emails copied to both rows, phone only where empty, organic row untouched.
	check("link", run("ace", false),
		"ace: selected=10 platform_skipped=4 hosts=5 linked_rows=3 emails_linked=4 phones_filled=1 jobs_completed=0 jobs_pending=2 jobs_dead=1 jobs_none=2 dry_run=false")
	if linkCount() != 4 {
		t.Errorf("website_match links = %d, want 4", linkCount())
	}
	phone := func(id int64) string {
		var p sql.NullString
		var ps pq.StringArray
		if err := d.QueryRow(`SELECT phone, phones FROM business_listings WHERE id=$1`, id).Scan(&p, &ps); err != nil {
			t.Fatal(err)
		}
		return p.String + "|" + strings.Join(ps, ",")
	}
	if g := phone(d1); g != "555-0199|555-0199" {
		t.Errorf("d1 phone = %q", g)
	}
	if g := phone(d2); g != "555-0100|" {
		t.Errorf("d2 phone overwritten: %q", g)
	}
	if g := phone(org); g != "555-0199|" {
		t.Errorf("organic row changed: %q", g)
	}
	if c := n(`SELECT COUNT(*) FROM business_emails WHERE business_id = $1`, org); c != 2 {
		t.Errorf("organic row emails = %d", c)
	}

	// idempotent: second run links nothing new.
	check("rerun", run("ace", false),
		"ace: selected=10 platform_skipped=4 hosts=5 linked_rows=3 emails_linked=0 phones_filled=0 jobs_completed=0 jobs_pending=2 jobs_dead=1 jobs_none=2 dry_run=false")

	// enrich completes a job: the new organic row is linked on the next run.
	h := dedup.HashURL("https://fresh-fixture.example.com/")
	must(`UPDATE enrichment_jobs SET status = 'processing', locked_by = 'w', locked_at = NOW() WHERE url_hash = $1`, h)
	must(`UPDATE enrichment_jobs SET status = 'completed', completed_at = NOW(), raw_emails = ARRAY['hello@fresh-fixture.example.com'],
		raw_business_name = 'Fresh Fixture' WHERE url_hash = $1`, h)
	check("after enrich", run("ace", false),
		"ace: selected=10 platform_skipped=4 hosts=5 linked_rows=4 emails_linked=1 phones_filled=0 jobs_completed=1 jobs_pending=1 jobs_dead=1 jobs_none=2 dry_run=false")
	if c := n(`SELECT COUNT(*) FROM business_emails be JOIN emails e ON e.id = be.email_id
		WHERE be.business_id = $1 AND e.email = 'hello@fresh-fixture.example.com' AND be.source = 'website_match'`, d3); c != 1 {
		t.Errorf("d3 did not receive the scraped email (%d)", c)
	}
}
