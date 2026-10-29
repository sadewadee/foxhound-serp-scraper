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
	n := func(q string, args ...any) int {
		t.Helper()
		var c int
		if err := d.QueryRow(q, args...).Scan(&c); err != nil {
			t.Fatal(err)
		}
		return c
	}
	row := func(domain, category, name, website, phone string) int64 {
		t.Helper()
		var id int64
		if err := d.QueryRow(`INSERT INTO business_listings (domain, url, business_name, category, website, phone)
			VALUES ($1, 'https://profile.example.com/', $2, $3, NULLIF($4,''), NULLIF($5,'')) RETURNING id`,
			domain, name, category, website, phone).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	addEmail := func(id int64, source, email string) {
		t.Helper()
		var eid int64
		if err := d.QueryRow(`INSERT INTO emails (email, domain, local_part) VALUES ($1,$2,'x')
			ON CONFLICT (email) DO UPDATE SET domain = EXCLUDED.domain RETURNING id`,
			email, email[strings.IndexByte(email, '@')+1:]).Scan(&eid); err != nil {
			t.Fatal(err)
		}
		must(`INSERT INTO business_emails (business_id, email_id, source) VALUES ($1,$2,$3)`, id, eid, source)
	}
	// dir creates a directory row (synthetic domain); organic creates the row enrich
	// made for the website host, with its scraped emails.
	seq := 0
	dir := func(name, website, phone string) int64 {
		seq++
		return row(strings.Repeat("x", seq)+".pro.acefitness.org", aceCategory, name, website, phone)
	}
	organic := func(host, phone string, emails ...string) {
		t.Helper()
		id := row(host, "", "", "", phone)
		for _, e := range emails {
			addEmail(id, "enrichment", e)
		}
	}

	// A: no page email, several site emails -> exactly the person's own; empty phone filled.
	a := dir("Jane Fixture", "https://janefixture.example.com/", "")
	organic("www.janefixture.example.com", "555-0101",
		"info@janefixture.example.com", "jane@janefixture.example.com", "bob@janefixture.example.com")
	// B: has a page email -> nothing linked, however good the site email is.
	b := dir("Page Person", "https://pageperson.example.com/", "")
	addEmail(b, aceCategory, "page.person@mail.example.com")
	organic("pageperson.example.com", "", "page.person@pageperson.example.com")
	// four rows sharing one host: an employer, skipped for email and phone.
	var shared []int64
	for _, nm := range []string{"Ann One", "Ben Two", "Cat Three", "Dan Four"} {
		shared = append(shared, dir(nm, "https://studio-shared.example.com/", ""))
	}
	organic("studio-shared.example.com", "555-0102", "hello@studio-shared.example.com")
	// a vendor address found on three unrelated sites is dropped.
	var vend []int64
	for _, h := range []string{"vendorvictim", "vendortwo", "vendorthree"} {
		vend = append(vend, dir("Vee "+h, "https://"+h+".example.com/", ""))
		organic(h+".example.com", "", "help@templatevendor.example.com")
	}
	// K: one address that is nobody's own -> no pick.
	k := dir("Kim Lee", "https://unrelated-site.example.com/", "")
	organic("unrelated-site.example.com", "", "zed@unrelated-site.example.com")
	// a platform website is skipped; a pending job for A's host.
	dir("Lin Ktree", "https://linktr.ee/fixture", "")
	u := "https://janefixture.example.com/"
	must(`INSERT INTO serp_results (url, url_hash, domain) VALUES ($1,$2,$3)`, u, dedup.HashURL(u), dedup.ExtractDomain(u))
	// Yoga Alliance rows are selected through "yogaalliance" / "all".
	for _, dom := range []string{"y1.ryt.yogaalliance.org", "y2.rys.yogaalliance.org"} {
		must(`INSERT INTO business_listings (domain, url, category, website) VALUES ($1,'https://profile.example.com/','yogaalliance',$2)`,
			dom, "https://"+strings.SplitN(dom, ".", 2)[0]+"-yoga-fixture.example.com/")
	}

	run := func(sel string, dry bool) string {
		t.Helper()
		var buf bytes.Buffer
		if err := Websites(ctx, d, sel, dry, &buf); err != nil {
			t.Fatal(err)
		}
		return strings.TrimSpace(buf.String())
	}
	check := func(label, got, want string) {
		t.Helper()
		if got != want {
			t.Errorf("%s:\n got %s\nwant %s", label, got, want)
		}
	}
	webLinks := func(id int64) string {
		t.Helper()
		rs, err := d.Query(`SELECT e.email FROM business_emails be JOIN emails e ON e.id = be.email_id
			WHERE be.business_id = $1 AND be.source = 'website_match' ORDER BY 1`, id)
		if err != nil {
			t.Fatal(err)
		}
		defer rs.Close()
		var out []string
		for rs.Next() {
			var e string
			rs.Scan(&e)
			out = append(out, e)
		}
		return strings.Join(out, ",")
	}

	if got := run("yogaalliance", true); !strings.HasPrefix(got, "yogaalliance: selected=2 platform_skipped=0 hosts=2 ") {
		t.Errorf("yogaalliance: %q", got)
	}

	const pre = "ace: selected=11 platform_skipped=1 hosts=7 shared_host_rows=4 page_email_rows=1 "
	check("dry-run", run("ace", true),
		pre+"web_email_rows=0 no_pick=1 organic_matched_rows=6 emails_linked=1 phones_filled=1 jobs_completed=0 jobs_pending=1 jobs_dead=0 jobs_none=6 dry_run=true")
	if n(`SELECT COUNT(*) FROM business_emails WHERE source = 'website_match'`) != 0 ||
		n(`SELECT COUNT(*) FROM business_listings WHERE id=$1 AND phone IS NOT NULL`, a) != 0 {
		t.Fatal("dry-run wrote something")
	}

	check("run", run("ace", false),
		pre+"web_email_rows=0 no_pick=1 organic_matched_rows=6 emails_linked=1 phones_filled=1 jobs_completed=0 jobs_pending=1 jobs_dead=0 jobs_none=6 dry_run=false")
	if g := webLinks(a); g != "jane@janefixture.example.com" {
		t.Errorf("A website emails = %q, want only the person's own", g)
	}
	if n(`SELECT COUNT(*) FROM business_emails WHERE business_id = $1`, a) != 1 {
		t.Error("A must hold exactly one email")
	}
	if g := webLinks(b); g != "" || n(`SELECT COUNT(*) FROM business_emails WHERE business_id = $1`, b) != 1 {
		t.Errorf("B has a page email, got website links %q", g)
	}
	for _, id := range append(append(append([]int64{}, shared...), vend...), k) {
		if n(`SELECT COUNT(*) FROM business_emails WHERE business_id = $1`, id) != 0 {
			t.Errorf("row %d must have no email", id)
		}
	}
	var ph sql.NullString
	var phs pq.StringArray
	d.QueryRow(`SELECT phone, phones FROM business_listings WHERE id=$1`, a).Scan(&ph, &phs)
	if ph.String != "555-0101" || len(phs) != 1 {
		t.Errorf("A phone = %q %v", ph.String, phs)
	}
	for _, id := range shared {
		if n(`SELECT COUNT(*) FROM business_listings WHERE id=$1 AND phone IS NOT NULL`, id) != 0 {
			t.Errorf("shared-host row %d got a phone", id)
		}
	}
	// the organic rows are untouched
	if n(`SELECT COUNT(*) FROM business_emails WHERE source = 'enrichment'`) != 9 {
		t.Error("organic email links changed")
	}

	// idempotent: A already has its website email and is not re-picked.
	check("rerun", run("ace", false),
		pre+"web_email_rows=1 no_pick=1 organic_matched_rows=6 emails_linked=0 phones_filled=0 jobs_completed=0 jobs_pending=1 jobs_dead=0 jobs_none=6 dry_run=false")
	if n(`SELECT COUNT(*) FROM business_emails WHERE source = 'website_match'`) != 1 {
		t.Error("rerun changed the links")
	}
}

// chooseWebEmail mirrors crmload/pick_web_email.py choose().
func TestChooseWebEmail(t *testing.T) {
	site := "janefixture.example.com"
	tests := []struct {
		name, first string
		emails      []string
		want        string
	}{
		{"own name beats generic", "Jane Fixture", []string{"info@" + site, "jane@" + site, "bob@" + site}, "jane@" + site},
		{"generic on own domain", "Jane Fixture", []string{"hello@" + site, "bob@" + site}, "hello@" + site},
		{"other staff only", "Kim Lee", []string{"zed@unrelated.example.com"}, ""},
		{"lone free-mail", "Kim Lee", []string{"someone@gmail.com"}, "someone@gmail.com"},
		{"two free-mails, no name", "Kim Lee", []string{"a1@gmail.com", "b2@gmail.com"}, ""},
		{"placeholder dropped", "Jane Fixture", []string{"janedoe@" + site, "info@" + site}, "info@" + site},
		{"initials", "Jane Fixture", []string{"jf@" + site, "zed@" + site}, "jf@" + site},
		{"free-mail ranks after non-free at equal rank", "Jane Fixture", []string{"jane@gmail.com", "jane@" + site}, "jane@" + site},
	}
	for _, tc := range tests {
		var cands []webCand
		for _, e := range tc.emails {
			cands = append(cands, webCand{e, site})
		}
		got := ""
		if c, ok := chooseWebEmail(cands, tc.first, ""); ok {
			got = c.email
		}
		if got != tc.want {
			t.Errorf("%s: got %q want %q", tc.name, got, tc.want)
		}
	}
}

func TestSiteEmailSet(t *testing.T) {
	raw := map[string][]string{
		"own.example.com":   {"hi@own.example.com", "emailhi@own.example.com", "icon@2x.png", "noreply@own.example.com"},
		"big.example.com":   {"a@big.example.com", "b@big.example.com", "c@big.example.com", "x@other.example.com", "y@other.example.com"},
		"crowd.example.com": {"hi@crowd.example.com"},
		"v1.example.com":    {"help@vendor.example.com", "credit@gmail.com"},
		"v2.example.com":    {"help@vendor.example.com", "credit@gmail.com"},
		"v3.example.com":    {"help@vendor.example.com", "credit@gmail.com", "real@v3.example.com"},
	}
	share := map[string]int{"own.example.com": 1, "big.example.com": 1, "crowd.example.com": 4, "v1.example.com": 1, "v2.example.com": 1, "v3.example.com": 1}
	got := siteEmailSet(raw, share)
	want := map[string]string{
		"own.example.com": "hi@own.example.com",                                    // glued, image-like and noreply addresses dropped
		"big.example.com": "a@big.example.com,b@big.example.com,c@big.example.com", // over-full site: own-domain only,
		"v3.example.com":  "real@v3.example.com",                                   // vendor domain and a free-mail address on 3 hosts/profiles dropped
	}
	for site, w := range want {
		if g := strings.Join(got[site], ","); g != w {
			t.Errorf("%s = %q, want %q", site, g, w)
		}
	}
	for _, site := range []string{"crowd.example.com", "v1.example.com", "v2.example.com"} {
		if len(got[site]) != 0 {
			t.Errorf("%s = %v, want none", site, got[site])
		}
	}
}
