package db

import (
	"testing"

	"github.com/sadewadee/serp-scraper/internal/testpg"
)

// trg_normalize_enrichment must copy raw_email_names onto business_emails,
// never clear an existing name when a later visit has none, and let a new
// non-null name replace the old one.
func TestTriggerEmailPersonName(t *testing.T) {
	d := testpg.Start(t, testpg.StartOpts{Prefix: "epn_"}).DB()
	defer d.Close()
	for i := 0; i < 2; i++ { // migrations must be idempotent
		if err := Migrate(d); err != nil {
			t.Fatalf("migrate #%d: %v", i+1, err)
		}
	}

	const domain = "epn.example"
	complete := func(hash string, names any) {
		t.Helper()
		if _, err := d.Exec(`INSERT INTO enrichment_jobs (url, url_hash, domain, source, status, raw_emails, raw_email_names)
			VALUES ($1, $2, $3, 'contact_page', 'processing', ARRAY['jane@epn.example','hi@epn.example'], $4)`,
			"https://"+domain+"/"+hash, hash, domain, names); err != nil {
			t.Fatal(err)
		}
		if _, err := d.Exec(`UPDATE enrichment_jobs SET status='completed' WHERE url_hash=$1`, hash); err != nil {
			t.Fatal(err)
		}
	}
	got := func(email string) (name, source *string) {
		t.Helper()
		if err := d.QueryRow(`SELECT be.person_name, be.person_name_source FROM business_emails be
			JOIN emails e ON e.id = be.email_id WHERE e.email = $1`, email).Scan(&name, &source); err != nil {
			t.Fatal(err)
		}
		return
	}
	check := func(label, email, wantName, wantSource string) {
		t.Helper()
		n, s := got(email)
		switch {
		case wantName == "" && (n != nil || s != nil):
			t.Errorf("%s: %s = %v/%v, want NULL", label, email, n, s)
		case wantName != "" && (n == nil || s == nil || *n != wantName || *s != wantSource):
			t.Errorf("%s: %s = %v/%v, want %s/%s", label, email, n, s, wantName, wantSource)
		}
	}

	complete("h1", `{"jane@epn.example":{"name":"Jane Tester","source":"mailto_text"}}`)
	check("first visit", "jane@epn.example", "Jane Tester", "mailto_text")
	check("first visit", "hi@epn.example", "", "")

	complete("h2", nil) // later visit, no names: existing value kept
	check("null keeps", "jane@epn.example", "Jane Tester", "mailto_text")

	complete("h3", `{"jane@epn.example":{"name":"Jane Q Tester","source":"jsonld_person"}}`)
	check("new name wins", "jane@epn.example", "Jane Q Tester", "jsonld_person")
}
