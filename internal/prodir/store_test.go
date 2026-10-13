package prodir

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/lib/pq"

	"github.com/sadewadee/serp-scraper/internal/db"
	"github.com/sadewadee/serp-scraper/internal/testpg"
)

// writeSyntheticState writes one ACE trainer (with a parsed profile), one ACE
// trainer without a profile and two ICF coaches (one fetched, one missing).
func writeSyntheticState(t *testing.T, dir string) {
	t.Helper()
	toks, err := tokenizeACE(readFixture(t, "ace_profile_SYNTHETIC.html"))
	if err != nil {
		t.Fatal(err)
	}
	prof, _ := marshalLine(struct {
		ID     string  `json:"id"`
		URL    string  `json:"url"`
		Tokens []Token `json:"tokens"`
	}{"AcE1", ACEBase + "/pro/AcE1/", toks})
	if err := os.WriteFile(filepath.Join(dir, "ace_profiles.jsonl"), prof, 0o644); err != nil {
		t.Fatal(err)
	}
	writeLines(t, filepath.Join(dir, "ace_list.jsonl"),
		`{"EncryptedCustomerNumber":"AcE1","ProfileURL":"/pro/AcE1/","FullName":"Jane Q. Fixture","City":"Exampleville","StateCode":"NY","CountryName":"US","CertDisplay":"x","_page":1}`,
		`{"EncryptedCustomerNumber":"AcE2","ProfileURL":"/pro/AcE2/","FullName":"No Profile Fixture","City":"Placeholder","StateCode":"","CountryName":"Atlantis","CertDisplay":"ACE Certified Health Coach","_page":1}`,
	)

	writeLines(t, filepath.Join(dir, "icf_list.jsonl"),
		`{"key":"IcF1","fullName":"Coach One Fixture","headline":"A headline","description":"","location":"Exampletown, NY UNITED STATES"}`,
		`{"key":"IcF2","fullName":"Coach Two Fixture","headline":"Another headline","description":"Real description","location":"Placeholder City, SOUTH AFRICA"}`,
	)
	writeLines(t, filepath.Join(dir, "icf_profiles.jsonl"),
		`{"key":"IcF1","url":"u","fields":{"Phone":["+1 555 010 0188"]},"email":"Coach.Fixture@Gmail.com","phone":"+1 555 010 0188","website":"http://www.fixture-coaching.example","location":"Exampletown, NY UNITED STATES"}`,
		`{"key":"IcF2","url":"u","missing":true}`,
	)
	b, _ := os.ReadFile("testdata/icf_countries.json")
	if err := os.WriteFile(filepath.Join(dir, "icf_countries.json"), b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func count(t *testing.T, d *sql.DB, q string) int {
	t.Helper()
	var n int
	if err := d.QueryRow(q).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestLoadMapping(t *testing.T) {
	d := testpg.Start(t, testpg.StartOpts{Prefix: "xxchk_prodir_"}).DB()
	defer d.Close()
	if err := db.Migrate(d); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	dir := t.TempDir()
	writeSyntheticState(t, dir)

	load := func() {
		t.Helper()
		for _, src := range []string{"ace", "icf"} {
			ls, err := Listings(src, dir)
			if err != nil {
				t.Fatal(err)
			}
			if code := Load(context.Background(), d, src, ls); code != ExitOK {
				t.Fatalf("%s load exit code %d", src, code)
			}
		}
	}
	load()

	type row struct {
		name, contact, category, niche, url, website, loc, desc, social string
		phone, addr, city, country, cc, ns, gs                          sql.NullString
		phones                                                          []string
		off                                                             bool
		reEnriched                                                      bool
		score                                                           sql.NullFloat64
	}
	get := func(domain string) row {
		t.Helper()
		var r row
		var phones pq.StringArray
		err := d.QueryRow(`SELECT business_name, contact_name, category, niche_category, url, website,
			COALESCE(location,''), COALESCE(description,''), social_links::text,
			phone, address, city, country, country_code::text, niche_source, geo_source, phones,
			off_niche, re_enriched_at IS NOT NULL, completeness_score
			FROM business_listings WHERE domain=$1`, domain).Scan(
			&r.name, &r.contact, &r.category, &r.niche, &r.url, &r.website, &r.loc, &r.desc, &r.social,
			&r.phone, &r.addr, &r.city, &r.country, &r.cc, &r.ns, &r.gs, &phones, &r.off, &r.reEnriched, &r.score)
		if err != nil {
			t.Fatalf("%s: %v", domain, err)
		}
		r.phones = phones
		return r
	}
	eq := func(what, got, want string) {
		t.Helper()
		if got != want {
			t.Errorf("%s = %q, want %q", what, got, want)
		}
	}

	a := get("ace1.pro.acefitness.org")
	eq("ace name", a.name, "Jane Q. Fixture")
	eq("ace contact", a.contact, "Jane Q. Fixture")
	eq("ace category", a.category, "acefitness")
	eq("ace niche", a.niche, "fitness")
	eq("ace website (own)", a.website, "http://www.fixture-fitness.example")
	eq("ace location", a.loc, "Exampleville, NY")
	eq("ace city", a.city.String, "Exampleville")
	eq("ace phone", a.phone.String, "(555) 010-0199")
	eq("ace address", a.addr.String, "EXAMPLEVILLE, NY US 10001")
	eq("ace country", a.country.String, "United States")
	eq("ace country_code", a.cc.String, "US")
	eq("ace description", a.desc, "Synthetic fixture bio. Second paragraph of the bio.")
	eq("ace phones", strings.Join(a.phones, "|"), "(555) 010-0199")
	var social map[string]string
	if err := json.Unmarshal([]byte(a.social), &social); err != nil || social["instagram"] != "https://www.instagram.com/fixture_fit" || len(social) != 1 {
		t.Errorf("ace social_links = %s", a.social)
	}
	if a.off || !a.reEnriched || a.ns.Valid || a.gs.Valid || a.score.Valid {
		t.Errorf("ace flags: off=%v re_enriched=%v niche_source=%v geo_source=%v score=%v", a.off, a.reEnriched, a.ns, a.gs, a.score)
	}

	// No profile: loads from listing data; website falls back to the profile URL; unknown country stays NULL.
	b := get("ace2.pro.acefitness.org")
	eq("ace2 niche", b.niche, "coaching")
	eq("ace2 website", b.website, ACEBase+"/pro/AcE2/")
	eq("ace2 url", b.url, ACEBase+"/pro/AcE2/")
	if b.phone.Valid || b.addr.Valid || b.country.Valid || b.cc.Valid || len(b.phones) != 0 {
		t.Errorf("ace2 should have NULL phone/address/country, got %+v", b)
	}

	i1 := get("icf1.coach.coachingfederation.org")
	eq("icf niche", i1.niche, "coaching")
	eq("icf category", i1.category, "icfcoaching")
	eq("icf website", i1.website, "http://www.fixture-coaching.example")
	eq("icf location", i1.loc, "Exampletown, NY UNITED STATES")
	eq("icf city", i1.city.String, "Exampletown")
	eq("icf country", i1.country.String, "United States")
	eq("icf country_code", i1.cc.String, "US")
	eq("icf description (headline fallback)", i1.desc, "A headline")
	eq("icf social_links", i1.social, "{}")
	eq("icf phones", strings.Join(i1.phones, "|"), "+1 555 010 0188")
	if i1.addr.Valid {
		t.Error("icf address must be NULL")
	}
	i2 := get("icf2.coach.coachingfederation.org")
	eq("icf2 website", i2.website, icfProfileURL("IcF2"))
	eq("icf2 description", i2.desc, "Real description")
	eq("icf2 country_code", i2.cc.String, "ZA")

	// Emails: ICF only, lower-cased, free_email flagged, linked with source icfcoaching.
	if n := count(t, d, `SELECT COUNT(*) FROM emails`); n != 1 {
		t.Errorf("%d emails, want 1 (ACE exposes none)", n)
	}
	var email string
	var free bool
	var src string
	if err := d.QueryRow(`SELECT e.email, e.free_email, be.source FROM emails e
		JOIN business_emails be ON be.email_id = e.id
		JOIN business_listings bl ON bl.id = be.business_id AND bl.domain='icf1.coach.coachingfederation.org'`).Scan(&email, &free, &src); err != nil {
		t.Fatal(err)
	}
	eq("email", email, "coach.fixture@gmail.com")
	eq("source", src, "icfcoaching")
	if !free {
		t.Error("gmail.com should be free_email")
	}

	// Idempotent: same counts after a second identical load.
	before := [3]int{count(t, d, `SELECT COUNT(*) FROM business_listings`), count(t, d, `SELECT COUNT(*) FROM emails`), count(t, d, `SELECT COUNT(*) FROM business_emails`)}
	load()
	after := [3]int{count(t, d, `SELECT COUNT(*) FROM business_listings`), count(t, d, `SELECT COUNT(*) FROM emails`), count(t, d, `SELECT COUNT(*) FROM business_emails`)}
	if before != after || before != [3]int{4, 1, 1} {
		t.Errorf("counts before=%v after=%v, want [4 1 1] both", before, after)
	}

	// An empty new value never blanks an existing one; a new non-empty value wins.
	again := Listing{
		Domain: "icf1.coach.coachingfederation.org", URL: icfProfileURL("IcF1"), Name: "Coach One Fixture",
		Category: icfCategory, Niche: "coaching", Website: icfProfileURL("IcF1"), Social: map[string]string{},
		Location: "Exampletown, NY UNITED STATES", Description: "A fresh description",
	}
	if code := Load(context.Background(), d, "icf", []Listing{again}); code != ExitOK {
		t.Fatalf("reload exit code %d", code)
	}
	r := get("icf1.coach.coachingfederation.org")
	eq("phone kept", r.phone.String, "+1 555 010 0188")
	eq("phones kept", strings.Join(r.phones, "|"), "+1 555 010 0188")
	eq("country kept", r.cc.String, "US")
	eq("description updated", r.desc, "A fresh description")
	if n := count(t, d, `SELECT COUNT(*) FROM business_emails`); n != 1 {
		t.Errorf("email link lost: %d", n)
	}
}

func TestPrintStats(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticState(t, dir)
	ls, err := Listings("icf", dir)
	if err != nil {
		t.Fatal(err)
	}
	var sb strings.Builder
	PrintStats(&sb, "icf", ls)
	for _, want := range []string{"records:          2", "with email:       1", "with own website: 1", "niche coaching   2", "resolved 2 (100.00%)"} {
		if !strings.Contains(sb.String(), want) {
			t.Errorf("stats missing %q:\n%s", want, sb.String())
		}
	}
}

func TestOwnWebsiteScheme(t *testing.T) {
	dir := t.TempDir()
	writeSyntheticState(t, dir)
	appendLines := func(name string, lines ...string) {
		f, err := os.OpenFile(filepath.Join(dir, name), os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		f.WriteString(strings.Join(lines, "\n") + "\n")
	}
	row := func(k string) string {
		return `{"key":"` + k + `","fullName":"Coach ` + k + `","location":"Exampletown, NY UNITED STATES"}`
	}
	prof := func(k, site string) string {
		return `{"key":"` + k + `","url":"u","fields":{"Web Site":["` + site + `"]},"email":"","phone":"","website":"` + site + `","location":""}`
	}
	appendLines("icf_list.jsonl", row("IcF3"), row("IcF4"), row("IcF5"))
	appendLines("icf_profiles.jsonl", prof("IcF3", "www.fixture-bare.example"), prof("IcF4", "fixture-bare.example/path"), prof("IcF5", "HTTPS://Secure.fixture.example"))

	ls, err := Listings("icf", dir)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, l := range ls {
		got[l.Domain] = l.Website
	}
	want := map[string]string{
		"icf1.coach.coachingfederation.org": "http://www.fixture-coaching.example", // already had a scheme
		"icf2.coach.coachingfederation.org": icfProfileURL("IcF2"),                 // profile-URL fallback untouched
		"icf3.coach.coachingfederation.org": "http://www.fixture-bare.example",
		"icf4.coach.coachingfederation.org": "http://fixture-bare.example/path",
		"icf5.coach.coachingfederation.org": "HTTPS://Secure.fixture.example", // https untouched
	}
	for d, w := range want {
		if got[d] != w {
			t.Errorf("%s website = %q, want %q", d, got[d], w)
		}
	}
	if withScheme("www.x.example") != "http://www.x.example" || withScheme("https://x.example") != "https://x.example" {
		t.Error("withScheme")
	}
}

func TestLoadMeta(t *testing.T) {
	d := testpg.Start(t, testpg.StartOpts{Prefix: "xxchk_prodir_"}).DB()
	defer d.Close()
	if err := db.Migrate(d); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	// Migrate created the table and EnsureBusinessMeta is idempotent. Load must
	// also create it when it is missing (a throwaway test DB, so removing it is
	// safe).
	if err := db.EnsureBusinessMeta(context.Background(), d); err != nil {
		t.Fatalf("EnsureBusinessMeta twice: %v", err)
	}
	if _, err := d.Exec("DROP TABLE business_meta"); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	writeSyntheticState(t, dir)

	loadAll := func() {
		t.Helper()
		for _, src := range []string{"ace", "icf"} {
			ls, err := Listings(src, dir)
			if err != nil {
				t.Fatal(err)
			}
			if code := Load(context.Background(), d, src, ls); code != ExitOK {
				t.Fatalf("%s load exit code %d", src, code)
			}
		}
	}
	metaOf := func(domain string) map[string][]string {
		t.Helper()
		rows, err := d.Query(`SELECT m.meta_key, m.meta_value FROM business_meta m
			JOIN business_listings bl ON bl.id = m.business_id WHERE bl.domain = $1
			ORDER BY m.meta_key, m.meta_value`, domain)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		out := map[string][]string{}
		for rows.Next() {
			var k, v string
			if err := rows.Scan(&k, &v); err != nil {
				t.Fatal(err)
			}
			out[k] = append(out[k], v)
		}
		return out
	}

	loadAll()
	ace1 := metaOf("ace1.pro.acefitness.org")
	if got := ace1["credential"]; !reflect.DeepEqual(got, []string{"ACE Certified Health Coach", "ACE Certified Personal Trainer"}) {
		t.Errorf("ace1 credential = %v", got)
	}
	if got := ace1["areas_of_focus"]; !reflect.DeepEqual(got, []string{"Active Aging", "Strength Training", "Weight Management"}) {
		t.Errorf("ace1 areas_of_focus = %v", got)
	}
	// List-only ACE record: credential only (from CertDisplay).
	if got := metaOf("ace2.pro.acefitness.org"); !reflect.DeepEqual(got, map[string][]string{"credential": {"ACE Certified Health Coach"}}) {
		t.Errorf("ace2 meta = %v", got)
	}
	// ICF fixtures carry no credential/designations/rate and no labels beyond
	// the contact ones: no meta rows at all.
	if got := metaOf("icf1.coach.coachingfederation.org"); len(got) != 0 {
		t.Errorf("icf1 meta = %v, want none", got)
	}

	// A second identical load leaves the same rows.
	total := count(t, d, `SELECT COUNT(*) FROM business_meta`)
	if total == 0 {
		t.Fatal("no business_meta rows written")
	}
	loadAll()
	if n := count(t, d, `SELECT COUNT(*) FROM business_meta`); n != total {
		t.Errorf("business_meta rows after reload = %d, want %d", n, total)
	}

	// A changed value list replaces that key's rows; a key the record no
	// longer carries keeps its old rows.
	l := Listing{
		Domain: "ace1.pro.acefitness.org", URL: ACEBase + "/pro/AcE1/", Name: "Jane Q. Fixture", Category: aceCategory,
		Niche: "fitness", Website: ACEBase + "/pro/AcE1/", Social: map[string]string{},
		Meta: map[string][]string{"areas_of_focus": {"Nutrition"}, "brand_new_key": {"v1", "v2"}},
	}
	if code := Load(context.Background(), d, "ace", []Listing{l}); code != ExitOK {
		t.Fatalf("reload exit code %d", code)
	}
	got := metaOf("ace1.pro.acefitness.org")
	if !reflect.DeepEqual(got["areas_of_focus"], []string{"Nutrition"}) {
		t.Errorf("areas_of_focus after replace = %v, want [Nutrition]", got["areas_of_focus"])
	}
	if !reflect.DeepEqual(got["brand_new_key"], []string{"v1", "v2"}) {
		t.Errorf("brand_new_key = %v", got["brand_new_key"])
	}
	if !reflect.DeepEqual(got["credential"], ace1["credential"]) {
		t.Errorf("credential (absent from the reload) = %v, want the old rows %v", got["credential"], ace1["credential"])
	}
	// An empty Meta touches nothing.
	l.Meta = nil
	before := count(t, d, `SELECT COUNT(*) FROM business_meta`)
	if code := Load(context.Background(), d, "ace", []Listing{l}); code != ExitOK {
		t.Fatalf("reload exit code %d", code)
	}
	if n := count(t, d, `SELECT COUNT(*) FROM business_meta`); n != before {
		t.Errorf("rows after empty-Meta reload = %d, want %d", n, before)
	}
}

func TestLoadEmailsPhones(t *testing.T) {
	d := testpg.Start(t, testpg.StartOpts{Prefix: "xxchk_prodir_"}).DB()
	defer d.Close()
	if err := db.Migrate(d); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	l := Listing{
		Domain: "ace9.pro.acefitness.org", URL: ACEBase + "/pro/AcE9/", Name: "Fixture Nine", Category: aceCategory,
		Niche: "fitness", Website: "http://someone@fixture.example", Social: map[string]string{},
		Emails: []string{"one@fixture.example", "two@gmail.com"},
		Phones: []string{"(555) 010-0199", "555.010.0142"},
	}
	load := func() {
		t.Helper()
		if code := Load(context.Background(), d, "ace", []Listing{l}); code != ExitOK {
			t.Fatalf("load exit code %d", code)
		}
	}
	state := func() (emails, links int, phone string, phones string, website string) {
		t.Helper()
		emails = count(t, d, `SELECT COUNT(*) FROM emails`)
		links = count(t, d, `SELECT COUNT(*) FROM business_emails`)
		var ph sql.NullString
		var arr pq.StringArray
		if err := d.QueryRow(`SELECT phone, phones, website FROM business_listings WHERE domain=$1`, l.Domain).Scan(&ph, &arr, &website); err != nil {
			t.Fatal(err)
		}
		return emails, links, ph.String, strings.Join(arr, "|"), website
	}
	load()
	e, k, phone, phones, website := state()
	if e != 2 || k != 2 || phone != "(555) 010-0199" || phones != "(555) 010-0199|555.010.0142" || website != "http://someone@fixture.example" {
		t.Errorf("after load: emails=%d links=%d phone=%q phones=%q website=%q", e, k, phone, phones, website)
	}
	var free bool
	if err := d.QueryRow(`SELECT free_email FROM emails WHERE email='two@gmail.com'`).Scan(&free); err != nil || !free {
		t.Errorf("gmail.com free_email = %v, %v", free, err)
	}

	load() // idempotent
	if e2, k2, p2, ps2, w2 := state(); e2 != e || k2 != k || p2 != phone || ps2 != phones || w2 != website {
		t.Errorf("reload changed state: %d %d %q %q %q", e2, k2, p2, ps2, w2)
	}

	// The website used to be an email-shaped URL; now it is the profile URL.
	l.Website = ACEBase + "/pro/AcE9/"
	l.Phones = nil // an empty list never blanks the stored numbers
	load()
	if _, _, phone, phones, website = state(); website != l.Website || phones != "(555) 010-0199|555.010.0142" || phone != "(555) 010-0199" {
		t.Errorf("after fix: website=%q phone=%q phones=%q", website, phone, phones)
	}
}
