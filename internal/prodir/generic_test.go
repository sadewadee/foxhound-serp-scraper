package prodir

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// testSource is a Source that is never registered: the stages take it directly.
func testSource(cat string) *Source {
	return &Source{
		Slug: "gen-test", Name: "Generic Test Directory", Host: "gen-test-directory.example.org",
		Category: cat, ListingType: "studio", MinInterval: time.Nanosecond,
		List: func(context.Context, Fetcher, func(Record) error) error { return nil },
	}
}

func listingOf(t *testing.T, s *Source, r Record) Listing {
	t.Helper()
	// the own-domain filter only knows registered hosts
	extOwnDomains = append(extOwnDomains, s.Host)
	defer func() { extOwnDomains = extOwnDomains[:len(extOwnDomains)-1] }()
	return recordListing(s, r, newCountryResolver())
}

func TestSourceDomain(t *testing.T) {
	const suffix = ".gen-test.gen-test-directory.example.org"
	s := testSource("yoga_teacher")
	for _, id := range []string{"abc-123", "90001", strings.Repeat("a", 63)} {
		if got := sourceDomain(s, id); got != id+suffix {
			t.Errorf("sourceDomain(%q) = %q, want the id as label", id, got)
		}
	}
	// not a DNS label: first 16 hex chars of sha1(id)
	if got := sourceDomain(s, "Jane Doe/ID 7"); got != "90cc4f7a9dcaa15f"+suffix {
		t.Errorf("hash fallback = %q", got)
	}
	for _, id := range []string{"UPPER", "with space", "dot.ted", strings.Repeat("a", 64), ""} {
		label, rest, _ := strings.Cut(sourceDomain(s, id), ".")
		if len(label) != 16 || "."+rest != suffix {
			t.Errorf("sourceDomain(%q): label %q, want 16 hex chars", id, label)
		}
	}
	// ids that differ only in case must not collide
	if sourceDomain(s, "AbC") == sourceDomain(s, "abc") {
		t.Error("ids differing in case collide")
	}
}

func TestRecordListingBasics(t *testing.T) {
	s := testSource("yoga_teacher")
	l := listingOf(t, s, Record{
		ID: "r1", URL: "https://gen-test-directory.example.org/p/r1", Name: "Pat  Fixture", Business: "Fixture Studio",
		Address: "1 Example Road", City: "Exampleton", Region: "Exampleshire", PostalCode: "EX1 1AA", Country: "Japan",
		Emails: []string{"Pat.Fixture@Example.org"}, Phones: []string{"+81 555 010 0123"},
		Websites: []string{"https://pat-fixture.example.com"},
		Social:   map[string]string{"instagram": "https://instagram.com/pat.fixture"},
		Bio:      "Fixture bio.",
		Meta:     map[string][]string{"level": {"Level 2", "Level 2"}, "styles": {"Fixture style"}},
	})
	checks := []struct {
		name      string
		got, want any
	}{
		{"domain", l.Domain, "r1.gen-test.gen-test-directory.example.org"},
		{"url", l.URL, "https://gen-test-directory.example.org/p/r1"},
		{"name", l.Name, "Pat Fixture"},
		{"category", l.Category, "yoga_teacher"},
		{"niche", l.Niche, "yoga"},
		{"website", l.Website, "https://pat-fixture.example.com"},
		{"own website", l.OwnWebsite, true},
		{"address", l.Address, "1 Example Road"},
		{"city", l.City, "Exampleton"},
		{"location", l.Location, "Exampleton, Exampleshire, Japan"},
		{"description", l.Description, "Fixture bio."},
		{"raw country", l.RawCountry, "Japan"},
		{"country", l.Country, "Japan"},
		{"country code", l.CountryCode, "JP"},
		{"emails", l.Emails, []string{"pat.fixture@example.org"}},
		{"phones", l.Phones, []string{"+81 555 010 0123"}},
		{"social", l.Social, map[string]string{"instagram": "https://instagram.com/pat.fixture"}},
		{"meta", l.Meta, map[string][]string{
			"level": {"Level 2"}, "styles": {"Fixture style"},
			"source_directory": {"Generic Test Directory"}, "listing_type": {"studio"}, "business_name": {"Fixture Studio"},
		}},
	}
	for _, c := range checks {
		if !reflect.DeepEqual(c.got, c.want) {
			t.Errorf("%s = %#v, want %#v", c.name, c.got, c.want)
		}
	}
}

func TestRecordListingDefaults(t *testing.T) {
	s := testSource("sports_tennis")
	l := listingOf(t, s, Record{ID: "r2", Business: "Only Business Fixture"})
	if l.Niche != "" {
		t.Errorf("sports niche = %q, want empty (NULL)", l.Niche)
	}
	if l.URL != "https://"+s.Host || l.Website != l.URL || l.OwnWebsite {
		t.Errorf("without a profile URL the directory URL is used: url=%q website=%q own=%v", l.URL, l.Website, l.OwnWebsite)
	}
	if l.Name != "Only Business Fixture" {
		t.Errorf("name falls back to Business, got %q", l.Name)
	}
	if _, ok := l.Meta["business_name"]; !ok {
		t.Error("business_name meta missing")
	}

	// a per-record category overrides the source's and brings its niche
	l = listingOf(t, s, Record{ID: "r3", Category: "pilates_instructor"})
	if l.Category != "pilates_instructor" || l.Niche != "pilates" {
		t.Errorf("override: category=%q niche=%q", l.Category, l.Niche)
	}
}

func TestRecordListingWebsites(t *testing.T) {
	s := testSource("fitness_trainer")
	profile := "https://gen-test-directory.example.org/p/r"

	// directory link + social profile as "websites": no own website, profile URL stays
	l := listingOf(t, s, Record{ID: "a", URL: profile, Websites: []string{
		"https://www.gen-test-directory.example.org/other", "https://www.facebook.com/fixture.page", "not a url", "http://www.fixturename"}})
	if l.OwnWebsite || l.Website != profile {
		t.Errorf("website=%q own=%v, want the profile URL and own=false", l.Website, l.OwnWebsite)
	}
	if l.Social["facebook"] != "https://www.facebook.com/fixture.page" {
		t.Errorf("a social profile in the website field belongs in social: %v", l.Social)
	}
	if len(l.Meta["other_websites"]) != 0 {
		t.Errorf("other_websites = %v, want none", l.Meta["other_websites"])
	}

	// first usable site is own, the rest (and Bio sites) go to other_websites; a bare domain gets a scheme
	l = listingOf(t, s, Record{ID: "b", URL: profile,
		Websites: []string{"first-site.example.com", "https://second-site.example.net/x"},
		Bio:      "Also see www.bio-site.example.org and https://first-site.example.com."})
	if !l.OwnWebsite || l.Website != "http://first-site.example.com" {
		t.Errorf("website=%q own=%v", l.Website, l.OwnWebsite)
	}
	wantOther := []string{"https://second-site.example.net/x", "http://www.bio-site.example.org"}
	if !reflect.DeepEqual(l.Meta["other_websites"], wantOther) {
		t.Errorf("other_websites = %v, want %v", l.Meta["other_websites"], wantOther)
	}
}

func TestRecordListingBioExtraction(t *testing.T) {
	s := testSource("yoga_teacher")
	l := listingOf(t, s, Record{
		ID: "c", Emails: []string{"structured@example.org"}, Phones: []string{"n/a", "+1 555 010 0100"},
		Social: map[string]string{"twitter": "https://twitter.com/structured"},
		Bio: "Mail bio.person@example.com, call (555) 010-0199 or +1 555 010 0100. " +
			"Instagram: instagram.com/bio.person and twitter.com/ignored.person. www.bio-only.example.net",
	})
	if want := []string{"structured@example.org", "bio.person@example.com"}; !reflect.DeepEqual(l.Emails, want) {
		t.Errorf("emails = %v, want %v", l.Emails, want)
	}
	// "n/a" is no phone, a bio phone with the same digits is not duplicated
	if want := []string{"+1 555 010 0100", "(555) 010-0199"}; !reflect.DeepEqual(l.Phones, want) {
		t.Errorf("phones = %v, want %v", l.Phones, want)
	}
	if l.Social["twitter"] != "https://twitter.com/structured" || l.Social["instagram"] != "http://instagram.com/bio.person" {
		t.Errorf("social = %v (structured wins, bio fills the rest)", l.Social)
	}
	if want := []string{"http://www.bio-only.example.net"}; !reflect.DeepEqual(l.Meta["other_websites"], want) {
		t.Errorf("other_websites = %v, want %v", l.Meta["other_websites"], want)
	}
}

func TestRecordListingOwnDomainFiltering(t *testing.T) {
	s := testSource("yoga_teacher")
	l := listingOf(t, s, Record{
		ID:       "d",
		Emails:   []string{"person@gen-test-directory.example.org"}, // a structured value is authoritative
		Websites: []string{"https://gen-test-directory.example.org/people/d"},
		Social: map[string]string{
			"facebook":  "https://www.gen-test-directory.example.org/pages/d", // the directory's own page
			"instagram": "https://instagram.com/real.person",
		},
		Bio: "Contact us@gen-test-directory.example.org, profile https://gen-test-directory.example.org/people/d " +
			"or https://www.facebook.com/gen-test-directory.example.org.",
	})
	if want := []string{"person@gen-test-directory.example.org"}; !reflect.DeepEqual(l.Emails, want) {
		t.Errorf("emails = %v, want only the structured one %v", l.Emails, want)
	}
	if l.OwnWebsite || len(l.Meta["other_websites"]) != 0 {
		t.Errorf("a directory link must not become a website: own=%v other=%v", l.OwnWebsite, l.Meta["other_websites"])
	}
	if want := map[string]string{"instagram": "https://instagram.com/real.person",
		"facebook": "https://www.facebook.com/gen-test-directory.example.org"}; !reflect.DeepEqual(l.Social, want) {
		t.Errorf("social = %v, want %v", l.Social, want)
	}
}

func TestRecordListingCountryWhitelist(t *testing.T) {
	s := testSource("yoga_teacher")
	for _, c := range []struct{ raw, code string }{
		{"US", "US"}, {"united states", "US"}, {"Japan", "JP"}, {"jp", "JP"},
		{"XX", ""}, {"ZZZ", ""}, {"Atlantis", ""}, {"", ""}, {"AB", ""},
	} {
		l := listingOf(t, s, Record{ID: "e", Country: c.raw})
		if l.CountryCode != c.code {
			t.Errorf("country %q -> code %q, want %q", c.raw, l.CountryCode, c.code)
		}
		if (l.CountryCode == "") != (l.Country == "") {
			t.Errorf("country %q: name %q and code %q disagree", c.raw, l.Country, l.CountryCode)
		}
		if l.RawCountry != strings.TrimSpace(c.raw) {
			t.Errorf("RawCountry = %q, want %q", l.RawCountry, c.raw)
		}
	}
}

func TestMergeRecord(t *testing.T) {
	base := Record{ID: "m", URL: "u1", Name: "List Name", City: "List City", Bio: "list bio",
		Emails: []string{"list@example.org"}, Social: map[string]string{"instagram": "i1", "twitter": "t1"},
		Meta: map[string][]string{"level": {"L1"}, "styles": {"S1"}}}
	detail := Record{ID: "other", Name: "", City: "", Address: " 1  Road ", Bio: "detail bio",
		Phones: []string{"+1 555 010 0100"}, Social: map[string]string{"twitter": "t2", "facebook": "f2"},
		Meta: map[string][]string{"level": {"L2"}, "credential": {"C2"}}}
	got := mergeRecord(base, detail)
	want := Record{ID: "m", URL: "u1", Name: "List Name", City: "List City", Address: "1 Road", Bio: "detail bio",
		Emails: []string{"list@example.org"}, Phones: []string{"+1 555 010 0100"},
		Social: map[string]string{"instagram": "i1", "twitter": "t2", "facebook": "f2"},
		Meta:   map[string][]string{"level": {"L2"}, "styles": {"S1"}, "credential": {"C2"}}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("merge = %#v\nwant   %#v", got, want)
	}
	// the inputs are not modified
	if base.Meta["level"][0] != "L1" || base.Social["twitter"] != "t1" {
		t.Error("mergeRecord modified its base")
	}
	if got := mergeRecord(Record{ID: "x", Missing: true}, Record{}); !got.Missing {
		t.Error("Missing must survive a merge")
	}
}

func writeRecords(t *testing.T, path string, recs ...Record) {
	t.Helper()
	var lines []string
	for _, r := range recs {
		b, _ := json.Marshal(r)
		lines = append(lines, string(b))
	}
	writeLines(t, path, lines...)
}

// profileServer serves /p/<id>: 404 for "gone", 400 for "broken" until fixed,
// 200 with a body otherwise; it counts hits per id.
type profileServer struct {
	mu     sync.Mutex
	hits   map[string]int
	fixed  bool
	server *httptest.Server
}

func newProfileServer() *profileServer {
	p := &profileServer{hits: map[string]int{}}
	p.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/p/")
		p.mu.Lock()
		p.hits[id]++
		fixed := p.fixed
		p.mu.Unlock()
		switch {
		case id == "gone":
			http.NotFound(w, r)
		case id == "broken" && !fixed:
			http.Error(w, "bad", http.StatusBadRequest)
		default:
			fmt.Fprintf(w, "email:%s@example.org", id)
		}
	}))
	return p
}

func (p *profileServer) hitsOf(id string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.hits[id]
}

func TestProfilesResumableAndMissing(t *testing.T) {
	ps := newProfileServer()
	defer ps.server.Close()
	s := testSource("yoga_teacher")
	s.Detail = func(ctx context.Context, f Fetcher, r Record) (Record, error) {
		p, err := f.Get(ctx, ps.server.URL+"/p/"+r.ID, nil)
		if err != nil {
			return Record{}, err
		}
		if err := p.Err(); err != nil {
			return Record{}, err
		}
		return Record{Emails: []string{strings.TrimPrefix(string(p.Body), "email:")}, Meta: map[string][]string{"detail": {"yes"}}}, nil
	}

	dir := t.TempDir()
	writeRecords(t, listPath(dir, s),
		Record{ID: "ok", Name: "Ok Fixture"},
		Record{ID: "gone", Name: "Gone Fixture", City: "Exampleton"},
		Record{ID: "broken", Name: "Broken Fixture"},
		Record{ID: "ok", Name: "Ok Fixture Newer", Meta: map[string][]string{"list": {"yes"}}}, // a re-run of list appended a duplicate: last wins
	)

	code, err := SourceProfiles(context.Background(), dir, s, 2, 0)
	if err != nil || code != ExitErrors {
		t.Fatalf("run 1: code=%d err=%v, want ExitErrors (one failed profile)", code, err)
	}
	profs, _ := readRecords(profilesPath(dir, s))
	byID := map[string]Record{}
	for _, p := range profs {
		byID[p.ID] = p
	}
	if len(byID) != 2 {
		t.Fatalf("profiles written: %v, want ok + gone", profs)
	}
	if o := byID["ok"]; !reflect.DeepEqual(o.Emails, []string{"ok@example.org"}) || o.Name != "Ok Fixture Newer" ||
		!reflect.DeepEqual(o.Meta, map[string][]string{"list": {"yes"}, "detail": {"yes"}}) || o.Missing {
		t.Errorf("ok profile = %#v", o)
	}
	if g := byID["gone"]; !g.Missing || g.City != "Exampleton" || len(g.Emails) != 0 {
		t.Errorf("gone profile = %#v, want the list record with missing=true", g)
	}
	var errLines int
	var errRec struct{ ID, URL, Error string }
	readJSONL(filepath.Join(dir, "gen-test_profiles_errors.jsonl"), func(l []byte) {
		errLines++
		json.Unmarshal(l, &errRec)
	})
	if errLines != 1 || errRec.ID != "broken" || !strings.Contains(errRec.Error, "HTTP 400") {
		t.Errorf("errors file: %d lines, last %+v", errLines, errRec)
	}

	// run 2: the server is fixed; only the failed id is fetched again
	ps.mu.Lock()
	ps.fixed = true
	ps.mu.Unlock()
	if code, err := SourceProfiles(context.Background(), dir, s, 2, 0); err != nil || code != ExitOK {
		t.Fatalf("run 2: code=%d err=%v", code, err)
	}
	if ps.hitsOf("ok") != 1 || ps.hitsOf("gone") != 1 || ps.hitsOf("broken") != 2 {
		t.Errorf("hits ok=%d gone=%d broken=%d, want 1 1 2 (resumable; a 404 is never refetched)",
			ps.hitsOf("ok"), ps.hitsOf("gone"), ps.hitsOf("broken"))
	}
	// run 3: nothing left
	SourceProfiles(context.Background(), dir, s, 2, 0)
	if ps.hitsOf("broken") != 2 {
		t.Error("run 3 refetched a finished profile")
	}

	// the loader reads list + profiles joined, deduped by id (last wins)
	recs, err := loadRecords(dir, s)
	if err != nil || len(recs) != 3 {
		t.Fatalf("loadRecords = %v, %v", recs, err)
	}
	if recs[0].ID != "ok" || recs[0].Emails[0] != "ok@example.org" || !recs[1].Missing || recs[2].Emails[0] != "broken@example.org" {
		t.Errorf("joined records: %#v", recs)
	}
}

func TestProfilesLimitAndNoDetail(t *testing.T) {
	ps := newProfileServer()
	defer ps.server.Close()
	s := testSource("yoga_teacher")
	dir := t.TempDir()
	writeRecords(t, listPath(dir, s), Record{ID: "a"}, Record{ID: "b"}, Record{ID: "c"})

	// no Detail: nothing is fetched or written
	if code, err := SourceProfiles(context.Background(), dir, s, 1, 0); err != nil || code != ExitOK {
		t.Fatalf("no detail: code=%d err=%v", code, err)
	}
	if _, err := os.Stat(profilesPath(dir, s)); !errors.Is(err, os.ErrNotExist) {
		t.Error("a profiles file was created for a source without Detail")
	}

	s.Detail = func(ctx context.Context, f Fetcher, r Record) (Record, error) {
		_, err := f.Get(ctx, ps.server.URL+"/p/"+r.ID, nil)
		return Record{Bio: "bio " + r.ID}, err
	}
	if code, _ := SourceProfiles(context.Background(), dir, s, 1, 2); code != ExitOK {
		t.Fatalf("limit run: code=%d", code)
	}
	if profs, _ := readRecords(profilesPath(dir, s)); len(profs) != 2 {
		t.Errorf("limit 2 wrote %d profiles", len(profs))
	}
}

func TestProfilesRejectsBadDetail(t *testing.T) {
	s := testSource("yoga_teacher")
	s.Detail = func(context.Context, Fetcher, Record) (Record, error) {
		return Record{Category: "knitting"}, nil
	}
	dir := t.TempDir()
	writeRecords(t, listPath(dir, s), Record{ID: "a"})
	if code, _ := SourceProfiles(context.Background(), dir, s, 1, 0); code != ExitErrors {
		t.Errorf("code = %d, want ExitErrors for an unknown category", code)
	}
	if profs, _ := readRecords(profilesPath(dir, s)); len(profs) != 0 {
		t.Error("an invalid profile was stored")
	}
}

func TestSourceListLimitAndValidation(t *testing.T) {
	s := testSource("yoga_teacher")
	pages := 0
	s.List = func(ctx context.Context, f Fetcher, emit func(Record) error) error {
		for p := 1; p <= 3; p++ {
			pages++
			for i := 0; i < 2; i++ {
				if err := emit(Record{ID: fmt.Sprintf("p%d-%d", p, i), Name: "n"}); err != nil {
					return err
				}
			}
		}
		return nil
	}
	dir := t.TempDir()
	if code, err := SourceList(context.Background(), dir, s, 3); err != nil || code != ExitOK {
		t.Fatalf("code=%d err=%v", code, err)
	}
	if recs, _ := readRecords(listPath(dir, s)); len(recs) != 3 {
		t.Errorf("limit 3 wrote %d records", len(recs))
	}
	if pages != 2 {
		t.Errorf("List kept going after the limit: %d pages", pages)
	}

	// an invalid record fails the stage
	s.List = func(ctx context.Context, f Fetcher, emit func(Record) error) error {
		return emit(Record{Name: "no id"})
	}
	if code, err := SourceList(context.Background(), t.TempDir(), s, 0); err == nil || code != ExitErrors {
		t.Errorf("record without ID: code=%d err=%v", code, err)
	}
	s.List = func(ctx context.Context, f Fetcher, emit func(Record) error) error {
		return emit(Record{ID: "x", Category: "knitting"})
	}
	if code, err := SourceList(context.Background(), t.TempDir(), s, 0); err == nil || code != ExitErrors {
		t.Errorf("unknown category: code=%d err=%v", code, err)
	}
}

func TestSourceExport(t *testing.T) {
	s := testSource("yoga_teacher")
	dir := t.TempDir()
	writeRecords(t, listPath(dir, s),
		Record{ID: "a", Name: "A, Fixture", Emails: []string{"a@example.org", "b@example.org"},
			Social: map[string]string{"instagram": "https://instagram.com/a", "facebook": "https://facebook.com/a"},
			Meta:   map[string][]string{"level": {"L1", "L2"}}},
		Record{ID: "b", Name: "B", Category: "pilates_instructor", Meta: map[string][]string{"styles": {"S"}}},
	)
	out := filepath.Join(dir, "x.csv")
	if err := SourceExport(dir, s, out); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(out)
	lines := strings.Split(strings.TrimSuffix(strings.TrimPrefix(string(b), "\xef\xbb\xbf"), "\r\n"), "\r\n")
	if len(lines) != 3 {
		t.Fatalf("csv lines: %q", lines)
	}
	if want := "id,url,name,business,address,city,region,postal_code,country,emails,phones,websites,social,category,bio,level,styles"; lines[0] != want {
		t.Errorf("header = %q\nwant     %q", lines[0], want)
	}
	if want := `a,,"A, Fixture",,,,,,,a@example.org; b@example.org,,,facebook=https://facebook.com/a; instagram=https://instagram.com/a,yoga_teacher,,L1; L2,`; lines[1] != want {
		t.Errorf("row a = %q\nwant    %q", lines[1], want)
	}
	if !strings.Contains(lines[2], ",pilates_instructor,") || !strings.HasSuffix(lines[2], ",S") {
		t.Errorf("row b = %q", lines[2])
	}
	// default path
	if err := SourceExport(dir, s, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "gen-test.csv")); err != nil {
		t.Error(err)
	}
}

func TestListingsAndStatsForGenericSource(t *testing.T) {
	// iyengaruk is registered; feed it a synthetic state dir
	s, _ := LookupSource("iyengaruk")
	dir := t.TempDir()
	writeRecords(t, listPath(dir, s),
		Record{ID: "alex-fixture-90001", URL: iyengarBase + "/teacher/alex-fixture-90001/", Name: "Alex Fixture", City: "Exampleton",
			Meta: map[string][]string{"level": {"Level 2"}}},
		Record{ID: "blair-placeholder-90002", Name: "Blair Placeholder", Country: "Atlantis"},
	)
	ls, err := Listings("iyengaruk", dir)
	if err != nil || len(ls) != 2 {
		t.Fatalf("Listings = %v, %v", ls, err)
	}
	if ls[0].Domain != "alex-fixture-90001.iyengaruk.iyengaryoga.org.uk" || ls[0].Niche != "yoga" {
		t.Errorf("listing = %+v", ls[0])
	}
	var buf bytes.Buffer
	PrintStats(&buf, "iyengaruk", ls)
	for _, want := range []string{"source: iyengaruk", "records:          2", "niche yoga       2", "unresolved \"Atlantis\"",
		"meta level", "meta source_directory", "meta listing_type"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("stats missing %q:\n%s", want, buf.String())
		}
	}
	if _, err := Listings("no-such-source", dir); err == nil {
		t.Error("an unknown source must fail")
	}

	buf.Reset()
	PrintStats(&buf, "x", []Listing{{Domain: "d", Niche: ""}})
	if !strings.Contains(buf.String(), "niche (null)") {
		t.Errorf("an empty niche must print as (null):\n%s", buf.String())
	}
}
