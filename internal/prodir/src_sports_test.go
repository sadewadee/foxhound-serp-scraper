package prodir

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"testing"
)

func sport80URL(host string, widget, page int) string {
	return fmt.Sprintf("https://%s/api/public/widget/data/new/%d?p=%d&i=100&s=&l=&d=0&f=", host, widget, page)
}

func TestSport80Sources(t *testing.T) {
	us := "United States"
	cases := []struct {
		slug, host, fixture string
		widget              int
		want                []Record
	}{
		{"usacycling", "usacycling.sport80.com", "usacycling_page_SYNTHETIC.json", 2, []Record{
			{ID: "900001", Name: "Fixture Coachone", Country: us, Websites: []string{"www.example.com/coaching"},
				Meta: map[string][]string{"credential": {"Level 2 Coach"}, "specialties": {"Junior", "Club/Team", "Remote/Online"}}},
			{ID: "900002", Name: "Sample Coachtwo", Country: us, Meta: map[string][]string{"credential": {"Volunteer Coach"}}},
		}},
		{"usatkd", "usataekwondo.sport80.com", "usatkd_page_SYNTHETIC.json", 1, []Record{
			{ID: "910001", Name: "Fixture Taekwondo Academy", Country: us, Emails: []string{"info@example.org"}, Websites: []string{"www.example.org/tkd"},
				Meta: map[string][]string{"state_association": {"Exampleland State Federation"}, "membership": {"Silver Club Membership"}}},
			{ID: "910002", Name: "Sample Dojang", Country: us, Meta: map[string][]string{"membership": {"Bronze Club Membership"}}},
		}},
		{"usatf", "usatf.sport80.com", "usatf_page_SYNTHETIC.json", 3, []Record{
			{ID: "920001", Name: "Fixture Sprintcoach", Country: us, Meta: map[string][]string{
				"credential":     {"USATF Level 2 - Sprints/Hurdles/Relays"},
				"certifications": {"Cross Country Specialist Course", "RRCA Level 1"}}}, // CEU courses and accomplishments are not credentials
			{ID: "920002", Name: "Sample Jumpscoach", Country: us, Meta: map[string][]string{}},
		}},
		{"usajudo", "usajudo.sport80.com", "usajudo_clubs_SYNTHETIC.json", 1, []Record{
			{ID: "930001", Name: "Fixture Judo Club", Address: "1 Example Street, Suite 2", City: "Exampleton", Region: "Exampleland",
				PostalCode: "90000", Country: "United States of America", Emails: []string{"dojo@example.org"}, Phones: []string{"+1 555-010-0199"},
				Websites: []string{"www.example.org/judo"}, Bio: "Senior: Monday, Wednesday. Senior schedule: 7:00PM - 8:00PM. Junior: Tuesday",
				Meta: map[string][]string{"state_association": {"Exampleland Judo Inc."}, "membership": {"Club Membership"}, "classes": {"Senior", "Junior"}}},
			// "n/a" is no phone and "not an email" no email
			{ID: "930002", Name: "Sample Judo School", Address: "22 Placeholder Road", City: "Sampleford", Region: "SF",
				PostalCode: "90001-1234", Country: "United States of America", Meta: map[string][]string{}},
		}},
		{"usajudo-coaches", "usajudo.sport80.com", "usajudo_coaches_SYNTHETIC.json", 3, []Record{
			{ID: "940001", Name: "Fixture Sensei", Business: "Fixture Judo Club", Country: us, Meta: map[string][]string{
				"credential": {"Level 2 Coach"}, "state_association": {"Exampleland Judo Inc."}, "membership": {"Adult Membership"},
				"cert_expires": {"1/31/2099"}, "membership_valid_to": {"12/31/2099"}}},
			{ID: "940002", Name: "Sample Sensei", Country: us, Meta: map[string][]string{"credential": {"Level 1 Coach"}}},
		}},
	}
	for _, c := range cases {
		t.Run(c.slug, func(t *testing.T) {
			s := mustSource(t, c.slug)
			f := &fakeFetcher{pages: map[string]Page{sport80URL(c.host, c.widget, 0): {Body: readFixture(t, c.fixture)}}}
			got := collect(t, s, f)
			for i := range got { // an absent value is nil or empty, either is "none"
				if len(got[i].Emails) == 0 {
					got[i].Emails = nil
				}
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("records = %#v\nwant      %#v", got, c.want)
			}
			if len(f.calls) != 1 { // a short page ends the walk
				t.Errorf("requests = %v", f.calls)
			}
			if s.Detail != nil || s.Host != c.host {
				t.Errorf("Detail=%v Host=%q", s.Detail != nil, s.Host)
			}
		})
	}
}

func s80Page(total string, ids ...int) Page {
	var rows []string
	for _, id := range ids {
		rows = append(rows, fmt.Sprintf(`{"id":"%d","name":"Fixture Member %d","info":[]}`, id, id))
	}
	return Page{Body: []byte(fmt.Sprintf(`{"total":%q,"items_per_page":100,"data":[%s]}`, total, strings.Join(rows, ",")))}
}

func TestSport80Paging(t *testing.T) {
	s := mustSource(t, "usatf")
	u := func(p int) string { return sport80URL("usatf.sport80.com", 3, p) }
	seq := func(from, to int) (ids []int) {
		for i := from; i <= to; i++ {
			ids = append(ids, i)
		}
		return
	}

	// a full page continues until a short one; an id repeated across pages is one record
	f := &fakeFetcher{pages: map[string]Page{u(0): s80Page("102", seq(1, 100)...), u(1): s80Page("102", 100, 101, 102)}}
	if got := collect(t, s, f); len(got) != 102 || len(f.calls) != 2 {
		t.Errorf("two pages: %d records, requests %v", len(got), f.calls)
	}

	// the reported total stops the walk even when the last page is full
	f = &fakeFetcher{pages: map[string]Page{u(0): s80Page("100", seq(1, 100)...), u(1): s80Page("100", 101)}}
	if got := collect(t, s, f); len(got) != 100 || len(f.calls) != 1 {
		t.Errorf("total reached: %d records, requests %v", len(got), f.calls)
	}

	// emit's error (the -limit) stops the walk at once
	f = &fakeFetcher{pages: map[string]Page{u(0): s80Page("500", seq(1, 100)...), u(1): s80Page("500", seq(101, 200)...)}}
	if err := s.List(context.Background(), f, func(Record) error { return errListLimit }); !errors.Is(err, errListLimit) || len(f.calls) != 1 {
		t.Errorf("limit: err=%v requests=%v", err, f.calls)
	}

	// HTTP errors and non-JSON fail the stage
	for _, p := range []Page{{Status: 500}, {Body: []byte("<html>blocked</html>")}} {
		f = &fakeFetcher{pages: map[string]Page{u(0): p}}
		if err := s.List(context.Background(), f, func(Record) error { return nil }); err == nil {
			t.Errorf("page %+v must fail List", p)
		}
	}
}

func TestSport80Val(t *testing.T) {
	var r s80Row
	const j = `{"id":7,"name":"Fixture","info":[{"title":"A","value":{"type":"label","text":"<b>Current</b> &amp; more"}},{"title":"B","value":"x"},{"title":"C","value":null},{"title":"D","value":3}]}`
	if err := json.Unmarshal([]byte(j), &r); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprintf("%s|%s|%s|%s|%s|%s", r.ID, r.info("A"), r.info("B"), r.info("C"), r.info("D"), r.info("Z")); got != "7|Current & more|x||3|" {
		t.Errorf("values = %q", got)
	}
}

func TestRRCA(t *testing.T) {
	s := mustSource(t, "rrca")
	listURL := rrcaBase + "/coaches/"
	f := &fakeFetcher{pages: map[string]Page{listURL: {Body: readFixture(t, "rrca_list_SYNTHETIC.html")}}}
	got := collect(t, s, f)
	want := []Record{
		{ID: "fixture-runner", URL: rrcaBase + "/coach/fixture-runner/", Name: "Fixture Runner", City: "Exampleton", Region: "EX", Country: "US"},
		{ID: "sample-pacer-2", URL: rrcaBase + "/coach/sample-pacer-2/", Name: "Sample Pacer", City: "Sampleford", Region: "ON", Country: "CA"},
	}
	// the row without a profile link is no entry and the repeated slug is one
	if !reflect.DeepEqual(got, want) || len(f.calls) != 1 {
		t.Errorf("records = %#v\nwant      %#v (requests %v)", got, want, f.calls)
	}
	if _, err := collectErr(s, &fakeFetcher{pages: map[string]Page{listURL: {Body: []byte("<html>new layout</html>")}}}); err == nil {
		t.Error("a page without the table must fail List")
	}

	d, err := s.Detail(context.Background(), &fakeFetcher{pages: map[string]Page{want[0].URL: {Body: readFixture(t, "rrca_profile_SYNTHETIC.html")}}}, want[0])
	if err != nil {
		t.Fatal(err)
	}
	wantD := Record{
		Name: "Fixture Runner", Emails: []string{"runner@example.org"}, Websites: []string{"http://www.example.org/run"},
		PostalCode: "90000-1234", Bio: "Synthetic coach of fixtures.",
		Meta: map[string][]string{"credential": {"Certified Coach 1", "Certified Coach 2"}, "specialties": {"In-person coaching services", "Virtual coaching services"}},
	}
	if !reflect.DeepEqual(d, wantD) {
		t.Errorf("detail = %#v\nwant     %#v", d, wantD)
	}
	d, err = s.Detail(context.Background(), &fakeFetcher{pages: map[string]Page{want[1].URL: {Body: readFixture(t, "rrca_profile_bare_SYNTHETIC.html")}}}, want[1])
	if err != nil || len(d.Emails) != 0 || len(d.Websites) != 0 || d.PostalCode != "M5V 3A8" || d.Name != "Sample Pacer" {
		t.Errorf("bare detail = %#v, %v", d, err)
	}
	detailErrors(t, s, want[0], "rrca")
}

func TestLTA(t *testing.T) {
	s := mustSource(t, "lta")
	u := func(n int) string { return fmt.Sprintf("%s/play/find-a-coach?p=%d", ltaBase, n) }
	f := &fakeFetcher{pages: map[string]Page{
		u(1): {Body: readFixture(t, "lta_list1_SYNTHETIC.html")},
		u(2): {Body: readFixture(t, "lta_list2_SYNTHETIC.html")},
		u(3): {Body: readFixture(t, "lta_list_empty_SYNTHETIC.html")},
	}}
	got := collect(t, s, f)
	meta := func(venues ...string) map[string][]string {
		return map[string][]string{
			"credential": {"Level 4 (Tennis)"}, "accreditation": {"Coach Accreditation+"},
			"accredited_to": {"Accredited to Coach Adults", "Accredited to Coach Children"}, "venues": venues,
		}
	}
	p := ltaBase + "/play/find-a-coach/coach-details/"
	want := []Record{
		{ID: "0039X000001AAAAQA0", URL: p + "0039X000001AAAAQA0/Fixture%20Racket", Name: "Fixture Racket", Country: "United Kingdom", Meta: meta("Fixture Park LTC", "Example Courts Ltd")},
		{ID: "0039X000002BBBBQA0", URL: p + "0039X000002BBBBQA0/Sample%20O%27Server", Name: "Sample O'Server", Country: "United Kingdom", Meta: meta("Fixture Park LTC", "Example Courts Ltd")},
		{ID: "0039X000003CCCCQA0", URL: p + "0039X000003CCCCQA0/Placeholder%20Volley", Name: "Placeholder Volley", Country: "United Kingdom", Meta: meta("Fixture Park LTC", "Example Courts Ltd")},
	}
	if !reflect.DeepEqual(got, want) || len(f.calls) != 3 {
		t.Errorf("records = %#v\nwant      %#v (requests %v)", got, want, f.calls)
	}

	d, err := s.Detail(context.Background(), &fakeFetcher{pages: map[string]Page{want[0].URL: {Body: readFixture(t, "lta_profile_SYNTHETIC.html")}}}, want[0])
	wantD := Record{Meta: map[string][]string{"gender": {"Female"}, "accreditation_renewal": {"24/08/2099"}}}
	if err != nil || !reflect.DeepEqual(d, wantD) {
		t.Errorf("detail = %#v, %v\nwant     %#v", d, err, wantD)
	}
	detailErrors(t, s, want[0], "lta")
}

func TestPGAAmerica(t *testing.T) {
	s := mustSource(t, "pga-america")
	f := &fakeFetcher{pages: map[string]Page{pgaAmericaBase + "/coaches-sitemap.xml": {Body: readFixture(t, "pga-america_sitemap_SYNTHETIC.xml")}}}
	got := collect(t, s, f)
	want := []Record{
		{ID: "fixturepro", URL: pgaAmericaBase + "/coach/fixturepro", Country: "United States"},
		{ID: "samplecoach", URL: pgaAmericaBase + "/coach/samplecoach", Country: "United States"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	if _, err := collectErr(s, &fakeFetcher{pages: map[string]Page{pgaAmericaBase + "/coaches-sitemap.xml": {Body: []byte("<urlset/>")}}}); err == nil {
		t.Error("an empty sitemap must fail List")
	}

	pages := map[string]Page{
		want[0].URL: {Body: readFixture(t, "pga-america_profile_SYNTHETIC.html")},
		want[1].URL: {Body: readFixture(t, "pga-america_profile_bare_SYNTHETIC.html")},
	}
	d, err := s.Detail(context.Background(), &fakeFetcher{pages: pages}, want[0])
	wantD := Record{
		Name: "Fixture Pro", Business: "Fixture Golf Club", Address: "1 Fairway Drive", City: "Clubtown", Region: "EX", PostalCode: "90001", Country: "United States",
		Phones: []string{"(555) 010-0123"}, Websites: []string{"https://www.example.org/fgc"},
		Social: map[string]string{"youtube": "https://www.youtube.com/@fixturepro"},
		Bio:    "Synthetic golf pro. Reach the shop at shop@example.org.",
		Meta: map[string][]string{
			"facilities": {"Fixture Golf Club", "Sample Range"}, "credential": {"MB"}, "member_type": {"MEMBER"}, "status": {"ACTIVE"},
			"specialties": {"Putting", "Short Game"}, "booking_url": {"https://booking.example.com/fixturepro"}},
	}
	if err != nil || !reflect.DeepEqual(d, wantD) {
		t.Errorf("detail = %#v, %v\nwant     %#v", d, err, wantD)
	}
	d, err = s.Detail(context.Background(), &fakeFetcher{pages: pages}, want[1])
	if err != nil || d.Name != "Sample Coach" || d.Business != "Sample Links" || d.City != "Sampleford" || d.Region != "SF" ||
		d.PostalCode != "90003" || len(d.Phones) != 0 || len(d.Websites) != 0 {
		t.Errorf("bare detail = %#v, %v", d, err)
	}
	detailErrors(t, s, want[0], "pga-america")
}

func TestPGAUK(t *testing.T) {
	s := mustSource(t, "pga-uk")
	f := &fakeFetcher{pages: map[string]Page{pgaUKBase + "/xml-directory/": {Body: readFixture(t, "pga-uk_xmldir_SYNTHETIC.xml")}}}
	got := collect(t, s, f)
	p := pgaUKBase + "/find-a-golf-lesson/profile/"
	want := []Record{
		{ID: "0100001", URL: p + "0100001/Fixture-Pro-Exampleton", Country: "GB"},
		{ID: "0100002", URL: p + "0100002/Sample%20Pro%20Sampleford", Country: "GB"}, // a name with spaces is percent-encoded
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}

	pages := map[string]Page{
		want[0].URL: {Body: readFixture(t, "pga-uk_profile_SYNTHETIC.html")},
		want[1].URL: {Body: readFixture(t, "pga-uk_profile_bare_SYNTHETIC.html")},
	}
	d, err := s.Detail(context.Background(), &fakeFetcher{pages: pages}, want[0])
	wantD := Record{
		Name: "Fixture Pro", Address: "Example Golf Club", City: "Exampleton", PostalCode: "EX1 1AA", Country: "GB",
		Phones: []string{"07700900123"}, Bio: "Fixture Pro has coached since 1999. Email pro@example.org to book.",
		Social: map[string]string{"facebook": "https://www.facebook.com/fixturepro/", "instagram": "https://www.instagram.com/fixturepro/"},
		Meta:   map[string][]string{},
	}
	if err != nil || !reflect.DeepEqual(d, wantD) { // the ld+json carries a raw line break, and a breadcrumb ld+json comes second
		t.Errorf("detail = %#v, %v\nwant     %#v", d, err, wantD)
	}
	d, err = s.Detail(context.Background(), &fakeFetcher{pages: pages}, want[1])
	if err != nil || d.Name != "Sample Pro" || d.Country != "GB" || d.PostalCode != "SF2 2BB" || len(d.Phones) != 0 {
		t.Errorf("bare detail = %#v, %v", d, err)
	}
	detailErrors(t, s, want[0], "pga-uk")
}

// lpgaFake plays the ASP.NET form: every POST must echo the previous
// response's hidden fields and press the right button.
type lpgaFake struct {
	t     *testing.T
	posts []url.Values
	pages [][]byte
}

func (f *lpgaFake) Get(_ context.Context, u string, _ map[string]string) (Page, error) {
	if u != lpgaURL {
		return Page{Status: 404, URL: u}, nil
	}
	return Page{Status: 200, URL: u, Body: readFixture(f.t, "lpga_form_SYNTHETIC.html")}, nil
}

func (f *lpgaFake) Post(_ context.Context, u string, hdr map[string]string, body []byte) (Page, error) {
	v, err := url.ParseQuery(string(body))
	if err != nil || u != lpgaURL || hdr["Content-Type"] != "application/x-www-form-urlencoded" {
		f.t.Fatalf("bad POST %s %v %v", u, hdr, err)
	}
	f.posts = append(f.posts, v)
	if len(f.posts) > len(f.pages) {
		return Page{Status: 500, URL: u}, nil
	}
	return Page{Status: 200, URL: u, Body: f.pages[len(f.posts)-1]}, nil
}

func lpgaID(key string) string {
	sum := sha1.Sum([]byte(key))
	return hex.EncodeToString(sum[:])[:16]
}

func TestLPGA(t *testing.T) {
	s := mustSource(t, "lpga")
	f := &lpgaFake{t: t, pages: [][]byte{readFixture(t, "lpga_page1_SYNTHETIC.html"), readFixture(t, "lpga_page2_SYNTHETIC.html")}}
	got := collect(t, s, f)
	want := []Record{
		{ID: lpgaID("teacher@example.org"), Name: "Fixture Teacher", Business: "Fixture Golf Club", Emails: []string{"teacher@example.org"},
			Phones: []string{"555 010-0101"}, Address: "1 Fairway Drive", City: "Exampleton", Region: "EX", PostalCode: "90000", Country: "United States",
			Meta: map[string][]string{"member_type": {"Class A"}, "lpga_section": {"Northeast"}}},
		{ID: lpgaID("sample pro||90001"), Name: "Sample Pro", City: "Sampleford", Region: "EX", PostalCode: "90001", Country: "United States",
			Meta: map[string][]string{"member_type": {"Life"}, "lpga_section": {"Western"}}},
		// the same teacher at a second facility (same email) is not a second entry
		{ID: lpgaID("placeholder@example.org"), Name: "Placeholder Pro", Business: "Example Links", Emails: []string{"placeholder@example.org"},
			City: "Toronto", Region: "ON", PostalCode: "M5V 3A8", Country: "Canada",
			Meta: map[string][]string{"member_type": {"LPGA A-T"}, "lpga_section": {"Central"}}},
	}
	for i := range got {
		if len(got[i].Emails) == 0 {
			got[i].Emails = nil
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	if len(f.posts) != 2 { // the second page has a disabled next button
		t.Fatalf("POSTs = %d", len(f.posts))
	}
	search, next := f.posts[0], f.posts[1]
	const btn = "ctl00$MainContentAreaPlaceHolder$C001$btnSearch"
	const nextBtn = "ctl00$MainContentAreaPlaceHolder$C001$TeacherGrid$ctl00$ctl03$ctl01$ctl28"
	if search.Get(btn) != "Search" || search.Get("__VIEWSTATE") != "tok0" || search.Get("__EVENTVALIDATION") != "ev-tok0" ||
		search.Get("cmbCountry") != "US" || search.Get("chkOn") != "1" || search.Has("chkOff") || !search.Has("txtKeyword") {
		t.Errorf("search POST = %v", search)
	}
	if next.Has(btn) || next.Get(nextBtn) != " " || next.Get("__VIEWSTATE") != "tok1" || next.Get("__EVENTVALIDATION") != "ev-tok1" {
		t.Errorf("pager POST = %v", next)
	}

	// emit's error (the -limit) stops the walk at once
	f = &lpgaFake{t: t, pages: f.pages}
	if err := s.List(context.Background(), f, func(Record) error { return errListLimit }); !errors.Is(err, errListLimit) || len(f.posts) != 1 {
		t.Errorf("limit: err=%v posts=%d", err, len(f.posts))
	}
	// a server that keeps answering with the same page does not loop forever
	f = &lpgaFake{t: t, pages: [][]byte{f.pages[0], f.pages[0], f.pages[0]}}
	if got := collect(t, s, f); len(got) != 2 || len(f.posts) != 2 {
		t.Errorf("stuck pager: %d records, %d POSTs", len(got), len(f.posts))
	}
	// no form on the page: a layout change fails the stage
	if _, err := collectErr(s, &fakeFetcher{pages: map[string]Page{lpgaURL: {Body: []byte("<html>new layout</html>")}}}); err == nil {
		t.Error("a page without the form must fail List")
	}
}

// The new sources end to end through the generic mapping: the country values
// they emit resolve against the curated whitelist and the categories map to
// the niche the loader stores.
func TestSportsListings(t *testing.T) {
	res := newCountryResolver()
	for _, c := range []struct {
		slug, country, code, category string
		niche                         string
	}{
		{"usajudo", "United States of America", "US", "sports_martial_arts", "fitness"},
		{"usacycling", "United States", "US", "sports_cycling", ""},
		{"usatf", "United States", "US", "sports_running", ""},
		{"lta", "United Kingdom", "GB", "sports_tennis", ""},
		{"pga-uk", "GB", "GB", "sports_golf", ""},
		{"lpga", "Canada", "CA", "sports_golf", ""},
		{"rrca", "CA", "CA", "sports_running", ""},
	} {
		s := mustSource(t, c.slug)
		l := recordListing(s, Record{ID: "1", Name: "Fixture", Country: c.country}, res)
		if l.CountryCode != c.code || l.Category != c.category || l.Niche != c.niche {
			t.Errorf("%s: country %q -> %q, category %q, niche %q", c.slug, c.country, l.CountryCode, l.Category, l.Niche)
		}
	}
}

// collectErr runs List and returns the records and its error.
func collectErr(s *Source, f Fetcher) ([]Record, error) {
	var out []Record
	err := s.List(context.Background(), f, func(r Record) error { out = append(out, r); return nil })
	return out, err
}

// detailErrors checks the shared Detail contract: a gone page is ErrNotFound
// and a changed layout is a plain error.
func detailErrors(t *testing.T, s *Source, r Record, name string) {
	t.Helper()
	if _, err := s.Detail(context.Background(), &fakeFetcher{}, r); !errors.Is(err, ErrNotFound) {
		t.Errorf("%s 404: %v, want ErrNotFound", name, err)
	}
	f := &fakeFetcher{pages: map[string]Page{r.URL: {Body: []byte("<html><body>new layout</body></html>")}}}
	if _, err := s.Detail(context.Background(), f, r); err == nil || errors.Is(err, ErrNotFound) {
		t.Errorf("%s layout change: %v, want a plain error", name, err)
	}
}
