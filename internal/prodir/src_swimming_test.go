package prodir

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

// swimPoster serves POSTs through fn (which sees the body) and 404s every GET,
// so the POST-based swimming sources can be tested with a body-dependent answer.
type swimPoster struct {
	fn    func(url string, body []byte) Page
	calls []string // "url body"
}

func (s *swimPoster) Get(_ context.Context, url string, _ map[string]string) (Page, error) {
	s.calls = append(s.calls, url)
	return Page{Status: 404, URL: url}, nil
}

func (s *swimPoster) Post(_ context.Context, url string, _ map[string]string, body []byte) (Page, error) {
	s.calls = append(s.calls, url+" "+string(body))
	p := s.fn(url, body)
	if p.Status == 0 {
		p.Status = 200
	}
	return p, nil
}

func checkRecords(t *testing.T, got, want []Record) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("records =\n%#v\nwant\n%#v", got, want)
	}
}

func TestUSASwimmingList(t *testing.T) {
	s := mustSource(t, "usaswimming")
	if s.Detail != nil {
		t.Error("usaswimming has no detail stage")
	}
	clubs := readFixture(t, "usaswimming_club_SYNTHETIC.json")
	lts := readFixture(t, "usaswimming_lts_SYNTHETIC.json")
	f := &swimPoster{fn: func(_ string, body []byte) Page {
		if strings.Contains(string(body), `"finderType":"Learn To Swim"`) {
			return Page{Body: lts}
		}
		return Page{Body: clubs}
	}}
	got := collect(t, s, f)
	want := []Record{
		// two facility rows of one club fold into one record
		{ID: "club-7001", Name: "Fixture Aquatics", Address: "1 Example Way, Suite 2", City: "Exampleville", Region: "CA",
			PostalCode: "90001", Country: "United States",
			Emails: []string{"coach@example.com", "registrar@example.com"}, Phones: []string{"15555550101", "15555550102"},
			Websites: []string{"https://fixture-swim.example.com"},
			Meta: map[string][]string{"finder_type": {"usa_swimming_club"}, "club_excellence_level": {"Gold"}, "club_size": {"120"},
				"contact_name": {"Pat Fixture", "Sam Fixture"}, "facility": {"Fixture Pool North", "Fixture Pool South"},
				"safesport_recognized": {"yes"}}},
		{ID: "club-7002", Name: "Plain Fixture SC", Address: "3 Placeholder Ave", City: "Noflag", Region: "TX", PostalCode: "75001",
			Country: "United States", Meta: map[string][]string{"finder_type": {"usa_swimming_club"}}},
		// the same orgUnitKey as a club, but a different record: the id carries the finder
		{ID: "lts-7001", Name: "Fixture Lessons", Address: "9 Lesson Lane", City: "Exampleville", Region: "CA", PostalCode: "90001",
			Country: "United States", Emails: []string{"lessons@example.org"}, Phones: []string{"555-0111"},
			Websites: []string{"www.fixture-lessons.example.org"},
			Meta: map[string][]string{"finder_type": {"learn_to_swim"}, "contact_name": {"Lee Fixture"}, "facility": {"Fixture Pool North"},
				"curriculum": {"Fixture Curriculum"}, "adult_learn_to_swim": {"yes"}, "special_needs": {"yes"}}},
	}
	// key 7003 has a blank club name: not a real entry
	checkRecords(t, got, want)
	if len(f.calls) != 2 || !strings.Contains(f.calls[0], `"finderType":"USA Swimming Club"`) ||
		!strings.Contains(f.calls[0], `"clubExcellenceLevel3":"Gold"`) {
		t.Errorf("requests = %v", f.calls)
	}

	// a failed POST and a body that is not JSON both fail the stage
	bad := &swimPoster{fn: func(string, []byte) Page { return Page{Status: 500} }}
	if err := s.List(context.Background(), bad, func(Record) error { return nil }); err == nil {
		t.Error("HTTP 500 must fail List")
	}
	bad = &swimPoster{fn: func(string, []byte) Page { return Page{Body: []byte("<html>blocked</html>")} }}
	if err := s.List(context.Background(), bad, func(Record) error { return nil }); err == nil {
		t.Error("non-JSON body must fail List")
	}

	// the limit stops after the first record
	f.calls = nil
	if err := s.List(context.Background(), f, func(Record) error { return errListLimit }); !errors.Is(err, errListLimit) || len(f.calls) != 1 {
		t.Errorf("limit: err=%v requests=%v", err, f.calls)
	}
}

func TestUSMSClubs(t *testing.T) {
	s := mustSource(t, "usms-clubs")
	f := &fakeFetcher{pages: map[string]Page{
		usmsClubsURL:                        {Body: readFixture(t, "usms-clubs_list_SYNTHETIC.json")},
		usmsBase + "/clubs/fixture-masters": {Body: readFixture(t, "usms-clubs_detail_SYNTHETIC.html")},
		usmsBase + "/clubs/second-fixture":  {Body: readFixture(t, "usms-clubs_detail_bare_SYNTHETIC.html")},
		usmsBase + "/clubs/gone":            {Status: 404},
		usmsBase + "/clubs/nolayout":        {Body: []byte("<html><body>maintenance</body></html>")},
	}}
	got := collect(t, s, f)
	// "salesforceId" is shared by the two clubs: the id is the url slug; the blank title is skipped
	want := []Record{
		{ID: "fixture-masters", URL: usmsBase + "/clubs/fixture-masters", Name: "Fixture Masters",
			Bio: "A fixture team. Reach us at bio@example.com", Address: "1 Example Way", City: "Exampleville", Region: "CO",
			PostalCode: "80001", Country: "United States",
			Meta: map[string][]string{"pool": {"Fixture Natatorium", "Second Pool"}, "pool_type": {"Indoor"},
				"badge": {"USMS Member Club", "USMS Certified Coach"}, "club_code": {"FIXM"}, "practice_times": {"Mon 6am"},
				"programs": {"gold club", "trial membership"}}},
		{ID: "second-fixture", URL: usmsBase + "/clubs/second-fixture", Name: "Second Fixture Swim", Country: "United States",
			Meta: map[string][]string{"club_code": {"SFS"}}},
	}
	checkRecords(t, got, want)

	d, err := s.Detail(context.Background(), f, got[0])
	if err != nil {
		t.Fatal(err)
	}
	wantD := Record{
		Emails: []string{"contact@example.com"}, Websites: []string{"https://fixture-masters.example.com"},
		Social: map[string]string{"instagram": "https://www.instagram.com/@fixturemasters", "facebook": "https://www.facebook.com/fixturemasters"},
		Meta:   map[string][]string{"contact_name": {"Pat Fixture"}},
	}
	checkRecords(t, []Record{d}, []Record{wantD})

	// a club page without contact block / website is fine, not an error
	d, err = s.Detail(context.Background(), f, got[1])
	if err != nil || len(d.Emails) != 0 || len(d.Websites) != 0 || len(d.Social) != 0 {
		t.Errorf("bare page: %+v, %v", d, err)
	}
	if _, err := s.Detail(context.Background(), f, Record{ID: "gone", URL: usmsBase + "/clubs/gone"}); !errors.Is(err, ErrNotFound) {
		t.Errorf("404: %v, want ErrNotFound", err)
	}
	if _, err := s.Detail(context.Background(), f, Record{ID: "x", URL: usmsBase + "/clubs/nolayout"}); err == nil || errors.Is(err, ErrNotFound) {
		t.Errorf("layout change: %v, want a plain error", err)
	}

	// an empty response (endpoint changed) fails the stage instead of "succeeding" with nothing
	empty := &fakeFetcher{pages: map[string]Page{usmsClubsURL: {Body: []byte(`{"data":{"clubs":[]}}`)}}}
	if err := s.List(context.Background(), empty, func(Record) error { return nil }); err == nil {
		t.Error("no clubs must fail List")
	}
}

func TestUSMSCoaches(t *testing.T) {
	s := mustSource(t, "usms-coaches")
	if s.Detail != nil {
		t.Error("usms-coaches has no detail stage")
	}
	f := &swimPoster{fn: func(string, []byte) Page { return Page{Body: readFixture(t, "usms-coaches_list_SYNTHETIC.html")} }}
	got := collect(t, s, f)
	want := []Record{
		// three levels fold into one coach: the highest is the credential, the earliest date is certified_since
		{ID: "ab12c", Name: "Pat Q Fixture", Business: "Fixture Masters [West]", Region: "Pacific", Country: "United States",
			Meta: map[string][]string{"club_code": {"FIXM"}, "credential": {"Level 3"},
				"certification_levels": {"Level 1", "Level 3", "Level 2"}, "certified_since": {"2019-05-04"}}},
		// the masked row ("?, ??") is skipped
		{ID: "cd34e", Name: "Robin Sample", Business: "Sample Swimmers", Region: "New England", Country: "United States",
			Meta: map[string][]string{"club_code": {"SMPL"}, "credential": {"Grandfathered"},
				"certification_levels": {"Grandfathered"}, "certified_since": {"2010-06-07"}}},
	}
	checkRecords(t, got, want)
	if len(f.calls) != 1 || !strings.HasSuffix(f.calls[0], "CourseType=CoachCert&LmscID=&ClubID=&WogID=") {
		t.Errorf("requests = %v", f.calls)
	}
	bad := &swimPoster{fn: func(string, []byte) Page { return Page{Body: []byte("<html>login</html>")} }}
	if err := s.List(context.Background(), bad, func(Record) error { return nil }); err == nil {
		t.Error("a response without datastr must fail List")
	}
}

func TestSwimEngland(t *testing.T) {
	s := mustSource(t, "swimengland")
	f := &fakeFetcher{pages: map[string]Page{
		swimEnglandSitemap: {Body: readFixture(t, "swimengland_sitemap_SYNTHETIC.xml")},
		swimEnglandBase + "/poolfinder/clubs/101/2001/Fixture+SC/":               {Body: readFixture(t, "swimengland_profile_SYNTHETIC.html")},
		swimEnglandBase + "/poolfinder/clubs/101/2002/Fixture+SC/":               {Status: 404},
		swimEnglandBase + "/poolfinder/clubs/102/2003/Sample+%2526+Co+Swimming/": {Body: []byte("<html><body>maintenance</body></html>")},
	}}
	got := collect(t, s, f)
	// only /poolfinder/clubs/ entries, once per clubId-facilityId; the name comes from the slug until the profile
	want := []Record{
		{ID: "101-2001", URL: swimEnglandBase + "/poolfinder/clubs/101/2001/Fixture+SC/", Name: "Fixture SC", Country: "United Kingdom"},
		{ID: "101-2002", URL: swimEnglandBase + "/poolfinder/clubs/101/2002/Fixture+SC/", Name: "Fixture SC", Country: "United Kingdom"},
		{ID: "102-2003", URL: swimEnglandBase + "/poolfinder/clubs/102/2003/Sample+%2526+Co+Swimming/", Name: "Sample %26 Co Swimming", Country: "United Kingdom"},
	}
	checkRecords(t, got, want)

	d, err := s.Detail(context.Background(), f, got[0])
	if err != nil {
		t.Fatal(err)
	}
	wantD := Record{
		Name: "Fixture SC", Address: "Example Road", City: "Exampleton", Region: "England", PostalCode: "EX1 1AA",
		Country: "United Kingdom", Websites: []string{"http://www.fixture-sc.example.org"},
		Meta: map[string][]string{"facility": {"Fixture Pool"}, "member_count": {"580"},
			"abilities":           {"Newcomer", "Improver", "Competitor"},
			"disciplines":         {"Swimming", "Masters Swimming"},
			"open_to_new_members": {"Swimming"}},
	}
	checkRecords(t, []Record{d}, []Record{wantD})
	if _, err := s.Detail(context.Background(), f, got[1]); !errors.Is(err, ErrNotFound) {
		t.Errorf("404: %v, want ErrNotFound", err)
	}
	if _, err := s.Detail(context.Background(), f, got[2]); err == nil || errors.Is(err, ErrNotFound) {
		t.Errorf("layout change: %v, want a plain error", err)
	}
	empty := &fakeFetcher{pages: map[string]Page{swimEnglandSitemap: {Body: []byte("<urlset></urlset>")}}}
	if err := s.List(context.Background(), empty, func(Record) error { return nil }); err == nil {
		t.Error("a sitemap without clubs must fail List")
	}
}

func TestSwimmingCanadaList(t *testing.T) {
	s := mustSource(t, "swimmingcanada")
	if s.Detail != nil {
		t.Error("swimmingcanada has no detail stage")
	}
	f := &fakeFetcher{pages: map[string]Page{swimmingCanadaURL: {Body: readFixture(t, "swimmingcanada_list_SYNTHETIC.js")}}}
	got := collect(t, s, f)
	if len(got) != 3 {
		t.Fatalf("records = %#v, want 3 (the blank name is skipped)", got)
	}
	if got[0].Name != "Fixture Aquatic Club" || got[0].Address != "1 Example St, Exampletown ON A1A 1A1" || got[0].Country != "Canada" ||
		!reflect.DeepEqual(got[0].Websites, []string{"https://fixture-aquatic.example.ca"}) || got[0].Emails != nil || got[0].Phones != nil {
		t.Errorf("first = %#v", got[0])
	}
	if got[1].Websites != nil || got[1].Address != "" {
		t.Errorf("second = %#v", got[1])
	}
	// ids: DNS-safe, stable across runs, and a repeated name stays unique
	for _, r := range got {
		if !dnsLabelRe.MatchString(r.ID) {
			t.Errorf("id %q is not a DNS label", r.ID)
		}
	}
	if got[0].ID == got[2].ID || !strings.HasPrefix(got[0].ID, "fixture-aquatic-club-") || !strings.HasSuffix(got[2].ID, "-2") {
		t.Errorf("ids = %q %q %q", got[0].ID, got[1].ID, got[2].ID)
	}
	again := collect(t, s, f)
	if again[0].ID != got[0].ID || again[2].ID != got[2].ID {
		t.Error("ids are not stable between runs")
	}
	bad := &fakeFetcher{pages: map[string]Page{swimmingCanadaURL: {Body: []byte("<html>blocked</html>")}}}
	if err := s.List(context.Background(), bad, func(Record) error { return nil }); err == nil {
		t.Error("a non-JSONP body must fail List")
	}
}

func TestTotalImmersion(t *testing.T) {
	s := mustSource(t, "totalimmersion")
	f := &fakeFetcher{pages: map[string]Page{
		totalImmersionList:                          {Body: readFixture(t, "totalimmersion_list_SYNTHETIC.html")},
		totalImmersionBase + "/coach/fixture-coach": {Body: readFixture(t, "totalimmersion_profile_SYNTHETIC.html")},
		totalImmersionBase + "/coach/uk-fixture":    {Status: 404},
	}}
	got := collect(t, s, f)
	want := []Record{
		{ID: "fixture-coach", URL: totalImmersionBase + "/coach/fixture-coach", Name: "Fixture Coach", City: "Phoenix", Region: "AZ",
			Country: "United States", Emails: []string{"fixture@example.com"}, Websites: []string{"http://www.fixture-coach.example.com"},
			Meta: map[string][]string{"level": {"Level 3"}}},
		// no profile page: the id is the slugified name and the list card carries the phone
		{ID: "no-profile", Name: "No Profile", City: "Enfield", Region: "CT", Country: "United States",
			Emails: []string{"noprofile@example.org"}, Phones: []string{"555 010 0199"}, Meta: map[string][]string{"level": {"Level 2"}}},
		// repeated under two headings: once; a relative profile link works
		{ID: "uk-fixture", URL: totalImmersionBase + "/coach/uk-fixture", Name: "UK Fixture", City: "Exampleton",
			Country: "United Kingdom", Meta: map[string][]string{"level": {"Level 1"}}},
	}
	checkRecords(t, got, want)

	d, err := s.Detail(context.Background(), f, got[0])
	if err != nil {
		t.Fatal(err)
	}
	wantD := Record{
		City: "Phoenix", Region: "Arizona", Country: "United States", Emails: []string{"fixture@example.com"},
		Phones: []string{"555 010 0100"}, Websites: []string{"http://www.fixture-coach.example.com"},
		Bio:    "Fixture Coach teaches freestyle in Exampleville.",
		Social: map[string]string{"facebook": "https://www.facebook.com/fixturecoach", "instagram": "https://www.instagram.com/fixturecoach"},
		Meta:   map[string][]string{"level": {"Level 3"}},
	}
	checkRecords(t, []Record{d}, []Record{wantD})
	// the profiles stage merges it: a spelled-out region replaces the abbreviation, the list's email stays
	if m := mergeRecord(got[0], d); m.Bio == "" || m.Phones[0] != "555 010 0100" || m.ID != "fixture-coach" {
		t.Errorf("merged = %#v", m)
	}

	if d, err := s.Detail(context.Background(), f, got[1]); err != nil || !reflect.DeepEqual(d, Record{}) {
		t.Errorf("no profile page: %#v, %v; want an empty record and no request", d, err)
	}
	if _, err := s.Detail(context.Background(), f, got[2]); !errors.Is(err, ErrNotFound) {
		t.Errorf("404: %v, want ErrNotFound", err)
	}

	for in, want := range map[string][3]string{
		"Phoenix, AZ": {"Phoenix", "AZ", "United States"},
		"Eastbourne, South Coast, England, United Kingdom": {"Eastbourne", "South Coast, England", "United Kingdom"},
		"Seoul, Korea": {"Seoul", "", "Korea"},
		"Singapore":    {"", "", "Singapore"},
		"Somewhere":    {"Somewhere", "", ""},
		"":             {"", "", ""},
	} {
		c, r, k := totalImmersionPlace(in)
		if [3]string{c, r, k} != want {
			t.Errorf("place(%q) = %q %q %q, want %v", in, c, r, k, want)
		}
	}
	bad := &fakeFetcher{pages: map[string]Page{totalImmersionList: {Body: []byte("<html><body>maintenance</body></html>")}}}
	if err := s.List(context.Background(), bad, func(Record) error { return nil }); err == nil {
		t.Error("a page without coach cards must fail List")
	}
}

func TestSwimSmoothList(t *testing.T) {
	s := mustSource(t, "swimsmooth")
	if s.Detail != nil {
		t.Error("swimsmooth has no detail stage")
	}
	f := &fakeFetcher{pages: map[string]Page{swimSmoothURL: {Body: readFixture(t, "swimsmooth_coaches_SYNTHETIC.json")}}}
	got := collect(t, s, f)
	want := []Record{
		{ID: "fixt", URL: swimSmoothGuru + "Fixt", Name: "Fixture Coach", Business: "Fixture Swim Co", Bio: "Fixture bio text.",
			City: "Exampleton", Region: "CA", Country: "USA",
			Emails: []string{"coach@example.com"}, Phones: []string{"+1 555 010 0100"},
			Websites: []string{"www.fixture-swim.example.com", "https://book.example.com/fixture"},
			// "Fixture Coach Page" is a name, not a link: dropped
			Social: map[string]string{"instagram": "https://www.instagram.com/fixturecoach", "twitter": "https://x.com/fixturecoach"},
			Meta: map[string][]string{"certification": {"Swim Smooth Certified Coach"}, "tags": {"Certified Coach", "Mentor"},
				"services": {"1 to 1 Sessions (Pool)"}}},
		// "Western Australia" is no country: the text stays the city, the country stays unknown
		{ID: "c2024-sample", URL: swimSmoothGuru + "C2024-sample", Name: "Sample Coach", City: "Western Australia",
			Social: map[string]string{}, Meta: map[string][]string{}},
	}
	checkRecords(t, got, want)
	bad := &fakeFetcher{pages: map[string]Page{swimSmoothURL: {Body: []byte(`{"success":false,"payload":[]}`)}}}
	if err := s.List(context.Background(), bad, func(Record) error { return nil }); err == nil {
		t.Error("an empty payload must fail List")
	}
}

func TestAustSwimList(t *testing.T) {
	s := mustSource(t, "austswim")
	if s.Detail != nil {
		t.Error("austswim has no detail stage")
	}
	p1, p2 := readFixture(t, "austswim_page1_SYNTHETIC.json"), readFixture(t, "austswim_page2_SYNTHETIC.json")
	f := &swimPoster{fn: func(_ string, body []byte) Page {
		if strings.Contains(string(body), `"after":2`) {
			return Page{Body: p2}
		}
		return Page{Body: p1}
	}}
	got := collect(t, s, f)
	want := []Record{
		{ID: "5001", Name: "Fixture Swim School", Address: "1 Example St, Unit 2", City: "Exampleton", Region: "NSW", PostalCode: "2000",
			Country: "Australia", Emails: []string{"info@example.com.au"}, Phones: []string{"02 5550 0101"},
			Websites: []string{"fixtureswim.example.com.au"},
			Meta:     map[string][]string{"recognition_level": {"Premium"}, "type": {"Swim Centre"}}},
		// 5002 is not in the public directory, 5004 is archived; INT names no country, "-" is no website
		{ID: "5003", Name: "Overseas Fixture Aquatics", Address: "3 Sample Rd", City: "Samplecity",
			Emails: []string{"os@example.com.au"}, Phones: []string{"555 0103"},
			Meta: map[string][]string{"recognition_level": {"Standard"}, "type": {"Swim Centre"}}},
	}
	checkRecords(t, got, want)
	if len(f.calls) != 2 || !strings.Contains(f.calls[0], `"after":0`) || !strings.Contains(f.calls[0], `"limit":200`) {
		t.Errorf("requests = %v", f.calls)
	}
	// a cursor that does not advance ends the walk instead of looping
	loop := &swimPoster{fn: func(string, []byte) Page {
		return Page{Body: []byte(`{"results":[{"id":"1","properties":{"name":"X"}}],"paging":{"next":{"after":"0"}}}`)}
	}}
	if recs := collect(t, s, loop); len(recs) != 1 || len(loop.calls) != 1 {
		t.Errorf("stuck cursor: %d records, requests %v", len(recs), loop.calls)
	}
}

func TestSwimAustraliaList(t *testing.T) {
	s := mustSource(t, "swimaustralia")
	if s.Detail != nil {
		t.Error("swimaustralia has no detail stage")
	}
	f := &fakeFetcher{pages: map[string]Page{
		strings.Replace(strings.Replace(swimAustraliaURL, "limit=%d", "limit=500", 1), "limitstart=%d", "limitstart=0", 1): {Body: readFixture(t, "swimaustralia_frame_SYNTHETIC.html")},
	}}
	got := collect(t, s, f)
	want := []Record{
		{ID: "3001", Name: "Fixture Swim School", Address: "1 Example St", City: "Exampleton", Region: "Victoria", PostalCode: "3000",
			Country: "AUSTRALIA", Emails: []string{"hello@example.com.au"}, Phones: []string{"03 5550 0101"},
			Websites: []string{"www.fixtureswim.example.com.au"}},
		{ID: "3002", Name: "Kiwi Fixture Pool", Address: "2 Sample Rd", City: "Samplecity", Region: "AUK", PostalCode: "1010", Country: "New Zealand"},
		// no country, but an Australian state; the repeated id 3001 and the blank name are skipped
		{ID: "3003", Name: "No Country Fixture", Address: "3 Placeholder Ave", City: "Noflag", Region: "NSW", PostalCode: "2000", Country: "Australia"},
	}
	checkRecords(t, got, want)
	if len(f.calls) != 1 {
		t.Errorf("requests = %v (fewer rows than the page size: no second page)", f.calls)
	}
	bad := &fakeFetcher{pages: map[string]Page{
		strings.Replace(strings.Replace(swimAustraliaURL, "limit=%d", "limit=500", 1), "limitstart=%d", "limitstart=0", 1): {Body: []byte("<html>no widget</html>")},
	}}
	if err := s.List(context.Background(), bad, func(Record) error { return nil }); err == nil {
		t.Error("a page without location_data must fail List")
	}
}

// The swimming sources through the generic Record -> Listing mapping.
func TestSwimmingListings(t *testing.T) {
	res := newCountryResolver()
	us := mustSource(t, "usaswimming")
	l := recordListing(us, Record{ID: "club-7001", Name: "Fixture Aquatics", Country: "United States", Region: "CA",
		Emails: []string{"coach@example.com"}, Phones: []string{"15555550101"}, Websites: []string{"https://fixture-swim.example.com"},
		Meta: map[string][]string{"finder_type": {"usa_swimming_club"}}}, res)
	if l.Domain != "club-7001.usaswimming.usaswimming.org" || l.Niche != "fitness" || l.Category != "swimming_coach" ||
		l.CountryCode != "US" || !l.OwnWebsite || l.Website != "https://fixture-swim.example.com" ||
		!reflect.DeepEqual(l.Meta["listing_type"], []string{"club"}) || !reflect.DeepEqual(l.Meta["finder_type"], []string{"usa_swimming_club"}) {
		t.Errorf("usaswimming listing = %+v", l)
	}
	// the directory's own pages are never the entry's website
	l = recordListing(mustSource(t, "usms-clubs"), Record{ID: "x", Name: "X", Websites: []string{"https://www.usms.org/clubs/x"}}, res)
	if l.OwnWebsite {
		t.Errorf("usms.org link taken as the club's own website: %+v", l)
	}
	for _, slug := range []string{"usaswimming", "usms-clubs", "usms-coaches", "swimengland", "swimmingcanada", "totalimmersion", "swimsmooth", "austswim", "swimaustralia"} {
		if s := mustSource(t, slug); s.Category != "swimming_coach" || s.MinInterval < 300e6 {
			t.Errorf("%s: category %q, interval %v", slug, s.Category, s.MinInterval)
		}
	}
}
