package prodir

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"testing"
)

// bodyFetcher is a fakeFetcher whose POST answers are picked by the request
// body (the plain fakeFetcher keys on the URL only); it keeps the POST headers.
type bodyFetcher struct {
	*fakeFetcher
	post func(url, body string) Page
	hdrs []map[string]string
}

func (f *bodyFetcher) Post(_ context.Context, url string, hdr map[string]string, body []byte) (Page, error) {
	f.calls = append(f.calls, "POST "+url+" "+string(body))
	f.hdrs = append(f.hdrs, hdr)
	p := f.post(url, string(body))
	if p.Status == 0 {
		p.Status = 200
	}
	if p.URL == "" {
		p.URL = url
	}
	return p, nil
}

func fixturePage(t *testing.T, name string) Page { return Page{Body: readFixture(t, name)} }

// byID indexes records by ID.
func byID(recs []Record) map[string]Record {
	m := map[string]Record{}
	for _, r := range recs {
		m[r.ID] = r
	}
	return m
}

func TestYogaprosList(t *testing.T) {
	s := mustSource(t, "yogapros")
	u := func(q string, n int) string {
		return fmt.Sprintf("%s/teachers?%spage=%d&sized=0", yogaprosBase, q, n)
	}
	f := &fakeFetcher{pages: map[string]Page{
		u("yoga_style=13&", 1): fixturePage(t, "yogapros_hot_SYNTHETIC.html"),
		u("", 1):               fixturePage(t, "yogapros_list_page1_SYNTHETIC.html"),
		u("", 2):               fixturePage(t, "yogapros_list_page2_SYNTHETIC.html"),
	}}
	got := collect(t, s, f)
	want := []Record{
		{ID: "900001", URL: yogaprosBase + "/teacher/alex-fixture", Name: "Alex Fixture", Business: "Fixture Studio",
			City: "Exampleton", Country: "Canada",
			Websites: []string{"https://alex-fixture.example.com/", "https://alex-fixture.example.com/blog/"},
			Social:   map[string]string{"instagram": "https://www.instagram.com/alexfixture/", "facebook": "https://www.facebook.com/alexfixture"},
			Meta:     map[string][]string{"level": {"Senior Yoga Teacher"}}},
		{ID: "900002", URL: yogaprosBase + "/teacher/blair-placeholder", Name: "Blair Placeholder",
			City: "Sampleford", Country: "United Kingdom",
			Social: map[string]string{"linkedin": "https://www.linkedin.com/in/blairplaceholder/"},
			Meta:   map[string][]string{"level": {"Yoga Teacher"}}},
		{ID: "900003", URL: yogaprosBase + "/teacher/cameron-overseas", Name: "Cameron Overseas",
			Country: "Germany", Category: "hot_yoga_teacher", // listed under the Hot Yoga style filter
			Meta: map[string][]string{"level": {"New Yoga Teacher"}}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	// hot pre-pass (1 page), then the counter ("17 results") says 2 pages: no third request
	if len(f.calls) != 3 {
		t.Errorf("requests = %v", f.calls)
	}

	// the counter ("1 result") ends the walk after page 1
	f = &fakeFetcher{pages: map[string]Page{
		u("yoga_style=13&", 1): fixturePage(t, "yogapros_list_empty_SYNTHETIC.html"),
		u("", 1):               fixturePage(t, "yogapros_hot_SYNTHETIC.html"),
	}}
	if got := collect(t, s, f); len(got) != 1 || got[0].Category != "" || len(f.calls) != 2 {
		t.Errorf("one-page counter: %d records, requests %v", len(got), f.calls)
	}

	// without a counter it walks until a page has no cards
	noCounter := Page{Body: []byte(strings.Replace(string(readFixture(t, "yogapros_hot_SYNTHETIC.html")), "1 result", "", 1))}
	f = &fakeFetcher{pages: map[string]Page{
		u("yoga_style=13&", 1): fixturePage(t, "yogapros_list_empty_SYNTHETIC.html"),
		u("", 1):               noCounter,
		u("", 2):               fixturePage(t, "yogapros_list_empty_SYNTHETIC.html"),
	}}
	if got := collect(t, s, f); len(got) != 1 || len(f.calls) != 3 {
		t.Errorf("no counter: %d records, requests %v", len(got), f.calls)
	}

	// a failing hot pre-pass fails List: no record may be stored with the wrong category
	f = &fakeFetcher{pages: map[string]Page{u("yoga_style=13&", 1): {Status: 500}}}
	if err := s.List(context.Background(), f, func(Record) error { return nil }); err == nil {
		t.Error("HTTP 500 on the hot pre-pass must fail List")
	}
}

func TestJivamuktiListAndDetail(t *testing.T) {
	s := mustSource(t, "jivamukti")
	f := &fakeFetcher{pages: map[string]Page{jivamuktiListURL: fixturePage(t, "jivamukti_all_SYNTHETIC.html")}}
	got := collect(t, s, f)
	want := []Record{
		{ID: "1001", URL: jivamuktiBase + "/teachers-global/alex-fixture/", Name: "Alex Fixture", City: "Exampleton", Region: "NY",
			Country: "United States", Emails: []string{"alex.fixture@example.com"}, Meta: map[string][]string{"certification": {"300hr"}}},
		// "." is a filler state; the empty email stays empty; the blank-named post 1003 is no entry
		{ID: "1002", URL: jivamuktiBase + "/teachers-global/blair-placeholder/", Name: "Blair & Placeholder", City: "Sampleford",
			Country: "United States", Meta: map[string][]string{}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}

	f = &fakeFetcher{pages: map[string]Page{want[0].URL: fixturePage(t, "jivamukti_profile_SYNTHETIC.html")}}
	d, err := s.Detail(context.Background(), f, want[0])
	if err != nil {
		t.Fatal(err)
	}
	wantD := Record{
		Websites: []string{"http://www.alex-fixture.example.com/"},
		Social:   map[string]string{"instagram": "http://www.instagram.com/alexfixture/"},
		Bio:      "Alex teaches in Exampleton.Contact: teach@example.org or 555-0100.", // the footer's studio email is not read
	}
	if !reflect.DeepEqual(d, wantD) {
		t.Errorf("detail = %#v\nwant     %#v", d, wantD)
	}
	// a gone profile is ErrNotFound; a page without the template is an error
	f = &fakeFetcher{}
	if _, err := s.Detail(context.Background(), f, want[0]); !errors.Is(err, ErrNotFound) {
		t.Errorf("404: err = %v", err)
	}
	f = &fakeFetcher{pages: map[string]Page{want[0].URL: {Body: []byte("<html><body>maintenance</body></html>")}}}
	if _, err := s.Detail(context.Background(), f, want[0]); err == nil {
		t.Error("a page without the body template must fail")
	}
}

func TestIynausList(t *testing.T) {
	s := mustSource(t, "iynaus")
	u := func(offset int) string {
		return fmt.Sprintf("%s?where=1%%3D1&outFields=*&returnGeometry=false&orderByFields=OBJECTID&resultOffset=%d&resultRecordCount=2000&f=json", iynausLayer, offset)
	}
	f := &fakeFetcher{pages: map[string]Page{
		u(0): fixturePage(t, "iynaus_page1_SYNTHETIC.json"),
		u(2): fixturePage(t, "iynaus_page2_SYNTHETIC.json"),
	}}
	got := collect(t, s, f)
	want := []Record{
		{ID: "1", Name: "Alex Fixture", Business: "Fixture Yoga Studio", Address: "1 Example Street, Suite 5", City: "Exampleton",
			Region: "NY", PostalCode: "10001", Country: "United States of America",
			Phones: []string{"555-0100"}, Websites: []string{"http://www.example.com/alex"},
			Meta: map[string][]string{"level": {"Level 2"}, "region": {"Northeast - IYANE"}, "specialties": {"Therapeutics", "Prenatal"},
				"languages": {"English", "Spanish"}, "online": {"Yes"}}},
		// no country: the US association's default
		{ID: "2", Name: "Blair Placeholder", City: "Sampleford", Country: "United States",
			Meta: map[string][]string{"level": {"Level 1"}, "online": {"No"}}},
		{ID: "3", Name: "Cameron Overseas", City: "Zurich", Country: "Switzerland", Meta: map[string][]string{"level": {"Level 3"}}},
	} // OBJECTID 4 has a blank name
	if !reflect.DeepEqual(got, want) {
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	if len(f.calls) != 2 { // page 1 says exceededTransferLimit, page 2 does not
		t.Errorf("requests = %v", f.calls)
	}

	// ArcGIS reports query errors with HTTP 200
	f = &fakeFetcher{pages: map[string]Page{u(0): {Body: []byte(`{"error":{"code":400,"message":"Invalid query"}}`)}}}
	if err := s.List(context.Background(), f, func(Record) error { return nil }); err == nil || !strings.Contains(err.Error(), "Invalid query") {
		t.Errorf("error body: err = %v", err)
	}
}

func TestYaaList(t *testing.T) {
	s := mustSource(t, "yaa")
	u := func(n int) string { return fmt.Sprintf("%s?per_page=100&page=%d", yaaURL, n) }
	f := &fakeFetcher{pages: map[string]Page{u(1): fixturePage(t, "yaa_places_SYNTHETIC.json")}}
	got := collect(t, s, f)
	want := []Record{
		{ID: "1001", URL: "https://yoga.ca/places/alex-fixture/", Name: "Alex Fixture", City: "Exampleton", Region: "Ontario",
			PostalCode: "A1A 1A1", Country: "Canada", Bio: "Teaches Hatha & Yin in Exampleton.",
			Emails: []string{"alex@example.com"}, Phones: []string{"555-0101"}, Websites: []string{"http://alex.example.com"},
			Social: map[string]string{"facebook": "https://www.facebook.com/alexfixture"},
			Meta: map[string][]string{"level": {"1000 Hr Level"}, "styles": {"Hatha Yoga", "Yin Yoga"},
				"delivery": {"In-person", "Virtual"}}},
		// "Hot Yoga" among the styles: per-record override; no country: Canada
		{ID: "1002", URL: "https://yoga.ca/places/blair-placeholder/", Name: "Blair Placeholder", Business: "Placeholder Yoga",
			Address: "12 Sample Road", City: "Sampleford", Region: "Alberta", Country: "Canada", Category: "hot_yoga_teacher",
			Social: map[string]string{"instagram": "https://www.instagram.com/blairplaceholder/"},
			Meta:   map[string][]string{"level": {"Other Teacher"}, "styles": {"Hatha Yoga", "hot yoga"}, "mentorship": {"Yes"}}},
		{ID: "1003", URL: "https://yoga.ca/places/fixture-studio/", Name: "Fixture Studio", City: "Testville", Region: "Quebec",
			Country: "Canada", Emails: []string{"studio@example.org"}, Phones: []string{"555-0103"},
			Meta: map[string][]string{"level": {"Studios"}, "entry_type": {"studio"}, "styles": {"Vinyasa Flow"}}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	if len(f.calls) != 1 { // under 100 places: the last page
		t.Errorf("requests = %v", f.calls)
	}

	// a full page continues; WordPress answers 400 for a page past the end
	var full []map[string]any
	for i := 1; i <= 100; i++ {
		full = append(full, map[string]any{"id": i, "title": map[string]string{"raw": fmt.Sprintf("Filler %d", i)}})
	}
	body, _ := json.Marshal(full)
	f = &fakeFetcher{pages: map[string]Page{u(1): {Body: body}, u(2): {Status: 400}}}
	if got := collect(t, s, f); len(got) != 100 || len(f.calls) != 2 {
		t.Errorf("past the end: %d records, requests %v", len(got), f.calls)
	}
	// a 400 on page 1 is an error
	f = &fakeFetcher{pages: map[string]Page{u(1): {Status: 400}}}
	if err := s.List(context.Background(), f, func(Record) error { return nil }); err == nil {
		t.Error("HTTP 400 on page 1 must fail List")
	}
}

func TestKriList(t *testing.T) {
	s := mustSource(t, "kri")
	trainers := kriApp + "/page-perma/Trainers1/FIXTURETRAINERSTOKEN123"
	f := &bodyFetcher{
		fakeFetcher: &fakeFetcher{pages: map[string]Page{
			kriHome:  fixturePage(t, "kri_home_SYNTHETIC.html"),
			trainers: fixturePage(t, "kri_trainers_SYNTHETIC.html"),
		}},
		post: func(u, body string) Page {
			q, _ := url.ParseQuery(body)
			if u != kriReport || q.Get("privatelink") != "FIXTUREREPORTKEY456" {
				return Page{Status: 404}
			}
			switch q.Get("fromIDX") {
			case "1":
				return fixturePage(t, "kri_page1_SYNTHETIC.json")
			case "3": // moved on by the 2 rows page 1 returned
				return fixturePage(t, "kri_page2_SYNTHETIC.json")
			}
			return Page{Status: 404}
		},
	}
	got := collect(t, s, f)
	want := []Record{
		{ID: "9001", Name: "Alex Fixture", City: "Exampleton", Region: "Ontario", Country: "Canada",
			Emails: []string{"alex@example.com"}, Websites: []string{"http://kri-fixture.example.com"},
			Meta: map[string][]string{"role": {"Intern"}, "level_two": {"L2 Mind and Meditation", "L2 Vitality and Stress"}}},
		{ID: "9002", Name: "Blair & Placeholder", City: "Sampleford", Country: "Germany", Emails: []string{"blair@example.org"},
			Meta: map[string][]string{"role": {"Mentor"}, "mentoring_pathway": {"Mentor"}, "twentyone_stages": {"21 Stages Trainer"}}},
		{ID: "9003", Name: "Cameron Overseas", City: "Zurich", Country: "Switzerland", Emails: []string{"cameron@example.com"},
			Meta: map[string][]string{"role": {"Professional"}}},
	} // 9002 comes twice (deduped); 9004 has no name
	if !reflect.DeepEqual(got, want) {
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	if len(f.calls) != 4 { // home, trainers page, two report pages
		t.Errorf("requests = %v", f.calls)
	}
	if h := f.hdrs[0]; h["x-requested-with"] != "XMLHttpRequest" {
		t.Errorf("report headers = %v", h)
	}

	// a Home page without the Trainers link is an error
	f = &bodyFetcher{fakeFetcher: &fakeFetcher{pages: map[string]Page{kriHome: {Body: []byte("<html></html>")}}}}
	if err := s.List(context.Background(), f, func(Record) error { return nil }); err == nil {
		t.Error("no Trainers link must fail List")
	}
}

func TestAnusaraList(t *testing.T) {
	s := mustSource(t, "anusara")
	f := &bodyFetcher{
		fakeFetcher: &fakeFetcher{pages: map[string]Page{
			anusaraBase + "/find-a-teacher":        {Body: []byte("<html></html>")},
			anusaraBase + "/_api/v1/access-tokens": fixturePage(t, "anusara_tokens_SYNTHETIC.json"),
		}},
		post: func(u, body string) Page {
			if u != anusaraMethod {
				return Page{Status: 404}
			}
			switch body {
			case "[null,null,null,1]":
				return fixturePage(t, "anusara_page1_SYNTHETIC.json")
			case "[null,null,null,2]":
				return fixturePage(t, "anusara_page2_SYNTHETIC.json")
			}
			return Page{Status: 404}
		},
	}
	got := collect(t, s, f)
	want := []Record{
		{ID: "a0000001-0000-4000-8000-000000000001", Name: "Alex Fixture", Country: "United States Of America",
			Emails: []string{"alex@example.com", "alex.main@example.com"}, Phones: []string{"5550100", "+1 555-0100"},
			Meta: map[string][]string{"credential": {"L2-IAYT", "ECATT"}}},
		// no parsedRoles: the role field is the credential
		{ID: "a0000002-0000-4000-8000-000000000002", Name: "Blair Placeholder", Emails: []string{"blair@example.org"},
			Meta: map[string][]string{"credential": {"CATT"}}},
		{ID: "a0000003-0000-4000-8000-000000000003", Name: "Cameron Overseas", Country: "Mexico", Emails: []string{"cameron@example.com"},
			Meta: map[string][]string{"credential": {"L1-EAYT"}}},
	} // page 2 repeats Blair (deduped) and has a nameless row
	if !reflect.DeepEqual(got, want) {
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	if len(f.calls) != 4 { // finder page, tokens, two method pages (hasNext false ends it)
		t.Errorf("requests = %v", f.calls)
	}
	// the wixcode-pub instance (not the other app's) authorises the call
	if h := f.hdrs[0]; h["Authorization"] != "wixcode-pub.fixture-instance" || h["x-wix-app-instance"] != "wixcode-pub.fixture-instance" {
		t.Errorf("headers = %v", h)
	}

	f = &bodyFetcher{fakeFetcher: &fakeFetcher{pages: map[string]Page{
		anusaraBase + "/find-a-teacher":        {Body: []byte("<html></html>")},
		anusaraBase + "/_api/v1/access-tokens": {Body: []byte(`{"apps":{"x":{"instance":"Other.x"}}}`)},
	}}}
	if err := s.List(context.Background(), f, func(Record) error { return nil }); err == nil {
		t.Error("no wixcode-pub instance must fail List")
	}
}

func TestYogaAustraliaList(t *testing.T) {
	s := mustSource(t, "yogaaustralia")
	u := func(offset int) string {
		return fmt.Sprintf("%s?limit=100&offset=%d&seed=prodir", yogaAustraliaURL, offset)
	}
	f := &fakeFetcher{pages: map[string]Page{
		u(0): fixturePage(t, "yogaaustralia_page1_SYNTHETIC.json"),
		u(2): fixturePage(t, "yogaaustralia_page2_SYNTHETIC.json"),
	}}
	got := collect(t, s, f)
	want := []Record{
		{ID: "2001", URL: "https://yogaaustralia.org.au/teachers/alex-fixture/", Name: "Alex Fixture", City: "Exampleton", Region: "VIC",
			Country: "Australia", Bio: "Teaches in Exampleton. Write to alex@example.com.",
			Meta: map[string][]string{"level": {"Level 3 (Senior, 1000hr)"}, "credentials": {"Registered Senior Teacher"},
				"specialties": {"Yoga Therapist", "Prenatal Yoga"}}},
		{ID: "2002", URL: "https://yogaaustralia.org.au/teachers/blair-placeholder/", Name: "Blair Placeholder", Region: "NSW",
			Country: "Australia", Meta: map[string][]string{"level": {"Level 2 (500hr)"}}},
		{ID: "2003", URL: "https://yogaaustralia.org.au/teachers/cameron-overseas/", Name: "Cameron Overseas", City: "Sampleford", Region: "QLD",
			Country: "Australia", Meta: map[string][]string{"level": {"Level 1 (350hr)"}, "credentials": {"Registered Teacher"}}},
	} // page 2 repeats 2001 (deduped)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	if len(f.calls) != 2 { // offset 2 reaches count 3
		t.Errorf("requests = %v", f.calls)
	}
}

func TestIyengarAUList(t *testing.T) {
	s := mustSource(t, "iyengar-au")
	f := &fakeFetcher{pages: map[string]Page{iyengarAUURL: fixturePage(t, "iyengar-au_search_SYNTHETIC.json")}}
	got := collect(t, s, f)
	want := []Record{
		{ID: "3001", URL: "https://iyengaryoga.asn.au/blog/teacher/alex-fixture/", Name: "Alex Fixture", Business: "Fixture Iyengar Yoga",
			City: "Exampleton", Region: "VIC", Country: "Australia", Emails: []string{"alex@example.com"},
			Phones: []string{"+61 400 555 010"}, Websites: []string{"https://alex.example.com"}, Meta: map[string][]string{}},
		{ID: "3002", URL: "https://iyengaryoga.asn.au/blog/studio/fixture-iyengar-yoga/", Name: "Fixture Iyengar Yoga",
			Address: "1 Example Street, Exampleton VIC 3000", City: "Exampleton", Region: "VIC", Country: "Australia",
			Emails: []string{"studio@example.org"}, Phones: []string{"03 5550 0102"},
			Meta: map[string][]string{"entry_type": {"school"}}},
		// the location text ends in a country the resolver knows
		{ID: "3003", URL: "https://iyengaryoga.asn.au/blog/teacher/blair-placeholder/", Name: "Blair Placeholder",
			City: "Sampleford", Country: "Ireland", Meta: map[string][]string{}},
		{ID: "3004", Name: "Cameron Overseas", Country: "Australia", Emails: []string{"cameron@example.com"}, Meta: map[string][]string{}},
	} // 3001 twice (deduped), 3005 has a blank name
	if !reflect.DeepEqual(got, want) {
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
}

func TestIyengarNZList(t *testing.T) {
	s := mustSource(t, "iyengar-nz")
	f := &bodyFetcher{
		fakeFetcher: &fakeFetcher{},
		post: func(u, body string) Page {
			if u != iyengarNZURL {
				return Page{Status: 404}
			}
			switch body {
			case "action=get_teacher&page=1":
				return fixturePage(t, "iyengar-nz_page1_SYNTHETIC.json")
			case "action=get_teacher&page=2":
				return fixturePage(t, "iyengar-nz_page2_SYNTHETIC.json")
			case "action=get_teacher&page=3":
				return fixturePage(t, "iyengar-nz_empty_SYNTHETIC.json")
			}
			return Page{Status: 404}
		},
	}
	got := collect(t, s, f)
	want := []Record{
		{ID: "alex-fixture", URL: "https://iyengar-yoga.org.nz/author/alex_fixture/", Name: "Alex Fixture", City: "Exampleton",
			Region: "Wellington Region", Country: "New Zealand", Emails: []string{"alex@example.com"},
			Websites: []string{"https://alex.example.com"}, Meta: map[string][]string{"level": {"Level 2"}}},
		// an empty website cell holds a link to a bare "https://": no website
		{ID: "blair-placeholder", URL: "https://iyengar-yoga.org.nz/author/blair-placeholder/", Name: "Blair Placeholder",
			Country: "New Zealand", Emails: []string{"blair@example.org"}, Meta: map[string][]string{"level": {"Level 1"}}},
		{ID: "cameron-overseas-example-com", URL: "https://iyengar-yoga.org.nz/author/cameron-overseas-example-com/", Name: "Cameron & Overseas",
			City: "Sampleford", Region: "Otago", Country: "New Zealand", Emails: []string{"cameron@example.com"},
			Meta: map[string][]string{"level": {"Level 3"}}},
	} // page 2 repeats Blair (deduped)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	if len(f.calls) != 3 { // stops at counter == 0, not at a short page
		t.Errorf("requests = %v", f.calls)
	}
}

func TestIyengarBEList(t *testing.T) {
	s := mustSource(t, "iyengar-be")
	f := &fakeFetcher{pages: map[string]Page{iyengarBEURL: fixturePage(t, "iyengar-be_page_SYNTHETIC.html")}}
	got := collect(t, s, f)
	if len(got) != 4 || len(f.calls) != 1 { // the blank-named block is no entry
		t.Fatalf("%d records, requests %v", len(got), f.calls)
	}
	want := []Record{
		// the decoy spans are stripped from the email; the first studio gives the address
		{ID: "alex-fixture", Name: "Alex Fixture", Business: "Fixture Iyengar Centre", Address: "Rue de l'Exemple, 26",
			PostalCode: "1050", City: "Ixelles", Country: "Belgium", Emails: []string{"alex@example.com"},
			Phones: []string{"+32 480 55 50 10"}, Websites: []string{"https://alex.example.com"},
			Meta: map[string][]string{"studios": {"Fixture Iyengar Centre", "Second Studio"}}},
		{ID: "blair-placeholder", Name: "Blair Placeholder", Address: "Route d'Exemple 30", PostalCode: "L-1234", City: "Luxembourg",
			Country: "Luxembourg", Emails: []string{"blair@example.org"}, Phones: []string{"+352 555 010"}, Meta: map[string][]string{}},
		// no address: the association's country
		{ID: "cameron-overseas", Name: "Cameron Overseas", Country: "Belgium", Emails: []string{"cameron@example.com"},
			Phones: []string{"+32 480 55 50 12"}, Meta: map[string][]string{}},
	}
	for i, w := range want {
		if !reflect.DeepEqual(got[i], w) {
			t.Errorf("record %d = %#v\nwant       %#v", i, got[i], w)
		}
	}
	// a second teacher of the same name gets a hash of the address after the slug
	if r := got[3]; !strings.HasPrefix(r.ID, "alex-fixture-") || len(r.ID) != len("alex-fixture-")+6 || r.City != "Ghent" || !reflect.DeepEqual(r.Emails, []string{"alex.two@example.net"}) {
		t.Errorf("duplicate-name record = %#v", r)
	}
	if byID(got)[got[3].ID].ID == "alex-fixture" {
		t.Error("ids must be unique")
	}
}

func TestYogaSplitLocation(t *testing.T) {
	cases := []struct{ in, head, country string }{
		{"Exampleton, Canada", "Exampleton", "Canada"},
		{"Germany", "", "Germany"},
		{"Exampleton, ZZ Region", "Exampleton, ZZ Region", ""},
		{"Somewhere", "Somewhere", ""},
		{"", "", ""},
	}
	for _, c := range cases {
		if h, ct := yogaSplitLocation(c.in); h != c.head || ct != c.country {
			t.Errorf("yogaSplitLocation(%q) = %q, %q; want %q, %q", c.in, h, ct, c.head, c.country)
		}
	}
	if got := yogaSlugID(" Amélie O'Neil_2 "); got != "am-lie-o-neil-2" {
		t.Errorf("yogaSlugID = %q", got)
	}
}

// emit's error (the -limit) stops every source's walk at once.
func TestYogaSourcesStopOnLimit(t *testing.T) {
	for _, tc := range []struct {
		slug  string
		pages map[string]Page
	}{
		{"iyengar-au", map[string]Page{iyengarAUURL: fixturePage(t, "iyengar-au_search_SYNTHETIC.json")}},
		{"iyengar-be", map[string]Page{iyengarBEURL: fixturePage(t, "iyengar-be_page_SYNTHETIC.html")}},
		{"jivamukti", map[string]Page{jivamuktiListURL: fixturePage(t, "jivamukti_all_SYNTHETIC.html")}},
	} {
		f := &fakeFetcher{pages: tc.pages}
		err := mustSource(t, tc.slug).List(context.Background(), f, func(Record) error { return errListLimit })
		if !errors.Is(err, errListLimit) {
			t.Errorf("%s: err = %v", tc.slug, err)
		}
	}
}
