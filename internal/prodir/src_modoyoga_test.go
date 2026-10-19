package prodir

import (
	"context"
	"reflect"
	"testing"
)

func TestModoYogaList(t *testing.T) {
	s := mustSource(t, "modoyoga")
	f := &fakeFetcher{pages: map[string]Page{modoListURL: {Body: readFixture(t, "modoyoga_list_SYNTHETIC.html")}}}
	got := collect(t, s, f)
	want := []Record{
		{ID: "fixtureville", URL: "https://modoyoga.com/fixtureville/", Name: "Modo Yoga Fixtureville",
			Address: "100 Example Street, Fixtureville, ON A1B 2C3", City: "Fixtureville", Region: "Ontario", PostalCode: "A1B 2C3", Country: "Canada"},
		// two accordion items on one subsite are told apart by name
		{ID: "sampletown-north", URL: "https://modoyoga.com/sampletown/", Name: "Modo Yoga Sampletown – North",
			Address: "200 Sample Road, Sampletown, ON A1B 2C4", City: "Sampletown", Region: "Ontario", PostalCode: "A1B 2C4", Country: "Canada"},
		{ID: "sampletown-south", URL: "https://modoyoga.com/sampletown/", Name: "Modo Yoga Sampletown – South",
			Address: "300 Sample Road, Sampletown, ON A1B 2C5", City: "Sampletown", Region: "Ontario", PostalCode: "A1B 2C5", Country: "Canada"},
		// own domain: that is the website; one-line address
		{ID: "ownsite-mills", URL: "http://www.modo-ownsite.example.org", Name: "Modo Yoga Ownsite Mills",
			Address: "400 Domain Pkwy, Ownsite, Ontario A1B 2C6", City: "Ownsite", Region: "Ontario", PostalCode: "A1B 2C6", Country: "Canada",
			Websites: []string{"http://www.modo-ownsite.example.org"}},
		{ID: "placeholder-bay", URL: "https://modoyoga.com/placeholder-bay/", Name: "Modo Yoga Placeholder Bay",
			Address: "500 Test Ave, Placeholder Bay, BC, V1A 2B3", City: "Placeholder Bay", Region: "British Columbia", PostalCode: "V1A 2B3", Country: "Canada"},
		{ID: "mockridge", URL: "https://modoyoga.com/mockridge/", Name: "Modo Yoga Mockridge",
			Address: "600 Demo Drive, Mockridge, CA 90001", City: "Mockridge", Region: "California", PostalCode: "90001", Country: "United States"},
		// International: the h3 is the country, the "FR" in the address is no region
		{ID: "exemple", URL: "https://modoyoga.com/exemple/", Name: "Modo Yoga Exemple",
			Address: "7 Rue Fictive, Exemple, FR 75011", City: "Exemple", PostalCode: "75011", Country: "France"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	if len(f.calls) != 1 {
		t.Errorf("requests = %v", f.calls)
	}
	if s.Category != "hot_yoga_teacher" || s.ListingType != "studio" {
		t.Errorf("category %q type %q", s.Category, s.ListingType)
	}
}

func TestModoYogaDetail(t *testing.T) {
	s := mustSource(t, "modoyoga")
	f := &fakeFetcher{pages: map[string]Page{"https://modoyoga.com/fixtureville/contact/": {Body: readFixture(t, "modoyoga_contact_SYNTHETIC.html")}}}
	d, err := s.Detail(context.Background(), f, Record{ID: "fixtureville", URL: "https://modoyoga.com/fixtureville/"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(d.Phones, []string{"(555) 010-0199"}) || len(d.Emails) != 0 {
		t.Errorf("phones %v emails %v", d.Phones, d.Emails)
	}
	wantSocial := map[string]string{"instagram": "https://www.instagram.com/modoyogafixtureville/", "facebook": "https://www.facebook.com/modoyogafixtureville"}
	if !reflect.DeepEqual(d.Social, wantSocial) {
		t.Errorf("social = %v", d.Social)
	}

	// a studio on its own domain has no subsite: no request
	f = &fakeFetcher{pages: map[string]Page{}}
	d, err = s.Detail(context.Background(), f, Record{ID: "ownsite-mills", URL: "http://www.modo-ownsite.example.org", Websites: []string{"http://www.modo-ownsite.example.org"}})
	if err != nil || len(f.calls) != 0 || len(d.Phones) != 0 {
		t.Errorf("own-domain detail: %v %v %v", d, err, f.calls)
	}

	// a gone contact page is ErrNotFound
	f = &fakeFetcher{pages: map[string]Page{}}
	if _, err := s.Detail(context.Background(), f, Record{ID: "x", URL: "https://modoyoga.com/x/"}); err == nil {
		t.Error("404 must fail Detail")
	}
}
