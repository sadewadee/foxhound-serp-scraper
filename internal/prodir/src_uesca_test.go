package prodir

import (
	"context"
	"reflect"
	"testing"
)

func TestUescaList(t *testing.T) {
	s := mustSource(t, "uesca")
	u := func(n string) string { return uescaBase + "?pagenum=" + n }
	f := &fakeFetcher{pages: map[string]Page{
		u("1"): {Body: readFixture(t, "uesca_list_SYNTHETIC.html")},
		u("2"): {Body: readFixture(t, "uesca_list_empty_SYNTHETIC.html")},
	}}
	got := collect(t, s, f)
	want := []Record{
		{ID: "101", URL: uescaBase + "entry/101/", Name: "Alex Fixture", Bio: "Synthetic endurance coach.",
			City: "Exampleton", Region: "Colorado", Country: "United States", Category: "sports_running",
			Meta: map[string][]string{"sports": {"Cycling", "Running (marathon or shorter)", "Triathlon"},
				"specialization_level": {"Beginner"}, "experience": {"5-10 years"}}},
		{ID: "102", URL: uescaBase + "entry/102/", Name: "Blair Placeholder", Bio: "Synthetic cyclist.",
			City: "Sampleford", Region: "Ontario", Country: "Canada", Category: "sports_cycling",
			Meta: map[string][]string{"sports": {"Cycling"}, "specialization_level": {"Expert/Advanced"}, "experience": {"1-5 years"}}},
		{ID: "103", URL: uescaBase + "entry/103/", Name: "Cameron Overseas", Bio: "Pool coach.",
			City: "Dublin", Country: "Ireland", Category: "swimming_coach",
			Meta: map[string][]string{"sports": {"Swimming", "Other"}, "specialization_level": {"Intermediate"}, "experience": {"10+ years"}}},
		{ID: "104", URL: uescaBase + "entry/104/", Name: "Dana Other", Bio: "Yoga for athletes.",
			City: "Nowhere", Country: "United States",
			Meta: map[string][]string{"sports": {"Yoga"}, "specialization_level": {"Beginner"}, "experience": {"1-5 years"}}},
	}
	if !reflect.DeepEqual(got, want) { // the nameless card 105 is dropped; no matching sport keeps the default category
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	if len(f.calls) != 2 {
		t.Errorf("requests = %v", f.calls)
	}
	if err := s.List(context.Background(), &fakeFetcher{pages: map[string]Page{u("1"): {Status: 500}}}, func(Record) error { return nil }); err == nil {
		t.Error("HTTP 500 must fail List")
	}
}

func TestUescaDetail(t *testing.T) {
	s := mustSource(t, "uesca")
	r := Record{ID: "101", URL: uescaBase + "entry/101/", Category: "sports_running"}
	f := &fakeFetcher{pages: map[string]Page{r.URL: {Body: readFixture(t, "uesca_profile_SYNTHETIC.html")}}}
	d, err := s.Detail(context.Background(), f, r)
	if err != nil {
		t.Fatal(err)
	}
	want := Record{
		Phones: []string{"+15555550101"}, Websites: []string{"https://alex-fixture.example.com"},
		Social: map[string]string{"facebook": "https://www.facebook.com/alexfixture",
			"instagram": "https://www.instagram.com/alexfixture/", "linkedin": "https://www.linkedin.com/in/alexfixture/"},
		Bio:  "Synthetic bio, mail me at alex@example.com. Train smart.",
		Meta: map[string][]string{"credential": {"UESCA Running Coach", "Example Strength Certificate"}, "other_specialties": {"Injury Prevention"}},
	}
	if !reflect.DeepEqual(d, want) {
		t.Errorf("detail = %#v\nwant     %#v", d, want)
	}
	l := recordListing(s, mergeRecord(r, d), newCountryResolver())
	if len(l.Emails) != 1 || len(l.Phones) != 1 || l.Social["instagram"] == "" || l.Category != "sports_running" {
		t.Errorf("listing = %+v", l)
	}

	// no phone / website / social: nothing but empty values
	f.pages[r.URL] = Page{Body: readFixture(t, "uesca_profile_plain_SYNTHETIC.html")}
	d, err = s.Detail(context.Background(), f, r)
	if err != nil || len(d.Phones) != 0 || len(d.Websites) != 0 || len(d.Social) != 0 || d.Bio != "" {
		t.Errorf("plain detail = %#v, %v", d, err)
	}

	f.pages[r.URL] = Page{Body: []byte("<html><body>nothing</body></html>")}
	if _, err := s.Detail(context.Background(), f, r); err == nil {
		t.Error("layout change must fail")
	}
}

func TestUescaCategory(t *testing.T) {
	for _, c := range []struct {
		sports []string
		want   string
	}{
		{[]string{"Ultrarunning"}, "sports_running"},
		{[]string{"Cycling", "Triathlon"}, "sports_triathlon"},
		{[]string{"Swimming", "Cycling"}, "sports_cycling"},
		{[]string{"Swimming"}, "swimming_coach"},
		{nil, ""},
	} {
		if got := uescaCategory(c.sports); got != c.want {
			t.Errorf("%v -> %q, want %q", c.sports, got, c.want)
		}
	}
}
