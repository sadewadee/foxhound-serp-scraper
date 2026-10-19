package prodir

import (
	"reflect"
	"testing"
)

func TestREPsNZList(t *testing.T) {
	old := repsNZPerPage
	repsNZPerPage = 3
	defer func() { repsNZPerPage = old }()

	s := mustSource(t, "repsnz")
	post := func(n string) string {
		return repsNZAPI + "/posts?per_page=3&page=" + n + "&_fields=id,link,title,tags,categories"
	}
	f := &fakeFetcher{pages: map[string]Page{
		repsNZAPI + "/tags?per_page=100&page=1&_fields=id,name":       {Body: readFixture(t, "repsnz_tags_SYNTHETIC.json")},
		repsNZAPI + "/categories?per_page=100&page=1&_fields=id,name": {Body: readFixture(t, "repsnz_categories_SYNTHETIC.json")},
		post("1"): {Body: readFixture(t, "repsnz_posts1_SYNTHETIC.json")},
		post("2"): {Body: readFixture(t, "repsnz_posts2_SYNTHETIC.json")},
		post("3"): {Status: 400, Body: []byte(`{"code":"rest_post_invalid_page_number"}`)},
	}}
	got := collect(t, s, f)
	link := func(slug string) string { return "https://reps.org.nz/2026/01/01/" + slug + "/" }
	want := []Record{
		{ID: "1001", URL: link("alex-fixture"), Name: "Alex Fixture", Region: "Auckland", Country: "New Zealand",
			Meta: map[string][]string{"level": {"Personal Trainer (Contractor)"}}},
		// yoga-only registrant: category override; "Uncategorised" is no region
		{ID: "1002", URL: link("blair-placeholder"), Name: "Blair Placeholder", Country: "New Zealand", Category: "yoga_teacher",
			Meta: map[string][]string{"level": {"Yoga Teacher"}}},
		{ID: "1003", URL: link("cameron-o&#8217;sample"), Name: "Cameron O’Sample", Region: "Wellington City", Country: "New Zealand",
			Category: "pilates_instructor", Meta: map[string][]string{"level": {"Pilates Exercise Professional (Contractor)"}}},
		// fitness + yoga stays the default category
		{ID: "1004", URL: link("dana-both"), Name: "Dana Both", Region: "Auckland", Country: "New Zealand",
			Meta: map[string][]string{"level": {"Personal Trainer (Contractor)", "Yoga Teacher"}}},
		// facility: stays in the default category, flagged in Meta
		{ID: "1005", URL: link("fixture-gym-ltd"), Name: "Fixture Gym Ltd", Region: "Wellington City", Country: "New Zealand",
			Meta: map[string][]string{"registrant_type": {"Exercise Facility"}}},
	}
	if !reflect.DeepEqual(got, want) { // the nameless post 1006 is skipped
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	if len(f.calls) != 5 { // tags, categories, posts 1, 2 (full pages), 3 = HTTP 400
		t.Errorf("requests = %v", f.calls)
	}
}
