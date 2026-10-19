package prodir

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestAnimalFlowListAndDetail(t *testing.T) {
	s := mustSource(t, "animalflow")
	f := &fakeFetcher{pages: map[string]Page{
		animalflowBase + "/instructor-directory/?country=all":        {Body: readFixture(t, "animalflow_list1_SYNTHETIC.html")},
		animalflowBase + "/instructor-directory/page/2/?country=all": {Body: readFixture(t, "animalflow_list2_SYNTHETIC.html")},
		animalflowBase + "/profile?user=90001":                       {Body: readFixture(t, "animalflow_profile_SYNTHETIC.html")},
		animalflowBase + "/profile?user=90003":                       {Body: readFixture(t, "animalflow_profile_empty_SYNTHETIC.html")},
	}}
	got := collect(t, s, f)
	want := []Record{
		{ID: "90001", URL: animalflowBase + "/profile?user=90001", Name: "Alex Fixture", Business: "Fixture Studio",
			Address: "1 Example St, Exampleton, ON, Canada", Country: "Canada",
			Meta: map[string][]string{"certification": {"Level 1 Certified Instructor"}, "languages": {"English", "French"}}},
		{ID: "90003", URL: animalflowBase + "/profile?user=90003", Name: "Blair Placeholder",
			Address: "Sampleford, TX, USA", Country: "USA",
			Meta: map[string][]string{"certification": {"Level 2 Certified Instructor"}, "languages": {"English"}}},
		{ID: "90004", URL: animalflowBase + "/profile?user=90004", Name: "Cameron Sample", Business: "Sample Gym",
			Address: "Nowhereville, Middle Earth", Meta: map[string][]string{}},
	}
	if !reflect.DeepEqual(got, want) { // the nameless card 90002 is skipped
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	if len(f.calls) != 2 { // "Page 1 of 2": two list requests, no third
		t.Errorf("requests = %v", f.calls)
	}

	d, err := s.Detail(context.Background(), f, got[0])
	if err != nil {
		t.Fatal(err)
	}
	wantD := Record{Business: "Fixture Studio", Address: "Exampleton, ON, Canada", Country: "Canada",
		Emails: []string{"alex@example.com"}, Phones: []string{"555 010 0101"}, Websites: []string{"https://example.org/alex"},
		Bio: "Synthetic bio of the fixture instructor.",
		Meta: map[string][]string{"languages": {"English"}, "certification": {"Level 1 Certified Instructor"},
			"services": {"Virtual Group Classes", "In-Person Personal Training"}}}
	if !reflect.DeepEqual(d, wantD) {
		t.Errorf("detail = %#v\nwant     %#v", d, wantD)
	}

	if _, err := s.Detail(context.Background(), f, got[1]); !errors.Is(err, ErrNotFound) {
		t.Errorf("empty profile err = %v, want ErrNotFound", err)
	}
}
