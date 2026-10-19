package prodir

import (
	"reflect"
	"testing"
)

func TestREPsUAEList(t *testing.T) {
	s := mustSource(t, "repsuae")
	u := func(n string) string { return repsUAEList + "?page=" + n }
	f := &fakeFetcher{pages: map[string]Page{
		u("1"): {Body: readFixture(t, "repsuae_list1_SYNTHETIC.html")},
		u("2"): {Body: readFixture(t, "repsuae_list2_SYNTHETIC.html")},
	}}
	got := collect(t, s, f)
	want := []Record{
		{ID: "900001", Name: "Alex Fixture", Region: "Dubai", Country: "United Arab Emirates", Meta: map[string][]string{
			"reps_number": {"900001"}, "category": {"Category A", "Category B"}, "role": {"Personal Trainer", "Gym Instructor"},
			"nationality": {"Examplian"}, "expires": {"30 Jun 2027"}}},
		// a single meta row that is an emirate is not a nationality
		{ID: "900002", Name: "Blair Placeholder", Region: "Dubai", Country: "United Arab Emirates", Meta: map[string][]string{
			"reps_number": {"900002"}, "category": {"Category C"}, "role": {"Group Exercise Instructor"}, "expires": {"01 Jan 2026"}}},
		{ID: "900003", Name: "Cameron Sample", Region: "Sharjah", Country: "United Arab Emirates", Meta: map[string][]string{
			"reps_number": {"900003"}, "category": {"Category A"}, "role": {"Personal Trainer"},
			"nationality": {"Sampleish"}, "expires": {"31 Oct 2026"}}},
	}
	if !reflect.DeepEqual(got, want) { // the row without a REPs number and the repeat of 900001 are skipped
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	if len(f.calls) != 2 { // page 2 ends at 5 of 5
		t.Errorf("requests = %v", f.calls)
	}
}
