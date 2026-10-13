package prodir

import (
	"reflect"
	"strings"
	"testing"
)

func TestACEMeta(t *testing.T) {
	e := aceEntry{
		id:   "AcE9",
		list: aceListRec{"FullName": "Meta Fixture", "CertDisplay": "ignored when the profile has credentials"},
		prof: &ACEProfile{
			Credentials:     "ACE Certified Personal Trainer, ACE Certified Health Coach; ACE Certified Personal Trainer",
			YearsAsPro:      "12",
			YearsInIndustry: "11-15 years",
			AreasOfFocus:    "Active Aging; Strength Training;  ; Active Aging",
			// "Contact Me" style filler is blanked by the parser, so empty here.
			ProgramsAndRates: "",
			Phone:            "(555) 010-0199",
			About:            "not meta",
			Sections: []*aceSection{
				{name: "Personal Trainer Specific Information", items: []kv{
					{"Preferred Client Type", "Older Adult, Healthy Adult; Youth"},
					{"Years Of Experience", ""},
				}},
			},
		},
	}
	want := map[string][]string{
		// credential: split on ";" and ",", de-duplicated, source order kept.
		"credential":          {"ACE Certified Personal Trainer", "ACE Certified Health Coach"},
		"years_ace_certified": {"12"},
		"years_in_industry":   {"11-15 years"},
		"areas_of_focus":      {"Active Aging", "Strength Training"},
		// Section columns split on ";" only: the comma stays inside the value.
		"personal_trainer_specific_information__preferred_client_type": {"Older Adult, Healthy Adult", "Youth"},
	}
	if got := aceMeta(e); !reflect.DeepEqual(got, want) {
		t.Errorf("aceMeta = %v\nwant %v", got, want)
	}
}

func TestACEMetaListOnly(t *testing.T) {
	e := aceEntry{id: "AcE8", list: aceListRec{"FullName": "List Only", "City": "Placeholder", "CertDisplay": "ACE Certified Health Coach"}}
	want := map[string][]string{"credential": {"ACE Certified Health Coach"}}
	if got := aceMeta(e); !reflect.DeepEqual(got, want) {
		t.Errorf("aceMeta = %v, want %v", got, want)
	}
	// Nothing at all to store: an empty (non-nil) map.
	if got := aceMeta(aceEntry{id: "AcE7", list: aceListRec{}}); len(got) != 0 {
		t.Errorf("aceMeta of an empty record = %v, want empty", got)
	}
}

func TestICFMeta(t *testing.T) {
	var f icfFields
	f.add("Web Site", "http://www.fixture-coaching.example") // contact label: not meta
	f.add("Email Address", "x@example.test")
	f.add("Phone", "+1 555 010 0188")
	f.add("Location", "Exampletown, NY UNITED STATES")
	f.add("Coaching Themes", "Leadership Development")
	f.add("Coaching Themes", "Team Effectiveness")
	f.add("Coaching-Themes", "Leadership Development") // same slug: merged, de-duplicated
	f.add("Coaching-Themes", "Career")
	f.add("Gender", "Unspecified") // label without values: no key
	f.add("Key", "shadows a fixed column")

	e := icfEntry{
		rec: ICFRecord{
			Key: "IcF1", FullName: "not meta", Credential: "PCC",
			Designations: []string{"Designation A", " ", "Designation B", "Designation A"},
			StandardRate: "100 USD",
		},
		prof: &ICFProfile{Key: "IcF1", Fields: f, Email: "x@example.test"},
	}
	want := map[string][]string{
		"credential":      {"PCC"},
		"designations":    {"Designation A", "Designation B"},
		"standard_rate":   {"100 USD"},
		"coaching_themes": {"Leadership Development", "Team Effectiveness", "Career"},
		"f_key":           {"shadows a fixed column"},
	}
	if got := icfMeta(e); !reflect.DeepEqual(got, want) {
		t.Errorf("icfMeta = %v\nwant %v", got, want)
	}

	// Unfetched profile (none, or missing): list-level keys only.
	wantList := map[string][]string{
		"credential": {"PCC"}, "designations": {"Designation A", "Designation B"}, "standard_rate": {"100 USD"},
	}
	for name, p := range map[string]*ICFProfile{"none": nil, "missing": {Key: "IcF1", Missing: true, Fields: f}} {
		e.prof = p
		if got := icfMeta(e); !reflect.DeepEqual(got, wantList) {
			t.Errorf("%s profile: icfMeta = %v, want %v", name, got, wantList)
		}
	}
}

func TestPrintStatsMeta(t *testing.T) {
	ls := []Listing{
		{Meta: map[string][]string{"credential": {"A", "B"}, "zeta": {"x"}}},
		{Meta: map[string][]string{"credential": {"A"}}},
		{},
	}
	var sb strings.Builder
	PrintStats(&sb, "ace", ls)
	out := sb.String()
	ic, iz := strings.Index(out, "meta credential"), strings.Index(out, "meta zeta")
	if ic < 0 || iz < ic {
		t.Fatalf("meta lines missing or not sorted by key:\n%s", out)
	}
	for _, want := range []string{"listings 2, values 3, distinct 2", "listings 1, values 1, distinct 1"} {
		if !strings.Contains(out, want) {
			t.Errorf("stats missing %q:\n%s", want, out)
		}
	}
}
