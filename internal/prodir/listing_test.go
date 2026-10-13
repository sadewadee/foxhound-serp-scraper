package prodir

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSplitICFPhone(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"555.010.0100 (5550100100)", []string{"555.010.0100"}}, // same digits
		{"555.010.0100 (5550100142)", []string{"555.010.0100", "5550100142"}},
		{"0711/1234567", []string{"0711/1234567"}},
		{"+15550100100; +15550100142", []string{"+15550100100", "+15550100142"}},
		{"555 010 0100 / 555 010 0142", []string{"555 010 0100", "555 010 0142"}},
		{"5550100100or5550100142", []string{"5550100100", "5550100142"}},
		{"555-010-0100 (ext 12)", []string{"555-010-0100 (ext 12)"}},
		{"  ", nil},
	}
	for _, c := range cases {
		if got := splitICFPhone(c.in); !reflect.DeepEqual(got, c.want) {
			t.Errorf("splitICFPhone(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func writeACEToks(t *testing.T, dir string, profiles map[string][]Token) {
	t.Helper()
	var lines []string
	for id, toks := range profiles {
		b, _ := marshalLine(struct {
			ID     string  `json:"id"`
			URL    string  `json:"url"`
			Tokens []Token `json:"tokens"`
		}{id, ACEBase + "/pro/" + id + "/", toks})
		lines = append(lines, string(b[:len(b)-1]))
	}
	writeLines(t, filepath.Join(dir, "ace_profiles.jsonl"), lines...)
}

func TestACEListingsExtract(t *testing.T) {
	dir := t.TempDir()
	row := func(id string) string {
		return `{"EncryptedCustomerNumber":"` + id + `","ProfileURL":"/pro/` + id + `/","FullName":"Fixture ` + id + `","City":"Exampleville","StateCode":"NY","CountryName":"US","CertDisplay":"ACE Certified Personal Trainer","_page":1}`
	}
	writeLines(t, filepath.Join(dir, "ace_list.jsonl"), row("AcE1"), row("AcE2"), row("AcE3"))
	head := func(contact ...string) []Token {
		text := ""
		for _, c := range contact {
			text += c + "\n"
		}
		return []Token{{Kind: "h2", Text: "Name"}, {Kind: "text", Text: text}, {Kind: "hr"}}
	}
	about := func(s string, links ...Link) []Token {
		return []Token{{Kind: "h4", Text: "About"}, {Kind: "text", Text: s, Links: links}}
	}
	writeACEToks(t, dir, map[string][]Token{
		// the website field holds an email
		"AcE1": append(head("EXAMPLEVILLE, NY US 10001", "(555) 010-0199", "http://someone@fixture.example"), about("Bio.")...),
		// everything in the About text
		"AcE2": append(head("EXAMPLEVILLE, NY US 10001", "(555) 010-0199", "www.fixture-own.com"),
			about("Email me at coach@fixture.example or call 555.010.0142 / (555) 010-0199. Instagram @fixture.coach. "+
				"Also see fixturefit.com and www.fixture-own.com. Studio at 900 Haddon Avenue, Collingswood NJ 08108.",
				Link{"https://www.facebook.com/fixture.fb", "fb"}, Link{"https://www.facebook.com/ACEfitness", "ace"})...),
		// own website only in the text, nothing in the contact block
		"AcE3": append(head("EXAMPLEVILLE, NY US 10001"), about("My site is fixture-text.com.")...),
	})
	ls, err := Listings("ace", dir)
	if err != nil {
		t.Fatal(err)
	}
	by := map[string]Listing{}
	for _, l := range ls {
		by[l.Domain] = l
	}

	a1 := by["ace1.pro.acefitness.org"]
	if !reflect.DeepEqual(a1.Emails, []string{"someone@fixture.example"}) || a1.Website != ACEBase+"/pro/AcE1/" || a1.OwnWebsite {
		t.Errorf("ace1: emails=%v website=%q own=%v", a1.Emails, a1.Website, a1.OwnWebsite)
	}

	a2 := by["ace2.pro.acefitness.org"]
	if !reflect.DeepEqual(a2.Emails, []string{"coach@fixture.example"}) {
		t.Errorf("ace2 emails = %v", a2.Emails)
	}
	if want := []string{"(555) 010-0199", "555.010.0142"}; !reflect.DeepEqual(a2.Phones, want) {
		t.Errorf("ace2 phones = %v, want %v", a2.Phones, want)
	}
	if a2.Website != "http://www.fixture-own.com" || !a2.OwnWebsite {
		t.Errorf("ace2 website = %q own=%v", a2.Website, a2.OwnWebsite)
	}
	if got := a2.Social["instagram"]; got != "https://www.instagram.com/fixture.coach" {
		t.Errorf("ace2 instagram = %q", got)
	}
	if got := a2.Social["facebook"]; got != "https://www.facebook.com/fixture.fb" {
		t.Errorf("ace2 facebook = %q (the ACE corporate page must not be picked)", got)
	}
	if got := a2.Meta["other_websites"]; !reflect.DeepEqual(got, []string{"http://fixturefit.com"}) {
		t.Errorf("ace2 other_websites = %v", got)
	}
	if got := a2.Meta["street_address"]; !reflect.DeepEqual(got, []string{"900 Haddon Avenue, Collingswood NJ 08108"}) {
		t.Errorf("ace2 street_address = %v", got)
	}

	a3 := by["ace3.pro.acefitness.org"]
	if a3.Website != "http://fixture-text.com" || !a3.OwnWebsite || len(a3.Meta["other_websites"]) != 0 {
		t.Errorf("ace3 website = %q own=%v other=%v", a3.Website, a3.OwnWebsite, a3.Meta["other_websites"])
	}
}

func TestICFListingsExtract(t *testing.T) {
	dir := t.TempDir()
	b, _ := os.ReadFile("testdata/icf_countries.json")
	if err := os.WriteFile(filepath.Join(dir, "icf_countries.json"), b, 0o644); err != nil {
		t.Fatal(err)
	}
	writeLines(t, filepath.Join(dir, "icf_list.jsonl"),
		`{"key":"IcF1","fullName":"Coach One","location":"Exampletown, NY UNITED STATES"}`,
		`{"key":"IcF2","fullName":"Coach Two","headline":"Reach me: list@fixture.example, +1 555 010 0177, www.fixture-list.com, https://www.linkedin.com/in/fixture-two","description":"Also at other-list.com","location":"Exampletown, NY UNITED STATES"}`,
	)
	writeLines(t, filepath.Join(dir, "icf_profiles.jsonl"),
		`{"key":"IcF1","url":"u","fields":{"Web Site":["https://www.linkedin.com/in/fixture-one"],"Coaching Themes":["Mail to themes@fixture.example"]},"email":"main@fixture.example/xx","phone":"555.010.0100 (5550100142)","website":"https://www.linkedin.com/in/fixture-one","location":""}`,
		`{"key":"IcF2","url":"u","missing":true}`,
	)
	ls, err := Listings("icf", dir)
	if err != nil {
		t.Fatal(err)
	}
	by := map[string]Listing{}
	for _, l := range ls {
		by[l.Domain] = l
	}

	i1 := by["icf1.coach.coachingfederation.org"]
	if i1.Website != icfProfileURL("IcF1") || i1.OwnWebsite {
		t.Errorf("icf1 website = %q own=%v, want the profile URL", i1.Website, i1.OwnWebsite)
	}
	if got := i1.Social["linkedin"]; got != "https://www.linkedin.com/in/fixture-one" {
		t.Errorf("icf1 linkedin = %q", got)
	}
	if want := []string{"main@fixture.example", "themes@fixture.example"}; !reflect.DeepEqual(i1.Emails, want) {
		t.Errorf("icf1 emails = %v, want %v", i1.Emails, want)
	}
	if want := []string{"555.010.0100", "5550100142"}; !reflect.DeepEqual(i1.Phones, want) {
		t.Errorf("icf1 phones = %v, want %v", i1.Phones, want)
	}

	i2 := by["icf2.coach.coachingfederation.org"]
	if !reflect.DeepEqual(i2.Emails, []string{"list@fixture.example"}) || !reflect.DeepEqual(i2.Phones, []string{"+1 555 010 0177"}) {
		t.Errorf("icf2 emails=%v phones=%v", i2.Emails, i2.Phones)
	}
	// description is scanned before headline, so its site comes first
	if i2.Website != "http://other-list.com" || !i2.OwnWebsite {
		t.Errorf("icf2 website = %q own=%v", i2.Website, i2.OwnWebsite)
	}
	if i2.Social["linkedin"] != "https://www.linkedin.com/in/fixture-two" {
		t.Errorf("icf2 linkedin = %v", i2.Social)
	}
	if got := i2.Meta["other_websites"]; !reflect.DeepEqual(got, []string{"http://www.fixture-list.com"}) {
		t.Errorf("icf2 other_websites = %v", got)
	}
}
