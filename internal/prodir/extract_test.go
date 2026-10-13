package prodir

import (
	"reflect"
	"testing"
)

func TestExtractEmails(t *testing.T) {
	cases := []struct {
		in   string
		want []string
		web  []string
	}{
		{"http://Someone@Example.test", []string{"someone@example.test"}, nil},
		{"www.someone@example.test", []string{"someone@example.test"}, nil},
		{"Contact me at coach@fixture.example.", []string{"coach@fixture.example"}, nil},
		{"mail A@x.example, a@X.example; b@y.example)", []string{"a@x.example", "b@y.example"}, nil},
		{"info@acefitness.org and logo@2x.png and me@mail.coachingfederation.org", nil, nil},
		{"name@host.example/xx", []string{"name@host.example"}, nil},
		// the email domain is not a website
		{"write to me@fixturefit.com today", []string{"me@fixturefit.com"}, nil},
	}
	for _, c := range cases {
		f := extractContacts(c.in)
		if !reflect.DeepEqual(f.Emails, c.want) || !reflect.DeepEqual(f.Websites, c.web) {
			t.Errorf("%q: emails=%v websites=%v, want %v / %v", c.in, f.Emails, f.Websites, c.want, c.web)
		}
	}
}

func TestExtractWebsitesAndSocial(t *testing.T) {
	f := extractContacts("Visit www.fixture-studio.com/classes, or fixturefit.com. Also https://www.acefitness.org/x and http://")
	if want := []string{"http://www.fixture-studio.com/classes", "http://fixturefit.com"}; !reflect.DeepEqual(f.Websites, want) {
		t.Errorf("websites = %v, want %v", f.Websites, want)
	}
	f = extractContacts("https://www.instagram.com/fixture_coach and www.linkedin.com/in/fixture-coach/")
	if len(f.Websites) != 0 || f.Social["instagram"] != "https://www.instagram.com/fixture_coach" ||
		f.Social["linkedin"] != "http://www.linkedin.com/in/fixture-coach/" {
		t.Errorf("urls: websites=%v social=%v", f.Websites, f.Social)
	}

	handles := []struct{ in, platform, want string }{
		{"Instagram @fixture.coach.", "instagram", "https://www.instagram.com/fixture.coach"},
		{"IG: @fixture_ig", "instagram", "https://www.instagram.com/fixture_ig"},
		{"FB@fixturefb", "facebook", "https://www.facebook.com/fixturefb"},
		{"TikTok@fixturetok", "tiktok", "https://www.tiktok.com/@fixturetok"},
		{"Twitter @fixturetw", "twitter", "https://twitter.com/fixturetw"},
		{"YouTube @fixtureyt", "youtube", "https://www.youtube.com/@fixtureyt"},
	}
	for _, c := range handles {
		f := extractContacts(c.in)
		if f.Social[c.platform] != c.want || len(f.Emails) != 0 {
			t.Errorf("%q: social=%v emails=%v, want %s", c.in, f.Social, f.Emails, c.want)
		}
	}
	// A bare handle is ambiguous; a URL for the platform beats a handle.
	if f := extractContacts("follow @nobody"); len(f.Social) != 0 {
		t.Errorf("bare handle: %v", f.Social)
	}
	f = extractContacts("IG @fixture_handle or https://instagram.com/fixture_url")
	if f.Social["instagram"] != "https://instagram.com/fixture_url" {
		t.Errorf("url must win over handle: %v", f.Social)
	}
}

func TestExtractPhones(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"Call (555) 010-0199 or 555.010.0142", []string{"(555) 010-0199", "555.010.0142"}},
		{"+1 555 010 0100", []string{"+1 555 010 0100"}},
		{"555-010-0100 and (555)010 0100 again", []string{"555-010-0100"}}, // same digits
		{"Cert 5550100199", nil},
		{"from 1995 - 2005", nil},
		{"ID A5550100199 X", nil},
		{"Since 2005 555 010 0199", []string{"555 010 0199"}},
		{"see http://fixture.example/555-010-0100-12", nil},
	}
	for _, c := range cases {
		if got := extractContacts(c.in).Phones; !reflect.DeepEqual(got, c.want) {
			t.Errorf("%q: phones = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestExtractAddresses(t *testing.T) {
	yes := []string{
		"14783 Pearl Rd",
		"900 Haddon Avenue, Collingswood NJ 08108",
		"6 E. 44th Street",
		"4110 Southpoint Blvd., Suite 222, Jacksonville, Fl. 32216",
		"7026 Corporate Way Suite 114",
	}
	for _, a := range yes {
		got := extractContacts("Find me at " + a + " any day.").Addresses
		if len(got) != 1 || got[0] != a {
			t.Errorf("address %q: got %v", a, got)
		}
	}
	no := []string{
		"10 years now. Along the way I have trained",
		"3 boys. Somewhere along the way",
		"1995 I became both and RD",
		"2019 at St. Bonaventure University",
		"8 state records along the way",
	}
	for _, s := range no {
		if got := extractContacts(s).Addresses; len(got) != 0 {
			t.Errorf("%q: false address %v", s, got)
		}
	}
}
