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

func TestExtractURLFalsePositives(t *testing.T) {
	none := []string{
		"www.FixtureFellowshipOf Fitness", "see www.fixturename .com", // www. needs a real domain
		"it brought improvement.Health to all", "Ms.Fit said", "a dancer.Fitness coach", // sentence boundary
		"https://www.linkedin.com", "linkedin.com/", // no profile path
		"https://www.youtube.com/watch?v=abc123", "https://youtu.be/abc123", // videos, not channels
	}
	for _, in := range none {
		if f := extractContacts(in); len(f.Websites) != 0 || len(f.Social) != 0 {
			t.Errorf("%q: websites=%v social=%v, want nothing", in, f.Websites, f.Social)
		}
	}
	found := []struct{ in, want string }{
		{"FIXTUREMOM.COM", "http://FIXTUREMOM.COM"},
		{"my site fixture.fitness", "http://fixture.fitness"},
		{"www.GoodFixture.Life", "http://www.GoodFixture.Life"},
	}
	for _, c := range found {
		if f := extractContacts(c.in); len(f.Websites) != 1 || f.Websites[0] != c.want {
			t.Errorf("%q: websites=%v, want %s", c.in, f.Websites, c.want)
		}
	}
	if f := extractContacts("https://www.youtube.com/@fixturechannel"); f.Social["youtube"] == "" {
		t.Errorf("youtube channel lost: %v", f.Social)
	}
}

func TestExtractHandleNearestPlatform(t *testing.T) {
	cases := []struct{ in, platform string }{
		{"Find me on Facebook, Google, or Instagram @fixture_handle", "instagram"},
		{"Instagram & Twitter @fixture_handle", "twitter"},
	}
	for _, c := range cases {
		f := extractContacts(c.in)
		if len(f.Social) != 1 || f.Social[c.platform] == "" {
			t.Errorf("%q: social=%v, want only %s", c.in, f.Social, c.platform)
		}
	}
}

func TestExtractPhonesRound2(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"Exampleville, NY 10001. (555) 010-0199.", []string{"(555) 010-0199"}},
		{"Exampleville, NY 10001 (555) 010-0199", []string{"(555) 010-0199"}},
		{"call (555)010-0199 now", []string{"(555)010-0199"}},
		{"+15550100100", []string{"+15550100100"}},
		{"+44 5550100100", []string{"+44 5550100100"}},
		{"+971 55501001", []string{"+971 55501001"}},
		{"+1 555", nil}, // too few digits
		{"call (555-010-0199) now", []string{"555-010-0199"}},
		{"2024-05-12 and 10001", nil},
	}
	for _, c := range cases {
		if got := extractContacts(c.in).Phones; !reflect.DeepEqual(got, c.want) {
			t.Errorf("%q: phones = %v, want %v", c.in, got, c.want)
		}
	}
	for _, v := range []string{"n/a", "none", "-", "+44", "0", "12345"} {
		if got := structuredPhone(v); got != "" {
			t.Errorf("structuredPhone(%q) = %q, want none", v, got)
		}
		if got := splitICFPhone(v); got != nil {
			t.Errorf("splitICFPhone(%q) = %v, want none", v, got)
		}
	}
	if got := structuredPhone(" 555 010 0199 "); got != "555 010 0199" {
		t.Errorf("structuredPhone = %q", got)
	}
}

func TestExtractAddressPrefix(t *testing.T) {
	f := extractContacts("Office: 5801 Fixture Road. Mail: 5801 Fixture Road, Exampleville, TX 75093")
	if want := []string{"5801 Fixture Road, Exampleville, TX 75093"}; !reflect.DeepEqual(f.Addresses, want) {
		t.Errorf("addresses = %v, want %v", f.Addresses, want)
	}
}

func TestStructuredEmailKeepsOwnDomain(t *testing.T) {
	if got := structuredEmails("Coach@mail.coachingfederation.org/xx"); !reflect.DeepEqual(got, []string{"coach@mail.coachingfederation.org"}) {
		t.Errorf("structuredEmails = %v", got)
	}
	if got := extractContacts("Coach@mail.coachingfederation.org").Emails; got != nil {
		t.Errorf("free text must still drop the directory's own domain: %v", got)
	}
}
