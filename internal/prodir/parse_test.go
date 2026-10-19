package prodir

import (
	"os"
	"reflect"
	"strings"
	"testing"
)

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func parseACEFixture(t *testing.T, name string) *ACEProfile {
	t.Helper()
	toks, err := tokenizeACE(readFixture(t, name))
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return parseTokens(toks)
}

func TestACEProfileParsing(t *testing.T) {
	p := parseACEFixture(t, "ace_profile_SYNTHETIC.html")

	checks := map[string][2]string{
		"address":     {p.Address, "EXAMPLEVILLE, NY US 10001"},
		"postal_code": {p.PostalCode, "10001"},
		"phone":       {p.Phone, "(555) 010-0199"},
		"website":     {p.Website, "http://www.fixture-fitness.example"},
		"about":       {p.About, "Synthetic fixture bio. Second paragraph of the bio."},
		"credentials": {p.Credentials, "ACE Certified Personal Trainer, ACE Certified Health Coach"},
		"years_pro":   {p.YearsAsPro, "12"},
		"industry":    {p.YearsInIndustry, "11-15 years"},
		"focus":       {p.AreasOfFocus, "Active Aging; Strength Training; Weight Management"},
		"contact_url": {p.ContactURL, "/contact/jane-fixture"},
		"programs":    {p.ProgramsAndRates, ""}, // "Contact Me" filler is blanked
		"socials":     {p.SocialLinks, "https://www.instagram.com/fixture_fit"},
		// Background collects until the next h2, which resets it.
		"background": {p.Background, "Former synthetic athlete. Studied fixtures."},
	}
	for name, c := range checks {
		if c[0] != c[1] {
			t.Errorf("%s = %q, want %q", name, c[0], c[1])
		}
	}

	if len(p.Sections) != 2 {
		t.Fatalf("sections = %d, want 2", len(p.Sections))
	}
	pt := p.Sections[0]
	if pt.name != "Personal Trainer Specific Information" ||
		!reflect.DeepEqual(pt.items, []kv{{"Years Of Experience", "11-15 years"}, {"Preferred Client Type", "Older Adult, Healthy Adult"}}) {
		t.Errorf("section 0 = %+v", pt)
	}
	hc := p.Sections[1]
	if hc.name != "ACE Health Coach Specific Information" ||
		!reflect.DeepEqual(hc.items, []kv{{"Years of Experience", ""}}) { // "Unspecified" -> empty
		t.Errorf("section 1 = %+v", hc)
	}
}

func TestACEPhonePrecedence(t *testing.T) {
	// A line that matches the phone pattern beats an earlier odd-format line.
	p := parseACEFixture(t, "ace_profile_oddphone_SYNTHETIC.html")
	if p.Phone != "+1 555 010 0123" {
		t.Errorf("phone = %q, want the PHONE_RE match", p.Phone)
	}
	if p.Address != "Example Street 1, Exampletown" {
		t.Errorf("address = %q", p.Address)
	}
	// With no real phone line, the odd-format line is the fallback.
	p = parseACEFixture(t, "ace_profile_oddonly_SYNTHETIC.html")
	if p.Phone != "tel 555 0100 abc" {
		t.Errorf("fallback phone = %q", p.Phone)
	}
}

func TestACERemovedProfileIsError(t *testing.T) {
	if _, err := tokenizeACE(readFixture(t, "ace_removed_SYNTHETIC.html")); err == nil {
		t.Fatal("a page without the profile article must be an error")
	}
	// An article-looking string inside <script> is not a profile either.
	if _, err := tokenizeACE([]byte(`<script>"<article class='ace-pro-profile__bio-container'>"</script>`)); err == nil {
		t.Fatal("script content must not count as a profile block")
	}
}

func TestACETokensRoundTrip(t *testing.T) {
	toks, err := tokenizeACE(readFixture(t, "ace_profile_SYNTHETIC.html"))
	if err != nil {
		t.Fatal(err)
	}
	line, err := marshalLine(struct {
		ID     string  `json:"id"`
		URL    string  `json:"url"`
		Tokens []Token `json:"tokens"`
	}{"x", "u", toks})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(line), `["h2","Jane Q. Fixture",[]]`) {
		t.Errorf("token shape is not [kind,text,links]: %s", line)
	}
}

func TestSplitAddress(t *testing.T) {
	cases := []struct{ line, country, state, want string }{
		{"WEST NYACK, NY US 10994", "", "", "10994"},
		{"VALLETTA, MT VLT 1", "MT", "", "VLT 1"},
		{"TORONTO, ON CA M5V 2", "CA", "ON", "M5V 2"},
		{"", "", "", ""},
	}
	for _, c := range cases {
		if got := splitAddress(c.line, c.country, c.state); got != c.want {
			t.Errorf("splitAddress(%q,%q,%q) = %q, want %q", c.line, c.country, c.state, got, c.want)
		}
	}
}

func TestICFProfileParsing(t *testing.T) {
	f := parseICFProfile(readFixture(t, "icf_detail_enhanced_SYNTHETIC.html"))
	want := map[string]string{
		"Web Site":      "http://www.fixture-coaching.example",
		"Email Address": "Coach.Fixture@Example.TEST", // the mailto: href, case preserved
		"Phone":         "+1 555 010 0188",
		"Location":      "Exampletown, NY UNITED STATES",
	}
	for label, v := range want {
		if got := f.first(label); got != v {
			t.Errorf("%s = %q, want %q", label, got, v)
		}
	}
	if got := f.vals["Coaching Themes"]; !reflect.DeepEqual(got, []string{"Leadership Development", "Team Effectiveness"}) {
		t.Errorf("themes = %v", got)
	}
	// The label survives with no values when the site says Unspecified.
	if v, ok := f.vals["Gender"]; !ok || len(v) != 0 {
		t.Errorf("Gender = %v, %v; want present and empty", v, ok)
	}

	b := parseICFProfile(readFixture(t, "icf_detail_basic_SYNTHETIC.html"))
	for _, l := range []string{"Web Site", "Email Address", "Phone"} {
		if got := b.first(l); got != "" {
			t.Errorf("%s = %q, want empty for Unspecified", l, got)
		}
	}
	if b.first("Location") != "Placeholder City, SOUTH AFRICA" {
		t.Errorf("location = %q", b.first("Location"))
	}
	// Field order is the contact tab order (it becomes the CSV column order).
	if !reflect.DeepEqual(b.labels, []string{"Web Site", "Email Address", "Phone", "Location"}) {
		t.Errorf("labels = %v", b.labels)
	}
}

func TestICFCountryMatcher(t *testing.T) {
	cs, err := loadICFCountries("testdata")
	if err != nil {
		t.Fatal(err)
	}
	m := newCountryMatcher(cs)
	cases := []struct{ loc, want string }{
		{"Exampletown, NY UNITED STATES", "UNITED STATES"},
		{"Placeholder City, SOUTH AFRICA", "SOUTH AFRICA"},
		{"AUSTRALIA", "AUSTRALIA"},
		{"Somewhere without a country", ""},
		{"", ""},
	}
	for _, c := range cases {
		if got, _ := m.match(c.loc); got != c.want {
			t.Errorf("match(%q) = %q, want %q", c.loc, got, c.want)
		}
	}
	// A suffix only counts at a word boundary.
	if got, _ := m.match("XAUSTRALIA"); got != "" {
		t.Errorf("match(XAUSTRALIA) = %q, want no match", got)
	}
}

func TestICFCity(t *testing.T) {
	cases := []struct{ loc, country, want string }{
		{"Exampletown, NY UNITED STATES", "UNITED STATES", "Exampletown"},
		{"UNITED STATES", "UNITED STATES", ""},
		{"Lonely Town", "", "Lonely Town"},
	}
	for _, c := range cases {
		if got := icfCity(c.loc, c.country); got != c.want {
			t.Errorf("icfCity(%q,%q) = %q, want %q", c.loc, c.country, got, c.want)
		}
	}
}

func TestCountryResolution(t *testing.T) {
	r := newCountryResolver()
	cases := []struct{ raw, name, code string }{
		{"US", "United States", "US"},
		{"us", "United States", "US"},
		{"United States", "United States", "US"},
		{"UNITED STATES", "United States", "US"},
		{"USA", "United States", "US"},
		{"GB", "United Kingdom", "GB"},
		{"UK", "United Kingdom", "GB"},
		{"ZZ", "", ""},       // not in the whitelist: never taken from the raw string
		{"Atlantis", "", ""}, // unknown name
		{"", "", ""},
	}
	for _, c := range cases {
		name, code := r.resolve(c.raw)
		if name != c.name || code != c.code {
			t.Errorf("resolve(%q) = (%q,%q), want (%q,%q)", c.raw, name, code, c.name, c.code)
		}
	}
}

func TestACENiche(t *testing.T) {
	cases := []struct{ cred, want string }{
		{"ACE Certified Personal Trainer", "fitness"},
		{"ACE Certified Health Coach", "coaching"},
		{"ACE Certified Health Coach, ACE Certified Personal Trainer", "fitness"},
		{"ACE Certified Health Coach, ACE Certified Group Fitness Instructor", "fitness"},
		{"ACE Certified Health Coach, ACE Medical Exercise Specialist", "fitness"},
		{"", "fitness"},
	}
	for _, c := range cases {
		if got := aceNiche(c.cred); got != c.want {
			t.Errorf("aceNiche(%q) = %q, want %q", c.cred, got, c.want)
		}
	}
}

func TestACEAreasOfFocusDropsStoreLabels(t *testing.T) {
	base := string(readFixture(t, "ace_profile_SYNTHETIC.html"))
	const orig = "Active Aging<br />\nStrength Training<br />\nWeight Management<br />\n"
	if !strings.Contains(base, orig) {
		t.Fatal("fixture focus block not found")
	}
	cases := []struct{ name, block, want string }{
		{"mixed", "Active Aging<br />\nOn Sale Now<br />\nStrength Training<br />\nexpert articles quizzes<br />\nNEW COURSES<br />\nWeight Management<br />\nRecorded Webinar<br />\n",
			"Active Aging; Strength Training; Weight Management"},
		{"only labels", "On Sale Now<br />\nExpert Articles Quizzes<br />\nNew Courses<br />\nRecorded Webinar<br />\n", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			toks, err := tokenizeACE([]byte(strings.Replace(base, orig, c.block, 1)))
			if err != nil {
				t.Fatal(err)
			}
			if got := parseTokens(toks).AreasOfFocus; got != c.want {
				t.Errorf("AreasOfFocus = %q, want %q", got, c.want)
			}
		})
	}
}
