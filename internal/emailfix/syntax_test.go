package emailfix

import (
	"regexp"
	"slices"
	"strings"
	"testing"
)

// zodRe is the CRM (Zod) address check. Its lookaheads are written out as
// checks in assertCRM because the production regex is RE2.
var zodRe = regexp.MustCompile(`(?i)^[A-Z0-9_'+\-.]*[A-Z0-9_+-]@([A-Z0-9][A-Z0-9\-]*\.)+[A-Z]{2,}$`)

// assertCRM fails when an address the repairer accepted would be rejected by
// the CRM validator.
func assertCRM(t *testing.T, addr string) {
	t.Helper()
	if strings.HasPrefix(addr, ".") || strings.Contains(addr, "..") || !zodRe.MatchString(addr) {
		t.Errorf("ok=true address %q fails the CRM check", addr)
	}
}

func TestRepairSyntax(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		// Prefix and wrapper text in front of the local part.
		{"mailto:info@studio-a.com", "info@studio-a.com"},
		{"malito:info@studio-a.com", "info@studio-a.com"},
		{"matilto:info@studio-a.com", "info@studio-a.com"},
		{"emailto:info@studio-a.com", "info@studio-a.com"},
		{"email:info@studio-a.com", "info@studio-a.com"},
		{"e-mail: info@studio-a.com", "info@studio-a.com"},
		{"tel:info@studio-a.com", "info@studio-a.com"},
		{"complaintsemail:complaints@studio-a.com", "complaints@studio-a.com"},
		{"http://helpdesk@studio-a.com", "helpdesk@studio-a.com"},
		{"https://sales@studio-a.com", "sales@studio-a.com"},
		{"//anna.berg@studio-a.com", "anna.berg@studio-a.com"},
		{";office@studio-a.com", "office@studio-a.com"},
		{":office@studio-a.com", "office@studio-a.com"},
		{"u003eoffice@studio-a.com.sa", "office@studio-a.com.sa"},
		// Percent-encoded wrappers.
		{"usiness%22%3a%22info@studio-a.com", "info@studio-a.com"},
		{"mailto%20anna.berg@studio-a.com", "anna.berg@studio-a.com"},
		{"href%3d%22mailto%3acontact@studio-a.com", "contact@studio-a.com"},
		{"880-6564%0adance.mi@gmail.com", "dance.mi@gmail.com"},
		{"name%20redaktion%20%3cpress@uni-a.de", "press@uni-a.de"},
		{"i%6efo@studio-a.ae", "info@studio-a.ae"},
		{"info@studio%2db.co%2euk", "info@studio-b.co.uk"},
		// Invisible characters and NFKC.
		{"\u200b\u200cinfo@studio-a.ch", "info@studio-a.ch"},
		{"\u2060\ufeffsales@studio-a.ch", "sales@studio-a.ch"},
		{"home\u00adservice_ber\u00adlin@shop-a.ch", "homeservice_berlin@shop-a.ch"},
		{"gerencia@\ufb01tness-a.com.co", "gerencia@fitness-a.com.co"},
		// Dots around the local part and the domain.
		{"help@.clinic-a.co.za", "help@clinic-a.co.za"},
		{"anna.berg@.gmail.com", "anna.berg@gmail.com"},
		{"info@studio-a.com.", "info@studio-a.com"},
		{".info@studio-a.net", "info@studio-a.net"},
		{"...info@studio-a.co.za", "info@studio-a.co.za"},
		{".contact@studio-a.com", "contact@studio-a.com"},
		{"studio berlin - info@studio-a.de", "info@studio-a.de"},
		{"kontakt | office@studio-a.de", "office@studio-a.de"},
	}
	for _, c := range cases {
		got, rules, ok := RepairCtx(c.in, Context{})
		if got != c.want || !ok || len(rules) == 0 {
			t.Errorf("RepairCtx(%q) = (%q, %v, %v), want (%q, _, true) with rules", c.in, got, rules, ok, c.want)
		}
		assertCRM(t, got)
		if again, _, _ := RepairCtx(got, Context{}); again != got {
			t.Errorf("RepairCtx(%q) is not idempotent: %q then %q", c.in, got, again)
		}
	}
}

func TestRepairSyntaxRound2(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		// Spacing around "@" and inside the domain.
		{"info @studio-a.de", "info@studio-a.de"},
		{"info@ studio-a.de", "info@studio-a.de"},
		{"info@studio- berlin.de", "info@studio-berlin.de"},
		// Brackets, a leading "@", HTML tags, doubled parts.
		{"(info@studio-a.de)", "info@studio-a.de"},
		{"[info@studio-a.de]", "info@studio-a.de"},
		{"@info@studio-a.de", "info@studio-a.de"},
		{"<p>info@studio-a.de</p>", "info@studio-a.de"},
		{"info@info@studio-a.de", "info@studio-a.de"},
		{"info@studio-a.deinfo@studio-a.de", "info@studio-a.de"},
		{"anna.berg@gmail@gmail.com", "anna.berg@gmail.com"},
		// Leading dot on a role local, and an escaped role local.
		{".info@studio-a.de", "info@studio-a.de"},
		{"%69nfo@studio-a.de", "info@studio-a.de"},
	}
	for _, c := range cases {
		got, rules, ok := RepairCtx(c.in, Context{})
		if got != c.want || !ok || len(rules) == 0 {
			t.Errorf("RepairCtx(%q) = (%q, %v, %v), want (%q, _, true) with rules", c.in, got, rules, ok, c.want)
		}
		assertCRM(t, got)
		if again, _, _ := RepairCtx(got, Context{}); again != got {
			t.Errorf("RepairCtx(%q) is not idempotent: %q then %q", c.in, got, again)
		}
	}

	rejects := []string{
		// Protection artifact, and an escaped non-role local.
		"protect.info@studio-a.de",
		"%61nna@studio-a.de",
		// A one-letter local with a CRM-invalid domain is flagged, not kept.
		"c@-.at",
	}
	for _, in := range rejects {
		got, rules, ok := RepairCtx(in, Context{})
		if ok || !slices.Contains(rules, "syntax") {
			t.Errorf("RepairCtx(%q) = (%q, %v, %v), want rules containing syntax and ok=false", in, got, rules, ok)
		}
	}

	// A one-letter local with a valid domain keeps the #104 result: not ok, no rule.
	if got, rules, ok := RepairCtx("c@gmail-fixture.com", Context{}); got != "c@gmail-fixture.com" || ok || len(rules) != 0 {
		t.Errorf("RepairCtx(c@gmail-fixture.com) = (%q, %v, %v), want unchanged, no rules, ok=false", got, rules, ok)
	}
}

func TestRepairSyntaxReject(t *testing.T) {
	cases := []string{
		// Local part cut at its start: the real address is unknown.
		".kowalski@uni-a.pl",
		"..jdoe1977@aol.com",
		".w@gmail.com",
		// Separator glued onto text, or continuing a dotted local part.
		"g,michon@att-a.net",
		"joseph%3egardner@pub-a.com",
		"14>contact:emailmario@gmail.com",
		"recruiting.%20germany@bank-a.com",
		// Per-character percent obfuscation with nothing stripped.
		"nf%6f@studio-a.com",
		"ohn%6dsmith71@gmail.com",
		// More than one address, or "@@".
		"info@@studio-a.de",
		"anna@studio-a.de; office@studio-a.de",
		// Prefix stripped but a prefix word is still in front.
		"http//mailtonicole@shop-a.com",
		"contact:emailmario@gmail.com",
		// Dots at the wrong place.
		"info.@studio-a.com",
		"in..fo@studio-a.com",
		"info@studio..a.com",
		// Only a public suffix left after stripping dots.
		"shop@.co.uk",
		"x@.com.cn",
		"x@.ne.jp",
		// Placeholders.
		"stxxxxxx@uni-a.ru",
		"xxx@xxxx-xxxx.com",
		"nachname@uni-a.de",
		"name@studio-a.com",
		// Non-ASCII and leftover characters.
		"françois@studio-a.fr",
		"café@studio-a.fr",
		"a&b@studio-a.com",
		"a%zz@studio-a.com",
		"info@studio-a.com!",
		// "studio" is a TLD, so the space after the dot is not joined: ambiguous, reject.
		"info@studio. berlin.de",
		// Unrepairable CRM-invalid results: the final check adds "syntax".
		"anna berg@studio-a.com",
		"name.@studio-a.com",
		// Gmail local shorter than 6 after a repair.
		"mailto:abc@gmail.com",
	}
	for _, in := range cases {
		got, rules, ok := RepairCtx(in, Context{})
		if ok || !slices.Contains(rules, "syntax") {
			t.Errorf("RepairCtx(%q) = (%q, %v, %v), want rules containing syntax and ok=false", in, got, rules, ok)
		}
		if _, changed, _ := Repair(in); !changed {
			t.Errorf("Repair(%q) changed=false, want true so classify rejects it", in)
		}
	}
}

func TestCleanList(t *testing.T) {
	cases := []struct {
		name string
		in   []string
		want []string
	}{
		{"split on separators", []string{"anna@studio-a.de; office@studio-a.de"}, []string{"anna@studio-a.de", "office@studio-a.de"}},
		{"split keeps only valid piece", []string{"anna@x-a.de, .kowalski@uni-a.pl"}, []string{"anna@x-a.de"}},
		{"double at is rejected", []string{"info@@studio-a.de"}, nil},
		{"dedupe after repair", []string{"mailto:info@a-b.com", "info@a-b.com"}, []string{"info@a-b.com"}},
		{"drops not ok", []string{"john@x-a.de", "nodomain", ""}, []string{"john@x-a.de"}},
		{"comma and encoded space", []string{"anna@studio-a.de,%20office@studio-a.de"}, []string{"anna@studio-a.de", "office@studio-a.de"}},
		{"colon join", []string{"info@studio-a.de:office@studio-a.de"}, []string{"info@studio-a.de", "office@studio-a.de"}},
		{"cc join", []string{"info@studio-a.de&cc=office@studio-a.de"}, []string{"info@studio-a.de", "office@studio-a.de"}},
		{"two-char split with encoded spaces", []string{"ab@one-fixture.com;%20%20cd@two-fixture.org"}, []string{"ab@one-fixture.com", "cd@two-fixture.org"}},
		{"br join", []string{"ab@one-fixture.com <br>cd@two-fixture.org"}, []string{"ab@one-fixture.com", "cd@two-fixture.org"}},
		{"encoded space around lub", []string{"ab@one-fixture.com%20lub%20cd.e@two-fixture.org"}, []string{"ab@one-fixture.com", "cd.e@two-fixture.org"}},
		{"cc join two-char", []string{"ab@one-fixture.com&cc=cd@two-fixture.org"}, []string{"ab@one-fixture.com", "cd@two-fixture.org"}},
		{"colon repeats one address", []string{"ab@one-fixture.com:ab@one-fixture.com"}, []string{"ab@one-fixture.com"}},
		{"double space between addresses", []string{"ab@one-fixture.com  cd@two-fixture.org"}, []string{"ab@one-fixture.com", "cd@two-fixture.org"}},
		{"quote-gt repeats one address", []string{"ab@one-fixture.com\">ab@one-fixture.com"}, []string{"ab@one-fixture.com"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := CleanList(c.in)
			if !slices.Equal(got, c.want) {
				t.Errorf("CleanList(%q) = %q, want %q", c.in, got, c.want)
			}
			for _, a := range got {
				assertCRM(t, a)
			}
		})
	}
}

func TestRepairSyntaxRound3(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		// Whitespace after "." joins only when the label before it is not a TLD.
		{"info@fixture.com. phone", "info@fixture.com"},
		{"info@fixture.com. tel", "info@fixture.com"},
		{"info@fixture.com. info", "info@fixture.com"},
		{"info@fixture.com. email", "info@fixture.com"},
		{"info@fixture.com. mobile", "info@fixture.com"},
		{"xy@pec.rupar. puglia.it", "xy@pec.rupar.puglia.it"},
		{"xy@a.b. c.it", "xy@a.b.c.it"},
		{"xy@hot-  tub.com", "xy@hot-tub.com"},
		// A symbol before a space is a separator; a letter before a space is not.
		{"\u2709 stay@fixture-hotel.co.uk", "stay@fixture-hotel.co.uk"},
		{"\U0001F4E7 info@fixture-hotel.co.uk", "info@fixture-hotel.co.uk"},
		{"\u2022 info@fixture-hotel.co.uk", "info@fixture-hotel.co.uk"},
		{"\u2192 info@fixture-hotel.co.uk", "info@fixture-hotel.co.uk"},
		// Repeated wrappers from both ends.
		{"{{info@fixture-body.co.uk}}", "info@fixture-body.co.uk"},
		{"[(info@fixture.com)]", "info@fixture.com"},
	}
	for _, c := range cases {
		got, rules, ok := RepairCtx(c.in, Context{})
		if got != c.want || !ok || len(rules) == 0 {
			t.Errorf("RepairCtx(%q) = (%q, %v, %v), want (%q, _, true) with rules", c.in, got, rules, ok, c.want)
		}
		assertCRM(t, got)
		if again, _, _ := RepairCtx(got, Context{}); again != got {
			t.Errorf("RepairCtx(%q) is not idempotent: %q then %q", c.in, got, again)
		}
	}

	rejects := []string{
		"jos\u00e9 maria@fixture.com",
		"\u00fcnal kaya@fixture.de",
		"xy@a.b c.it",
		"x@sattva samadhi.com",
		"mary beth smith msmith@gmail.com",
		"mailto:prayerful touch massage@gmail.com",
		"info@pilatesfixture@gmail.com",
		"info@sales@fixture-mon.com",
		"ab@fixture.dekb@fixture.de",
	}
	for _, in := range rejects {
		got, rules, ok := RepairCtx(in, Context{})
		if ok || !slices.Contains(rules, "syntax") {
			t.Errorf("RepairCtx(%q) = (%q, %v, %v), want rules containing syntax and ok=false", in, got, rules, ok)
		}
	}
}

func TestRepairSyntaxRound4(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		// A space after a token with no letter or digit separates a label.
		{"\u2709 stay@fixture-hotel.co.uk", "stay@fixture-hotel.co.uk"},
		{"\u2022 info@fixture-hotel.co.uk", "info@fixture-hotel.co.uk"},
		{"studio berlin - info@fixture.de", "info@fixture.de"},
		{"grace lee | ir@fixture-ir.com", "ir@fixture-ir.com"},
		{"<span>email:</span> info@fixture-den.ca", "info@fixture-den.ca"},
		{"e%20mail%20info@fixture-zero.com", "info@fixture-zero.com"},
		{"via email applications.xy@fixture.com", "applications.xy@fixture.com"},
		// A dot or dash followed by a space, with a TLD before the dot.
		{"info@fixture.com. phone", "info@fixture.com"},
		{"info@fixture.com. tel", "info@fixture.com"},
		{"info@fixture.com tel", "info@fixture.com"},
		{"xy@pec.rupar. puglia.it", "xy@pec.rupar.puglia.it"},
	}
	for _, c := range cases {
		got, rules, ok := RepairCtx(c.in, Context{})
		if got != c.want || !ok || len(rules) == 0 {
			t.Errorf("RepairCtx(%q) = (%q, %v, %v), want (%q, _, true) with rules", c.in, got, rules, ok, c.want)
		}
		assertCRM(t, got)
		if again, _, _ := RepairCtx(got, Context{}); again != got {
			t.Errorf("RepairCtx(%q) is not idempotent: %q then %q", c.in, got, again)
		}
	}

	rejects := []string{
		// Punctuation glued to a word is not a separator.
		"anna- maria@fixture.de",
		"john_ smith@fixture.de",
		"o' brien@fixture.ie",
		// The space may hide a second label of the same domain.
		"xy@company.co. uk",
		"info@fixture.com. au plaisir",
		"info@company.co uk",
	}
	for _, in := range rejects {
		got, rules, ok := RepairCtx(in, Context{})
		if ok || !slices.Contains(rules, "syntax") {
			t.Errorf("RepairCtx(%q) = (%q, %v, %v), want rules containing syntax and ok=false", in, got, rules, ok)
		}
	}
}

func TestRepairSyntaxRound5(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"studio berlin  -  info@fixture.de", "info@fixture.de"},
		{"email:   info@fixture.de", "info@fixture.de"},
		{"\u2709   stay@fixture-hotel.co.uk", "stay@fixture-hotel.co.uk"},
	}
	for _, c := range cases {
		got, rules, ok := RepairCtx(c.in, Context{})
		if got != c.want || !ok || len(rules) == 0 {
			t.Errorf("RepairCtx(%q) = (%q, %v, %v), want (%q, _, true) with rules", c.in, got, rules, ok, c.want)
		}
		assertCRM(t, got)
		if again, _, _ := RepairCtx(got, Context{}); again != got {
			t.Errorf("RepairCtx(%q) is not idempotent: %q then %q", c.in, got, again)
		}
	}

	rejects := []string{
		"mary  smith@fixture.com",
		"anna-  maria@fixture.de",
		"contact  info@fixture.de",
		"mt_%20teamxy@fixture-mt.com",
	}
	for _, in := range rejects {
		got, rules, ok := RepairCtx(in, Context{})
		if ok || !slices.Contains(rules, "syntax") {
			t.Errorf("RepairCtx(%q) = (%q, %v, %v), want rules containing syntax and ok=false", in, got, rules, ok)
		}
	}
}

func TestRepairGlueBoundary(t *testing.T) {
	// G2: the leftover after a mid-domain cut is glue on its own ("www." or the
	// first label repeated). G1: a glued phone or code run in front of the local
	// part is stripped only when the cut-off tail confirms the remainder.
	cases := []struct {
		in, want string
	}{
		// A: domain still glued after a phone strip.
		{"555-1234info@acmeyoga.comwww.acmeyoga.com", "info@acmeyoga.com"},
		{"555-123-4567info@acmeyoga.comwww.acmeyoga.com", "info@acmeyoga.com"},
		{"emailcontact@acmeyoga.comwww.acmeyoga.com", "contact@acmeyoga.com"},
		{"acme@acme.lvwww.acme.lv", "acme@acme.lv"},
		// The phone rule strips these first; G1 is not reached.
		{"5551234info@acmeyoga.comwww.acmeyoga.com", "info@acmeyoga.com"},
		{"33693773info@acmeyoga.comwww.acmeyoga.com", "info@acmeyoga.com"},
		// B: domain glue cut, left boundary confirmed by the tail.
		{"0612345678studioname@gmail.comstudioname.org", "studioname@gmail.com"},
		{"8123456789studio.yoga@gmail.comstudio.yoga", "studio.yoga@gmail.com"},
		{"ltd0612345678brandname@gmail.combrandname.lk", "brandname@gmail.com"},
		// Negatives: no domain cut, or no glue, so nothing changes.
		{"123john@gmail.com", "123john@gmail.com"},
		{"xy@mail.company.com", "xy@mail.company.com"},
		{"xy@live.mercer.edu", "xy@live.mercer.edu"},
		{"info@sub.acmeyoga.com", "info@sub.acmeyoga.com"},
	}
	for _, c := range cases {
		got, rules, ok := RepairCtx(c.in, Context{})
		if got != c.want || !ok {
			t.Errorf("RepairCtx(%q) = (%q, %v, %v), want (%q, _, true)", c.in, got, rules, ok, c.want)
		}
		assertCRM(t, got)
		if again, rules2, _ := RepairCtx(got, Context{}); again != got || len(rules2) != 0 {
			t.Errorf("RepairCtx(%q) is not idempotent: %q then %q (rules %v)", c.in, got, again, rules2)
		}
	}

	// Ambiguous left boundary: the tail does not confirm the text, so ok=false.
	// The cut rule stays in rules so repair-emails rejects the row.
	rejects := []string{
		"0612345678coachanna@gmail.comwww.facebook.com",
		"2010boardmembers@hotmail.comother.kw",
		"ltd0612345678brandname@gmail.comwww.facebook.com",
		// Four digits are below the phone rule; no evidence in the tail.
		"6789info@internetspeedtest.prointernetspeedtest.pro",
		// Left part is not vouched by the tail and is not a role word on a non-free domain.
		"0612345678anna@acmeyoga.comwww.acmeyoga.com",
		"lee@studio-one.netwww.other-site.net",
		"201010.01anna@yahoo.commsn.com",
		"10.01carlos@msn.commsn.com",
		"hotmail.comaol.com@mac.commac.com",
		"korea+82-2-1234-5678acme_korea@acmespace.comwww.acme.co.kr",
		"info@yahoo.comwww.acme.com",
		"cakesanna.smith@acmecakes.comacmecakes.com",
	}
	for _, in := range rejects {
		got, rules, ok := RepairCtx(in, Context{})
		if ok || !(slices.Contains(rules, "middomain") || slices.Contains(rules, "tld")) {
			t.Errorf("RepairCtx(%q) = (%q, %v, %v), want ok=false with a domain-cut rule", in, got, rules, ok)
		}
	}

	// Evidence from the context accepts a cut: the address exists on its own.
	got, rules, ok := RepairCtx("anna@acmeyoga.comacmeyoga.com", Context{Exists: set("anna@acmeyoga.com")})
	if got != "anna@acmeyoga.com" || !ok || !slices.Contains(rules, "middomain") {
		t.Errorf("RepairCtx with Exists = (%q, %v, %v), want anna@acmeyoga.com ok with middomain", got, rules, ok)
	}
	assertCRM(t, got)
}

// TestRepairWebHostLocal: a web or mail host left inside the final local part makes
// the result ok=false, whether or not a rule fired and whether or not the address
// exists. Words that only contain a host name (olive.com, live.music) stay ok.
func TestRepairWebHostLocal(t *testing.T) {
	rejects := []string{
		"561.267.6131www.acme.comacme@gmail.com",
		"758-8818www.acmefh.comlebanon@acmefh.com",
		"u003ewww.acmeactors@yahoo.com",
		"yahoo.cominfo@acmepractice.comacme.clinic",
		"www.acme@yahoo.com",
		"acme.gmail.comx@acme.com",
	}
	for _, in := range rejects {
		got, _, ok := RepairCtx(in, Context{})
		if ok {
			t.Errorf("RepairCtx(%q) = (%q, _, true), want ok=false", in, got)
		}
	}

	// The merge target exists in the database, which is evidence (a) for a cut.
	// The web host check still rejects it.
	const merge = "gmail.comgmail.com@hotmail.comhotmail.com"
	if got, _, ok := RepairCtx(merge, Context{Exists: func(string) bool { return true }}); ok {
		t.Errorf("RepairCtx(%q) with Exists = (%q, _, true), want ok=false", merge, got)
	}

	stays := []string{
		"olive.combs@acme.com",
		"decrypt.computer@gmail.com",
		"live.music@acmeband.com",
		"me.frederic@protonmail.com",
		"web.team@acme.com",
		"info@acme.com",
		"anna.yahoo@acme.com",
	}
	for _, in := range stays {
		got, rules, ok := RepairCtx(in, Context{})
		if got != in || len(rules) != 0 || !ok {
			t.Errorf("RepairCtx(%q) = (%q, %v, %v), want unchanged ok=true", in, got, rules, ok)
		}
		assertCRM(t, got)
	}
}

// TestRepairRound6d covers trailing punctuation, the free-mail phone remainder,
// existence on free-mail, and a provider glued to a TLD in the domain.
func TestRepairRound6d(t *testing.T) {
	// Trailing punctuation after a valid domain is not a cut.
	for in, want := range map[string]string{
		"jmkeif@gmail.com/":  "jmkeif@gmail.com",
		"a.person@acme.net|": "a.person@acme.net",
	} {
		got, rules, ok := RepairCtx(in, Context{})
		if got != want || !ok {
			t.Errorf("RepairCtx(%q) = (%q, %v, %v), want (%q, _, true)", in, got, rules, ok, want)
		}
		assertCRM(t, got)
	}
	split := CleanList([]string{"jmkeif@gmail.com/ sklever@acmeymca.org"})
	if !slices.Equal(split, []string{"jmkeif@gmail.com", "sklever@acmeymca.org"}) {
		t.Errorf("CleanList split = %q, want both pieces", split)
	}

	// Phone strip on free-mail with a short remainder: rejected, phone kept in rules.
	// "info" is a role word, but it is generic only on a business domain.
	for _, in := range []string{"201010.01mark@yahoo.com", "800010.01219morgan@hotmail.com", "966-7660bjesup@aol.com", "9346082info@yahoo.com"} {
		got, rules, ok := RepairCtx(in, Context{})
		if ok || !slices.Contains(rules, "phone") {
			t.Errorf("RepairCtx(%q) = (%q, %v, %v), want ok=false with phone rule", in, got, rules, ok)
		}
	}
	// Distinctive remainders on free-mail, and any remainder on a business domain, are accepted.
	for in, want := range map[string]string{
		"417-2648chayilcots@gmail.com":   "chayilcots@gmail.com",
		"4-2536-3512rgartside@gmail.com": "rgartside@gmail.com",
		"849-6789info@acmedesign.com":    "info@acmedesign.com",
		"9346082info.acmeyoga@yahoo.com": "info.acmeyoga@yahoo.com",
	} {
		got, rules, ok := RepairCtx(in, Context{})
		if got != want || !ok {
			t.Errorf("RepairCtx(%q) = (%q, %v, %v), want (%q, _, true)", in, got, rules, ok, want)
		}
		assertCRM(t, got)
	}

	// Existence on free-mail counts only with a distinctive local part.
	exists := Context{Exists: func(string) bool { return true }}
	if got, _, ok := RepairCtx("anna@yahoo.comyahoo.com", exists); ok {
		t.Errorf("RepairCtx(anna@yahoo.comyahoo.com) with Exists = (%q, _, true), want ok=false", got)
	}
	if got, _, ok := RepairCtx("annasmith77@yahoo.comyahoo.com", exists); !ok || got != "annasmith77@yahoo.com" {
		t.Errorf("RepairCtx(annasmith77@yahoo.comyahoo.com) with Exists = (%q, _, %v), want ok", got, ok)
	}
	if got, _, ok := RepairCtx("anna@acmeyoga.comacmeyoga.com", exists); !ok || got != "anna@acmeyoga.com" {
		t.Errorf("RepairCtx(anna@acmeyoga.comacmeyoga.com) with Exists = (%q, _, %v), want ok", got, ok)
	}

	// Provider glued to a TLD or a short label in the domain: rejected.
	for _, in := range []string{"arif@msn.comyahoo.com.sa", "hong@yahoo.com.hkhotmail.com", "johnbrown@att.netaol.com"} {
		if got, rules, ok := RepairCtx(in, Context{}); ok {
			t.Errorf("RepairCtx(%q) = (%q, %v, true), want ok=false", in, got, rules)
		}
	}
	// Words that only contain a provider name stay ok.
	for _, in := range []string{"xy@nysgmail.com", "xy@sifymail.com", "xy@huaweicloud.com", "xy@golive.com", "xy@triblive.com", "xy@hiergmx.de"} {
		got, rules, ok := RepairCtx(in, Context{})
		if got != in || len(rules) != 0 || !ok {
			t.Errorf("RepairCtx(%q) = (%q, %v, %v), want unchanged ok=true", in, got, rules, ok)
		}
	}
}

// TestRepairLinkedEvidence: a linked address (exists and linked to this listing) is
// ownership evidence on free-mail too. A plain existing address is not.
func TestRepairLinkedEvidence(t *testing.T) {
	linked := Context{ExistsLinked: func(e string) bool { return e == "mark@yahoo.com" || e == "gaelkm@yahoo.com" }}
	existsOnly := Context{Exists: func(e string) bool { return e == "gaelkm@yahoo.com" }}

	if got, rules, ok := RepairCtx("201010.01mark@yahoo.com", linked); !ok || got != "mark@yahoo.com" {
		t.Errorf("RepairCtx(201010.01mark@yahoo.com) with ExistsLinked = (%q, %v, %v), want mark@yahoo.com ok", got, rules, ok)
	}
	if got, _, ok := RepairCtx("201010.01mark@yahoo.com", Context{}); ok {
		t.Errorf("RepairCtx(201010.01mark@yahoo.com) empty context = (%q, _, true), want ok=false", got)
	}

	if got, rules, ok := RepairCtx("gaelkm@yahoo.comfacebook.com", linked); !ok || got != "gaelkm@yahoo.com" {
		t.Errorf("RepairCtx(gaelkm@yahoo.comfacebook.com) with ExistsLinked = (%q, %v, %v), want gaelkm@yahoo.com ok", got, rules, ok)
	}
	if got, _, ok := RepairCtx("gaelkm@yahoo.comfacebook.com", existsOnly); ok {
		t.Errorf("RepairCtx(gaelkm@yahoo.comfacebook.com) with Exists only = (%q, _, true), want ok=false", got)
	}
}

// TestRepairFixedPoint: one RepairCtx call gives what a second call would.
func TestRepairFixedPoint(t *testing.T) {
	team := Context{
		Exists:       func(e string) bool { return e == "team@fixture.org" },
		ExistsLinked: func(e string) bool { return e == "team@fixture.org" },
	}
	for _, c := range []struct {
		in, want string
		ctx      Context
		rules    []string
		ok       bool
	}{
		{"8001151emailinfo@fixture.org", "info@fixture.org", Context{}, []string{"phone", "label"}, true},
		{"24504800-934-9410emailteam@fixture.org", "team@fixture.org", team, []string{"phone", "label"}, true},
		{"+4912345678email@fixture.org", "email@fixture.org", Context{}, []string{"phone", "syntax"}, false},
		{"+4912345678name@fixture.org", "name@fixture.org", Context{}, []string{"phone", "syntax"}, false},
	} {
		got, rules, ok := RepairCtx(c.in, c.ctx)
		if got != c.want || ok != c.ok || !slices.Equal(rules, c.rules) {
			t.Errorf("RepairCtx(%q) = (%q, %v, %v), want (%q, %v, %v)", c.in, got, rules, ok, c.want, c.rules, c.ok)
		}
	}

	// Property: repairing the output changes nothing, for every input the tables use.
	inputs := []string{
		"8001151emailinfo@fixture.org", "24504800-934-9410emailteam@fixture.org", "+4912345678email@fixture.org",
		"info@fixture.comwebsitewww.fixture.com", "info@fixture.dewebsitefixture.de", "info@fixture.comfacebook.com",
		"info@fixture.cominstagraminstagram.com", "info@fixture.live.com.aufacebook.com", "info@fixture.comview",
		"+1-212-555-7575info@studio.com", "9346082info@yahoo.com", "201010.01mark@yahoo.com", "emailteam@fixture.org",
		"gaelkm@yahoo.comfacebook.com", "anna@acmeyoga.comacmeyoga.com", "hello@liveingermany.de/",
	}
	ctxs := []Context{{}, team,
		{Exists: func(string) bool { return true }},
		{ExistsLinked: func(e string) bool { return e == "mark@yahoo.com" || e == "gaelkm@yahoo.com" }}}
	for _, in := range inputs {
		for i, c := range ctxs {
			once, _, ok1 := RepairCtx(in, c)
			twice, rules2, ok2 := RepairCtx(once, c)
			// A rejected repair (ok=false) is not applied, so only its string must be stable.
			if once != twice || (ok1 && (!ok2 || len(rules2) != 0)) {
				t.Errorf("not a fixed point (ctx %d): %q -> %q -> %q (%v) ok %v/%v", i, in, once, twice, rules2, ok1, ok2)
			}
		}
	}
}

// TestRepairWebSocialGlue: web or social text after a TLD is cut; real domains that
// merely contain a web word are not.
func TestRepairWebSocialGlue(t *testing.T) {
	for _, c := range []struct {
		in, want string
		rules    []string
		ok       bool
	}{
		{"info@fixture.comwebsitewww.fixture.com", "info@fixture.com", []string{"middomain"}, true},
		{"info@fixture.dewebsitefixture.de", "info@fixture.de", []string{"middomain"}, true},
		{"info@fixture.comfacebook.com", "info@fixture.com", []string{"middomain"}, true},
		{"info@fixture.cominstagraminstagram.com", "info@fixture.com", []string{"middomain"}, true},
		{"info@fixture.live.com.aufacebook.com", "info@fixture.live.com.au", []string{"middomain"}, true},
		{"emailinfo@fixture.comfacebookfacebook.com", "info@fixture.com", []string{"middomain", "label"}, true},
		{"info@fixture.orgfacebook.com", "info@fixture.org", []string{"middomain"}, true},
		// "team" is not a role word (syntaxRoles), so the cut has no evidence: rejected.
		{"team@fixture.comlinkedin.com", "team@fixture.com", []string{"middomain"}, false},
		{"usinfo@fixture.orgfacebook.com", "usinfo@fixture.org", []string{"middomain"}, false},
		// Free-mail-like local part without evidence: rejected.
		{"jane.doe.fixture@live.com.aufacebook.com", "jane.doe.fixture@live.com.au", []string{"middomain"}, false},
		{"kontakt@fixture.dewebwww.other-fixture.de", "kontakt@fixture.de", []string{"middomain"}, true},
		{"e-mailkontakt@fixture.dewebsitefixture.de", "kontakt@fixture.de", []string{"middomain", "label"}, true},
		{"info@fixture.dewebwww.fs-fixture.de", "info@fixture.de", []string{"middomain"}, true},
		// Domain is cut, local stays glued ("0761" is too short to be a phone run): rejected.
		{"0761emailinfo@fixture.com.phwebsitefixture.com.ph", "0761emailinfo@fixture.com.ph", []string{"middomain"}, false},
		{"801600e-mailkreisarchiv@fixture.dewebwww.fixture-kreis.de", "801600e-mailkreisarchiv@fixture.de", []string{"middomain"}, false},
		{"newsletter.someone@fixture.cominstagram.com", "newsletter.someone@fixture.com", []string{"middomain"}, false},
	} {
		got, rules, ok := RepairCtx(c.in, Context{})
		if got != c.want || ok != c.ok || !slices.Equal(rules, c.rules) {
			t.Errorf("RepairCtx(%q) = (%q, %v, %v), want (%q, %v, %v)", c.in, got, rules, ok, c.want, c.rules, c.ok)
		}
	}
	// Real domains that contain a web word: unchanged. The subdomain forms reach the
	// web-word branch of cutMidDomain; the .com* forms are TLD-prefix lookalikes.
	for _, in := range []string{
		"anna@shop.tmweb.ru", "sales@mysite.vpweb.com", "user@mb.infoweb.ne.jp", "info@parks.myweblinx.net",
		"kefu@idc.ucweb.com", "bob@dept.fourlakes.com", "info@fixture.comurlaub.de", "info@fixture.comweb.de",
		"xy@fixture.comwebmail.com", "post@fixture.nowebbladfixtureoslo.no",
	} {
		got, rules, ok := RepairCtx(in, Context{})
		if got != in || len(rules) != 0 || !ok {
			t.Errorf("RepairCtx(%q) = (%q, %v, %v), want unchanged ok=true", in, got, rules, ok)
		}
	}
}
