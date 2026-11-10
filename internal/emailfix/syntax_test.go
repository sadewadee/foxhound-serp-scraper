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
		{"anna@acmeyoga.comacmeyoga.com", "anna@acmeyoga.com"},
		{"lee@studio-one.netwww.other-site.net", "lee@studio-one.net"},
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
		"5551234info@acmeyoga.comwww.acmeyoga.com",
	}
	for _, in := range rejects {
		got, rules, ok := RepairCtx(in, Context{})
		if ok || !(slices.Contains(rules, "middomain") || slices.Contains(rules, "tld")) {
			t.Errorf("RepairCtx(%q) = (%q, %v, %v), want ok=false with a domain-cut rule", in, got, rules, ok)
		}
	}
}
