package emailfix

import (
	"strings"
	"testing"
)

func TestRepair(t *testing.T) {
	cases := []struct {
		in      string
		want    string
		changed bool
		ok      bool
	}{
		// R1: phone glued in front of the local part.
		{"+1-212-555-7575info@studio.com", "info@studio.com", true, true},
		{"+1.808.228.8417info@dralysa.com", "info@dralysa.com", true, true},
		{"9346082info@x.com", "info@x.com", true, true},
		{"812-3868-7387marcom@x.com", "marcom@x.com", true, true},
		{"(212) 555-7575hello@studio.com", "hello@studio.com", true, true},
		{"9346082info@yahoo.com", "info@yahoo.com", true, false},                 // a role word is generic only on a business domain: a short personal Yahoo mailbox is rejected
		{"9346082info@gmail.com", "9346082info@gmail.com", false, true},          // Gmail cannot issue a 4-char local part
		{"+6281234567890johnsmith@gmail.com", "johnsmith@gmail.com", true, true}, // '+' marks a phone
		{"9346082john@studio.com", "john@studio.com", true, true},                // non-free domain
		// R2: website text glued after the TLD.
		{"info@inivie.comview", "info@inivie.com", true, true},
		{"hi@balifitness.asiaphone", "hi@balifitness.asia", true, false},
		{"yo@ttbeach.clubmenu", "yo@ttbeach.club", true, false},
		{"zo@studio.co.ukhome", "zo@studio.co.uk", true, false},
		// Both at once, and case folding.
		{"+1-212-555-7575INFO@Studio.ComView", "info@studio.com", true, true},
		// Legitimate addresses stay untouched.
		{"13812345678@163.com", "13812345678@163.com", false, true},
		{"john2024@gmail.com", "john2024@gmail.com", false, true},
		{"24hourfitness@x.com", "24hourfitness@x.com", false, true},
		{"info@studio.uk.com", "info@studio.uk.com", false, true},
		{"ab@b.company", "ab@b.company", false, true},
		{"a@b.company", "a@b.company", false, false}, // single-char local part is rejected by rule
		{"12345678901@gmail.com", "12345678901@gmail.com", false, true},
		{"9346082john@gmail.com", "9346082john@gmail.com", false, true}, // bare digits, free-mail, no role word
		{"Info@Studio.com", "info@studio.com", false, true},
		// Unrecoverable.
		{"nodomain", "nodomain", false, false},
		// CRM-invalid and unrepairable: carries the "syntax" rule so repair-emails marks it malformed.
		{"info@studio", "info@studio", true, false},
		{"ameliahgroves@gmail.comn", "ameliahgroves@gmail.com", true, false},
		{"info@studio.zzzzqq", "info@studio.zzzzqq", false, false},
		{"a@b.com", "a@b.com", false, false},                                               // local part too short
		{"kelsey@elevationfitnesspdx.con", "kelsey@elevationfitnesspdx.con", false, false}, // typo, not glue
		{"aaron@gmail.com.archive", "aaron@gmail.com.archive", false, false},               // ".archi" would be a wrong guess
		{"paint@gmail.coom", "paint@gmail.coom", false, false},
		{"hello@liveingermany.de/", "hello@liveingermany.de", true, true},
		// CRM-invalid and unrepairable: carries the "syntax" rule so repair-emails marks it malformed.
		{"in fo@x.com", "in fo@x.com", true, false},
		{"+1-212-555-7575@x.com", "+1-212-555-7575@x.com", false, true}, // no letter after the run: left alone
	}
	for _, c := range cases {
		got, changed, ok := Repair(c.in)
		if got != c.want || changed != c.changed || ok != c.ok {
			t.Errorf("Repair(%q) = (%q, %v, %v), want (%q, %v, %v)", c.in, got, changed, ok, c.want, c.changed, c.ok)
		}
		if ok {
			assertCRM(t, got)
		}
	}
}

func set(addrs ...string) func(string) bool {
	return func(e string) bool {
		for _, a := range addrs {
			if a == e {
				return true
			}
		}
		return false
	}
}

func TestRepairCtx(t *testing.T) {
	cases := []struct {
		name  string
		in    string
		ctx   Context
		want  string
		rules string
	}{
		// Case 1: literal label glue.
		{"label keeps the s", "emails.anwar369@gmail.com", Context{ExistsLinked: set("s.anwar369@gmail.com")}, "s.anwar369@gmail.com", "label"},
		{"label needs a linked free-mail twin", "emails.anwar369@gmail.com", Context{Exists: set("s.anwar369@gmail.com")}, "emails.anwar369@gmail.com", ""},
		{"label before role word", "emailinfo@studio.com", Context{}, "info@studio.com", "label"},
		{"e-mail label", "e-mailinfo@studio.com", Context{}, "info@studio.com", "label"},
		{"label non-role needs existing", "emailjohn@studio.com", Context{}, "emailjohn@studio.com", ""},
		{"label non-role existing", "emailjohn@studio.com", Context{Exists: set("john@studio.com")}, "john@studio.com", "label"},
		{"bare mail local untouched", "mail@studio.com", Context{}, "mail@studio.com", ""},
		// Case 2: place/word glued before a role word.
		// An existing role elsewhere, or the listing's own domain, is not evidence:
		// the lead may be a real mailbox name. Only a linked role strips.
		{"rostock exists only stays", "rostockinfo@uni-rostock.de", Context{Exists: set("info@uni-rostock.de")}, "rostockinfo@uni-rostock.de", ""},
		{"rostock", "rostockinfo@uni-rostock.de", Context{ExistsLinked: set("info@uni-rostock.de")}, "info@uni-rostock.de", "prefix"},
		{"hannover own domain stays", "hannoverinfo@hannover-yoga.de", Context{ListingDomains: []string{"www.hannover-yoga.de"}}, "hannoverinfo@hannover-yoga.de", ""},
		{"hannover linked", "hannoverinfo@hannover-yoga.de", Context{ExistsLinked: set("info@hannover-yoga.de")}, "info@hannover-yoga.de", "prefix"},
		{"muenster exists only stays", "nsterinfo@uni.de", Context{Exists: set("info@uni.de")}, "nsterinfo@uni.de", ""},
		{"muenster", "nsterinfo@uni.de", Context{ExistsLinked: set("info@uni.de")}, "info@uni.de", "prefix"},
		{"palaiseau contact exists only stays", "palaiseaucontact@mairie.fr", Context{Exists: set("contact@mairie.fr")}, "palaiseaucontact@mairie.fr", ""},
		{"palaiseau contact", "palaiseaucontact@mairie.fr", Context{ExistsLinked: set("contact@mairie.fr")}, "contact@mairie.fr", "prefix"},
		{"republic own domain stays", "republicinfo@republic.com", Context{ListingDomains: []string{"republic.com"}}, "republicinfo@republic.com", ""},
		{"republic", "republicinfo@republic.com", Context{ExistsLinked: set("info@republic.com")}, "info@republic.com", "prefix"},
		{"say is a real lead", "sayhello@x.com", Context{ExistsLinked: set("hello@x.com")}, "sayhello@x.com", ""},
		{"unvouched place glue stays", "rostockinfo@uni-rostock.de", Context{}, "rostockinfo@uni-rostock.de", ""},
		{"boxoffice protected", "boxoffice@theatre.com", Context{Exists: set("office@theatre.com"), ListingDomains: []string{"theatre.com"}}, "boxoffice@theatre.com", ""},
		{"apoeppel", "apoeppel@uni.de", Context{Exists: set("poeppel@uni.de")}, "apoeppel@uni.de", ""},
		{"customerservice", "customerservice@x.com", Context{Exists: set("service@x.com")}, "customerservice@x.com", ""},
		{"studierendenservice", "studierendenservice@uni.de", Context{Exists: set("service@uni.de")}, "studierendenservice@uni.de", ""},
		{"studierendenoffice protected", "studierendenoffice@uni.de", Context{Exists: set("office@uni.de")}, "studierendenoffice@uni.de", ""},
		{"role lead protected", "contactinfo@x.com", Context{Exists: set("info@x.com")}, "contactinfo@x.com", ""},
		// Case 3: text between two domains.
		{"own domain mid glue", "sales@plyconvanlines.complycongroup.com", Context{ListingDomains: []string{"plyconvanlines.com"}}, "sales@plyconvanlines.com", "middomain"},
		{"existing mid glue", "a1@vendor.comother.com", Context{Exists: set("a1@vendor.com")}, "a1@vendor.com", "middomain"},
		{"mid glue unvouched", "a1@vendor.comother.com", Context{}, "a1@vendor.comother.com", ""},
		{"multi-label suffix kept", "info@studio.com.au", Context{ListingDomains: []string{"studio.com.au"}}, "info@studio.com.au", ""},
		{"co.uk kept", "info@studio.co.uk", Context{}, "info@studio.co.uk", ""},
		{"suffix-like label kept", "info@mail.company.com", Context{ListingDomains: []string{"company.com"}}, "info@mail.company.com", ""},
		{"live.mercer.edu kept", "luke.sadd@live.mercer.edu", Context{}, "luke.sadd@live.mercer.edu", ""},
		{"dealerdesk subdomain kept", "verkauf@auto-zeilinger.dealerdesk.de", Context{Exists: set("verkauf@auto-zeilinger.de")}, "verkauf@auto-zeilinger.dealerdesk.de", ""},
		{"wwww after ccTLD", "nicki@ritesforgirls.co.ukwww.ritesforgirls.com", Context{Exists: set("nicki@ritesforgirls.co.uk")}, "nicki@ritesforgirls.co.uk", "middomain"},
		{"np tld kept", "info@foodforlife.org.np", Context{}, "info@foodforlife.org.np", ""},
		{"mm tld kept", "zgs@baganmail.net.mm", Context{}, "zgs@baganmail.net.mm", ""},
		{"tech support compound", "techsupport@usailighting.com", Context{ListingDomains: []string{"usailighting.com"}}, "techsupport@usailighting.com", ""},
		{"ptymail kept on own domain", "ptymail@pattayamail.com", Context{ListingDomains: []string{"pattayamail.com"}}, "ptymail@pattayamail.com", ""},
		// Case 4: free-mail false positives stay blocked.
		{"services@gmail", "customerservices@gmail.com", Context{Exists: set("services@gmail.com"), ExistsLinked: set("services@gmail.com")}, "customerservices@gmail.com", ""},
		{"pilates@gmail", "clubpilates@gmail.com", Context{Exists: set("pilates@gmail.com"), ExistsLinked: set("pilates@gmail.com")}, "clubpilates@gmail.com", ""},
		{"srodriguez@gmail", "msrodriguez@gmail.com", Context{Exists: set("srodriguez@gmail.com"), ExistsLinked: set("srodriguez@gmail.com")}, "msrodriguez@gmail.com", ""},
		{"tudios@gmail", "studios@gmail.com", Context{Exists: set("tudios@gmail.com"), ExistsLinked: set("tudios@gmail.com")}, "studios@gmail.com", ""},
		{"gmail result too short", "rostockinfo@gmail.com", Context{ExistsLinked: set("info@gmail.com")}, "rostockinfo@gmail.com", ""},
		{"free-mail needs linked", "rostockinfos@yahoo.com", Context{Exists: set("info@yahoo.com")}, "rostockinfos@yahoo.com", ""},
		{"free-mail linked ok", "rostockinfo@yahoo.com", Context{ExistsLinked: set("info@yahoo.com")}, "info@yahoo.com", "prefix"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, rules, ok := RepairCtx(c.in, c.ctx)
			if got != c.want || strings.Join(rules, "+") != c.rules || !ok {
				t.Errorf("RepairCtx(%q) = (%q, %v, %v), want (%q, %q, true)", c.in, got, rules, ok, c.want, c.rules)
			}
			if ok {
				assertCRM(t, got)
			}
		})
	}
}

// TestRepairCtxRejects covers cuts with no evidence for the left part: the result
// is ok=false and the cut rule stays in rules.
func TestRepairCtxRejects(t *testing.T) {
	cases := []struct {
		name  string
		in    string
		ctx   Context
		want  string
		rules string
	}{
		{"gmail mid glue", "someone01@gmail.comfacebookfacebook.com", Context{}, "someone01@gmail.com", "middomain"},
		{"trailing glue yahoo.com.ph", "drstmh@yahoo.com.phcontact", Context{}, "drstmh@yahoo.com.ph", "tld"},
		{"trailing glue naver", "zebramats@naver.comhome", Context{}, "zebramats@naver.com", "tld"},
		{"trailing glue .de", "safir@bella-und-dent.dedetails", Context{}, "safir@bella-und-dent.de", "tld"},
		{"trailing glue .it", "postmaster@lezionidistoria.itcontatti", Context{}, "postmaster@lezionidistoria.it", "tld"},
		{"web.de repeat signal", "martins10@web.deweb.de", Context{}, "martins10@web.de", "middomain"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, rules, ok := RepairCtx(c.in, c.ctx)
			if got != c.want || strings.Join(rules, "+") != c.rules || ok {
				t.Errorf("RepairCtx(%q) = (%q, %v, %v), want (%q, %q, false)", c.in, got, rules, ok, c.want, c.rules)
			}
		})
	}
}
