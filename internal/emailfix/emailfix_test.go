package emailfix

import "testing"

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
		{"9346082info@gmail.com", "info@gmail.com", true, true},        // role word beats free-mail
		{"+6281234567890john@gmail.com", "john@gmail.com", true, true}, // '+' marks a phone
		{"9346082john@studio.com", "john@studio.com", true, true},      // non-free domain
		// R2: website text glued after the TLD.
		{"info@inivie.comview", "info@inivie.com", true, true},
		{"hi@balifitness.asiaphone", "hi@balifitness.asia", true, true},
		{"yo@ttbeach.clubmenu", "yo@ttbeach.club", true, true},
		{"zo@studio.co.ukhome", "zo@studio.co.uk", true, true},
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
		{"info@studio", "info@studio", false, false},
		{"info@studio.zzzzqq", "info@studio.zzzzqq", false, false},
		{"a@b.com", "a@b.com", false, false}, // local part too short
		{"in fo@x.com", "in fo@x.com", false, false},
		{"+1-212-555-7575@x.com", "+1-212-555-7575@x.com", false, true}, // no letter after the run: left alone
	}
	for _, c := range cases {
		got, changed, ok := Repair(c.in)
		if got != c.want || changed != c.changed || ok != c.ok {
			t.Errorf("Repair(%q) = (%q, %v, %v), want (%q, %v, %v)", c.in, got, changed, ok, c.want, c.changed, c.ok)
		}
	}
}
