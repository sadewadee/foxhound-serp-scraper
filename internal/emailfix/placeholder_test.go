package emailfix

import (
	"slices"
	"strings"
	"testing"
)

func TestRepairRejectsJunk(t *testing.T) {
	long31 := strings.Repeat("a", 31)
	for _, in := range []string{
		"abcde@gmail.com",
		"a.b.c.d.e@gmail.com",
		"ab+newsletter@gmail.com",
		long31 + "@gmail.com",
		long31 + "@googlemail.com",
		"info@example.com",
		"your@email.example.org",
		"jane@foo.test",
		"test@hotmail.co.uk",
		"email@mail.com",
		"you@live.com",
		"e-mail@yahoo.com",
		"abc@gmail.con",
	} {
		if fixed, _, ok := Repair(in); ok {
			t.Errorf("Repair(%q) = %q ok=true, want rejected", in, fixed)
		}
	}
}

func TestRepairKeepsReal(t *testing.T) {
	for _, in := range []string{
		"jane.doe@gmail.com",
		"jane.doe+news@gmail.com",
		"abcdefghijklmnopqrstuvwxyz1234+promo@gmail.com", // 30 before the +
		"0612345678s@gmail.com",
		"xxx_janedoe@hotmail.com",
		"mail@domain.de",
		"email@personal.it",
		"test@studio.de",
		"jane.doe@outlook.sa",
		"jane.doe@yahoo.co.uk",
	} {
		fixed, changed, ok := Repair(in)
		if !ok || changed || fixed != in {
			t.Errorf("Repair(%q) = %q changed=%v ok=%v, want unchanged ok", in, fixed, changed, ok)
		}
	}
}

func TestRepairTypoTLD(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"jane.doe@gmail.con", "jane.doe@gmail.com"},
		{"jane.doe@yahoo.om", "jane.doe@yahoo.com"},
		{"jane.doe@hotmail.cpm", "jane.doe@hotmail.com"},
		{"jane.doe@aol.comm", "jane.doe@aol.com"},
	} {
		fixed, rules, ok := RepairCtx(c.in, Context{})
		if !ok || fixed != c.want || !slices.Contains(rules, "typotld") {
			t.Errorf("RepairCtx(%q) = %q %v ok=%v, want %q with typotld", c.in, fixed, rules, ok, c.want)
		}
	}
	for _, in := range []string{"jane.doe@mygmail.con", "jane.doe@gmail.org"} {
		fixed, rules, _ := RepairCtx(in, Context{})
		if strings.HasSuffix(fixed, ".com") || slices.Contains(rules, "typotld") {
			t.Errorf("RepairCtx(%q) = %q %v, must not be typo-repaired to .com", in, fixed, rules)
		}
	}
}

func TestRejectionLeavesSyntaxRule(t *testing.T) {
	// repair-emails only marks a row malformed when a rule fired.
	for _, in := range []string{"abcde@gmail.com", "info@example.com", "test@hotmail.co.uk", "abc@gmail.con"} {
		if _, rules, ok := RepairCtx(in, Context{}); ok || !slices.Contains(rules, "syntax") {
			t.Errorf("RepairCtx(%q) rules=%v ok=%v, want rejected with syntax", in, rules, ok)
		}
	}
}

func TestCleanListAppliesRepairsAndRejects(t *testing.T) {
	got := CleanList([]string{"abcde@gmail.com", "jane.doe@gmail.con", "info@example.com", "jane.doe@studio.de"})
	want := []string{"jane.doe@gmail.com", "jane.doe@studio.de"}
	if !slices.Equal(got, want) {
		t.Errorf("CleanList = %v, want %v", got, want)
	}
}
