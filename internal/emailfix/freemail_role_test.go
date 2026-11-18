package emailfix

import (
	"slices"
	"testing"
)

func setCtx(exists, linked string) Context {
	c := Context{}
	if exists != "" {
		c.Exists = func(e string) bool { return e == exists }
	}
	if linked != "" {
		c.ExistsLinked = func(e string) bool { return e == linked }
	}
	return c
}

func TestRepairCtxB5(t *testing.T) {
	cases := []struct {
		name, in string
		ctx      Context
		want     string
		ok       bool
		digitrol bool // "digitrole" rule expected
	}{
		// digitrole on free-mail needs linked evidence for a bare role word.
		{"freemail bare exists-only", "7781office@gmail.com", setCtx("office@gmail.com", ""), "7781office@gmail.com", true, false},
		{"freemail bare linked", "7781office@gmail.com", setCtx("office@gmail.com", "office@gmail.com"), "office@gmail.com", true, true},
		{"freemail non-bare exists", "60123info.studioname@gmail.com", setCtx("info.studioname@gmail.com", ""), "info.studioname@gmail.com", true, true},
		{"business bare", "240info@studio-a.fixture.com", Context{}, "info@studio-a.fixture.com", true, true},
		// Brand guard.
		{"brand mail.bg", "55mail@mail.bg", setCtx("mail@mail.bg", ""), "55mail@mail.bg", true, false},
		{"brand mail.bg 2", "123321mail@mail.bg", setCtx("mail@mail.bg", ""), "123321mail@mail.bg", true, false},
		// Provider and localized placeholders.
		{"mail.com", "mail@mail.com", Context{}, "", false, false},
		{"mail.fr", "mail@mail.fr", Context{}, "", false, false},
		{"me.com", "me@me.com", Context{}, "", false, false},
		{"yahoo.co.in", "yahoo@yahoo.co.in", Context{}, "", false, false},
		{"web.de", "web@web.de", Context{}, "", false, false},
		{"votreemail", "votreemail@mail.com", Context{}, "", false, false},
		{"ihre.email", "ihre.email@beispiel.de", Context{}, "", false, false},
		{"tucorreo", "tucorreo@hotmail.com", Context{}, "", false, false},
		{"seuemail", "seuemail@gmail.com", Context{}, "", false, false},
		{"subdomain host kept", "mail@mail.studio-a.com", Context{}, "mail@mail.studio-a.com", true, false},
		// JSON unicode-escape prefix.
		{"u0022", "u0022info.studioname@gmail.com", Context{}, "info.studioname@gmail.com", true, false},
		{"u0027", "u0027sales@studio-a.fixture.com", Context{}, "sales@studio-a.fixture.com", true, false},
		{"u0022 empty", "u0022@hotmail.com", Context{}, "", false, false},
		{"u0022 mailto", "u0022mailto@gmail.com", Context{}, "", false, false},
		// adres label.
		{"adres nl", "04e-mailadresbevolkingsdienst@gemeente-a.be", Context{}, "bevolkingsdienst@gemeente-a.be", true, false},
		{"adresse de", "121e-mail-adresseanna.berger@firm-a.at", Context{}, "anna.berger@firm-a.at", true, false},
		{"adres postal code", "00e-mailadres2400@gemeente-a.be", Context{}, "", false, false},
		{"adres glue label", "e-mailadresinfo@gemeente-a.be", Context{}, "info@gemeente-a.be", true, false},
		// Web glue.
		{"webseite host", "info@studio-a.dewebseitestudio-b.de", Context{}, "info@studio-a.de", true, false},
		{"internet www", "museum@museum-a.deinternetwww.museum-a.de", Context{}, "museum@museum-a.de", false, false}, // fixed, but "museum" is no role word: no cut evidence,
		{"website host", "hello@studio-a.comwebsiteother-b.com", Context{}, "hello@studio-a.com", true, false},
		{"homepage host", "info@studio-a.dehomepagestudio-b.de", Context{}, "info@studio-a.de", true, false},
		{"short web not cut", "info@studio-a.dewebstudio-b.de", Context{}, "info@studio-a.dewebstudio-b.de", true, false},
		{"empty first label", "info@studio-a.comweb.de", Context{}, "info@studio-a.comweb.de", true, false},
		{"tmweb.ru", "xy@tmweb.ru", Context{}, "xy@tmweb.ru", true, false},
		{"infoweb.ne.jp", "xy@infoweb.ne.jp", Context{}, "xy@infoweb.ne.jp", true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, rules, ok := RepairCtx(tc.in, tc.ctx)
			if ok != tc.ok || (tc.want != "" && got != tc.want) {
				t.Fatalf("RepairCtx(%q) = %q ok=%v rules=%v, want %q ok=%v", tc.in, got, ok, rules, tc.want, tc.ok)
			}
			if slices.Contains(rules, "digitrole") != tc.digitrol {
				t.Errorf("RepairCtx(%q) rules=%v, digitrole want %v", tc.in, rules, tc.digitrol)
			}
		})
	}
}

func TestCleanListB5(t *testing.T) {
	got := CleanList([]string{"mail@mail.com", "u0022info.studioname@gmail.com"})
	if len(got) != 1 || got[0] != "info.studioname@gmail.com" {
		t.Fatalf("CleanList = %v, want [info.studioname@gmail.com]", got)
	}
}
