package prodir

import (
	"reflect"
	"testing"
)

func TestPrecisionNutritionList(t *testing.T) {
	s := mustSource(t, "precisionnutrition")
	q := func(c, pg string) string { return pnURL + "?tab=country&country=" + c + "&pg=" + pg }
	f := &fakeFetcher{pages: map[string]Page{
		pnURL:               {Body: readFixture(t, "precisionnutrition_base_SYNTHETIC.html")},
		q("Freedonia", "1"): {Body: readFixture(t, "precisionnutrition_freedonia1_SYNTHETIC.html")},
		q("Freedonia", "2"): {Body: readFixture(t, "precisionnutrition_freedonia2_SYNTHETIC.html")},
		q("Sylvania", "1"):  {Body: readFixture(t, "precisionnutrition_sylvania1_SYNTHETIC.html")},
	}}
	got := collect(t, s, f)
	if len(got) != 4 { // 3 Freedonia coaches (one repeated on page 2) + 1 Sylvania; the nameless card is skipped
		t.Fatalf("records = %d: %#v", len(got), got)
	}
	a := got[0]
	if a.Name != "Alex Fixture, MD" || a.Business != "Fixture Fitness" || a.Country != "Freedonia" ||
		a.Address != "1 Example Road, Exampleton, 1000, Freedonia" ||
		!reflect.DeepEqual(a.Emails, []string{"alex@example.com"}) ||
		!reflect.DeepEqual(a.Phones, []string{"+1 555 010 0101"}) ||
		!reflect.DeepEqual(a.Websites, []string{"http://example.org/alex"}) ||
		!reflect.DeepEqual(a.Meta, map[string][]string{"level": {"Level 1 NC"}, "specialties": {"CES, PT"}}) {
		t.Errorf("first record = %#v", a)
	}
	b := got[1]
	if b.Name != "Blair Placeholder" || len(b.Emails) != 0 || !reflect.DeepEqual(b.Phones, []string{"555-010-0102"}) || len(b.Websites) != 0 {
		t.Errorf("second record = %#v", b)
	}
	if got[2].Name != "Cameron Sample" || !reflect.DeepEqual(got[2].Emails, []string{"cameron@example.org"}) || len(got[2].Phones) != 0 {
		t.Errorf("third record = %#v", got[2])
	}
	if got[3].Country != "Sylvania" || got[3].Name != "Dana Demo" {
		t.Errorf("fourth record = %#v", got[3])
	}
	ids := map[string]bool{}
	for _, r := range got {
		ids[r.ID] = true
	}
	if len(ids) != 4 {
		t.Errorf("ids not unique: %v", ids)
	}
	// base + Freedonia p1, p2 + Sylvania p1 ("N results" = one page)
	if len(f.calls) != 4 {
		t.Errorf("requests = %v", f.calls)
	}
}
