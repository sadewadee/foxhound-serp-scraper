package prodir

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestCorePowerList(t *testing.T) {
	s := mustSource(t, "corepower")
	f := &fakeFetcher{pages: map[string]Page{
		corepowerSitemap:                 {Body: readFixture(t, "corepower_sitemap_SYNTHETIC.xml")},
		corepowerBase + "/sitemap-0.xml": {Body: readFixture(t, "corepower_sitemap0_SYNTHETIC.xml")},
	}}
	got := collect(t, s, f)
	// only /yoga-studios/<state>/<market>/<studio>, once each
	want := []Record{
		{ID: "zz-fixtureland-main-street", URL: corepowerBase + "/yoga-studios/zz/fixtureland/main-street", Name: "CorePower Yoga ZZ main-street"},
		{ID: "zz-fixtureland-second-avenue", URL: corepowerBase + "/yoga-studios/zz/fixtureland/second-avenue", Name: "CorePower Yoga ZZ second-avenue"},
		{ID: "bc-sampleport-harbour", URL: corepowerBase + "/yoga-studios/bc/sampleport/harbour", Name: "CorePower Yoga BC harbour"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	if len(f.calls) != 2 {
		t.Errorf("requests = %v", f.calls)
	}
}

func TestCorePowerDetail(t *testing.T) {
	s := mustSource(t, "corepower")
	us, ca, closed := corepowerBase+"/yoga-studios/zz/fixtureland/main-street", corepowerBase+"/yoga-studios/bc/sampleport/harbour", corepowerBase+"/yoga-studios/zz/fixtureland/second-avenue"
	f := &fakeFetcher{pages: map[string]Page{
		us:     {Body: readFixture(t, "corepower_studio_SYNTHETIC.html")},
		ca:     {Body: readFixture(t, "corepower_studio_canada_SYNTHETIC.html")},
		closed: {Body: readFixture(t, "corepower_studio_closed_SYNTHETIC.html")},
	}}
	ctx := context.Background()

	d, err := s.Detail(ctx, f, Record{ID: "zz-fixtureland-main-street", URL: us})
	if err != nil {
		t.Fatal(err)
	}
	want := Record{Name: "CorePower Yoga Main Street", Address: "1 Fixture Blvd, Suite 5", City: "Fixtureland", Region: "ZZ", PostalCode: "00000",
		Country: "United States", Phones: []string{"555-010-0123"}, Emails: []string{"zz-mainstreet@example.com"},
		Meta: map[string][]string{"studio_type": {"Corporate"}}}
	if !reflect.DeepEqual(d, want) { // no manager email, no chain social, no website
		t.Errorf("detail = %#v\nwant     %#v", d, want)
	}

	if d, err = s.Detail(ctx, f, Record{ID: "bc-sampleport-harbour", URL: ca}); err != nil || d.Country != "Canada" || d.Region != "BC" {
		t.Errorf("canada: %#v %v", d, err)
	}
	// closed and not reopened: stored as missing
	if _, err = s.Detail(ctx, f, Record{ID: "x", URL: closed}); !errors.Is(err, ErrNotFound) {
		t.Errorf("closed: err = %v", err)
	}
	// a gone page and a page without __NEXT_DATA__
	if _, err = s.Detail(ctx, f, Record{ID: "y", URL: corepowerBase + "/yoga-studios/a/b/c"}); !errors.Is(err, ErrNotFound) {
		t.Errorf("404: err = %v", err)
	}
	f.pages[us] = Page{Body: []byte("<html></html>")}
	if _, err = s.Detail(ctx, f, Record{ID: "z", URL: us}); err == nil || errors.Is(err, ErrNotFound) {
		t.Errorf("no next data: err = %v", err)
	}
}
