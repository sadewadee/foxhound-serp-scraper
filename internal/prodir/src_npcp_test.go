package prodir

import (
	"context"
	"net/url"
	"reflect"
	"strings"
	"testing"
)

// npcpFake serves the grid: GET gives the initial page, every POST the next
// canned response, and the posted forms are kept for the assertions.
type npcpFake struct {
	initial []byte
	posts   [][]byte
	forms   []url.Values
}

func (f *npcpFake) Get(_ context.Context, u string, _ map[string]string) (Page, error) {
	if u != npcpListURL {
		return Page{Status: 404, URL: u}, nil
	}
	return Page{Status: 200, URL: u, Body: f.initial}, nil
}

func (f *npcpFake) Post(_ context.Context, u string, _ map[string]string, body []byte) (Page, error) {
	v, _ := url.ParseQuery(string(body))
	f.forms = append(f.forms, v)
	if len(f.forms) > len(f.posts) {
		return Page{Status: 500, URL: u}, nil
	}
	return Page{Status: 200, URL: u, Body: f.posts[len(f.forms)-1]}, nil
}

func TestNPCPList(t *testing.T) {
	s := mustSource(t, "npcp")
	f := &npcpFake{
		initial: readFixture(t, "npcp_list_initial_SYNTHETIC.html"),
		posts:   [][]byte{readFixture(t, "npcp_list_p1_SYNTHETIC.html"), readFixture(t, "npcp_list_p2_SYNTHETIC.html")},
	}
	got := collect(t, s, f)
	want := []Record{
		{ID: "90001", URL: npcpDetail + "90001", Name: "Ada Fixture", City: "Samplevale", Region: "CA", PostalCode: "90000", Country: "United States",
			Meta: map[string][]string{"certification": {"NCPT"}, "certification_id": {"N90001"}, "certified_since": {"06/26/2026"}, "expires": {"06/26/2028"}}},
		{ID: "90002", URL: npcpDetail + "90002", Name: "Bo Placeholder", City: "Mockton", PostalCode: "99040", Country: "Cyprus",
			Meta: map[string][]string{"certification": {"NCPT"}, "certification_id": {"10414"}, "certified_since": {"08/09/2006"}, "expires": {"08/08/2028"}}},
		{ID: "90003", URL: npcpDetail + "90003", Name: "Cy Sample", City: "Fakeham", Region: "TX", PostalCode: "75001-1234", Country: "United States",
			Meta: map[string][]string{"certification": {"PMC"}, "certification_id": {"N90003"}, "certified_since": {"01/02/2020"}, "expires": {"01/01/2030"}}},
	}
	if !reflect.DeepEqual(got, want) { // the repeated Ada row on page 2 and the link-less row are dropped
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	if len(f.forms) != 2 {
		t.Fatalf("posts = %d, want 2 (page size, page 2)", len(f.forms))
	}
	// 1st POST: the page-size command with the ViewState of the initial page
	if v := f.forms[0]; v.Get("__EVENTTARGET") != npcpGrid || v.Get("__EVENTARGUMENT") != "FireCommand:"+npcpGrid+"$ctl00;PageSize;50" ||
		v.Get("__VIEWSTATE") != "VS-INITIAL" || v.Get("PageInstanceKey") == "" {
		t.Errorf("page-size form = %v", v)
	}
	// 2nd POST: the "Go to Page 2" link of the LAST response and its fresh ViewState
	if v := f.forms[1]; v.Get("__EVENTTARGET") != npcpGrid+"$ctl00$ctl03$ctl01$ctl07" || v.Get("__EVENTARGUMENT") != "" || v.Get("__VIEWSTATE") != "VS-PAGE1" {
		t.Errorf("page-2 form = %v", v)
	}

	// the limit stops the walk before any paging
	f = &npcpFake{initial: f.initial, posts: f.posts}
	if err := s.List(context.Background(), f, func(Record) error { return errListLimit }); err == nil || len(f.forms) != 1 {
		t.Errorf("limit: err=%v posts=%d", err, len(f.forms))
	}
	// a grid that does not move to the asked page is an error, not a loop
	f = &npcpFake{initial: f.initial, posts: [][]byte{f.posts[0], f.posts[0]}}
	if err := s.List(context.Background(), f, func(Record) error { return nil }); err == nil {
		t.Error("a repeated page must fail the walk")
	}
	// no ViewState: layout changed
	if err := s.List(context.Background(), &npcpFake{initial: []byte("<html></html>")}, func(Record) error { return nil }); err == nil {
		t.Error("a page without ViewState must fail")
	}
}

func TestNPCPNextButton(t *testing.T) {
	// when the numbered link is not on the page, the "next page" button is posted
	s := mustSource(t, "npcp")
	p1 := string(readFixture(t, "npcp_list_p1_SYNTHETIC.html"))
	p1 = strings.ReplaceAll(p1, `title="Go to Page 2"`, `title="x"`)
	p1 = strings.ReplaceAll(p1, `</div>
<div class="rgWrap rgInfoPart">`, `<input type="submit" name="NEXTBTN" value=" " title="Next Page" class="rgPageNext" /></div>
<div class="rgWrap rgInfoPart">`)
	f := &npcpFake{initial: readFixture(t, "npcp_list_initial_SYNTHETIC.html"), posts: [][]byte{[]byte(p1), readFixture(t, "npcp_list_p2_SYNTHETIC.html")}}
	if got := collect(t, s, f); len(got) != 3 {
		t.Fatalf("records = %d", len(got))
	}
	if v := f.forms[1]; v.Get("NEXTBTN") != " " || v.Get("__EVENTTARGET") != "" {
		t.Errorf("next-button form = %v", v)
	}
}

func TestNPCPDetail(t *testing.T) {
	s := mustSource(t, "npcp")
	r := Record{ID: "90001", URL: npcpDetail + "90001", Name: "Ada Fixture"}
	f := &fakeFetcher{pages: map[string]Page{r.URL: {Body: readFixture(t, "npcp_detail_full_SYNTHETIC.html")}}}
	d, err := s.Detail(context.Background(), f, r)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(d.Emails, []string{"ada.fixture@example.com"}) || !reflect.DeepEqual(d.Phones, []string{"555-010-0101"}) ||
		!reflect.DeepEqual(d.Websites, []string{"www.ada-fixture.example.org"}) || d.Business != "Fixture Studio" || d.Bio != "Teaches mat and reformer." {
		t.Errorf("detail = %#v", d)
	}
	if !reflect.DeepEqual(d.Meta, map[string][]string{"modalities_taught": {"Mat, Reformer"}, "pmc_certification_id": {"P-555"}}) {
		t.Errorf("meta = %#v", d.Meta)
	}

	// a teacher who shares nothing: no contact fields, no error
	r2 := Record{ID: "90002", URL: npcpDetail + "90002"}
	f = &fakeFetcher{pages: map[string]Page{r2.URL: {Body: readFixture(t, "npcp_detail_empty_SYNTHETIC.html")}}}
	d, err = s.Detail(context.Background(), f, r2)
	if err != nil || len(d.Emails)+len(d.Phones)+len(d.Websites) != 0 || d.Bio != "" {
		t.Errorf("empty detail = %#v, %v", d, err)
	}
	// a page without profile fields is a layout error
	f = &fakeFetcher{pages: map[string]Page{r2.URL: {Body: []byte("<html></html>")}}}
	if _, err := s.Detail(context.Background(), f, r2); err == nil {
		t.Error("no profile fields must fail")
	}
}
