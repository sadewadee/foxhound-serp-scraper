package prodir

// LPGA find a teacher, https://professionals.lpga.com/find-a-teacher/find
// (~1,220 golf teachers, individuals).
//
// A Sitefinity / Telerik RadGrid ASP.NET form. List GETs the page, POSTs the
// form with the Search button pressed (an empty search returns every teacher)
// and then walks the grid with its "next page" submit button, 5 teachers per
// page (~245 sequential ~120 KB requests): every response's hidden fields
// (__VIEWSTATE, __EVENTVALIDATION, ...) must be echoed in the next POST, so
// pages cannot be parallelised or skipped (the shared cookie jar keeps the
// session). The card already carries everything the directory has: name,
// company (Business), email (mailto), phone (often empty), address lines,
// member type and LPGA section; there is no profile page, website or social.
// The directory exposes no id: it is the hash of the email, else of
// name|company|postal code. Country is read from the card's last address line.

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const (
	lpgaURL       = "https://professionals.lpga.com/find-a-teacher/find"
	lpgaSearchBtn = "ctl00$MainContentAreaPlaceHolder$C001$btnSearch"
)

func init() {
	register(&Source{
		Slug: "lpga", Name: "LPGA find a teacher", Host: "lpga.com",
		Category: "sports_golf", ListingType: "individual", MinInterval: time.Second,
		List: lpgaList,
	})
}

// lpgaForm is the data a postback of the page's form sends: every named
// input, select and textarea except buttons (the pressed one is added by the
// caller).
func lpgaForm(doc *goquery.Document) (url.Values, error) {
	form := doc.Find("input[name=__VIEWSTATE]").First().Closest("form")
	if form.Length() == 0 {
		return nil, fmt.Errorf("no ASP.NET form on the page (layout changed?)")
	}
	v := url.Values{}
	form.Find("input[name], textarea[name]").Each(func(_ int, in *goquery.Selection) {
		name := in.AttrOr("name", "")
		switch strings.ToLower(in.AttrOr("type", "text")) {
		case "submit", "button", "image", "reset", "file":
		case "checkbox", "radio":
			if _, on := in.Attr("checked"); on {
				v.Add(name, in.AttrOr("value", "on"))
			}
		default:
			if in.Is("textarea") {
				v.Add(name, in.Text())
			} else {
				v.Add(name, in.AttrOr("value", ""))
			}
		}
	})
	form.Find("select[name]").Each(func(_ int, sel *goquery.Selection) {
		opt := sel.Find("option[selected]").First()
		if opt.Length() == 0 {
			opt = sel.Find("option").First()
		}
		v.Add(sel.AttrOr("name", ""), opt.AttrOr("value", opt.Text()))
	})
	return v, nil
}

func lpgaPost(ctx context.Context, f Fetcher, form url.Values) (*goquery.Document, error) {
	hdr := map[string]string{"Content-Type": "application/x-www-form-urlencoded", "Referer": lpgaURL}
	p, err := f.Post(ctx, lpgaURL, hdr, []byte(form.Encode()))
	if err != nil {
		return nil, err
	}
	if err := p.Err(); err != nil {
		return nil, err
	}
	return docFromBody(p.Body)
}

func lpgaList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	doc, _, err := getDoc(ctx, f, lpgaURL)
	if err != nil {
		return err
	}
	form, err := lpgaForm(doc)
	if err != nil {
		return err
	}
	form.Set(lpgaSearchBtn, firstNonEmpty(doc.Find(`input[name="`+lpgaSearchBtn+`"]`).AttrOr("value", ""), "Search"))
	seen := map[string]bool{}
	prev := ""
	for page := 1; page <= 1000; page++ {
		if doc, err = lpgaPost(ctx, f, form); err != nil {
			return err
		}
		rows := doc.Find("tr.rgRow, tr.rgAltRow")
		var recs []Record
		rows.Each(func(_ int, tr *goquery.Selection) {
			if r, ok := lpgaCard(tr); ok && !seen[r.ID] {
				seen[r.ID] = true
				recs = append(recs, r)
			}
		})
		sig := clean(rows.First().Text())
		if rows.Length() == 0 || sig == prev { // empty or stuck on the same page
			return nil
		}
		prev = sig
		for _, r := range recs {
			if err := emit(r); err != nil {
				return err
			}
		}
		next := doc.Find("input.rgPageNext").First()
		if next.Length() == 0 || strings.Contains(next.AttrOr("onclick", ""), "return false") {
			return nil // the last page has no usable next button
		}
		if form, err = lpgaForm(doc); err != nil {
			return err
		}
		form.Set(next.AttrOr("name", ""), next.AttrOr("value", " "))
	}
	return nil
}

// lpgaCard parses one result row; ok is false for a row without a name.
func lpgaCard(tr *goquery.Selection) (Record, bool) {
	name := clean(tr.Find("#PersonName").First().Text())
	if name == "" { // not a real entry
		return Record{}, false
	}
	r := Record{
		Name: name, Business: clean(tr.Find("#companyName").First().Text()),
		Emails: emailsIn(tr), Meta: map[string][]string{},
	}
	if p := structuredPhone(tr.Find("#phone").First().Text()); p != "" {
		r.Phones = []string{p}
	}
	addMeta(r.Meta, "member_type", lpgaLabelled(tr, "#membertype"))
	addMeta(r.Meta, "lpga_section", lpgaLabelled(tr, "#section"))

	// the address spans have no id: street line(s), "City, ST ZIP", country
	var lines []string
	tr.Find("div > span:not([id])").Each(func(_ int, sp *goquery.Selection) {
		if sp.Closest(".distrem").Length() == 0 {
			if l := clean(sp.Text()); l != "" {
				lines = append(lines, l)
			}
		}
	})
	if len(lines) >= 2 {
		r.Country = lines[len(lines)-1]
		lines = lines[:len(lines)-1]
	}
	if len(lines) >= 1 {
		city, rest, _ := strings.Cut(lines[len(lines)-1], ",")
		if rest == "" { // no comma: not a "City, ST ZIP" line, keep it as a street line
			r.Address = strings.Join(lines, ", ")
		} else {
			r.City = clean(city)
			f := strings.Fields(rest)
			if len(f) > 0 {
				r.Region = f[0]
				r.PostalCode = strings.Join(f[1:], " ")
			}
			r.Address = strings.Join(lines[:len(lines)-1], ", ")
		}
	}
	key := strings.ToLower(strings.Join([]string{name, r.Business, r.PostalCode}, "|"))
	if len(r.Emails) > 0 {
		key = r.Emails[0] // a teacher listed at two facilities is one entry
	}
	sum := sha1.Sum([]byte(key))
	r.ID = hex.EncodeToString(sum[:])[:16]
	return r, true
}

// lpgaLabelled is the value of a "Label: value" span without its label.
func lpgaLabelled(tr *goquery.Selection, sel string) string {
	_, v, _ := strings.Cut(clean(tr.Find(sel).First().Text()), ":")
	return clean(v)
}
