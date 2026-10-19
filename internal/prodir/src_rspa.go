package prodir

// RSPA (Racquet Sports Professionals Association, formerly USPTA) Find a Pro,
// https://members.rspa.net/USPTA/New-FInd-A-Pro/Find_A_Pro (note the "FInd"
// capitalisation; ~11.7k professionals, individuals; default category tennis).
//
// The directory is an iMIS ASP.NET WebForms page. List: for every option of
// the State/Province select, GET the search page (the shared cookie jar keeps
// the session), POST its hidden fields (viewstate, tokens) plus that state and
// the Find button, and read the whole result grid, which comes back in one page
// without a pager (Florida alone is ~1.6k rows). A fresh GET per state keeps
// the viewstate valid; that is ~110 requests for the whole list. The grid
// carries name, member type, "business Loc: City, ST zip", and the tennis /
// pickleball / platform / padel levels. Detail: FindAProfile.aspx?ID={id}
// adds the email (mailto link), mobile phone, website (on part of the
// profiles), the RSPA level and the specialties / certifications answered Yes.
//
// The directory has no country field: it is derived from the State/Province
// option the record was found under (US states and territories, Canadian
// provinces, Australian states; the option value disambiguates NT and WA).
// There is no street address and no social links.

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const (
	rspaBase     = "https://members.rspa.net"
	rspaFind     = rspaBase + "/USPTA/New-FInd-A-Pro/Find_A_Pro"
	rspaProfile  = rspaBase + "/USPTA/New-FInd-A-Pro/FindAProfile.aspx?ID="
	rspaStateSel = `select[name$="Sheet0$Input3$DropDown1"]`
	rspaFindBtn  = `input[name$="Sheet0$SubmitButton"]`
)

var (
	rspaLocRe      = regexp.MustCompile(`^(.*?),\s*([A-Za-z]{2,3})\s*([\w -]*)$`)
	rspaHonorific  = regexp.MustCompile(`(?i)^(mr|mrs|ms|miss|mx|dr)\.?\s+`)
	rspaIDRe       = regexp.MustCompile(`(?i)[?&]ID=(\d+)`)
	rspaCanada     = map[string]bool{"AB": true, "BC": true, "MB": true, "NB": true, "NL": true, "NS": true, "NU": true, "ON": true, "PE": true, "QC": true, "SK": true, "YT": true, "NT:::52": true}
	rspaAustralia  = map[string]bool{"ACT": true, "NSW": true, "QLD": true, "SA": true, "TAS": true, "VIC": true, "NT:::51": true, "WA:::80": true}
	rspaLevelCols  = []string{"tennis_level", "pickleball_level", "platform_level", "padel_level"} // grid columns 4..7
	rspaDetailKeys = map[string]string{"rspa certification level": "level", "safe play status": "safe_play"}
)

func init() {
	register(&Source{
		Slug: "rspa", Name: "RSPA Find a Pro (tennis professionals)", Host: "members.rspa.net",
		Category: "sports_tennis", ListingType: "individual", MinInterval: time.Second,
		List: rspaList, Detail: rspaDetail,
	})
}

// rspaCountry is the country of a State/Province option value ("NT:::51").
func rspaCountry(opt string) string {
	switch {
	case rspaCanada[opt] || rspaCanada[strings.SplitN(opt, ":::", 2)[0]]:
		return "Canada"
	case rspaAustralia[opt] || rspaAustralia[strings.SplitN(opt, ":::", 2)[0]]:
		return "Australia"
	}
	return "United States"
}

func rspaList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	doc, _, err := getDoc(ctx, f, rspaFind)
	if err != nil {
		return err
	}
	var states []string
	doc.Find(rspaStateSel + " option").Each(func(_ int, o *goquery.Selection) {
		if v := strings.TrimSpace(o.AttrOr("value", "")); v != "" {
			states = append(states, v)
		}
	})
	if len(states) == 0 {
		return fmt.Errorf("no state options on %s (layout changed?)", rspaFind)
	}
	seen := map[string]bool{}
	for i, st := range states {
		if i > 0 { // the first state reuses the page fetched above
			if doc, _, err = getDoc(ctx, f, rspaFind); err != nil {
				return err
			}
		}
		res, err := rspaSearch(ctx, f, doc, st)
		if err != nil {
			return err
		}
		for _, r := range rspaRows(res, rspaCountry(st)) {
			if seen[r.ID] {
				continue
			}
			seen[r.ID] = true
			if err := emit(r); err != nil {
				return err
			}
		}
	}
	return nil
}

// rspaSearch posts the search form of doc for one State/Province option.
func rspaSearch(ctx context.Context, f Fetcher, doc *goquery.Document, state string) (*goquery.Document, error) {
	form := url.Values{}
	doc.Find(`form#aspnetForm input[type="hidden"][name]`).Each(func(_ int, in *goquery.Selection) {
		form.Set(in.AttrOr("name", ""), in.AttrOr("value", ""))
	})
	stateName, btn := doc.Find(rspaStateSel).AttrOr("name", ""), doc.Find(rspaFindBtn)
	if form.Get("__VIEWSTATE") == "" || stateName == "" || btn.Length() == 0 {
		return nil, fmt.Errorf("search form not found on %s (layout changed?)", rspaFind)
	}
	form.Set(stateName, state)
	form.Set(btn.AttrOr("name", ""), btn.AttrOr("value", "Find"))
	p, err := f.Post(ctx, rspaFind+"?", map[string]string{"Content-Type": "application/x-www-form-urlencoded"}, []byte(form.Encode()))
	if err != nil {
		return nil, err
	}
	if err := p.Err(); err != nil {
		return nil, err
	}
	return docFromBody(p.Body)
}

// rspaRows parses the result grid; country is the one of the searched state.
func rspaRows(doc *goquery.Document, country string) []Record {
	var out []Record
	doc.Find("tr.rgRow, tr.rgAltRow").Each(func(_ int, tr *goquery.Selection) {
		tds := tr.Find("td")
		a := tds.First().Find("a[href]")
		m := rspaIDRe.FindStringSubmatch(a.AttrOr("href", ""))
		if tds.Length() < 8 || m == nil {
			return
		}
		cell := func(i int) string { return clean(tds.Eq(i).Text()) }
		name, suffix, _ := strings.Cut(rspaHonorific.ReplaceAllString(clean(a.Text()), ""), ",")
		r := Record{ID: m[1], URL: rspaProfile + m[1], Name: clean(name), Country: country, Meta: map[string][]string{}}
		if r.Name == "" { // no usable name: not an entry
			return
		}
		addMeta(r.Meta, "name_suffix", suffix)
		addMeta(r.Meta, "member_type", cell(1))
		business, loc, _ := strings.Cut(cell(2), "Loc:")
		r.Business = clean(business)
		loc = clean(loc)
		if lm := rspaLocRe.FindStringSubmatch(loc); lm != nil {
			r.City, r.Region, r.PostalCode = clean(lm[1]), strings.ToUpper(lm[2]), clean(lm[3])
		} else {
			r.City = loc
		}
		for i, key := range rspaLevelCols {
			if v := cell(4 + i); !strings.EqualFold(v, "none") {
				addMeta(r.Meta, key, v)
			}
		}
		out = append(out, r)
	})
	return out
}

func rspaDetail(ctx context.Context, f Fetcher, r Record) (Record, error) {
	doc, base, err := getDoc(ctx, f, r.URL)
	if err != nil {
		return Record{}, err
	}
	if doc.Find("div.PanelField").Length() == 0 {
		return Record{}, fmt.Errorf("no profile fields on %s (layout changed?)", base)
	}
	d := Record{Meta: map[string][]string{}}
	doc.Find("div.PanelField").Each(func(_ int, pf *goquery.Selection) {
		label := strings.ToLower(clean(pf.Find("span.Label").First().Text()))
		val := pf.Find("div.PanelFieldValue").First()
		text := clean(val.Text())
		if text == "" {
			return
		}
		switch label {
		case "email":
			d.Emails = emailsIn(val)
		case "mobile phone":
			d.Phones = []string{text}
		case "website":
			if w := firstNonEmpty(absURL(base, val.Find("a").AttrOr("href", "")), text); strings.Contains(w, ".") {
				d.Websites = []string{w}
			}
		case "about":
			d.Bio = text
		default:
			if key := rspaDetailKeys[label]; key != "" {
				addMeta(d.Meta, key, text)
			} else if strings.EqualFold(text, "yes") { // a checked specialty or extra certification
				kind := "certifications"
				if strings.Contains(val.Find("span[id]").First().AttrOr("id", ""), "ciSpecialties") {
					kind = "specialties"
				}
				addMeta(d.Meta, kind, clean(pf.Find("span.Label").First().Text()))
			}
		}
	})
	return d, nil
}
