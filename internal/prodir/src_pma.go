package prodir

// Pilates Method Alliance teacher member directory (i4a),
// https://portal.pilatesmethodalliance.org/i4a/memberDirectory/ (~1,000 teachers,
// individuals; US ~770, CA ~30, the rest a handful each).
//
// The search is a plain GET (no cookie) but only answers when the hidden member
// criteria are sent (isDirectory_hidden_45=1, MEMBERTYPE_hidden_50='TCH', quotes
// included) together with at least one real criterion. The walk: read the
// country <select> of the search form (US first), then per country page with
// start=1,21,41... (20 per page, "Results 21-40 of 768 matching records" gives
// the total); finally sweep the last-name prefixes a-z to catch members whose
// country is blank (deduped by id). A list row (li.searchResultsListGroupItem)
// carries name, optional company, "City ST ZIP", the ISO-2 country and the
// email (mailto, plain). The detail page adds the street address, phone
// (tel:), the optional website and the "About Me" text.
//
// A member's ID is the 16-hex detail_lookup_id. Country is the ISO-2 code of the
// row; for a row that shows none, the country of the search it was found by.

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const (
	pmaHost = "portal.pilatesmethodalliance.org"
	pmaBase = "https://" + pmaHost + "/i4a/memberDirectory/index.cfm"
)

var (
	pmaTotalRe  = regexp.MustCompile(`Results\s+(\d+)\s*-\s*(\d+)\s+of\s+(\d+)`)
	pmaLookupRe = regexp.MustCompile(`detail_lookup_id=([0-9A-Za-z]+)`)
	pmaUSCityRe = regexp.MustCompile(`^(.*?),?\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$`)
	pmaCodeRe   = regexp.MustCompile(`^[A-Z]{2}$`)
)

func init() {
	register(&Source{
		Slug: "pma", Name: "Pilates Method Alliance member directory", Host: pmaHost,
		Category: "pilates_instructor", ListingType: "individual", MinInterval: time.Second,
		List: pmaList, Detail: pmaDetail,
	})
}

func pmaSearchURL(criterion, value string, start int) string {
	v := url.Values{
		"controller": {"memberDirectory"}, "action": {"resultsListing"}, "directory_id": {"5"},
		"pageID": {"1"}, "submitFlag": {"1"}, "start": {strconv.Itoa(start)},
		"srch_isDirectory_hidden_45": {"1"}, "srch_MEMBERTYPE_hidden_50": {"'TCH'"},
		criterion: {value},
	}
	return pmaBase + "?" + v.Encode()
}

func pmaList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	doc, base, err := getDoc(ctx, f, pmaBase+"?controller=memberDirectory&action=SearchForm&directory_id=5")
	if err != nil {
		return err
	}
	var countries []string
	doc.Find(`select[name="srch_COUNTRY_countryList_47"] option`).Each(func(_ int, o *goquery.Selection) {
		if c := strings.TrimSpace(o.AttrOr("value", "")); c != "" {
			if c == "US" {
				countries = append([]string{c}, countries...)
			} else {
				countries = append(countries, c)
			}
		}
	})
	if len(countries) == 0 {
		return fmt.Errorf("no country options on %s (layout changed?)", base)
	}

	seen := map[string]bool{}
	walk := func(criterion, value, fallbackCountry string) error {
		for start := 1; start <= 20000; start += 20 {
			doc, base, err := getDoc(ctx, f, pmaSearchURL(criterion, value, start))
			if err != nil {
				return err
			}
			recs := pmaRows(doc, base, fallbackCountry)
			for _, r := range recs {
				if seen[r.ID] {
					continue
				}
				seen[r.ID] = true
				if err := emit(r); err != nil {
					return err
				}
			}
			total := 0
			if m := pmaTotalRe.FindStringSubmatch(doc.Find(".prevNextBtnsDiv").Text()); m != nil {
				total, _ = strconv.Atoi(m[3])
			}
			if len(recs) == 0 || (total > 0 && start+20 > total) || (total == 0 && len(recs) < 20) {
				return nil
			}
		}
		return nil
	}
	for _, c := range countries {
		if err := walk("srch_COUNTRY_countryList_47", c, c); err != nil {
			return err
		}
	}
	// members with a blank country: last-name prefixes
	for c := 'a'; c <= 'z'; c++ {
		if err := walk("srch_LASTNAME_singleLineText_46", string(c), ""); err != nil {
			return err
		}
	}
	return nil
}

// pmaRows parses the result rows of one listing page.
func pmaRows(doc *goquery.Document, base, fallbackCountry string) []Record {
	var out []Record
	doc.Find("li.searchResultsListGroupItem").Each(func(_ int, li *goquery.Selection) {
		ref := li.Find(`a[href*="detail_lookup_id"]`).First().AttrOr("href", "")
		if ref == "" {
			ref = li.Find("button[data-clickurl]").First().AttrOr("data-clickurl", "")
		}
		m := pmaLookupRe.FindStringSubmatch(ref)
		name := clean(li.Find("h3").First().Text())
		if m == nil || name == "" { // not a member row
			return
		}
		id := strings.ToLower(m[1])
		r := Record{
			ID: id, Name: name, Country: fallbackCountry, Meta: map[string][]string{},
			URL: pmaBase + "?controller=memberDirectory&action=resultsDetail&directory_id=5&detail_lookup_id=" + m[1] + "&bCustomMapData=0",
		}
		r.Emails = emailsIn(li)
		// plain <p> lines: [company,] "City ST ZIP", country code (the email line has a link)
		var lines []string
		li.Find(".i4a_ck_right67 p").Each(func(_ int, p *goquery.Selection) {
			if p.Find("a").Length() == 0 {
				if t := clean(p.Text()); t != "" {
					lines = append(lines, t)
				}
			}
		})
		if n := len(lines); n > 0 && pmaCodeRe.MatchString(lines[n-1]) {
			r.Country = lines[n-1]
			lines = lines[:n-1]
		}
		switch n := len(lines); {
		case n >= 2:
			r.Business = lines[0]
			pmaPlace(&r, lines[n-1])
		case n == 1:
			pmaPlace(&r, lines[0])
		}
		out = append(out, r)
	})
	return out
}

// pmaPlace splits "City ST ZIP" (or "City, ST ZIP"); a line in another shape
// goes to Address whole rather than a guessed city.
func pmaPlace(r *Record, line string) {
	if m := pmaUSCityRe.FindStringSubmatch(line); m != nil {
		r.City, r.Region, r.PostalCode = clean(m[1]), m[2], m[3]
		return
	}
	r.Address = line
}

func pmaDetail(ctx context.Context, f Fetcher, r Record) (Record, error) {
	doc, base, err := getDoc(ctx, f, r.URL)
	if err != nil {
		return Record{}, err
	}
	box := doc.Find("div.memeberDirectory_details").First() // sic
	if box.Length() == 0 {
		return Record{}, fmt.Errorf("no member details on %s (layout changed?)", base)
	}
	d := Record{}
	d.Emails = emailsIn(box)
	if p := structuredPhone(strings.TrimPrefix(box.Find(`a[href^="tel:"]`).First().AttrOr("href", ""), "tel:")); p != "" {
		d.Phones = []string{p}
	}
	// the website: an external link that is not the contact block's mail / phone / vCard
	box.Find(`a[target="_blank"]`).EachWithBreak(func(_ int, a *goquery.Selection) bool {
		h := strings.TrimSpace(a.AttrOr("href", ""))
		if strings.HasPrefix(strings.ToLower(h), "http") && !strings.Contains(h, "vCard") {
			d.Websites = []string{h}
			return false
		}
		return true
	})
	// contact block: street / "City, ST ZIP" lines separated by <br>
	ps := box.Find("p")
	contact := ps.FilterFunction(func(_ int, p *goquery.Selection) bool {
		return p.Find(`a[href^="tel:"], a[href^="mailto:"]`).Length() > 0
	}).First()
	if contact.Length() > 0 {
		c := contact.Clone()
		c.Find("a, i").Remove()
		html, _ := c.Html()
		var lines []string
		for _, ln := range strings.Split(strings.NewReplacer("<br/>", "\n", "<br>", "\n", "<br />", "\n").Replace(html), "\n") {
			if ln = clean(ln); ln != "" {
				lines = append(lines, ln)
			}
		}
		if n := len(lines); n > 0 {
			var place Record
			pmaPlace(&place, lines[n-1])
			if place.City != "" {
				d.City, d.Region, d.PostalCode = place.City, place.Region, place.PostalCode
				lines = lines[:n-1]
			}
			d.Address = strings.Join(lines, ", ")
		}
	}
	// "About Me:" text: the paragraph after the label
	ps.Each(func(i int, p *goquery.Selection) {
		if strings.HasPrefix(clean(p.Text()), "About Me:") {
			if t := clean(ps.Eq(i + 1).Text()); t != "" {
				d.Bio = t
			}
			if t := clean(strings.TrimPrefix(clean(p.Text()), "About Me:")); t != "" {
				d.Bio = t
			}
		}
	})
	return d, nil
}
