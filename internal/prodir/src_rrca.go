package prodir

// Road Runners Club of America certified running coaches,
// https://www.rrca.org/coaches/ (~3,500 individuals).
//
// The whole directory is ONE server-rendered table (table#tblIndividuals,
// ~900 KB): name with its profile link, city, state/province and a country
// code per row. The id is the slug of /coach/{slug}/ (always taken from the
// link, slugs get "-2" suffixes on collision). The profile adds the
// Cloudflare-protected email (decoded), the website (some profiles), the
// certification, the postal code ("City ST ZIP", no street) and the coaching
// services. There is no phone. The list's country is already ISO-2.

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const rrcaBase = "https://www.rrca.org"

func init() {
	register(&Source{
		Slug: "rrca", Name: "RRCA certified running coaches", Host: "rrca.org",
		Category: "sports_running", ListingType: "individual", MinInterval: time.Second,
		List: rrcaList, Detail: rrcaDetail,
	})
}

func rrcaList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	doc, base, err := getDoc(ctx, f, rrcaBase+"/coaches/")
	if err != nil {
		return err
	}
	rows := doc.Find("table#tblIndividuals tbody tr")
	if rows.Length() == 0 {
		return fmt.Errorf("no coach table on %s (layout changed?)", base)
	}
	var out []Record
	seen := map[string]bool{}
	rows.Each(func(_ int, tr *goquery.Selection) {
		cells := tr.Find("td")
		a := cells.Eq(0).Find("a[href]").First()
		href := absURL(base, a.AttrOr("href", ""))
		id := path.Base(strings.TrimRight(href, "/"))
		if href == "" || id == "" || id == "." || seen[id] { // a row without a profile link is no entry
			return
		}
		seen[id] = true
		out = append(out, Record{
			ID: id, URL: href, Name: clean(a.Text()),
			City: clean(cells.Eq(1).Text()), Region: clean(cells.Eq(2).Text()), Country: clean(cells.Eq(3).Text()),
		})
	})
	for _, r := range out {
		if err := emit(r); err != nil {
			return err
		}
	}
	return nil
}

func rrcaDetail(ctx context.Context, f Fetcher, r Record) (Record, error) {
	doc, base, err := getDoc(ctx, f, r.URL)
	if err != nil {
		return Record{}, err
	}
	info := doc.Find(".profile-info").First()
	if info.Length() == 0 {
		return Record{}, fmt.Errorf("no profile info on %s (layout changed?)", base)
	}
	d := Record{Meta: map[string][]string{}}
	if n := strings.TrimSpace(strings.TrimPrefix(clean(doc.Find("h1").First().Text()), "Coach ")); n != "" {
		d.Name = n
	}
	d.Emails = emailsIn(info)
	info.Find("li").Each(func(_ int, li *goquery.Selection) {
		if strings.HasPrefix(strings.ToLower(clean(li.Find("strong").First().Text())), "certification") {
			c := li.Clone()
			c.Find("strong").Remove()
			addMeta(d.Meta, "credential", splitList(c.Text())...)
		}
	})
	if a := info.Find("li a[target=_blank]").First(); a.Length() > 0 {
		d.Websites = []string{absURL(base, a.AttrOr("href", ""))}
	}
	// "City ST ZIP": the postal code is what follows the list's region
	if addr := clean(info.Find("address").First().Text()); addr != "" {
		if i := strings.LastIndex(addr, " "+r.Region+" "); r.Region != "" && i >= 0 {
			d.PostalCode = clean(addr[i+len(r.Region)+2:])
		}
	}
	doc.Find(".coaching-services li").Each(func(_ int, li *goquery.Selection) {
		addMeta(d.Meta, "specialties", li.Text())
	})
	d.Bio = clean(doc.Find(".main-info .bio").First().Text())
	return d, nil
}
