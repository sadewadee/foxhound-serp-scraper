package prodir

// EREPS (European Register of Exercise Professionals) member directory,
// https://www.ereps.eu/member-directory (~13.8k registered fitness
// professionals, individuals).
//
// The listing is a Drupal view with plain GET pagination: ?page=N (0-based,
// 40 rows per page, ~345 pages), a table row per member with membership id,
// name, city, country, "qualified as" and expiry. The last page has no pager
// link, so the walk stops at the first page without rows. The row's "full
// profile" link (/member-profile/{id}) gives the stable id and the detail URL.
// The profile adds only an optional website (the globe icon link) and the
// free-text "About me" bio. There is no email, phone, address or social
// profile (contact is a logged-in form), so Detail is only worth it for the
// website. robots.txt asks for Crawl-delay 10 but allows these paths; the
// crawl runs at 1s like the other sources.

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const erepsBase = "https://www.ereps.eu"

// erepsDefaultBio is the placeholder text of a profile whose owner wrote none.
const erepsDefaultBio = "This member is a Registered Exercise Professional."

func init() {
	register(&Source{
		Slug: "ereps", Name: "EREPS member directory", Host: "www.ereps.eu",
		Category: "fitness_trainer", ListingType: "individual", MinInterval: time.Second,
		List: erepsList, Detail: erepsDetail,
	})
}

func erepsList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	for page := 0; page < 1000; page++ {
		doc, base, err := getDoc(ctx, f, fmt.Sprintf("%s/member-directory?page=%d", erepsBase, page))
		if err != nil {
			return err
		}
		n := 0
		var rows []Record
		doc.Find("table tbody tr").Each(func(_ int, tr *goquery.Selection) {
			href := tr.Find("td.views-field-nothing a[href]").First().AttrOr("href", "")
			if href == "" {
				return
			}
			n++
			r := Record{
				ID: path.Base(strings.TrimRight(href, "/")), URL: absURL(base, href),
				Name:    clean(tr.Find("td.views-field-display-name").Text()),
				City:    clean(tr.Find("td.views-field-city").Text()),
				Country: clean(tr.Find("td.views-field-country").Text()),
				Meta:    map[string][]string{},
			}
			addMeta(r.Meta, "qualified_as", splitMeta(tr.Find("td.views-field-ereps-status-7").Text(), ",")...)
			addMeta(r.Meta, "master_trainer", clean(tr.Find("td.views-field-discipline-565").Text()))
			addMeta(r.Meta, "expires", clean(tr.Find("td.views-field-end-date").Text()))
			rows = append(rows, r)
		})
		if n == 0 {
			return nil
		}
		for _, r := range rows {
			if err := emit(r); err != nil {
				return err
			}
		}
	}
	return nil
}

func erepsDetail(ctx context.Context, f Fetcher, r Record) (Record, error) {
	doc, base, err := getDoc(ctx, f, r.URL)
	if err != nil {
		return Record{}, err
	}
	// members who filled in nothing get the page frame without any section
	if doc.Find("h1.page-title").Length() == 0 {
		return Record{}, fmt.Errorf("no member profile on %s (layout changed?)", base)
	}
	d := Record{}
	// the bio is the az-text block that follows the "About me:" heading block
	doc.Find("h4").EachWithBreak(func(_ int, h *goquery.Selection) bool {
		if !strings.HasPrefix(clean(h.Text()), "About me") {
			return true
		}
		if bio := clean(h.Closest(".az-text").NextFiltered(".az-text").Text()); bio != erepsDefaultBio {
			d.Bio = bio
		}
		return false
	})
	// the website is the link wrapped around the globe icon
	doc.Find(`a[href]:has(img[alt="website"])`).EachWithBreak(func(_ int, a *goquery.Selection) bool {
		if w := absURL(base, a.AttrOr("href", "")); strings.HasPrefix(w, "http") {
			d.Websites = []string{w}
			return false
		}
		return true
	})
	return d, nil
}
