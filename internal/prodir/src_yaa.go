package prodir

// Yoga Alliance of Canada (Yoga Association), https://yoga.ca/ (~285 teachers
// and studios, mostly individuals).
//
// A GeoDirectory REST endpoint lists every place with its contact details:
// /wp-json/geodir/v2/places?per_page=100&page=N (3 pages; a page past the end
// is HTTP 400, and a page under 100 places is the last). Email (geo_email),
// phone, website, facebook / instagram / twitter and the company come from
// the place itself; default_category / post_category carry the YAA level.
// robots.txt disallows generic agents; the API itself answers without auth.
//
// Per-record override: a place whose yoga_style list names "Hot Yoga" is
// category hot_yoga_teacher (2 of 285). The one studio entry (category 248)
// is kept and flagged with meta entry_type=studio.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	yaaURL         = "https://yoga.ca/wp-json/geodir/v2/places"
	yaaPerPage     = 100
	yaaStudioCatID = "248"
)

func init() {
	register(&Source{
		Slug: "yaa", Name: "Yoga Alliance of Canada directory", Host: "yoga.ca",
		Category: "yoga_teacher", ListingType: "individual", MinInterval: time.Second,
		List: yaaList,
	})
}

type yaaPlace struct {
	ID    int `json:"id"`
	Title struct {
		Raw string `json:"raw"`
	} `json:"title"`
	Link    string `json:"link"`
	Content struct {
		Rendered string `json:"rendered"`
	} `json:"content"`
	DefaultCategory string `json:"default_category"`
	PostCategory    []struct {
		Name string `json:"name"`
	} `json:"post_category"`
	First     string `json:"first_name"`
	Last      string `json:"last_name"`
	Street    string `json:"street"`
	Country   string `json:"country"`
	Region    string `json:"region"`
	City      string `json:"city"`
	Zip       string `json:"zip"`
	Email     string `json:"geo_email"`
	Phone     string `json:"phone"`
	Company   string `json:"company_name"`
	Website   string `json:"website"`
	Facebook  string `json:"facebook"`
	Instagram string `json:"instagram"`
	Twitter   string `json:"twitter"`
	YogaStyle struct {
		Raw string `json:"raw"`
	} `json:"yoga_style"`
	Delivery struct {
		Raw string `json:"raw"`
	} `json:"delivery"`
	Mentorship struct {
		Raw string `json:"raw"`
	} `json:"mentorship"`
}

func yaaList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	for page := 1; page <= 100; page++ {
		p, err := f.Get(ctx, fmt.Sprintf("%s?per_page=%d&page=%d", yaaURL, yaaPerPage, page), nil)
		if err != nil {
			return err
		}
		if p.Status == 400 && page > 1 { // WordPress: page number past the last page
			return nil
		}
		if err := p.Err(); err != nil {
			return err
		}
		var places []yaaPlace
		if err := json.Unmarshal(p.Body, &places); err != nil {
			return fmt.Errorf("page %d: %w", page, err)
		}
		for _, pl := range places {
			if r, ok := pl.record(); ok {
				if err := emit(r); err != nil {
					return err
				}
			}
		}
		if len(places) < yaaPerPage {
			return nil
		}
	}
	return nil
}

func (p yaaPlace) record() (Record, bool) {
	name := firstNonEmpty(clean(p.Title.Raw), clean(p.First+" "+p.Last))
	if p.ID == 0 || name == "" { // no name: not an entry
		return Record{}, false
	}
	r := Record{
		ID: fmt.Sprint(p.ID), URL: p.Link, Name: name, Business: clean(p.Company),
		City: clean(p.City), Region: clean(p.Region), PostalCode: clean(p.Zip),
		Country: firstNonEmpty(clean(p.Country), "Canada"),
		Bio:     yogaHTMLText(p.Content.Rendered),
		Meta:    map[string][]string{},
	}
	// street often repeats the city instead of holding a street address
	if s := clean(p.Street); !strings.EqualFold(s, clean(p.City)) {
		r.Address = s
	}
	if e := strings.TrimSpace(p.Email); e != "" {
		r.Emails = []string{e}
	}
	if ph := strings.TrimSpace(p.Phone); ph != "" {
		r.Phones = []string{ph}
	}
	if w := strings.TrimSpace(p.Website); w != "" {
		r.Websites = []string{w}
	}
	for plat, u := range map[string]string{"facebook": p.Facebook, "instagram": p.Instagram, "twitter": p.Twitter} {
		if u = strings.TrimSpace(u); u != "" {
			if r.Social == nil {
				r.Social = map[string]string{}
			}
			r.Social[plat] = u
		}
	}
	for _, c := range p.PostCategory {
		addMeta(r.Meta, "level", c.Name)
	}
	if p.DefaultCategory == yaaStudioCatID {
		addMeta(r.Meta, "entry_type", "studio")
	}
	styles := yogaSplitList(p.YogaStyle.Raw, ",")
	addMeta(r.Meta, "styles", styles...)
	addMeta(r.Meta, "delivery", yogaSplitList(p.Delivery.Raw, ",")...)
	addMeta(r.Meta, "mentorship", p.Mentorship.Raw)
	for _, s := range styles {
		if strings.EqualFold(s, "Hot Yoga") {
			r.Category = "hot_yoga_teacher"
		}
	}
	return r, true
}
