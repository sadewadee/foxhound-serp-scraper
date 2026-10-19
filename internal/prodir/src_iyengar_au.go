package prodir

// Iyengar Yoga Australia (find a school or teacher), https://iyengaryoga.asn.au/
// (~430 entries: ~317 teachers, ~113 schools).
//
// A public WP REST endpoint returns every teacher and school at once, no
// paging: /wp-json/iya/v1/search-teachers?type=all ({results[], total}). Each
// entry gives email, phone and website (when published), "Suburb, STATE" and
// the geo point; schools also give a street address, teachers the school they
// teach at. The list has no level. UK and Ireland are the UK association's
// (iyengaruk), the USA iynaus. Country is Australia unless the location text
// ends in a country the resolver knows. Schools stay in this source, flagged
// with meta entry_type=school.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const iyengarAUURL = "https://iyengaryoga.asn.au/wp-json/iya/v1/search-teachers?type=all"

func init() {
	register(&Source{
		Slug: "iyengar-au", Name: "Iyengar Yoga Australia teachers and schools", Host: "iyengaryoga.asn.au",
		Category: "yoga_teacher", ListingType: "individual", MinInterval: time.Second,
		List: iyengarAUList,
	})
}

type iyengarAUEntry struct {
	ID       int    `json:"id"`
	Type     string `json:"type"` // teacher | school
	Name     string `json:"name"`
	Location string `json:"location"`
	Address  string `json:"address"`
	Link     string `json:"link"`
	Email    string `json:"email"`
	Phone    string `json:"phone"`
	Website  string `json:"website"`
	School   string `json:"school"`
}

func iyengarAUList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	p, err := f.Get(ctx, iyengarAUURL, nil)
	if err != nil {
		return err
	}
	if err := p.Err(); err != nil {
		return err
	}
	var resp struct {
		Results []iyengarAUEntry `json:"results"`
	}
	if err := json.Unmarshal(p.Body, &resp); err != nil {
		return err
	}
	seen := map[int]bool{}
	for _, e := range resp.Results {
		if e.ID == 0 || clean(e.Name) == "" || seen[e.ID] { // no id or name: not an entry
			continue
		}
		seen[e.ID] = true
		if err := emit(e.record()); err != nil {
			return err
		}
	}
	return nil
}

func (e iyengarAUEntry) record() Record {
	r := Record{
		ID: fmt.Sprint(e.ID), URL: e.Link, Name: clean(e.Name), Address: clean(e.Address),
		Business: clean(e.School), Meta: map[string][]string{},
	}
	head, country := yogaSplitLocation(e.Location)
	r.Country = firstNonEmpty(country, "Australia")
	// "Suburb, STATE": the last part is the state
	if i := strings.LastIndex(head, ","); i >= 0 {
		r.City, r.Region = clean(head[:i]), clean(head[i+1:])
	} else {
		r.City = head
	}
	if v := strings.TrimSpace(e.Email); v != "" {
		r.Emails = []string{v}
	}
	if v := strings.TrimSpace(e.Phone); v != "" {
		r.Phones = []string{v}
	}
	if v := strings.TrimSpace(e.Website); v != "" {
		r.Websites = []string{v}
	}
	if strings.EqualFold(e.Type, "school") {
		addMeta(r.Meta, "entry_type", "school")
	}
	return r
}
