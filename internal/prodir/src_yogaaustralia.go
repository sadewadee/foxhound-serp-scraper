package prodir

// Yoga Australia teacher search, https://yogaaustralia.org.au/teachers/
// (~2,278 registered Level 1-3 teachers, individuals).
//
// The HTML site (profiles included) sits behind a Cloudflare challenge, but the
// custom WP REST endpoint of the NON-www host is open:
// /wp-json/ya/v1/teachers/search?limit=100&offset=N&seed=S (limit is capped at
// 100, ~23 requests; a fixed seed keeps the order stable across pages). The
// response carries a total "count". Data is thin: name, suburb, state, level,
// credential and specialty labels and a bio excerpt. There is no email, phone
// or website, and no Detail (profiles are challenged); Bio is where the
// loader may still find a contact. Every record is Australian.

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

const (
	yogaAustraliaURL   = "https://yogaaustralia.org.au/wp-json/ya/v1/teachers/search"
	yogaAustraliaLimit = 100
)

func init() {
	register(&Source{
		Slug: "yogaaustralia", Name: "Yoga Australia teacher search", Host: "yogaaustralia.org.au",
		Category: "yoga_teacher", ListingType: "individual", MinInterval: time.Second,
		List: yogaAustraliaList,
	})
}

type yogaAustraliaTeacher struct {
	ID          int      `json:"id"`
	Name        string   `json:"name"`
	LevelMarker string   `json:"level_marker"`
	CredLabels  []string `json:"cred_labels"`
	SpecLabels  []string `json:"spec_labels"`
	Suburb      string   `json:"suburb"`
	State       string   `json:"state"`
	Bio         string   `json:"bio_excerpt"`
	ProfileURL  string   `json:"profile_url"`
}

func yogaAustraliaList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	seen := map[int]bool{}
	for offset := 0; offset < 100000; {
		p, err := f.Get(ctx, fmt.Sprintf("%s?limit=%d&offset=%d&seed=prodir", yogaAustraliaURL, yogaAustraliaLimit, offset), nil)
		if err != nil {
			return err
		}
		if err := p.Err(); err != nil {
			return err
		}
		var resp struct {
			Results []yogaAustraliaTeacher `json:"results"`
			Count   int                    `json:"count"`
		}
		if err := json.Unmarshal(p.Body, &resp); err != nil {
			return fmt.Errorf("offset %d: %w", offset, err)
		}
		for _, t := range resp.Results {
			if t.ID == 0 || clean(t.Name) == "" || seen[t.ID] { // no id or name: not an entry
				continue
			}
			seen[t.ID] = true
			r := Record{
				ID: fmt.Sprint(t.ID), URL: t.ProfileURL, Name: clean(t.Name),
				City: clean(t.Suburb), Region: clean(t.State), Country: "Australia",
				Bio: clean(t.Bio), Meta: map[string][]string{},
			}
			addMeta(r.Meta, "level", t.LevelMarker)
			addMeta(r.Meta, "credentials", t.CredLabels...)
			addMeta(r.Meta, "specialties", t.SpecLabels...)
			if err := emit(r); err != nil {
				return err
			}
		}
		offset += len(resp.Results)
		if len(resp.Results) == 0 || offset >= resp.Count {
			return nil
		}
	}
	return nil
}
