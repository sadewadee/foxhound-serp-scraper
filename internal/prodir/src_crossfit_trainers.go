package prodir

// CrossFit trainer directory, https://www.crossfit.com/trainer-directory
// (trainerdirectory.crossfit.com redirects there; ~93,000 trainers, individuals).
//
// The directory SPA reads a public JSON API, c3po.crossfit.com/api/lms/v1/trainers
// ?page=N&per_page=10 (10 is the hard maximum; a bare JSON array, an empty
// array past the end). The full crawl is ~9,300 requests, ~2.6 hours at 1s.
// Only the documented query parameters may be sent (others are HTTP 400). The
// API wants the SPA's Origin and Referer headers. There is no detail stage:
// the list carries name, location (country, state, city, zip), the level and
// the certification emails, which are plain text and repeat on every
// certification row (the distinct set is taken). There is no phone, website or
// social link.
//
// Opt-outs: is_visible=false listings are skipped, and the emails of a trainer
// with is_contactable=false ("don't contact me") are not taken (the API still
// returns them). Records are deduped by user_id, since the order shifts while
// the crawl runs.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const crossfitTrainersAPI = "https://c3po.crossfit.com/api/lms/v1/trainers"

func init() {
	register(&Source{
		Slug: "crossfit-trainers", Name: "CrossFit trainer directory", Host: "trainerdirectory.crossfit.com",
		Category: "crossfit_trainer", ListingType: "individual", MinInterval: time.Second,
		List: crossfitTrainersList,
	})
}

type crossfitTrainer struct {
	UserID         int64  `json:"user_id"`
	IsContactable  *bool  `json:"is_contactable"`
	IsVisible      *bool  `json:"is_visible"`
	IsOpenToWork   bool   `json:"is_open_to_work"`
	About          string `json:"about"`
	Headline       string `json:"headline"`
	Level          string `json:"level"`
	LevelLabel     string `json:"level_label"`
	First          string `json:"first_name"`
	Last           string `json:"last_name"`
	City           string `json:"city"`
	State          string `json:"state"`
	Zip            string `json:"zip"`
	CountryName    string `json:"country_name"`
	CountryCode    string `json:"country_code"`
	Certifications []struct {
		Name   string `json:"name"`
		Email  string `json:"email"`
		Level  string `json:"level"`
		Active bool   `json:"active"`
	} `json:"certifications"`
}

func crossfitTrainersList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	hdr := map[string]string{"Origin": "https://www.crossfit.com", "Referer": "https://www.crossfit.com/"}
	seen := map[int64]bool{}
	for page := 1; page <= 20000; page++ {
		p, err := f.Get(ctx, fmt.Sprintf("%s?page=%d&per_page=10", crossfitTrainersAPI, page), hdr)
		if err != nil {
			return err
		}
		if err := p.Err(); err != nil {
			return err
		}
		var items []crossfitTrainer
		if err := json.Unmarshal(p.Body, &items); err != nil {
			return fmt.Errorf("page %d: %w", page, err)
		}
		if len(items) == 0 {
			return nil
		}
		for _, t := range items {
			// removed listings and rows without an id or a name are not entries
			if t.UserID == 0 || seen[t.UserID] || (t.IsVisible != nil && !*t.IsVisible) ||
				clean(t.First+" "+t.Last) == "" {
				continue
			}
			seen[t.UserID] = true
			if err := emit(t.record()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (t crossfitTrainer) record() Record {
	r := Record{
		ID: fmt.Sprint(t.UserID), Name: clean(t.First + " " + t.Last),
		City: t.City, Region: t.State, PostalCode: t.Zip,
		Country: firstNonEmpty(clean(t.CountryName), clean(t.CountryCode)),
		Bio:     strings.TrimSpace(strings.Join([]string{t.Headline, t.About}, "\n")),
		Meta:    map[string][]string{},
	}
	contactable := t.IsContactable == nil || *t.IsContactable
	for _, c := range t.Certifications {
		if contactable {
			for _, e := range structuredEmails(c.Email) {
				r.Emails = addString(r.Emails, e)
			}
		}
		if c.Active && c.Level == "SPECIALTY" {
			addMeta(r.Meta, "specialties", c.Name)
		}
	}
	addMeta(r.Meta, "level", firstNonEmpty(t.LevelLabel, t.Level))
	if t.IsOpenToWork {
		addMeta(r.Meta, "open_to_work", "yes")
	}
	return r
}
