package prodir

// CrossFit affiliates (gyms), https://www.crossfit.com/cf/find-a-box.php
//
// The "find a box" page is backed by a JSON endpoint:
// find-a-box.php?page=N returns {"affiliates":[...]}, 50 per page (~80-100
// pages); a page past the end is an empty array. All values are strings. There
// is no profile page and no email or phone, only name, address, an optional
// website and the affiliate status, so the list carries everything.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const crossfitURL = "https://www.crossfit.com/cf/find-a-box.php"

func init() {
	register(&Source{
		Slug: "crossfit-affiliates", Name: "CrossFit affiliate directory", Host: "crossfit.com",
		Category: "crossfit_trainer", ListingType: "gym", MinInterval: time.Second,
		List: crossfitList,
	})
}

type crossfitAffiliate struct {
	AID             string `json:"aid"`
	Name            string `json:"name"`
	Address         string `json:"address"`
	City            string `json:"city"`
	Zip             string `json:"zip"`
	Country         string `json:"country"`
	CountryCode     string `json:"country_short_code"`
	State           string `json:"full_state"`
	StateCode       string `json:"state_code"`
	Website         string `json:"website"`
	Kids            string `json:"kids"`
	Active          string `json:"active"`
	Status          string `json:"status"`
	BadStanding     string `json:"bad_standing"`
	EstablishedDate string `json:"established_date"`
}

func crossfitList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	seen := map[string]bool{}
	for page := 1; page <= 1000; page++ {
		p, err := f.Get(ctx, fmt.Sprintf("%s?page=%d", crossfitURL, page), nil)
		if err != nil {
			return err
		}
		if err := p.Err(); err != nil {
			return err
		}
		var resp struct {
			Affiliates []crossfitAffiliate `json:"affiliates"`
		}
		if err := json.Unmarshal(p.Body, &resp); err != nil {
			return fmt.Errorf("page %d: %w", page, err)
		}
		if len(resp.Affiliates) == 0 {
			return nil
		}
		for _, a := range resp.Affiliates {
			if seen[a.AID] || !a.live() {
				continue
			}
			seen[a.AID] = true
			if err := emit(a.record()); err != nil {
				return err
			}
		}
	}
	return nil
}

// live is an active affiliate in good standing.
func (a crossfitAffiliate) live() bool { return a.Active == "1" && a.BadStanding == "0" }

func (a crossfitAffiliate) record() Record {
	r := Record{
		ID: a.AID, Name: a.Name, Address: a.Address, City: a.City, PostalCode: a.Zip,
		Region:  firstNonEmpty(a.State, a.StateCode),
		Country: firstNonEmpty(a.Country, a.CountryCode),
		Meta:    map[string][]string{},
	}
	if w := strings.TrimSpace(a.Website); w != "" {
		r.Websites = []string{w}
	}
	addMeta(r.Meta, "status", a.Status)
	addMeta(r.Meta, "kids", a.Kids)
	date, _, _ := strings.Cut(a.EstablishedDate, "T")
	addMeta(r.Meta, "established_date", date)
	return r
}
